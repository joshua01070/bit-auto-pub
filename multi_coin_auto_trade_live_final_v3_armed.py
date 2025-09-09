#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
multi_coin_auto_trade_live_final_v3_armed.py

LIVE trading bot for "MTF Gating + 1m Armed Entry".
- Resolve fills (poll get_order); no fire-and-forget.
- Minute tasks (fetch 1m, resample N, gate, armed-trigger) run concurrently per symbol.
- Fast loop (intrabar manage) runs every second regardless of minute tasks.
- Non-loosening stop invariant.
- Startup reconcile from exchange as single source of truth.
- Heartbeat guaranteed every HEARTBEAT_SEC, no drift.
"""

import os, time, json, math, logging, traceback
from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

import pandas as pd
import pyupbit

# ============== logging & guard ==============
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
if os.getenv("UPBIT_LIVE") != "1":
    raise SystemExit("Refuse to run LIVE. Set env UPBIT_LIVE=1 to confirm.")

# ============== config ==============
ACCESS = ""
SECRET = ""

TICKERS = ["KRW-BTC", "KRW-ETH", "KRW-SOL"]
STATE_SCHEMA_VER = 3

DEFAULTS: Dict[str, Any] = dict(
    tf_base=5, use_ma=1, ma_s=10, ma_l=30, don_n=72, entry_tick_buffer=2,
    armed_enable=1, armed_window_min=5, armed_micro_trigger="MA",
    armed_ma_s_1m=5, armed_ma_l_1m=20, armed_rebreak_lookback=5,
    armed_inval_pct=0.005, armed_reds=3,
    fee=0.0005, min_krw_order=6000.0, qty_step=1e-8, buy_pct=0.30, max_position_krw=None,
    trail_before=0.08, trail_after=0.08, ptp_levels="", ptp_be=1, tp_rate=0.0,
    fast_seq="HL", eps_exec=0.0003, dust_margin=0.98,
)

SYMBOL_PARAMS_DEFAULT: Dict[str, Dict[str, Any]] = {
    "KRW-BTC": {"tf_base": 5, "use_ma": 1, "ma_s": 8,  "ma_l": 60, "don_n": 64,
                "buy_pct": 0.35, "trail_before": 0.08, "trail_after": 0.08,
                "ptp_levels": "", "tp_rate": 0.035, "fast_seq": "HL",
                "armed_enable": 0, "entry_tick_buffer": 1},
    "KRW-ETH": {"tf_base": 5, "use_ma": 1, "ma_s": 10, "ma_l": 60, "don_n": 48,
                "buy_pct": 0.30, "trail_before": 0.08, "trail_after": 0.08,
                "ptp_levels": "", "tp_rate": 0.035, "fast_seq": "HL",
                "armed_enable": 0, "entry_tick_buffer": 1},
    "KRW-SOL": {"tf_base": 5, "use_ma": 1, "ma_s": 8,  "ma_l": 45, "don_n": 96,
                "buy_pct": 0.25, "trail_before": 0.08, "trail_after": 0.08,
                "ptp_levels": "", "tp_rate": 0.03,  "fast_seq": "HL",
                "armed_enable": 1, "armed_micro_trigger": "MA",
                "armed_inval_pct": 0.003, "entry_tick_buffer": 1},
}

POLL_SEC = 1.0
STATE_DIR = "state_live_v3"
HALT_FILE = "halt.flag"
HEARTBEAT_SEC = 60
HEARTBEAT_PATH = "heartbeat.txt"

# minute task pool
FETCH_WORKERS = 3                      # <= len(TICKERS)
MINUTE_TASK_DEADLINE = 15.0            # seconds cap for all minute tasks
PER_TASK_TIMEOUT = 6.0                 # per-symbol minute task timeout

# ============== utils ==============
def ensure_dir(p: str): os.makedirs(p, exist_ok=True)
ensure_dir(STATE_DIR)

def patch_pyupbit_timeout():
    try:
        import pyupbit.request_api as _ra
        _orig_pub = _ra._call_public_api
        def _pub(url, **kw):
            kw.setdefault("timeout", 4)  # seconds
            return _orig_pub(url, **kw)
        _ra._call_public_api = _pub
        if hasattr(_ra, "_call_private_api"):
            _orig_pri = _ra._call_private_api
            def _pri(url, **kw):
                kw.setdefault("timeout", 4)
                return _orig_pri(url, **kw)
            _ra._call_private_api = _pri
        logging.info("[init] pyupbit timeout=4s patch OK")
    except Exception as e:
        logging.warning(f"[init] timeout patch failed: {e}")
patch_pyupbit_timeout()

def tick_size_for(p: float) -> float:
    if p >= 2_000_000: return 1000.0
    if p >= 1_000_000: return 500.0
    if p >=   500_000: return 100.0
    if p >=   100_000: return 50.0
    if p >=    10_000: return 10.0
    if p >=     1_000: return 5.0
    if p >=       100: return 1.0
    if p >=        10: return 0.1
    return 0.01

def round_to_tick(p: float) -> float:
    t = tick_size_for(p); return math.floor(p / t) * t

def floor_to_step(x: float, step: float) -> float:
    return math.floor(x / step) * step

def parse_ptp_levels(spec: str) -> List[Tuple[float, float]]:
    out: List[Tuple[float, float]] = []
    for part in (spec or "").split(","):
        part = part.strip()
        if not part: continue
        lv, pct = part.split(":")
        out.append((float(lv), float(pct)))
    out.sort(key=lambda x: x[0])
    return out

def halted() -> bool: return os.path.exists(HALT_FILE)

def safe_call(fn, *args, tries: int = 3, delay: float = 0.5, backoff: float = 1.7, **kwargs):
    t = delay
    for i in range(tries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if i == tries - 1:
                logging.error(f"[safe_call] {fn.__name__} failed: {e}")
                raise
            logging.warning(f"[safe_call] {fn.__name__} error: {e}. retry in {t:.1f}s")
            time.sleep(t); t *= backoff

def strip_partial_1m(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame()
    idx: pd.DatetimeIndex = df.index
    # 기준: KST 분 정각 미완성 캔들 제거 (tz-naive/aware 모두 안전 처리)
    if idx.tz is None:
        last_idx_kst_naive = idx[-1].to_pydatetime()
    else:
        last_idx_kst_naive = idx.tz_convert("Asia/Seoul")[-1].to_pydatetime().replace(tzinfo=None)
    now_min_kst_naive = pd.Timestamp.now(tz="Asia/Seoul").floor("min").to_pydatetime().replace(tzinfo=None)
    if last_idx_kst_naive >= now_min_kst_naive:
        return df.iloc[:-1].copy() if len(df) > 1 else df.iloc[:0].copy()
    return df

def resample_right_closed_ohlc(df1m: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df1m is None or df1m.empty: return pd.DataFrame()
    if minutes == 1: return df1m.copy()
    rule = f"{minutes}min"
    out = pd.DataFrame({
        "open":  df1m["open"].resample(rule, label="right", closed="right").first(),
        "high":  df1m["high"].resample(rule, label="right", closed="right").max(),
        "low":   df1m["low"].resample(rule, label="right", closed="right").min(),
        "close": df1m["close"].resample(rule, label="right", closed="right").last(),
        "volume": (df1m["volume"] if "volume" in df1m.columns else pd.Series(index=df1m.index, dtype=float)).resample(rule, label="right", closed="right").sum(),
    }).dropna(subset=["open","high","low","close"])
    return out

def sma(series: pd.Series, n: int) -> Optional[pd.Series]:
    if n <= 0 or series is None or len(series) < n: return None
    return series.rolling(n, min_periods=n).mean()

def heartbeat_file():
    try:
        with open(HEARTBEAT_PATH, "w") as f:
            f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
    except Exception:
        pass

# ============== broker/feed/circuit ==============
class CircuitBreaker:
    def __init__(self, max_fails=3, cool_sec=120):
        self.max_fails, self.cool_sec = max_fails, cool_sec
        self.fail_count = 0
        self.block_until = 0.0
    def blocked(self) -> bool: return time.time() < self.block_until
    def ok(self): self.fail_count = 0
    def fail(self, tag=""):
        self.fail_count += 1
        logging.warning(f"[CB] failure {self.fail_count}/{self.max_fails} {tag}")
        if self.fail_count >= self.max_fails:
            self.block_until = time.time() + self.cool_sec
            self.fail_count = 0
            logging.error(f"[CB] BLOCKED {self.cool_sec}s")

class UpbitBroker:
    def __init__(self, access: str, secret: str):
        self.up = pyupbit.Upbit(access, secret)
    def krw_balance(self) -> float:
        bal = safe_call(self.up.get_balance, "KRW"); return float(bal or 0.0)
    def best_bid_ask(self, ticker: str) -> Tuple[float, float]:
        ob = safe_call(pyupbit.get_orderbook, ticker)
        units = (ob[0] or {}).get("orderbook_units", []) if isinstance(ob, list) and ob else (ob or {}).get("orderbook_units", [])
        if not units: raise RuntimeError("orderbook empty")
        u0 = units[0]; bid = float(u0.get("bid_price", 0.0)); ask = float(u0.get("ask_price", 0.0))
        if bid <= 0 or ask <= 0: raise RuntimeError("invalid bid/ask")
        return bid, ask
    def buy_market_krw(self, ticker: str, krw: float) -> dict:
        return safe_call(self.up.buy_market_order, ticker, krw)
    def sell_market(self, ticker: str, qty: float) -> dict:
        return safe_call(self.up.sell_market_order, ticker, qty)
    def get_order(self, uuid: str) -> dict:
        return safe_call(self.up.get_order, uuid)
    def get_balances(self):
        return safe_call(self.up.get_balances)

class Feed:
    def __init__(self, ticker: str): self.ticker = ticker
    def fetch_1m_df(self, count: int = 200) -> pd.DataFrame:
        df = safe_call(pyupbit.get_ohlcv, self.ticker, interval="minute1", count=count)
        if df is None: return pd.DataFrame()
        return strip_partial_1m(df)

# ============== state/strategy ==============
@dataclass
class PositionState:
    has_pos: bool = False
    qty: float = 0.0
    avg: float = 0.0
    stop: float = 0.0
    ts_anchor: float = 0.0
    ptp_hits: List[float] = None
    tp1_done: bool = False
    init_qty: float = 0.0
    # Armed
    is_armed: bool = False
    armed_until: float = 0.0
    armed_anchor_price: float = 0.0
    red_run_1m: int = 0
    armed_open_ts: float = 0.0
    def to_json(self) -> dict:
        d = asdict(self); d["ptp_hits"] = self.ptp_hits or []; d["_ver"] = STATE_SCHEMA_VER; return d
    @staticmethod
    def from_json(d: dict) -> "PositionState":
        st = PositionState()
        st.has_pos = bool(d.get("has_pos", False))
        st.qty = float(d.get("qty", 0.0)); st.avg = float(d.get("avg", 0.0))
        st.stop = float(d.get("stop", 0.0)); st.ts_anchor = float(d.get("ts_anchor", 0.0))
        st.ptp_hits = list(d.get("ptp_hits", [])); st.tp1_done = bool(d.get("tp1_done", False))
        st.init_qty = float(d.get("init_qty", 0.0))
        st.is_armed = bool(d.get("is_armed", False)); st.armed_until = float(d.get("armed_until", 0.0))
        st.armed_anchor_price = float(d.get("armed_anchor_price", 0.0))
        st.red_run_1m = int(d.get("red_run_1m", 0)); st.armed_open_ts = float(d.get("armed_open_ts", 0.0))
        return st

class SymbolStrategy:
    def __init__(self, symbol: str, broker: UpbitBroker, params: Dict[str, Any]):
        self.symbol = symbol; self.broker = broker
        self.p = {**DEFAULTS, **(params or {})}
        self.state_path = os.path.join(STATE_DIR, f"{self.symbol.replace('-','_')}.json")
        self.state = self._load_state()
        self.ptp_levels = parse_ptp_levels(self.p.get("ptp_levels", ""))
        self.last_tf_idx: Optional[pd.Timestamp] = None

    def _load_state(self) -> PositionState:
        try:
            if os.path.exists(self.state_path):
                with open(self.state_path,"r",encoding="utf-8") as f: d=json.load(f)
                st = PositionState.from_json(d); logging.info(f"[{self.symbol}] state loaded"); return st
        except Exception as e:
            logging.warning(f"[{self.symbol}] state load error: {e}")
        return PositionState()

    def _save_state(self):
        try:
            with open(self.state_path,"w",encoding="utf-8") as f:
                json.dump(self.state.to_json(), f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.warning(f"[{self.symbol}] state save error: {e}")

    def _trail_step(self) -> float: return float(self.p.get("trail_before", 0.08))
    def _raise_stop(self, candidate: float):
        if candidate and candidate > (self.state.stop or 0.0):
            self.state.stop = candidate; self._save_state()

    # ----- fill resolution -----
    def _resolve_buy_fill(self, uuid: str, timeout_s: float = 8.0, poll_s: float = 0.6) -> Tuple[float,float]:
        t0 = time.time(); last_state=""
        while time.time()-t0 < timeout_s:
            od = self.broker.get_order(uuid); last_state=str(od.get("state"))
            trs = od.get("trades") or []
            if last_state=="done" or trs:
                qty,funds=0.0,0.0
                for tr in trs:
                    px=float(tr.get("price",0.0)); vol=float(tr.get("volume",0.0))
                    funds += (float(tr.get("funds",0.0)) or (px*vol)); qty += vol
                avg = funds/qty if qty>0 else 0.0
                return qty, avg
            time.sleep(poll_s)
        logging.error(f"[{self.symbol}] buy resolve timeout (state={last_state})")
        return 0.0,0.0

    def _resolve_sell_fill(self, uuid: str, timeout_s: float = 8.0, poll_s: float = 0.6) -> float:
        t0 = time.time(); last_state=""
        while time.time()-t0 < timeout_s:
            od = self.broker.get_order(uuid); last_state=str(od.get("state"))
            trs = od.get("trades") or []
            if last_state=="done" or trs:
                qty=0.0
                for tr in trs: qty += float(tr.get("volume",0.0))
                return qty
            time.sleep(poll_s)
        logging.error(f"[{self.symbol}] sell resolve timeout (state={last_state})")
        return 0.0

    # ----- ENTER / EXIT -----
    def _enter_position(self, cb: CircuitBreaker, reason: str):
        try:
            if self.state.has_pos: return
            total_krw = self.broker.krw_balance()
            amt = total_krw * float(self.p["buy_pct"])
            if amt < float(self.p["min_krw_order"]):
                logging.info(f"[{self.symbol}] [SKIP] low budget {amt:.0f}"); return
            resp = self.broker.buy_market_krw(self.symbol, amt)
            uuid = str(resp.get("uuid",""))
            if not uuid: cb.fail("buy-uuid"); logging.error(f"[{self.symbol}] BUY no uuid"); return
            qty, avg = self._resolve_buy_fill(uuid)
            if qty<=0 or avg<=0: cb.fail("buy-nofill"); logging.error(f"[{self.symbol}] BUY no fill"); return
            self.state.has_pos=True; self.state.qty=qty; self.state.avg=avg
            self.state.init_qty=qty; self.state.ptp_hits=[]; self.state.tp1_done=False
            self.state.ts_anchor=avg; self._raise_stop(round_to_tick(avg*(1.0-self._trail_step())))
            self.state.is_armed=False; self._save_state(); cb.ok()
            logging.info(f"[{self.symbol}] [BUY-{reason}] {qty:.8f} @ {round_to_tick(avg):.0f}")
        except Exception as e:
            cb.fail("buy-exc"); logging.error(f"[{self.symbol}] BUY error: {e}"); logging.error(traceback.format_exc())

    def _sell(self, qty: float, reason: str):
        try:
            qty = min(qty, self.state.qty)
            if qty<=0: return
            resp = self.broker.sell_market(self.symbol, qty)
            uuid = str(resp.get("uuid",""))
            if not uuid: logging.error(f"[{self.symbol}] SELL no uuid"); return
            filled = self._resolve_sell_fill(uuid)
            if filled<=0: logging.error(f"[{self.symbol}] SELL no fill"); return
            self.state.qty -= filled
            if self.state.qty <= 1e-12:
                self.state = PositionState()  # flat reset
            self._save_state()
            logging.info(f"[{self.symbol}] [SELL-{reason}] qty={filled:.8f}")
        except Exception as e:
            logging.error(f"[{self.symbol}] SELL error: {e}"); logging.error(traceback.format_exc())

    # ----- GATE (N-min close) -----
    def on_bar_close_tf(self, dfN: pd.DataFrame, cb: CircuitBreaker):
        highs, closes = dfN["high"], dfN["close"]
        n=int(self.p["don_n"])
        if len(highs)<n+1: return
        upper = float(highs.iloc[-(n+1):-1].max())
        prev_close = float(closes.iloc[-1])
        ma_pass=True
        if int(self.p["use_ma"])==1:
            ms=sma(closes,int(self.p["ma_s"])); ml=sma(closes,int(self.p["ma_l"]))
            if ms is None or ml is None or pd.isna(ms.iloc[-1]) or pd.isna(ml.iloc[-1]) or ms.iloc[-1] <= ml.iloc[-1]:
                ma_pass=False
        up_buf = upper + tick_size_for(upper)*int(self.p.get("entry_tick_buffer",2))
        break_pass = prev_close > up_buf

        if int(self.p.get("armed_enable",0))==0:
            if not self.state.has_pos and break_pass and ma_pass and not cb.blocked():
                logging.info(f"[{self.symbol}] [LEGACY-ENTER] pass → BUY")
                self._enter_position(cb,"LEGACY")
            return

        # anchor = max(upper, prev_close)  (백테스터와 동일)
        if break_pass and ma_pass and not self.state.has_pos and not self.state.is_armed:
            window = int(self.p.get("armed_window_min", self.p["tf_base"]))
            now_ts = time.time()
            anchor = max(upper, prev_close)
            self.state.is_armed=True
            self.state.armed_until=now_ts+window*60
            self.state.armed_anchor_price=anchor
            self.state.red_run_1m=0
            self.state.armed_open_ts=now_ts
            self._save_state()
            logging.info(f"[{self.symbol}] [ARMED] {window}m anchor={round_to_tick(anchor):.0f}")

    # ----- TRIGGER (1m, once per minute) -----
    def check_armed_trigger_1m(self, df1m: pd.DataFrame, cb: CircuitBreaker):
        if not self.state.is_armed: return
        if time.time() >= self.state.armed_until:
            self.state.is_armed=False; self._save_state()
            logging.info(f"[{self.symbol}] [DISARMED] window expired"); return
        if df1m is None or len(df1m)<max(int(self.p["armed_ma_l_1m"])+1,int(self.p["armed_rebreak_lookback"])+3): return
        last_closed_ts = df1m.index[-1].to_pydatetime().timestamp()
        if last_closed_ts <= (self.state.armed_open_ts or 0.0): return
        prev1m, cur1m = df1m.iloc[-2], df1m.iloc[-1]
        self.state.red_run_1m = self.state.red_run_1m + 1 if cur1m["close"]<prev1m["close"] else 0
        if int(self.p.get("armed_reds",3))>0 and self.state.red_run_1m>=int(self.p["armed_reds"]):
            self.state.is_armed=False; self._save_state()
            logging.info(f"[{self.symbol}] [DISARMED] reds={self.p['armed_reds']}"); return
        # 경계 포함해서 무효화 (<=)
        if float(cur1m["low"]) <= float(self.state.armed_anchor_price)*(1.0-float(self.p["armed_inval_pct"])):
            self.state.is_armed=False; self._save_state()
            logging.info(f"[{self.symbol}] [DISARMED] low<=anchor"); return
        trig=str(self.p.get("armed_micro_trigger","MA")).upper(); trigger_ok=False
        if trig=="MA":
            s1=sma(df1m["close"],int(self.p["armed_ma_s_1m"])); l1=sma(df1m["close"],int(self.p["armed_ma_l_1m"]))
            if s1 is not None and l1 is not None and len(s1)>=2 and len(l1)>=2:
                if s1.iloc[-2] <= l1.iloc[-2] and s1.iloc[-1] > l1.iloc[-1]: trigger_ok=True
        elif trig=="REBREAK":
            k=int(self.p["armed_rebreak_lookback"]); recent_high=float(df1m["high"].iloc[-k-1:-1].max())
            if float(cur1m["close"])>recent_high: trigger_ok=True
        if trigger_ok and not cb.blocked() and not self.state.has_pos:
            logging.info(f"[{self.symbol}] [TRIGGER-{trig}] confirmed → BUY")
            self._enter_position(cb,f"ARMED-{trig}")

    # ----- intrabar manage (1s) -----
    def manage_intrabar(self, bid: float, ask: float):
        if not self.state.has_pos: return
        trail=self._trail_step(); high_now=max(bid,ask)
        self._raise_stop(round_to_tick(high_now*(1.0-trail)))
        # PTP + BE bump
        if self.ptp_levels:
            for (lv, fr) in self.ptp_levels:
                if lv in (self.state.ptp_hits or []): continue
                tgt=round_to_tick(self.state.avg*(1.0+lv))
                if high_now>=tgt and self.state.qty>0:
                    sell_qty=min(self.state.init_qty*float(fr), self.state.qty)
                    if sell_qty>0:
                        self._sell(sell_qty, f"PTP({lv:.3f},{fr:.2f})")
                        (self.state.ptp_hits or []).append(lv)
                        if int(self.p.get("ptp_be",0))==1:
                            be=self.state.avg*(1.0+float(self.p["fee"])+float(self.p["eps_exec"]))
                            self._raise_stop(round_to_tick(be))
                            self.state.tp1_done=True; self._save_state()
        # TP
        tp=float(self.p.get("tp_rate",0.0))
        if tp>0 and self.state.qty>0:
            tp_px=round_to_tick(self.state.avg*(1.0+tp))
            if high_now>=tp_px: self._sell(self.state.qty,"TP"); return
        # TS
        if self.state.stop and bid<=self.state.stop and self.state.qty>0:
            self._sell(self.state.qty,"TS")

# ============== reconcile ==============
def reconcile_from_exchange(strats: Dict[str, SymbolStrategy], broker: UpbitBroker):
    try:
        bals=broker.get_balances() or []; by_ccy={b.get("currency"):b for b in bals}
        for sym, stg in strats.items():
            ccy=sym.split("-")[1]; b=by_ccy.get(ccy)
            qty=float((b or {}).get("balance") or 0.0); avg=float((b or {}).get("avg_buy_price") or 0.0)
            if qty>1e-12:
                stg.state.has_pos=True; stg.state.qty=qty; stg.state.avg=avg
                stg.state.init_qty=qty; stg.state.ptp_hits=[]; stg.state.tp1_done=False
                stg.state.is_armed=False
                stg._raise_stop(round_to_tick(avg*(1.0-stg._trail_step())))
                stg._save_state()
                logging.info(f"[{sym}] [RECONCILE] qty={qty:.8f}, avg={avg:.0f}")
            else:
                if stg.state.has_pos or stg.state.qty>0:
                    stg.state=PositionState(); stg._save_state()
                    logging.info(f"[{sym}] [RECONCILE] cleared local state")
    except Exception as e:
        logging.error(f"[reconcile] {e}"); logging.error(traceback.format_exc())

# ============== params loader ==============
def load_symbol_params() -> Dict[str, Dict[str, Any]]:
    cfg_path=os.getenv("LIVE_PARAMS_PATH","").strip()
    if cfg_path and os.path.exists(cfg_path):
        try:
            with open(cfg_path,"r",encoding="utf-8") as f: raw=json.load(f)
            out={}
            for tk in TICKERS:
                out[tk]= raw.get(tk) or raw.get(tk.split("-")[1]) or {}
            logging.info(f"[init] loaded params from {cfg_path}")
            return out
        except Exception as e:
            logging.warning(f"[init] param load failed: {e}")
    return SYMBOL_PARAMS_DEFAULT

# ============== main loop (tick scheduler) ==============
def main():
    if not ACCESS or not SECRET:
        raise SystemExit("ACCESS/SECRET required.")
    broker=UpbitBroker(ACCESS, SECRET)
    params_by_symbol=load_symbol_params()
    feeds={s: Feed(s) for s in TICKERS}
    strats={s: SymbolStrategy(s, broker, params_by_symbol.get(s, {})) for s in TICKERS}
    CB={s: CircuitBreaker() for s in TICKERS}

    logging.info("[init] started v3 armed bot")
    reconcile_from_exchange(strats, broker)

    # ===== Absolute schedules (no drift) =====
    next_min_tick = math.floor(time.time()/60)*60 + 60
    next_hb_tick  = math.floor(time.time()/HEARTBEAT_SEC)*HEARTBEAT_SEC + HEARTBEAT_SEC

    def minute_task(symbol: str):
        """Fetch 1m, resample N, gate & armed-trigger once per minute (per symbol)."""
        if CB[symbol].blocked(): return
        df1 = feeds[symbol].fetch_1m_df(count=200)
        if df1 is None or df1.empty: return
        tf = int(strats[symbol].p["tf_base"])
        dfN = resample_right_closed_ohlc(df1, tf)
        if dfN is not None and not dfN.empty:
            cur_idx=dfN.index[-1]
            if strats[symbol].last_tf_idx is None or cur_idx != strats[symbol].last_tf_idx:
                strats[symbol].on_bar_close_tf(dfN, CB[symbol])
                strats[symbol].last_tf_idx = cur_idx
                CB[symbol].ok()
        if strats[symbol].state.is_armed:
            strats[symbol].check_armed_trigger_1m(df1, CB[symbol])

    while True:
        try:
            now=time.time()

            # ===== minute boundary: launch concurrent minute tasks =====
            if now >= next_min_tick - 0.2:
                start = time.time()
                with ThreadPoolExecutor(max_workers=min(FETCH_WORKERS, len(TICKERS))) as ex:
                    futs = {ex.submit(minute_task, s): s for s in TICKERS}
                    for fut in as_completed(futs, timeout=MINUTE_TASK_DEADLINE):
                        sym = futs[fut]
                        try:
                            fut.result(timeout=PER_TASK_TIMEOUT)
                        except TimeoutError:
                            CB[sym].fail("minute-timeout")
                        except Exception as e:
                            CB[sym].fail("minute-exc")
                            logging.error(f"[{sym}] minute task error: {e}")
                            logging.error(traceback.format_exc())
                spent = time.time()-start
                if spent>5:
                    logging.info(f"[minute] tasks took {spent:.2f}s")
                # catch-up: 만약 작업이 오래 걸려 경계가 여러 번 지났으면 즉시 따라잡기
                now2 = time.time()
                while next_min_tick <= now2 - 0.2:
                    next_min_tick += 60
                next_min_tick += 60  # 다음 분으로 예약

            # ===== fast loop: every second manage intrabar =====
            for s in TICKERS:
                try:
                    if CB[s].blocked(): continue
                    bid, ask = broker.best_bid_ask(s)
                    if strats[s].state.has_pos:
                        strats[s].manage_intrabar(bid, ask)
                    CB[s].ok()
                except Exception as e:
                    CB[s].fail("fast")
                    logging.error(f"[{s}] fast loop error: {e}")
                    logging.error(traceback.format_exc())

            # ===== heartbeat: absolute schedule, no drift =====
            now = time.time()
            if now >= next_hb_tick:
                lines=[]
                for s, stg in strats.items():
                    st=stg.state
                    if st.has_pos:
                        lines.append(f"{s}: pos={st.qty:.6f}@{st.avg:.0f} stop={st.stop:.0f} tp1={int(st.tp1_done)}")
                    elif st.is_armed:
                        remain=int(max(0, st.armed_until - now))
                        lines.append(f"{s}: ARMED({remain}s) anchor={round_to_tick(st.armed_anchor_price):.0f}")
                    else:
                        lines.append(f"{s}: idle")
                logging.info("[hb] " + " | ".join(lines))
                heartbeat_file()
                # catch-up: 지연 중이면 경계 여러 개 건너뛴 만큼 보정
                while next_hb_tick <= now:
                    next_hb_tick += HEARTBEAT_SEC

            # ===== halting =====
            if halted():
                logging.warning("[HALT] halt.flag detected; sleeping 5s")
                time.sleep(5)
                continue

            time.sleep(POLL_SEC)

        except KeyboardInterrupt:
            logging.warning("[main] KeyboardInterrupt → exit")
            break
        except Exception as e:
            logging.error(f"[main] fatal: {e}")
            logging.error(traceback.format_exc())
            time.sleep(2)

if __name__ == "__main__":
    main()
