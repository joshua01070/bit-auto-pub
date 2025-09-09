#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
multi_coin_auto_trade_live_final_v3_armed.py

LIVE trading bot for "MTF Gating + 1m Armed Entry".
- Backfilling 1m feed cache (no more 200-bar limit) with gaps/length diagnostics.
- Minute tasks (fetch 1m, resample N, gate, armed-trigger) run concurrently per symbol.
- Fast loop (intrabar manage) runs every second regardless of minute tasks.
- "Next 1m open" optional execution for ARMED trigger (backtest-timing compatible).
- Non-loosening stop invariant.
- Startup reconcile from exchange as single source of truth.
- Heartbeat guaranteed every HEARTBEAT_SEC, no drift.
- Text logs + JSONL event logs for audit/analysis (lightweight, transition-focused).

Backtest parity targets (align with backtest_breakout_partial_final_armed.py):
- Donchian upper uses previous n bars (excluding current).
- Gating at just-closed N-minute bar, resample(label='right', closed='right').
- Entry buffer = upper + price_unit(upper) * entry_tick_buffer.
- Armed anchor = max(Donch upper, prev N-min close).
- Optional execution at next 1m open on trigger (ARMED_EXEC=next_open).
- Rounding via price_unit/round_price throughout.
"""

import os, time, json, math, logging, traceback, uuid
from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import logging.handlers

import pandas as pd
import pyupbit

# ========================= guard =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
if os.getenv("UPBIT_LIVE") != "1":
    raise SystemExit("Refuse to run LIVE. Set env UPBIT_LIVE=1 to confirm.")

# ========================= run/session logging =========================
RUN_ID = time.strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
LOG_DIR, EVT_DIR = "logs", "events"
def ensure_dir(p: str): os.makedirs(p, exist_ok=True)
ensure_dir(LOG_DIR); ensure_dir(EVT_DIR)

LOG_PATH = os.path.join(LOG_DIR, f"live_{RUN_ID}.log")
_fh = logging.handlers.TimedRotatingFileHandler(LOG_PATH, when="midnight", backupCount=7, encoding="utf-8")
_fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logging.getLogger().addHandler(_fh)

EVENTS_PATH = os.path.join(EVT_DIR, f"events_{RUN_ID}.ndjson")
def log_event(event: str, **data):
    data.update({"ts": time.time(), "run_id": RUN_ID, "event": event})
    try:
        with open(EVENTS_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(data, ensure_ascii=False) + "\n")
    except Exception as e:
        logging.warning(f"[eventlog] write failed: {e}")

# ========================= config =========================
ACCESS = ""
SECRET = ""

TICKERS = ["KRW-BTC", "KRW-ETH", "KRW-SOL"]
STATE_SCHEMA_VER = 4  # bumped (pending_buy_ts added)

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
                "armed_enable": 1, "armed_micro_trigger": "REBREAK",
                "armed_inval_pct": 0.003, "entry_tick_buffer": 1},
}

POLL_SEC = 1.0
STATE_DIR = "state_live_v3"
HALT_FILE = "halt.flag"
HEARTBEAT_SEC = 60
HEARTBEAT_PATH = "heartbeat.txt"
ensure_dir(STATE_DIR)

# ===== DIAG/FEED ENV =====
LOG_DATA_DIAG     = int(os.getenv("LOG_DATA_DIAG", "1"))
BACKFILL_LIMIT    = int(os.getenv("LIVE_BACKFILL_LIMIT", "10"))
NEED_BUF_TF       = int(os.getenv("LIVE_NEED_BUF_TF", "80"))
NEED_BUF_1M       = int(os.getenv("LIVE_NEED_BUF_1M", "60"))
FETCH_PAGE        = int(os.getenv("LIVE_FETCH_COUNT", "200"))   # <=200 per pyupbit limit
MAX_CACHE_MIN     = int(os.getenv("LIVE_MAX_CACHE_MIN", "5000"))
ARMED_EXEC        = os.getenv("ARMED_EXEC", "next_open").lower()  # "next_open" | "immediate"

# minute task pool
FETCH_WORKERS = min(3, len(TICKERS))     # conservative
MINUTE_TASK_DEADLINE = 15.0              # seconds cap for all minute tasks
PER_TASK_TIMEOUT = 6.0                   # per-symbol minute task timeout

# ========================= utils =========================
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

def price_unit(p: float) -> float:
    # Upbit tick table
    if p >= 2_000_000: return 1000.0
    if p >= 1_000_000: return 500.0
    if p >=   500_000: return 100.0
    if p >=   100_000: return 50.0
    if p >=    10_000: return 10.0
    if p >=     1_000: return 5.0
    if p >=       100: return 1.0
    if p >=        10: return 0.1
    return 0.01

def round_price(p: float, direction: str = "nearest") -> float:
    u = price_unit(p)
    if direction == "down": return math.floor(p / u) * u
    if direction == "up":   return math.ceil(p / u) * u
    return round(p / u) * u

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

# ========================= broker/feed/circuit =========================
class CircuitBreaker:
    def __init__(self, max_fails=3, cool_sec=120):
        self.max_fails, self.cool_sec = max_fails, cool_sec
        self.fail_count = 0
        self.block_until = 0.0
    def blocked(self) -> bool: return time.time() < self.block_until
    def ok(self):
        if self.fail_count != 0 or self.blocked():
            log_event("cb_ok")
        self.fail_count = 0
    def fail(self, tag=""):
        self.fail_count += 1
        logging.warning(f"[CB] failure {self.fail_count}/{self.max_fails} {tag}")
        log_event("cb_fail", tag=tag, count=self.fail_count)
        if self.fail_count >= self.max_fails:
            self.block_until = time.time() + self.cool_sec
            self.fail_count = 0
            logging.error(f"[CB] BLOCKED {self.cool_sec}s")
            log_event("cb_blocked", cool_sec=self.cool_sec)

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

# -------- Backfilling Feed (1m cache + diagnostics) --------
class Feed:
    def __init__(self, ticker: str):
        self.ticker = ticker
        self._cache: Optional[pd.DataFrame] = None

    def _pull(self, to_ts: Optional[pd.Timestamp] = None, count: int = FETCH_PAGE) -> pd.DataFrame:
        kw = {"interval": "minute1", "count": min(int(count), 200)}
        if to_ts is not None:
            kw["to"] = to_ts.strftime("%Y-%m-%d %H:%M:%S")  # KST naive acceptable for pyupbit
        df = safe_call(pyupbit.get_ohlcv, self.ticker, **kw) or pd.DataFrame()
        if df.empty: return df
        df = strip_partial_1m(df).sort_index()
        df = df[~df.index.duplicated(keep="last")]
        return df

    def _sync_latest(self):
        latest = self._pull()
        if latest is None or latest.empty: return
        if self._cache is None or self._cache.empty:
            self._cache = latest
        else:
            self._cache = pd.concat([self._cache, latest]).sort_index()
            self._cache = self._cache[~self._cache.index.duplicated(keep="last")]
        if len(self._cache) > MAX_CACHE_MIN:
            self._cache = self._cache.iloc[-MAX_CACHE_MIN:]

    def _backfill_until(self, min_len: int, limit: int = BACKFILL_LIMIT):
        tries = 0
        while self._cache is not None and len(self._cache) < min_len and tries < limit:
            oldest = self._cache.index[0]
            older = self._pull(oldest)
            if older is None or older.empty:
                break
            self._cache = pd.concat([older, self._cache]).sort_index()
            self._cache = self._cache[~self._cache.index.duplicated(keep="last")]
            tries += 1
            time.sleep(0.25)

    def fetch_1m_needed(self, need_minutes: int) -> pd.DataFrame:
        self._sync_latest()
        if self._cache is None or self._cache.empty:
            self._sync_latest()
        if self._cache is not None and len(self._cache) < need_minutes:
            self._backfill_until(need_minutes)

        out = self._cache.iloc[-need_minutes:].copy() if self._cache is not None else pd.DataFrame()
        # diagnostics: minute gaps
        gaps = 0
        if len(out) >= 2:
            dif = out.index.to_series().diff().dropna()
            gaps = int((dif != pd.Timedelta(minutes=1)).sum())
        if LOG_DATA_DIAG:
            logging.info(f"[{self.ticker}] [data] 1m={len(out)} need={need_minutes} gaps={gaps}")
        if gaps > 0:
            log_event("data_gap", symbol=self.ticker, gaps=gaps, length=len(out), need=need_minutes)
        return out

# ========================= state/strategy =========================
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
    # Pending (for next-open execution)
    pending_buy_ts: float = 0.0

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
        st.pending_buy_ts = float(d.get("pending_buy_ts", 0.0))
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
        log_event("resolve_timeout", side="buy", symbol=self.symbol, uuid=uuid, last_state=last_state)
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
        log_event("resolve_timeout", side="sell", symbol=self.symbol, uuid=uuid, last_state=last_state)
        return 0.0

    # ----- ENTER / EXIT -----
    def _enter_position(self, cb: CircuitBreaker, reason: str):
        try:
            if self.state.has_pos: return
            total_krw = self.broker.krw_balance()
            amt = total_krw * float(self.p["buy_pct"])
            if amt < float(self.p["min_krw_order"]):
                logging.info(f"[{self.symbol}] [SKIP] low budget {amt:.0f}")
                log_event("skip_buy", symbol=self.symbol, reason="low_budget", krw=amt)
                return
            resp = self.broker.buy_market_krw(self.symbol, amt)
            uuid_ = str(resp.get("uuid",""))
            log_event("order_submitted", side="buy", symbol=self.symbol, uuid=uuid_, krw=amt, reason=reason)
            if not uuid_:
                cb.fail("buy-uuid"); logging.error(f"[{self.symbol}] BUY no uuid"); return
            qty, avg = self._resolve_buy_fill(uuid_)
            if qty<=0 or avg<=0:
                cb.fail("buy-nofill"); logging.error(f"[{self.symbol}] BUY no fill"); return
            self.state.has_pos=True; self.state.qty=qty; self.state.avg=avg
            self.state.init_qty=qty; self.state.ptp_hits=[]; self.state.tp1_done=False
            self.state.ts_anchor=avg
            self.state.pending_buy_ts=0.0
            self._raise_stop(round_price(avg*(1.0-self._trail_step()), "down"))
            self.state.is_armed=False; self._save_state(); cb.ok()
            logging.info(f"[{self.symbol}] [BUY-{reason}] {qty:.8f} @ {round_price(avg,'down'):.0f}")
            log_event("trade_filled", side="buy", symbol=self.symbol, uuid=uuid_, qty=qty, avg=avg, reason=reason)
        except Exception as e:
            cb.fail("buy-exc"); logging.error(f"[{self.symbol}] BUY error: {e}"); logging.error(traceback.format_exc())

    def _sell(self, qty: float, reason: str):
        try:
            qty = min(qty, self.state.qty)
            if qty<=0: return
            resp = self.broker.sell_market(self.symbol, qty)
            uuid_ = str(resp.get("uuid",""))
            log_event("order_submitted", side="sell", symbol=self.symbol, uuid=uuid_, qty=qty, reason=reason)
            if not uuid_:
                logging.error(f"[{self.symbol}] SELL no uuid"); return
            filled = self._resolve_sell_fill(uuid_)
            if filled<=0: logging.error(f"[{self.symbol}] SELL no fill"); return
            self.state.qty -= filled
            if self.state.qty <= 1e-12:
                self.state = PositionState()  # flat reset
            self._save_state()
            logging.info(f"[{self.symbol}] [SELL-{reason}] qty={filled:.8f}")
            log_event("trade_filled", side="sell", symbol=self.symbol, uuid=uuid_, qty=filled, reason=reason, stop=self.state.stop)
        except Exception as e:
            logging.error(f"[{self.symbol}] SELL error: {e}"); logging.error(traceback.format_exc())

    # ----- GATE (N-min close) -----
    def on_bar_close_tf(self, dfN: pd.DataFrame, cb: CircuitBreaker):
        n = int(self.p["don_n"]); ma_l = int(self.p["ma_l"]); ma_s_v = int(self.p["ma_s"])
        if LOG_DATA_DIAG:
            logging.info(f"[{self.symbol}] [gate-diag] barsN={len(dfN)} need_don>={n+1} need_ma_l>={ma_l+1}")
        highs, closes = dfN["high"], dfN["close"]
        if len(highs)<n+1:
            if LOG_DATA_DIAG: logging.info(f"[{self.symbol}] [gate-skip] short_don_n({len(highs)}/{n+1})")
            log_event("gate_skip", symbol=self.symbol, reason="short_don_n", bars=len(highs), need=n+1)
            return
        upper = float(highs.iloc[-(n+1):-1].max())              # exclude current
        prev_close = float(closes.iloc[-1])                     # just-closed N-min close

        ma_pass=True
        if int(self.p["use_ma"])==1:
            ms=sma(closes,ma_s_v); ml=sma(closes,ma_l)
            if ms is None or ml is None or pd.isna(ms.iloc[-1]) or pd.isna(ml.iloc[-1]) or ms.iloc[-1] <= ml.iloc[-1]:
                ma_pass=False

        up_buf = upper + price_unit(upper)*int(self.p.get("entry_tick_buffer",2))
        break_pass = prev_close > up_buf

        if int(self.p.get("armed_enable",0))==0:
            if not self.state.has_pos and break_pass and ma_pass and not cb.blocked():
                logging.info(f"[{self.symbol}] [LEGACY-ENTER] pass → BUY")
                self._enter_position(cb,"LEGACY")
            return

        # Armed window open (anchor per backtest: max(upper, prev_close))
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
            logging.info(f"[{self.symbol}] [ARMED] {window}m anchor={round_price(anchor,'down'):.0f}")
            log_event("armed_open", symbol=self.symbol, window=window, anchor=anchor)

    # ----- TRIGGER (1m, once per minute) -----
    def check_armed_trigger_1m(self, df1m: pd.DataFrame, cb: CircuitBreaker):
        if not self.state.is_armed: return
        now_t = time.time()
        if now_t >= self.state.armed_until:
            self.state.is_armed=False; self._save_state()
            logging.info(f"[{self.symbol}] [DISARMED] window expired")
            log_event("armed_close", symbol=self.symbol, reason="window_expired")
            return
        if df1m is None or len(df1m)<max(int(self.p["armed_ma_l_1m"])+1,int(self.p["armed_rebreak_lookback"])+3): return
        last_closed_ts = df1m.index[-1].to_pydatetime().timestamp()
        if last_closed_ts <= (self.state.armed_open_ts or 0.0): return

        prev1m, cur1m = df1m.iloc[-2], df1m.iloc[-1]
        self.state.red_run_1m = self.state.red_run_1m + 1 if cur1m["close"]<prev1m["close"] else 0
        if int(self.p.get("armed_reds",3))>0 and self.state.red_run_1m>=int(self.p["armed_reds"]):
            self.state.is_armed=False; self._save_state()
            logging.info(f"[{self.symbol}] [DISARMED] reds={self.p['armed_reds']}")
            log_event("armed_close", symbol=self.symbol, reason="reds")
            return

        # invalidate if 1m low pierces anchor*(1-inval)
        if float(cur1m["low"]) <= float(self.state.armed_anchor_price)*(1.0-float(self.p["armed_inval_pct"])):
            self.state.is_armed=False; self._save_state()
            logging.info(f"[{self.symbol}] [DISARMED] low<=anchor")
            log_event("armed_close", symbol=self.symbol, reason="anchor_break")
            return

        trig=str(self.p.get("armed_micro_trigger","MA")).upper(); trigger_ok=False
        if trig=="MA":
            s1=sma(df1m["close"],int(self.p["armed_ma_s_1m"])); l1=sma(df1m["close"],int(self.p["armed_ma_l_1m"]))
            if s1 is not None and l1 is not None and len(s1)>=2 and len(l1)>=2:
                if s1.iloc[-2] <= l1.iloc[-2] and s1.iloc[-1] > l1.iloc[-1]: trigger_ok=True
        elif trig=="REBREAK":
            k=int(self.p["armed_rebreak_lookback"]); recent_high=float(df1m["high"].iloc[-k-1:-1].max())
            if float(cur1m["close"])>recent_high: trigger_ok=True

        if trigger_ok and not cb.blocked() and not self.state.has_pos:
            if ARMED_EXEC == "next_open":
                self.state.pending_buy_ts = math.floor(time.time()/60)*60 + 60
                self._save_state()
                logging.info(f"[{self.symbol}] [TRIGGER-{trig}] confirmed → BUY_PENDING(NEXT_OPEN)")
                log_event("armed_trigger", symbol=self.symbol, trigger=trig, exec="next_open",
                          pending_ts=self.state.pending_buy_ts)
            else:
                logging.info(f"[{self.symbol}] [TRIGGER-{trig}] confirmed → BUY_NOW")
                log_event("armed_trigger", symbol=self.symbol, trigger=trig, exec="immediate")
                self._enter_position(cb,f"ARMED-{trig}")

    # ----- intrabar manage (1s) -----
    def manage_intrabar(self, bid: float, ask: float):
        # pending next-open buy (safety: execute when time reached)
        if (not self.state.has_pos) and (self.state.pending_buy_ts > 0.0) and (time.time() >= self.state.pending_buy_ts):
            self._enter_position(CircuitBreaker(), "ARMED-NEXTOPEN")
            self.state.pending_buy_ts = 0.0
            self._save_state()

        if not self.state.has_pos: return
        trail=self._trail_step(); high_now=max(bid,ask)
        self._raise_stop(round_price(high_now*(1.0-trail), "down"))

        # PTP + BE bump
        if self.ptp_levels:
            for (lv, fr) in self.ptp_levels:
                if lv in (self.state.ptp_hits or []): continue
                tgt=round_price(self.state.avg*(1.0+lv), "down")
                if high_now>=tgt and self.state.qty>0:
                    sell_qty=min(self.state.init_qty*float(fr), self.state.qty)
                    if sell_qty>0:
                        self._sell(sell_qty, f"PTP({lv:.3f},{fr:.2f})")
                        (self.state.ptp_hits or []).append(lv)
                        if int(self.p.get("ptp_be",0))==1:
                            be=self.state.avg*(1.0+float(self.p["fee"])+float(self.p["eps_exec"]))
                            self._raise_stop(round_price(be, "up"))
                            self.state.tp1_done=True; self._save_state()

        # TP
        tp=float(self.p.get("tp_rate",0.0))
        if tp>0 and self.state.qty>0:
            tp_px=round_price(self.state.avg*(1.0+tp), "down")
            if high_now>=tp_px: self._sell(self.state.qty,"TP"); return

        # TS
        if self.state.stop and bid<=self.state.stop and self.state.qty>0:
            self._sell(self.state.qty,"TS")

# ========================= reconcile =========================
def reconcile_from_exchange(strats: Dict[str, SymbolStrategy], broker: UpbitBroker):
    try:
        bals=broker.get_balances() or []; by_ccy={b.get("currency"):b for b in bals}
        for sym, stg in strats.items():
            ccy=sym.split("-")[1]; b=by_ccy.get(ccy)
            qty=float((b or {}).get("balance") or 0.0); avg=float((b or {}).get("avg_buy_price") or 0.0)
            if qty>1e-12:
                stg.state.has_pos=True; stg.state.qty=qty; stg.state.avg=avg
                stg.state.init_qty=qty; stg.state.ptp_hits=[]; stg.state.tp1_done=False
                stg.state.is_armed=False; stg.state.pending_buy_ts=0.0
                stg._raise_stop(round_price(avg*(1.0-stg._trail_step()), "down"))
                stg._save_state()
                logging.info(f"[{sym}] [RECONCILE] qty={qty:.8f}, avg={avg:.0f}")
                log_event("reconcile_pos", symbol=sym, qty=qty, avg=avg)
            else:
                if stg.state.has_pos or stg.state.qty>0:
                    stg.state=PositionState(); stg._save_state()
                    logging.info(f"[{sym}] [RECONCILE] cleared local state")
                    log_event("reconcile_clear", symbol=sym)
    except Exception as e:
        logging.error(f"[reconcile] {e}"); logging.error(traceback.format_exc())

# ========================= params loader =========================
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

# ========================= need-bars calculator =========================
def compute_need_1m(p: Dict[str, Any]) -> Tuple[int,int,int]:
    tf = int(p["tf_base"])
    need_tf_bars = max(int(p["don_n"])+1, int(p["ma_l"])+1)
    need_1m_from_tf = (need_tf_bars + NEED_BUF_TF) * tf
    need_1m_for_armed = max(int(p.get("armed_ma_l_1m",20))+2,
                            int(p.get("armed_rebreak_lookback",5))+3,
                            50)
    need_1m_total = max(need_1m_from_tf, need_1m_for_armed) + NEED_BUF_1M
    return need_1m_total, need_tf_bars, need_1m_for_armed

# ========================= main loop (tick scheduler) =========================
def main():
    if not ACCESS or not SECRET:
        raise SystemExit("ACCESS/SECRET required. Set env UPBIT_ACCESS/UPBIT_SECRET or fill in code.")
    broker=UpbitBroker(ACCESS, SECRET)
    params_by_symbol=load_symbol_params()
    feeds={s: Feed(s) for s in TICKERS}
    strats={s: SymbolStrategy(s, broker, params_by_symbol.get(s, {})) for s in TICKERS}
    CB={s: CircuitBreaker() for s in TICKERS}

    logging.info(f"[init] started v3 armed bot RUN_ID={RUN_ID}")
    log_event("boot", tickers=TICKERS, run_id=RUN_ID)
    reconcile_from_exchange(strats, broker)

    # ===== Absolute schedules (no drift) =====
    next_min_tick = (int(time.time() // 60) + 1) * 60
    next_hb_tick  = (int(time.time() // HEARTBEAT_SEC) + 1) * HEARTBEAT_SEC

    DATA_READY: Dict[str, Optional[str]] = {s: None for s in TICKERS}

    def minute_task(symbol: str):
        """Fetch 1m (backfill), resample N, gate & armed-trigger once per minute (per symbol)."""
        if CB[symbol].blocked(): return
        p = strats[symbol].p
        need_1m_total, need_tf_bars, need_1m_for_armed = compute_need_1m(p)

        # 1) 1m ensure length
        df1 = feeds[symbol].fetch_1m_needed(need_minutes=need_1m_total)
        if df1 is None or df1.empty:
            if LOG_DATA_DIAG: logging.info(f"[{symbol}] [diag] df1m empty")
            return

        # 2) N-minute resample
        tf = int(p["tf_base"])
        dfN = resample_right_closed_ohlc(df1, tf)
        if dfN is None or dfN.empty:
            if LOG_DATA_DIAG: logging.info(f"[{symbol}] [diag] dfN empty (tf={tf}m)")
            return

        # 3) readiness
        ready_gate  = len(dfN) >= need_tf_bars
        ready_armed = len(df1) >= need_1m_for_armed + NEED_BUF_1M
        reasons = []
        if not ready_gate:  reasons.append(f"short_tfN({len(dfN)}/{need_tf_bars})")
        if int(p.get("armed_enable",0))==1 and not ready_armed:
            reasons.append(f"short_armed1m({len(df1)}/{need_1m_for_armed+NEED_BUF_1M})")

        cur_state = "READY" if (ready_gate and (int(p.get("armed_enable",0))==0 or ready_armed)) else "NOT_READY"
        if LOG_DATA_DIAG:
            state_str = cur_state if cur_state=="READY" else (cur_state+":"+",".join(reasons))
            logging.info(f"[{symbol}] [diag] tf={tf}m dfN={len(dfN)} needN>={need_tf_bars} | df1m={len(df1)} need1m>={need_1m_total} | {state_str}")

        prev_state = DATA_READY.get(symbol)
        if prev_state != cur_state:
            DATA_READY[symbol] = cur_state
            log_event("data_ready_change", symbol=symbol, state=cur_state,
                      dfN=len(dfN), needN=need_tf_bars, df1m=len(df1), need1m=need_1m_total,
                      reasons=reasons)

        # 4) gate/trigger on new N-minute close
        cur_idx = dfN.index[-1]
        if strats[symbol].last_tf_idx is None or cur_idx != strats[symbol].last_tf_idx:
            strats[symbol].on_bar_close_tf(dfN, CB[symbol])
            strats[symbol].last_tf_idx = cur_idx
            CB[symbol].ok()

        if strats[symbol].state.is_armed:
            strats[symbol].check_armed_trigger_1m(df1, CB[symbol])

    while True:
        try:
            now=time.time()

            # ===== minute boundary: launch concurrent minute tasks AFTER boundary =====
            if now >= next_min_tick:
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
                # catch-up precisely
                now2 = time.time()
                while next_min_tick <= now2:
                    next_min_tick += 60

            # ===== fast loop: every second manage intrabar =====
            for s in TICKERS:
                try:
                    if CB[s].blocked(): continue
                    bid, ask = broker.best_bid_ask(s)
                    # manage pending buy and trailing/ptp/ts
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
                        lines.append(f"{s}: pos={st.qty:.6f}@{st.avg:.0f} stop={round_price(st.stop,'down'):.0f} tp1={int(st.tp1_done)}")
                    elif st.is_armed:
                        remain=int(max(0, st.armed_until - now))
                        lines.append(f"{s}: ARMED({remain}s) anchor={round_price(st.armed_anchor_price,'down'):.0f}")
                    else:
                        lines.append(f"{s}: idle")
                logging.info("[hb] " + " | ".join(lines))
                heartbeat_file()
                while next_hb_tick <= now:
                    next_hb_tick += HEARTBEAT_SEC

            # ===== halting =====
            if os.path.exists(HALT_FILE):
                logging.warning("[HALT] halt.flag detected; sleeping 5s")
                time.sleep(5)
                continue

            time.sleep(POLL_SEC)

        except KeyboardInterrupt:
            logging.warning("[main] KeyboardInterrupt → exit")
            log_event("shutdown", reason="keyboard")
            break
        except Exception as e:
            logging.error(f"[main] fatal: {e}")
            logging.error(traceback.format_exc())
            log_event("fatal", error=str(e))
            time.sleep(2)

if __name__ == "__main__":
    main()
