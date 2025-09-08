#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
multi_coin_auto_trade_live_final_v3_armed.py

LIVE trading bot for "MTF Gating + 1m Armed Entry".
- Resolve fills: no fire-and-forget. Poll get_order(uuid) and update state from actual fills.
- Data pipeline: fetch 1m only; resample (right/closed) to N-min for gating (backtest alignment).
- Trigger evaluation: once per minute on minute tick — no per-second 1m OHLCV calls.
- Non-loosening stop: helper ensures stop never decreases.
- One-shot startup reconcile: sync local state from exchange balances (source of truth).
"""

import os, time, json, math, logging, traceback
from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple, Dict, Any

import pandas as pd
import pyupbit

# =========================
# 0) RUN GUARD & LOGGING
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
if os.getenv("UPBIT_LIVE") != "1":
    raise SystemExit("Refuse to run LIVE. Set env UPBIT_LIVE=1 to confirm.")

# =========================
# 1) CONFIG (aligned to backtest keys)
# =========================
ACCESS = ""
SECRET = ""

TICKERS = ["KRW-BTC", "KRW-ETH", "KRW-SOL"]
STATE_SCHEMA_VER = 3  # +armed_open_ts

DEFAULTS: Dict[str, Any] = dict(
    # base / gating
    tf_base=5,
    use_ma=1, ma_s=10, ma_l=30,
    don_n=72,
    entry_tick_buffer=2,

    # armed
    armed_enable=1,
    armed_window_min=5,
    armed_micro_trigger="MA",        # "MA" | "REBREAK"
    armed_ma_s_1m=5,
    armed_ma_l_1m=20,
    armed_rebreak_lookback=5,
    armed_inval_pct=0.005,
    armed_reds=3,

    # trade costs / sizing
    fee=0.0005,
    min_krw_order=6000.0,
    qty_step=1e-8,
    buy_pct=0.30,
    max_position_krw=None,

    # exits
    trail_before=0.08,               # unified trail step
    trail_after=0.08,
    ptp_levels="",                   # "0.015:0.6,0.03:0.4"
    ptp_be=1,
    tp_rate=0.0,                     # if 0, rely on trail/PTP only
    fast_seq="HL",                   # HL or LH

    # misc
    eps_exec=0.0003,                 # for BE bump
    dust_margin=0.98,
)

# Default per symbol (override via LIVE_PARAMS_PATH if needed)
SYMBOL_PARAMS_DEFAULT: Dict[str, Dict[str, Any]] = {
    "KRW-BTC": {"tf_base": 5, "use_ma": 1, "ma_s": 8,  "ma_l": 60, "don_n": 64,
                "buy_pct": 0.35, "trail_before": 0.08, "trail_after": 0.08,
                "ptp_levels": "", "tp_rate": 0.035, "fast_seq": "HL", "armed_enable": 0, "entry_tick_buffer": 1},
    "KRW-ETH": {"tf_base": 5, "use_ma": 1, "ma_s": 10, "ma_l": 60, "don_n": 48,
                "buy_pct": 0.30, "trail_before": 0.08, "trail_after": 0.08,
                "ptp_levels": "", "tp_rate": 0.035, "fast_seq": "HL", "armed_enable": 0, "entry_tick_buffer": 1},
    "KRW-SOL": {"tf_base": 5, "use_ma": 1, "ma_s": 8,  "ma_l": 45, "don_n": 96,
                "buy_pct": 0.25, "trail_before": 0.08, "trail_after": 0.08,
                "ptp_levels": "", "tp_rate": 0.03,  "fast_seq": "HL", "armed_enable": 1,
                "armed_micro_trigger": "MA", "armed_inval_pct": 0.003, "entry_tick_buffer": 1},
}

POLL_SEC = 1.0
STATE_DIR = "state_live_v3"
HALT_FILE = "halt.flag"
HEARTBEAT_PATH = "heartbeat.txt"

# =========================
# 2) UTILITIES (PATCHED)
# =========================
def ensure_dir(p: str): os.makedirs(p, exist_ok=True)
ensure_dir(STATE_DIR)

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
    t = tick_size_for(p)
    return math.floor(p / t) * t

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

def halted() -> bool:
    return os.path.exists(HALT_FILE)

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
            time.sleep(t)
            t *= backoff

def strip_partial_1m(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove the *current-minute* row to guarantee closed 1m bars.
    Upbit OHLCV index is usually tz-naive (KST). We compare using KST-naive time.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    idx: pd.DatetimeIndex = df.index  # assume DatetimeIndex
    # last index as tz-naive in Asia/Seoul
    if getattr(idx, "tz", None) is not None:
        last_idx = idx.tz_convert("Asia/Seoul")[-1].tz_localize(None)
    else:
        last_idx = idx[-1]

    # current minute (KST, tz-aware → tz-naive)
    now_min_kst_naive = pd.Timestamp.now(tz="Asia/Seoul").floor("min").tz_localize(None)

    # if last row belongs to the current (still-forming) minute, drop it
    if last_idx >= now_min_kst_naive:
        return df.iloc[:-1].copy() if len(df) > 1 else df.iloc[:0].copy()
    return df

def resample_right_closed_ohlc(df1m: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """Resample with right/closed semantics to mirror backtest."""
    if df1m is None or df1m.empty: return pd.DataFrame()
    if minutes == 1: return df1m.copy()
    rule = f"{minutes}min"
    o = df1m["open"].resample(rule, label="right", closed="right").first()
    h = df1m["high"].resample(rule, label="right", closed="right").max()
    l = df1m["low"].resample(rule, label="right", closed="right").min()
    c = df1m["close"].resample(rule, label="right", closed="right").last()
    vsrc = df1m["volume"] if "volume" in df1m.columns else pd.Series(index=df1m.index, dtype=float)
    v = vsrc.resample(rule, label="right", closed="right").sum()
    out = pd.DataFrame({"open": o, "high": h, "low": l, "close": c, "volume": v}).dropna(subset=["open","high","low","close"])
    return out

def sma(series: pd.Series, n: int) -> Optional[pd.Series]:
    if n <= 0 or series is None or len(series) < n: return None
    return series.rolling(n, min_periods=n).mean()

# =========================
# 3) BROKER / FEED / GUARDS (PATCHED best_bid_ask)
# =========================
class CircuitBreaker:
    def __init__(self, max_fails=3, cool_sec=120):
        self.max_fails = max_fails
        self.cool_sec = cool_sec
        self.fail_count = 0
        self.block_until = 0.0
    def blocked(self) -> bool:
        return time.time() < self.block_until
    def ok(self):
        self.fail_count = 0
    def fail(self, why: str = ""):
        self.fail_count += 1
        logging.warning(f"[CB] failure {self.fail_count}/{self.max_fails} {why}")
        if self.fail_count >= self.max_fails:
            self.block_until = time.time() + self.cool_sec
            self.fail_count = 0
            logging.error(f"[CB] BLOCKED {self.cool_sec}s")

class UpbitBroker:
    def __init__(self, access: str, secret: str):
        self.up = pyupbit.Upbit(access, secret)

    def krw_balance(self) -> float:
        bal = safe_call(self.up.get_balance, "KRW")
        return float(bal or 0.0)

    def best_bid_ask(self, ticker: str) -> Tuple[float, float]:
        ob = safe_call(pyupbit.get_orderbook, ticker)
        units = None

        if isinstance(ob, list) and ob:
            # 정상: [ { "orderbook_units": [ {...}, ... ] } ]
            units = (ob[0] or {}).get("orderbook_units", [])
        elif isinstance(ob, dict):
            # 간헐적으로 dict가 올 수도 있음: { "orderbook_units": [ ... ] } 혹은 에러포맷
            units = ob.get("orderbook_units", [])

        if not units:
            # raise to let CB handle and cool down
            raise RuntimeError("orderbook empty or malformed")

        u0 = units[0]
        bid = float(u0.get("bid_price", 0.0))
        ask = float(u0.get("ask_price", 0.0))
        if bid <= 0 or ask <= 0:
            raise RuntimeError("invalid bid/ask from orderbook")

        return bid, ask

    def buy_market_krw(self, ticker: str, krw: float) -> dict:
        return safe_call(self.up.buy_market_order, ticker, krw)

    def sell_market(self, ticker: str, qty: float) -> dict:
        return safe_call(self.up.sell_market_order, ticker, qty)

    def get_order(self, uuid: str) -> dict:
        return safe_call(self.up.get_order, uuid)

    def cancel_order(self, uuid: str) -> dict:
        return safe_call(self.up.cancel_order, uuid)

    def get_balances(self):
        return safe_call(self.up.get_balances)

    def get_position_qty(self, ticker: str) -> float:
        cc = ticker.split("-")[1]
        bals = self.get_balances() or []
        for b in bals:
            if b.get("currency") == cc:
                return float(b.get("balance") or 0.0)
        return 0.0

class Feed:
    def __init__(self, ticker: str):
        self.ticker = ticker
    def fetch_1m_df(self, count: int = 200) -> pd.DataFrame:
        df = safe_call(pyupbit.get_ohlcv, self.ticker, interval="minute1", count=count)
        if df is None: return pd.DataFrame()
        return strip_partial_1m(df)

# =========================
# 4) STATE
# =========================
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
    armed_open_ts: float = 0.0  # last armed open time (epoch seconds)

    def to_json(self) -> dict:
        d = asdict(self)
        d["ptp_hits"] = self.ptp_hits or []
        d["_ver"] = STATE_SCHEMA_VER
        return d

    @staticmethod
    def from_json(d: dict) -> "PositionState":
        st = PositionState()
        st.has_pos = bool(d.get("has_pos", False))
        st.qty = float(d.get("qty", 0.0))
        st.avg = float(d.get("avg", 0.0))
        st.stop = float(d.get("stop", 0.0))
        st.ts_anchor = float(d.get("ts_anchor", 0.0))
        st.ptp_hits = list(d.get("ptp_hits", []))
        st.tp1_done = bool(d.get("tp1_done", False))
        st.init_qty = float(d.get("init_qty", 0.0))
        # armed
        st.is_armed = bool(d.get("is_armed", False))
        st.armed_until = float(d.get("armed_until", 0.0))
        st.armed_anchor_price = float(d.get("armed_anchor_price", 0.0))
        st.red_run_1m = int(d.get("red_run_1m", 0))
        st.armed_open_ts = float(d.get("armed_open_ts", 0.0))
        return st

# =========================
# 5) STRATEGY
# =========================
class SymbolStrategy:
    def __init__(self, symbol: str, broker: UpbitBroker, params: Dict[str, Any]):
        self.symbol = symbol
        self.broker = broker
        self.p = {**DEFAULTS, **(params or {})}
        self.state_path = os.path.join(STATE_DIR, f"{self.symbol.replace('-','_')}.json")
        self.state = self._load_state()
        self.ptp_levels = parse_ptp_levels(self.p.get("ptp_levels", ""))
        self.last_tf_idx: Optional[pd.Timestamp] = None  # last closed N-min idx

    # ---------- state io ----------
    def _load_state(self) -> PositionState:
        try:
            if os.path.exists(self.state_path):
                with open(self.state_path, "r", encoding="utf-8") as f:
                    d = json.load(f)
                st = PositionState.from_json(d)
                logging.info(f"[{self.symbol}] state loaded")
                return st
        except Exception as e:
            logging.warning(f"[{self.symbol}] state load error: {e}")
        return PositionState()

    def _save_state(self):
        try:
            with open(self.state_path, "w", encoding="utf-8") as f:
                json.dump(self.state.to_json(), f, ensure_ascii=False, indent=2)
        except Exception as e:
            logging.warning(f"[{self.symbol}] state save error: {e}")

    # ---------- helpers ----------
    def _trail_step(self) -> float:
        return float(self.p.get("trail_before", 0.08))

    def _has_budget(self, krw: float) -> bool:
        if krw < float(self.p["min_krw_order"]): return False
        m = self.p.get("max_position_krw")
        if m and krw > float(m): return False
        return True

    def _raise_stop(self, candidate: float):
        """Non-loosening stop: never decreases stop (with immediate persist)."""
        if candidate is None or candidate <= 0: return
        cur = self.state.stop or 0.0
        if candidate > cur:
            self.state.stop = candidate
            self._save_state()

    # ---------- order resolve (no fire-and-forget) ----------
    def _resolve_buy_fill(self, uuid: str, timeout_s: float = 8.0, poll_s: float = 0.6) -> Tuple[float, float]:
        """Return (qty, avg) actually filled; (0,0) if failed."""
        t0 = time.time()
        last_state = ""
        while time.time() - t0 < timeout_s:
            od = self.broker.get_order(uuid)
            last_state = str(od.get("state"))
            trades = od.get("trades") or []
            if last_state == "done" or trades:
                total_qty, total_funds = 0.0, 0.0
                for tr in trades:
                    px = float(tr.get("price", 0.0))
                    vol = float(tr.get("volume", 0.0))
                    funds = float(tr.get("funds", px * vol))
                    total_qty += vol
                    total_funds += funds
                avg = (total_funds / total_qty) if total_qty > 0 else 0.0
                return total_qty, avg
            time.sleep(poll_s)
        logging.error(f"[{self.symbol}] buy resolve timeout, last_state={last_state}")
        return 0.0, 0.0

    def _resolve_sell_fill(self, uuid: str, timeout_s: float = 8.0, poll_s: float = 0.6) -> float:
        """Return qty actually sold; 0 if failed."""
        t0 = time.time()
        last_state = ""
        while time.time() - t0 < timeout_s:
            od = self.broker.get_order(uuid)
            last_state = str(od.get("state"))
            trades = od.get("trades") or []
            if last_state == "done" or trades:
                total_qty = 0.0
                for tr in trades:
                    vol = float(tr.get("volume", 0.0))
                    total_qty += vol
                return total_qty
            time.sleep(poll_s)
        logging.error(f"[{self.symbol}] sell resolve timeout, last_state={last_state}")
        return 0.0

    # ---------- GATE (on N-min close) ----------
    def on_bar_close_tf(self, dfN: pd.DataFrame, cb: CircuitBreaker):
        """Called when a *new* N-min closed candle is detected."""
        if dfN is None or len(dfN) < max(int(self.p["don_n"]) + 1, int(self.p["ma_l"]) + 1):
            return

        highs = dfN["high"]; closes = dfN["close"]
        n = int(self.p["don_n"])
        upper = float(highs.iloc[-(n+1):-1].max())  # Donchian upper excluding current
        prev_close = float(closes.iloc[-1])

        ma_pass = True
        if int(self.p["use_ma"]) == 1:
            ms = sma(closes, int(self.p["ma_s"]))
            ml = sma(closes, int(self.p["ma_l"]))
            if ms is None or ml is None or pd.isna(ms.iloc[-1]) or pd.isna(ml.iloc[-1]) or ms.iloc[-1] <= ml.iloc[-1]:
                ma_pass = False

        tick = tick_size_for(upper)
        up_buf = upper + tick * int(self.p.get("entry_tick_buffer", 2))
        break_pass = prev_close > up_buf

        if int(self.p.get("armed_enable", 0)) == 0:
            # legacy: immediate entry
            if not self.state.has_pos and break_pass and ma_pass and not cb.blocked():
                logging.info(f"[{self.symbol}] [LEGACY-ENTER] {int(self.p['tf_base'])}m pass → BUY")
                self._enter_position(cb, "LEGACY")
            return

        # Armed mode: open window
        if break_pass and ma_pass and not self.state.has_pos and not self.state.is_armed:
            window = int(self.p.get("armed_window_min", self.p["tf_base"]))
            now_ts = time.time()
            self.state.is_armed = True
            self.state.armed_until = now_ts + window * 60
            self.state.armed_anchor_price = upper
            self.state.red_run_1m = 0
            self.state.armed_open_ts = now_ts   # triggers must use 1m bars *after* this
            self._save_state()
            logging.info(f"[{self.symbol}] [ARMED] Gate opened {window}m. Anchor={round_to_tick(upper):.0f}")

    # ---------- TRIGGER (1m, evaluated once per minute) ----------
    def check_armed_trigger_1m(self, df1m: pd.DataFrame, cb: CircuitBreaker):
        if not self.state.is_armed:
            return
        if time.time() >= self.state.armed_until:
            self.state.is_armed = False
            self._save_state()
            logging.info(f"[{self.symbol}] [DISARMED] Window expired")
            return

        if df1m is None or len(df1m) < max(int(self.p["armed_ma_l_1m"]) + 1, int(self.p["armed_rebreak_lookback"]) + 3):
            return

        # Ensure latest closed 1m bar is AFTER arming time
        last_closed_ts = df1m.index[-1].to_pydatetime().timestamp()
        if last_closed_ts <= (self.state.armed_open_ts or 0.0):
            return

        prev1m = df1m.iloc[-2]; cur1m = df1m.iloc[-1]
        # invalidations
        self.state.red_run_1m = self.state.red_run_1m + 1 if cur1m["close"] < prev1m["close"] else 0
        if int(self.p.get("armed_reds", 3)) > 0 and self.state.red_run_1m >= int(self.p["armed_reds"]):
            self.state.is_armed = False
            self._save_state()
            logging.info(f"[{self.symbol}] [DISARMED] {self.p['armed_reds']} red candles")
            return
        if float(cur1m["low"]) < float(self.state.armed_anchor_price) * (1.0 - float(self.p["armed_inval_pct"])):
            self.state.is_armed = False
            self._save_state()
            logging.info(f"[{self.symbol}] [DISARMED] Low < anchor")
            return

        # trigger
        trigger_ok = False
        trig = str(self.p.get("armed_micro_trigger", "MA")).upper()
        if trig == "MA":
            s1 = sma(df1m["close"], int(self.p["armed_ma_s_1m"]))
            l1 = sma(df1m["close"], int(self.p["armed_ma_l_1m"]))
            if s1 is not None and l1 is not None and len(s1) >= 2 and len(l1) >= 2:
                if s1.iloc[-2] <= l1.iloc[-2] and s1.iloc[-1] > l1.iloc[-1]:
                    trigger_ok = True
        elif trig == "REBREAK":
            k = int(self.p["armed_rebreak_lookback"])
            recent_high = float(df1m["high"].iloc[-k-1:-1].max())
            if float(cur1m["close"]) > recent_high:
                trigger_ok = True

        if trigger_ok and not cb.blocked() and not self.state.has_pos:
            logging.info(f"[{self.symbol}] [TRIGGER-{trig}] confirmed → BUY")
            self._enter_position(cb, f"ARMED-{trig}")

    # ---------- ENTER / EXIT (resolve fills) ----------
    def _enter_position(self, cb: CircuitBreaker, reason: str):
        try:
            if self.state.has_pos: return
            total_krw = self.broker.krw_balance()
            amt = total_krw * float(self.p["buy_pct"])
            if not self._has_budget(amt):
                logging.info(f"[{self.symbol}] [SKIP] budget too small ({amt:.0f} KRW)")
                return

            resp = self.broker.buy_market_krw(self.symbol, amt)
            uuid = str(resp.get("uuid", ""))
            if not uuid:
                cb.fail("buy-uuid")
                logging.error(f"[{self.symbol}] BUY no uuid")
                if self.state.is_armed:
                    self.state.is_armed = False; self._save_state()
                return

            qty, avg = self._resolve_buy_fill(uuid)
            if qty <= 0 or avg <= 0:
                cb.fail("buy-nofill")
                logging.error(f"[{self.symbol}] BUY no fill")
                if self.state.is_armed:
                    self.state.is_armed = False; self._save_state()
                return

            # state update from actual fill
            self.state.has_pos = True
            self.state.qty = qty
            self.state.avg = avg
            self.state.stop = round_to_tick(avg * (1.0 - self._trail_step()))
            self.state.ts_anchor = avg
            self.state.ptp_hits = []
            self.state.tp1_done = False
            self.state.init_qty = qty
            if self.state.is_armed:
                self.state.is_armed = False
            self._save_state()
            cb.ok()
            logging.info(f"[{self.symbol}] [BUY-{reason}] {qty:.8f} @ {round_to_tick(avg):.0f}")
        except Exception as e:
            cb.fail("buy-exc")
            logging.error(f"[{self.symbol}] BUY error: {e}")
            logging.error(traceback.format_exc())
            if self.state.is_armed:
                self.state.is_armed = False; self._save_state()

    def _sell(self, qty: float, reason: str):
        try:
            qty = min(qty, self.state.qty)
            if qty <= 0: return
            resp = self.broker.sell_market(self.symbol, qty)
            uuid = str(resp.get("uuid", ""))
            if not uuid:
                logging.error(f"[{self.symbol}] SELL no uuid")
                return
            filled = self._resolve_sell_fill(uuid)
            if filled <= 0:
                logging.error(f"[{self.symbol}] SELL no fill")
                return
            self.state.qty -= filled
            if self.state.qty <= 1e-12:
                self.state.has_pos = False
                self.state.qty = 0.0
                self.state.avg = 0.0
                self.state.stop = 0.0
                self.state.ts_anchor = 0.0
                self.state.ptp_hits = []
                self.state.tp1_done = False
                self.state.init_qty = 0.0
            self._save_state()
            logging.info(f"[{self.symbol}] [SELL-{reason}] qty={filled:.8f}")
        except Exception as e:
            logging.error(f"[{self.symbol}] SELL error: {e}")
            logging.error(traceback.format_exc())

    # ---------- INTRABAR MGMT ----------
    def manage_intrabar(self, last_bid: float, last_ask: float):
        if not self.state.has_pos: return
        trail = self._trail_step()
        high_now = max(last_bid, last_ask)

        # Non-loosening trailing raise
        ts_bar = round_to_tick(high_now * (1.0 - trail))
        self._raise_stop(ts_bar)

        # PTP + BE bump
        if self.ptp_levels:
            for (lv, fr) in self.ptp_levels:
                if lv in (self.state.ptp_hits or []): continue
                tgt = round_to_tick(self.state.avg * (1.0 + lv))
                if high_now >= tgt and self.state.qty > 0:
                    sell_qty = min(self.state.init_qty * float(fr), self.state.qty)
                    if sell_qty > 0:
                        self._sell(sell_qty, f"PTP({lv:.3f},{fr:.2f})")
                        (self.state.ptp_hits or []).append(lv)
                        if int(self.p.get("ptp_be", 0)) == 1:
                            be = self.state.avg * (1.0 + float(self.p["fee"]) + float(self.p["eps_exec"]))
                            self._raise_stop(round_to_tick(be))
                            self.state.tp1_done = True
                            self._save_state()

        # Optional TP
        if float(self.p.get("tp_rate", 0.0)) > 0 and self.state.qty > 0:
            tp_px = round_to_tick(self.state.avg * (1.0 + float(self.p["tp_rate"])))
            if high_now >= tp_px:
                self._sell(self.state.qty, "TP")
                return

        # Trailing stop exit
        if self.state.stop and last_bid <= self.state.stop and self.state.qty > 0:
            self._sell(self.state.qty, "TS")

# =========================
# 6) STARTUP RECONCILE (one-shot)
# =========================
def reconcile_from_exchange(strats: Dict[str, "SymbolStrategy"], broker: "UpbitBroker"):
    """
    One-shot sync at startup:
    - If exchange shows qty > 0: trust exchange and set local state to that position.
    - If exchange shows qty == 0 but local says has_pos: clear local state to avoid ghost.
    """
    try:
        bals = broker.get_balances() or []
        by_ccy = {b.get("currency"): b for b in bals}
        for sym, st in strats.items():
            ccy = sym.split("-")[1]
            b = by_ccy.get(ccy)
            exch_qty = float((b or {}).get("balance") or 0.0)
            avg_buy = float((b or {}).get("avg_buy_price") or 0.0)

            if exch_qty > 1e-12:
                # Trust exchange as source of truth
                st.state.has_pos = True
                st.state.qty = exch_qty
                st.state.avg = avg_buy
                st.state.init_qty = exch_qty
                st.state.ptp_hits = []
                st.state.tp1_done = False
                st.state.is_armed = False  # with an open position, don't arm
                # conservative stop
                if avg_buy > 0:
                    trail = st._trail_step()
                    st._raise_stop(round_to_tick(avg_buy * (1.0 - trail)))
                else:
                    logging.warning(f"[{sym}] avg_buy_price=0 (deposit or transfer). Stop not set; managing with PTP/TS later.")
                st._save_state()
                logging.info(f"[{sym}] [RECONCILE] synced pos from exchange: qty={exch_qty:.8f}, avg={avg_buy:.0f}")
            else:
                # No position on exchange: clear any local leftover
                if st.state.has_pos or st.state.qty > 0:
                    st.state = PositionState()
                    st._save_state()
                    logging.info(f"[{sym}] [RECONCILE] no exchange pos; cleared local state.")
    except Exception as e:
        logging.error(f"[reconcile] error: {e}")
        logging.error(traceback.format_exc())

# =========================
# 7) MAIN LOOP (minute tick; fast intrabar only)
# =========================
def load_symbol_params() -> Dict[str, Dict[str, Any]]:
    cfg_path = os.getenv("LIVE_PARAMS_PATH", "").strip()
    if cfg_path and os.path.exists(cfg_path):
        try:
            with open(cfg_path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            out: Dict[str, Dict[str, Any]] = {}
            for tk in TICKERS:
                key1 = tk; key2 = tk.split("-")[1]
                src = raw.get(key1) or raw.get(key2)
                out[tk] = src or {}
            logging.info(f"[init] loaded params from {cfg_path}")
            return out
        except Exception as e:
            logging.warning(f"[init] param load failed: {e}")
    return SYMBOL_PARAMS_DEFAULT

def heartbeat():
    try:
        with open(HEARTBEAT_PATH, "w") as f:
            f.write(time.strftime("%Y-%m-%d %H:%M:%S"))
    except Exception:
        pass

def sleep_to_next_minute(offset_sec: float = 0.8):
    now = time.time()
    nxt = (int(now // 60) + 1) * 60 + offset_sec
    time.sleep(max(0.0, nxt - now))

def main():
    broker = UpbitBroker(ACCESS, SECRET)
    params_by_symbol = load_symbol_params()
    feeds: Dict[str, Feed] = {s: Feed(s) for s in TICKERS}
    strats: Dict[str, SymbolStrategy] = {s: SymbolStrategy(s, broker, params_by_symbol.get(s, {})) for s in TICKERS}
    CB: Dict[str, CircuitBreaker] = {s: CircuitBreaker() for s in TICKERS}

    logging.info("[init] started v3 armed bot")

    # <<< One-shot reconcile from exchange >>>
    reconcile_from_exchange(strats, broker)

    while True:
        try:
            # === 1) Minute tick ===
            sleep_to_next_minute(offset_sec=0.8)
            if halted():
                logging.warning("[HALT] halt.flag detected. sleep 5s")
                time.sleep(5)
                continue

            # Fetch 1m once per symbol, resample to N, handle 5m close, then (once per minute) evaluate armed trigger
            for s in TICKERS:
                try:
                    if CB[s].blocked(): continue
                    df1 = feeds[s].fetch_1m_df(count=200)
                    if df1 is None or df1.empty: continue

                    tf = int(strats[s].p["tf_base"])
                    dfN = resample_right_closed_ohlc(df1, tf)
                    if dfN is not None and not dfN.empty:
                        cur_idx = dfN.index[-1]
                        if strats[s].last_tf_idx is None or cur_idx != strats[s].last_tf_idx:
                            # New N-min closed candle: process GATE first
                            strats[s].on_bar_close_tf(dfN, CB[s])
                            strats[s].last_tf_idx = cur_idx
                            CB[s].ok()

                    # Armed trigger: evaluate ONCE per minute with the same closed 1m data
                    if strats[s].state.is_armed:
                        strats[s].check_armed_trigger_1m(df1, CB[s])

                except Exception as e:
                    CB[s].fail("slow")
                    logging.error(f"[{s}] slow loop error: {e}")
                    logging.error(traceback.format_exc())

            # === 2) Fast loop until next minute: only manage positions (no 1m OHLCV calls) ===
            deadline = (int(time.time() // 60) + 1) * 60 - 0.2
            while time.time() < deadline:
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
                # heartbeat & wait
                heartbeat()
                time.sleep(POLL_SEC)

        except KeyboardInterrupt:
            logging.warning("[main] KeyboardInterrupt -> exit")
            break
        except Exception as e:
            logging.error(f"[main] fatal error: {e}")
            logging.error(traceback.format_exc())
            time.sleep(2)

if __name__ == "__main__":
    main()
