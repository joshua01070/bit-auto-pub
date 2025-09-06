#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
multi_coin_auto_trade_live_final_v2.py (LIVE only, no dry-run, multi-symbol)

[V2] Stability & Simplicity:
 - Slow Loop (per-minute) + Fast Loop (per-second) to reduce API pressure.
 - SELL fill is reconciled via get_order(trades/executed_volume) before state update.
 - Simple-Dust: minKRW(6,000원) 미만은 매도 시도 스킵 → 다음 거래에서 합쳐서 매도.
 - dust→신규진입 시 새 사이클 리셋(PTP 기록/TP1/앵커/스탑).
 - Watchdog heartbeat includes positions.
 - Upbit timeouts patched; safe_call(retries/backoff).
"""

import os, sys, time, json, math, logging, threading
from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple, Dict, Any

import pandas as pd
import pyupbit

# =========================
# 0) 실행 가드 & 로깅
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

if os.getenv("UPBIT_LIVE") != "1":
    raise SystemExit("Refuse to run LIVE. Set environment UPBIT_LIVE=1 to confirm.")

# =========================
# 1) 설정
# =========================
ACCESS = ""  # <<< PUT YOUR ACCESS KEY
SECRET = ""  # <<< PUT YOUR SECRET KEY

TICKERS = ["KRW-BTC", "KRW-ETH"]

DEFAULTS = dict(
    use_ma = 1, ma_s = 10, ma_l = 30,
    don_n = 72,
    fee = 0.0007,
    min_krw_order = 6000,
    qty_step = 1e-8,
    buy_pct = 0.30,
    max_position_krw = None,
    trail_before = 0.075,
    trail_after  = 0.075,
    ptp_levels = "",
    tp_rate = 0.0,
    eps_exec = 0.0003,
    entry_tick_buffer = 2,
    fast_seq = "HL",
    dust_margin = 0.98,
)

SYMBOL_PARAMS: Dict[str, Dict[str, Any]] = {
    "KRW-BTC": {
        "use_ma": 1, "ma_s": 10, "ma_l": 60,
        "don_n": 72,
        "fee": 0.0005,
        "buy_pct": 0.40,
        "trail_before": 0.08, "trail_after": 0.08,
        "ptp_levels": "",
        "tp_rate": 0.035,
        "entry_tick_buffer": 2,
        "fast_seq": "HL",
    },
    "KRW-ETH": {
        "use_ma": 1, "ma_s": 10, "ma_l": 30,
        "don_n": 72,
        "fee": 0.0005,
        "buy_pct": 0.30,
        "trail_before": 0.075, "trail_after": 0.075,
        "ptp_levels": "0.015:0.4,0.035:0.3,0.06:0.3",
        "tp_rate": 0.03,
        "entry_tick_buffer": 2,
        "fast_seq": "HL",
    },
}

POLL_SEC = 1
STATE_DIR = "state_live"
HALT_FILE = "halt.flag"

HEARTBEAT_SEC = 60
HB_WATCHDOG_SEC = 20

# =========================
# 2) 유틸 & 공통
# =========================
def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def sym_to_ccy(symbol: str) -> str:
    return symbol.split("-")[1]

def sleep_to_next_minute(offset_sec: float = 0.0):
    now = time.time()
    next_min = (int(now // 60) + 1) * 60 + offset_sec
    time.sleep(max(0.0, next_min - now))

def tick_size_for(krw_price: float) -> float:
    p = krw_price
    if p >= 2_000_000: return 1000
    if p >= 1_000_000: return 500
    if p >=   500_000: return 100
    if p >=   100_000: return 50
    if p >=    10_000: return 10
    if p >=     1_000: return 5
    if p >=       100: return 0.1
    if p >=        10: return 0.01
    return 0.001

def round_to_tick(price: float) -> float:
    t = tick_size_for(price)
    return math.floor(price / t) * t

def floor_to_step(x: float, step: float) -> float:
    return math.floor(x / step) * step

def be_stop(avg: float, fee: float, cushion: float = 0.0003) -> float:
    return avg * (1.0 + fee + cushion)

def parse_ptp_levels(spec: str) -> List[Tuple[float, float]]:
    out = []
    for part in spec.split(","):
        part = part.strip()
        if not part: continue
        lv, pct = part.split(":")
        out.append((float(lv), float(pct)))
    return out

def halted() -> bool:
    return os.path.exists(HALT_FILE)

def safe_call(fn, *args, tries: int = 3, delay: float = 0.4, backoff: float = 1.7, **kwargs):
    t = delay
    for i in range(tries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if i == tries - 1:
                logging.error(f"[safe_call] {fn.__name__} failed after {tries} tries: {e}")
                raise
            logging.warning(f"[safe_call] {fn.__name__} error ({i+1}/{tries}): {e}. retry in {t:.2f}s")
            time.sleep(t)
            t *= backoff

def patch_pyupbit_timeout():
    try:
        import pyupbit.request_api as _ra
        _orig_pub = _ra._call_public_api
        def _pub(url, **kw):
            kw.setdefault("timeout", 4)
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

def normalize_orderbook(resp: Any, symbol: str) -> Tuple[float, float]:
    if resp is None:
        raise RuntimeError("orderbook None")
    data = resp
    if isinstance(data, list):
        item = data[0] if data else None
    elif isinstance(data, dict):
        item = data
    else:
        raise RuntimeError("orderbook unknown type")
    if not item:
        raise RuntimeError("orderbook empty item")
    if "orderbook_units" in item:
        units = item["orderbook_units"]
        if not units: raise RuntimeError("orderbook units empty")
        bid = float(units[0]["bid_price"])
        ask = float(units[0]["ask_price"])
        return bid, ask
    for k in ("bid_price", "best_bid", "bids"):
        if k in item:
            bid = float(item[k]) if isinstance(item[k], (int,float,str)) else float(item[k][0])
            break
    else:
        raise RuntimeError("orderbook no bid_price")
    for k in ("ask_price", "best_ask", "asks"):
        if k in item:
            ask = float(item[k]) if isinstance(item[k], (int,float,str)) else float(item[k][0])
            break
    else:
        raise RuntimeError("orderbook no ask_price")
    return bid, ask

# =========================
# 3) 브로커/피드
# =========================
class UpbitBroker:
    def __init__(self, access: str, secret: str):
        self.upbit = pyupbit.Upbit(access, secret)
    def best_bid_ask(self, symbol: str) -> Tuple[float, float]:
        ob = safe_call(pyupbit.get_orderbook, symbol)
        return normalize_orderbook(ob, symbol)
    def balances(self) -> List[dict]:
        return safe_call(self.upbit.get_balances) or []
    def krw_balance(self) -> float:
        for b in self.balances():
            if b.get("currency") == "KRW":
                return float(b.get("balance") or 0.0)
        return 0.0
    def base_position(self, symbol: str) -> Tuple[float, float]:
        base = sym_to_ccy(symbol)
        for b in self.balances():
            if b.get("currency") == base:
                qty = float(b.get("balance") or 0.0)
                avg = float(b.get("avg_buy_price") or 0.0)
                return qty, avg
        return 0.0, 0.0
    def buy_market_krw(self, symbol: str, krw: float) -> dict:
        return safe_call(self.upbit.buy_market_order, symbol, krw)
    def sell_market_qty(self, symbol: str, qty: float) -> dict:
        return safe_call(self.upbit.sell_market_order, symbol, qty)
    def get_order(self, uuid: str) -> dict:
        return safe_call(self.upbit.get_order, uuid)

class UpbitFeed:
    def __init__(self, symbol: str):
        self.symbol = symbol
    def fetch_1m_df(self, count: int = 200) -> pd.DataFrame:  # reduced: 200 is enough (72 Don + 60MA + margin)
        df = safe_call(pyupbit.get_ohlcv, self.symbol, interval="minute1", count=count)
        if df is None or df.empty:
            raise RuntimeError(f"failed to fetch 1m ohlcv: {self.symbol}")
        return df
    @staticmethod
    def aggregate_5m_closed(df1m: pd.DataFrame) -> pd.DataFrame:
        df5 = df1m.resample("5min").agg({
            "open":"first","high":"max","low":"min","close":"last","volume":"sum"
        })
        df5 = df5.dropna()
        if len(df5) >= 1:
            df5 = df5.iloc[:-1]  # closed only
        return df5

# =========================
# 4) 상태
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
    def to_json(self) -> dict:
        d = asdict(self); d["ptp_hits"] = self.ptp_hits or []; return d
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
        return st

# =========================
# 5) 전략
# =========================
class SymbolStrategy:
    def __init__(self, symbol: str, broker: UpbitBroker, params: Dict[str, Any]):
        self.symbol = symbol
        self.broker = broker
        self.p = {**DEFAULTS, **(params or {})}
        self.state_path = os.path.join(STATE_DIR, f"{self.symbol.replace('-','_')}.json")
        self.state = self._load_state()
        self.ptp_levels = parse_ptp_levels(self.p["ptp_levels"])

    # persistence
    def _load_state(self) -> PositionState:
        ensure_dir(STATE_DIR)
        if os.path.exists(self.state_path):
            try:
                with open(self.state_path, "r", encoding="utf-8") as f:
                    return PositionState.from_json(json.load(f))
            except Exception:
                logging.exception(f"[{self.symbol}] state load failed; starting fresh")
        return PositionState(has_pos=False, ptp_hits=[])
    def _save_state(self):
        with open(self.state_path, "w", encoding="utf-8") as f:
            json.dump(self.state.to_json(), f, ensure_ascii=False)

    # indicators
    @staticmethod
    def sma(series: pd.Series, n: int) -> Optional[float]:
        if len(series) < n: return None
        return float(series.iloc[-n:].mean())

    # dust判定 (Simple-Dust용 ENTRY-UNLOCK)
    def _is_dust_with_price(self, px: float) -> bool:
        if not self.state.has_pos: return False
        return (self.state.qty * px) < (self.p["min_krw_order"] * float(self.p.get("dust_margin", 0.98)))

    # ---- SELL helpers (Simple-Dust & fill reconciliation) ----
    def _try_sell_qty(self, qty: float, bid: float, force: bool = False) -> Optional[str]:
        """Submit sell and return uuid if submitted. None means skipped/not submitted."""
        qty_to_sell = floor_to_step(qty, self.p["qty_step"])
        if qty_to_sell <= 0:
            return None
        if (not force) and (bid * qty_to_sell) < self.p["min_krw_order"]:
            return None  # simple-dust: skip quietly
        try:
            od = self.broker.sell_market_qty(self.symbol, qty_to_sell)
            uuid = od.get("uuid") if isinstance(od, dict) else None
            logging.info(f"[{self.symbol}] [SELL] qty={qty_to_sell:.8f} submitted. uuid={uuid}")
            return uuid
        except Exception:
            logging.exception(f"[{self.symbol}] [SELL] submission failed")
            return None

    def _get_filled_qty_from_trades(self, uuid: str) -> float:
        """Poll get_order and return actual filled volume. Uses trades or executed_volume fallback."""
        filled_qty = 0.0
        if not uuid:
            return 0.0
        deadline = time.time() + 4.0
        while time.time() < deadline:
            try:
                od = self.broker.get_order(uuid)
                trades = od.get("trades") or []
                vol_sum = 0.0
                for tr in trades:
                    v = float(tr.get("volume") or tr.get("qty") or 0.0)
                    vol_sum += v
                if vol_sum > 0:
                    filled_qty = vol_sum
                    break
                # fallback: executed_volume may be available
                ev = od.get("executed_volume")
                if ev is not None:
                    evf = float(ev)
                    if evf > 0:
                        filled_qty = evf
                        break
                st = od.get("state")
                if st in ("done", "cancel"):
                    break
            except Exception as e:
                logging.warning(f"[{self.symbol}] [FILL] get_order error: {e}")
            time.sleep(0.5)
        return filled_qty

    def _resolve_sell_fill(self, uuid: str):
        """After sell submit, reconcile fill and re-sync state/stop/anchor safely."""
        if not uuid:
            return
        filled_qty = self._get_filled_qty_from_trades(uuid)
        if filled_qty > 0:
            logging.info(f"[{self.symbol}] [FILL] sell resolved: sold_qty={filled_qty:.8f}")
            # adjust local qty then re-sync from exchange to re-evaluate stop/anchor (no-loosen)
            self.state.qty = max(0.0, self.state.qty - filled_qty)
            self._refresh_position_from_exchange()
            if self.state.qty <= self.p["qty_step"]:
                logging.info(f"[{self.symbol}] [POS] position is now flat.")
                self.state = PositionState(has_pos=False, ptp_hits=[])
                self._save_state()
        else:
            logging.warning(f"[{self.symbol}] [FILL] unresolved sell fill. Forcing re-sync.")
            self._refresh_position_from_exchange()

    # ---- EXCHANGE SYNC ----
    def _refresh_position_from_exchange(self):
        qty, avg = self.broker.base_position(self.symbol)
        if qty <= self.p["qty_step"]:
            if self.state.has_pos:
                logging.info(f"[{self.symbol}] [POS] exchange shows flat; clearing local state")
            self.state = PositionState(has_pos=False, ptp_hits=[])
            self._save_state()
            return
        self.state.has_pos = True
        self.state.qty = qty
        self.state.avg = avg
        if self.state.init_qty <= 0:
            self.state.init_qty = qty
        step = self.p["trail_after"] if self.state.tp1_done else self.p["trail_before"]
        if self.state.ts_anchor <= 0:
            self.state.ts_anchor = self.state.avg
        trail_stop = round_to_tick(self.state.ts_anchor * (1.0 - step))
        new_stop = trail_stop
        if self.state.tp1_done:
            be = be_stop(self.state.avg, self.p["fee"], self.p["eps_exec"])
            new_stop = max(new_stop, be)
        if self.state.stop <= 0 or new_stop > self.state.stop:
            self.state.stop = new_stop
        if self.state.ptp_hits is None:
            self.state.ptp_hits = []
        self._save_state()

    # ---- BUY path ----
    def on_bar_close_5m(self, df5: pd.DataFrame, budget_left: float) -> float:
        closes = df5["close"]; highs = df5["high"]
        n = int(self.p["don_n"])
        if len(highs) < n + 1:
            return budget_left
        upper = float(highs.iloc[-(n+1):-1].max())
        prev_close = float(closes.iloc[-1])
        if self.p["use_ma"]:
            ms = self.sma(closes, int(self.p["ma_s"]))
            ml = self.sma(closes, int(self.p["ma_l"]))
            if ms is None or ml is None or ms <= ml:
                return budget_left
        tick = tick_size_for(upper)
        up_buf = upper + tick * int(self.p.get("entry_tick_buffer", 2))
        if ((not self.state.has_pos) or self._is_dust_with_price(prev_close)) and (not halted()) and (prev_close > up_buf):
            is_new_cycle = self.state.has_pos and self._is_dust_with_price(prev_close)
            budget_left = self._enter_position(budget_left, new_cycle=is_new_cycle)
        return budget_left

    def _enter_position(self, budget_left: float, new_cycle: bool = False) -> float:
        # PATCH: < (not <=) to allow exact 6000 KRW cases
        if budget_left < self.p["min_krw_order"]:
            return budget_left
        krw_alloc = budget_left * float(self.p["buy_pct"])
        if self.p["max_position_krw"] is not None:
            krw_alloc = min(krw_alloc, self.p["max_position_krw"])
        krw_alloc = math.floor(krw_alloc)
        if krw_alloc < self.p["min_krw_order"]:
            return budget_left
        try:
            od = self.broker.buy_market_krw(self.symbol, krw_alloc)
            uuid = od.get("uuid") if isinstance(od, dict) else None
            logging.info(f"[{self.symbol}] [BUY] krw={krw_alloc} uuid={uuid}")
            time.sleep(1.0)  # propagation
            self._resolve_buy_fill(uuid, new_cycle=new_cycle)
            return max(0.0, budget_left - krw_alloc)
        except Exception:
            logging.exception(f"[{self.symbol}] [BUY] failed")
            return budget_left

    def _resolve_buy_fill(self, uuid: Optional[str], new_cycle: bool = False):
        # wait briefly via get_order then re-sync from exchange
        self._get_filled_qty_from_trades(uuid or "")
        self._refresh_position_from_exchange()
        if new_cycle and self.state.has_pos and self.state.qty > 0:
            self.state.ptp_hits = []
            self.state.tp1_done = False
            self.state.ts_anchor = 0.0
            self.state.stop = 0.0
            logging.info(f"[{self.symbol}] [NEW CYCLE] reset flags due to dust→entry")
        self.state.init_qty = self.state.qty
        self._save_state()

    def _after_tp1_bump_be(self):
        be = be_stop(self.state.avg, self.p["fee"], self.p["eps_exec"])
        if be > self.state.stop:
            self.state.stop = be
            logging.info(f"[{self.symbol}] [BE BUMP] stop -> {round_to_tick(self.state.stop):.0f}")
            self._save_state()

    def manage_intrabar(self, bid: float, ask: float):
        if not self.state.has_pos:
            return
        step = self.p["trail_after"] if self.state.tp1_done else self.p["trail_before"]
        if ask > self.state.ts_anchor:
            self.state.ts_anchor = ask
            new_stop = round_to_tick(self.state.ts_anchor * (1.0 - step))
            if new_stop > self.state.stop:
                self.state.stop = new_stop
                logging.info(f"[{self.symbol}] [TRAIL UP] anchor={self.state.ts_anchor:.0f} stop={self.state.stop:.0f}")
                self._save_state()

        def do_ts():
            if bid <= self.state.stop:
                uuid = self._try_sell_qty(self.state.qty, bid, force=False)  # Simple-Dust: force=False
                if uuid:
                    logging.info(f"[{self.symbol}] [TS EXIT] triggered. Resolving fill...")
                    self._resolve_sell_fill(uuid)
                    return True
            return False

        def do_ptp_and_tp():
            # PTP
            if self.ptp_levels and self.state.qty > 0:
                base = self.state.init_qty if self.state.init_qty > 0 else self.state.qty
                for i, (lv, pct) in enumerate(self.ptp_levels):
                    if self.state.ptp_hits and (lv in self.state.ptp_hits):
                        continue
                    if bid >= self.state.avg * (1.0 + lv):
                        is_last = (i == len(self.ptp_levels) - 1)
                        qty_part = self.state.qty if is_last else base * pct
                        qty_part = min(qty_part, self.state.qty)
                        uuid = self._try_sell_qty(qty_part, bid, force=False)
                        if uuid:
                            logging.info(f"[{self.symbol}] [PTP EXIT] lv={lv} triggered. Resolving...")
                            if self.state.ptp_hits is None: self.state.ptp_hits = []
                            if lv not in self.state.ptp_hits: self.state.ptp_hits.append(lv)
                            if not self.state.tp1_done:
                                self.state.tp1_done = True
                                self._after_tp1_bump_be()
                            self._save_state()
                            self._resolve_sell_fill(uuid)
                            return True
            # TP
            tp_rate = float(self.p.get("tp_rate") or 0.0)
            if self.state.qty > 0 and tp_rate > 0.0 and bid >= self.state.avg * (1.0 + tp_rate):
                uuid = self._try_sell_qty(self.state.qty, bid, force=False)
                if uuid:
                    logging.info(f"[{self.symbol}] [TP EXIT] rate={tp_rate:.4f} triggered. Resolving...")
                    self._resolve_sell_fill(uuid)
                    return True
            return False

        if (self.p.get("fast_seq", "HL").upper() == "HL"):
            if do_ptp_and_tp(): return
            if do_ts(): return
        else:
            if do_ts(): return
            if do_ptp_and_tp(): return

# =========================
# 6) 하트비트/메인 루프
# =========================
def start_watchdog(strats: Dict[str, SymbolStrategy]):
    def _run():
        while True:
            try:
                lines = []
                for s, stg in strats.items():
                    st = stg.state
                    if st.has_pos:
                        lines.append(f"{s}: pos={st.qty:.6f}@{st.avg:.0f} stop={st.stop:.0f}")
                if lines:
                    logging.info("[hb/watchdog] " + " | ".join(lines))
                else:
                    logging.info("[hb/watchdog] alive, no positions.")
            except Exception:
                logging.exception("[hb/watchdog] error")
            time.sleep(HB_WATCHDOG_SEC)
    t = threading.Thread(target=_run, daemon=True); t.start()

def main():
    if not ACCESS or not SECRET:
        raise SystemExit("ACCESS/SECRET required for LIVE trading.")
    logging.info("=== LIVE MODE: Upbit REAL trading started (MULTI-V2) ===")
    logging.info(f"Tickers={TICKERS}")

    broker = UpbitBroker(ACCESS, SECRET)
    try:
        _ = broker.krw_balance()
        logging.info(f"[boot] KRW balance check OK: {broker.krw_balance():.0f}")
    except Exception:
        logging.exception("boot check failed"); raise SystemExit(1)

    feeds = {s: UpbitFeed(s) for s in TICKERS}
    strats = {s: SymbolStrategy(s, broker, SYMBOL_PARAMS.get(s, {})) for s in TICKERS}
    for s in TICKERS:
        strats[s]._refresh_position_from_exchange()

    start_watchdog(strats)
    hb_t0 = time.time()
    last5_idx = {s: None for s in TICKERS}

    while True:
        try:
            # === 1) Slow loop: once per minute ===
            sleep_to_next_minute(offset_sec=1.0)
            budget_left = broker.krw_balance()

            for s in TICKERS:
                try:
                    df1 = feeds[s].fetch_1m_df(count=200)  # reduced call size
                    df5 = UpbitFeed.aggregate_5m_closed(df1)
                    if not df5.empty:
                        cur_5m_index = df5.index[-1]
                        if last5_idx.get(s) is None or cur_5m_index != last5_idx.get(s):
                            budget_left = strats[s].on_bar_close_5m(df5, budget_left)
                            last5_idx[s] = cur_5m_index
                except Exception:
                    logging.exception(f"[{s}] 5m signal processing error")

            # === 2) Fast loop: ~per second until next minute ===
            fast_loop_deadline = (int(time.time() // 60) + 1) * 60 - 1
            while time.time() < fast_loop_deadline:
                if halted():
                    logging.warning("Halt file detected. Skipping intrabar management...")
                    time.sleep(5)
                    continue
                for s in TICKERS:
                    if strats[s].state.has_pos:
                        try:
                            bid, ask = broker.best_bid_ask(s)
                            strats[s].manage_intrabar(bid, ask)
                        except Exception:
                            logging.exception(f"[{s}] intrabar management error")

                if time.time() - hb_t0 >= HEARTBEAT_SEC:
                    lines = []
                    for s, stg in strats.items():
                        st = stg.state
                        if st.has_pos:
                            lines.append(f"{s}: pos={st.qty:.6f}@{st.avg:.0f} stop={st.stop:.0f} tp1={st.tp1_done}")
                    logging.info("[hb] " + (" | ".join(lines) if lines else "No open positions."))
                    hb_t0 = time.time()

                time.sleep(POLL_SEC)

        except KeyboardInterrupt:
            logging.info("Interrupted by user, shutting down.")
            break
        except Exception:
            logging.exception("Critical error in main loop")
            time.sleep(5)

if __name__ == "__main__":
    main()
