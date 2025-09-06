#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
multi_coin_auto_trade_live_final_v2_hardened.py

LIVE only, no dry-run, multi-symbol — hardened edition.

Hardening applied (7,9,10 excluded as per request):
 1) Atomic state save + schema version
 2) Circuit breaker per symbol (API error cooldown)
 3) Per-TF-bar entry lock (prevent duplicate entries on same bar)
 4) KRW balance recheck just before buy (reduce cross-symbol race)
 5) Market-wide drop guard (block NEW entries only)
 6) Generic N-min resample + 1m backfill cache (Upbit 200-bars limit workaround)
 8) Non-loosening stop invariant (trail/BE never moves down)

Other changes:
 - tf_base honored per symbol (e.g., BTC/ETH tf=5, SOL tf=10)
 - fee default set to 0.0005
 - Current approved parameters embedded (BTC/ETH/SOL)
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

# 돌릴 심볼들 — 필요 없으면 주석 처리
TICKERS = ["KRW-BTC", "KRW-ETH", "KRW-SOL"]

STATE_SCHEMA_VER = 1

DEFAULTS = dict(
    tf_base = 5,              # 기본 TF (분)
    use_ma = 1, ma_s = 10, ma_l = 30,
    don_n = 72,
    fee = 0.0005,
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
    # BTC — 확정 채택안 (tf=5)
    "KRW-BTC": {
        "tf_base": 5,
        "use_ma": 1, "ma_s": 8, "ma_l": 30,
        "don_n": 88,
        "fee": 0.0005,
        "buy_pct": 0.35,
        "trail_before": 0.06, "trail_after": 0.06,
        "ptp_levels": "0.015:0.6",
        "tp_rate": 0.025,
        "entry_tick_buffer": 2,
        "fast_seq": "LH",
    },
    # ETH — 확정 채택안 (tf=5)
    "KRW-ETH": {
        "tf_base": 5,
        "use_ma": 1, "ma_s": 10, "ma_l": 72,
        "don_n": 64,
        "fee": 0.0005,
        "buy_pct": 0.30,
        "trail_before": 0.075, "trail_after": 0.075,
        "ptp_levels": "0.015:0.6",
        "tp_rate": 0.029,
        "entry_tick_buffer": 2,
        "fast_seq": "LH",
    },
    # SOL — 확정 채택안 (tf=10)
    "KRW-SOL": {
        "tf_base": 10,
        "use_ma": 1, "ma_s": 8, "ma_l": 75,
        "don_n": 42,
        "fee": 0.0005,
        "buy_pct": 0.25,
        "trail_before": 0.096, "trail_after": 0.096,
        "ptp_levels": "0.02:0.5",
        "tp_rate": 0.040,
        "entry_tick_buffer": 2,
        "fast_seq": "LH",
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
        bid = float(units[0]["bid_price"])  # best bid
        ask = float(units[0]["ask_price"])  # best ask
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
# 2.5) Circuit Breaker
# =========================
class Circuit:
    def __init__(self, max_fail=5, cool=30):
        self.max_fail = max_fail
        self.cool = cool
        self.fail = 0
        self.until = 0.0
    def bad(self):
        self.fail += 1
        if self.fail >= self.max_fail:
            self.until = time.time() + self.cool
    def ok(self):
        self.fail = 0
    def blocked(self) -> bool:
        return time.time() < self.until

CB: Dict[str, Circuit] = {}

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
        self._cache_1m: Optional[pd.DataFrame] = None  # 1분봉 캐시 (백필)

    def fetch_1m_df(self, count: int = 200) -> pd.DataFrame:
        df = safe_call(pyupbit.get_ohlcv, self.symbol, interval="minute1", count=count)
        if df is None or df.empty:
            raise RuntimeError(f"failed to fetch 1m ohlcv: {self.symbol}")
        return df

    def fetch_1m_needed(self, need_minutes: int, backfill_limit: int = 3) -> pd.DataFrame:
        """
        최소 need_minutes 이상을 보장하도록 1분봉을 캐시에 누적.
        Upbit 단건 200개 제한을 우회하기 위해 'to' 파라미터로 과거 페이지 최대 backfill_limit회 요청.
        """
        def _pull(to_ts: Optional[pd.Timestamp] = None) -> pd.DataFrame:
            kw = {"interval": "minute1", "count": 200}
            if to_ts is not None:
                # pyupbit는 KST 문자열을 기대(naive도 허용).
                kw["to"] = to_ts.strftime("%Y-%m-%d %H:%M:%S")
            return safe_call(pyupbit.get_ohlcv, self.symbol, **kw)

        if self._cache_1m is None or self._cache_1m.empty:
            self._cache_1m = _pull()

        # 최신 페이지로 갱신 (중복 제거)
        latest = _pull()
        if latest is not None and not latest.empty:
            self._cache_1m = pd.concat([self._cache_1m, latest]).sort_index().drop_duplicates()

        # 부족하면 과거로 백필
        tries = 0
        while len(self._cache_1m) < need_minutes and tries < backfill_limit:
            oldest = self._cache_1m.index[0]
            older = _pull(oldest)
            if older is None or older.empty:
                break
            self._cache_1m = pd.concat([older, self._cache_1m]).sort_index().drop_duplicates()
            tries += 1
            time.sleep(0.25)

        if len(self._cache_1m) > need_minutes:
            return self._cache_1m.iloc[-need_minutes:].copy()
        return self._cache_1m.copy()

    @staticmethod
    def aggregate_nmin_closed(df1m: pd.DataFrame, n_minutes: int) -> pd.DataFrame:
        rule = f"{int(n_minutes)}min"
        dfN = df1m.resample(rule).agg({
            "open":"first","high":"max","low":"min","close":"last","volume":"sum"
        })
        dfN = dfN.dropna()
        if len(dfN) >= 1:
            dfN = dfN.iloc[:-1]  # 진행중 봉 제외
        return dfN

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
        d = asdict(self); d["ptp_hits"] = self.ptp_hits or []; d["_ver"] = STATE_SCHEMA_VER; return d
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
        self.enter_lock_until = 0.0  # 중복 진입락
        self.block_entry = False     # 시장 급락 가드

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
        tmp = self.state_path + ".tmp"
        data = self.state.to_json()
        data["_ver"] = STATE_SCHEMA_VER
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp, self.state_path)  # atomic replace

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
        if not uuid:
            return
        filled_qty = self._get_filled_qty_from_trades(uuid)
        if filled_qty > 0:
            logging.info(f"[{self.symbol}] [FILL] sell resolved: sold_qty={filled_qty:.8f}")
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
        # 절대 비이완
        if self.state.stop > 0:
            new_stop = max(new_stop, self.state.stop)
        self.state.stop = new_stop
        if self.state.ptp_hits is None:
            self.state.ptp_hits = []
        self._save_state()

    # ---- BUY path ----
    def on_bar_close_tf(self, dfN: pd.DataFrame, budget_left: float) -> float:
        closes = dfN["close"]; highs = dfN["high"]
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

        # 진입 조건 + dust 진입 리셋
        if ((not self.state.has_pos) or self._is_dust_with_price(prev_close)) and (not halted()) and (prev_close > up_buf):
            is_new_cycle = self.state.has_pos and self._is_dust_with_price(prev_close)
            budget_left = self._enter_position(budget_left, new_cycle=is_new_cycle)
        return budget_left

    def _enter_position(self, budget_left: float, new_cycle: bool = False) -> float:
        # 중복 진입락
        if time.time() < self.enter_lock_until:
            return budget_left
        # 시장 급락 가드 (신규 진입 차단)
        if getattr(self, "block_entry", False):
            return budget_left
        # 최소 주문 금액
        if budget_left < self.p["min_krw_order"]:
            return budget_left

        # 예산 산출
        krw_alloc = budget_left * float(self.p["buy_pct"])
        if self.p["max_position_krw"] is not None:
            krw_alloc = min(krw_alloc, self.p["max_position_krw"])
        krw_alloc = math.floor(krw_alloc)
        if krw_alloc < self.p["min_krw_order"]:
            return budget_left

        # 주문 직전 KRW 재검증(교차 경합 방지)
        try:
            actual = self.broker.krw_balance()
        except Exception:
            logging.exception(f"[{self.symbol}] KRW balance recheck failed")
            return budget_left
        if actual < self.p["min_krw_order"]:
            return budget_left
        if actual < krw_alloc * 0.98:
            krw_alloc = min(math.floor(actual), krw_alloc)
            if krw_alloc < self.p["min_krw_order"]:
                return budget_left

        try:
            od = self.broker.buy_market_krw(self.symbol, krw_alloc)
            uuid = od.get("uuid") if isinstance(od, dict) else None
            logging.info(f"[{self.symbol}] [BUY] krw={krw_alloc} uuid={uuid}")
            time.sleep(1.0)  # propagation
            self._resolve_buy_fill(uuid, new_cycle=new_cycle)
            # 엔트리 락: 현재 TF의 남은 시간 동안 재진입 금지(느슨하게 tf*60-5)
            tf = int(self.p.get("tf_base", 5))
            self.enter_lock_until = time.time() + max(5, tf*60 - 5)
            return max(0.0, budget_left - krw_alloc)
        except Exception:
            logging.exception(f"[{self.symbol}] [BUY] failed")
            return budget_left

    def _resolve_buy_fill(self, uuid: Optional[str], new_cycle: bool = False):
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
            # 절대 비이완
            new_stop = max(new_stop, self.state.stop)
            if new_stop > self.state.stop:
                self.state.stop = new_stop
                logging.info(f"[{self.symbol}] [TRAIL UP] anchor={self.state.ts_anchor:.0f} stop={self.state.stop:.0f}")
                self._save_state()

        def do_ts():
            if bid <= self.state.stop:
                uuid = self._try_sell_qty(self.state.qty, bid, force=False)
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
            # TP (잔량)
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
# 6) 하트비트/메인 루프 + 가드
# =========================

def market_drop_guard(btc_symbol: str = "KRW-BTC", look_min: int = 10, thresh: float = -0.025) -> bool:
    try:
        df = safe_call(pyupbit.get_ohlcv, btc_symbol, interval="minute1", count=look_min+1)
        if df is None or len(df) < 2:
            return False
        ret = float(df['close'].iloc[-1] / df['close'].iloc[-(look_min+1)] - 1.0)
        return ret <= thresh
    except Exception as e:
        logging.warning(f"[guard] market drop guard error: {e}")
        return False

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
    logging.info("=== LIVE MODE: Upbit REAL trading started (MULTI-V2 Hardened) ===")
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

    # Circuit breakers init
    CB.update({s: Circuit(max_fail=5, cool=30) for s in TICKERS})

    start_watchdog(strats)
    hb_t0 = time.time()
    last_tf_idx: Dict[str, Optional[pd.Timestamp]] = {s: None for s in TICKERS}

    while True:
        try:
            # === Global market guard (once per minute) ===
            global_block = market_drop_guard()
            for s in TICKERS:
                strats[s].block_entry = bool(global_block)
            if global_block:
                logging.warning("[guard] market-wide drop; blocking NEW entries (exits allowed).")

            # === 1) Slow loop: once per minute ===
            sleep_to_next_minute(offset_sec=1.0)
            budget_left = broker.krw_balance()

            for s in TICKERS:
                try:
                    if CB[s].blocked():
                        logging.warning(f"[{s}] circuit-open (cooldown); skip minute cycle")
                        continue
                    p = strats[s].p
                    tf = int(p.get("tf_base", 5))
                    need_N = max(int(p.get("don_n", 72)), int(p.get("ma_s", 10)), int(p.get("ma_l", 30))) + 6
                    need_1m = tf * need_N
                    df1 = feeds[s].fetch_1m_needed(need_minutes=need_1m, backfill_limit=3)
                    dfN = UpbitFeed.aggregate_nmin_closed(df1, tf)
                    if not dfN.empty:
                        cur_tf_index = dfN.index[-1]
                        if last_tf_idx.get(s) is None or cur_tf_index != last_tf_idx.get(s):
                            budget_left = strats[s].on_bar_close_tf(dfN, budget_left)
                            last_tf_idx[s] = cur_tf_index
                    else:
                        logging.warning(f"[{s}] insufficient history: need~{need_1m}m, have={len(df1)}m")
                    CB[s].ok()
                except Exception:
                    logging.exception(f"[{s}] TF signal processing error")
                    CB[s].bad()

            # === 2) Fast loop: ~per second until next minute ===
            fast_loop_deadline = (int(time.time() // 60) + 1) * 60 - 1
            while time.time() < fast_loop_deadline:
                if halted():
                    logging.warning("Halt file detected. Skipping intrabar management...")
                    time.sleep(5)
                    continue
                for s in TICKERS:
                    try:
                        if strats[s].state.has_pos:
                            if CB[s].blocked():
                                continue
                            bid, ask = broker.best_bid_ask(s)
                            strats[s].manage_intrabar(bid, ask)
                            CB[s].ok()
                    except Exception:
                        logging.exception(f"[{s}] intrabar management error")
                        CB[s].bad()

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
