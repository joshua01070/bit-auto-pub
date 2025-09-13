import os, time, json, math, logging, traceback, uuid, threading
from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple, Dict, Any
from concurrent.futures import ThreadPoolExecutor
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
#하드코딩된 API 키 (여기에 직접 입력)
ACCESS = ""
SECRET = ""

TICKERS = ["KRW-BTC", "KRW-ETH", "KRW-SOL", "KRW-XRP"]
STATE_SCHEMA_VER = 5  # (+pending_buy_uuid, +buy_guard_until)

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
    "KRW-BTC": {
        "tf_base": 5, "use_ma": 1, "ma_s": 8, "ma_l": 60, "don_n": 64,
        "buy_pct": 0.40, "trail_before": 0.08, "trail_after": 0.08,
        "ptp_levels": "0.035:0.7", "tp_rate": 0.055, "fast_seq": "HL",
        "armed_enable": 0, "entry_tick_buffer": 1
    },
    "KRW-ETH": {
        "tf_base": 5, "use_ma": 1, "ma_s": 10, "ma_l": 60, "don_n": 48,
        "buy_pct": 0.35, "trail_before": 0.08, "trail_after": 0.08,
        "ptp_levels": "0.03:0.7", "tp_rate": 0.055, "fast_seq": "HL",
        "armed_enable": 0, "entry_tick_buffer": 1
    },
    "KRW-SOL": {
        "tf_base": 5, "use_ma": 1, "ma_s": 8, "ma_l": 45, "don_n": 96,
        "buy_pct": 0.35, "trail_before": 0.08, "trail_after": 0.08,
        "ptp_levels": "0.03:0.3", "tp_rate": 0.045, "fast_seq": "HL",
        "armed_enable": 1, "armed_micro_trigger": "MA",
        "armed_ma_s_1m": 5, "armed_ma_l_1m": 30,
        "armed_inval_pct": 0.003, "armed_reds": 3, "armed_window_min": 5,
        "entry_tick_buffer": 1
    },
    "KRW-XRP": {
        "tf_base": 5, "use_ma": 1, "ma_s": 10, "ma_l": 60, "don_n": 120,
        "buy_pct": 0.30,
        "trail_before": 0.04, "trail_after": 0.04,
        "ptp_levels": "0.020:0.5",   # 1차 2%에서 50% 익절
        "tp_rate": 0.06, "fast_seq": "HL",
        "armed_enable": 0, "entry_tick_buffer": 1
    },
}

POLL_SEC = 1.0
STATE_DIR = "state_live_v3"
HALT_FILE = "halt.flag"
HEARTBEAT_SEC = 60
HEARTBEAT_PATH = "heartbeat.txt"
ensure_dir(STATE_DIR)

# ===== DIAG/FEED ENV =====
LOG_DATA_DIAG  = int(os.getenv("LOG_DATA_DIAG", "1"))
LOG_TRAIL      = int(os.getenv("LOG_TRAIL", "0"))           # stop 인상 로그 토글
BACKFILL_LIMIT = int(os.getenv("LIVE_BACKFILL_LIMIT", "10"))
NEED_BUF_TF    = int(os.getenv("LIVE_NEED_BUF_TF", "80"))
NEED_BUF_1M    = int(os.getenv("LIVE_NEED_BUF_1M", "60"))
FETCH_PAGE     = int(os.getenv("LIVE_FETCH_COUNT", "200"))  # <=200 per pyupbit limit
MAX_CACHE_MIN  = int(os.getenv("LIVE_MAX_CACHE_MIN", "5000"))
ARMED_EXEC     = os.getenv("ARMED_EXEC", "immediate").lower()  # "next_open" | "immediate"
PREWARM_AT_BOOT = int(os.getenv("LIVE_PREWARM", "1"))  # 부팅 시 필요 길이까지 즉시 백필
PTP_DEFER_THROTTLE_SEC = int(os.getenv("PTP_DEFER_THROTTLE_SEC", "60"))

# minute task pool
FETCH_WORKERS    = min(len(TICKERS), 8)     # 티커 수만큼 병렬, 상한 8
PER_TASK_TIMEOUT = 8.0                      # per-symbol minute task timeout

# === BUY serialization across symbols (avoid KRW oversubscribe)
BUY_LOCK = threading.Lock()

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
        if not part:
            continue
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
    def blocked(self) -> bool:
        return time.time() < self.block_until
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

class UpbitBroker:
    def __init__(self, access: str, secret: str):
        self.up = pyupbit.Upbit(access, secret)

    # robust orderbook normalizer (handles list/dict variants)
    def _normalize_orderbook(self, resp: Any) -> Tuple[float, float]:
        if resp is None:
            raise RuntimeError("orderbook None")
        item = None
        if isinstance(resp, list):
            item = resp[0] if resp else None
        elif isinstance(resp, dict):
            item = resp
        else:
            raise RuntimeError(f"orderbook unexpected type: {type(resp)}")
        if not item:
            raise RuntimeError("orderbook empty item")
        if "orderbook_units" in item:
            units = item["orderbook_units"] or []
            if not units:
                raise RuntimeError("orderbook units empty")
            bid = float(units[0]["bid_price"])
            ask = float(units[0]["ask_price"])
            return bid, ask
        # fallback keys
        for k in ("bid_price", "best_bid"):
            if k in item:
                bid = float(item[k]); break
        else:
            raise RuntimeError("orderbook no bid")
        for k in ("ask_price", "best_ask"):
            if k in item:
                ask = float(item[k]); break
        else:
            raise RuntimeError("orderbook no ask")
        return bid, ask

    def krw_balance(self) -> float:
        bal = safe_call(self.up.get_balance, "KRW"); return float(bal or 0.0)
    def best_bid_ask(self, ticker: str) -> Tuple[float, float]:
        ob = safe_call(pyupbit.get_orderbook, ticker)
        bid, ask = self._normalize_orderbook(ob)
        if bid <= 0 or ask <= 0:
            raise RuntimeError("invalid bid/ask")
        return bid, ask
    def buy_market_krw(self, ticker: str, krw: float) -> dict:
        return safe_call(self.up.buy_market_order, ticker, krw)
    def sell_market(self, ticker: str, qty: float) -> dict:
        return safe_call(self.up.sell_market_order, ticker, qty)
    def get_order(self, uuid: str) -> dict:
        return safe_call(self.up.get_order, uuid)
    def get_balances(self):
        return safe_call(self.up.get_balances)

# -------- Backfilling Feed (1m cache + diagnostics) --------
class Feed:
    def __init__(self, ticker: str):
        self.ticker = ticker
        self._cache: Optional[pd.DataFrame] = None

    def _pull(self, to_ts: Optional[pd.Timestamp] = None, count: int = FETCH_PAGE) -> pd.DataFrame:
        kw = {"interval": "minute1", "count": min(int(count), 200)}
        if to_ts is not None:
            # KST naive for pyupbit; convert if tz-aware
            if getattr(to_ts, "tzinfo", None) is not None:
                to_ts = to_ts.tz_convert("Asia/Seoul").tz_localize(None)
            kw["to"] = to_ts.strftime("%Y-%m-%d %H:%M:%S")
        tmp = safe_call(pyupbit.get_ohlcv, self.ticker, **kw)
        df = tmp if isinstance(tmp, pd.DataFrame) else pd.DataFrame()
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
        # Cursor-based pagination with robust fallback and real past advancement
        if self._cache is None or self._cache.empty:
            self._sync_latest()
            if self._cache is None or self._cache.empty:
                return

        tries = 0
        prev_len = len(self._cache)
        prev_oldest = self._cache.index[0]
        cursor = prev_oldest - pd.Timedelta(seconds=1)  # start just before current oldest

        while len(self._cache) < min_len and tries < limit:
            older = self._pull(cursor)
            got = 0 if (older is None or older.empty) else len(older)

            # progress log
            try:
                now_oldest = self._cache.index[0]
                now_latest = self._cache.index[-1]
                logging.info(
                    f"[{self.ticker}] [backfill] try={tries+1}/{limit} to={cursor} got={got} "
                    f"| cache_len={len(self._cache)} oldest={now_oldest} latest={now_latest}"
                )
            except Exception:
                logging.info(f"[{self.ticker}] [backfill] try={tries+1}/{limit} to={cursor} got={got}")

            if older is None or older.empty:
                # push cursor further back (defensive jump)
                cursor = cursor - pd.Timedelta(minutes=FETCH_PAGE * 2)
                tries += 1
                time.sleep(0.15)
                continue

            older_earliest = older.index[0]
            older_latest   = older.index[-1]

            # if the fetched block doesn't extend past prev_oldest, force jump and retry
            if older_earliest >= prev_oldest and older_latest >= prev_oldest:
                cursor = cursor - pd.Timedelta(minutes=FETCH_PAGE * 2)
                tries += 1
                time.sleep(0.15)
                continue

            # merge + dedup
            self._cache = pd.concat([older, self._cache]).sort_index()
            self._cache = self._cache[~self._cache.index.duplicated(keep="last")]

            new_len = len(self._cache)
            if new_len == prev_len:
                cursor = cursor - pd.Timedelta(minutes=FETCH_PAGE * 2)
            else:
                cursor = older_earliest - pd.Timedelta(seconds=1)
                prev_len = new_len
                prev_oldest = self._cache.index[0]

            if new_len > MAX_CACHE_MIN:
                self._cache = self._cache.iloc[-MAX_CACHE_MIN:]
                prev_len = len(self._cache)
                prev_oldest = self._cache.index[0]

            tries += 1
            time.sleep(0.15)

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
    # Inflight protection
    pending_buy_uuid: str = ""
    buy_guard_until: float = 0.0

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
        st.pending_buy_uuid = str(d.get("pending_buy_uuid", ""))
        st.buy_guard_until = float(d.get("buy_guard_until", 0.0))
        return st

class SymbolStrategy:
    def __init__(self, symbol: str, broker: UpbitBroker, params: Dict[str, Any]):
        self.symbol = symbol; self.broker = broker
        self.p = {**DEFAULTS, **(params or {})}
        self.state_path = os.path.join(STATE_DIR, f"{self.symbol.replace('-','_')}.json")
        self.state = self._load_state()
        self.ptp_levels = parse_ptp_levels(self.p.get("ptp_levels", ""))
        self.last_tf_idx: Optional[pd.Timestamp] = None
        self._lock = threading.RLock()
        # (PATCH) use str keys for throttling/inflight to avoid float equality pitfalls
        self._ptp_deferred_ts: Dict[str, float] = {}  # level_key -> last log ts
        self._ptp_inflight = set()                    # {level_key}
        # (PATCH-PTP) runtime cache of completed levels (string keys) + tolerant check
        self._ptp_done_keys = set()                   # {"0.035000"}
        # --- intrabar high tracking (bid-only), 1-min epoch reset ---
        self._intrabar_high = 0.0
        self._intrabar_epoch_min = -1
        # seed runtime cache from persisted state
        for _lv in (self.state.ptp_hits or []):
            try:
                self._ptp_done_keys.add(self._lv_key(float(_lv)))
            except Exception:
                pass

    # helper: stable key for levels
    @staticmethod
    def _lv_key(lv: float) -> str:
        return f"{lv:.6f}"

    # tolerant "already hit" detector (handles float json roundtrip)
    def _ptp_already(self, lv: float) -> bool:
        key = self._lv_key(lv)
        if key in self._ptp_done_keys:
            return True
        hits = self.state.ptp_hits or []
        return any(abs(float(x) - float(lv)) <= 1e-9 for x in hits)

    # ---------- dust helper ----------
    def _is_dust(self, qty: float, ref_px: float) -> bool:
        """qty*ref_px가 최소주문금액*margin보다 작으면 더스트로 간주(주문 없이 로컬만 플랫)."""
        if qty <= 0 or ref_px <= 0:
            return False
        min_krw = float(self.p.get("min_krw_order", 6000.0))
        margin  = float(self.p.get("dust_margin", 0.98))
        return (qty * ref_px) < (min_krw * margin)

    # ---------- state helpers ----------
    def _load_state(self) -> PositionState:
        try:
            if os.path.exists(self.state_path):
                with open(self.state_path,"r",encoding="utf-8") as f: d=json.load(f)
                st = PositionState.from_json(d); logging.info(f"[{self.symbol}] state loaded"); return st
        except Exception as e:
            logging.warning(f"[{self.symbol}] state load error: {e}")
        return PositionState()

    def _save_state(self):
        with self._lock:
            try:
                with open(self.state_path,"w",encoding="utf-8") as f:
                    json.dump(self.state.to_json(), f, ensure_ascii=False, indent=2)
            except Exception as e:
                logging.warning(f"[{self.symbol}] state save error: {e}")

    def reset_state_inplace(self):
        with self._lock:
            st = self.state
            st.has_pos=False; st.qty=0.0; st.avg=0.0; st.stop=0.0; st.ts_anchor=0.0
            st.ptp_hits=[]; st.tp1_done=False; st.init_qty=0.0
            st.is_armed=False; st.armed_until=0.0; st.armed_anchor_price=0.0
            st.red_run_1m=0; st.armed_open_ts=0.0
            st.pending_buy_ts=0.0; st.pending_buy_uuid=""; st.buy_guard_until=0.0
            # (PATCH-PTP) clear runtime caches as this is a new flat state
            self._ptp_done_keys.clear()
            self._ptp_inflight.clear()
            self._ptp_deferred_ts.clear()
            self._save_state()

    def _trail_step(self) -> float:
        with self._lock:
            before = float(self.p.get("trail_before", 0.08))
            after  = float(self.p.get("trail_after",  0.08))
            return after if bool(self.state.tp1_done) else before

    def _raise_stop(self, candidate: float) -> bool:
        with self._lock:
            prev = self.state.stop or 0.0
            if candidate and candidate > prev:
                self.state.stop = candidate
                self._save_state()
                if LOG_TRAIL:
                    try:
                        logging.info(f"[{self.symbol}] [TS] stop {round_price(prev,'down'):.0f} → {round_price(candidate,'down'):.0f}")
                    except Exception:
                        logging.info(f"[{self.symbol}] [TS] stop {prev:.0f} → {candidate:.0f}")
                return True
            return False

    # ---------- exchange snapshot helpers ----------
    def _exchange_position_snapshot(self) -> Tuple[float, float]:
        """Return (qty, avg) for this symbol from exchange balances."""
        bals = self.broker.get_balances() or []
        by_ccy = {b.get("currency"): b for b in bals}
        ccy = self.symbol.split("-")[1]
        b = by_ccy.get(ccy) or {}
        qty = float(b.get("balance") or 0.0)
        avg = float(b.get("avg_buy_price") or 0.0)
        return qty, avg

    # ----- fill resolution -----
    def _resolve_buy_fill(self, uuid: str, timeout_s: float = 20.0, poll_s: float = 0.7) -> Tuple[float,float]:
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

    def _resolve_sell_fill(self, uuid: str, timeout_s: float = 12.0, poll_s: float = 0.6) -> float:
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
            # HALT / CB
            if os.path.exists(HALT_FILE):
                logging.info(f"[{self.symbol}] [SKIP] HALT (entry blocked)")
                log_event("skip_buy", symbol=self.symbol, reason="halt"); return
            if cb.blocked():
                logging.info(f"[{self.symbol}] [SKIP] CB blocked (entry)")
                log_event("skip_buy", symbol=self.symbol, reason="cb_blocked"); return

            # pre-buy: reconcile & guard
            exch_qty, exch_avg = self._exchange_position_snapshot()
            # (DUST) 거래소 잔고가 더스트면 0으로 간주하여 진입 방해 제거
            try:
                bid, _ = self.broker.best_bid_ask(self.symbol)
                if self._is_dust(exch_qty, bid):
                    exch_qty = 0.0
            except Exception:
                pass

            with self._lock:
                if self.state.has_pos or exch_qty > 1e-12:
                    # sync if needed
                    if exch_qty > 1e-12 and not self.state.has_pos:
                        self.state.has_pos=True; self.state.qty=exch_qty; self.state.avg=exch_avg
                        self.state.init_qty=exch_qty; self.state.ptp_hits=[]; self.state.tp1_done=False
                        self.state.is_armed=False; self.state.pending_buy_ts=0.0
                        self.state.pending_buy_uuid=""; self.state.buy_guard_until=0.0
                        self._ptp_done_keys.clear()  # 새 포지션 동기화 시 캐시 초기화
                        self._raise_stop(round_price(exch_avg*(1.0-self._trail_step()), "down"))
                        self._save_state()
                        logging.info(f"[{self.symbol}] [SYNC-before-buy] pos exists on exchange qty={exch_qty:.8f}@{exch_avg:.0f}")
                    return
                # guard: if within guard window BUT exchange says still flat -> allow
                if (self.state.buy_guard_until or 0.0) > time.time():
                    logging.info(f"[{self.symbol}] [guard] in window but flat on exchange → proceed")

            # serialize buys to avoid KRW oversubscribe
            with BUY_LOCK:
                total_krw = self.broker.krw_balance()
                amt = total_krw * float(self.p["buy_pct"])
                if amt < float(self.p["min_krw_order"]):
                    logging.info(f"[{self.symbol}] [SKIP] low budget {amt:.0f}")
                    log_event("skip_buy", symbol=self.symbol, reason="low_budget", krw=amt); return
                resp = self.broker.buy_market_krw(self.symbol, amt)
                uuid_ = str(resp.get("uuid",""))
                log_event("order_submitted", side="buy", symbol=self.symbol, uuid=uuid_, krw=amt, reason=reason)
                if not uuid_:
                    cb.fail("buy-uuid"); logging.error(f"[{self.symbol}] BUY no uuid"); return

            # set inflight guard
            with self._lock:
                self.state.pending_buy_uuid = uuid_
                self.state.buy_guard_until = time.time() + 90.0
                self._save_state()

            qty, avg = self._resolve_buy_fill(uuid_)
            if qty<=0 or avg<=0:
                cb.fail("buy-nofill"); logging.error(f"[{self.symbol}] BUY no fill (timeout or rejected)")
                # leave guard; minute/next attempt will pre-check exchange again
                return

            with self._lock:
                self.state.has_pos=True; self.state.qty=qty; self.state.avg=avg
                self.state.init_qty=qty; self.state.ptp_hits=[]; self.state.tp1_done=False
                self.state.ts_anchor=avg
                self.state.pending_buy_ts=0.0
                self.state.pending_buy_uuid=""; self.state.buy_guard_until=0.0
                self._ptp_done_keys.clear()  # 새 포지션 시작: 레벨 캐시 초기화
                self._raise_stop(round_price(avg*(1.0-self._trail_step()), "down"))
                self.state.is_armed=False; self._save_state()
            cb.ok()
            logging.info(f"[{self.symbol}] [BUY-{reason}] {qty:.8f} @ {round_price(avg,'down'):.0f}")
            log_event("trade_filled", side="buy", symbol=self.symbol, uuid=uuid_, qty=qty, avg=avg, reason=reason)
        except Exception as e:
            cb.fail("buy-exc"); logging.error(f"[{self.symbol}] BUY error: {e}"); logging.error(traceback.format_exc())

    def _sell(self, qty: float, reason: str) -> float:
        """Place market sell; return filled quantity (0.0 if none)."""
        try:
            # Snapshot under lock
            with self._lock:
                qty = min(qty, self.state.qty)
                step = float(self.p.get("qty_step", 1e-8))
                qty = floor_to_step(qty, step)
                if qty<=0: return 0.0
                cur_qty = self.state.qty

            # partial sell minKRW check (except when liquidating all)
            is_last = abs(qty - cur_qty) <= 1e-12
            try:
                bid, _ = self.broker.best_bid_ask(self.symbol)
                if (not is_last) and (bid * qty) < float(self.p.get("min_krw_order", 6000.0)):
                    logging.info(f"[{self.symbol}] [SELL-skip] partial notional<{self.p.get('min_krw_order')} ({bid*qty:.1f})")
                    return 0.0
            except Exception as e:
                # Only allow last-all when bid lookup fails
                if not is_last:
                    logging.warning(f"[{self.symbol}] [SELL-skip] no bid for partial; {e}")
                    return 0.0
                logging.warning(f"[{self.symbol}] [SELL] no bid; proceed last-all: {e}")

            resp = self.broker.sell_market(self.symbol, qty)
            uuid_ = str(resp.get("uuid",""))
            log_event("order_submitted", side="sell", symbol=self.symbol, uuid=uuid_, qty=qty, reason=reason)
            if not uuid_:
                logging.error(f"[{self.symbol}] SELL no uuid"); return 0.0
            filled = self._resolve_sell_fill(uuid_)
            if filled<=0:
                logging.error(f"[{self.symbol}] SELL no fill"); return 0.0

            with self._lock:
                self.state.qty -= filled
                if self.state.qty <= 1e-12:
                    # in-place reset (no object swap)
                    self.reset_state_inplace()
                else:
                    self._save_state()
            logging.info(f"[{self.symbol}] [SELL-{reason}] qty={filled:.8f}")
            log_event("trade_filled", side="sell", symbol=self.symbol, uuid=uuid_, qty=filled, reason=reason, stop=self.state.stop)
            return filled
        except Exception as e:
            logging.error(f"[{self.symbol}] SELL error: {e}"); logging.error(traceback.format_exc())
            return 0.0

    # ----- GATE (N-min close) -----
    def on_bar_close_tf(self, dfN: pd.DataFrame, cb: CircuitBreaker):
        # HALT: skip gating/arming window opening
        if os.path.exists(HALT_FILE):
            logging.info(f"[{self.symbol}] [HALT] gate skipped")
            return

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
        ms = ml = None
        if int(self.p["use_ma"])==1:
            ms=sma(closes,ma_s_v); ml=sma(closes,ma_l)
            if ms is None or ml is None or pd.isna(ms.iloc[-1]) or pd.isna(ml.iloc[-1]) or ms.iloc[-1] <= ml.iloc[-1]:
                ma_pass=False

        up_buf = upper + price_unit(upper)*int(self.p.get("entry_tick_buffer",2))
        break_pass = prev_close > up_buf

        # [gate-check] 상세 로그
        if LOG_DATA_DIAG:
            try:
                msg = (f"[{self.symbol}] [gate-check] "
                       f"break_pass={bool(break_pass)} "
                       f"(prev_close={round_price(prev_close,'down'):.0f} "
                       f"> up_buf={round_price(up_buf,'down'):.0f} "
                       f"from upper={round_price(upper,'down'):.0f}) | "
                       f"ma_pass={bool(ma_pass)}")
                if int(self.p['use_ma'])==1 and ms is not None and ml is not None \
                   and not pd.isna(ms.iloc[-1]) and not pd.isna(ml.iloc[-1]):
                    msg += f" (ms={float(ms.iloc[-1]) :.2f} > ml={float(ml.iloc[-1]) :.2f})"
                logging.info(msg)
            except Exception:
                logging.info(f"[{self.symbol}] [gate-check] break_pass={break_pass} ma_pass={ma_pass}")

        # 게이트 실패 사유 이벤트 (flat & unarmed 상태에서만)
        with self._lock:
            flat_unarmed = (not self.state.has_pos) and (not self.state.is_armed)
        if flat_unarmed and not (break_pass and ma_pass):
            reasons = []
            if not break_pass: reasons.append("no_break")
            if not ma_pass:    reasons.append("ma_filter")
            reason = "&".join(reasons) or "unknown"
            try:
                log_event("gate_noarm", symbol=self.symbol, reason=reason,
                          upper=float(upper), up_buf=float(up_buf), prev_close=float(prev_close),
                          ms=(float(ms.iloc[-1]) if ms is not None and not pd.isna(ms.iloc[-1]) else None),
                          ml=(float(ml.iloc[-1]) if ml is not None and not pd.isna(ml.iloc[-1]) else None))
            except Exception:
                log_event("gate_noarm", symbol=self.symbol, reason=reason)

        if int(self.p.get("armed_enable",0))==0:
            with self._lock:
                can_buy = not self.state.has_pos
            if can_buy and break_pass and ma_pass and not cb.blocked():
                logging.info(f"[{self.symbol}] [LEGACY-ENTER] pass → BUY")
                self._enter_position(cb,"LEGACY")
            return

        # Armed window open (anchor per backtest: max(upper, prev_close))
        with self._lock:
            can_arm = break_pass and ma_pass and (not self.state.has_pos) and (not self.state.is_armed)
        if can_arm:
            window = int(self.p.get("armed_window_min", self.p["tf_base"]))
            now_ts = time.time()
            anchor = max(upper, prev_close)
            with self._lock:
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
        # HALT: ignore triggers
        if os.path.exists(HALT_FILE):
            return
        with self._lock:
            if not self.state.is_armed:
                return
            now_t = time.time()
            armed_until = self.state.armed_until or 0.0
            opened_ts = self.state.armed_open_ts or 0.0
            if now_t >= armed_until:
                self.state.is_armed=False; self._save_state()
                logging.info(f"[{self.symbol}] [DISARMED] window expired")
                log_event("armed_close", symbol=self.symbol, reason="window_expired")
                return

        need_len = max(int(self.p["armed_ma_l_1m"])+1, int(self.p["armed_rebreak_lookback"])+3)
        if df1m is None or len(df1m) < (need_len+1): 
            return

        # 마지막으로 닫힌 1분봉 마감시각
        last_closed_ts = df1m.index[-1].to_pydatetime().timestamp()
        preopen_epoch = (last_closed_ts <= opened_ts + 1e-9)

        # 최근 두 개 1분봉
        prev1m, cur1m = df1m.iloc[-2], df1m.iloc[-1]

        # --- (pre-open이 아닐 때만) 디스암 조건 평가 ---
        if not preopen_epoch:
            with self._lock:
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
        # pre-open 구간에서는 예약 판단을 우선하고 디스암은 보류

        # --- 트리거 판정 (MA / REBREAK) ---
        trig=str(self.p.get("armed_micro_trigger","MA")).upper(); trigger_ok=False
        if trig=="MA":
            s1=sma(df1m["close"],int(self.p["armed_ma_s_1m"])); l1=sma(df1m["close"],int(self.p["armed_ma_l_1m"]))
            if s1 is not None and l1 is not None and len(s1)>=2 and len(l1)>=2:
                # 직전→현재 1분 사이 크로스
                if s1.iloc[-2] <= l1.iloc[-2] and s1.iloc[-1] > l1.iloc[-1]:
                    trigger_ok=True
        elif trig=="REBREAK":
            k=int(self.p["armed_rebreak_lookback"])
            recent_high=float(df1m["high"].iloc[-k-1:-1].max())
            if float(cur1m["close"])>recent_high:
                trigger_ok=True

        # --- 실행(예약/즉시) ---
        with self._lock:
            can_buy = trigger_ok and (not cb.blocked()) and (not self.state.has_pos)

        if can_buy:
            if ARMED_EXEC == "next_open":
                # pre-open이면 윈도우 오픈 직후 첫 1분 오픈가, 아니면 현재 시각 기준 다음 1분 오픈가
                base_ts = opened_ts if preopen_epoch else time.time()
                pending_ts = math.floor(base_ts/60)*60 + 60
                # 혹시 지났다면 현재 기준으로 보정
                if pending_ts <= time.time():
                    pending_ts = math.floor(time.time()/60)*60 + 60
                with self._lock:
                    self.state.pending_buy_ts = pending_ts
                    self._save_state()
                logging.info(f"[{self.symbol}] [TRIGGER-{trig}] confirmed → BUY_PENDING(NEXT_OPEN){' (pre-open)' if preopen_epoch else ''}")
                log_event("armed_trigger", symbol=self.symbol, trigger=trig, exec="next_open",
                          pending_ts=pending_ts, preopen=int(preopen_epoch))
            else:
                reason = f"ARMED-{trig}-PREOPEN" if preopen_epoch else f"ARMED-{trig}"
                logging.info(f"[{self.symbol}] [TRIGGER-{trig}] confirmed → BUY_NOW{' (pre-open)' if preopen_epoch else ''}")
                log_event("armed_trigger", symbol=self.symbol, trigger=trig, exec="immediate", preopen=int(preopen_epoch))
                self._enter_position(cb, reason)

    # ----- intrabar manage (1s) -----
    def manage_intrabar(self, bid: float, ask: float, cb: CircuitBreaker):
        now = time.time()

        # --- DUST: 포지션이 있고 평가금액이 더스트면 주문 없이 로컬만 플랫 처리 ---
        with self._lock:
            had_pos = self.state.has_pos
            qty_now = self.state.qty
        if had_pos and self._is_dust(qty_now, bid):
            self.reset_state_inplace()
            logging.info(f"[{self.symbol}] [DUST->FLAT] notional≈{bid*qty_now:.1f} KRW (qty={qty_now:.8f})")
            log_event("dust_local_flat", symbol=self.symbol, qty=qty_now, bid=bid)
            # 플랫이므로 이후 TS/PTP/TP 관리 스킵
            # (armed/pending 등은 상태에 의해 영향 없음)
            return

        # intrabar high (bid-only) with 1-min reset
        cur_min = int(now // 60)
        if cur_min != self._intrabar_epoch_min:
            self._intrabar_epoch_min = cur_min
            self._intrabar_high = bid
        else:
            if bid > self._intrabar_high:
                self._intrabar_high = bid

        # fast-loop ARMED expiry
        with self._lock:
            if self.state.is_armed and now >= (self.state.armed_until or 0.0):
                self.state.is_armed = False
                self._save_state()
                logging.info(f"[{self.symbol}] [DISARMED] window expired (fast loop)")
                log_event("armed_close", symbol=self.symbol, reason="window_expired_fast")

            # HALT: cancel pending-buy; do NOT block stops/TP/PTP
            if os.path.exists(HALT_FILE) and self.state.pending_buy_ts > 0.0:
                logging.info(f"[{self.symbol}] [HALT] pending-buy canceled")
                self.state.pending_buy_ts = 0.0
                self._save_state()

            # pending next-open buy (CB-aware)
            if (not self.state.has_pos) and (self.state.pending_buy_ts > 0.0) and (now >= self.state.pending_buy_ts):
                if cb.blocked():
                    self.state.pending_buy_ts = math.floor(now/60)*60 + 60  # postpone 1m
                    self._save_state()
                else:
                    # double-check exchange before placing
                    pass_to_buy = True
                    exch_qty, exch_avg = self._exchange_position_snapshot()
                    if exch_qty > 1e-12:
                        # already filled elsewhere
                        self.state.has_pos=True; self.state.qty=exch_qty; self.state.avg=exch_avg
                        self.state.init_qty=exch_qty; self.state.ptp_hits=[]; self.state.tp1_done=False
                        self.state.pending_buy_ts = 0.0
                        self.state.is_armed=False
                        self.state.pending_buy_uuid=""; self.state.buy_guard_until=0.0
                        self._ptp_done_keys.clear()
                        self._raise_stop(round_price(exch_avg*(1.0-self._trail_step()), "down"))
                        self._save_state()
                        pass_to_buy = False
                    if pass_to_buy:
                        # release lock to avoid nesting during _enter_position
                        pass
                # end with; fall-through

        # If we decided to buy now (pending window hit and not blocked), call enter outside lock
        with self._lock:
            should_enter = (not self.state.has_pos) and (self.state.pending_buy_ts > 0.0) and (now >= self.state.pending_buy_ts) and (not cb.blocked())
        if should_enter:
            self._enter_position(cb, "ARMED-NEXTOPEN")
            with self._lock:
                self.state.pending_buy_ts = 0.0
                self._save_state()

        # If flat, nothing more to manage
        with self._lock:
            has_pos = self.state.has_pos
        if not has_pos:
            return

        # trailing stop (bid-only intrabar high)
        trail = self._trail_step()
        high_now = self._intrabar_high
        self._raise_stop(round_price(high_now*(1.0 - trail), "down"))

        # PTP + BE bump (with deferred throttle on notional<minKRW)
        if self.ptp_levels:
            for (lv, fr) in self.ptp_levels:
                level_key = self._lv_key(lv)
                with self._lock:
                    already  = self._ptp_already(lv)          # (PATCH) tolerant check + runtime cache
                    inflight = (level_key in self._ptp_inflight)
                    qty_now  = self.state.qty
                    init_qty = self.state.init_qty
                    avg_px   = self.state.avg
                # already filled, same-level inflight, or no qty
                if already or inflight or qty_now<=0:
                    continue
                tgt=round_price(avg_px*(1.0+lv), "down")
                if high_now>=tgt:
                    step = float(self.p.get("qty_step", 1e-8))
                    sell_qty = floor_to_step(min(init_qty*float(fr), qty_now), step)
                    if sell_qty <= 0:
                        continue
                    # pre-check notional for throttle
                    is_last = abs(sell_qty - qty_now) <= 1e-12
                    if not is_last:
                        try:
                            if bid * sell_qty < float(self.p.get("min_krw_order", 6000.0)):
                                last_log = self._ptp_deferred_ts.get(level_key, 0.0)
                                if (time.time() - last_log) >= PTP_DEFER_THROTTLE_SEC:
                                    logging.info(f"[{self.symbol}] [PTP-defer] lv={lv:.3f} qty={sell_qty:.8f} notional<{self.p.get('min_krw_order')}")
                                    self._ptp_deferred_ts[level_key] = time.time()
                                continue
                        except Exception:
                            # if bid check fails, fall back to _sell's own check
                            pass

                    # Inflight guard set once we've passed checks
                    with self._lock:
                        self._ptp_inflight.add(level_key)
                    try:
                        filled = self._sell(sell_qty, f"PTP({lv:.3f},{fr:.2f})")
                        if filled > 0.0:
                            with self._lock:
                                # (PATCH) 기록은 허용오차 비교 후 한 번만
                                if self.state.ptp_hits is None:
                                    self.state.ptp_hits = []
                                if not any(abs(float(x) - float(lv)) <= 1e-9 for x in (self.state.ptp_hits or [])):
                                    self.state.ptp_hits.append(lv)
                                self._ptp_done_keys.add(level_key)
                                if int(self.p.get("ptp_be",0))==1:
                                    fee = float(self.p["fee"])
                                    eps = float(self.p["eps_exec"])
                                    be  = avg_px * ((1.0 + fee) / (1.0 - fee)) * (1.0 + eps)
                                    self._raise_stop(round_price(be, "up"))
                                    self.state.tp1_done=True
                                self._save_state()
                                logging.info(f"[{self.symbol}] [BE] stop→{round_price(be,'up'):.0f}" if int(self.p.get("ptp_be",0))==1 else f"[{self.symbol}] [PTP] hit lv={lv:.3f}")
                    finally:
                        with self._lock:
                            self._ptp_inflight.discard(level_key)

        # TP
        tp=float(self.p.get("tp_rate",0.0))
        if tp>0:
            with self._lock:
                qty_now = self.state.qty; avg_px = self.state.avg
            if qty_now>0:
                tp_px=round_price(avg_px*(1.0+tp), "down")
                if high_now>=tp_px:
                    self._sell(qty_now,"TP"); return

        # TS
        with self._lock:
            st_stop = self.state.stop; qty_now = self.state.qty
        if st_stop and bid<=st_stop and qty_now>0:
            self._sell(qty_now,"TS")

# ========================= reconcile =========================
def reconcile_from_exchange(strats: Dict[str, SymbolStrategy], broker: UpbitBroker):
    try:
        bals=broker.get_balances() or []; by_ccy={b.get("currency"):b for b in bals}
        for sym, stg in strats.items():
            ccy=sym.split("-")[1]; b=by_ccy.get(ccy)
            qty=float((b or {}).get("balance") or 0.0); avg=float((b or {}).get("avg_buy_price") or 0.0)

            # (DUST) 부팅 시 더스트면 flat 간주
            try:
                bid, _ = broker.best_bid_ask(sym)
                if bid > 0 and stg._is_dust(qty, bid):
                    qty = 0.0
            except Exception:
                pass

            if qty>1e-12:
                with stg._lock:
                    stg.state.has_pos=True; stg.state.qty=qty; stg.state.avg=avg
                    stg.state.init_qty=qty; stg.state.ptp_hits=[]; stg.state.tp1_done=False
                    stg.state.is_armed=False; stg.state.pending_buy_ts=0.0
                    stg.state.pending_buy_uuid=""; stg.state.buy_guard_until=0.0
                    stg._ptp_done_keys.clear()
                    stg._raise_stop(round_price(avg*(1.0-stg._trail_step()), "down"))
                    stg._save_state()
                logging.info(f"[{sym}] [RECONCILE] qty={qty:.8f}, avg={avg:.0f}")
                log_event("reconcile_pos", symbol=sym, qty=qty, avg=avg)
            else:
                with stg._lock:
                    if stg.state.has_pos or stg.state.qty>0:
                        stg.reset_state_inplace()
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

# ========================= prewarm (optional at boot) =========================
def prewarm_at_boot(feeds: Dict[str, 'Feed'], strats: Dict[str, 'SymbolStrategy']):
    for s in TICKERS:
        p = strats[s].p
        need_1m_total, _, _ = compute_need_1m(p)
        df = feeds[s].fetch_1m_needed(need_minutes=need_1m_total)
        logging.info(f"[{s}] [prewarm] 1m={len(df)} / need={need_1m_total}")
        log_event("prewarm_done", symbol=s, have=len(df), need=need_1m_total)

# ========================= main loop (tick scheduler) =========================
def main():
    if not ACCESS or not SECRET:
        raise SystemExit("ACCESS/SECRET required. Set ACCESS/SECRET variables in this script.")
    broker=UpbitBroker(ACCESS, SECRET)
    params_by_symbol=load_symbol_params()
    feeds={s: Feed(s) for s in TICKERS}
    strats={s: SymbolStrategy(s, broker, params_by_symbol.get(s, {})) for s in TICKERS}
    CB={s: CircuitBreaker() for s in TICKERS}

    logging.info(f"[init] started v3 armed bot RUN_ID={RUN_ID}")
    log_event("boot", tickers=TICKERS, run_id=RUN_ID)
    reconcile_from_exchange(strats, broker)

    # Prewarm: fetch needed minutes immediately
    if PREWARM_AT_BOOT:
        prewarm_at_boot(feeds, strats)

    # ===== Absolute schedules (no drift) =====
    next_min_tick = (int(time.time() // 60) + 1) * 60
    next_hb_tick  = (int(time.time() // HEARTBEAT_SEC) + 1) * HEARTBEAT_SEC

    DATA_READY: Dict[str, Optional[str]] = {s: None for s in TICKERS}

    def minute_task(symbol: str):
        """Fetch 1m (backfill), resample N, gate & armed-trigger once per minute (per symbol)."""
        if CB[symbol].blocked(): return
        p = strats[symbol].p
        need_1m_total, need_tf_bars, _ = compute_need_1m(p)

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

        # 3) readiness (short_armed1m 포함)
        ready_gate  = len(dfN) >= need_tf_bars
        ready_armed = True
        if int(p.get("armed_enable",0))==1:
            ready_armed = len(df1) >= need_1m_total

        reasons = []
        if not ready_gate:  reasons.append(f"short_tfN({len(dfN)}/{need_tf_bars})")
        if int(p.get("armed_enable",0))==1 and not ready_armed:
            reasons.append(f"short_armed1m({len(df1)}/{need_1m_total})")

        cur_state = "READY" if (ready_gate and ready_armed) else "NOT_READY"
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

        if True:
            # trigger check only if ARMED
            strats[symbol].check_armed_trigger_1m(df1, CB[symbol])

    # ---- minute task executor (non-blocking) ----
    minute_pool = ThreadPoolExecutor(max_workers=FETCH_WORKERS)
    pending: Dict[str, Tuple[Any, float]] = {}  # symbol -> (future, start_ts)

    try:
        while True:
            try:
                now=time.time()

                # ===== minute boundary: schedule concurrent minute tasks (non-blocking) =====
                if now >= next_min_tick:
                    # housekeeping old futures (timeout/exception)
                    for s, pack in list(pending.items()):
                        fut, t0 = pack
                        if not fut.done() and (now - t0) > PER_TASK_TIMEOUT:
                            CB[s].fail("minute-timeout")
                        if fut.done():
                            try:
                                fut.result(timeout=0.001)
                            except Exception as e:
                                CB[s].fail("minute-exc")
                                logging.error(f"[{s}] minute task error: {e}")
                                logging.error(traceback.format_exc())
                            finally:
                                pending.pop(s, None)

                    # submit new tasks for all symbols
                    for s in TICKERS:
                        if s in pending and not pending[s][0].done():
                            continue
                        pending[s] = (minute_pool.submit(minute_task, s), now)

                    # schedule next minute (catch-up precisely)
                    now2 = time.time()
                    while next_min_tick <= now2:
                        next_min_tick += 60

                # ===== fast loop: every second manage intrabar =====
                for s in TICKERS:
                    try:
                        # CB 차단 여부와 무관하게 포지션 관리(스탑/익절)는 항상 실행
                        bid, ask = broker.best_bid_ask(s)
                        strats[s].manage_intrabar(bid, ask, CB[s])
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
                        with stg._lock:
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
                    pass

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
    finally:
        minute_pool.shutdown(wait=False)

if __name__ == "__main__":
    main()
