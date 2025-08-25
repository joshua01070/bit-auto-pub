# === multi_coin_auto_trade_final.py ===
# Upbit multi-coin live trading bot — FINAL
# Key reliability upgrades:
#  - Robust fill reconciliation (trades → qty/avg, else balance delta, else warned estimate)
#  - pyupbit HTTP timeout monkeypatch (prevents indefinite blocking)
#  - Heartbeat watchdog thread (logs every HB_WATCHDOG_SEC regardless of main loop)
#  - Heartbeat check at loop head and tail
#  - Heartbeat log includes current positions like legacy version
#  - Per-ticker time budget (skip slow ticker to keep loop on schedule)
#  - Orderbook compat wrapper; balances normalization; safe logging boot

import os, sys, time, json, math, atexit, logging, threading
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List, Dict, Any

import pandas as pd
import pyupbit

# ==============================================================================
# 1) CONFIG
# ==============================================================================

ACCESS = ""   # <- fill
SECRET = ""   # <- fill

# Markets to trade (KRW markets)
TICKERS = ["KRW-BTC", "KRW-ETH"]

# Entry filters
K = 0.25
ATR_PERIOD = 14
RSI_PERIOD = 14
RSI_THRESHOLD = 39
MA_SHORT, MA_LONG = 5, 30
EMA15_EMA_SPAN = 20

# Exit / risk
FEE = 0.0005
TP1_RATE, TP2_RATE = 0.0155, 0.04
TP1_PCT = 0.50
TRAILING_STEP = 0.08
TRAILING_STEP_AFTER_TP1 = 0.035
REENTRY_TH = 0.005

# Ops
INTERVAL = "minute1"
WARMUP = max(ATR_PERIOD, RSI_PERIOD, MA_LONG) + 2
FAST_POLL = 5
STATE_PATH = "multi_state.json"
MIN_KRW_ORDER = 6000
BUY_PCT = 0.40
BUY_KRW_CAP: Optional[float] = None

# Reconciliation guard (skip TP/TS when mismatch is large)
RECON_WARN_KRW = MIN_KRW_ORDER * 0.5

LOG_LEVEL = logging.INFO
HEARTBEAT_SEC = 60
HB_WATCHDOG_SEC = 20
KST = timezone(timedelta(hours=9))

# Per-ticker soft time budget (seconds). If exceeded, skip remaining work for that ticker.
TICKER_TIME_BUDGET = 5.0

# ==============================================================================
# 2) Logging
# ==============================================================================

def setup_logging():
    # ensure clean root logger (VSCode/Windows sometimes pre-installs handlers)
    for h in logging.root.handlers[:]:
        logging.root.removeHandler(h)
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

# Early boot print (visible even if logging misconfigured)
print(f">>> boot: file={__file__}")
print(f">>> boot: python={sys.executable}")

# ==============================================================================
# 3) pyupbit request timeout monkeypatch (prevents indefinite blocking)
# ==============================================================================
try:
    import pyupbit.request_api as _ra
    _orig_pub = _ra._call_public_api
    def _call_public_api_timeout(url, **kwargs):
        kwargs.setdefault("timeout", 4)  # seconds
        return _orig_pub(url, **kwargs)
    _ra._call_public_api = _call_public_api_timeout

    _orig_priv = getattr(_ra, "_call_private_api", None)
    if _orig_priv:
        def _call_private_api_timeout(url, **kwargs):
            kwargs.setdefault("timeout", 4)
            return _orig_priv(url, **kwargs)
        _ra._call_private_api = _call_private_api_timeout
    print("[init] pyupbit timeout patch set to 4s")
except Exception as e:
    print(f"[init] timeout patch failed: {e}")

# ==============================================================================
# 4) Utils
# ==============================================================================

def now_kst() -> datetime:
    return datetime.now(KST)

def q8(x: float) -> float:
    return float(f"{x:.8f}")

def price_unit(p: float) -> float:
    if p >= 2_000_000: return 1000
    if p >= 1_000_000: return 500
    if p >=   500_000: return 100
    if p >=   100_000: return 50
    if p >=    10_000: return 10
    if p >=     1_000: return 5
    if p >=       100: return 1
    if p >=        10: return 0.1
    return 0.01

def round_price(p: float, direction: str = "nearest") -> float:
    unit = price_unit(p)
    if direction == "down": return math.floor(p / unit) * unit
    if direction == "up":   return math.ceil(p / unit) * unit
    return round(p / unit) * unit

def sleep_to_next_minute(offset_sec: float = 0.0):
    now = time.time()
    next_min = (int(now // 60) + 1) * 60 + offset_sec
    time.sleep(max(0.0, next_min - now))

def safe_call(fn, *args, retries=3, base_wait=0.5, **kwargs):
    wait = base_wait
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            logging.warning(f"[safe_call] retry {i+1}/{retries}: {e}", exc_info=True)
            time.sleep(wait)
            wait = min(wait * 2, 4.0)
    raise RuntimeError(f"{fn.__name__} failed after {retries} retries")

def can_sell(qty: float, price: float, min_krw_order: float = MIN_KRW_ORDER) -> bool:
    return (qty * price) >= min_krw_order

def min_sell_qty(price: float, min_krw_order: float = MIN_KRW_ORDER) -> float:
    return q8(min_krw_order / max(price, 1e-8))

def safe_qty(qty: float, avail: float) -> float:
    return q8(max(0.0, min(qty, avail)))

# ==============================================================================
# 5) Position & persistence
# ==============================================================================
class Position:
    def __init__(self):
        self.entries: List[Tuple[float, float]] = []
        self.tp1_done: bool = False
        self.trailing_stop: Optional[float] = None

    @property
    def total_qty(self) -> float:
        return q8(sum(q for _, q in self.entries))

    def avg_price(self) -> float:
        q = self.total_qty
        if q <= 0: return 0.0
        v = sum(p * q for p, q in self.entries)
        return v / q

    def add_entry(self, price: float, qty: float):
        if qty > 0: self.entries.append((float(price), q8(qty)))

    def reduce_qty(self, qty: float):
        rem = q8(qty)
        new_entries: List[Tuple[float,float]] = []
        for p, q in self.entries:
            if rem <= 0:
                new_entries.append((p, q)); continue
            take = min(q, rem)
            left = q8(q - take)
            rem = q8(rem - take)
            if left > 0:
                new_entries.append((p, left))
        self.entries = new_entries

    def clear(self):
        self.entries = []
        self.tp1_done = False
        self.trailing_stop = None

    def to_dict(self) -> Dict[str, Any]:
        return {"entries": self.entries, "tp1_done": self.tp1_done, "trailing_stop": self.trailing_stop}

# state I/O

def load_positions() -> Dict[str, Position]:
    if not os.path.exists(STATE_PATH):
        return {ticker: Position() for ticker in TICKERS}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        out: Dict[str, Position] = {}
        for ticker in TICKERS:
            if ticker in data:
                d = data[ticker]
                p = Position()
                p.entries = [tuple(x) for x in d.get("entries", [])]
                p.tp1_done = bool(d.get("tp1_done", False))
                p.trailing_stop = d.get("trailing_stop", None)
                out[ticker] = p
            else:
                out[ticker] = Position()
        logging.info("State file loaded successfully.")
        return out
    except Exception as e:
        logging.error(f"State file loading failed: {e}. Init empty.")
        return {ticker: Position() for ticker in TICKERS}

def save_positions(pos_map: Dict[str, Position]):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({t: p.to_dict() for t, p in pos_map.items()}, f, indent=2)
    os.replace(tmp, STATE_PATH)

positions = load_positions()
pos_lock = threading.Lock()

def on_exit():
    with pos_lock:
        save_positions(positions)
    logging.info("[exit] All positions saved.")

atexit.register(on_exit)

# ==============================================================================
# 6) Upbit objects & wrappers
# ==============================================================================
upbit = pyupbit.Upbit(ACCESS, SECRET)

# balances cache
_cached_balances: Any = None
_cache_time = 0.0

def _normalize_balances(bals: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if bals is None: return out
    def as_dict(x: Any) -> Optional[Dict[str, Any]]:
        if isinstance(x, dict): return x
        if isinstance(x, str):
            try:
                d = {}
                for kv in x.split(","):
                    if ":" in kv:
                        k, v = kv.split(":", 1)
                        d[k.strip()] = v.strip()
                return d
            except Exception:
                return None
        return None
    if isinstance(bals, list):
        for it in bals:
            d = as_dict(it)
            if d: out.append(d)
    elif isinstance(bals, dict):
        out.append(bals)
    elif isinstance(bals, str):
        d = as_dict(bals)
        if d: out.append(d)
    return out

def get_all_balances(force_refresh=False) -> List[Dict[str, Any]]:
    global _cached_balances, _cache_time
    now = time.time()
    if (not force_refresh) and _cached_balances and (now - _cache_time < 1.0):
        return _cached_balances
    try:
        bals = upbit.get_balances()
        norm = _normalize_balances(bals)
        _cached_balances = norm
        _cache_time = now
        return norm
    except Exception as e:
        logging.warning(f"Failed to get balances: {e}")
        return _cached_balances or []

def get_krw_balance() -> float:
    try:
        v = upbit.get_balance("KRW")
        if v is not None:
            return float(v or 0.0)
    except Exception:
        pass
    for b in get_all_balances():
        if str(b.get("currency", "")).upper() == "KRW":
            return float(b.get("balance", 0.0))
    return 0.0

def get_coin_balance(ticker: str) -> float:
    coin = ticker.split("-")[1].upper()
    for b in get_all_balances():
        if str(b.get("currency", "")).upper() == coin:
            free = float(b.get("balance", 0.0))
            locked = float(b.get("locked", 0.0))
            return free + locked
    return 0.0

def get_orderbooks_compat(tickers: List[str]) -> Optional[List[Dict]]:
    if not tickers:
        return []
    def _normalize(resp) -> List[Dict]:
        items = resp if isinstance(resp, (list, tuple)) else [resp] if isinstance(resp, dict) else []
        out = []
        for it in items:
            try:
                market = it.get('market') or it.get('code') or it.get('symbol') or tickers[0]
                units = it.get('orderbook_units') or it.get('orderbookUnits') or []
                norm_units = []
                for u in units:
                    ask = u.get('ask_price', u.get('askPrice'))
                    bid = u.get('bid_price', u.get('bidPrice'))
                    if ask is None or bid is None:
                        continue
                    norm_units.append({'ask_price': float(ask), 'bid_price': float(bid)})
                if norm_units:
                    out.append({'market': market, 'orderbook_units': norm_units})
            except Exception:
                continue
        return out
    def _try_calls():
        yield lambda: pyupbit.get_orderbook(tickers=tickers)
        yield lambda: pyupbit.get_orderbook(ticker=tickers)
        if len(tickers) == 1:
            t = tickers[0]
            yield lambda: pyupbit.get_orderbook(ticker=t)
            yield lambda: pyupbit.get_orderbook(tickers=[t])
            yield lambda: pyupbit.get_orderbook(t)
            yield lambda: pyupbit.get_orderbook([t])
    for _ in range(3):
        for call in _try_calls():
            try:
                raw = call()
                if not raw:
                    continue
                obs = _normalize(raw)
                if obs:
                    return obs
            except TypeError:
                continue
            except Exception as e:
                logging.warning(f"[WARN] get_orderbooks compat error: {e}", exc_info=True)
                break
        time.sleep(0.2)
    return None

def get_ohlc_safe(ticker: str, interval="minute1", count=200) -> Optional[pd.DataFrame]:
    try:
        df = pyupbit.get_ohlcv(ticker, interval=interval, count=count)
        if df is None or df.empty:
            logging.warning(f"[{ticker}] get_ohlcv returned empty")
            return None
        return df
    except Exception as e:
        logging.warning(f"[{ticker}] get_ohlcv error: {e}")
        return None

# --- Fill reconciliation ---

def fetch_fill_info(uuid: Optional[str]) -> Tuple[float, Optional[float]]:
    """Return (filled_qty, avg_price or None) using trades list."""
    if not uuid: return (q8(0.0), None)
    for _ in range(8):  # ~4s
        try:
            od = upbit.get_order(uuid)
            if isinstance(od, dict):
                trades = od.get('trades', [])
                if trades:
                    tot_q = 0.0
                    tot_v = 0.0
                    for t in trades:
                        v = float(t.get('volume', 0.0))
                        p = float(t.get('price', 0.0))
                        tot_q += v; tot_v += v * p
                    if tot_q > 0:
                        return q8(tot_q), (tot_v / tot_q)
                state = od.get('state')
                if state in ['done', 'cancel'] and not trades:
                    return q8(0.0), None
            time.sleep(0.5)
        except Exception as e:
            logging.warning(f"[WARN] get_order uuid={uuid} err: {e}", exc_info=True)
            time.sleep(0.5)
    return q8(0.0), None

# ==============================================================================
# 7) Indicators
# ==============================================================================

def compute_indicators_1m_sync(df1m, ATR_PERIOD, K, RSI_PERIOD, MA_SHORT, MA_LONG):
    df = df1m.copy(); d = df['close'].diff()
    df['H-L']  = df['high'] - df['low']
    df['H-PC'] = (df['high'] - df['close'].shift(1)).abs()
    df['L-PC'] = (df['low']  - df['close'].shift(1)).abs()
    df['TR']   = df[['H-L','H-PC','L-PC']].max(axis=1)
    df['ATR']  = df['TR'].rolling(ATR_PERIOD).mean()
    df['target'] = df['open'] + df['ATR'].shift(1) * K
    gain = d.clip(lower=0).rolling(RSI_PERIOD).mean()
    loss = (-d.clip(upper=0)).rolling(RSI_PERIOD).mean().replace(0, 1e-12)
    df['rsi'] = 100 - 100 / (1 + gain / loss)
    df['ma_s'] = df['close'].rolling(MA_SHORT).mean()
    df['ma_l'] = df['close'].rolling(MA_LONG).mean()
    df.dropna(inplace=True); return df

def ema15_on_1min_sync(df1m_index, series_15m_close, span=20):
    s15 = series_15m_close.tz_localize(None); s1m = s15.resample('1min').ffill()
    ema1m = s1m.ewm(span=span, adjust=False).mean()
    return ema1m.reindex(df1m_index, method='ffill')

# ==============================================================================
# 8) Recon / mismatch measure
# ==============================================================================

def mismatch_krw(ticker: str, internal_qty: float) -> Tuple[float, float]:
    ob = get_orderbooks_compat([ticker]) or []
    bid = float(ob[0]['orderbook_units'][0]['bid_price']) if ob else 0.0
    exch_qty = get_coin_balance(ticker)
    gap_krw = abs(exch_qty - internal_qty) * bid
    return gap_krw, exch_qty

# ==============================================================================
# 9) Heartbeat watchdog (daemon thread)
# ==============================================================================

def start_heartbeat_watchdog():
    def _run():
        while True:
            try:
                log_msg = f"[hb-wd {now_kst().strftime('%H:%M:%S')}]"
                with pos_lock:
                    for ticker, pos in positions.items():
                        if pos.total_qty > 0:
                            log_msg += f" | {ticker}: {pos.total_qty:.6f} (avg:{pos.avg_price():.0f}, ts:{(pos.trailing_stop or 0):.0f}, tp1:{pos.tp1_done})"
                logging.info(log_msg)
            except Exception:
                pass
            time.sleep(HB_WATCHDOG_SEC)
    threading.Thread(target=_run, daemon=True).start()

# ==============================================================================
# 10) Main loop
# ==============================================================================

def main_loop():
    logging.info("🚀 Starting multi-coin trading bot...")
    logging.info(f"Tracking {len(TICKERS)} tickers: {TICKERS}")
    hb_t0 = time.time()

    first_loop = True

    while True:
        try:
            if INTERVAL == "minute1":
                if first_loop:
                    first_loop = False
                else:
                    sleep_to_next_minute(offset_sec=1.0)
            else:
                time.sleep(1.0)

            # Heartbeat at loop head
            if time.time() - hb_t0 >= HEARTBEAT_SEC:
                log_msg = f"[hb {now_kst().strftime('%H:%M:%S')}]"
                with pos_lock:
                    for ticker, pos in positions.items():
                        if pos.total_qty > 0:
                            log_msg += f" | {ticker}: {pos.total_qty:.6f} (avg:{pos.avg_price():.0f}, ts:{(pos.trailing_stop or 0):.0f}, tp1:{pos.tp1_done})"
                logging.info(log_msg)
                hb_t0 = time.time()

            # --- BUY window (per minute) ---
            available_krw = get_krw_balance()
            reserved_krw = 0.0

            for ticker in TICKERS:
                tick_deadline = time.time() + TICKER_TIME_BUDGET
                try:
                    if time.time() > tick_deadline:
                        logging.warning(f"[{ticker}] time budget exceeded before data fetch → skip")
                        continue

                    df_1m_raw = get_ohlc_safe(ticker, "minute1", max(200, WARMUP + 10))
                    if df_1m_raw is None: continue

                    if time.time() > tick_deadline:
                        logging.warning(f"[{ticker}] time budget exceeded after ohlcv → skip")
                        continue

                    df_ind = compute_indicators_1m_sync(df_1m_raw, ATR_PERIOD, K, RSI_PERIOD, MA_SHORT, MA_LONG)
                    df_15m_close = df_1m_raw['close'].resample('15min').last()
                    ema15_sync = ema15_on_1min_sync(df_ind.index, df_15m_close, span=EMA15_EMA_SPAN)

                    row_prev = df_ind.iloc[-2]
                    close_prev = float(row_prev['close'])
                    tgt_up = round_price(float(row_prev['target']), "up")
                    buy_cond = (
                        (close_prev > tgt_up) and
                        (float(row_prev['rsi']) < RSI_THRESHOLD) and
                        (float(row_prev['ma_s']) > float(row_prev['ma_l'])) and
                        (close_prev > float(ema15_sync.iloc[-2]))
                    )
                    if not buy_cond: continue

                    with pos_lock:
                        current_pos = positions[ticker]
                    allow_reentry = (
                        (current_pos.total_qty <= 0) or
                        (REENTRY_TH is None) or
                        (close_prev > current_pos.avg_price() * (1.0 + REENTRY_TH))
                    )
                    if not allow_reentry: continue

                    if time.time() > tick_deadline:
                        logging.warning(f"[{ticker}] time budget exceeded before order sizing → skip")
                        continue

                    cash_for_this_trade = max(0.0, available_krw - reserved_krw)
                    buy_cap = cash_for_this_trade * BUY_PCT
                    if BUY_KRW_CAP is not None: buy_cap = min(buy_cap, BUY_KRW_CAP)
                    buy_amt = buy_cap * (1 - FEE)
                    if buy_amt < MIN_KRW_ORDER: continue

                    # Pre snapshot
                    pre_coin = get_coin_balance(ticker)
                    reserved_krw += buy_amt

                    ob_now = get_orderbooks_compat([ticker]) or []
                    if ob_now:
                        ask_px = ob_now[0]['orderbook_units'][0]['ask_price']
                        entry_px = round_price(float(ask_px), "up")
                    else:
                        entry_px = round_price(close_prev, "up")

                    if time.time() > tick_deadline:
                        logging.warning(f"[{ticker}] time budget exceeded before placing order → skip")
                        continue

                    order = safe_call(upbit.buy_market_order, ticker, buy_amt)
                    uuid = order.get('uuid') if isinstance(order, dict) else None

                    filled, avg_fill_px = fetch_fill_info(uuid)
                    if filled <= 0:
                        # balance delta fallback (~3s window)
                        t_end = time.time() + 3.0
                        while time.time() < t_end and filled <= 0:
                            post_coin = get_coin_balance(ticker)
                            delta = q8(post_coin - pre_coin)
                            if delta > 0:
                                filled = delta
                                if not avg_fill_px and filled > 0:
                                    avg_fill_px = buy_amt / filled
                                break
                            time.sleep(0.3)

                    if filled <= 0:
                        est = (buy_amt / max(entry_px, 1e-8)) * (1.0 - FEE)
                        logging.warning(f"[{ticker}] BUY fill uncertain → using estimate {est:.8f}")
                        filled = est

                    if avg_fill_px:
                        entry_px = round_price(float(avg_fill_px), "up")

                    qty = q8(filled)
                    if qty > 0:
                        with pos_lock:
                            positions[ticker].add_entry(entry_px, qty)
                            step0 = TRAILING_STEP_AFTER_TP1 if positions[ticker].tp1_done else TRAILING_STEP
                            ts0 = round_price(entry_px * (1 - step0), "down")
                            positions[ticker].trailing_stop = max(positions[ticker].trailing_stop or 0.0, ts0)
                            save_positions(positions)
                        logging.info(f"[{ticker}] BUY krw={buy_amt:.0f} qty={qty} px={entry_px} → INIT TS={ts0:.0f}")

                    # one-time trail up using previous bar high
                    with pos_lock:
                        if positions[ticker].total_qty > 0:
                            step_now = TRAILING_STEP_AFTER_TP1 if positions[ticker].tp1_done else TRAILING_STEP
                            ts_bar = round_price(float(row_prev['high']) * (1 - step_now), "down")
                            if ts_bar > (positions[ticker].trailing_stop or 0.0):
                                positions[ticker].trailing_stop = ts_bar
                                save_positions(positions)
                                logging.info(f"[{ticker}] BAR TRAIL UP → {ts_bar:.0f}")

                except Exception as e:
                    logging.error(f"Error processing BUY for {ticker}: {e}", exc_info=True)
                    time.sleep(0.5)

            # --- FAST loop (TP/TS) ---
            next_min = (int(time.time() // 60) + 1) * 60
            deadline = next_min - 0.5

            while time.time() < deadline:
                try:
                    tickers_in_position = []
                    with pos_lock:
                        for ticker, pos in positions.items():
                            if pos.total_qty > 0:
                                tickers_in_position.append(ticker)

                    if not tickers_in_position:
                        break

                    orderbooks = get_orderbooks_compat(tickers_in_position)
                    if not orderbooks:
                        time.sleep(FAST_POLL); continue

                    acted_any = False

                    for ob in orderbooks:
                        ticker = ob['market']
                        bid_price = float(ob['orderbook_units'][0]['bid_price'])

                        # recon guard
                        with pos_lock:
                            internal_q = positions[ticker].total_qty
                        gap_krw, exch_q = mismatch_krw(ticker, internal_q)
                        if gap_krw >= RECON_WARN_KRW:
                            logging.warning(f"[{ticker}] recon mismatch: internal={internal_q:.8f}, exch={exch_q:.8f} (~{gap_krw:.0f} KRW) → skip TP/TS this tick")
                            step_tmp = TRAILING_STEP_AFTER_TP1 if positions[ticker].tp1_done else TRAILING_STEP
                            new_ts_tmp = round_price(bid_price * (1 - step_tmp), "down")
                            with pos_lock:
                                if new_ts_tmp > (positions[ticker].trailing_stop or 0.0):
                                    positions[ticker].trailing_stop = new_ts_tmp
                                    save_positions(positions)
                            continue

                        with pos_lock:
                            pos_to_check = positions[ticker]
                            avg = pos_to_check.avg_price()
                            tp1_done = pos_to_check.tp1_done
                            cur_ts = pos_to_check.trailing_stop
                            q_total = pos_to_check.total_qty

                        if q_total <= 0:
                            continue

                        # intrabar trail up
                        step = TRAILING_STEP_AFTER_TP1 if tp1_done else TRAILING_STEP
                        new_ts = round_price(bid_price * (1 - step), "down")
                        if new_ts > (cur_ts or 0.0):
                            with pos_lock:
                                if new_ts > (positions[ticker].trailing_stop or 0.0):
                                    positions[ticker].trailing_stop = new_ts
                                    save_positions(positions)
                            logging.info(f"[{ticker}] FAST TRAIL UP @ {new_ts:.0f}")
                            cur_ts = new_ts

                        acted = False

                        # TP1
                        if (not tp1_done) and (bid_price >= avg * (1 + TP1_RATE)):
                            with pos_lock:
                                q_total = positions[ticker].total_qty
                            q1_raw = safe_qty(q_total * TP1_PCT, q_total)
                            if (q_total - q1_raw) < min_sell_qty(bid_price, MIN_KRW_ORDER):
                                q1 = safe_qty(max(0.0, q_total - min_sell_qty(bid_price, MIN_KRW_ORDER)), q_total)
                            else:
                                q1 = q1_raw

                            if can_sell(q1, bid_price, MIN_KRW_ORDER) and q1 > 0:
                                try:
                                    od = safe_call(upbit.sell_market_order, ticker, q1)
                                    uuid = od.get('uuid') if isinstance(od, dict) else None
                                    filled_qty, _ = fetch_fill_info(uuid)
                                except Exception as e:
                                    logging.error(f"[{ticker}] TP1 SELL error: {e}", exc_info=True)
                                    filled_qty = 0.0

                                if filled_qty > 0:
                                    with pos_lock:
                                        positions[ticker].reduce_qty(filled_qty)
                                        positions[ticker].tp1_done = True
                                        be_floor = round_price(avg * (1 + FEE*2 + 0.001), "up")
                                        positions[ticker].trailing_stop = max(positions[ticker].trailing_stop or 0.0, be_floor)
                                        save_positions(positions)
                                        remain = positions[ticker].total_qty
                                    logging.info(f"[{ticker}] TP1 SELL filled={filled_qty:.8f} remain={remain:.8f} → BE TS={positions[ticker].trailing_stop:.0f}")
                                    tp1_done = True
                                    acted = True

                        with pos_lock:
                            q_total = positions[ticker].total_qty
                            tp1_done = positions[ticker].tp1_done
                            avg = positions[ticker].avg_price()
                            cur_ts = positions[ticker].trailing_stop

                        if q_total <= 0:
                            acted_any = acted_any or acted
                            continue

                        # TP2
                        if tp1_done and (bid_price >= avg * (1 + TP2_RATE)):
                            qty_all = q_total
                            if can_sell(qty_all, bid_price, MIN_KRW_ORDER) and qty_all > 0:
                                try:
                                    od = safe_call(upbit.sell_market_order, ticker, qty_all)
                                    uuid = od.get('uuid') if isinstance(od, dict) else None
                                    filled, _ = fetch_fill_info(uuid)
                                except Exception as e:
                                    logging.error(f"[{ticker}] TP2 SELL error: {e}", exc_info=True)
                                    filled = 0.0

                                if filled > 0:
                                    with pos_lock:
                                        if filled >= qty_all * 0.999:
                                            positions[ticker].clear()
                                            save_positions(positions)
                                            logging.info(f"[{ticker}] TP2 SELL qty={qty_all:.8f} (ALL)")
                                            acted = True
                                        else:
                                            positions[ticker].reduce_qty(filled)
                                            save_positions(positions)
                                            logging.info(f"[{ticker}] TP2 PARTIAL filled={filled:.8f}, remain={positions[ticker].total_qty:.8f}")
                                            acted = True

                        with pos_lock:
                            q_total = positions[ticker].total_qty
                            cur_ts = positions[ticker].trailing_stop

                        if q_total <= 0:
                            acted_any = acted_any or acted
                            continue

                        # TS
                        if (cur_ts is not None) and (bid_price <= cur_ts):
                            qty_all = q_total
                            if can_sell(qty_all, bid_price, MIN_KRW_ORDER) and qty_all > 0:
                                try:
                                    od = safe_call(upbit.sell_market_order, ticker, qty_all)
                                    uuid = od.get('uuid') if isinstance(od, dict) else None
                                    filled, _ = fetch_fill_info(uuid)
                                except Exception as e:
                                    logging.error(f"[{ticker}] TS SELL error: {e}", exc_info=True)
                                    filled = 0.0

                                if filled > 0:
                                    with pos_lock:
                                        if filled >= qty_all * 0.999:
                                            positions[ticker].clear()
                                            save_positions(positions)
                                            logging.info(f"[{ticker}] TS SELL qty={qty_all:.8f} (ALL)")
                                            acted = True
                                        else:
                                            positions[ticker].reduce_qty(filled)
                                            save_positions(positions)
                                            logging.info(f"[{ticker}] TS PARTIAL filled={filled:.8f}, remain={positions[ticker].total_qty:.8f}")
                                            acted = True

                        acted_any = acted_any or acted

                    time.sleep(max(0.05, FAST_POLL if not acted_any else max(1, FAST_POLL // 2)))

                except Exception as e:
                    logging.error(f"Error in FAST loop: {e}", exc_info=True)
                    time.sleep(FAST_POLL)

            # Heartbeat at loop tail
            if time.time() - hb_t0 >= HEARTBEAT_SEC:
                log_msg = f"[hb {now_kst().strftime('%H:%M:%S')}]"
                with pos_lock:
                    for ticker, pos in positions.items():
                        if pos.total_qty > 0:
                            log_msg += f" | {ticker}: {pos.total_qty:.6f} (avg:{pos.avg_price():.0f}, ts:{(pos.trailing_stop or 0):.0f}, tp1:{pos.tp1_done})"
                logging.info(log_msg)
                hb_t0 = time.time()

        except KeyboardInterrupt:
            logging.info("[kill] interrupted by user")
            break
        except Exception as e:
            logging.error("Main loop error", exc_info=True)
            time.sleep(1.0)

# ==============================================================================
# 11) Main
# ==============================================================================
if __name__ == "__main__":
    setup_logging()
    start_heartbeat_watchdog()
    atexit.register(on_exit)
    main_loop()
