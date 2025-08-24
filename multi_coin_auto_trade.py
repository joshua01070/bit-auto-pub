# === multi_coin_auto_trade.py ===
# bitcoinAutoTrade_min_v4_fin_patched.py를 기반으로 한 멀티 코인 실거래 버전
# [패치] TP1/TP2/TS 실제 매도 로직 이식 + fetch_filled_volume 체결량 즉시 반영
# [추가패치] acted_any 누적 버그 픽스, get_orderbooks_compat 호환 래퍼 도입

import os, sys, time, json, math, atexit, logging, threading
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List, Dict

import pandas as pd
import pyupbit

# ==============================================================================
# 1단계: 설정 및 상태 관리 구조
# ==============================================================================

# ========== 사용자 설정 ==========
ACCESS = ""
SECRET = ""

# --- 거래할 코인 목록 ---
TICKERS = ["KRW-BTC", "KRW-ETH"]  # 원하는 KRW 마켓 티커 추가

# 시그널/전략 파라미터 (모든 코인에 공통 적용)
K = 0.25
ATR_PERIOD = 14
RSI_PERIOD = 14
RSI_THRESHOLD = 39
MA_SHORT, MA_LONG = 5, 30
EMA15_EMA_SPAN = 20

# TP/TS/수수료 설정 (모든 코인에 공통 적용)
FEE = 0.0005
TP1_RATE, TP2_RATE = 0.0155, 0.04
TP1_PCT = 0.50
TRAILING_STEP = 0.08
TRAILING_STEP_AFTER_TP1 = 0.035
REENTRY_TH = 0.005

# 운영 설정
INTERVAL = "minute1"
WARMUP = max(ATR_PERIOD, RSI_PERIOD, MA_LONG) + 2
FAST_POLL = 5  # FAST 루프 폴링 간격(초)
STATE_PATH = "multi_state.json"
MIN_KRW_ORDER = 6000
BUY_PCT = 0.40  # 한 번에 매수할 최대 비율
BUY_KRW_CAP = None

LOG_LEVEL = logging.INFO
HEARTBEAT_SEC = 60
KST = timezone(timedelta(hours=9))

# ========== 로깅 ==========
def setup_logging():
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

# ========== 유틸 ==========
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
    delta = max(0.0, next_min - now)
    time.sleep(delta)

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

# --- 체결/수량 유틸 ---
def can_sell(qty: float, price: float, min_krw_order: float = MIN_KRW_ORDER) -> bool:
    return (qty * price) >= min_krw_order

def min_sell_qty(price: float, min_krw_order: float = MIN_KRW_ORDER) -> float:
    return q8(min_krw_order / max(price, 1e-8))

def safe_qty(qty: float, avail: float) -> float:
    return q8(max(0.0, min(qty, avail)))

# ========== 포지션 ==========
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

    def to_dict(self) -> Dict:
        return {
            "entries": self.entries,
            "tp1_done": self.tp1_done,
            "trailing_stop": self.trailing_stop
        }

# --- 멀티 코인 상태 관리 ---
def load_positions() -> Dict[str, Position]:
    if not os.path.exists(STATE_PATH):
        return {ticker: Position() for ticker in TICKERS}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        loaded_positions = {}
        for ticker in TICKERS:
            if ticker in data:
                pos_data = data[ticker]
                pos = Position()
                pos.entries = [tuple(x) for x in pos_data.get("entries", [])]
                pos.tp1_done = bool(pos_data.get("tp1_done", False))
                pos.trailing_stop = pos_data.get("trailing_stop", None)
                loaded_positions[ticker] = pos
            else:
                loaded_positions[ticker] = Position()
        logging.info("State file loaded successfully.")
        return loaded_positions
    except Exception as e:
        logging.error(f"State file loading failed: {e}. Initializing with empty positions.")
        return {ticker: Position() for ticker in TICKERS}

def save_positions(positions_to_save: Dict[str, Position]):
    data_to_save = {ticker: pos.to_dict() for ticker, pos in positions_to_save.items()}
    tmp_path = STATE_PATH + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data_to_save, f, indent=2)
    os.replace(tmp_path, STATE_PATH)

# ==============================================================================
# 2단계: 잔고 및 호가 조회
# ==============================================================================
upbit = pyupbit.Upbit(ACCESS, SECRET)

# --- 효율적인 잔고 조회 ---
_cached_balances = None
_cache_time = 0

def get_all_balances(force_refresh=False) -> Optional[List[Dict]]:
    global _cached_balances, _cache_time
    now = time.time()
    if not force_refresh and _cached_balances and (now - _cache_time < 1):
        return _cached_balances
    try:
        balances = upbit.get_balances()
        _cached_balances = balances
        _cache_time = now
        return balances
    except Exception as e:
        logging.warning(f"Failed to get balances: {e}")
        return None

def get_krw_balance() -> float:
    balances = get_all_balances()
    if balances:
        for b in balances:
            if b['currency'] == 'KRW':
                return float(b.get('balance', 0.0))
    return 0.0

def get_coin_balance(ticker: str) -> float:
    coin_currency = ticker.split('-')[1]
    balances = get_all_balances()
    if balances:
        for b in balances:
            if b['currency'] == coin_currency:
                return float(b.get('balance', 0.0)) + float(b.get('locked', 0.0))
    return 0.0

# --- (교체) pyupbit 호환 멀티 호가 래퍼 ---
def get_orderbooks_compat(tickers: List[str]) -> Optional[List[Dict]]:
    """
    다양한 pyupbit 버전/반환 형태(ticker/tickers, list/dict, orderbook_units/orderbookUnits)를
    일관된 형태 [{'market': str, 'orderbook_units': [{'ask_price': float,'bid_price': float}, ...]}]로 정규화.
    """
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
        # 다중 우선 시도
        yield lambda: pyupbit.get_orderbook(tickers=tickers)
        yield lambda: pyupbit.get_orderbook(ticker=tickers)
        # 단일로 폴백(호환)
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
                # 시그니처 불일치 재시도
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

# --- 체결 조회: uuid → 실제 체결 수량 합산 ---
def fetch_filled_volume(uuid: Optional[str]) -> float:
    if not uuid: return q8(0.0)
    for _ in range(5):
        try:
            od = upbit.get_order(uuid)
            if isinstance(od, dict):
                state = od.get('state')
                trades = od.get('trades', [])
                if trades:
                    filled = sum(float(t.get('volume', 0.0)) for t in trades)
                    return q8(filled)
                if state in ['done', 'cancel']:
                    return q8(0.0)
            time.sleep(0.5)
        except Exception as e:
            logging.warning(f"[WARN] get_order uuid={uuid} err: {e}", exc_info=True)
            time.sleep(0.5)
    return q8(0.0)

# ==============================================================================
# 3/4/5단계: 메인 로직 (루프, 자금관리, FAST 등)
# ==============================================================================

positions = load_positions()
pos_lock = threading.Lock()

def on_exit():
    with pos_lock:
        save_positions(positions)
    logging.info("[exit] All positions saved.")

atexit.register(on_exit)

# --- 지표 계산 ---
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

# --- 메인 루프 ---
def main_loop():
    logging.info("Starting multi-coin trading bot...")
    logging.info(f"Tracking {len(TICKERS)} tickers: {TICKERS}")
    hb_t0 = time.time()

    while True:
        sleep_to_next_minute(offset_sec=1.0)

        # --- 매수 신호 판단 및 주문 (분 단위) ---
        available_krw = get_krw_balance()
        reserved_krw = 0.0
        
        for ticker in TICKERS:
            try:
                # 1. 데이터 및 지표 계산
                df_1m_raw = get_ohlc_safe(ticker, "minute1", max(200, WARMUP + 10))
                if df_1m_raw is None: continue
                
                df_ind = compute_indicators_1m_sync(df_1m_raw, ATR_PERIOD, K, RSI_PERIOD, MA_SHORT, MA_LONG)
                df_15m_close = df_1m_raw['close'].resample('15min').last()
                ema15_sync = ema15_on_1min_sync(df_ind.index, df_15m_close, span=EMA15_EMA_SPAN)

                # 2. 신호 판단 (전 분 기준)
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
                
                # 3. 자금/재진입
                with pos_lock:
                    current_pos = positions[ticker]
                allow_reentry = (
                    (current_pos.total_qty <= 0) or
                    (REENTRY_TH is None) or
                    (close_prev > current_pos.avg_price() * (1.0 + REENTRY_TH))
                )
                if not allow_reentry: continue

                cash_for_this_trade = max(0.0, available_krw - reserved_krw)
                buy_cap = cash_for_this_trade * BUY_PCT
                if BUY_KRW_CAP is not None: buy_cap = min(buy_cap, BUY_KRW_CAP)
                buy_amt = buy_cap * (1 - FEE)
                if buy_amt < MIN_KRW_ORDER: continue

                reserved_krw += buy_amt # 자금 예약
                
                # 4. 매수 주문 실행 + 체결 수량 즉시 반영
                order = safe_call(upbit.buy_market_order, ticker, buy_amt)
                uuid = order.get('uuid') if isinstance(order, dict) else None
                filled = fetch_filled_volume(uuid)

                # 호가 기준 체결가 근사(안전하게 올림 라운딩)
                ob_now = get_orderbooks_compat([ticker]) or []
                if ob_now:
                    ask_px = ob_now[0]['orderbook_units'][0]['ask_price']
                    entry_px = round_price(float(ask_px), "up")
                else:
                    # fallback: 전 분 종가 근사
                    entry_px = round_price(close_prev, "up")

                if filled <= 0:
                    filled = (buy_amt / max(entry_px, 1e-8)) * (1.0 - FEE)

                qty = q8(filled)
                if qty > 0:
                    with pos_lock:
                        positions[ticker].add_entry(entry_px, qty)
                        # 매수 직후 TS 초기화 (백테스트 타이밍과 동일)
                        step0 = TRAILING_STEP_AFTER_TP1 if positions[ticker].tp1_done else TRAILING_STEP
                        ts0 = round_price(entry_px * (1 - step0), "down")
                        positions[ticker].trailing_stop = max(positions[ticker].trailing_stop or 0.0, ts0)
                        save_positions(positions)
                    logging.info(f"[{ticker}] BUY krw={buy_amt:.0f} qty={qty} px={entry_px} → INIT TS={ts0:.0f}")

                # BAR 기반 1회 트레일 상향 (전 분 high 기준)
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
                time.sleep(1)

        # --- TP/TS 관리 (FAST 루프) ---
        next_min = (int(time.time() // 60) + 1) * 60
        deadline = next_min - 0.5
        
        while time.time() < deadline:
            try:
                tickers_in_position = []
                with pos_lock:
                    for ticker, pos in positions.items():
                        if pos.total_qty > 0:
                            tickers_in_position.append(ticker)

                if not tickers_in_position:  # 보유 포지션 없으면 FAST 루프 종료
                    break

                orderbooks = get_orderbooks_compat(tickers_in_position)
                if not orderbooks:
                    time.sleep(FAST_POLL); continue

                acted_any = False  # ← 누적 플래그 (버그 픽스)

                for ob in orderbooks:
                    ticker = ob['market']
                    bid_price = float(ob['orderbook_units'][0]['bid_price'])

                    # 로컬 스냅샷
                    with pos_lock:
                        pos_to_check = positions[ticker]
                        avg = pos_to_check.avg_price()
                        tp1_done = pos_to_check.tp1_done
                        cur_ts = pos_to_check.trailing_stop
                        q_total = pos_to_check.total_qty

                    if q_total <= 0:
                        continue

                    # 1) intrabar 트레일 상향
                    step = TRAILING_STEP_AFTER_TP1 if tp1_done else TRAILING_STEP
                    new_ts = round_price(bid_price * (1 - step), "down")
                    if new_ts > (cur_ts or 0.0):
                        with pos_lock:
                            if new_ts > (positions[ticker].trailing_stop or 0.0):
                                positions[ticker].trailing_stop = new_ts
                                save_positions(positions)
                        logging.info(f"[{ticker}] FAST TRAIL UP @ {new_ts:.0f}")
                        cur_ts = new_ts  # 최신값 반영

                    acted = False  # per-ticker

                    # 2) TP1 분할 익절
                    if (not tp1_done) and (bid_price >= avg * (1 + TP1_RATE)):
                        with pos_lock:
                            q_total = positions[ticker].total_qty
                        q1_raw = safe_qty(q_total * TP1_PCT, q_total)
                        # 남길 잔량이 최소매도금액보다 작아지는 경우 보정
                        if (q_total - q1_raw) < min_sell_qty(bid_price, MIN_KRW_ORDER):
                            q1 = safe_qty(max(0.0, q_total - min_sell_qty(bid_price, MIN_KRW_ORDER)), q_total)
                        else:
                            q1 = q1_raw

                        if can_sell(q1, bid_price, MIN_KRW_ORDER) and q1 > 0:
                            try:
                                od = safe_call(upbit.sell_market_order, ticker, q1)
                                uuid = od.get('uuid') if isinstance(od, dict) else None
                                filled_qty = fetch_filled_volume(uuid)
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

                    # 최신 스냅샷
                    with pos_lock:
                        q_total = positions[ticker].total_qty
                        tp1_done = positions[ticker].tp1_done
                        avg = positions[ticker].avg_price()
                        cur_ts = positions[ticker].trailing_stop

                    if q_total <= 0:
                        acted_any = acted_any or acted
                        continue

                    # 3) TP2 전량 익절
                    if tp1_done and (bid_price >= avg * (1 + TP2_RATE)):
                        qty_all = q_total
                        if can_sell(qty_all, bid_price, MIN_KRW_ORDER) and qty_all > 0:
                            try:
                                od = safe_call(upbit.sell_market_order, ticker, qty_all)
                                uuid = od.get('uuid') if isinstance(od, dict) else None
                                filled = fetch_filled_volume(uuid)
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

                    # 최신 스냅샷
                    with pos_lock:
                        q_total = positions[ticker].total_qty
                        cur_ts = positions[ticker].trailing_stop

                    if q_total <= 0:
                        acted_any = acted_any or acted
                        continue

                    # 4) 트레일 스탑 청산
                    if (cur_ts is not None) and (bid_price <= cur_ts):
                        qty_all = q_total
                        if can_sell(qty_all, bid_price, MIN_KRW_ORDER) and qty_all > 0:
                            try:
                                od = safe_call(upbit.sell_market_order, ticker, qty_all)
                                uuid = od.get('uuid') if isinstance(od, dict) else None
                                filled = fetch_filled_volume(uuid)
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

                    acted_any = acted_any or acted  # 누적

                # 폴링 템포: 하나라도 액션 있었으면 빠르게 (버그 픽스 반영)
                time.sleep(max(0.05, FAST_POLL if not acted_any else max(1, FAST_POLL // 2)))

            except Exception as e:
                logging.error(f"Error in FAST loop: {e}", exc_info=True)
                time.sleep(FAST_POLL)

        # 하트비트 로그
        if time.time() - hb_t0 >= HEARTBEAT_SEC:
            log_msg = f"[hb {now_kst().strftime('%H:%M:%S')}]"
            with pos_lock:
                for ticker, pos in positions.items():
                    if pos.total_qty > 0:
                        log_msg += f" | {ticker}: {pos.total_qty:.6f} (avg:{pos.avg_price():.0f}, ts:{(pos.trailing_stop or 0):.0f}, tp1:{pos.tp1_done})"
            logging.info(log_msg)
            hb_t0 = time.time()

if __name__ == "__main__":
    setup_logging()
    main_loop()
