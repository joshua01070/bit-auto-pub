# === bitcoinAutoTrade_v4_fin_patched.py ===
# 백테스트(v4_fin)와 진입 로직 동기화를 위해 4가지 사항이 수정된 버전 + 트레일 동기화 보완(A,B)
# 1. 지표 계산(TR/ATR/target)을 백테스트와 동일하게 변경
# 2. EMA15 계산 파이프라인을 백테스트와 동일하게 변경
# 3. 전 분(t-2) 기준으로 진입 조건 판정 + target 라운딩(올림) 적용
# 4. 재진입(피라미딩) 기준을 실시간 호가(ask) → 전 분 종가(close_prev)로 변경
# ++ 패치A: 매수 직후 트레일링 스탑 초기화(백테스트 타이밍과 동일)
# ++ 패치B: 전 분 고가 기반 1회 트레일 상향(TRAIL_UP_BAR) 반영
# ++ 패치C: 체결량 0 반환 시 q8(0.0)로 일관화
# ++ 패치D: 신호 성분 디버그 로그 추가

import os, sys, time, json, math, atexit, logging, threading
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List, Dict

import pandas as pd
import pyupbit

# ========== 사용자 설정 ==========
ACCESS = ""
SECRET = ""

TICKER = "KRW-BTC"

# 시그널/전략 파라미터
K = 0.35
ATR_PERIOD = 14
RSI_PERIOD = 14
RSI_THRESHOLD = 45
MA_SHORT, MA_LONG = 3, 30

FEE = 0.0005
TP1_RATE, TP2_RATE = 0.0175, 0.04
TP1_PCT = 0.50

TRAILING_STEP = 0.07
TRAILING_STEP_AFTER_TP1 = 0.035

INTERVAL = "minute1"
POLL_SECONDS = 20
WARMUP = max(ATR_PERIOD, RSI_PERIOD, MA_LONG) + 2
FAST_POLL = 5

STATE_PATH = "state.json"
MIN_KRW_ORDER = 6000

BUY_PCT = 0.30
BUY_KRW_CAP = None
REENTRY_TH = 0.005                 # None이면 재진입 필터 OFF(=재진입 허용)
# (선택) 총 포지션 상한(원): None이면 제한 없음
MAX_POSITION_KRW = None

EMA15_EMA_SPAN = 20

LOG_LEVEL = logging.INFO
HEARTBEAT_SEC = 60
CRIT_ERR_THRESHOLD = 10

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
    """Upbit KRW 틱사이즈(간략화)"""
    if p >= 2_000_000: return 1000
    if p >= 1_000_000: return 500
    if p >=   500_000: return 100
    if p >=   100_000: return 50
    if p >=    10_000: return 10
    if p >=     1_000: return 5
    # 백테스트 코드와의 호환성을 위해 1000원 미만 상세화
    if p >=       100: return 1
    if p >=        10: return 0.1
    return 0.01

def round_price(p: float, direction: str = "nearest") -> float:
    unit = price_unit(p)
    if direction == "down": return math.floor(p / unit) * unit
    if direction == "up":   return math.ceil(p / unit) * unit
    return round(p / unit) * unit

def ceil_price(p: float) -> float:
    return round_price(p, "up")

def floor_price(p: float) -> float:
    return round_price(p, "down")

def sleep_safely(sec: float):
    end = time.time() + sec
    while True:
        left = end - time.time()
        if left <= 0: return
        time.sleep(min(1.0, left))

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

# ========== 거래/시세 ==========
upbit = pyupbit.Upbit(ACCESS, SECRET)

def get_ohlc_safe(interval="minute1", count=200) -> Optional[pd.DataFrame]:
    for _ in range(3):
        try:
            df = pyupbit.get_ohlcv(TICKER, interval=interval, count=count)
            if df is None or df.empty:
                logging.warning("[WARN] get_ohlcv returned empty")
                time.sleep(0.8); continue
            return df
        except Exception as e:
            logging.warning(f"[WARN] get_ohlcv error: {e}", exc_info=True)
            time.sleep(0.8)
    return None

def get_orderbook_best() -> Tuple[Optional[float], Optional[float]]:
    """Return (best_ask, best_bid) across pyupbit versions."""
    def _try_calls():
        yield lambda: pyupbit.get_orderbook(TICKER)
        yield lambda: pyupbit.get_orderbook(ticker=TICKER)
        yield lambda: pyupbit.get_orderbook([TICKER])
        yield lambda: pyupbit.get_orderbook(tickers=[TICKER])
    for _ in range(3):
        for call in _try_calls():
            try:
                ob = call()
                if not ob:
                    continue
                data = ob[0] if isinstance(ob, (list, tuple)) else ob
                units = data.get('orderbook_units') or data.get('orderbookUnits')
                if not units:
                    continue
                u0 = units[0]
                ask = u0.get('ask_price', u0.get('askPrice'))
                bid = u0.get('bid_price', u0.get('bidPrice'))
                if ask is None or bid is None:
                    continue
                return float(ask), float(bid)
            except TypeError:
                continue
            except Exception as e:
                logging.warning(f"[WARN] get_orderbook error: {e}", exc_info=True)
                time.sleep(0.3)
                break
        time.sleep(0.3)
    return None, None

def krw_balance() -> float:
    try:
        v = upbit.get_balance("KRW")
        return float(v or 0.0)
    except Exception as e:
        logging.warning(f"[WARN] get_balance(KRW) err: {e}")
        return 0.0

def btc_total_balance() -> float:
    """free + locked 합산 (리콘실리에이션용)"""
    try:
        bals = upbit.get_balances() or []
        for b in bals:
            if b.get('currency') == 'BTC':
                free = float(b.get('balance', 0.0))
                locked = float(b.get('locked', 0.0))
                return free + locked
        return 0.0
    except Exception as e:
        logging.warning(f"[recon] get_balances error: {e}", exc_info=True)
        return 0.0

def fetch_filled_volume(uuid: Optional[str]) -> float:
    """주문 uuid로 실제 체결량 조회 (최대 ~2.5s 대기)"""
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

# ========== 지표 ==========

# [수정 1] 백테스트와 동일한 1분 지표 계산 함수
def compute_indicators_1m_sync(df1m: pd.DataFrame, ATR_PERIOD: int, K: float, 
                               RSI_PERIOD: int, MA_SHORT: int, MA_LONG: int) -> pd.DataFrame:
    df = df1m.copy()
    d = df['close'].diff()

    # TR: 이전 종가 기준
    df['H-L']  = df['high'] - df['low']
    df['H-PC'] = (df['high'] - df['close'].shift(1)).abs()
    df['L-PC'] = (df['low']  - df['close'].shift(1)).abs()
    df['TR']   = df[['H-L','H-PC','L-PC']].max(axis=1)
    df['ATR']  = df['TR'].rolling(ATR_PERIOD).mean()

    # target: open + ATR.shift(1) * K  ← 반드시 shift(1)
    df['target'] = df['open'] + df['ATR'].shift(1) * K

    # RSI (단순 rolling, 소문자 컬럼명)
    gain = d.clip(lower=0).rolling(RSI_PERIOD).mean()
    loss = (-d.clip(upper=0)).rolling(RSI_PERIOD).mean().replace(0, 1e-12)
    df['rsi'] = 100 - 100 / (1 + gain / loss)

    # SMA
    df['ma_s'] = df['close'].rolling(MA_SHORT).mean()
    df['ma_l'] = df['close'].rolling(MA_LONG).mean()
    df.dropna(inplace=True)
    return df

# [수정 2] 백테스트와 동일한 EMA15 브릿지 함수
def ema15_on_1min_sync(df1m_index: pd.DatetimeIndex, 
                       series_15m_close: pd.Series, span: int) -> pd.Series:
    s15 = series_15m_close.tz_localize(None)         # 15분 close
    s1m = s15.resample('1min').ffill()               # → 1분으로 ffill
    ema1m = s1m.ewm(span=span, adjust=False).mean()  # → 1분 EMA
    return ema1m.reindex(df1m_index, method='ffill')

# ========== 상태/포지션 ==========
class Position:
    def __init__(self):
        self.entries: List[Tuple[float, float]] = []
        self.tp1_done: bool = False
        self.trailing_stop: Optional[float] = None
        self._err_count: int = 0

    @property
    def total_qty(self) -> float:
        return q8(sum(q for _, q in self.entries))

    def avg_price(self) -> float:
        q = self.total_qty
        if q <= 0:
            return 0.0
        v = sum(p * q for p, q in self.entries)
        return v / q

    def add_entry(self, price: float, qty: float):
        if qty <= 0:
            return
        self.entries.append((float(price), q8(qty)))

    def reduce_qty(self, qty: float):
        """FIFO로 줄임"""
        rem = q8(qty)
        new_entries: List[Tuple[float,float]] = []
        for p, q in self.entries:
            if rem <= 0:
                new_entries.append((p, q))
                continue
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

    def save(self):
        tmp = STATE_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f)
        os.replace(tmp, STATE_PATH)

    @classmethod
    def load(cls) -> "Position":
        if not os.path.exists(STATE_PATH):
            return cls()
        try:
            data = json.load(open(STATE_PATH, "r", encoding="utf-8"))
            obj = cls()
            obj.entries = [tuple(x) for x in data.get("entries", [])]
            obj.tp1_done = bool(data.get("tp1_done", False))
            obj.trailing_stop = data.get("trailing_stop", None)
            return obj
        except Exception:
            return cls()

position = Position.load()
pos_lock = threading.Lock()

# ========== 체결/수량 보정 ==========
def can_buy(krw: float) -> bool:
    return krw >= MIN_KRW_ORDER

def can_sell(qty: float, ref_price: float) -> bool:
    return (qty * ref_price) >= MIN_KRW_ORDER

def safe_qty(qty: float, avail: float) -> float:
    return q8(max(0.0, min(qty, avail)))

def min_sell_qty(cur_px: float, min_krw: float) -> float:
    if cur_px <= 0: return 0.0
    return q8(min_krw / cur_px)

# ========== 리콘실리에이션(상태 대사) ==========
def reconcile_worker():
    while True:
        try:
            with pos_lock:
                internal_btc = position.total_qty
            exch_btc = btc_total_balance()
            ask, bid = get_orderbook_best()
            best_bid = float(bid or 0.0)
            krw_gap = abs(exch_btc - internal_btc) * best_bid
            if krw_gap >= (MIN_KRW_ORDER * 0.5):
                logging.warning(f"[recon] mismatch: internal={internal_btc:.8f} exch_btc={exch_btc:.8f} (≈{krw_gap:.0f} KRW)")
            time.sleep(600)
        except Exception as e:
            logging.warning(f"[recon] worker error: {e}", exc_info=True)
            time.sleep(60)

# ========== 종료 핸들러 ==========
def on_exit():
    try:
        with pos_lock:
            position.save()
    except Exception:
        pass
    logging.info("[exit] state saved")

# ========== 메인 루프 ==========
def main_loop():
    global position
    logging.info("[boot] starting bot...")
    hb_t0 = time.time()

    while True:
        if INTERVAL == "minute1":
            sleep_to_next_minute(offset_sec=1.0)
        else:
            time.sleep(POLL_SECONDS)

        if time.time() - hb_t0 >= HEARTBEAT_SEC:
            with pos_lock:
                p_qty, p_avg, p_tp1, p_ts = position.total_qty, position.avg_price(), position.tp1_done, position.trailing_stop
            logging.info(f"[hb {now_kst().strftime('%H:%M:%S')}] pos={p_qty:.8f} avg={p_avg:.0f} tp1={p_tp1} ts={(p_ts if p_ts else 0):.0f}")
            hb_t0 = time.time()

        try:
            # 1) 시세/지표 (전 분 기준)
            df_1m_raw = get_ohlc_safe("minute1", max(200, WARMUP + 10))
            if df_1m_raw is None:
                continue
            
            # [수정 1, 2 적용] 백테스트와 동일한 지표/EMA 계산
            df_ind = compute_indicators_1m_sync(df_1m_raw, ATR_PERIOD, K, RSI_PERIOD, MA_SHORT, MA_LONG)
            df_15m_close = df_1m_raw['close'].resample('15min').last()
            ema15_sync = ema15_on_1min_sync(df_ind.index, df_15m_close, span=EMA15_EMA_SPAN)

            # [수정 3] 전 분(t-2)에서 엔트리 조건 판정
            row_prev = df_ind.iloc[-2]
            close_prev = float(row_prev['close'])
            tgt_up = round_price(float(row_prev['target']), "up")  # 비교 전에 올림
            rsi_prev = float(row_prev['rsi'])                      # 소문자 rsi 사용
            ma_s_prev = float(row_prev['ma_s'])
            ma_l_prev = float(row_prev['ma_l'])
            ema15_prev = float(ema15_sync.iloc[-2])

            buy_cond = (
                (close_prev > tgt_up) and
                (rsi_prev < RSI_THRESHOLD) and
                (ma_s_prev > ma_l_prev) and
                (close_prev > ema15_prev)
            )

            if buy_cond:
                # [패치D] 신호 성분 로그
                logging.info(
                    f"[SIGNAL] close_prev={close_prev:.0f} > tgt_up={tgt_up:.0f}, "
                    f"RSI={rsi_prev:.2f}<{RSI_THRESHOLD}, MA {ma_s_prev:.0f}>{ma_l_prev:.0f}, "
                    f"EMA15={ema15_prev:.0f}"
                )

            # 2) 다음 분 시작 → 호가 조회 및 재진입 판단
            ask, bid = get_orderbook_best()
            if ask is None or bid is None:
                continue

            with pos_lock:
                cur_qty = position.total_qty
                cur_avg = position.avg_price()

            # [수정 4] 재진입(피라미딩) 기준가격을 전 분 종가로 변경
            allow_reentry = (
                (cur_qty <= 0) or
                (REENTRY_TH is None) or
                (close_prev > cur_avg * (1.0 + REENTRY_TH))
            )

            if MAX_POSITION_KRW is not None:
                pos_val_krw = cur_qty * float(bid or 0.0)
                if pos_val_krw > MAX_POSITION_KRW:
                    allow_reentry = False

            krw = krw_balance()
            buy_cap = krw * (BUY_PCT if BUY_PCT is not None else 1.0)
            if BUY_KRW_CAP is not None:
                buy_cap = min(buy_cap, float(BUY_KRW_CAP))
            buy_amt = buy_cap * (1 - FEE)

            if buy_cond and allow_reentry and can_buy(buy_amt):
                order = safe_call(upbit.buy_market_order, TICKER, buy_amt)
                uuid = order.get('uuid') if isinstance(order, dict) else None
                filled = fetch_filled_volume(uuid)
                entry_px = round_price(ask, "up")  # 체결가는 실시간 호가 사용(유지)
                if filled <= 0:
                    filled = (buy_amt / max(entry_px, 1e-8)) * (1.0 - FEE)
                qty = q8(filled)
                if qty > 0:
                    with pos_lock:
                        position.add_entry(entry_px, qty)
                        entries_len = len(position.entries)
                        position.save()
                    logging.info(f"[BUY] krw={buy_amt:.0f} qty={qty} px={entry_px} entries={entries_len}")

                    # [패치A] 매수 직후 TS 초기화 (백테스터와 동일 타이밍)
                    step0 = TRAILING_STEP_AFTER_TP1 if position.tp1_done else TRAILING_STEP
                    ts0 = round_price(entry_px * (1 - step0), "down")
                    with pos_lock:
                        position.trailing_stop = max(position.trailing_stop or 0.0, ts0)
                        position.save()
                    logging.info(f"[INIT TS] set to {ts0:.0f} after BUY")

            # [패치B] BAR 기반 1회 트레일 상향 (전 분 high 기준)
            with pos_lock:
                if position.total_qty > 0:
                    step_now = TRAILING_STEP_AFTER_TP1 if position.tp1_done else TRAILING_STEP
                    ts_bar = round_price(float(row_prev['high']) * (1 - step_now), "down")
                    if ts_bar > (position.trailing_stop or 0.0):
                        position.trailing_stop = ts_bar
                        position.save()
                        logging.info(f"[BAR] TRAIL UP prev_high → {ts_bar:.0f}")

            # 3) FAST intrabar: 다음 분 0.5초 전까지 TP/TS 관리 (로직 유지)
            next_min = (int(time.time() // 60) + 1) * 60
            deadline = next_min - 0.5

            while True:
                if time.time() >= deadline: break
                
                with pos_lock:
                    qty_total = position.total_qty
                if qty_total <= 0: break

                ask, bid = get_orderbook_best()
                if ask is None or bid is None:
                    time.sleep(0.3); continue

                with pos_lock:
                    avg, tp1_done, cur_ts = position.avg_price(), position.tp1_done, position.trailing_stop

                bid_high = round_price(bid, "down")
                acted = False

                step = TRAILING_STEP_AFTER_TP1 if tp1_done else TRAILING_STEP
                new_ts = round_price(bid_high * (1 - step), "down")
                if new_ts > (cur_ts or 0.0):
                    with pos_lock:
                        if new_ts > (position.trailing_stop or 0.0):
                            position.trailing_stop = new_ts
                            position.save()
                    logging.info(f"[FAST] TRAIL UP @ {new_ts:.0f}")

                if (not tp1_done) and (bid_high >= avg * (1 + TP1_RATE)):
                    with pos_lock: q_total = position.total_qty
                    q1_raw = safe_qty(q_total * TP1_PCT, q_total)
                    if (q_total - q1_raw) < min_sell_qty(bid_high, MIN_KRW_ORDER):
                        q1 = q_total
                    else:
                        q1 = q1_raw
                    if can_sell(q1, bid_high) and q1 > 0:
                        order = safe_call(upbit.sell_market_order, TICKER, q1)
                        uuid = order.get('uuid') if isinstance(order, dict) else None
                        filled_qty = fetch_filled_volume(uuid)
                        if filled_qty > 0:
                            with pos_lock:
                                position.reduce_qty(filled_qty)
                                position.tp1_done = True
                                be_floor = round_price(avg * (1 + FEE*2 + 0.001), "up")
                                position.trailing_stop = max(position.trailing_stop or 0.0, be_floor)
                                position.save()
                                remain_qty = position.total_qty
                            logging.info(f"[FAST] TP1 SELL filled={filled_qty:.8f} remain={remain_qty:.8f}")
                            acted = True

                with pos_lock:
                    qty_total, tp1_done, avg = position.total_qty, position.tp1_done, position.avg_price()
                if qty_total > 0 and tp1_done and bid_high >= avg * (1 + TP2_RATE):
                    qty_all = qty_total
                    if can_sell(qty_all, bid_high) and qty_all > 0:
                        order = safe_call(upbit.sell_market_order, TICKER, qty_all)
                        uuid = order.get('uuid') if isinstance(order, dict) else None
                        filled = fetch_filled_volume(uuid)
                        if filled > 0:
                            if filled >= qty_all * 0.999:
                                logging.info(f"[FAST] TP2 SELL qty={qty_all:.8f}")
                                with pos_lock: position.clear(); position.save()
                                acted = True; break
                            else:
                                with pos_lock: position.reduce_qty(filled); position.save()
                                logging.info(f"[FAST] TP2 PARTIAL filled={filled:.8f}, remain={position.total_qty:.8f}")
                                acted = True

                with pos_lock:
                    qty_total, cur_ts = position.total_qty, position.trailing_stop
                if qty_total > 0 and cur_ts is not None and bid_high <= cur_ts:
                    qty_all = qty_total
                    if can_sell(qty_all, bid_high) and qty_all > 0:
                        order = safe_call(upbit.sell_market_order, TICKER, qty_all)
                        uuid = order.get('uuid') if isinstance(order, dict) else None
                        filled = fetch_filled_volume(uuid)
                        if filled > 0:
                            if filled >= qty_all * 0.999:
                                logging.info(f"[FAST] TRAIL STOP SELL qty={qty_all:.8f}")
                                with pos_lock: position.clear(); position.save()
                                acted = True; break
                            else:
                                with pos_lock: position.reduce_qty(filled); position.save()
                                logging.info(f"[FAST] TS PARTIAL filled={filled:.8f}, remain={position.total_qty:.8f}")
                                acted = True

                remain = deadline - time.time()
                nap = FAST_POLL if not acted else max(1, FAST_POLL // 2)
                time.sleep(max(0.05, min(nap, max(0.05, remain - 0.05))))
            
            if time.time() - hb_t0 >= HEARTBEAT_SEC:
                with pos_lock:
                    p_qty, p_avg, p_tp1, p_ts = position.total_qty, position.avg_price(), position.tp1_done, position.trailing_stop
                logging.info(f"[hb {now_kst().strftime('%H:%M:%S')}] pos={p_qty:.8f} avg={p_avg:.0f} tp1={p_tp1} ts={(p_ts if p_ts else 0):.0f}")
                hb_t0 = time.time()

        except Exception as e:
            with pos_lock:
                position._err_count = getattr(position, "_err_count", 0) + 1
                err_no = position._err_count
            logging.error(f"Main loop error #{err_no}", exc_info=True)
            if err_no >= CRIT_ERR_THRESHOLD:
                logging.critical("[CRIT] too many errors, backing off 10s")
                with pos_lock: position._err_count = 0
                time.sleep(10)
            continue
        else:
            with pos_lock:
                position._err_count = 0

# ========== 엔트리 ==========
if __name__ == "__main__":
    setup_logging()
    atexit.register(on_exit)
    recon_thread = threading.Thread(target=reconcile_worker, daemon=True)
    recon_thread.start()
    try:
        main_loop()
    except KeyboardInterrupt:
        logging.info("[kill] interrupted by user")
