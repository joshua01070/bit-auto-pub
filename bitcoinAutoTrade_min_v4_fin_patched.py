# === bitcoinAutoTrade_v4_fin_patched.py ===
"""
v4_fin (final, locked & backtest-equal)
- [중요] 거래 로직/조건/라운딩 정의는 '백테스트와 동일'하게 유지.
- [추가] 공유 객체(position) 접근에 threading.Lock 적용 → 메인루프와 리콘실 워커의 경쟁 상태 방지.
- [유지] MAX_ENTRIES 제거(백테스트와 규칙 일치). 재진입은 REENTRY_TH(가격 간격)로만 제어.
- [유지] 매도(TP1/TP2/TS): 주문 후 '실제 체결량'을 조회해 상태에 반영(부분체결 안전).
- [유지] 재진입 비교 기준: 최우선 매도호가(best_ask) 틱 상향(UP) 라운드.
- [유지] 리콘실 워커: free+locked vs 내부 포지션 비교 경고.
- [미세개선] 로그 일관성: 로그에 쓰이는 entries/remain 값을 모두 락 안에서 캡처해 출력.
"""

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
    return 1

def round_price(p: float, direction: str = "nearest") -> float:
    unit = price_unit(p)
    if direction == "down": return math.floor(p / unit) * unit
    if direction == "up":   return math.ceil(p / unit) * unit
    # nearest (ties up)
    return math.floor((p + unit * 0.5) / unit) * unit

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
    for _ in range(3):
        try:
            ob = pyupbit.get_orderbook(tickers=[TICKER]) or []
            if not ob:
                time.sleep(0.3); continue
            unit = ob[0]['orderbook_units'][0]
            ask = float(unit['ask_price'])
            bid = float(unit['bid_price'])
            return ask, bid
        except Exception as e:
            logging.warning(f"[WARN] get_orderbook error: {e}", exc_info=True)
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
    if not uuid: return 0.0
    for _ in range(5):
        try:
            od = upbit.get_order(uuid)
            if isinstance(od, dict):
                state = od.get('state')
                trades = od.get('trades', [])
                if trades:
                    filled = sum(float(t.get('volume', 0.0)) for t in trades)
                    return q8(filled)
                if state in ['done', 'cancel']:  # 거래 없이 주문 종료
                    return 0.0
            time.sleep(0.5)
        except Exception as e:
            logging.warning(f"[WARN] get_order uuid={uuid} err: {e}", exc_info=True)
            time.sleep(0.5)
    return 0.0

# ========== 지표 ==========
def rsi(series: pd.Series, period: int) -> pd.Series:
    delta = series.diff()
    up, down = delta.clip(lower=0), -delta.clip(upper=0)
    ma_up = up.rolling(period).mean()
    ma_down = down.rolling(period).mean().replace(0, 1e-12)
    return 100 - (100 / (1 + ma_up / ma_down))

def compute_indicators_1m(df_1m: pd.DataFrame) -> pd.DataFrame:
    df = df_1m.copy()
    df['H-L'] = df['high'] - df['low']
    df['H-C'] = (df['high'] - df['close']).abs()
    df['L-C'] = (df['low'] - df['close']).abs()
    tr = df[['H-L', 'H-C', 'L-C']].max(axis=1)
    df['ATR'] = tr.rolling(ATR_PERIOD).mean()
    df['RSI'] = rsi(df['close'], RSI_PERIOD)
    df['ma_s'] = df['close'].rolling(MA_SHORT).mean()
    df['ma_l'] = df['close'].rolling(MA_LONG).mean()
    df['target'] = df['open'] + df['ATR'].shift(1) * K
    return df

def ema15_on_1min(df_1m: pd.DataFrame) -> pd.Series:
    # 15분 종가 → EMA(span=20) → 1분으로 ffill
    df_15 = df_1m['close'].resample('15T').last()
    ema15 = df_15.ewm(span=EMA15_EMA_SPAN, adjust=False).mean()
    return ema15.reindex(df_1m.index, method='ffill')

# ========== 상태/포지션 ==========
class Position:
    def __init__(self):
        # entries: List[(price, qty)]
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
pos_lock = threading.Lock()   # 공유 상태 락

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
            # 내부 상태 스냅샷(락)
            with pos_lock:
                internal_btc = position.total_qty
            # 거래소/시세(API, 락 밖)
            exch_btc = btc_total_balance()
            ask, bid = get_orderbook_best()
            best_bid = float(bid or 0.0)
            # 비교/로깅
            krw_gap = abs(exch_btc - internal_btc) * best_bid
            if krw_gap >= (MIN_KRW_ORDER * 0.5):  # 노이즈 억제
                logging.warning(f"[recon] mismatch: internal={internal_btc:.8f} exch={exch_btc:.8f} (≈{krw_gap:.0f} KRW)")
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
        # 분 동기화
        if INTERVAL == "minute1":
            sleep_to_next_minute(offset_sec=1.0)
        else:
            time.sleep(POLL_SECONDS)

        # 하트비트(락으로 스냅샷)
        if time.time() - hb_t0 >= HEARTBEAT_SEC:
            with pos_lock:
                p_qty = position.total_qty
                p_avg = position.avg_price()
                p_tp1 = position.tp1_done
                p_ts  = position.trailing_stop
            logging.info(f"[hb {now_kst().strftime('%H:%M:%S')}] pos={p_qty:.8f} avg={p_avg:.0f} tp1={p_tp1} ts={p_ts if p_ts else 0:.0f}")
            hb_t0 = time.time()

        try:
            # 1) 시세/지표 (전 분 기준)
            df = get_ohlc_safe("minute1", max(200, WARMUP + 10))
            if df is None:
                continue
            df = compute_indicators_1m(df)
            ema15 = ema15_on_1min(df)

            # 전 분(신호 판단), 다음 분(체결)
            row_prev = df.iloc[-2]
            ema_prev = float(ema15.iloc[-2])

            close = float(row_prev['close'])
            tgt_up = ceil_price(float(row_prev['target']))
            mas = float(row_prev['ma_s'])
            mal = float(row_prev['ma_l'])
            rsi_v = float(row_prev['RSI'])

            buy_cond = (close > tgt_up) and (rsi_v < RSI_THRESHOLD) and (mas > mal) and (close > ema_prev)

            # 2) 다음 분 시작 → 호가 기반 체결/재진입 판단
            ask, bid = get_orderbook_best()
            if ask is None or bid is None:
                continue

            # 재진입 허용 여부 (None이면 필터 OFF → 재진입 허용)
            allow_reentry_price = round_price(ask, "up")
            with pos_lock:
                cur_qty = position.total_qty
                cur_avg = position.avg_price()
            allow_reentry = (
                (cur_qty == 0) or
                (REENTRY_TH is None) or
                (allow_reentry_price >= cur_avg * (1.0 + REENTRY_TH))
            )

            # (선택) 총 포지션 상한 체크
            if MAX_POSITION_KRW is not None:
                pos_val_krw = cur_qty * float(bid or 0.0)
                if pos_val_krw > MAX_POSITION_KRW:
                    allow_reentry = False

            # 매수액 산출
            krw = krw_balance()
            buy_cap = krw * (BUY_PCT if BUY_PCT is not None else 1.0)
            if BUY_KRW_CAP is not None:
                buy_cap = min(buy_cap, float(BUY_KRW_CAP))
            buy_amt = buy_cap * (1 - FEE)

            # --- 주문(API, 락 밖) ---
            if buy_cond and allow_reentry and can_buy(buy_amt):
                order = safe_call(upbit.buy_market_order, TICKER, buy_amt)
                uuid = order.get('uuid') if isinstance(order, dict) else None
                filled = fetch_filled_volume(uuid)
                if filled <= 0:  # 폴백: 보수적 추정
                    filled = (buy_amt / max(ask, 1e-8)) * (1.0 - FEE)
                entry_px = round_price(ask, "up")
                qty = q8(filled)
                # --- 상태 갱신(락) + 로그용 entries_len 캡처 ---
                if qty > 0:
                    with pos_lock:
                        position.add_entry(entry_px, qty)
                        entries_len = len(position.entries)  # 로그용 캡처
                        position.save()
                    logging.info(f"[BUY] krw={buy_amt:.0f} qty={qty} px={entry_px} entries={entries_len}")

            # 3) FAST intrabar: 다음 분 0.5초 전까지 TP/TS 관리
            next_min = (int(time.time() // 60) + 1) * 60
            deadline = next_min - 0.5

            while True:
                now = time.time()
                if now >= deadline:
                    break
                # 포지션 유무 스냅샷(락)
                with pos_lock:
                    qty_total = position.total_qty
                if qty_total <= 0:
                    break

                ask, bid = get_orderbook_best()
                if ask is None or bid is None:
                    time.sleep(0.3)
                    continue

                # 스냅샷: avg/tp1/ts(락)
                with pos_lock:
                    avg = position.avg_price()
                    tp1_done = position.tp1_done
                    cur_ts   = position.trailing_stop

                bid_high = round_price(bid, "down")
                acted = False

                # ① intrabar 트레일 상향(호가 기준)
                step = TRAILING_STEP_AFTER_TP1 if tp1_done else TRAILING_STEP
                new_ts = round_price(bid_high * (1 - step), "down")
                if new_ts > (cur_ts or 0.0):
                    with pos_lock:
                        if new_ts > (position.trailing_stop or 0.0):
                            position.trailing_stop = new_ts
                            position.save()
                    logging.info(f"[FAST] TRAIL UP @ {new_ts:.0f}")

                # ② TP1 (실체결량 반영)
                if (not tp1_done) and (bid_high >= avg * (1 + TP1_RATE)):
                    with pos_lock:
                        q_total = position.total_qty
                    q1_raw = safe_qty(q_total * TP1_PCT, q_total)
                    # 잔량이 최소주문 미만이면 전량으로 조정
                    if (q_total - q1_raw) < min_sell_qty(bid_high, MIN_KRW_ORDER):
                        q1 = q_total
                    else:
                        q1 = q1_raw

                    if can_sell(q1, bid_high) and q1 > 0:
                        # 주문/API (락 밖)
                        order = safe_call(upbit.sell_market_order, TICKER, q1)
                        uuid = order.get('uuid') if isinstance(order, dict) else None
                        filled_qty = fetch_filled_volume(uuid)
                        if filled_qty > 0:
                            # 상태 갱신(락) + 로그용 remain_qty 캡처
                            with pos_lock:
                                position.reduce_qty(filled_qty)
                                position.tp1_done = True
                                be_floor = round_price(avg * (1 + FEE*2 + 0.001), "up")
                                position.trailing_stop = max(position.trailing_stop or 0.0, be_floor)
                                position.save()
                                remain_qty = position.total_qty  # 로그용 캡처
                            logging.info(f"[FAST] TP1 SELL filled={filled_qty:.8f} remain={remain_qty:.8f}")
                            acted = True

                # ③ TP2 (실체결량 반영, 부분체결 안전화)
                with pos_lock:
                    qty_total = position.total_qty
                    tp1_done  = position.tp1_done
                    avg       = position.avg_price()
                if qty_total > 0 and tp1_done and bid_high >= avg * (1 + TP2_RATE):
                    qty_all = qty_total
                    if can_sell(qty_all, bid_high) and qty_all > 0:
                        order = safe_call(upbit.sell_market_order, TICKER, qty_all)
                        uuid = order.get('uuid') if isinstance(order, dict) else None
                        filled = fetch_filled_volume(uuid)
                        if filled > 0:
                            if filled >= qty_all * 0.999:
                                logging.info(f"[FAST] TP2 SELL qty={qty_all:.8f}")
                                with pos_lock:
                                    position.clear()
                                    position.save()
                                acted = True
                                break
                            else:
                                with pos_lock:
                                    position.reduce_qty(filled)
                                    position.save()
                                    remain_qty = position.total_qty  # 로그용 캡처
                                logging.info(f"[FAST] TP2 PARTIAL filled={filled:.8f}, remain={remain_qty:.8f}")
                                acted = True  # 전량 미체결 → 루프 계속

                # ④ Trailing Stop (실체결량 반영, 부분체결 안전화)
                with pos_lock:
                    qty_total = position.total_qty
                    cur_ts    = position.trailing_stop
                if qty_total > 0 and cur_ts is not None and bid_high <= cur_ts:
                    qty_all = qty_total
                    if can_sell(qty_all, bid_high) and qty_all > 0:
                        order = safe_call(upbit.sell_market_order, TICKER, qty_all)
                        uuid = order.get('uuid') if isinstance(order, dict) else None
                        filled = fetch_filled_volume(uuid)
                        if filled > 0:
                            if filled >= qty_all * 0.999:
                                logging.info(f"[FAST] TRAIL STOP SELL qty={qty_all:.8f}")
                                with pos_lock:
                                    position.clear()
                                    position.save()
                                acted = True
                                break
                            else:
                                with pos_lock:
                                    position.reduce_qty(filled)
                                    position.save()
                                    remain_qty = position.total_qty  # 로그용 캡처
                                logging.info(f"[FAST] TS PARTIAL filled={filled:.8f}, remain={remain_qty:.8f}")
                                acted = True  # 전량 미체결 → 루프 계속

                # 동적 슬립
                remain = deadline - time.time()
                nap = FAST_POLL if not acted else max(1, FAST_POLL // 2)
                time.sleep(max(0.05, min(nap, max(0.05, remain - 0.05))))

        except Exception as e:
            # 루프 에러 관리 (연속 임계 시 백오프)
            with pos_lock:
                position._err_count = getattr(position, "_err_count", 0) + 1
                err_no = position._err_count
            logging.error(f"Main loop error #{err_no}", exc_info=True)
            if err_no >= CRIT_ERR_THRESHOLD:
                logging.critical("[CRIT] too many errors, backing off 10s")
                with pos_lock:
                    position._err_count = 0
                time.sleep(10)
            continue
        else:
            with pos_lock:
                position._err_count = 0

# ========== 엔트리 ==========
if __name__ == "__main__":
    setup_logging()
    atexit.register(on_exit)
    # 리콘실리에이션 워커 시작
    recon_thread = threading.Thread(target=reconcile_worker, daemon=True)
    recon_thread.start()
    try:
        main_loop()
    except KeyboardInterrupt:
        logging.info("[kill] interrupted by user")

