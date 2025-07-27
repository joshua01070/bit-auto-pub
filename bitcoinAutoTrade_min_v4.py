# === bitcoinAutoTrade_min_v4.py ===
"""
v4: Full-Balance Market Buy + Multi-Entry + v3 인프라 유지
- ATR·Risk 기반 사이징 제거
- 잔고 전액 KRW 시장가 주문 (MIN_KRW_ORDER 이상일 때)
- Multi-Entry: 평단 대비 +1% 시 추가 진입
- 에러 카운팅 및 임계치 초과 시 크리티컬 알림
"""
import time, logging, json, os, atexit
import pandas as pd  # pandas DataFrame methods used on OHLCV data
import pyupbit

# === 파라미터 ===
ACCESS, SECRET = "", ""
TICKER = "KRW-BTC"
K = 0.4
ATR_PERIOD = RSI_PERIOD = 14
RSI_THRESHOLD = 45
MA_SHORT, MA_LONG = 5, 30
FEE = 0.0005
TP1_RATE, TP2_RATE = 0.02, 0.05
TP1_PCT = 0.40
TRAILING_STEP, TRAILING_STEP_AFTER_TP1 = 0.02, 0.06
INTERVAL, POLL_SECONDS = "minute1", 20
WARMUP = max(ATR_PERIOD, RSI_PERIOD, MA_LONG) + 2
STATE_PATH = "state.json"
MIN_KRW_ORDER = 6000
MAX_ENTRIES = None  # 최대 진입 횟수 제한 (None = 무제한)

# 로깅 설정
logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S')
upbit = pyupbit.Upbit(ACCESS, SECRET)

# === safe_order ===
def safe_order(func, *args, **kwargs):
    for i in range(3):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.warning(f"Order retry {i+1}: {e}", exc_info=True)
            time.sleep(1)
    raise RuntimeError("Order failed after retries")

# === Position 클래스 ===
class Position:
    def __init__(self):
        self.entries = []
        self.total_qty = 0.0
        self.tp1_done = False
        self.trailing_stop = None
    def to_dict(self):
        return {"entries": self.entries, "tp1_done": self.tp1_done, "trailing_stop": self.trailing_stop}
    def save(self):
        tmp = STATE_PATH + ".tmp"
        with open(tmp, 'w') as f:
            json.dump(self.to_dict(), f)
        os.replace(tmp, STATE_PATH)
    @classmethod
    def load(cls):
        if not os.path.exists(STATE_PATH):
            return cls()
        try:
            data = json.load(open(STATE_PATH))
            obj = cls()
            obj.entries = [tuple(e) for e in data.get('entries', [])]
            obj.total_qty = sum(q for _, q in obj.entries)
            obj.tp1_done = data.get('tp1_done', False)
            obj.trailing_stop = data.get('trailing_stop')
            return obj
        except:
            return cls()
    def add_entry(self, price, qty):
        self.entries.append((price, qty))
        self.total_qty += qty
    def reduce_qty(self, qty):
        rem = qty
        new = []
        for p, q in self.entries:
            if rem <= 0:
                new.append((p, q))
            elif rem >= q:
                rem -= q
            else:
                new.append((p, q - rem))
                rem = 0
        self.entries = new
        self.total_qty = max(0, self.total_qty - qty)
    def avg_price(self):
        return sum(p * q for p, q in self.entries) / self.total_qty if self.total_qty else 0
    def clear(self):
        self.__init__()

# === state init ===
position = Position.load()
logging.info(f"⏪ state loaded – qty={position.total_qty:.6f}, avg={position.avg_price():.0f}")
atexit.register(position.save)

# === OHLCV cache ===
_CACHE = {}
def get_ohlc(ticker, interval, count):
    key = (ticker, interval, count)
    if key in _CACHE:
        return _CACHE[key].copy()
    for _ in range(5):
        df = pyupbit.get_ohlcv(ticker, interval=interval, count=count)
        if df is not None and not df.empty:
            _CACHE[key] = df.copy()
            return df.copy()
        time.sleep(0.3)
    raise RuntimeError("OHLCV fetch failed")

logging.info("=== v4 Bot Start ===")

# === Main Loop ===
while True:
    logging.info(f"Heartbeat – bot running")
    try:
        # 지표 계산
        df = get_ohlc(TICKER, INTERVAL, WARMUP)
        df.index = df.index.tz_localize(None)
        df['H-L'] = df.high - df.low
        df['H-PC'] = (df.high - df.close.shift()).abs()
        df['L-PC'] = (df.low - df.close.shift()).abs()
        df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1)
        df['ATR'] = df.TR.rolling(ATR_PERIOD).mean()
        df['target'] = df.open + df.ATR.shift(1) * K
        d = df.close.diff()
        df['rsi'] = 100 - 100 / (
            1 + d.clip(lower=0).rolling(RSI_PERIOD).mean() /
                (-d.clip(upper=0).rolling(RSI_PERIOD).mean().replace(0, 1e-8))
        )
        df['ma_s'] = df.close.rolling(MA_SHORT).mean()
        df['ma_l'] = df.close.rolling(MA_LONG).mean()
        df.dropna(inplace=True)

        # 15분봉 EMA
        cnt15 = WARMUP // 15 + 20
        df15 = get_ohlc(TICKER, 'minute15', cnt15)
        df15.index = df15.index.tz_localize(None)
        ema15 = (
            df15.close.resample('1min').ffill()
            .ewm(span=20)
            .mean()
            .reindex(df.index, method='ffill')
        )

        # 최신 데이터
        row = df.iloc[-1]
        close, tgt, rsi, mas, mal = row.close, row.target, row.rsi, row.ma_s, row.ma_l
        ema_val = ema15.loc[row.name]

        # 잔고 및 호가 조회
        orderbook = safe_order(pyupbit.get_orderbook, TICKER)
        price = orderbook['orderbook_units'][0]['ask_price']
        bals = safe_order(upbit.get_balances)
        bals = bals if isinstance(bals, list) else []
        krw_bal = next((float(b['balance']) for b in bals if b['currency']=='KRW'), 0)
        btc_bal = next((float(b['balance']) for b in bals if b['currency']=='BTC'), 0)

        # 수동 입출금 동기화
        diff = btc_bal - position.total_qty
        if abs(diff) > 1e-9:
            logging.warning(f"Manual BTC change Δ={diff:.8f}, sync")
            (position.add_entry if diff>0 else position.reduce_qty)(abs(diff))
            position.save()

        # 매수 로직 (풀 밸런스)
        if MAX_ENTRIES is not None and len(position.entries) >= MAX_ENTRIES:
            logging.info(f"Max entries reached ({MAX_ENTRIES}), skip buy")
        elif close>tgt and rsi<RSI_THRESHOLD and mas>mal and close>ema_val and krw_bal>MIN_KRW_ORDER:
            prev_avg = position.avg_price()
            if position.total_qty==0 or close>prev_avg*1.01:
                buy_amt = krw_bal*(1-FEE)
                order = safe_order(upbit.buy_market_order, TICKER, buy_amt)
                filled = float(order['volume']) - float(order['remaining_volume'])
                position.add_entry(close, filled)
                position.trailing_stop = max(position.trailing_stop or 0, close*(1-TRAILING_STEP))
                position.save()
                logging.info(f"BUY #{len(position.entries)} @ {close:.0f} qty={filled:.6f}")

        # 관리 / TP1 / TP2 / 트레일링
        if position.total_qty>0:
            avg = position.avg_price()
            high, low = row.high, row.low
            # TP1
            if not position.tp1_done and high>=avg*(1+TP1_RATE):
                q1 = position.total_qty*TP1_PCT
                safe_order(upbit.sell_market_order, TICKER, q1)
                position.reduce_qty(q1)
                position.tp1_done=True
                # TP1 직후 트레일 스톱 초기화
                position.trailing_stop = avg * (1 - TRAILING_STEP_AFTER_TP1)
                position.save()
                logging.info(f"TP1 SELL @ {avg*(1+TP1_RATE):.0f} qty={q1:.6f}")
            # TP2
            elif position.tp1_done and high>=avg*(1+TP2_RATE):
                safe_order(upbit.sell_market_order, TICKER, position.total_qty)
                logging.info(f"TP2 SELL @ {avg*(1+TP2_RATE):.0f} qty={position.total_qty:.6f}")
                position.clear(); position.save()
            else:
                step = TRAILING_STEP_AFTER_TP1 if position.tp1_done else TRAILING_STEP
                ts_new = high*(1-step)
                if ts_new>(position.trailing_stop or 0):
                    position.trailing_stop=ts_new; position.save()
                    logging.info(f"TRAIL UP @ {ts_new:.0f}")
                if low<=(position.trailing_stop or 0):
                    safe_order(upbit.sell_market_order, TICKER, position.total_qty)
                    logging.info(f"TRAIL STOP SELL @ {position.trailing_stop:.0f} qty={position.total_qty:.6f}")
                    position.clear(); position.save()

                    # Reset error counter after successful cycle
            if hasattr(position, '_err_count'):
                position._err_count = 0
        time.sleep(POLL_SECONDS)
    except Exception as e:
        # 연속 오류 카운팅
        if not hasattr(position, '_err_count'):
            position._err_count = 0
        position._err_count += 1
        logging.error(f"Main loop error #{position._err_count}", exc_info=True)
        if position._err_count >= 5:
            logging.critical(f"Bot has failed {position._err_count} times in a row – manual intervention recommended")
            position._err_count = 0
        time.sleep(POLL_SECONDS)
        continue
