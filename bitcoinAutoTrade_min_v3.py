import time, logging, json, os, atexit
import pyupbit

# === 파라미터 ===
ACCESS        = ""  
SECRET        = ""
TICKER        = "KRW-BTC"
K             = 0.4
ATR_PERIOD    = 14
RSI_PERIOD    = 14
RSI_THRESHOLD = 40
MA_SHORT      = 7
MA_LONG       = 30
FEE           = 0.0005
RISK_RATE     = 0.9      #계좌 위험 한도
TP1_RATE      = 0.02
TP2_RATE      = 0.05
TP1_PCT       = 0.40
TRAILING_STEP = 0.08
INTERVAL      = "minute1"
POLL_SECONDS  = 20
WARMUP        = max(MA_LONG, RSI_PERIOD, ATR_PERIOD) + 2
STATE_PATH    = "state.json"

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S')
upbit = pyupbit.Upbit(ACCESS, SECRET)

# === Position ===
class Position:
    def __init__(self):
        self.entries = []
        self.total_qty = 0.0
        self.tp1_done = False
        self.trailing_stop = None

    def to_dict(self):
        return {
            "entries": self.entries,
            "tp1_done": self.tp1_done,
            "trailing_stop": self.trailing_stop,
        }

    def save(self):
        tmp = STATE_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.to_dict(), f)
        os.replace(tmp, STATE_PATH)

    @classmethod
    def load(cls):
        if not os.path.exists(STATE_PATH):
            return cls()
        try:
            with open(STATE_PATH, "r") as f:
                data = json.load(f)
            obj = cls()
            obj.entries = [tuple(e) for e in data.get("entries", [])]
            obj.total_qty = sum(q for _, q in obj.entries)
            obj.tp1_done = data.get("tp1_done", False)
            obj.trailing_stop = data.get("trailing_stop")
            return obj
        except Exception as e:
            logging.warning(f"state load failed: {e}")
            return cls()

    def add_entry(self, price: float, qty: float):
        self.entries.append((price, qty))
        self.total_qty += qty

    def reduce_qty(self, qty: float):
        remain = qty
        new = []
        for p, q in self.entries:
            if remain <= 0:
                new.append((p, q))
            elif remain >= q:
                remain -= q
            else:
                new.append((p, q - remain))
                remain = 0
        self.entries = new
        self.total_qty -= qty

    def avg_price(self):
        return sum(p * q for p, q in self.entries) / self.total_qty if self.total_qty else 0

    def clear(self):
        self.__init__()

position = Position.load()
logging.info(f"⏪ state loaded – qty={position.total_qty:.6f}, avg={position.avg_price():.0f}")
atexit.register(position.save)

# === utils ===

def safe_order(func, *args, **kwargs):
    for _ in range(3):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.warning(f"order retry: {e}")
            time.sleep(1)
    raise RuntimeError("order failed")

_DATA_CACHE = {}

def get_ohlc(ticker, interval, count):
    k = (ticker, interval, count)
    if k in _DATA_CACHE:
        return _DATA_CACHE[k].copy()
    for _ in range(5):
        df = pyupbit.get_ohlcv(ticker, interval=interval, count=count)
        if df is not None and not df.empty:
            _DATA_CACHE[k] = df
            return df.copy()
        time.sleep(0.3)
    raise RuntimeError("OHLCV fetch failed")

# === main ===
logging.info("=== v3 Bot Start ===")

while True:
    # Heartbeat log
    logging.info("Heartbeat – bot running")
    try:
        df = get_ohlc(TICKER, INTERVAL, WARMUP)
        df['H-L'] = df['high'] - df['low']
        df['H-PC'] = abs(df['high'] - df['close'].shift(1))
        df['L-PC'] = abs(df['low'] - df['close'].shift(1))
        df['TR'] = df[['H-L','H-PC','L-PC']].max(axis=1)
        df['ATR'] = df['TR'].rolling(ATR_PERIOD).mean()
        df['target'] = df['open'] + df['ATR'].shift(1) * K

        delta = df['close'].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(RSI_PERIOD).mean()
        avg_loss = loss.rolling(RSI_PERIOD).mean().replace(0,1e-8)
        df['rsi'] = 100 - 100 / (1 + avg_gain / avg_loss)

        df['ma_s'] = df['close'].rolling(MA_SHORT).mean()
        df['ma_l'] = df['close'].rolling(MA_LONG).mean()
        df.dropna(inplace=True)

        df15 = get_ohlc(TICKER, "minute15", 200)
        df15['ema20'] = df15['close'].ewm(span=20).mean()
        ema15 = df15['ema20'].iloc[-1]

        row = df.iloc[-1]
        price = pyupbit.get_orderbook(TICKER)['orderbook_units'][0]['ask_price']
        tgt, rsi = row['target'], row['rsi']
        mas, mal, atr = row['ma_s'], row['ma_l'], row['ATR']

        krw = float(next((b['balance'] for b in upbit.get_balances() if b['currency']=='KRW'), 0))

        # === 진입 ===
        if price > tgt and rsi < RSI_THRESHOLD and mas > mal and price > ema15 and krw > 5000:
            avg_entry = position.avg_price()
            if position.total_qty == 0 or price > avg_entry * 1.01:
                sl_price = price - atr * 1.5
                risk_krw = krw * RISK_RATE
                qty = risk_krw / (price - sl_price)
                buy_krw = min(krw, qty * price)

                order = safe_order(upbit.buy_market_order, TICKER, buy_krw * (1 - FEE))
                filled_qty = float(order.get('volume', 0)) - float(order.get('remaining_volume', 0))
                position.add_entry(price, filled_qty)
                position.trailing_stop = max(position.trailing_stop or 0, price * (1 - TRAILING_STEP))
                position.save()
                logging.info(f"BUY @ {price:.0f} | qty={filled_qty:.6f} | avg={position.avg_price():.0f}")

        # === 청산 ===
        if position.total_qty > 0:
            avg_price = position.avg_price()
            if not position.tp1_done and price >= avg_price * (1 + TP1_RATE):
                sell_q = position.total_qty * TP1_PCT
                safe_order(upbit.sell_market_order, TICKER, sell_q * (1 - FEE))
                position.reduce_qty(sell_q)
                position.tp1_done = True
                position.save()
                logging.info(f"TP1 SELL @ {price:.0f} | qty={sell_q:.6f}")
            elif position.tp1_done and price >= avg_price * (1 + TP2_RATE):
                safe_order(upbit.sell_market_order, TICKER, position.total_qty * (1 - FEE))
                logging.info(f"TP2 SELL @ {price:.0f} | qty={position.total_qty:.6f}")
                position.clear(); position.save()
            else:
                new_ts = price * (1 - TRAILING_STEP)
                if new_ts > (position.trailing_stop or 0):
                    position.trailing_stop = new_ts; position.save(); logging.info(f"TRAIL UP @ {new_ts:.0f}")
                if price <= (position.trailing_stop or 0):
                    safe_order(upbit.sell_market_order, TICKER, position.total_qty * (1 - FEE))
                    logging.info(f"TRAIL STOP SELL @ {price:.0f}")
                    position.clear(); position.save()

        time.sleep(POLL_SECONDS)

    except Exception as e:
        logging.error("Main loop error", exc_info=True)
        time.sleep(POLL_SECONDS)
