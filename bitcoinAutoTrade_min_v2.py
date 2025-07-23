import time
import datetime
import logging
import pyupbit

# === 파라미터 설정 ===
ACCESS        = ""
SECRET        = ""
TICKER        = "KRW-BTC"

# 전략 파라미터
K             = 0.5
ATR_PERIOD    = 14
RSI_PERIOD    = 14
RSI_THRESHOLD = 30
MA_SHORT      = 7
MA_LONG       = 50

FEE           = 0.0005
RISK_RATE     = 0.01
TP1_RATE      = 0.01
TP2_RATE      = 0.02
TP1_PCT       = 0.30
TRAILING_STEP = 0.07

INTERVAL      = "minute1"
WARMUP        = max(MA_LONG, RSI_PERIOD, ATR_PERIOD) + 2
POLL_SECONDS  = 30

# === 로깅 설정 (시간만 HH:MM:SS) ===
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%H:%M:%S"
)

upbit = pyupbit.Upbit(ACCESS, SECRET)
logging.info("=== Multi-Trade Intraday Bot Start ===")

in_position    = False
entry_price    = 0.0
sl_price       = 0.0
trailing_stop  = 0.0
tp1_executed   = False

def fetch_ohlcv(ticker, interval, count, retries=3, wait=1.0):
    for _ in range(retries):
        df = pyupbit.get_ohlcv(ticker, interval=interval, count=count)
        if isinstance(df, type(df)) and not df.empty:
            return df
        time.sleep(wait)
    raise RuntimeError(f"OHLCV fetch failed for {ticker}")

while True:
    try:
        # — 1분봉+지표 계산 —
        df = fetch_ohlcv(TICKER, INTERVAL, WARMUP)
        df['H-L']  = df['high'] - df['low']
        df['H-PC'] = abs(df['high'] - df['close'].shift(1))
        df['L-PC'] = abs(df['low']  - df['close'].shift(1))
        df['TR']   = df[['H-L','H-PC','L-PC']].max(axis=1)
        df['ATR']  = df['TR'].rolling(ATR_PERIOD).mean()
        df['target'] = df['open'] + df['ATR'].shift(1) * K

        delta     = df['close'].diff()
        gain      = delta.clip(lower=0)
        loss      = -delta.clip(upper=0)
        avg_gain  = gain.rolling(RSI_PERIOD).mean()
        avg_loss  = loss.rolling(RSI_PERIOD).mean().replace(0,1e-8)
        df['rsi'] = 100 - 100/(1 + avg_gain/avg_loss)

        df['ma_s'] = df['close'].rolling(MA_SHORT).mean()
        df['ma_l'] = df['close'].rolling(MA_LONG).mean()
        df = df.dropna()

        # — 상위 TF EMA 필터 —
        df15 = fetch_ohlcv(TICKER, "minute15", count=200)
        df15['ema20'] = df15['close'].ewm(span=20).mean()
        ema15 = df15['ema20'].iloc[-1]

        # — 현재값 읽기 —
        now    = datetime.datetime.now()
        row    = df.iloc[-1]
        price  = pyupbit.get_orderbook(ticker=TICKER)["orderbook_units"][0]["ask_price"]
        tgt    = row['target']
        rsi    = row['rsi']
        mas    = row['ma_s']
        mal    = row['ma_l']
        atr    = row['ATR']

        balances = upbit.get_balances()
        krw_bal  = float(next((b['balance'] for b in balances if b['currency']=='KRW'), 0) or 0)

        # — 매수 로직 —
        if (not in_position
            and price > tgt
            and rsi < RSI_THRESHOLD
            and mas > mal
            and price > ema15
            and krw_bal > 5000):

            sl_price    = price - atr * 1.5
            risk_krw    = krw_bal * RISK_RATE
            qty         = risk_krw / (price - sl_price)
            buy_krw     = min(krw_bal, qty * price)
            upbit.buy_market_order(TICKER, buy_krw * (1 - FEE))

            in_position   = True
            entry_price   = price
            trailing_stop = entry_price * (1 - TRAILING_STEP)
            tp1_executed  = False
            logging.info(f"BUY  @ {price:.0f} | qty={qty:.6f}, SL={sl_price:.0f}")

        # — 포지션 관리 & 청산 로직 —
        if in_position:
            balances = upbit.get_balances()
            btc_amt  = float(next((b['balance'] for b in balances if b['currency']=='BTC'), 0) or 0)

            # 1차 익절
            if not tp1_executed and price >= entry_price * (1 + TP1_RATE):
                sell_amt     = btc_amt * TP1_PCT
                upbit.sell_market_order(TICKER, sell_amt * (1 - FEE))
                tp1_executed = True
                logging.info(f"TP1 SELL @ {price:.0f}")

            # 2차 익절
            elif tp1_executed and price >= entry_price * (1 + TP2_RATE):
                upbit.sell_market_order(TICKER, btc_amt * (1 - FEE))
                in_position = False
                logging.info(f"TP2 SELL @ {price:.0f}")

            # 트레일링 스탑
            else:
                new_ts = price * (1 - TRAILING_STEP)
                if new_ts > trailing_stop:
                    trailing_stop = new_ts
                    logging.info(f"TRAIL UP @ {trailing_stop:.0f}")
                if price <= trailing_stop:
                    upbit.sell_market_order(TICKER, btc_amt * (1 - FEE))
                    in_position = False
                    logging.info(f"TRAIL STOP SELL @ {price:.0f}")

        # — 루프 완료 알림 —
        logging.info("auto-trade running...")

        time.sleep(POLL_SECONDS)

    except Exception as e:
        logging.error("Main loop error", exc_info=True)
        time.sleep(POLL_SECONDS)
