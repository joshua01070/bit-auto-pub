import time
import pyupbit
import datetime
import numpy as np
import pandas as pd

# === 파라미터 설정 ===
ACCESS        = ""
SECRET        = ""
TICKER        = "KRW-BTC"

# 전략 파라미터
K             = 0.5              # 변동성 돌파 k (0~1)
RSI_PERIOD    = 14               # RSI 기간 (분봉 기준)
RSI_THRESHOLD = 30               # RSI 과매도 기준
MA_SHORT      = 5                # 단기 이평선 기간
MA_LONG       = 20               # 장기 이평선 기간

FEE           = 0.0005           # 거래 수수료 비율
TP_RATE       = 0.05             # 목표 수익률 5%
TRAILING_STEP = 0.03             # 트레일 스탑 3%

# 폴링 및 데이터 개수
INTERVAL      = "minute1"       # 1분봉 사용
WARMUP        = max(MA_LONG, RSI_PERIOD) + 2
POLL_SECONDS  = 5               # 초 주기 폴링

# === 보조함수 정의 ===

def fetch_ohlcv(ticker, interval, count, retries=3, wait=1.0):
    for _ in range(retries):
        df = pyupbit.get_ohlcv(ticker, interval=interval, count=count)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df
        time.sleep(wait)
    raise RuntimeError(f"OHLCV fetch failed for {ticker}")

# === 로그인 ===
upbit = pyupbit.Upbit(ACCESS, SECRET)
print("=== Multi-Trade Intraday Bot Start ===")

# 상태 초기화
in_position   = False
entry_price   = 0.0
trailing_stop = 0.0

while True:
    try:
        # 1) 분봉 데이터 조회
        df = fetch_ohlcv(TICKER, INTERVAL, WARMUP)
        # 2) 지표 계산
        df['range']  = (df['high'] - df['low']) * K
        df['target'] = df['open'] + df['range'].shift(1)
        delta       = df['close'].diff()
        gain        = delta.clip(lower=0)
        loss        = -delta.clip(upper=0)
        avg_gain    = gain.rolling(RSI_PERIOD).mean()
        avg_loss    = loss.rolling(RSI_PERIOD).mean().replace(0, 1e-8)
        df['rsi']   = 100 - 100 / (1 + avg_gain/avg_loss)
        df['ma_s']  = df['close'].rolling(MA_SHORT).mean()
        df['ma_l']  = df['close'].rolling(MA_LONG).mean()
        df = df.dropna()

        # 3) 최신 봉 기준 변수
        now    = datetime.datetime.now()
        row    = df.iloc[-1]
        price  = pyupbit.get_orderbook(ticker=TICKER)["orderbook_units"][0]["ask_price"]
        tgt    = row['target']
        rsi    = row['rsi']
        mas    = row['ma_s']
        mal    = row['ma_l']

        # 4) 매수 로직
        if not in_position and price > tgt and rsi < RSI_THRESHOLD and mas > mal:
            krw = float([b['balance'] for b in upbit.get_balances() if b['currency']=='KRW'][0] or 0)
            if krw > 5000:
                upbit.buy_market_order(TICKER, krw * (1-FEE))
                entry_price   = price * (1 + FEE)
                trailing_stop = entry_price * (1 - TRAILING_STEP)
                in_position   = True
                print(f"[{now}] BUY  @ {price:.0f}")

        # 5) 포지션 중 청산 로직
        if in_position:
            # 익절
            if price >= entry_price * (1 + TP_RATE):
                btc = float([b['balance'] for b in upbit.get_balances() if b['currency']=='BTC'][0] or 0)
                upbit.sell_market_order(TICKER, btc * (1-FEE))
                in_position = False
                print(f"[{now}] TP SELL @ {price:.0f}")
            else:
                # 트레일 스탑 업데이트
                new_ts = price * (1 - TRAILING_STEP)
                if new_ts > trailing_stop:
                    trailing_stop = new_ts
                    print(f"[{now}] TRAIL UP @ {trailing_stop:.0f}")
                # 트레일 손절
                if price <= trailing_stop:
                    btc = float([b['balance'] for b in upbit.get_balances() if b['currency']=='BTC'][0] or 0)
                    upbit.sell_market_order(TICKER, btc * (1-FEE))
                    in_position = False
                    print(f"[{now}] TRAIL STOP SELL @ {price:.0f}")

        time.sleep(POLL_SECONDS)

    except Exception as e:
        print(f"Error: {e}")
        time.sleep(POLL_SECONDS)
