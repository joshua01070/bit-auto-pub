import pyupbit
import numpy as np
import pandas as pd
import time
from itertools import product

# === 사용자 설정 ===
# 테스트할 일수 리스트
DAYS_LIST         = [7, 14, 30]    # 3개 기간을 한 번에 테스트
POLL_SECONDS      = 30             # 실전 폴링 주기(초) - 참고용

# 파라미터 리스트 (리스트로 비교)
K_LIST             = [0.1, 0.2, 0.3, 0.4, 0.5]
RSI_THRESHOLD_LIST = [20, 30, 40, 60]
MA_SHORT_LIST      = [3, 5, 7]
MA_LONG_LIST       = [15, 30, 40, 50, 60]
TP_LIST            = [0.01, 0.02, 0.03, 0.05]
TRAIL_LIST         = [0.01, 0.03, 0.05, 0.07, 0.1]

# 기본 상수
INTERVAL     = "minute1"
RSI_PERIOD   = 14
FEE          = 0.0005
MIN_TRADES   = 1  # 참고용 최소 거래 횟수 필터 (지정 가능)

# OHLCV 조회 함수
def fetch_ohlcv(ticker, interval, count, retries=3, wait=1.0):
    for _ in range(retries):
        df = pyupbit.get_ohlcv(ticker, interval=interval, count=count)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return df
        time.sleep(wait)
    raise RuntimeError(f"OHLCV fetch failed for {ticker}")

# 시뮬레이터 정의 (수익률과 거래 횟수 반환)
def simulate(df, k, rsi_th, ma_s, ma_l, tp_rate, trail_pct):
    d = df.copy()
    d['range']  = (d['high'] - d['low']) * k
    d['target'] = d['open'] + d['range'].shift(1)
    d['ma_s']   = d['close'].rolling(ma_s).mean()
    d['ma_l']   = d['close'].rolling(ma_l).mean()
    d = d.dropna(subset=['target','ma_s','ma_l','rsi'])
    if len(d) < 2:
        return None, None
    in_pos = False
    entry_price = 0.0
    trailing_stop = 0.0
    returns = []
    trades = 0
    for _, row in d.iterrows():
        h, l = row['high'], row['low']
        tgt, rsi = row['target'], row['rsi']
        mas, mal = row['ma_s'], row['ma_l']
        # 진입 조건
        if not in_pos and h > tgt and rsi < rsi_th and mas > mal:
            entry_price   = row['open'] * (1 + FEE)
            trailing_stop = entry_price * (1 - trail_pct)
            in_pos = True
            trades += 1
        # 청산 조건
        if in_pos:
            # 익절
            if h >= entry_price * (1 + tp_rate):
                returns.append((entry_price * (1 + tp_rate)) / entry_price - FEE)
                in_pos = False
            else:
                # 트레일 갱신
                new_ts = h * (1 - trail_pct)
                if new_ts > trailing_stop:
                    trailing_stop = new_ts
                # 트레일 손절
                if l <= trailing_stop:
                    returns.append(trailing_stop / entry_price - FEE)
                    in_pos = False
    # 종가 청산
    if in_pos:
        returns.append(d.iloc[-1]['close'] / entry_price - FEE)
    if trades < MIN_TRADES or not returns:
        return None, None
    return np.prod(returns), trades

# === 멀티 기간 그리드 서치 실행 ===
for DAYS in DAYS_LIST:
    # warmup, count 계산
    warmup = max(max(MA_LONG_LIST), RSI_PERIOD) + 2
    count = DAYS * 24 * 60 + warmup

    # 데이터 로드 및 RSI 계산
    df = fetch_ohlcv("KRW-BTC", INTERVAL, count)
    delta    = df['close'].diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.rolling(RSI_PERIOD).mean()
    avg_loss = loss.rolling(RSI_PERIOD).mean().replace(0,1e-8)
    df['rsi'] = 100 - 100/(1 + avg_gain/avg_loss)
    df = df.iloc[warmup:].dropna().copy()

    # 결과 수집
    results = []
    for k, rsi_th, ma_s, ma_l, tp, trail in product(
        K_LIST, RSI_THRESHOLD_LIST, MA_SHORT_LIST, MA_LONG_LIST,
        TP_LIST, TRAIL_LIST
    ):
        if ma_s >= ma_l:
            continue
        ror, trades = simulate(df, k, rsi_th, ma_s, ma_l, tp, trail)
        if ror is not None:
            results.append({
                'DAYS': DAYS, 'k': k, 'rsi_th': rsi_th,
                'ma_s': ma_s, 'ma_l': ma_l,
                'tp': tp, 'trail': trail,
                'ROR': ror, 'trades': trades
            })
    # 출력
    print(f"\n=== BACKTEST DAYS={DAYS} (1분봉) 최상위 50 ===")
    if not results:
        print("조건을 만족하는 전략이 없습니다.")
    else:
        out = pd.DataFrame(results).sort_values('ROR', ascending=False)
        print(out[['k','rsi_th','ma_s','ma_l','tp','trail','ROR','trades']]
              .head(50).to_string(index=False))
