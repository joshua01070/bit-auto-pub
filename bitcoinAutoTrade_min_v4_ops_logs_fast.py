# === bitcoinAutoTrade_min_v4_ops_logs_fast.py ===
"""
v4 (ops + fills log + fast exits) + qty 8-dec normalization + minute-sync fix
- 운영 안정화(1~6) + NDJSON 체결로그 + 분 동기화(진입) + intrabar 빠른 청산(FAST_POLL)
- 주문 직전 수량 8자리 정규화(q8) + 초과 방지(safe_qty)
- FAST 루프가 분 경계를 넘지 않도록 동적 슬립(2분 간격처럼 보이는 현상 방지)
"""
import time, logging, json, os, atexit, sys, threading
import pandas as pd
import pyupbit

# === 하드코딩 API 키 (여기에 입력) ===
ACCESS = ""
SECRET = ""

# === 파라미터 ===
TICKER = "KRW-BTC"
K = 0.5
ATR_PERIOD = 14
RSI_PERIOD = 14
RSI_THRESHOLD = 45
MA_SHORT, MA_LONG = 3, 30
FEE = 0.0005
TP1_RATE, TP2_RATE = 0.015, 0.05
TP1_PCT = 0.60
TRAILING_STEP, TRAILING_STEP_AFTER_TP1 = 0.06, 0.04
INTERVAL = "minute1"         # 1분봉 기준 분동기화 사용
POLL_SECONDS = 20            # (minute1이 아닌 경우만 사용)
WARMUP = max(ATR_PERIOD, RSI_PERIOD, MA_LONG) + 2
STATE_PATH = "state.json"
MIN_KRW_ORDER = 6000
MAX_ENTRIES = 3
FAST_POLL = 5                # 빠른 청산 루프 주기(초) 5~10 권장

# === 로깅 ===
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s', datefmt='%H:%M:%S')
logging.info("=== v4 ops + fills log + fast exits start ===")

# === 체결 로그 (NDJSON) ===
_log_lock = threading.Lock()
def _log_path():
    return f"fills_{time.strftime('%Y%m%d')}.ndjson"
def log_exec(event, **kw):
    rec = {"ts": time.time(), "iso": time.strftime("%Y-%m-%d %H:%M:%S"), "event": event, **kw}
    try:
        with _log_lock, open(_log_path(), "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:
        logging.warning(f"fill-log write failed: {e}")

# === 수량 8자리 정규화 유틸 ===
def q8(x: float) -> float:
    return float(f"{x:.8f}")
def safe_qty(x: float, max_qty: float) -> float:
    y = q8(x)
    if y > max_qty:
        y = q8(max_qty - 1e-9)
    return max(0.0, y)

# === 클라이언트/안전 호출 ===
upbit = pyupbit.Upbit(ACCESS, SECRET)
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

# === 포지션 ===
class Position:
    def __init__(self):
        self.entries = []          # list[(price, qty)]
        self.total_qty = 0.0
        self.tp1_done = False
        self.trailing_stop = None
        self._err_count = 0
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
        except Exception:
            return cls()
    def add_entry(self, price, qty):
        if qty <= 0: return
        self.entries.append((float(price), float(qty)))
        self.total_qty += float(qty)
    def reduce_qty(self, qty):
        rem = float(qty)
        if rem <= 0: return
        new = []
        for p, q in self.entries:
            if rem <= 0:
                new.append((p, q))
            elif rem >= q:
                rem -= q
            else:
                new.append((p, q - rem)); rem = 0.0
        self.entries = new
        self.total_qty = sum(q for _, q in new)
    def avg_price(self):
        return (sum(p * q for p, q in self.entries) / self.total_qty) if self.total_qty else 0.0
    def clear(self):
        self.__init__()

position = Position.load()
logging.info(f"⏪ state loaded – qty={position.total_qty:.8f}, avg={position.avg_price():.0f}")
atexit.register(position.save)

# === OHLCV 캐시(TTL) ===
_CACHE = {}
def get_ohlc(ticker, interval, count, ttl=5):
    key = (ticker, interval, count)
    now = time.time()
    cached = _CACHE.get(key)
    if cached and now - cached['ts'] < ttl:
        return cached['df'].copy()
    for _ in range(5):
        df = pyupbit.get_ohlcv(ticker, interval=interval, count=count)
        if df is not None and not df.empty:
            _CACHE[key] = {'df': df.copy(), 'ts': now}
            return df.copy()
        time.sleep(0.3)
    raise RuntimeError("OHLCV fetch failed")

# === 유틸 ===
def compute_indicators(df):
    df = df.copy()
    df.index = df.index.tz_localize(None)
    df['H-L']  = df.high - df.low
    df['H-PC'] = (df.high - df.close.shift(1)).abs()
    df['L-PC'] = (df.low  - df.close.shift(1)).abs()
    df['TR']   = df[['H-L','H-PC','L-PC']].max(axis=1)
    df['ATR']  = df.TR.rolling(ATR_PERIOD).mean()
    df['target'] = df.open + df.ATR.shift(1) * K
    d = df.close.diff()
    gain = d.clip(lower=0).rolling(RSI_PERIOD).mean()
    loss = (-d.clip(upper=0)).rolling(RSI_PERIOD).mean().replace(0, 1e-8)
    df['rsi'] = 100 - 100 / (1 + gain / loss)
    df['ma_s'] = df.close.rolling(MA_SHORT).mean()
    df['ma_l'] = df.close.rolling(MA_LONG).mean()
    df.dropna(inplace=True)
    return df

def ema15_on_1min(df_index, cnt_extra=20):
    cnt15 = max(WARMUP // 15 + cnt_extra, 50)
    df15 = get_ohlc(TICKER, 'minute15', cnt15)
    df15.index = df15.index.tz_localize(None)
    return (df15.close.resample('1min').ffill().ewm(span=20).mean().reindex(df_index, method='ffill'))

def can_sell(qty, ref_price):
    return (qty * ref_price) >= MIN_KRW_ORDER

def sleep_to_next_minute(offset_sec=1.0):
    now = time.time()
    next_mark = (int(now // 60) + 1) * 60 + offset_sec
    sleep_s = next_mark - now
    if sleep_s > 0:
        time.sleep(sleep_s)

def fetch_balances():
    bals = safe_call(upbit.get_balances)
    if not isinstance(bals, list):
        return 0.0, 0.0
    krw = next((float(b['balance']) for b in bals if b.get('currency') == 'KRW'), 0.0)
    btc = next((float(b['balance']) for b in bals if b.get('currency') == 'BTC'), 0.0)
    return krw, btc

def fetch_orderbook():
    ob = safe_call(pyupbit.get_orderbook, TICKER)
    unit = ob['orderbook_units'][0]
    best_ask = float(unit['ask_price'])
    best_bid = float(unit['bid_price'])
    return best_ask, best_bid

def fetch_filled_volume(uuid):
    try:
        if not uuid: return 0.0
        detail = safe_call(upbit.get_order, uuid)
        exec_vol = detail.get('executed_volume') or detail.get('volume')
        return float(exec_vol or 0.0)
    except Exception:
        return 0.0

# === 메인 루프 ===
while True:
    # 1) 분 동기화(1분봉), 아니면 POLL_SECONDS 대기
    if INTERVAL == "minute1":
        sleep_to_next_minute(offset_sec=1.0)
    else:
        time.sleep(POLL_SECONDS)

    logging.info("Heartbeat – bot running")

    try:
        # 2) 지표/신호 계산 (새 캔들 기준)
        df = compute_indicators(get_ohlc(TICKER, INTERVAL, WARMUP))
        ema15 = ema15_on_1min(df.index)
        row = df.iloc[-1]
        close, tgt, rsi, mas, mal = row.close, row.target, row.rsi, row.ma_s, row.ma_l
        ema_val = ema15.loc[row.name]

        # 3) 호가/잔고
        best_ask, best_bid = fetch_orderbook()
        krw_bal, btc_bal = fetch_balances()

        # 4) 외부 입출금 동기화
        diff = btc_bal - position.total_qty
        if abs(diff) > 1e-9:
            logging.warning(f"Manual BTC change Δ={diff:.8f}, sync")
            if diff > 0:
                ref_price = best_ask if best_ask > 0 else close
                position.add_entry(ref_price, diff)
                log_exec("MANUAL_IN", qty=diff, price=ref_price, pos_qty=position.total_qty)
            else:
                position.reduce_qty(-diff)
                log_exec("MANUAL_OUT", qty=-diff, pos_qty=position.total_qty)
            position.save()

        # 5) 매수 (분 경계에서만 평가)
        buy_cond = (close > tgt) and (rsi < RSI_THRESHOLD) and (mas > mal) and (close > ema_val) and (krw_bal > MIN_KRW_ORDER)
        if MAX_ENTRIES is not None and len(position.entries) >= MAX_ENTRIES:
            logging.info(f"Max entries reached ({MAX_ENTRIES}), skip buy")
        elif buy_cond:
            prev_avg = position.avg_price()
            if position.total_qty == 0 or close > prev_avg * 1.01:
                buy_amt = krw_bal * (1 - FEE)
                order = safe_call(upbit.buy_market_order, TICKER, buy_amt)
                uuid = order.get('uuid')
                filled = fetch_filled_volume(uuid)
                if filled <= 0:
                    filled = (buy_amt / max(best_ask, 1e-8)) * (1 - FEE)
                entry_price = best_ask
                position.add_entry(entry_price, filled)
                step = TRAILING_STEP_AFTER_TP1 if position.tp1_done else TRAILING_STEP
                position.trailing_stop = max(position.trailing_stop or 0.0, entry_price * (1 - step))
                position.save()
                log_exec("BUY", uuid=uuid, req_krw=buy_amt, filled=filled, price=entry_price,
                         book={"ask": best_ask, "bid": best_bid}, pos_avg=position.avg_price(),
                         pos_qty=position.total_qty, tp1_done=position.tp1_done, tstop=position.trailing_stop)
                logging.info(f"BUY #{len(position.entries)} @ {entry_price:.0f} qty={filled:.8f} (uuid={uuid})")

        # 6) 직전 봉 기준 trailing 상향만
        if position.total_qty > 0:
            step_now = TRAILING_STEP_AFTER_TP1 if position.tp1_done else TRAILING_STEP
            new_ts = row.high * (1 - step_now)
            if new_ts > (position.trailing_stop or 0.0):
                position.trailing_stop = new_ts
                position.save()
                logging.info(f"TRAIL UP (bar) @ {new_ts:.0f}")

        # 7) 빠른 청산 루프: 다음 분 시작 '직전'까지 intrabar TP/TS (동적 슬립)
        base = time.time()
        next_min = (int(base // 60) + 1) * 60        # 이번 사이클의 '다음 분 00초'
        deadline = next_min - 0.5                    # 경계 0.5초 전에는 루프 종료

        while True:
            now = time.time()
            if now >= deadline:
                break

            if position.total_qty <= 0:
                # 무포지션이면 가볍게 대기만
                remain = deadline - time.time()
                time.sleep(max(0.05, min(FAST_POLL, max(0.05, remain - 0.05))))
                continue

            best_ask, best_bid = fetch_orderbook()
            avg = position.avg_price()
            step = TRAILING_STEP_AFTER_TP1 if position.tp1_done else TRAILING_STEP

            # 트레일링 스탑 상향(호가 기준)
            new_ts = best_bid * (1 - step)
            if new_ts > (position.trailing_stop or 0.0):
                position.trailing_stop = new_ts
                position.save()
                logging.info(f"[FAST] TRAIL UP @ {new_ts:.0f}")

            acted = False

            # TP1
            if not position.tp1_done and best_bid >= avg * (1 + TP1_RATE):
                q1 = safe_qty(position.total_qty * TP1_PCT, position.total_qty)
                if can_sell(q1, best_bid) and q1 > 0:
                    safe_call(upbit.sell_market_order, TICKER, q1)
                    position.reduce_qty(q1)
                    position.tp1_done = True
                    ts_after = avg * (1 - TRAILING_STEP_AFTER_TP1)
                    position.trailing_stop = max(position.trailing_stop or 0.0, ts_after)
                    position.save()
                    log_exec("TP1", qty=q1, ref_price=avg * (1 + TP1_RATE), best_bid=best_bid,
                             avg_entry=avg, pos_qty=position.total_qty, tstop=position.trailing_stop)
                    logging.info(f"[FAST] TP1 SELL qty={q1:.8f}")
                    acted = True
                    # (원하면 여기서 TP2 즉시 재평가 추가 가능)

            # TP2
            elif position.tp1_done and best_bid >= avg * (1 + TP2_RATE):
                qty_all = safe_qty(position.total_qty, position.total_qty)
                if can_sell(qty_all, best_bid) and qty_all > 0:
                    safe_call(upbit.sell_market_order, TICKER, qty_all)
                    log_exec("TP2", qty=qty_all, ref_price=avg * (1 + TP2_RATE),
                             best_bid=best_bid, avg_entry=avg)
                    logging.info(f"[FAST] TP2 SELL qty={qty_all:.8f}")
                    position.clear()
                    position.save()
                    break

            # 트레일 스탑 발동
            if (position.total_qty > 0) and (position.trailing_stop is not None) and (best_bid <= position.trailing_stop):
                qty_all = safe_qty(position.total_qty, position.total_qty)
                if can_sell(qty_all, best_bid) and qty_all > 0:
                    safe_call(upbit.sell_market_order, TICKER, qty_all)
                    log_exec("TRAIL_STOP", qty=qty_all, stop_price=position.trailing_stop,
                             best_bid=best_bid, avg_entry=avg)
                    logging.info(f"[FAST] TRAIL STOP SELL qty={qty_all:.8f}")
                    position.clear()
                    position.save()
                    break

            # 동적 슬립: 데드라인 넘지 않도록 캡
            remain = deadline - time.time()
            nap = FAST_POLL if not acted else max(1, FAST_POLL // 2)
            time.sleep(max(0.05, min(nap, max(0.05, remain - 0.05))))

        position._err_count = 0

    except Exception as e:
        position._err_count += 1
        logging.error(f"Main loop error #{position._err_count}", exc_info=True)
        log_exec("ERROR", where="main_loop", msg=str(e))
        if position._err_count >= 5:
            logging.critical("Bot has failed repeatedly – manual check recommended")
            position._err_count = 0
        continue
