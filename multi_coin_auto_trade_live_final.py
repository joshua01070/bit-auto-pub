#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
multi_coin_auto_trade.py (LIVE only, no dry-run, multi-symbol)

Applied:
  (A) 체결 리콘실리에이션: uuid→get_order(trades)→잔고 델타 fallback
  (B) pyupbit timeout=4s 몽키패치
  (C) 하트비트 워치독(20s) + 루프 하트비트(60s)
  (F) safe_call 재시도/백오프 래퍼
  (G) 오더북 호환 래퍼

Parity updates (this build):
  - [PTP]  분할매도 기준을 "초기 진입 수량"으로 통일(마지막 레벨은 잔량 전량)
  - [ENTRY] Donchian upper(직전 N봉) + 2틱 버퍼(prev_close > upper + 2*tick)
  - [DUST] 마지막 청산(TS/마지막 PTP/TP) 시 minKRW 검사 우회 +倒數2(마지막 전) 레벨에서 dust 사전 흡수

Signal/Exec:
  - 닫힌 5분봉 prev_close > DonchianUpper(prev N) + 2 ticks
  - (옵션) MA 필터(ms>ml)
  - PTP: 마지막 레벨은 '잔량 전량', 매도 전 최소주문금액 검사(전량일 때는 우회 시도)
  - TP1 체결 후 BE(본전 스탑)로 즉시 끌어올림 (avg*(1+fee+eps) 이상)
  - TS: 앵커(ask) 기반 상향 (고정 스텝)
  - HALT: halt.flag 존재 시 신규 매수만 차단(청산은 허용)
  - LIVE 가드: UPBIT_LIVE=1 없으면 즉시 종료
  - 멀티코인 예산: 루프마다 KRW 잔고로 budget_left를 계산해 진입 시 차감(경합 방지)
"""

import os, sys, time, json, math, logging, threading
from dataclasses import dataclass, asdict
from typing import List, Optional, Tuple, Dict, Any

import pandas as pd
import pyupbit

# =========================
# 0) 실행 가드 & 로깅
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

if os.getenv("UPBIT_LIVE") != "1":
    raise SystemExit("Refuse to run LIVE. Set environment UPBIT_LIVE=1 to confirm.")

# =========================
# 1) 설정
# =========================
ACCESS = ""
SECRET = ""

# 거래 심볼들
TICKERS = ["KRW-BTC", "KRW-ETH"]

# 심볼별 파라미터(없으면 DEFAULTS 사용)
DEFAULTS = dict(
    use_ma = 1, ma_s = 10, ma_l = 30,
    don_n = 72,
    fee = 0.0007,              # 보수화
    min_krw_order = 6000,
    qty_step = 1e-8,
    buy_pct = 0.30,
    max_position_krw = None,   # per-entry cap
    trail_before = 0.075,      # 고정 스텝(PTP 전/후 동일하게 사용 가능)
    trail_after  = 0.075,
    ptp_levels = "",           # 기본은 BASE(분할 없음)
    tp_rate = 0.0,             # 기본 0(PTP 전용 코인 제외)
    eps_exec = 0.0003,         # BE/사이징 보수 여유
    entry_tick_buffer = 2,     # ★ 2틱 버퍼(백테스터와 일치)
)

# 채택안 반영 (코인별 오버라이드)
SYMBOL_PARAMS: Dict[str, Dict[str, Any]] = {
    "KRW-BTC": {
        "use_ma": 1, "ma_s": 10, "ma_l": 60,
        "don_n": 72,
        "fee": 0.0007,
        "buy_pct": 0.40,
        "trail_before": 0.08, "trail_after": 0.08,   # 고정 트레일
        "ptp_levels": "",                            # BASE
        "tp_rate": 0.035,                            # 단일 TP
        "entry_tick_buffer": 2,
    },
    "KRW-ETH": {
        "use_ma": 1, "ma_s": 10, "ma_l": 30,
        "don_n": 72,
        "fee": 0.0007,
        "buy_pct": 0.30,
        "trail_before": 0.075, "trail_after": 0.075, # 고정 트레일
        "ptp_levels": "0.015:0.4,0.035:0.3,0.06:0.3",
        "tp_rate": 0.03,                             # 잔량 단일 TP
        "entry_tick_buffer": 2,
    },
}

POLL_SEC = 1
STATE_DIR = "state_live"
HALT_FILE = "halt.flag"

# Heartbeat/Watchdog
HEARTBEAT_SEC = 60
HB_WATCHDOG_SEC = 20

# =========================
# 2) 유틸 & 공통
# =========================
def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def sym_to_ccy(symbol: str) -> str:
    return symbol.split("-")[1]

def tick_size_for(krw_price: float) -> float:
    p = krw_price
    if p >= 2_000_000: return 1000
    if p >= 1_000_000: return 500
    if p >=   500_000: return 100
    if p >=   100_000: return 50
    if p >=    10_000: return 10
    if p >=     1_000: return 1
    if p >=       100: return 0.1
    if p >=        10: return 0.01
    return 0.001

def round_to_tick(price: float) -> float:
    t = tick_size_for(price)
    return math.floor(price / t) * t

def floor_to_step(x: float, step: float) -> float:
    return math.floor(x / step) * step

def be_stop(avg: float, fee: float, cushion: float = 0.0003) -> float:
    """Break-even stop above avg (conservative margin)."""
    return avg * (1.0 + fee + cushion)

def parse_ptp_levels(spec: str) -> List[Tuple[float, float]]:
    out = []
    for part in spec.split(","):
        part = part.strip()
        if not part: continue
        lv, pct = part.split(":")
        out.append((float(lv), float(pct)))
    total = sum(p for _, p in out)
    if total > 1.001:
        logging.warning(f"[PTP] cumulative fraction > 1.0 ({total:.3f}), will still sell last level as full remainder.")
    return out

def halted() -> bool:
    return os.path.exists(HALT_FILE)

# (F) safe_call: 재시도/백오프
def safe_call(fn, *args, tries: int = 3, delay: float = 0.4, backoff: float = 1.7, **kwargs):
    t = delay
    for i in range(tries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if i == tries - 1:
                logging.error(f"[safe_call] {fn.__name__} failed after {tries} tries: {e}")
                raise
            logging.warning(f"[safe_call] {fn.__name__} error ({i+1}/{tries}): {e}. retry in {t:.2f}s")
            time.sleep(t)
            t *= backoff

# (B) pyupbit timeout=4s 몽키패치
def patch_pyupbit_timeout():
    try:
        import pyupbit.request_api as _ra
        _orig_pub = _ra._call_public_api
        def _pub(url, **kw):
            kw.setdefault("timeout", 4)
            return _orig_pub(url, **kw)
        _ra._call_public_api = _pub
        if hasattr(_ra, "_call_private_api"):
            _orig_pri = _ra._call_private_api
            def _pri(url, **kw):
                kw.setdefault("timeout", 4)
                return _orig_pri(url, **kw)
            _ra._call_private_api = _pri
        logging.info("[init] pyupbit timeout=4s patch OK")
    except Exception as e:
        logging.warning(f"[init] timeout patch failed: {e}")

patch_pyupbit_timeout()

# (G) 오더북 호환
def normalize_orderbook(resp: Any, symbol: str) -> Tuple[float, float]:
    if resp is None:
        raise RuntimeError("orderbook None")
    data = resp
    if isinstance(data, list):
        item = data[0] if data else None
    elif isinstance(data, dict):
        item = data
    else:
        raise RuntimeError("orderbook unknown type")

    if not item:
        raise RuntimeError("orderbook empty item")

    if "orderbook_units" in item:
        units = item["orderbook_units"]
        if not units: raise RuntimeError("orderbook units empty")
        bid = float(units[0]["bid_price"])
        ask = float(units[0]["ask_price"])
        return bid, ask

    for k in ("bid_price", "best_bid", "bids"):
        if k in item:
            bid = float(item[k]) if isinstance(item[k], (int,float,str)) else float(item[k][0])
            break
    else:
        raise RuntimeError("orderbook no bid_price")

    for k in ("ask_price", "best_ask", "asks"):
        if k in item:
            ask = float(item[k]) if isinstance(item[k], (int,float,str)) else float(item[k][0])
            break
    else:
        raise RuntimeError("orderbook no ask_price")

    return bid, ask

# =========================
# 3) 브로커/피드
# =========================
class UpbitBroker:
    def __init__(self, access: str, secret: str):
        self.upbit = pyupbit.Upbit(access, secret)

    def best_bid_ask(self, symbol: str) -> Tuple[float, float]:
        ob = safe_call(pyupbit.get_orderbook, symbol)
        bid, ask = normalize_orderbook(ob, symbol)
        return bid, ask

    def balances(self) -> List[dict]:
        return safe_call(self.upbit.get_balances) or []

    def krw_balance(self) -> float:
        for b in self.balances():
            if b.get("currency") == "KRW":
                return float(b.get("balance") or 0.0)
        return 0.0

    def base_position(self, symbol: str) -> Tuple[float, float]:
        base = sym_to_ccy(symbol)
        for b in self.balances():
            if b.get("currency") == base:
                qty = float(b.get("balance") or 0.0)
                avg = float(b.get("avg_buy_price") or 0.0)
                return qty, avg
        return 0.0, 0.0

    def buy_market_krw(self, symbol: str, krw: float) -> dict:
        return safe_call(self.upbit.buy_market_order, symbol, krw)

    def sell_market_qty(self, symbol: str, qty: float) -> dict:
        return safe_call(self.upbit.sell_market_order, symbol, qty)

    def get_order(self, uuid: str) -> dict:
        return safe_call(self.upbit.get_order, uuid)

class UpbitFeed:
    def __init__(self, symbol: str):
        self.symbol = symbol

    def fetch_1m_df(self, count: int = 400) -> pd.DataFrame:
        df = safe_call(pyupbit.get_ohlcv, self.symbol, interval="minute1", count=count)
        if df is None or df.empty:
            raise RuntimeError(f"failed to fetch 1m ohlcv: {self.symbol}")
        return df

    @staticmethod
    def aggregate_5m_closed(df1m: pd.DataFrame) -> pd.DataFrame:
        df5 = df1m.resample("5min").agg({
            "open":"first","high":"max","low":"min","close":"last","volume":"sum","value":"sum"
        })
        df5 = df5.dropna()
        if len(df5) >= 1:
            df5 = df5.iloc[:-1]  # 닫힌 캔들만
        return df5

# =========================
# 4) 상태
# =========================
@dataclass
class PositionState:
    has_pos: bool = False
    qty: float = 0.0
    avg: float = 0.0
    stop: float = 0.0
    ts_anchor: float = 0.0
    ptp_hits: List[float] = None
    tp1_done: bool = False
    init_qty: float = 0.0         # ★ 초기 진입 수량(PTP 기준)

    def to_json(self) -> dict:
        d = asdict(self)
        d["ptp_hits"] = self.ptp_hits or []
        return d

    @staticmethod
    def from_json(d: dict) -> "PositionState":
        st = PositionState()
        st.has_pos = bool(d.get("has_pos", False))
        st.qty = float(d.get("qty", 0.0))
        st.avg = float(d.get("avg", 0.0))
        st.stop = float(d.get("stop", 0.0))
        st.ts_anchor = float(d.get("ts_anchor", 0.0))
        st.ptp_hits = list(d.get("ptp_hits", []))
        st.tp1_done = bool(d.get("tp1_done", False))
        st.init_qty = float(d.get("init_qty", 0.0))
        return st

# =========================
# 5) 전략(심볼 개별 인스턴스)
# =========================
class SymbolStrategy:
    def __init__(self, symbol: str, broker: UpbitBroker, params: Dict[str, Any]):
        self.symbol = symbol
        self.broker = broker
        self.p = {**DEFAULTS, **(params or {})}
        self.state_path = os.path.join(STATE_DIR, f"{self.symbol.replace('-','_')}.json")
        self.state = self._load_state()
        self.ptp_levels = parse_ptp_levels(self.p["ptp_levels"])
        self.last_5m_index = None

    # persistence
    def _load_state(self) -> PositionState:
        ensure_dir(STATE_DIR)
        if os.path.exists(self.state_path):
            try:
                with open(self.state_path, "r", encoding="utf-8") as f:
                    d = json.load(f)
                return PositionState.from_json(d)
            except Exception:
                logging.exception(f"[{self.symbol}] state load failed; starting fresh")
        return PositionState(has_pos=False, ptp_hits=[])

    def _save_state(self):
        with open(self.state_path, "w", encoding="utf-8") as f:
            json.dump(self.state.to_json(), f, ensure_ascii=False)

    # indicators
    @staticmethod
    def sma(series: pd.Series, n: int) -> Optional[float]:
        if len(series) < n: return None
        return float(series.iloc[-n:].mean())

    # trading helpers
    def _try_sell_qty(self, qty: float, bid: float, force: bool = False) -> bool:
        """force=True면 minKRW 검사를 우회(마지막 전량 청산 시도). 거래소가 거부할 수는 있음."""
        qty = floor_to_step(qty, self.p["qty_step"])
        if qty <= 0:
            return False
        if (not force) and (bid * qty) < self.p["min_krw_order"]:
            logging.info(f"[{self.symbol}] [SELL skip] notional < min ({bid*qty:.1f} < {self.p['min_krw_order']})")
            return False
        try:
            od = self.broker.sell_market_qty(self.symbol, qty)
            logging.info(f"[{self.symbol}] [SELL] qty={qty:.8f} resp={str(od)[:120]}")
            return True
        except Exception:
            logging.exception(f"[{self.symbol}] [SELL] failed")
            return False

    def _refresh_position_from_exchange(self):
        qty, avg = self.broker.base_position(self.symbol)
        if qty <= 0:
            if self.state.has_pos:
                logging.info(f"[{self.symbol}] [POS] exchange shows flat; clearing local state")
            self.state = PositionState(has_pos=False, ptp_hits=[])
            self._save_state()
            return

        self.state.has_pos = True
        self.state.qty = qty
        self.state.avg = avg
        # 초기 진입 수량 복원(재기동 시 fallback)
        if self.state.init_qty <= 0:
            self.state.init_qty = qty
        # anchor/stop 초기화: BE와 트레일 중 더 높은 쪽 (No-Loosen)
        step = self.p["trail_after"] if self.state.tp1_done else self.p["trail_before"]
        if self.state.ts_anchor <= 0:
            self.state.ts_anchor = self.state.avg
        be = be_stop(self.state.avg, self.p["fee"], self.p["eps_exec"])
        trail_stop = round_to_tick(self.state.ts_anchor * (1.0 - step))
        new_stop = max(be, trail_stop)
        if self.state.stop <= 0 or new_stop > self.state.stop:
            self.state.stop = new_stop
        if self.state.ptp_hits is None:
            self.state.ptp_hits = []
        self._save_state()

    # (A) 체결 리콘실리에이션
    def _resolve_buy_fill(self, uuid: Optional[str], pre_qty: float, pre_avg: float, budget: float):
        qty_new, avg_new = self.broker.base_position(self.symbol)
        if uuid:
            t0 = time.time()
            while time.time() - t0 < 4.0:
                try:
                    od = self.broker.get_order(uuid)
                    trades = od.get("trades") if isinstance(od, dict) else None
                    if trades:
                        vol_sum = 0.0; funds_sum = 0.0
                        for tr in trades:
                            v = float(tr.get("volume") or tr.get("qty") or 0.0)
                            f = float(tr.get("funds")  or (float(tr.get("price",0.0))*v))
                            vol_sum += v; funds_sum += f
                        if vol_sum > 0:
                            fill_qty = vol_sum
                            fill_avg = funds_sum / vol_sum
                            logging.info(f"[{self.symbol}] [FILL] buy resolved by trades: qty={fill_qty:.8f}, avg~{fill_avg:.0f}")
                            break
                except Exception as e:
                    logging.warning(f"[{self.symbol}] [FILL] get_order error: {e}")
                time.sleep(0.5)
            else:
                fill_qty = max(0.0, qty_new - pre_qty)
                if fill_qty > 0:
                    fill_avg = max(1e-8, (avg_new * qty_new - pre_avg * pre_qty) / fill_qty)
                    logging.info(f"[{self.symbol}] [FILL] fallback by balances: qty={fill_qty:.8f}, avg~{fill_avg:.0f}")
                else:
                    logging.warning(f"[{self.symbol}] [FILL] uncertain → using estimate from budget")
        else:
            fill_qty = max(0.0, qty_new - pre_qty)
            if fill_qty > 0:
                fill_avg = max(1e-8, (avg_new * qty_new - pre_avg * pre_qty) / fill_qty)
                logging.info(f"[{self.symbol}] [FILL] fallback by balances(no uuid): qty={fill_qty:.8f}, avg~{fill_avg:.0f}")
            else:
                logging.warning(f"[{self.symbol}] [FILL] uncertain(no uuid) → using estimate")

        self._refresh_position_from_exchange()
        # ★ 초기 진입 수량 확정
        self.state.init_qty = self.state.qty
        self._save_state()

    # 5분봉 신호 (닫힌 캔들)
    def on_bar_close_5m(self, df5: pd.DataFrame, budget_left: float) -> float:
        closes = df5["close"]; highs = df5["high"]
        n = int(self.p["don_n"])

        # Donchian upper: "직전 N봉" (현재봉 제외)
        if len(highs) < n + 1:
            return budget_left
        upper = float(highs.iloc[-(n+1):-1].max())

        prev_close = float(closes.iloc[-1])

        # MA filter (ms>ml, 동률 불가)
        if self.p["use_ma"]:
            ms = self.sma(closes, int(self.p["ma_s"]))
            ml = self.sma(closes, int(self.p["ma_l"]))
            if ms is None or ml is None or ms <= ml:
                return budget_left

        # ★ Entry: prev_close > upper + tick*buffer (백테스트와 동일: '>')
        tick = tick_size_for(upper)
        up_buf = upper + tick * int(self.p.get("entry_tick_buffer", 2))
        if (not self.state.has_pos) and (not halted()) and (prev_close > up_buf):
            budget_left = self._enter_position(budget_left)
        return budget_left

    def _enter_position(self, budget_left: float) -> float:
        # 멀티코인 경합 방지를 위해 루프에서 전달된 budget_left 사용
        if budget_left <= 0:
            logging.info(f"[{self.symbol}] [SKIP BUY] budget_left=0")
            return 0.0

        # 1) 이 심볼의 예산
        krw_alloc = budget_left * float(self.p["buy_pct"])
        if self.p["max_position_krw"] is not None:
            krw_alloc = min(krw_alloc, self.p["max_position_krw"])
        krw_alloc = math.floor(krw_alloc)

        if krw_alloc < self.p["min_krw_order"]:
            logging.info(f"[{self.symbol}] [SKIP BUY] alloc<{self.p['min_krw_order']}, alloc={krw_alloc:.0f}")
            return budget_left

        # 2) 잔고 스냅샷
        pre_qty, pre_avg = self.broker.base_position(self.symbol)

        # 3) 주문
        try:
            od = self.broker.buy_market_krw(self.symbol, krw_alloc)
            uuid = od.get("uuid") if isinstance(od, dict) else None
            logging.info(f"[{self.symbol}] [BUY] krw={krw_alloc} uuid={uuid}")
        except Exception:
            logging.exception(f"[{self.symbol}] [BUY] failed")
            return budget_left

        # 4) 리콘실리에이션
        time.sleep(0.6)
        self._resolve_buy_fill(uuid, pre_qty, pre_avg, krw_alloc)

        # 5) 예산 차감 & 상태 저장
        budget_left = max(0.0, budget_left - krw_alloc)
        self._save_state()
        return budget_left

    def _after_tp1_bump_be(self):
        be = be_stop(self.state.avg, self.p["fee"], self.p["eps_exec"])
        if be > self.state.stop:
            self.state.stop = be
            logging.info(f"[{self.symbol}] [BE BUMP] stop -> {self.state.stop:.0f}")
            self._save_state()

    def manage_intrabar(self, bid: float, ask: float):
        if not self.state.has_pos:
            return

        step = self.p["trail_after"] if self.state.tp1_done else self.p["trail_before"]

        # 앵커 상향 (ask 기준) & stop No-Loosen
        if ask > self.state.ts_anchor:
            self.state.ts_anchor = ask
            new_stop = round_to_tick(self.state.ts_anchor * (1.0 - step))
            if new_stop > self.state.stop:
                self.state.stop = new_stop
                logging.info(f"[{self.symbol}] [TRAIL UP] anchor={self.state.ts_anchor:.0f} stop={self.state.stop:.0f}")
                self._save_state()

        # TS 체결 (LH: TS가 PTP/TP보다 우선) — 마지막 전량: minKRW 우회(force=True)
        if bid <= self.state.stop:
            qty = self.state.qty
            if self._try_sell_qty(qty, bid, force=True):
                logging.info(f"[{self.symbol}] [TS EXIT] all-out by trailing stop")
                self.state = PositionState(has_pos=False, ptp_hits=[])
                self._save_state()
            return

        # PTP (있다면) — 초기 수량 기준, 마지막 전에서 dust 흡수
        if self.ptp_levels and self.state.qty > 0:
            cur = bid
            base = self.state.init_qty if self.state.init_qty > 0 else self.state.qty
            for i, (lv, pct) in enumerate(self.ptp_levels):
                if self.state.ptp_hits and (lv in self.state.ptp_hits):
                    continue
                target = self.state.avg * (1.0 + lv)
                if cur >= target:
                    is_last = (i == len(self.ptp_levels) - 1)
                    qty_part = self.state.qty if is_last else base * pct
                    qty_part = min(qty_part, self.state.qty)
                    # 倒數2 레벨에서 dust 사전 흡수
                    if (not is_last):
                        rem_after = self.state.qty - qty_part
                        if (rem_after * bid) < self.p["min_krw_order"]:
                            qty_part = self.state.qty
                            is_last = True  # 이번에 전량 처리
                    qty_part = floor_to_step(qty_part, self.p["qty_step"])
                    if qty_part <= 0:
                        self.state.ptp_hits = list(set((self.state.ptp_hits or []) + [lv]))
                        self._save_state()
                        continue

                    if self._try_sell_qty(qty_part, bid, force=is_last):
                        self.state.qty = max(0.0, self.state.qty - qty_part)
                        if (self.state.ptp_hits is None):
                            self.state.ptp_hits = []
                        if lv not in self.state.ptp_hits:
                            self.state.ptp_hits.append(lv)
                        if not self.state.tp1_done:
                            self.state.tp1_done = True
                            self._after_tp1_bump_be()

                        if self.state.qty <= 0:
                            logging.info(f"[{self.symbol}] [PTP EXIT] all-out by last PTP level")
                            self.state = PositionState(has_pos=False, ptp_hits=[])
                        self._save_state()

        # 단일 TP (잔량 정리; BTC=전체, ETH=PTP 이후 잔량) — 마지막 전량: force=True
        tp_rate = float(self.p.get("tp_rate") or 0.0)
        if self.state.qty > 0 and tp_rate > 0.0 and bid >= self.state.avg * (1.0 + tp_rate):
            if self._try_sell_qty(self.state.qty, bid, force=True):
                logging.info(f"[{self.symbol}] [TP EXIT] all-out by TP {tp_rate:.4f}")
                self.state = PositionState(has_pos=False, ptp_hits=[])
                self._save_state()
            return

# =========================
# 6) 하트비트/메인 루프
# =========================
def start_watchdog():
    def _run():
        while True:
            logging.info("[hb/watchdog] alive")
            time.sleep(HB_WATCHDOG_SEC)
    t = threading.Thread(target=_run, daemon=True); t.start()

def main():
    if not ACCESS or not SECRET:
        raise SystemExit("ACCESS/SECRET required for LIVE trading.")

    logging.info("=== LIVE MODE: Upbit REAL trading started (MULTI) ===")
    logging.info(f"Tickers={TICKERS}")

    broker = UpbitBroker(ACCESS, SECRET)

    # 부팅 점검
    try:
        _ = broker.krw_balance()
        for sym in TICKERS:
            bid, ask = broker.best_bid_ask(sym)
            logging.info(f"[boot] {sym} bid/ask: {bid:.0f}/{ask:.0f}")
    except Exception:
        logging.exception("boot check failed")
        raise SystemExit(1)

    # 전략 인스턴스 & 피드
    feeds: Dict[str, UpbitFeed] = {s: UpbitFeed(s) for s in TICKERS}
    strats: Dict[str, SymbolStrategy] = {s: SymbolStrategy(s, broker, SYMBOL_PARAMS.get(s, {})) for s in TICKERS}

    # 거래소 포지션과 동기화
    for s in TICKERS:
        strats[s]._refresh_position_from_exchange()

    start_watchdog()
    hb_t0 = time.time()
    last5_idx: Dict[str, Any] = {s: None for s in TICKERS}

    while True:
        try:
            # 0) 루프 시작 시점에 KRW 잔고를 한 번만 읽고 budget_left로 사용
            budget_left = broker.krw_balance()

            # 1) 각 심볼별 1분봉 & 닫힌 5분 신호 처리
            for s in TICKERS:
                df1 = feeds[s].fetch_1m_df(count=400)
                df5 = UpbitFeed.aggregate_5m_closed(df1)
                if not df5.empty:
                    cur_5m_index = df5.index[-1]
                    if last5_idx[s] is None or cur_5m_index != last5_idx[s]:
                        budget_left = strats[s].on_bar_close_5m(df5, budget_left)
                        last5_idx[s] = cur_5m_index

            # 2) 인트라바 포지션 관리(각 심볼 호가 1회씩)
            for s in TICKERS:
                bid, ask = broker.best_bid_ask(s)
                strats[s].manage_intrabar(bid, ask)

            # 3) 루프 하트비트 (모든 심볼 요약)
            if time.time() - hb_t0 >= HEARTBEAT_SEC:
                lines = []
                for s, stg in strats.items():
                    st = stg.state
                    lines.append(f"{s}: pos={st.qty:.8f}@{st.avg:.0f} stop={st.stop:.0f} tp1={st.tp1_done} init={st.init_qty:.8f}")
                logging.info("[hb] " + " | ".join(lines))
                hb_t0 = time.time()

        except Exception:
            logging.exception("main loop error")

        time.sleep(POLL_SEC)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("bye")
