from __future__ import annotations

import hashlib
import math
from collections import deque
from dataclasses import dataclass
from statistics import median
from typing import Any, Deque, Iterable, Mapping, Optional, Sequence

from strategies.orderflow_models import BookLevel, BookSnapshot, RollingTradeMetrics, TradePrint

BOOK_WEIGHTS: tuple[float, ...] = (1.00, 0.70, 0.50, 0.35, 0.25)
TRADE_WINDOWS_SECONDS: tuple[int, ...] = (2, 5, 10, 30, 60)


def _float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_l2_book_snapshot(
    message: Mapping[str, Any],
    *,
    received_monotonic_ms: int,
    max_data_age_ms: Optional[int] = None,
    now_exchange_ts_ms: Optional[int] = None,
) -> Optional[BookSnapshot]:
    data = message.get("data", message)
    if not isinstance(data, Mapping):
        return None

    coin = str(data.get("coin") or "").upper()
    exchange_ts_ms = int(_float(data.get("time")) or 0)
    if not coin or exchange_ts_ms <= 0:
        return None
    if max_data_age_ms is not None and now_exchange_ts_ms is not None:
        if now_exchange_ts_ms - exchange_ts_ms > max_data_age_ms:
            return None

    raw_levels = data.get("levels")
    if not isinstance(raw_levels, Sequence) or len(raw_levels) < 2:
        return None

    def _parse_side(levels: Any, reverse: bool) -> tuple[BookLevel, ...]:
        parsed: list[BookLevel] = []
        if not isinstance(levels, Sequence):
            return ()
        for level in levels:
            if not isinstance(level, Mapping):
                continue
            px = _float(level.get("px"))
            sz = _float(level.get("sz"))
            n = int(_float(level.get("n")) or 0)
            if px is None or sz is None or px <= 0.0 or sz <= 0.0:
                continue
            parsed.append(BookLevel(price=px, size=sz, orders=n))
        parsed.sort(key=lambda item: item.price, reverse=reverse)
        return tuple(parsed)

    bids = _parse_side(raw_levels[0], reverse=True)
    asks = _parse_side(raw_levels[1], reverse=False)
    if not bids or not asks:
        return None

    best_bid = bids[0].price
    best_ask = asks[0].price
    if best_bid <= 0.0 or best_ask <= 0.0 or best_bid >= best_ask:
        return None

    mid = (best_bid + best_ask) / 2.0
    spread = best_ask - best_bid
    spread_bps = (spread / mid) * 10_000.0 if mid > 0.0 else float("inf")
    return BookSnapshot(
        coin=coin,
        exchange_ts_ms=exchange_ts_ms,
        received_monotonic_ms=received_monotonic_ms,
        bids=bids,
        asks=asks,
        best_bid=best_bid,
        best_ask=best_ask,
        mid=mid,
        spread=spread,
        spread_bps=spread_bps,
    )


def parse_trade_print(
    trade: Mapping[str, Any],
    *,
    received_monotonic_ms: int,
) -> Optional[TradePrint]:
    coin = str(trade.get("coin") or "").upper()
    side = str(trade.get("side") or "").upper()
    px = _float(trade.get("px"))
    sz = _float(trade.get("sz"))
    exchange_ts_ms = int(_float(trade.get("time")) or 0)
    if not coin or side not in {"A", "B"} or px is None or sz is None:
        return None
    if px <= 0.0 or sz <= 0.0 or exchange_ts_ms <= 0:
        return None

    tid = trade.get("tid")
    if tid is None:
        fallback = f"{coin}|{exchange_ts_ms}|{side}|{px:.10f}|{sz:.10f}"
        tid = hashlib.sha1(fallback.encode("ascii")).hexdigest()[:16]
    trade_id = str(tid)
    return TradePrint(
        coin=coin,
        side=side,
        price=px,
        size=sz,
        exchange_ts_ms=exchange_ts_ms,
        received_monotonic_ms=received_monotonic_ms,
        trade_id=trade_id,
        is_aggressive_buy=(side == "B"),
        is_aggressive_sell=(side == "A"),
    )


def weighted_book_imbalance(
    bids: Sequence[BookLevel],
    asks: Sequence[BookLevel],
    *,
    weights: Sequence[float] = BOOK_WEIGHTS,
) -> tuple[float, float, float]:
    weighted_bid = sum(weight * level.size for weight, level in zip(weights, bids))
    weighted_ask = sum(weight * level.size for weight, level in zip(weights, asks))
    denominator = weighted_bid + weighted_ask
    imbalance = ((weighted_bid - weighted_ask) / denominator) if denominator > 0.0 else 0.0
    return weighted_bid, weighted_ask, imbalance


def compute_microprice(book: BookSnapshot) -> tuple[float, float]:
    best_bid_size = book.bids[0].size
    best_ask_size = book.asks[0].size
    denominator = best_bid_size + best_ask_size
    if denominator <= 0.0:
        return book.mid, 0.0
    microprice = (
        (book.best_ask * best_bid_size) + (book.best_bid * best_ask_size)
    ) / denominator
    micro_bias = ((microprice - book.mid) / book.spread) if book.spread > 0.0 else 0.0
    return microprice, micro_bias


def executable_depth_notional(
    book: BookSnapshot,
    *,
    depth_bps: float,
) -> tuple[float, float]:
    bid_floor = book.mid * (1.0 - depth_bps / 10_000.0)
    ask_ceiling = book.mid * (1.0 + depth_bps / 10_000.0)
    bid_notional = sum(level.price * level.size for level in book.bids if level.price >= bid_floor)
    ask_notional = sum(level.price * level.size for level in book.asks if level.price <= ask_ceiling)
    return bid_notional, ask_notional


def compute_executable_depth_ratio(
    bid_depth_notional: float,
    ask_depth_notional: float,
    intended_order_notional: float,
) -> float:
    if intended_order_notional <= 0.0:
        return 0.0
    return min(bid_depth_notional, ask_depth_notional) / intended_order_notional


def compute_trade_window_metrics(
    trades: Iterable[TradePrint],
    *,
    window_seconds: int,
    now_exchange_ts_ms: int,
) -> RollingTradeMetrics:
    cutoff = now_exchange_ts_ms - (window_seconds * 1000)
    buy_volume = 0.0
    sell_volume = 0.0
    trade_count = 0
    for trade in trades:
        if trade.exchange_ts_ms < cutoff:
            continue
        notional = trade.price * trade.size
        if trade.is_aggressive_buy:
            buy_volume += notional
        elif trade.is_aggressive_sell:
            sell_volume += notional
        trade_count += 1
    total_volume = buy_volume + sell_volume
    imbalance = ((buy_volume - sell_volume) / total_volume) if total_volume > 0.0 else 0.0
    return RollingTradeMetrics(
        window_seconds=window_seconds,
        buy_volume=buy_volume,
        sell_volume=sell_volume,
        total_volume=total_volume,
        trade_imbalance=imbalance,
        trade_count=trade_count,
    )


class RollingTradeStore:
    def __init__(self, max_window_seconds: int = 300) -> None:
        self._trades: Deque[TradePrint] = deque()
        self._seen: set[tuple[str, int, str]] = set()
        self._max_window_ms = max_window_seconds * 1000

    def add_trade(self, trade: TradePrint) -> bool:
        key = (trade.coin, trade.exchange_ts_ms, trade.trade_id)
        if key in self._seen:
            return False
        if self._trades and trade.exchange_ts_ms < self._trades[-1].exchange_ts_ms:
            # Conservative policy for live feeds: ignore older out-of-order prints.
            return False
        self._trades.append(trade)
        self._seen.add(key)
        self.prune(now_exchange_ts_ms=trade.exchange_ts_ms)
        return True

    def prune(self, *, now_exchange_ts_ms: int) -> None:
        cutoff = now_exchange_ts_ms - self._max_window_ms
        while self._trades and self._trades[0].exchange_ts_ms < cutoff:
            trade = self._trades.popleft()
            self._seen.discard((trade.coin, trade.exchange_ts_ms, trade.trade_id))

    def metrics(self, *, now_exchange_ts_ms: int) -> dict[int, RollingTradeMetrics]:
        self.prune(now_exchange_ts_ms=now_exchange_ts_ms)
        return {
            seconds: compute_trade_window_metrics(
                self._trades,
                window_seconds=seconds,
                now_exchange_ts_ms=now_exchange_ts_ms,
            )
            for seconds in TRADE_WINDOWS_SECONDS
        }

    def recent_trades(self) -> tuple[TradePrint, ...]:
        return tuple(self._trades)

    def aggressive_notional_bucket_medians(
        self,
        *,
        side: str,
        bucket_seconds: int,
        lookback_seconds: int,
        now_exchange_ts_ms: int,
    ) -> float:
        lookback_cutoff = now_exchange_ts_ms - (lookback_seconds * 1000)
        bucket_ms = bucket_seconds * 1000
        buckets: dict[int, float] = {}
        for trade in self._trades:
            if trade.exchange_ts_ms < lookback_cutoff:
                continue
            if side == "long" and not trade.is_aggressive_buy:
                continue
            if side == "short" and not trade.is_aggressive_sell:
                continue
            bucket = (trade.exchange_ts_ms // bucket_ms) * bucket_ms
            buckets[bucket] = buckets.get(bucket, 0.0) + (trade.price * trade.size)
        non_zero = [value for key, value in sorted(buckets.items()) if key < ((now_exchange_ts_ms // bucket_ms) * bucket_ms)]
        if len(non_zero) < 3:
            return 0.0
        return float(median(non_zero))


def trend_efficiency(closes: Sequence[float]) -> float:
    if len(closes) < 2:
        return 0.0
    net_change = abs(closes[-1] - closes[0])
    path_length = sum(abs(current - previous) for previous, current in zip(closes, closes[1:]))
    return (net_change / path_length) if path_length > 0.0 else 0.0


def rolling_vwap(candles: Sequence[Mapping[str, Any]]) -> Optional[float]:
    numerator = 0.0
    denominator = 0.0
    for candle in candles:
        high = _float(candle.get("h"))
        low = _float(candle.get("l"))
        close = _float(candle.get("c"))
        volume = _float(candle.get("v"))
        if None in {high, low, close, volume}:
            continue
        if volume is None or volume <= 0.0:
            continue
        typical = (high + low + close) / 3.0
        numerator += typical * volume
        denominator += volume
    if denominator <= 0.0:
        return None
    return numerator / denominator


def simple_atr(candles: Sequence[Mapping[str, Any]], period: int = 14) -> Optional[float]:
    if len(candles) < period + 1:
        return None
    true_ranges: list[float] = []
    previous_close: Optional[float] = None
    for candle in candles[-(period + 1):]:
        high = _float(candle.get("h"))
        low = _float(candle.get("l"))
        close = _float(candle.get("c"))
        if None in {high, low, close}:
            return None
        if previous_close is None:
            true_range = high - low
        else:
            true_range = max(high - low, abs(high - previous_close), abs(low - previous_close))
        true_ranges.append(true_range)
        previous_close = close
    return sum(true_ranges[-period:]) / float(period)


def median_spread_bps(history: Sequence[float]) -> float:
    usable = [value for value in history if math.isfinite(value) and value >= 0.0]
    if not usable:
        return float("inf")
    return float(median(usable))


@dataclass(frozen=True)
class CostEstimate:
    maker_fee_bps: float
    taker_fee_bps: float
    spread_cost_bps: float
    slippage_bps: float
    estimated_round_trip_cost_bps: float


def estimate_round_trip_cost_bps(
    *,
    spread_bps: float,
    maker_fee_bps: float,
    taker_fee_bps: float,
    slippage_bps: float,
    entry_style: str,
    exit_style: str,
) -> CostEstimate:
    entry_fee = maker_fee_bps if entry_style == "maker" else taker_fee_bps
    exit_fee = maker_fee_bps if exit_style == "maker" else taker_fee_bps
    spread_cost = spread_bps if exit_style == "taker" else (spread_bps * 0.5)
    estimated = entry_fee + exit_fee + spread_cost + slippage_bps
    return CostEstimate(
        maker_fee_bps=maker_fee_bps,
        taker_fee_bps=taker_fee_bps,
        spread_cost_bps=spread_cost,
        slippage_bps=slippage_bps,
        estimated_round_trip_cost_bps=estimated,
    )
