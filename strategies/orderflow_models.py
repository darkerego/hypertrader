from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Literal, Optional


OrderFlowSide = Literal["long", "short"]


@dataclass(frozen=True)
class BookLevel:
    price: float
    size: float
    orders: int = 0


@dataclass(frozen=True)
class BookSnapshot:
    coin: str
    exchange_ts_ms: int
    received_monotonic_ms: int
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]
    best_bid: float
    best_ask: float
    mid: float
    spread: float
    spread_bps: float


@dataclass(frozen=True)
class TradePrint:
    coin: str
    side: str
    price: float
    size: float
    exchange_ts_ms: int
    received_monotonic_ms: int
    trade_id: str
    is_aggressive_buy: bool
    is_aggressive_sell: bool


@dataclass(frozen=True)
class RollingTradeMetrics:
    window_seconds: int
    buy_volume: float
    sell_volume: float
    total_volume: float
    trade_imbalance: float
    trade_count: int


@dataclass(frozen=True)
class OrderFlowMetrics:
    book: BookSnapshot
    weighted_bid: float
    weighted_ask: float
    book_imbalance: float
    microprice: float
    micro_bias: float
    executable_bid_depth_notional: float
    executable_ask_depth_notional: float
    executable_depth_ratio: float
    trade_metrics_2s: RollingTradeMetrics
    trade_metrics_5s: RollingTradeMetrics
    trade_metrics_10s: RollingTradeMetrics
    trade_metrics_30s: RollingTradeMetrics
    trade_metrics_60s: RollingTradeMetrics


@dataclass(frozen=True)
class ImpulseState:
    side: OrderFlowSide
    start_ts_ms: int
    end_ts_ms: int
    origin_price: float
    extreme_price: float
    impulse_range: float
    atr_1m: float
    spread_bps: float
    trade_imbalance_10s: float
    aggressive_notional_ratio: float
    feed_generation: int


@dataclass(frozen=True)
class PullbackState:
    side: OrderFlowSide
    started_ts_ms: int
    last_ts_ms: int
    retracement: float
    pullback_low: float
    pullback_high: float
    support_price: float
    valid: bool
    reason: str


class OrderFlowSetupState(str, Enum):
    IDLE = "IDLE"
    REGIME_VALID = "REGIME_VALID"
    IMPULSE_DETECTED = "IMPULSE_DETECTED"
    PULLBACK_ACTIVE = "PULLBACK_ACTIVE"
    FLOW_CONFIRMED = "FLOW_CONFIRMED"
    ENTRY_PENDING = "ENTRY_PENDING"
    POSITION_OPEN = "POSITION_OPEN"
    EXITING = "EXITING"
    COOLDOWN = "COOLDOWN"


@dataclass
class OrderFlowSetup:
    setup_id: str
    coin: str
    side: Optional[OrderFlowSide] = None
    state: OrderFlowSetupState = OrderFlowSetupState.IDLE
    state_since_ms: int = 0
    state_reason: str = ""
    regime_score: float = 0.0
    scan_score: float = 0.0
    last_event_ms: int = 0
    last_transition_ms: int = 0
    feed_generation: int = 0
    signal_claimed: bool = False
    impulse: Optional[ImpulseState] = None
    pullback: Optional[PullbackState] = None
    confirmation_history: list[bool] = field(default_factory=list)
    last_signal_ts_ms: int = 0
    cooldown_until_ms: int = 0
    invalidation_reason: str = ""


@dataclass(frozen=True)
class OrderFlowSignal:
    setup_id: str
    coin: str
    side: OrderFlowSide
    signal_ts_ms: int
    feed_generation: int
    score: float
    trend_quality: float
    impulse_quality: float
    pullback_quality: float
    executed_flow_quality: float
    book_quality: float
    execution_quality: float
    entry_reference_price: float
    structural_stop_price: float
    target_prices: tuple[float, ...]
    max_entry_age_ms: int
    max_hold_ms: int
    metadata: dict[str, object]
    reasons: tuple[str, ...]


def monotonic_ms() -> int:
    return int(time.monotonic() * 1000)
