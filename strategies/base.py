from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, Sequence


TradeDirection = Literal["long", "short"]


@dataclass(frozen=True)
class StrategySignal:
    strategy: str
    coin: str
    direction: TradeDirection
    signal_candle_ms: int
    entry_price: float
    stop_price: float
    take_profit_prices: tuple[float, ...]
    score: float
    reasons: tuple[str, ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StrategyContext:
    coin: str
    now_ms: int
    config: Any
    market_metadata: Any
    trend_candles: Sequence[Any]
    entry_candles: Sequence[Any]
    current_position: Any | None


@dataclass(frozen=True)
class StrategyExecutionPlan:
    kind: str
    coin: str
    direction: TradeDirection
    size: float
    expected_entry: float
    stop_price: float
    take_profit_prices: tuple[float, ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)


class AutoStrategy(ABC):
    name: str

    async def start(self, **_: Any) -> None:
        """Optional async lifecycle hook for strategies that own live state."""

    async def shutdown(self) -> None:
        """Optional async lifecycle hook for strategies that own live state."""

    @abstractmethod
    async def evaluate(self, context: StrategyContext) -> StrategySignal | None:
        """Evaluate the latest closed-candle market data and return one signal at most."""
        raise NotImplementedError

    @abstractmethod
    def build_execution_plan(
        self,
        context: StrategyContext,
        signal: StrategySignal,
        size: float,
    ) -> StrategyExecutionPlan:
        """Translate a pure signal into an execution plan for the shared auto engine."""
        raise NotImplementedError

    def rank_signal(self, signal: StrategySignal) -> tuple[float, int]:
        return signal.score, signal.signal_candle_ms

    def on_position_update(self, context: StrategyContext) -> None:
        """Update strategy-owned position-management state when required."""

    def reconcile(self, context: StrategyContext) -> None:
        """Rebuild strategy-owned state after startup or reconnect when required."""
