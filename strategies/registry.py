from __future__ import annotations

from collections.abc import Callable

from strategies.base import AutoStrategy
from strategies.default import DefaultAutoStrategy
from strategies.orderflow_pullback import OrderflowPullbackStrategy
from strategies.reversal import ReversalAutoStrategy

StrategyFactory = Callable[[object], AutoStrategy]

_ALIASES: dict[str, str] = {
    "legacy": "default",
    "of_pullback": "orderflow_pullback",
    "orderflow": "orderflow_pullback",
}

_STRATEGIES: dict[str, StrategyFactory] = {
    "default": DefaultAutoStrategy,
    "orderflow_pullback": OrderflowPullbackStrategy,
    "reversal": ReversalAutoStrategy,
}


def available_strategies() -> tuple[str, ...]:
    return tuple(sorted(_STRATEGIES))


def available_strategy_names() -> tuple[str, ...]:
    return tuple(sorted(set(_STRATEGIES) | set(_ALIASES)))


def normalize_strategy_name(name: str) -> str:
    normalized = name.strip().lower()
    return _ALIASES.get(normalized, normalized)


def create_strategy(name: str, config: object) -> AutoStrategy:
    normalized = normalize_strategy_name(name)
    try:
        factory = _STRATEGIES[normalized]
    except KeyError as exc:
        choices = ", ".join(available_strategies())
        raise ValueError(f"Unknown auto strategy {name!r}. Available strategies: {choices}") from exc
    return factory(config)
