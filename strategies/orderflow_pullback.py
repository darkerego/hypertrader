from __future__ import annotations

import asyncio
import json
import logging
import math
import uuid
from collections import deque
from dataclasses import dataclass
from statistics import mean, median
from types import SimpleNamespace
from typing import Any, Deque, Mapping, Optional

from strategies.base import AutoStrategy, StrategyContext, StrategyExecutionPlan, StrategySignal
from strategies.orderflow_metrics import (
    RollingTradeStore,
    compute_executable_depth_ratio,
    compute_microprice,
    estimate_round_trip_cost_bps,
    executable_depth_notional,
    median_spread_bps,
    parse_l2_book_snapshot,
    parse_trade_print,
    rolling_vwap,
    simple_atr,
    trend_efficiency,
    weighted_book_imbalance,
)
from strategies.orderflow_models import (
    ImpulseState,
    OrderFlowSetup,
    OrderFlowSetupState,
    PullbackState,
    monotonic_ms,
)
from strategies.reversal import _adx, normalize_candles

logger = logging.getLogger(__name__)

try:
    import numpy as np
except Exception:
    np = None  # type: ignore[assignment]


name = "orderflow_pullback"
aliases = ("of_pullback", "orderflow")
description = "Order-flow-confirmed trend pullback scalp strategy"


@dataclass(frozen=True)
class OrderflowPullbackConfig:
    max_active_books: int
    max_spread_bps: float
    max_spread_ratio: float
    depth_bps: float
    min_depth_ratio: float
    max_data_age_seconds: float
    warmup_seconds: float
    min_edge_cost_multiple: float
    trend_efficiency_min: float
    adx_min: float
    impulse_atr_multiple: float
    impulse_spread_multiple: float
    impulse_flow_min: float
    impulse_volume_ratio_min: float
    impulse_expiry_seconds: float
    pullback_min: float
    pullback_max: float
    pullback_min_seconds: float
    pullback_timeout_seconds: float
    book_imbalance_min: float
    micro_bias_min: float
    trade_imbalance_2s_min: float
    trade_imbalance_10s_min: float
    confirmations_required: int
    confirmation_window: int
    min_score: float
    min_flow_score: float
    min_book_score: float
    entry_timeout_seconds: float
    max_chase_ticks: int
    allow_market_fallback: bool
    stop_atr_fraction: float
    tp1_r_multiple: float
    tp1_size_fraction: float
    tp2_r_multiple: float
    continuation_timeout_seconds: float
    continuation_min_progress_r: float
    max_hold_seconds: float
    flow_scratch: bool
    flow_scratch_grace_seconds: float
    log_evaluations: bool
    scan_interval_seconds: float
    top_markets: int
    size_estimate_fraction: Optional[float]

    @classmethod
    def from_namespace(cls, ns: Any) -> "OrderflowPullbackConfig":
        def g(name: str, default: Any) -> Any:
            return getattr(ns, name, default)

        tp1_size_fraction = float(g("of_tp1_size_pct", 50.0)) / 100.0
        return cls(
            max_active_books=int(g("of_max_active_books", 8)),
            max_spread_bps=float(g("of_max_spread_bps", 3.0)),
            max_spread_ratio=float(g("of_max_spread_ratio", 1.5)),
            depth_bps=float(g("of_depth_bps", 5.0)),
            min_depth_ratio=float(g("of_min_depth_ratio", 10.0)),
            max_data_age_seconds=float(g("of_max_data_age_seconds", 1.5)),
            warmup_seconds=float(g("of_warmup_seconds", 60.0)),
            min_edge_cost_multiple=float(g("of_min_edge_cost_multiple", 2.5)),
            trend_efficiency_min=float(g("of_trend_efficiency_min", 0.30)),
            adx_min=float(g("of_adx_min", 18.0)),
            impulse_atr_multiple=float(g("of_impulse_atr_multiple", 0.35)),
            impulse_spread_multiple=float(g("of_impulse_spread_multiple", 4.0)),
            impulse_flow_min=float(g("of_impulse_flow_min", 0.20)),
            impulse_volume_ratio_min=float(g("of_impulse_volume_ratio_min", 1.40)),
            impulse_expiry_seconds=float(g("of_impulse_expiry_seconds", 120.0)),
            pullback_min=float(g("of_pullback_min", 0.20)),
            pullback_max=float(g("of_pullback_max", 0.60)),
            pullback_min_seconds=float(g("of_pullback_min_seconds", 5.0)),
            pullback_timeout_seconds=float(g("of_pullback_timeout_seconds", 90.0)),
            book_imbalance_min=float(g("of_book_imbalance_min", 0.15)),
            micro_bias_min=float(g("of_micro_bias_min", 0.10)),
            trade_imbalance_2s_min=float(g("of_trade_imbalance_2s_min", 0.20)),
            trade_imbalance_10s_min=float(g("of_trade_imbalance_10s_min", 0.08)),
            confirmations_required=int(g("of_confirmations_required", 2)),
            confirmation_window=int(g("of_confirmation_window", 3)),
            min_score=float(g("of_min_score", 0.70)),
            min_flow_score=float(g("of_min_flow_score", 0.60)),
            min_book_score=float(g("of_min_book_score", 0.55)),
            entry_timeout_seconds=float(g("of_entry_timeout_seconds", 2.0)),
            max_chase_ticks=int(g("of_max_chase_ticks", 2)),
            allow_market_fallback=bool(g("of_allow_market_fallback", False)),
            stop_atr_fraction=float(g("of_stop_atr_fraction", 0.10)),
            tp1_r_multiple=float(g("of_tp1_r", 1.0)),
            tp1_size_fraction=tp1_size_fraction,
            tp2_r_multiple=float(g("of_tp2_r", 1.6)),
            continuation_timeout_seconds=float(g("of_continuation_timeout_seconds", 45.0)),
            continuation_min_progress_r=float(g("of_continuation_min_progress_r", 0.35)),
            max_hold_seconds=float(g("of_max_hold_seconds", 180.0)),
            flow_scratch=bool(g("of_flow_scratch", True)),
            flow_scratch_grace_seconds=float(g("of_flow_scratch_grace_seconds", 1.0)),
            log_evaluations=bool(g("of_log_evaluations", False)),
            scan_interval_seconds=float(g("scan_interval", 1.0)),
            top_markets=int(g("top_markets", 10)),
            size_estimate_fraction=g("size_pct_fraction", None),
        )


@dataclass
class _CoinState:
    coin: str
    promoted: bool = False
    promoted_at_mono_ms: int = 0
    last_rank_score: float = 0.0
    book: Any | None = None
    trades: RollingTradeStore | None = None
    spread_history_bps: Deque[float] | None = None
    mid_history: Deque[tuple[int, float]] | None = None
    setup: OrderFlowSetup | None = None
    book_sub_id: Optional[int] = None
    trade_sub_id: Optional[int] = None
    generation: int = 0
    last_trade_exchange_ts_ms: int = 0
    last_trade_received_mono_ms: int = 0
    last_book_exchange_ts_ms: int = 0
    last_book_received_mono_ms: int = 0
    warmup_started_mono_ms: int = 0
    book_gap_history_mono_ms: Deque[int] | None = None
    last_recovery_mono_ms: int = 0
    recovery_count: int = 0

    def __post_init__(self) -> None:
        if self.trades is None:
            self.trades = RollingTradeStore()
        if self.spread_history_bps is None:
            self.spread_history_bps = deque(maxlen=120)
        if self.mid_history is None:
            self.mid_history = deque(maxlen=400)
        if self.book_gap_history_mono_ms is None:
            self.book_gap_history_mono_ms = deque(maxlen=64)
        if self.setup is None:
            self.setup = OrderFlowSetup(setup_id=f"{self.coin}-{uuid.uuid4().hex[:8]}", coin=self.coin)


class OrderflowPullbackStrategy(AutoStrategy):
    name = "orderflow_pullback"
    aliases = aliases
    description = description

    def __init__(self, config: object) -> None:
        self.config = OrderflowPullbackConfig.from_namespace(config)
        self._runtime_config = config
        self._lock = asyncio.Lock()
        self._states: dict[str, _CoinState] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._info: Any = None
        self._account_address: str = ""
        self._started = False
        self._global_generation = 0

    async def start(self, **kwargs: Any) -> None:
        self._loop = asyncio.get_running_loop()
        self._info = kwargs.get("info")
        self._account_address = str(kwargs.get("account_address") or "")
        self._global_generation += 1
        self._started = True
        self._log_event("strategy_start", {"strategy": self.name, "generation": self._global_generation})

    async def shutdown(self) -> None:
        if not self._started or self._info is None:
            return
        for state in list(self._states.values()):
            await self._demote_coin(state, reason="strategy_shutdown")
        self._started = False

    async def evaluate(self, context: StrategyContext) -> StrategySignal | None:
        coin = context.coin.upper()
        state = self._states.setdefault(coin, _CoinState(coin=coin))
        one_minute = normalize_candles(context.market_metadata.get("interval_candles", {}).get("1m", []))
        five_minute = normalize_candles(context.market_metadata.get("interval_candles", {}).get("5m", []))
        if len(one_minute) < 30 or len(five_minute) < 3:
            return None

        current_price = self._current_price(state, context)
        if current_price is None or current_price <= 0.0:
            return None

        scan_score = self._scan_score(current_price=current_price, candles_1m=one_minute, candles_5m=five_minute)
        state.last_rank_score = scan_score
        should_promote = scan_score >= 0.25 or state.promoted
        if should_promote:
            await self._ensure_promoted(state)
        if not state.promoted or state.book is None or state.last_trade_exchange_ts_ms <= 0:
            return None
        if not self._fresh(state):
            await self._recover_stale_feed(state)
            return None

        result = self._evaluate_live_setup(state=state, context=context, candles_1m=one_minute, candles_5m=five_minute)
        if self.config.log_evaluations and result is None:
            self._log_event("signal_rejected", {"coin": coin, "reason": state.setup.state_reason, "score": state.last_rank_score})
        return result

    def build_execution_plan(
        self,
        context: StrategyContext,
        signal: StrategySignal,
        size: float,
    ) -> StrategyExecutionPlan:
        metadata = dict(signal.metadata)
        metadata.setdefault("take_profit_pct", metadata.get("take_profit_pct"))
        metadata.setdefault("stop_loss_trigger_px", metadata.get("stop_loss_trigger_px"))
        metadata["entry_retries_override"] = max(
            1,
            int(math.ceil(self.config.entry_timeout_seconds / max(float(self._runtime_config.entry_repost_interval), 0.05))),
        )
        metadata["market_fallback_override"] = bool(self.config.allow_market_fallback)
        metadata["canonical_strategy"] = self.name
        return StrategyExecutionPlan(
            kind="bracket_entry",
            coin=signal.coin,
            direction=signal.direction,
            size=size,
            expected_entry=signal.entry_price,
            stop_price=signal.stop_price,
            take_profit_prices=signal.take_profit_prices,
            metadata=metadata,
        )

    async def _ensure_promoted(self, state: _CoinState) -> None:
        if self._info is None or self._loop is None:
            return
        if state.promoted:
            return
        promoted = [item for item in self._states.values() if item.promoted]
        if len(promoted) >= self.config.max_active_books:
            rotation_eligible = [item for item in promoted if self._eligible_for_rotation(item)]
            if not rotation_eligible:
                return
            weakest = min(rotation_eligible, key=lambda item: item.last_rank_score)
            if weakest.last_rank_score >= state.last_rank_score:
                return
            await self._demote_coin(weakest, reason="promotion_rotation")

        self._global_generation += 1
        state.promoted = True
        state.promoted_at_mono_ms = monotonic_ms()
        state.warmup_started_mono_ms = state.promoted_at_mono_ms
        state.generation = self._global_generation
        self._reset_live_feed_state(state)
        state.setup = OrderFlowSetup(setup_id=f"{state.coin}-{uuid.uuid4().hex[:8]}", coin=state.coin, feed_generation=state.generation)
        state.book_sub_id = await self._info.subscribe({"type": "l2Book", "coin": state.coin}, self._book_callback(state.coin, state.generation))
        state.trade_sub_id = await self._info.subscribe({"type": "trades", "coin": state.coin}, self._trade_callback(state.coin, state.generation))
        self._log_event("subscription_promoted", {"coin": state.coin, "generation": state.generation, "score": state.last_rank_score})

    async def _demote_coin(self, state: _CoinState, *, reason: str) -> None:
        if self._info is None or not state.promoted:
            return
        if state.book_sub_id is not None:
            try:
                await self._info.unsubscribe({"type": "l2Book", "coin": state.coin}, state.book_sub_id)
            except Exception:
                pass
        if state.trade_sub_id is not None:
            try:
                await self._info.unsubscribe({"type": "trades", "coin": state.coin}, state.trade_sub_id)
            except Exception:
                pass
        self._global_generation += 1
        state.generation = self._global_generation
        state.promoted = False
        state.book_sub_id = None
        state.trade_sub_id = None
        self._reset_live_feed_state(state)
        self._transition(state, OrderFlowSetupState.IDLE, f"demoted:{reason}")
        self._log_event("subscription_demoted", {"coin": state.coin, "reason": reason})

    def _reset_live_feed_state(self, state: _CoinState) -> None:
        state.book = None
        state.trades = RollingTradeStore()
        state.spread_history_bps = deque(maxlen=120)
        state.mid_history = deque(maxlen=400)
        state.book_gap_history_mono_ms = deque(maxlen=64)
        state.last_trade_exchange_ts_ms = 0
        state.last_trade_received_mono_ms = 0
        state.last_book_exchange_ts_ms = 0
        state.last_book_received_mono_ms = 0

    async def _recover_stale_feed(self, state: _CoinState) -> None:
        if self._info is None or self._loop is None or not state.promoted:
            return
        now_ms = monotonic_ms()
        recovery_cooldown_ms = max(15_000, self._book_staleness_limit_ms(state))
        if state.last_recovery_mono_ms > 0 and now_ms - state.last_recovery_mono_ms < recovery_cooldown_ms:
            self._transition(state, OrderFlowSetupState.IDLE, "data_stale")
            return

        book_age_ms = now_ms - state.last_book_received_mono_ms if state.last_book_received_mono_ms > 0 else None
        trade_age_ms = now_ms - state.last_trade_received_mono_ms if state.last_trade_received_mono_ms > 0 else None
        self._transition(state, OrderFlowSetupState.IDLE, "data_stale")
        self._log_event(
            "data_stale",
            {
                "coin": state.coin,
                "book_age_ms": book_age_ms,
                "trade_age_ms": trade_age_ms,
                "book_staleness_limit_ms": self._book_staleness_limit_ms(state),
            },
        )

        if state.book_sub_id is not None:
            try:
                await self._info.unsubscribe({"type": "l2Book", "coin": state.coin}, state.book_sub_id)
            except Exception:
                pass
        if state.trade_sub_id is not None:
            try:
                await self._info.unsubscribe({"type": "trades", "coin": state.coin}, state.trade_sub_id)
            except Exception:
                pass

        self._global_generation += 1
        state.generation = self._global_generation
        state.promoted_at_mono_ms = now_ms
        state.warmup_started_mono_ms = now_ms
        state.last_recovery_mono_ms = now_ms
        state.recovery_count += 1
        self._reset_live_feed_state(state)
        state.setup = OrderFlowSetup(setup_id=f"{state.coin}-{uuid.uuid4().hex[:8]}", coin=state.coin, feed_generation=state.generation)
        state.book_sub_id = await self._info.subscribe({"type": "l2Book", "coin": state.coin}, self._book_callback(state.coin, state.generation))
        state.trade_sub_id = await self._info.subscribe({"type": "trades", "coin": state.coin}, self._trade_callback(state.coin, state.generation))
        self._log_event(
            "subscription_recovered",
            {"coin": state.coin, "generation": state.generation, "recoveries": state.recovery_count},
        )

    def _eligible_for_rotation(self, state: _CoinState) -> bool:
        if not state.promoted:
            return False
        hold_ms = int(max(self.config.warmup_seconds, self.config.scan_interval_seconds) * 1000)
        if hold_ms <= 0:
            return True
        return monotonic_ms() - state.promoted_at_mono_ms >= hold_ms

    def _book_callback(self, coin: str, generation: int):
        def _callback(message: Mapping[str, Any]) -> None:
            if self._loop is None:
                return
            self._loop.call_soon_threadsafe(self._handle_book_message, coin, generation, dict(message))
        return _callback

    def _trade_callback(self, coin: str, generation: int):
        def _callback(message: Mapping[str, Any]) -> None:
            if self._loop is None:
                return
            self._loop.call_soon_threadsafe(self._handle_trade_message, coin, generation, dict(message))
        return _callback

    def _handle_book_message(self, coin: str, generation: int, message: Mapping[str, Any]) -> None:
        state = self._states.get(coin)
        if state is None or generation != state.generation:
            return
        snapshot = parse_l2_book_snapshot(
            message,
            received_monotonic_ms=monotonic_ms(),
        )
        if snapshot is None:
            return
        if state.last_book_received_mono_ms > 0 and snapshot.received_monotonic_ms >= state.last_book_received_mono_ms:
            state.book_gap_history_mono_ms.append(snapshot.received_monotonic_ms - state.last_book_received_mono_ms)
        state.book = snapshot
        state.last_book_exchange_ts_ms = snapshot.exchange_ts_ms
        state.last_book_received_mono_ms = snapshot.received_monotonic_ms
        state.spread_history_bps.append(snapshot.spread_bps)
        state.mid_history.append((snapshot.exchange_ts_ms, snapshot.mid))

    def _handle_trade_message(self, coin: str, generation: int, message: Mapping[str, Any]) -> None:
        state = self._states.get(coin)
        if state is None or generation != state.generation:
            return
        data = message.get("data", message)
        rows = data if isinstance(data, list) else data.get("trades", []) if isinstance(data, Mapping) else []
        if not isinstance(rows, list):
            return
        received_ms = monotonic_ms()
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            trade = parse_trade_print(row, received_monotonic_ms=received_ms)
            if trade is None:
                continue
            if state.trades.add_trade(trade):
                state.last_trade_exchange_ts_ms = trade.exchange_ts_ms
                state.last_trade_received_mono_ms = trade.received_monotonic_ms

    def _scan_score(
        self,
        *,
        current_price: float,
        candles_1m: list[dict[str, float]],
        candles_5m: list[dict[str, float]],
    ) -> float:
        closes_1m = [float(candle["c"]) for candle in candles_1m[-10:]]
        closes_5m = [float(candle["c"]) for candle in candles_5m[-3:]]
        if len(closes_1m) < 3 or len(closes_5m) < 2:
            return 0.0
        ret_1m = (closes_1m[-1] - closes_1m[0]) / closes_1m[0]
        ret_5m = (closes_5m[-1] - closes_5m[0]) / closes_5m[0]
        vol_ratio = float(candles_1m[-1].get("v", 0.0)) / max(1e-9, mean(float(candle.get("v", 0.0)) for candle in candles_1m[-10:]))
        eff = trend_efficiency(closes_1m)
        vwap_30m = rolling_vwap(candles_1m[-30:]) or mean(closes_1m)
        vwap_distance = abs(current_price - vwap_30m) / max(vwap_30m, 1e-9)
        return min(1.0, abs(ret_5m) * 50.0 + abs(ret_1m) * 80.0 + min(0.3, vol_ratio * 0.1) + eff * 0.3 + min(0.2, vwap_distance * 10.0))

    def _evaluate_live_setup(
        self,
        *,
        state: _CoinState,
        context: StrategyContext,
        candles_1m: list[dict[str, float]],
        candles_5m: list[dict[str, float]],
    ) -> StrategySignal | None:
        if state.book is None:
            return None
        now_mono_ms = monotonic_ms()
        if now_mono_ms - state.warmup_started_mono_ms < int(self.config.warmup_seconds * 1000):
            self._transition(state, OrderFlowSetupState.IDLE, "warmup")
            return None
        if not self._fresh(state):
            self._transition(state, OrderFlowSetupState.IDLE, "data_stale")
            now_ms = monotonic_ms()
            self._log_event(
                "data_stale",
                {
                    "coin": state.coin,
                    "book_age_ms": now_ms - state.last_book_received_mono_ms if state.last_book_received_mono_ms > 0 else None,
                    "trade_age_ms": now_ms - state.last_trade_received_mono_ms if state.last_trade_received_mono_ms > 0 else None,
                    "book_staleness_limit_ms": self._book_staleness_limit_ms(state),
                },
            )
            return None

        metrics = state.trades.metrics(now_exchange_ts_ms=state.book.exchange_ts_ms)
        weighted_bid, weighted_ask, book_imbalance = weighted_book_imbalance(state.book.bids[:5], state.book.asks[:5])
        microprice, micro_bias = compute_microprice(state.book)
        bid_depth, ask_depth = executable_depth_notional(state.book, depth_bps=self.config.depth_bps)
        intended_notional = max(1.0, state.book.mid * 1.0)
        depth_ratio = compute_executable_depth_ratio(bid_depth, ask_depth, intended_notional)
        spread_median = median_spread_bps(tuple(state.spread_history_bps))

        regime = self._classify_regime(state.book.mid, candles_1m, candles_5m)
        if regime is None:
            self._transition(state, OrderFlowSetupState.IDLE, "regime_invalid")
            return None

        direction, regime_score, regime_meta = regime
        self._transition(state, OrderFlowSetupState.REGIME_VALID, f"regime:{direction}")
        impulse = self._detect_impulse(state, direction, metrics, candles_1m)
        if impulse is None:
            return None
        state.setup.impulse = impulse
        self._transition(state, OrderFlowSetupState.IMPULSE_DETECTED, "impulse_detected")

        pullback = self._detect_pullback(state, impulse)
        if pullback is None:
            return None
        state.setup.pullback = pullback
        self._transition(state, OrderFlowSetupState.PULLBACK_ACTIVE, "pullback_active")

        side_sign = 1.0 if direction == "long" else -1.0
        flow_2s = metrics[2].trade_imbalance * side_sign >= self.config.trade_imbalance_2s_min
        flow_10s = metrics[10].trade_imbalance * side_sign >= self.config.trade_imbalance_10s_min
        book_ok = book_imbalance * side_sign >= self.config.book_imbalance_min
        micro_ok = micro_bias * side_sign >= self.config.micro_bias_min
        persistence_ok = self._update_persistence(state, book_ok and micro_ok)
        liquidity_ok = (
            state.book.spread_bps <= self.config.max_spread_bps
            and state.book.spread_bps <= spread_median * self.config.max_spread_ratio
            and depth_ratio >= self.config.min_depth_ratio
        )
        if not (flow_2s and flow_10s and persistence_ok and liquidity_ok):
            state.setup.state_reason = "flow_confirmation_pending"
            return None
        self._transition(state, OrderFlowSetupState.FLOW_CONFIRMED, "flow_confirmed")

        stop_buffer = max(state.book.spread * 2.0, impulse.atr_1m * self.config.stop_atr_fraction)
        if direction == "long":
            stop_price = pullback.pullback_low - stop_buffer
            risk = max(state.book.mid - stop_price, state.book.spread)
            tp1 = state.book.mid + (risk * self.config.tp1_r_multiple)
            tp2 = state.book.mid + (risk * self.config.tp2_r_multiple)
        else:
            stop_price = pullback.pullback_high + stop_buffer
            risk = max(stop_price - state.book.mid, state.book.spread)
            tp1 = state.book.mid - (risk * self.config.tp1_r_multiple)
            tp2 = state.book.mid - (risk * self.config.tp2_r_multiple)

        expected_move_bps = abs(tp2 - state.book.mid) / max(state.book.mid, 1e-9) * 10_000.0
        cost = estimate_round_trip_cost_bps(
            spread_bps=state.book.spread_bps,
            maker_fee_bps=0.5,
            taker_fee_bps=3.5,
            slippage_bps=0.5,
            entry_style="maker",
            exit_style="taker",
        )
        edge_cost_ratio = expected_move_bps / max(cost.estimated_round_trip_cost_bps, 1e-9)
        if edge_cost_ratio < self.config.min_edge_cost_multiple:
            state.setup.state_reason = "cost_gate_failed"
            return None

        flow_quality = min(1.0, max(0.0, ((metrics[2].trade_imbalance * side_sign) + 0.40) / 0.80))
        book_quality = min(1.0, max(0.0, ((book_imbalance * side_sign) + 0.25) / 0.50))
        trend_quality = min(1.0, regime_score)
        impulse_quality = min(1.0, abs(impulse.impulse_range) / max(impulse.atr_1m * 1.5, state.book.spread))
        pullback_quality = 1.0 - min(1.0, abs(pullback.retracement - 0.35) / 0.35)
        execution_quality = min(1.0, edge_cost_ratio / max(self.config.min_edge_cost_multiple, 1.0))
        score = (
            0.15 * trend_quality
            + 0.15 * impulse_quality
            + 0.15 * pullback_quality
            + 0.25 * flow_quality
            + 0.20 * book_quality
            + 0.10 * execution_quality
        )
        if score < self.config.min_score or flow_quality < self.config.min_flow_score or book_quality < self.config.min_book_score:
            state.setup.state_reason = "score_gate_failed"
            return None
        if state.setup.signal_claimed:
            return None

        state.setup.signal_claimed = True
        self._transition(state, OrderFlowSetupState.ENTRY_PENDING, "signal_emitted")
        tp_pct = abs(tp2 - state.book.mid) / max(state.book.mid, 1e-9)
        signal = StrategySignal(
            strategy=self.name,
            coin=state.coin,
            direction=direction,
            signal_candle_ms=state.book.exchange_ts_ms,
            entry_price=state.book.mid,
            stop_price=stop_price,
            take_profit_prices=(tp1, tp2),
            score=score,
            reasons=(
                f"regime={direction}",
                f"retracement={pullback.retracement:.3f}",
                f"book_imbalance={book_imbalance:.3f}",
                f"micro_bias={micro_bias:.3f}",
                f"edge_cost_ratio={edge_cost_ratio:.3f}",
            ),
            metadata={
                "setup_id": state.setup.setup_id,
                "canonical_strategy": self.name,
                "take_profit_pct": tp_pct,
                "stop_loss_pct": None,
                "stop_loss_trigger_px": stop_price,
                "tp1_price": tp1,
                "tp2_price": tp2,
                "tp1_size_fraction": self.config.tp1_size_fraction,
                "continuation_timeout_seconds": self.config.continuation_timeout_seconds,
                "continuation_min_progress_r": self.config.continuation_min_progress_r,
                "max_hold_seconds": self.config.max_hold_seconds,
                "flow_scratch_enabled": self.config.flow_scratch,
                "metrics": {
                    "mid": state.book.mid,
                    "best_bid": state.book.best_bid,
                    "best_ask": state.book.best_ask,
                    "spread_bps": state.book.spread_bps,
                    "book_imbalance": book_imbalance,
                    "microprice": microprice,
                    "micro_bias": micro_bias,
                    "trade_imbalance_2s": metrics[2].trade_imbalance,
                    "trade_imbalance_10s": metrics[10].trade_imbalance,
                    "trade_imbalance_30s": metrics[30].trade_imbalance,
                    "weighted_bid": weighted_bid,
                    "weighted_ask": weighted_ask,
                    "bid_depth_notional": bid_depth,
                    "ask_depth_notional": ask_depth,
                    "depth_ratio": depth_ratio,
                    "regime": regime_meta,
                    "impulse_origin": impulse.origin_price,
                    "impulse_extreme": impulse.extreme_price,
                    "impulse_range": impulse.impulse_range,
                    "pullback_retracement": pullback.retracement,
                    "cost_bps": cost.estimated_round_trip_cost_bps,
                    "edge_cost_ratio": edge_cost_ratio,
                },
            },
        )
        self._log_event("signal_emitted", {"coin": state.coin, "direction": direction, "score": score, "setup_id": state.setup.setup_id})
        return signal

    def _fresh(self, state: _CoinState) -> bool:
        now_ms = monotonic_ms()
        if state.last_book_received_mono_ms <= 0:
            return False
        return now_ms - state.last_book_received_mono_ms <= self._book_staleness_limit_ms(state)

    def _book_staleness_limit_ms(self, state: _CoinState) -> int:
        # Live feeds are evaluated after the outer scan loop finishes its REST
        # candle work, so the observed gap between checks is meaningfully larger
        # than the configured sleep interval under normal load.
        baseline_ms = int(max(self.config.max_data_age_seconds, self.config.scan_interval_seconds * 3.0) * 1000)
        gaps = tuple(gap for gap in (state.book_gap_history_mono_ms or ()) if gap > 0)
        if len(gaps) < 3:
            return baseline_ms

        observed_median_ms = int(median(gaps))
        observed_max_ms = max(gaps)
        adaptive_ms = max(baseline_ms, observed_median_ms * 4, observed_max_ms * 2)
        return min(30_000, adaptive_ms)

    def _current_price(self, state: _CoinState, context: StrategyContext) -> Optional[float]:
        if state.book is not None:
            return float(state.book.mid)
        current_px = context.market_metadata.get("current_px")
        try:
            return float(current_px)
        except (TypeError, ValueError):
            return None

    def _classify_regime(
        self,
        current_price: float,
        candles_1m: list[dict[str, float]],
        candles_5m: list[dict[str, float]],
    ) -> Optional[tuple[str, float, dict[str, float]]]:
        vwap_series: list[float] = []
        for idx in range(30, len(candles_1m) + 1):
            value = rolling_vwap(candles_1m[idx - 30:idx])
            if value is not None:
                vwap_series.append(value)
        current_vwap = vwap_series[-1] if vwap_series else mean(float(candle["c"]) for candle in candles_1m[-30:])
        slope = (vwap_series[-1] - vwap_series[-5]) if len(vwap_series) >= 5 else 0.0
        ret_5m = (float(candles_5m[-1]["c"]) - float(candles_5m[-2]["c"])) / max(float(candles_5m[-2]["c"]), 1e-9)
        eff = trend_efficiency([float(candle["c"]) for candle in candles_1m[-10:]])
        adx_value = None
        if np is not None and len(candles_1m) >= 20:
            try:
                adx_values = _adx(candles_1m[-30:], 14)
                adx_value = float(adx_values[-1])
            except Exception:
                adx_value = None
        adx_ok = adx_value is None or adx_value >= self.config.adx_min
        if current_price > current_vwap and slope > 0.0 and ret_5m > 0.0 and eff >= self.config.trend_efficiency_min and adx_ok:
            return "long", min(1.0, eff + min(0.3, ret_5m * 20.0)), {"vwap": current_vwap, "vwap_slope": slope, "ret_5m": ret_5m, "trend_efficiency": eff, "adx": adx_value or 0.0}
        if current_price < current_vwap and slope < 0.0 and ret_5m < 0.0 and eff >= self.config.trend_efficiency_min and adx_ok:
            return "short", min(1.0, eff + min(0.3, abs(ret_5m) * 20.0)), {"vwap": current_vwap, "vwap_slope": slope, "ret_5m": ret_5m, "trend_efficiency": eff, "adx": adx_value or 0.0}
        return None

    def _detect_impulse(self, state: _CoinState, direction: str, metrics: dict[int, Any], candles_1m: list[dict[str, float]]) -> Optional[ImpulseState]:
        if state.book is None or len(state.mid_history) < 10:
            return None
        atr_value = simple_atr(candles_1m[-20:], 14)
        if atr_value is None:
            return None
        direction_sign = 1.0 if direction == "long" else -1.0
        threshold = max(self.config.impulse_atr_multiple * atr_value, self.config.impulse_spread_multiple * state.book.spread)
        current_ts_ms = state.book.exchange_ts_ms
        candidate_start: Optional[tuple[int, float]] = None
        for ts_ms, mid in reversed(state.mid_history):
            age = current_ts_ms - ts_ms
            if age < 10_000:
                continue
            if age > 45_000:
                break
            candidate_start = (ts_ms, mid)
        if candidate_start is None:
            return None
        start_ts_ms, start_mid = candidate_start
        displacement = (state.book.mid - start_mid) * direction_sign
        imbalance_10s = metrics[10].trade_imbalance * direction_sign
        baseline = state.trades.aggressive_notional_bucket_medians(
            side=direction,
            bucket_seconds=10,
            lookback_seconds=300,
            now_exchange_ts_ms=current_ts_ms,
        )
        side_notional = metrics[10].buy_volume if direction == "long" else metrics[10].sell_volume
        ratio = (side_notional / baseline) if baseline > 0.0 else 0.0
        extreme = max(mid for _, mid in state.mid_history if _ >= start_ts_ms) if direction == "long" else min(mid for _, mid in state.mid_history if _ >= start_ts_ms)
        impulse_range = abs(extreme - start_mid)
        if displacement < threshold or imbalance_10s < self.config.impulse_flow_min or ratio < self.config.impulse_volume_ratio_min:
            return None
        if impulse_range <= 0.0:
            return None
        if direction == "long":
            close_position = (state.book.mid - start_mid) / impulse_range
        else:
            close_position = (start_mid - state.book.mid) / impulse_range
        if close_position < 0.70:
            return None
        return ImpulseState(
            side=direction,  # type: ignore[arg-type]
            start_ts_ms=start_ts_ms,
            end_ts_ms=current_ts_ms,
            origin_price=start_mid,
            extreme_price=extreme,
            impulse_range=impulse_range,
            atr_1m=atr_value,
            spread_bps=state.book.spread_bps,
            trade_imbalance_10s=metrics[10].trade_imbalance,
            aggressive_notional_ratio=ratio,
            feed_generation=state.generation,
        )

    def _detect_pullback(self, state: _CoinState, impulse: ImpulseState) -> Optional[PullbackState]:
        if state.book is None:
            return None
        current_mid = state.book.mid
        if impulse.impulse_range <= 0.0:
            return None
        if impulse.side == "long":
            retracement = (impulse.extreme_price - current_mid) / impulse.impulse_range
            pullback_low = min(current_mid, impulse.extreme_price)
            if current_mid < impulse.origin_price:
                self._transition(state, OrderFlowSetupState.IDLE, "impulse_origin_breached")
                return None
            support_price = current_mid
            pullback_high = impulse.extreme_price
        else:
            retracement = (current_mid - impulse.extreme_price) / impulse.impulse_range
            pullback_high = max(current_mid, impulse.extreme_price)
            if current_mid > impulse.origin_price:
                self._transition(state, OrderFlowSetupState.IDLE, "impulse_origin_breached")
                return None
            support_price = current_mid
            pullback_low = impulse.extreme_price
        duration_seconds = (state.book.exchange_ts_ms - impulse.end_ts_ms) / 1000.0
        valid = (
            self.config.pullback_min <= retracement <= self.config.pullback_max
            and self.config.pullback_min_seconds <= duration_seconds <= self.config.pullback_timeout_seconds
        )
        if not valid:
            return None
        return PullbackState(
            side=impulse.side,
            started_ts_ms=impulse.end_ts_ms,
            last_ts_ms=state.book.exchange_ts_ms,
            retracement=retracement,
            pullback_low=pullback_low,
            pullback_high=pullback_high,
            support_price=support_price,
            valid=True,
            reason="retracement_zone",
        )

    def _update_persistence(self, state: _CoinState, condition: bool) -> bool:
        history = state.setup.confirmation_history
        history.append(condition)
        if len(history) > self.config.confirmation_window:
            del history[:-self.config.confirmation_window]
        return sum(1 for item in history if item) >= self.config.confirmations_required

    def _transition(self, state: _CoinState, new_state: OrderFlowSetupState, reason: str) -> None:
        setup = state.setup
        if setup.state == new_state and setup.state_reason == reason:
            return
        setup.state = new_state
        setup.state_reason = reason
        setup.last_transition_ms = monotonic_ms()
        self._log_event("state_transition", {"coin": state.coin, "state": new_state.value, "reason": reason, "setup_id": setup.setup_id})

    def _log_event(self, event: str, payload: Mapping[str, Any]) -> None:
        logger.info(json.dumps({"event": event, **payload}, sort_keys=True))
