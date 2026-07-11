from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from utils.constants import INTERVAL_TO_MS

from strategies.base import AutoStrategy, StrategyContext, StrategyExecutionPlan, StrategySignal

try:
    import numpy as np
    import talib
except Exception:
    np = None  # type: ignore[assignment]
    talib = None  # type: ignore[assignment]


@dataclass
class AutoIntervalSignal:
    interval: str
    closes: List[float]
    close: float
    macd: float
    macd_signal: float
    macd_hist: float
    sar: float
    adx: float
    bb_upper: float
    bb_middle: float
    bb_lower: float
    direction: str
    reason: str


@dataclass
class AutoTradeDecision:
    direction: Optional[str]
    current_px: float
    target_px: Optional[float]
    take_profit_pct: Optional[float]
    stop_loss_pct: Optional[float]
    stop_loss_trigger_px: Optional[float]
    long_votes: int
    short_votes: int
    required_votes: int
    snapshots: List[AutoIntervalSignal]
    reason: str


@dataclass
class BollingerState:
    interval: str
    basis: float
    upper: float
    lower: float
    percent_b: float
    basis_slope: float
    bandwidth: float
    previous_bandwidth: float
    bandwidth_expanding: bool
    latest_close: float


def require_talib_available() -> None:
    if np is None or talib is None:
        raise RuntimeError(
            "The auto command requires numpy and TA-Lib. Install them with:\n"
            "  pip install numpy TA-Lib"
        )


def compute_default_stop_loss_pct(
    take_profit_pct: Optional[float],
    stop_loss_pct: Optional[float],
) -> Optional[float]:
    if stop_loss_pct is not None:
        return stop_loss_pct
    if take_profit_pct is not None:
        return take_profit_pct * 0.5
    return None


def last_finite_index(*arrays: Any) -> int:
    if not arrays:
        raise RuntimeError("No arrays supplied for finite-index scan.")
    length = min(len(array) for array in arrays)
    for idx in range(length - 1, -1, -1):
        ok = True
        for array in arrays:
            try:
                value = float(array[idx])
            except (TypeError, ValueError):
                ok = False
                break
            if not math.isfinite(value):
                ok = False
                break
        if ok:
            return idx
    raise RuntimeError("No finite TA-Lib indicator row is available yet; fetch more candles.")


def clamp_pct(value: float, minimum: float, maximum: float) -> float:
    return min(max(value, minimum), maximum)


def get_shortest_interval_snapshot(snapshots: List[AutoIntervalSignal]) -> Optional[AutoIntervalSignal]:
    if not snapshots:
        return None
    return min(snapshots, key=lambda snapshot: INTERVAL_TO_MS.get(snapshot.interval, math.inf))


def interval_to_seconds(interval: str) -> int:
    normalized = str(interval).strip()
    if normalized in INTERVAL_TO_MS:
        return int(INTERVAL_TO_MS[normalized] / 1000)

    lower = normalized.lower()
    if lower in INTERVAL_TO_MS:
        return int(INTERVAL_TO_MS[lower] / 1000)
    if lower.endswith("m"):
        return int(lower[:-1]) * 60
    if lower.endswith("h"):
        return int(lower[:-1]) * 60 * 60
    if lower.endswith("d"):
        return int(lower[:-1]) * 24 * 60 * 60
    if lower.endswith("w"):
        return int(lower[:-1]) * 7 * 24 * 60 * 60
    raise ValueError(f"Unsupported interval: {interval}")


def select_bollinger_intervals(active_intervals: List[str], scalp: bool) -> Dict[str, str]:
    if not active_intervals:
        raise RuntimeError("No active intervals were configured for Bollinger confirmation.")

    ordered = sorted(active_intervals, key=interval_to_seconds)
    if scalp:
        return {"entry": ordered[0]}

    if len(ordered) == 1:
        return {"entry": ordered[0], "regime": ordered[0]}
    if len(ordered) == 2:
        return {"entry": ordered[0], "regime": ordered[1]}
    if len(ordered) == 3:
        return {"entry": ordered[0], "setup": ordered[1], "regime": ordered[2]}

    entry_index = 1 if ordered[0] == "1m" else 0
    return {
        "entry": ordered[entry_index],
        "setup": ordered[len(ordered) // 2],
        "regime": ordered[-1],
    }


def normalize_bollinger_side(side: str) -> str:
    normalized = str(side).strip().lower()
    if normalized in {"long", "buy", "b", "bid"}:
        return "long"
    if normalized in {"short", "sell", "s", "a", "ask"}:
        return "short"
    raise RuntimeError(f"Unsupported side for Bollinger confirmation: {side!r}")


def compute_bollinger_state(
    interval: str,
    closes: List[float],
    period: int = 20,
    stddev_multiplier: float = 2.0,
) -> BollingerState:
    require_talib_available()
    if period <= 1:
        raise RuntimeError(f"Bollinger period must be > 1, got {period}.")
    if len(closes) < period + 1:
        raise RuntimeError(
            f"{interval} has insufficient close data for Bollinger confirmation: "
            f"got {len(closes)}, need at least {period + 1}."
        )

    close_arr = np.asarray(closes, dtype=float)  # type: ignore[union-attr]
    upper_arr, basis_arr, lower_arr = talib.BBANDS(  # type: ignore[union-attr]
        close_arr,
        timeperiod=period,
        nbdevup=stddev_multiplier,
        nbdevdn=stddev_multiplier,
        matype=0,
    )

    current_idx = last_finite_index(close_arr, upper_arr, basis_arr, lower_arr)
    previous_idx: Optional[int] = None
    for idx in range(current_idx - 1, -1, -1):
        values = (close_arr[idx], upper_arr[idx], basis_arr[idx], lower_arr[idx])
        if all(math.isfinite(float(value)) for value in values):
            previous_idx = idx
            break
    if previous_idx is None:
        raise RuntimeError(f"{interval} does not have enough completed Bollinger windows for slope confirmation.")

    latest_close = float(close_arr[current_idx])
    upper = float(upper_arr[current_idx])
    basis = float(basis_arr[current_idx])
    lower = float(lower_arr[current_idx])
    previous_upper = float(upper_arr[previous_idx])
    previous_basis = float(basis_arr[previous_idx])
    previous_lower = float(lower_arr[previous_idx])

    band_span = upper - lower
    if abs(band_span) <= 0.0:
        raise RuntimeError(f"{interval} Bollinger band span is zero; skipping confirmation safely.")
    if basis == 0.0:
        raise RuntimeError(f"{interval} Bollinger basis is zero; skipping confirmation safely.")
    previous_band_span = previous_upper - previous_lower
    if previous_basis == 0.0:
        raise RuntimeError(f"{interval} previous Bollinger basis is zero; skipping confirmation safely.")

    percent_b = (latest_close - lower) / band_span
    basis_slope = basis - previous_basis
    bandwidth = band_span / basis
    previous_bandwidth = previous_band_span / previous_basis

    return BollingerState(
        interval=interval,
        basis=basis,
        upper=upper,
        lower=lower,
        percent_b=percent_b,
        basis_slope=basis_slope,
        bandwidth=bandwidth,
        previous_bandwidth=previous_bandwidth,
        bandwidth_expanding=bandwidth > previous_bandwidth,
        latest_close=latest_close,
    )


def _scalp_quality_label(side: str, percent_b: float) -> str:
    if side == "long":
        if percent_b <= 0.25:
            return "ideal"
        if percent_b <= 0.35:
            return "good"
        if percent_b <= 0.50:
            return "valid"
        return "extended"
    if percent_b >= 0.75:
        return "ideal"
    if percent_b >= 0.65:
        return "good"
    if percent_b >= 0.50:
        return "valid"
    return "extended"


def confirm_signal_with_bollinger(
    side: str,
    interval_to_closes: Dict[str, List[float]],
    active_intervals: List[str],
    scalp: bool,
    period: int = 20,
    stddev_multiplier: float = 2.0,
) -> Tuple[bool, str, Dict[str, BollingerState]]:
    try:
        normalized_side = normalize_bollinger_side(side)
        selected_intervals = select_bollinger_intervals(active_intervals, scalp)
    except Exception as exc:
        return False, str(exc), {}

    states: Dict[str, BollingerState] = {}
    for role, interval in selected_intervals.items():
        closes = interval_to_closes.get(interval)
        if not closes:
            return False, f"{role} interval {interval} has no usable close data for Bollinger confirmation.", states
        try:
            states[role] = compute_bollinger_state(
                interval=interval,
                closes=closes,
                period=period,
                stddev_multiplier=stddev_multiplier,
            )
        except Exception as exc:
            return False, f"{role} interval {interval} Bollinger confirmation failed: {exc}", states

    if scalp:
        entry_state = states["entry"]
        quality = _scalp_quality_label(normalized_side, entry_state.percent_b)
        if normalized_side == "long":
            allowed = entry_state.percent_b <= 0.50
            if not allowed:
                return False, (
                    f"mode=scalp side=long entry_tf={entry_state.interval} %B={entry_state.percent_b:.3f} "
                    f"result=REJECT reason=\"long rejected: entry interval above middle band\" quality={quality}"
                ), states
        else:
            allowed = entry_state.percent_b >= 0.50
            if not allowed:
                return False, (
                    f"mode=scalp side=short entry_tf={entry_state.interval} %B={entry_state.percent_b:.3f} "
                    f"result=REJECT reason=\"short rejected: entry interval below middle band\" quality={quality}"
                ), states

        return True, (
            f"mode=scalp side={normalized_side} entry_tf={entry_state.interval} %B={entry_state.percent_b:.3f} "
            f"basis={entry_state.basis:.8f} upper={entry_state.upper:.8f} lower={entry_state.lower:.8f} "
            f"result=PASS quality={quality}"
        ), states

    regime_state = states.get("regime")
    setup_state = states.get("setup")
    entry_state = states["entry"]

    if normalized_side == "long":
        if regime_state is not None:
            if regime_state.percent_b < 0.25 and regime_state.basis_slope < 0.0:
                return False, (
                    f"mode=non_scalp side=long regime_tf={regime_state.interval} setup_tf="
                    f"{setup_state.interval if setup_state is not None else 'N/A'} entry_tf={entry_state.interval} "
                    f"result=REJECT reason=\"long rejected: regime interval is in bearish breakdown\""
                ), states
            if regime_state.percent_b < 0.0 and regime_state.bandwidth_expanding:
                return False, (
                    f"mode=non_scalp side=long regime_tf={regime_state.interval} setup_tf="
                    f"{setup_state.interval if setup_state is not None else 'N/A'} entry_tf={entry_state.interval} "
                    f"result=REJECT reason=\"long rejected: regime interval is below lower band with expanding bandwidth\""
                ), states
        if setup_state is not None and setup_state.percent_b > 0.90:
            return False, (
                f"mode=non_scalp side=long regime_tf={regime_state.interval if regime_state is not None else 'N/A'} "
                f"setup_tf={setup_state.interval} entry_tf={entry_state.interval} "
                f"result=REJECT reason=\"long rejected: setup interval is already overextended high\""
            ), states
        if entry_state.percent_b > 0.50:
            return False, (
                f"mode=non_scalp side=long regime_tf={regime_state.interval if regime_state is not None else 'N/A'} "
                f"setup_tf={setup_state.interval if setup_state is not None else 'N/A'} entry_tf={entry_state.interval} "
                f"result=REJECT reason=\"long rejected: entry interval above middle band\""
            ), states
    else:
        if regime_state is not None:
            if regime_state.percent_b > 0.75 and regime_state.basis_slope > 0.0:
                return False, (
                    f"mode=non_scalp side=short regime_tf={regime_state.interval} setup_tf="
                    f"{setup_state.interval if setup_state is not None else 'N/A'} entry_tf={entry_state.interval} "
                    f"result=REJECT reason=\"short rejected: regime interval is in bullish breakout\""
                ), states
            if regime_state.percent_b > 1.0 and regime_state.bandwidth_expanding:
                return False, (
                    f"mode=non_scalp side=short regime_tf={regime_state.interval} setup_tf="
                    f"{setup_state.interval if setup_state is not None else 'N/A'} entry_tf={entry_state.interval} "
                    f"result=REJECT reason=\"short rejected: regime interval is above upper band with expanding bandwidth\""
                ), states
        if setup_state is not None and setup_state.percent_b < 0.10:
            return False, (
                f"mode=non_scalp side=short regime_tf={regime_state.interval if regime_state is not None else 'N/A'} "
                f"setup_tf={setup_state.interval} entry_tf={entry_state.interval} "
                f"result=REJECT reason=\"short rejected: setup interval is already overextended low\""
            ), states
        if entry_state.percent_b < 0.50:
            return False, (
                f"mode=non_scalp side=short regime_tf={regime_state.interval if regime_state is not None else 'N/A'} "
                f"setup_tf={setup_state.interval if setup_state is not None else 'N/A'} entry_tf={entry_state.interval} "
                f"result=REJECT reason=\"short rejected: entry interval below middle band\""
            ), states

    reason = (
        f"mode=non_scalp side={normalized_side} "
        f"regime_tf={regime_state.interval if regime_state is not None else 'N/A'} "
        f"setup_tf={setup_state.interval if setup_state is not None else 'N/A'} "
        f"entry_tf={entry_state.interval} result=PASS "
    )
    if regime_state is not None:
        reason += f"regime_b={regime_state.percent_b:.3f} "
    if setup_state is not None:
        reason += f"setup_b={setup_state.percent_b:.3f} "
    reason += f"entry_b={entry_state.percent_b:.3f}"
    return True, reason, states


def compute_interval_signal_from_candles(
    candles: List[Dict[str, Any]],
    interval: str,
    periods: int,
    adx_threshold: float,
    macd_fast: int,
    macd_slow: int,
    macd_signal: int,
    sar_acceleration: float,
    sar_maximum: float,
    adx_timeperiod: int,
    bb_timeperiod: int,
    bb_dev: float,
    use_last_closed_candle: bool,
) -> AutoIntervalSignal:
    require_talib_available()
    minimum_periods = max(macd_slow + macd_signal + 5, adx_timeperiod + 5, bb_timeperiod + 5, 40)
    if periods < minimum_periods:
        raise RuntimeError(f"--auto-periods must be at least {minimum_periods} for the chosen indicator settings.")

    usable_candles = candles[:-1] if use_last_closed_candle and len(candles) > 1 else candles
    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []
    for candle in usable_candles:
        try:
            highs.append(float(candle["h"]))
            lows.append(float(candle["l"]))
            closes.append(float(candle["c"]))
        except (KeyError, TypeError, ValueError):
            continue

    if len(closes) < minimum_periods:
        raise RuntimeError(
            f"Not enough valid candles for interval {interval}: got {len(closes)}, need at least {minimum_periods}."
        )

    high_arr = np.asarray(highs, dtype=float)  # type: ignore[union-attr]
    low_arr = np.asarray(lows, dtype=float)  # type: ignore[union-attr]
    close_arr = np.asarray(closes, dtype=float)  # type: ignore[union-attr]

    macd_arr, macd_sig_arr, macd_hist_arr = talib.MACD(  # type: ignore[union-attr]
        close_arr,
        fastperiod=macd_fast,
        slowperiod=macd_slow,
        signalperiod=macd_signal,
    )
    sar_arr = talib.SAR(  # type: ignore[union-attr]
        high_arr,
        low_arr,
        acceleration=sar_acceleration,
        maximum=sar_maximum,
    )
    adx_arr = talib.ADX(  # type: ignore[union-attr]
        high_arr,
        low_arr,
        close_arr,
        timeperiod=adx_timeperiod,
    )
    bb_upper_arr, bb_middle_arr, bb_lower_arr = talib.BBANDS(  # type: ignore[union-attr]
        close_arr,
        timeperiod=bb_timeperiod,
        nbdevup=bb_dev,
        nbdevdn=bb_dev,
        matype=0,
    )

    idx = last_finite_index(
        close_arr,
        macd_arr,
        macd_sig_arr,
        macd_hist_arr,
        sar_arr,
        adx_arr,
        bb_upper_arr,
        bb_middle_arr,
        bb_lower_arr,
    )
    close_px = float(close_arr[idx])
    macd_value = float(macd_arr[idx])
    macd_signal_value = float(macd_sig_arr[idx])
    macd_hist_value = float(macd_hist_arr[idx])
    sar_value = float(sar_arr[idx])
    adx_value = float(adx_arr[idx])
    bb_upper = float(bb_upper_arr[idx])
    bb_middle = float(bb_middle_arr[idx])
    bb_lower = float(bb_lower_arr[idx])

    macd_bullish = macd_value > macd_signal_value and macd_hist_value > 0.0
    macd_bearish = macd_value < macd_signal_value and macd_hist_value < 0.0
    sar_bullish = close_px > sar_value
    sar_bearish = close_px < sar_value
    trend_ok = adx_value >= adx_threshold

    if trend_ok and macd_bullish and sar_bullish:
        direction = "long"
    elif trend_ok and macd_bearish and sar_bearish:
        direction = "short"
    else:
        direction = "neutral"

    reason = (
        f"macd={'bull' if macd_bullish else 'bear' if macd_bearish else 'flat'} "
        f"sar={'bull' if sar_bullish else 'bear' if sar_bearish else 'flat'} "
        f"adx={adx_value:.2f}/{adx_threshold:.2f}"
    )

    return AutoIntervalSignal(
        interval=interval,
        closes=list(closes),
        close=close_px,
        macd=macd_value,
        macd_signal=macd_signal_value,
        macd_hist=macd_hist_value,
        sar=sar_value,
        adx=adx_value,
        bb_upper=bb_upper,
        bb_middle=bb_middle,
        bb_lower=bb_lower,
        direction=direction,
        reason=reason,
    )


def evaluate_default_decision(
    interval_candles: Dict[str, List[Dict[str, Any]]],
    current_px: float,
    intervals: List[str],
    periods: int,
    min_agreement: int,
    adx_threshold: float,
    take_profit_pct_override: Optional[float],
    stop_loss_pct_override: Optional[float],
    min_take_profit_pct: float,
    max_take_profit_pct: float,
    macd_fast: int,
    macd_slow: int,
    macd_signal_period: int,
    sar_acceleration: float,
    sar_maximum: float,
    adx_timeperiod: int,
    bb_timeperiod: int,
    bb_dev: float,
    use_last_closed_candle: bool,
    use_sar_stop_on_shortest_interval: bool,
) -> AutoTradeDecision:
    snapshots: List[AutoIntervalSignal] = []
    errors: List[str] = []
    for interval in intervals:
        candles = interval_candles.get(interval)
        if not candles:
            errors.append(f"{interval}: no candles")
            continue
        try:
            snapshots.append(
                compute_interval_signal_from_candles(
                    candles=candles,
                    interval=interval,
                    periods=periods,
                    adx_threshold=adx_threshold,
                    macd_fast=macd_fast,
                    macd_slow=macd_slow,
                    macd_signal=macd_signal_period,
                    sar_acceleration=sar_acceleration,
                    sar_maximum=sar_maximum,
                    adx_timeperiod=adx_timeperiod,
                    bb_timeperiod=bb_timeperiod,
                    bb_dev=bb_dev,
                    use_last_closed_candle=use_last_closed_candle,
                )
            )
        except Exception as exc:
            errors.append(f"{interval}: {exc}")

    if not snapshots:
        return AutoTradeDecision(
            direction=None,
            current_px=0.0,
            target_px=None,
            take_profit_pct=None,
            stop_loss_pct=None,
            stop_loss_trigger_px=None,
            long_votes=0,
            short_votes=0,
            required_votes=max(1, min_agreement),
            snapshots=[],
            reason="No usable interval snapshots. " + "; ".join(errors),
        )

    required_votes = len(intervals) if min_agreement <= 0 else min_agreement
    long_votes = sum(1 for snapshot in snapshots if snapshot.direction == "long")
    short_votes = sum(1 for snapshot in snapshots if snapshot.direction == "short")

    if current_px <= 0.0:
        current_px = float(snapshots[-1].close)

    if len(snapshots) < required_votes:
        return AutoTradeDecision(
            direction=None,
            current_px=current_px,
            target_px=None,
            take_profit_pct=None,
            stop_loss_pct=None,
            stop_loss_trigger_px=None,
            long_votes=long_votes,
            short_votes=short_votes,
            required_votes=required_votes,
            snapshots=snapshots,
            reason=f"Only {len(snapshots)} usable snapshots; required {required_votes}.",
        )

    direction: Optional[str]
    if long_votes >= required_votes and long_votes > short_votes:
        direction = "long"
    elif short_votes >= required_votes and short_votes > long_votes:
        direction = "short"
    else:
        direction = None

    if direction is None:
        return AutoTradeDecision(
            direction=None,
            current_px=current_px,
            target_px=None,
            take_profit_pct=None,
            stop_loss_pct=None,
            stop_loss_trigger_px=None,
            long_votes=long_votes,
            short_votes=short_votes,
            required_votes=required_votes,
            snapshots=snapshots,
            reason=f"No trade: long_votes={long_votes}, short_votes={short_votes}, required={required_votes}.",
        )

    if direction == "long":
        candidate_targets = sorted(snapshot.bb_upper for snapshot in snapshots if snapshot.bb_upper > current_px)
        raw_target_px = candidate_targets[0] if candidate_targets else current_px * (1.0 + min_take_profit_pct)
        raw_take_profit_pct = max(0.0, (raw_target_px - current_px) / current_px)
    else:
        candidate_targets = sorted(
            (snapshot.bb_lower for snapshot in snapshots if snapshot.bb_lower < current_px),
            reverse=True,
        )
        raw_target_px = candidate_targets[0] if candidate_targets else current_px * (1.0 - min_take_profit_pct)
        raw_take_profit_pct = max(0.0, (current_px - raw_target_px) / current_px)

    if take_profit_pct_override is not None:
        take_profit_pct = take_profit_pct_override
    else:
        take_profit_pct = clamp_pct(raw_take_profit_pct, min_take_profit_pct, max_take_profit_pct)

    if direction == "long":
        target_px = current_px * (1.0 + take_profit_pct)
    else:
        target_px = current_px * (1.0 - take_profit_pct)

    stop_loss_pct = compute_default_stop_loss_pct(take_profit_pct, stop_loss_pct_override)
    stop_loss_trigger_px: Optional[float] = None
    shortest_snapshot = get_shortest_interval_snapshot(snapshots)
    if use_sar_stop_on_shortest_interval and shortest_snapshot is not None:
        candidate_stop = float(shortest_snapshot.sar)
        if direction == "long" and candidate_stop < current_px:
            stop_loss_trigger_px = candidate_stop
        elif direction == "short" and candidate_stop > current_px:
            stop_loss_trigger_px = candidate_stop
    return AutoTradeDecision(
        direction=direction,
        current_px=current_px,
        target_px=target_px,
        take_profit_pct=take_profit_pct,
        stop_loss_pct=stop_loss_pct,
        stop_loss_trigger_px=stop_loss_trigger_px,
        long_votes=long_votes,
        short_votes=short_votes,
        required_votes=required_votes,
        snapshots=snapshots,
        reason=(
            f"{direction.upper()} signal: long_votes={long_votes}, short_votes={short_votes}, "
            f"required={required_votes}, bb_target={raw_target_px:.8f}, tp_pct={take_profit_pct * 100:.4f}%"
        ),
    )


class DefaultAutoStrategy(AutoStrategy):
    name = "default"

    def __init__(self, config: object) -> None:
        self.config = config

    async def evaluate(self, context: StrategyContext) -> StrategySignal | None:
        decision = evaluate_default_decision(
            interval_candles=dict(context.market_metadata["interval_candles"]),
            current_px=float(context.market_metadata.get("current_px") or 0.0),
            intervals=list(self.config.intervals),
            periods=int(self.config.periods),
            min_agreement=int(self.config.min_agreement),
            adx_threshold=float(self.config.adx_threshold),
            take_profit_pct_override=self.config.take_profit_pct,
            stop_loss_pct_override=self.config.stop_loss_pct,
            min_take_profit_pct=float(self.config.min_take_profit_pct),
            max_take_profit_pct=float(self.config.max_take_profit_pct),
            macd_fast=int(self.config.macd_fast),
            macd_slow=int(self.config.macd_slow),
            macd_signal_period=int(self.config.macd_signal_period),
            sar_acceleration=float(self.config.sar_acceleration),
            sar_maximum=float(self.config.sar_maximum),
            adx_timeperiod=int(self.config.adx_timeperiod),
            bb_timeperiod=int(self.config.bb_timeperiod),
            bb_dev=float(self.config.bb_dev),
            use_last_closed_candle=bool(self.config.use_last_closed_candle),
            use_sar_stop_on_shortest_interval=bool(self.config.use_sar_stop_on_shortest_interval),
        )
        context.market_metadata["default_decision"] = decision
        if decision.direction is None or decision.take_profit_pct is None or decision.target_px is None:
            return None

        interval_to_closes = {snapshot.interval: list(snapshot.closes) for snapshot in decision.snapshots}
        bb_allowed, bb_reason, _ = confirm_signal_with_bollinger(
            side=decision.direction,
            interval_to_closes=interval_to_closes,
            active_intervals=list(self.config.intervals),
            scalp=bool(self.config.scalp),
            period=int(self.config.bb_timeperiod),
            stddev_multiplier=float(self.config.bb_dev),
        )
        context.market_metadata["bollinger_reason"] = bb_reason
        if not bb_allowed:
            return None

        shortest_snapshot = get_shortest_interval_snapshot(decision.snapshots)
        stop_price = decision.stop_loss_trigger_px
        if stop_price is None and decision.stop_loss_pct is not None:
            if decision.direction == "long":
                stop_price = decision.current_px * (1.0 - decision.stop_loss_pct)
            else:
                stop_price = decision.current_px * (1.0 + decision.stop_loss_pct)
        if stop_price is None:
            stop_price = decision.current_px

        reasons = tuple(snapshot.reason for snapshot in decision.snapshots) + (bb_reason, decision.reason)
        return StrategySignal(
            strategy=self.name,
            coin=context.coin,
            direction=decision.direction,  # type: ignore[arg-type]
            signal_candle_ms=int(context.now_ms),
            entry_price=float(decision.current_px),
            stop_price=float(stop_price),
            take_profit_prices=(float(decision.target_px),),
            score=float(max(decision.long_votes, decision.short_votes)),
            reasons=reasons,
            metadata={
                "decision": decision,
                "shortest_interval": shortest_snapshot.interval if shortest_snapshot is not None else None,
                "take_profit_pct": decision.take_profit_pct,
                "stop_loss_pct": decision.stop_loss_pct,
                "stop_loss_trigger_px": decision.stop_loss_trigger_px,
            },
        )

    def build_execution_plan(
        self,
        context: StrategyContext,
        signal: StrategySignal,
        size: float,
    ) -> StrategyExecutionPlan:
        return StrategyExecutionPlan(
            kind="bracket_entry",
            coin=signal.coin,
            direction=signal.direction,
            size=size,
            expected_entry=signal.entry_price,
            stop_price=signal.stop_price,
            take_profit_prices=signal.take_profit_prices,
            metadata=dict(signal.metadata),
        )
