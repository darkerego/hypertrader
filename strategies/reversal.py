from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass, field
from enum import Enum
from statistics import median
from typing import Any, Dict, Iterable, List, Literal, Mapping, Optional, Sequence, Tuple

from utils.constants import INTERVAL_TO_MS, PRICE_EPS

from strategies.base import AutoStrategy, StrategyContext, StrategyExecutionPlan, StrategySignal

try:
    import numpy as np
    import talib
except Exception:
    np = None  # type: ignore[assignment]
    talib = None  # type: ignore[assignment]


TrendLabel = Literal["up", "down", "none"]


class ReversalState(str, Enum):
    SEEKING_TREND = "seeking_trend"
    SEEKING_EXHAUSTION = "seeking_exhaustion"
    WAITING_FOR_STRUCTURE_BREAK = "waiting_for_structure_break"
    WAITING_FOR_RETEST = "waiting_for_retest"
    RETEST_CONFIRMED = "retest_confirmed"
    POSITION_OPEN = "position_open"
    COOLDOWN = "cooldown"


@dataclass(frozen=True)
class FractalPoint:
    index: int
    timestamp_ms: int
    price: float
    kind: Literal["high", "low"]


@dataclass(frozen=True)
class FractalSeries:
    highs: tuple[FractalPoint, ...]
    lows: tuple[FractalPoint, ...]


@dataclass(frozen=True)
class ExhaustionResult:
    score: int
    reasons: tuple[str, ...]
    reversal_extreme: Optional[float]


@dataclass(frozen=True)
class StructureBreak:
    direction: Literal["long", "short"]
    level: float
    close: float
    atr: float
    candle_ms: int
    body_atr: float


@dataclass(frozen=True)
class RetestResult:
    confirmed: bool
    expected_entry: Optional[float]
    stop_price: Optional[float]
    take_profit_prices: tuple[float, ...]
    expected_rr: Optional[float]
    reason: str
    candle_ms: int


@dataclass
class ReversalSetup:
    coin: str
    direction: Literal["long", "short"]
    state: ReversalState
    trend_interval: str
    entry_interval: str
    detected_at_ms: int
    last_processed_candle_ms: int
    exhaustion_score: int = 0
    exhaustion_reasons: List[str] = field(default_factory=list)
    structure_level: float = 0.0
    structure_break_close: float = 0.0
    structure_break_atr: float = 0.0
    structure_break_candle_ms: int = 0
    reversal_extreme: float = 0.0
    retest_deadline_candle_ms: Optional[int] = None
    retest_attempts: int = 0
    proposed_entry: Optional[float] = None
    proposed_stop: Optional[float] = None
    proposed_tp1: Optional[float] = None
    proposed_tp2: Optional[float] = None
    proposed_tp3: Optional[float] = None
    invalidation_reason: Optional[str] = None
    expected_rr: Optional[float] = None
    prior_trend: TrendLabel = "none"


def require_talib_available() -> None:
    if np is None or talib is None:
        raise RuntimeError(
            "The reversal auto strategy requires numpy and TA-Lib. Install them with:\n"
            "  pip install numpy TA-Lib"
        )


def _candle_open_ms(candle: Mapping[str, Any]) -> int:
    for key in ("t", "time", "openTime", "timestamp"):
        value = candle.get(key)
        if value is None:
            continue
        try:
            return int(float(value))
        except (TypeError, ValueError):
            continue
    raise RuntimeError(f"Candle is missing an open timestamp: {candle}")


def normalize_candles(candles: Sequence[Mapping[str, Any]], *, drop_incomplete: bool = True) -> list[dict[str, float]]:
    normalized: dict[int, dict[str, float]] = {}
    for candle in candles:
        try:
            open_ms = _candle_open_ms(candle)
            item = {
                "t": float(open_ms),
                "o": float(candle["o"]),
                "h": float(candle["h"]),
                "l": float(candle["l"]),
                "c": float(candle["c"]),
                "v": float(candle.get("v", 0.0)),
            }
        except (KeyError, TypeError, ValueError):
            continue
        if not all(math.isfinite(value) for value in item.values()):
            continue
        if item["o"] <= 0.0 or item["h"] <= 0.0 or item["l"] <= 0.0 or item["c"] <= 0.0:
            continue
        normalized[open_ms] = item

    ordered = [normalized[key] for key in sorted(normalized)]
    if drop_incomplete and len(ordered) > 1:
        ordered = ordered[:-1]
    return ordered


def _series(candles: Sequence[Mapping[str, float]], key: str) -> list[float]:
    return [float(candle[key]) for candle in candles]


def _timestamps(candles: Sequence[Mapping[str, float]]) -> list[int]:
    return [int(candle["t"]) for candle in candles]


def last_finite_index(*arrays: Any) -> int:
    if not arrays:
        raise RuntimeError("No arrays supplied for finite-index scan.")
    length = min(len(array) for array in arrays)
    for idx in range(length - 1, -1, -1):
        if all(math.isfinite(float(array[idx])) for array in arrays):
            return idx
    raise RuntimeError("No finite indicator row is available yet.")


def find_confirmed_fractals(candles: Sequence[Mapping[str, float]], width: int = 2) -> FractalSeries:
    if width != 2:
        raise RuntimeError("Only Williams 5-candle fractals (width=2) are supported.")
    if len(candles) < (width * 2 + 1):
        return FractalSeries(highs=(), lows=())

    highs = _series(candles, "h")
    lows = _series(candles, "l")
    times = _timestamps(candles)
    fractal_highs: list[FractalPoint] = []
    fractal_lows: list[FractalPoint] = []
    for idx in range(width, len(candles) - width):
        center_high = highs[idx]
        if (
            center_high > highs[idx - 1]
            and center_high > highs[idx - 2]
            and center_high > highs[idx + 1]
            and center_high > highs[idx + 2]
        ):
            fractal_highs.append(FractalPoint(index=idx, timestamp_ms=times[idx], price=center_high, kind="high"))

        center_low = lows[idx]
        if (
            center_low < lows[idx - 1]
            and center_low < lows[idx - 2]
            and center_low < lows[idx + 1]
            and center_low < lows[idx + 2]
        ):
            fractal_lows.append(FractalPoint(index=idx, timestamp_ms=times[idx], price=center_low, kind="low"))

    return FractalSeries(highs=tuple(fractal_highs), lows=tuple(fractal_lows))


def _ema(values: Sequence[float], period: int) -> list[float]:
    require_talib_available()
    return list(talib.EMA(np.asarray(values, dtype=float), timeperiod=period))  # type: ignore[union-attr]


def _rsi(values: Sequence[float], period: int) -> list[float]:
    require_talib_available()
    return list(talib.RSI(np.asarray(values, dtype=float), timeperiod=period))  # type: ignore[union-attr]


def _atr(candles: Sequence[Mapping[str, float]], period: int) -> list[float]:
    require_talib_available()
    return list(
        talib.ATR(
            np.asarray(_series(candles, "h"), dtype=float),  # type: ignore[union-attr]
            np.asarray(_series(candles, "l"), dtype=float),  # type: ignore[union-attr]
            np.asarray(_series(candles, "c"), dtype=float),  # type: ignore[union-attr]
            timeperiod=period,
        )
    )


def _adx(candles: Sequence[Mapping[str, float]], period: int) -> list[float]:
    require_talib_available()
    return list(
        talib.ADX(
            np.asarray(_series(candles, "h"), dtype=float),  # type: ignore[union-attr]
            np.asarray(_series(candles, "l"), dtype=float),  # type: ignore[union-attr]
            np.asarray(_series(candles, "c"), dtype=float),  # type: ignore[union-attr]
            timeperiod=period,
        )
    )


def _macd_hist(values: Sequence[float], fast: int, slow: int, signal: int) -> list[float]:
    require_talib_available()
    _macd, _sig, hist = talib.MACD(  # type: ignore[union-attr]
        np.asarray(values, dtype=float),
        fastperiod=fast,
        slowperiod=slow,
        signalperiod=signal,
    )
    return list(hist)


def _bollinger(values: Sequence[float], period: int, dev: float) -> tuple[list[float], list[float], list[float]]:
    require_talib_available()
    upper, middle, lower = talib.BBANDS(  # type: ignore[union-attr]
        np.asarray(values, dtype=float),
        timeperiod=period,
        nbdevup=dev,
        nbdevdn=dev,
        matype=0,
    )
    return list(upper), list(middle), list(lower)


def _sar(candles: Sequence[Mapping[str, float]], acceleration: float, maximum: float) -> list[float]:
    require_talib_available()
    return list(
        talib.SAR(
            np.asarray(_series(candles, "h"), dtype=float),  # type: ignore[union-attr]
            np.asarray(_series(candles, "l"), dtype=float),  # type: ignore[union-attr]
            acceleration=acceleration,
            maximum=maximum,
        )
    )


def classify_prior_trend(
    candles: Sequence[Mapping[str, float]],
    fractals: FractalSeries,
    ema_fast_period: int,
    ema_slow_period: int,
    adx_period: int,
    min_adx: float,
    min_abs_ema_slope: float,
) -> TrendLabel:
    if len(candles) < max(ema_slow_period + 5, adx_period + 5, 20):
        return "none"

    closes = _series(candles, "c")
    ema_fast = _ema(closes, ema_fast_period)
    ema_slow = _ema(closes, ema_slow_period)
    adx_values = _adx(candles, adx_period)
    idx = last_finite_index(ema_fast, ema_slow, adx_values)
    if idx < 4:
        return "none"

    latest_close = closes[idx]
    latest_ema_fast = float(ema_fast[idx])
    latest_ema_slow = float(ema_slow[idx])
    latest_adx = float(adx_values[idx])
    ema50_slope = (latest_ema_slow - float(ema_slow[idx - 3])) / max(abs(float(ema_slow[idx - 3])), 1e-12)
    if latest_adx < min_adx or abs(ema50_slope) < min_abs_ema_slope:
        return "none"

    recent_highs = [point for point in fractals.highs if point.index <= idx][-2:]
    recent_lows = [point for point in fractals.lows if point.index <= idx][-2:]
    if len(recent_highs) < 2 or len(recent_lows) < 2:
        return "none"

    highs_ascending = recent_highs[-1].price > recent_highs[-2].price
    lows_ascending = recent_lows[-1].price > recent_lows[-2].price
    highs_descending = recent_highs[-1].price < recent_highs[-2].price
    lows_descending = recent_lows[-1].price < recent_lows[-2].price

    if (
        latest_ema_fast > latest_ema_slow
        and latest_close > latest_ema_slow
        and ema50_slope > 0.0
        and highs_ascending
        and lows_ascending
    ):
        return "up"
    if (
        latest_ema_fast < latest_ema_slow
        and latest_close < latest_ema_slow
        and ema50_slope < 0.0
        and highs_descending
        and lows_descending
    ):
        return "down"
    return "none"


def _extreme_fractal(
    fractals: Iterable[FractalPoint],
    lookback_start_idx: int,
    current_idx: int,
    kind: Literal["high", "low"],
) -> Optional[FractalPoint]:
    recent = [point for point in fractals if lookback_start_idx <= point.index <= current_idx]
    if not recent:
        return None
    if kind == "low":
        return min(recent, key=lambda point: point.price)
    return max(recent, key=lambda point: point.price)


def calculate_exhaustion(
    *,
    candles: Sequence[Mapping[str, float]],
    fractals: FractalSeries,
    direction: Literal["long", "short"],
    atr_period: int,
    rsi_period: int,
    volume_multiple: float,
    extension_atr_multiple: float,
    divergence_lookback: int,
    min_swing_separation: int,
    max_swing_separation: int,
    min_rsi_divergence: float,
    min_price_divergence_atr: float,
    sar_acceleration: float,
    sar_maximum: float,
) -> ExhaustionResult:
    if len(candles) < max(atr_period + 5, rsi_period + 5, 30):
        return ExhaustionResult(score=0, reasons=(), reversal_extreme=None)

    closes = _series(candles, "c")
    highs = _series(candles, "h")
    lows = _series(candles, "l")
    volumes = _series(candles, "v")
    atr_values = _atr(candles, atr_period)
    rsi_values = _rsi(closes, rsi_period)
    macd_hist = _macd_hist(closes, 12, 26, 9)
    upper_bb, _middle_bb, lower_bb = _bollinger(closes, 20, 2.0)
    sar_values = _sar(candles, sar_acceleration, sar_maximum)
    idx = last_finite_index(atr_values, rsi_values, macd_hist, upper_bb, lower_bb, sar_values)
    reasons: list[str] = []
    reversal_extreme: Optional[float] = None
    atr_now = float(atr_values[idx])
    if atr_now <= 0.0:
        return ExhaustionResult(score=0, reasons=(), reversal_extreme=None)

    recent_volume = [value for value in volumes[max(0, idx - 20):idx] if math.isfinite(value) and value > 0.0]
    median_volume = median(recent_volume) if recent_volume else 0.0

    if direction == "long":
        swing_points = [point for point in fractals.lows if idx - divergence_lookback <= point.index <= idx]
        reversal_extreme = min((point.price for point in swing_points), default=lows[idx])
        if len(swing_points) >= 2:
            latest = swing_points[-1]
            previous_candidates = [
                point
                for point in swing_points[:-1]
                if min_swing_separation <= latest.index - point.index <= max_swing_separation
            ]
            if previous_candidates:
                previous = previous_candidates[-1]
                price_diff = previous.price - latest.price
                rsi_diff = float(rsi_values[latest.index]) - float(rsi_values[previous.index])
                if price_diff >= (min_price_divergence_atr * atr_now) and rsi_diff >= min_rsi_divergence:
                    reasons.append("RSI divergence")
        if idx >= 2 and macd_hist[idx] < 0.0 and macd_hist[idx] > macd_hist[idx - 1] > macd_hist[idx - 2]:
            reasons.append("MACD weakening")
        if lows[idx] <= lower_bb[idx]:
            reasons.append("lower BB extension")
        if (highs[idx] - lows[idx]) >= extension_atr_multiple * atr_now:
            reasons.append("ATR downside extension")
        if median_volume > 0.0 and volumes[idx] >= volume_multiple * median_volume:
            reasons.append("volume climax")
        if idx >= 1 and sar_values[idx - 1] >= closes[idx - 1] and sar_values[idx] < closes[idx]:
            reasons.append("SAR flip")
        extreme_fractal = _extreme_fractal(fractals.lows, max(0, idx - divergence_lookback), idx, "low")
        if extreme_fractal is not None and abs(extreme_fractal.price - reversal_extreme) <= PRICE_EPS * max(1.0, reversal_extreme):
            reasons.append("extreme fractal low")
    else:
        swing_points = [point for point in fractals.highs if idx - divergence_lookback <= point.index <= idx]
        reversal_extreme = max((point.price for point in swing_points), default=highs[idx])
        if len(swing_points) >= 2:
            latest = swing_points[-1]
            previous_candidates = [
                point
                for point in swing_points[:-1]
                if min_swing_separation <= latest.index - point.index <= max_swing_separation
            ]
            if previous_candidates:
                previous = previous_candidates[-1]
                price_diff = latest.price - previous.price
                rsi_diff = float(rsi_values[previous.index]) - float(rsi_values[latest.index])
                if price_diff >= (min_price_divergence_atr * atr_now) and rsi_diff >= min_rsi_divergence:
                    reasons.append("RSI divergence")
        if idx >= 2 and macd_hist[idx] > 0.0 and macd_hist[idx] < macd_hist[idx - 1] < macd_hist[idx - 2]:
            reasons.append("MACD weakening")
        if highs[idx] >= upper_bb[idx]:
            reasons.append("upper BB extension")
        if (highs[idx] - lows[idx]) >= extension_atr_multiple * atr_now:
            reasons.append("ATR upside extension")
        if median_volume > 0.0 and volumes[idx] >= volume_multiple * median_volume:
            reasons.append("volume climax")
        if idx >= 1 and sar_values[idx - 1] <= closes[idx - 1] and sar_values[idx] > closes[idx]:
            reasons.append("SAR flip")
        extreme_fractal = _extreme_fractal(fractals.highs, max(0, idx - divergence_lookback), idx, "high")
        if extreme_fractal is not None and abs(extreme_fractal.price - reversal_extreme) <= PRICE_EPS * max(1.0, reversal_extreme):
            reasons.append("extreme fractal high")

    deduped_reasons = tuple(dict.fromkeys(reasons))
    return ExhaustionResult(score=len(deduped_reasons), reasons=deduped_reasons, reversal_extreme=reversal_extreme)


def detect_structure_break(
    *,
    candles: Sequence[Mapping[str, float]],
    fractals: FractalSeries,
    direction: Literal["long", "short"],
    reversal_extreme: float,
    atr_period: int,
    breakout_body_atr: float,
    max_breakout_range_atr: float,
    ema_fast_period: int,
    ema_confirm_period: int,
    sar_acceleration: float,
    sar_maximum: float,
) -> Optional[StructureBreak]:
    if len(candles) < max(atr_period + 5, ema_confirm_period + 5, 30):
        return None
    closes = _series(candles, "c")
    opens = _series(candles, "o")
    highs = _series(candles, "h")
    lows = _series(candles, "l")
    times = _timestamps(candles)
    atr_values = _atr(candles, atr_period)
    ema_fast = _ema(closes, ema_fast_period)
    ema_confirm = _ema(closes, ema_confirm_period)
    sar_values = _sar(candles, sar_acceleration, sar_maximum)
    idx = last_finite_index(atr_values, ema_fast, ema_confirm, sar_values)
    atr_now = float(atr_values[idx])
    body = abs(closes[idx] - opens[idx])
    range_now = highs[idx] - lows[idx]
    if atr_now <= 0.0 or body < breakout_body_atr * atr_now or range_now > max_breakout_range_atr * atr_now:
        return None

    if direction == "long":
        structure_candidates = [
            point for point in fractals.highs if point.index < idx and point.price >= reversal_extreme - (0.25 * atr_now)
        ]
        if not structure_candidates:
            return None
        structure = structure_candidates[-1]
        ema_ok = float(ema_fast[idx]) > float(ema_confirm[idx]) or float(ema_fast[idx]) > float(ema_fast[idx - 1])
        sar_ok = sar_values[idx] < closes[idx]
        if closes[idx] > structure.price and ema_ok and sar_ok:
            return StructureBreak(
                direction="long",
                level=float(structure.price),
                close=float(closes[idx]),
                atr=atr_now,
                candle_ms=times[idx],
                body_atr=body / atr_now,
            )
    else:
        structure_candidates = [
            point for point in fractals.lows if point.index < idx and point.price <= reversal_extreme + (0.25 * atr_now)
        ]
        if not structure_candidates:
            return None
        structure = structure_candidates[-1]
        ema_ok = float(ema_fast[idx]) < float(ema_confirm[idx]) or float(ema_fast[idx]) < float(ema_fast[idx - 1])
        sar_ok = sar_values[idx] > closes[idx]
        if closes[idx] < structure.price and ema_ok and sar_ok:
            return StructureBreak(
                direction="short",
                level=float(structure.price),
                close=float(closes[idx]),
                atr=atr_now,
                candle_ms=times[idx],
                body_atr=body / atr_now,
            )
    return None


def _find_opposing_structure(
    direction: Literal["long", "short"],
    entry: float,
    fractals: FractalSeries,
    current_idx: int,
) -> Optional[float]:
    if direction == "long":
        candidates = [point.price for point in fractals.highs if point.index > current_idx and point.price > entry]
        return min(candidates) if candidates else None
    candidates = [point.price for point in fractals.lows if point.index > current_idx and point.price < entry]
    return max(candidates) if candidates else None


def evaluate_retest(
    *,
    candles: Sequence[Mapping[str, float]],
    structure_break: StructureBreak,
    reversal_extreme: float,
    timeout_candles: int,
    retest_atr_tolerance: float,
    retest_min_price_pct: float,
    stop_atr_buffer: float,
    max_stop_atr: float,
    min_rr: float,
    tp1_r: float,
    tp2_r: float,
    tp3_r: float,
    sar_acceleration: float,
    sar_maximum: float,
    fractals: FractalSeries,
    entry_interval_ms: int,
) -> RetestResult:
    closes = _series(candles, "c")
    opens = _series(candles, "o")
    highs = _series(candles, "h")
    lows = _series(candles, "l")
    times = _timestamps(candles)
    atr_values = _atr(candles, 14)
    sar_values = _sar(candles, sar_acceleration, sar_maximum)
    idx = last_finite_index(atr_values, sar_values)

    breakout_idx = next((i for i, ts in enumerate(times) if ts == structure_break.candle_ms), -1)
    if breakout_idx < 0:
        return RetestResult(False, None, None, (), None, "breakout candle missing", times[idx])
    expiry_ms = structure_break.candle_ms + (timeout_candles * entry_interval_ms)

    for candle_idx in range(breakout_idx + 1, len(candles)):
        current_atr = float(atr_values[candle_idx])
        level = structure_break.level
        tolerance = max(retest_atr_tolerance * current_atr, level * retest_min_price_pct)
        candle_time = times[candle_idx]
        if candle_time > expiry_ms:
            return RetestResult(False, None, None, (), None, "retest timeout expired", candle_time)

        if structure_break.direction == "long":
            if closes[candle_idx] < (reversal_extreme - (0.10 * current_atr)):
                return RetestResult(False, None, None, (), None, "reversal extreme broken", candle_time)
            lower_wick = min(opens[candle_idx], closes[candle_idx]) - lows[candle_idx]
            body = abs(closes[candle_idx] - opens[candle_idx])
            rejection_ok = lower_wick >= max(body * 1.25, 0.10 * current_atr)
            if (
                lows[candle_idx] <= level + tolerance
                and closes[candle_idx] >= level
                and closes[candle_idx] >= opens[candle_idx] or rejection_ok
            ):
                if closes[candle_idx] < level - tolerance or sar_values[candle_idx] >= closes[candle_idx]:
                    return RetestResult(False, None, None, (), None, "bullish retest invalidated", candle_time)
                expected_entry = closes[candle_idx]
                stop_price = reversal_extreme - (stop_atr_buffer * current_atr)
                risk = expected_entry - stop_price
                if risk <= 0.0 or risk > (max_stop_atr * current_atr):
                    return RetestResult(False, None, None, (), None, "stop distance rejected", candle_time)
                tp1 = expected_entry + (tp1_r * risk)
                tp2 = expected_entry + (tp2_r * risk)
                opposing = _find_opposing_structure("long", expected_entry, fractals, candle_idx)
                tp3 = opposing if opposing is not None else expected_entry + (tp3_r * risk)
                expected_rr = (tp3 - expected_entry) / risk
                if expected_rr < min_rr:
                    return RetestResult(False, None, None, (), expected_rr, "reward-to-risk rejected", candle_time)
                return RetestResult(True, expected_entry, stop_price, (tp1, tp2, tp3), expected_rr, "retest confirmed", candle_time)
        else:
            if closes[candle_idx] > (reversal_extreme + (0.10 * current_atr)):
                return RetestResult(False, None, None, (), None, "reversal extreme broken", candle_time)
            upper_wick = highs[candle_idx] - max(opens[candle_idx], closes[candle_idx])
            body = abs(closes[candle_idx] - opens[candle_idx])
            rejection_ok = upper_wick >= max(body * 1.25, 0.10 * current_atr)
            if (
                highs[candle_idx] >= level - tolerance
                and closes[candle_idx] <= level
                and closes[candle_idx] <= opens[candle_idx] or rejection_ok
            ):
                if closes[candle_idx] > level + tolerance or sar_values[candle_idx] <= closes[candle_idx]:
                    return RetestResult(False, None, None, (), None, "bearish retest invalidated", candle_time)
                expected_entry = closes[candle_idx]
                stop_price = reversal_extreme + (stop_atr_buffer * current_atr)
                risk = stop_price - expected_entry
                if risk <= 0.0 or risk > (max_stop_atr * current_atr):
                    return RetestResult(False, None, None, (), None, "stop distance rejected", candle_time)
                tp1 = expected_entry - (tp1_r * risk)
                tp2 = expected_entry - (tp2_r * risk)
                opposing = _find_opposing_structure("short", expected_entry, fractals, candle_idx)
                tp3 = opposing if opposing is not None else expected_entry - (tp3_r * risk)
                expected_rr = (expected_entry - tp3) / risk
                if expected_rr < min_rr:
                    return RetestResult(False, None, None, (), expected_rr, "reward-to-risk rejected", candle_time)
                return RetestResult(True, expected_entry, stop_price, (tp1, tp2, tp3), expected_rr, "retest confirmed", candle_time)

    return RetestResult(False, None, None, (), None, "awaiting retest", times[idx])


class ReversalAutoStrategy(AutoStrategy):
    name = "reversal"

    def __init__(self, config: object) -> None:
        self.config = config
        self._setups: dict[str, ReversalSetup] = {}
        self._lock = asyncio.Lock()

    def _state_log(self, coin: str, message: str) -> None:
        print(f"[REVERSAL][{coin}][{self.config.entry_interval}] {message}")

    def _invalidate(self, setup: ReversalSetup, reason: str) -> None:
        setup.invalidation_reason = reason
        setup.state = ReversalState.SEEKING_TREND
        self._state_log(setup.coin, f"Setup invalidated: {reason}")

    async def evaluate(self, context: StrategyContext) -> StrategySignal | None:
        require_talib_available()
        coin = context.coin.upper()
        async with self._lock:
            entry_candles = normalize_candles(context.entry_candles)
            trend_candles = normalize_candles(context.trend_candles)
            if not entry_candles or not trend_candles:
                return None

            latest_entry_ms = int(entry_candles[-1]["t"])
            setup = self._setups.get(coin)
            if setup is not None and setup.last_processed_candle_ms == latest_entry_ms:
                return None

            trend_fractals = find_confirmed_fractals(trend_candles, width=int(self.config.reversal_fractal_width))
            prior_trend = classify_prior_trend(
                trend_candles,
                trend_fractals,
                ema_fast_period=int(self.config.reversal_ema_trend_fast),
                ema_slow_period=int(self.config.reversal_ema_trend_slow),
                adx_period=int(self.config.reversal_adx_period),
                min_adx=float(self.config.reversal_min_adx),
                min_abs_ema_slope=float(self.config.reversal_min_ema50_slope),
            )
            if prior_trend == "none":
                self._setups[coin] = ReversalSetup(
                    coin=coin,
                    direction="long",
                    state=ReversalState.SEEKING_TREND,
                    trend_interval=self.config.trend_interval,
                    entry_interval=self.config.entry_interval,
                    detected_at_ms=latest_entry_ms,
                    last_processed_candle_ms=latest_entry_ms,
                    prior_trend=prior_trend,
                )
                return None

            direction: Literal["long", "short"] = "long" if prior_trend == "down" else "short"
            entry_fractals = find_confirmed_fractals(entry_candles, width=int(self.config.reversal_fractal_width))
            exhaustion = calculate_exhaustion(
                candles=entry_candles,
                fractals=entry_fractals,
                direction=direction,
                atr_period=int(self.config.reversal_atr_period),
                rsi_period=int(self.config.reversal_rsi_period),
                volume_multiple=float(self.config.reversal_volume_climax_multiple),
                extension_atr_multiple=float(self.config.reversal_extension_atr_multiple),
                divergence_lookback=int(self.config.reversal_divergence_lookback),
                min_swing_separation=int(self.config.reversal_min_swing_separation),
                max_swing_separation=int(self.config.reversal_max_swing_separation),
                min_rsi_divergence=float(self.config.reversal_min_rsi_divergence),
                min_price_divergence_atr=float(self.config.reversal_min_price_divergence_atr),
                sar_acceleration=float(self.config.sar_acceleration),
                sar_maximum=float(self.config.sar_maximum),
            )
            if exhaustion.score < int(self.config.reversal_exhaustion_score) or exhaustion.reversal_extreme is None:
                self._setups[coin] = ReversalSetup(
                    coin=coin,
                    direction=direction,
                    state=ReversalState.SEEKING_EXHAUSTION,
                    trend_interval=self.config.trend_interval,
                    entry_interval=self.config.entry_interval,
                    detected_at_ms=latest_entry_ms,
                    last_processed_candle_ms=latest_entry_ms,
                    exhaustion_score=exhaustion.score,
                    exhaustion_reasons=list(exhaustion.reasons),
                    reversal_extreme=float(exhaustion.reversal_extreme or 0.0),
                    prior_trend=prior_trend,
                )
                return None

            structure_break = detect_structure_break(
                candles=entry_candles,
                fractals=entry_fractals,
                direction=direction,
                reversal_extreme=float(exhaustion.reversal_extreme),
                atr_period=int(self.config.reversal_atr_period),
                breakout_body_atr=float(self.config.reversal_breakout_body_atr),
                max_breakout_range_atr=float(self.config.reversal_max_breakout_range_atr),
                ema_fast_period=int(self.config.reversal_ema_fast),
                ema_confirm_period=int(self.config.reversal_ema_confirm),
                sar_acceleration=float(self.config.sar_acceleration),
                sar_maximum=float(self.config.sar_maximum),
            )
            if structure_break is None:
                self._setups[coin] = ReversalSetup(
                    coin=coin,
                    direction=direction,
                    state=ReversalState.WAITING_FOR_STRUCTURE_BREAK,
                    trend_interval=self.config.trend_interval,
                    entry_interval=self.config.entry_interval,
                    detected_at_ms=latest_entry_ms,
                    last_processed_candle_ms=latest_entry_ms,
                    exhaustion_score=exhaustion.score,
                    exhaustion_reasons=list(exhaustion.reasons),
                    reversal_extreme=float(exhaustion.reversal_extreme),
                    prior_trend=prior_trend,
                )
                self._state_log(
                    coin,
                    f"Prior trend: {prior_trend.upper()}, exhaustion {exhaustion.score}/{self.config.reversal_exhaustion_score}: "
                    + ", ".join(exhaustion.reasons),
                )
                return None

            retest = evaluate_retest(
                candles=entry_candles,
                structure_break=structure_break,
                reversal_extreme=float(exhaustion.reversal_extreme),
                timeout_candles=int(self.config.reversal_retest_timeout),
                retest_atr_tolerance=float(self.config.reversal_retest_atr_tolerance),
                retest_min_price_pct=float(self.config.reversal_retest_min_price_pct),
                stop_atr_buffer=float(self.config.reversal_stop_atr_buffer),
                max_stop_atr=float(self.config.reversal_max_stop_atr),
                min_rr=float(self.config.reversal_min_rr),
                tp1_r=float(self.config.reversal_tp1_r),
                tp2_r=float(self.config.reversal_tp2_r),
                tp3_r=float(self.config.reversal_tp3_r),
                sar_acceleration=float(self.config.sar_acceleration),
                sar_maximum=float(self.config.sar_maximum),
                fractals=entry_fractals,
                entry_interval_ms=int(INTERVAL_TO_MS[self.config.entry_interval]),
            )
            new_setup = ReversalSetup(
                coin=coin,
                direction=direction,
                state=ReversalState.WAITING_FOR_RETEST if not retest.confirmed else ReversalState.RETEST_CONFIRMED,
                trend_interval=self.config.trend_interval,
                entry_interval=self.config.entry_interval,
                detected_at_ms=latest_entry_ms,
                last_processed_candle_ms=latest_entry_ms,
                exhaustion_score=exhaustion.score,
                exhaustion_reasons=list(exhaustion.reasons),
                structure_level=structure_break.level,
                structure_break_close=structure_break.close,
                structure_break_atr=structure_break.atr,
                structure_break_candle_ms=structure_break.candle_ms,
                reversal_extreme=float(exhaustion.reversal_extreme),
                retest_deadline_candle_ms=structure_break.candle_ms + (int(self.config.reversal_retest_timeout) * int(INTERVAL_TO_MS[self.config.entry_interval])),
                proposed_entry=retest.expected_entry,
                proposed_stop=retest.stop_price,
                proposed_tp1=retest.take_profit_prices[0] if len(retest.take_profit_prices) > 0 else None,
                proposed_tp2=retest.take_profit_prices[1] if len(retest.take_profit_prices) > 1 else None,
                proposed_tp3=retest.take_profit_prices[2] if len(retest.take_profit_prices) > 2 else None,
                invalidation_reason=None if retest.confirmed else (None if retest.reason == "awaiting retest" else retest.reason),
                expected_rr=retest.expected_rr,
                prior_trend=prior_trend,
            )
            self._setups[coin] = new_setup

            self._state_log(coin, f"Prior trend: {prior_trend.upper()}, ADX gate passed")
            self._state_log(coin, f"Exhaustion {exhaustion.score}/{self.config.reversal_exhaustion_score}: {', '.join(exhaustion.reasons)}")
            self._state_log(coin, f"Structure break confirmed: close={structure_break.close:.8f}, body={structure_break.body_atr:.2f} ATR")
            if not retest.confirmed:
                if retest.reason == "awaiting retest":
                    tolerance = max(
                        float(self.config.reversal_retest_atr_tolerance) * structure_break.atr,
                        structure_break.level * float(self.config.reversal_retest_min_price_pct),
                    )
                    self._state_log(
                        coin,
                        f"Waiting for retest: level={structure_break.level:.8f}, tolerance={tolerance:.8f}, "
                        f"expires in {self.config.reversal_retest_timeout} candles",
                    )
                    return None
                self._invalidate(new_setup, retest.reason)
                return None

            self._state_log(coin, f"Retest confirmed: expected entry={retest.expected_entry:.8f}")
            self._state_log(
                coin,
                f"Proposed stop={retest.stop_price:.8f}, TP1={retest.take_profit_prices[0]:.8f}, "
                f"TP2={retest.take_profit_prices[1]:.8f}, R:R={retest.expected_rr:.2f}",
            )
            return StrategySignal(
                strategy=self.name,
                coin=coin,
                direction=direction,
                signal_candle_ms=retest.candle_ms,
                entry_price=float(retest.expected_entry),
                stop_price=float(retest.stop_price),
                take_profit_prices=tuple(float(value) for value in retest.take_profit_prices),
                score=float(exhaustion.score + structure_break.body_atr + (retest.expected_rr or 0.0)),
                reasons=tuple(exhaustion.reasons) + ("structure break", "retest confirmed"),
                metadata={
                    "prior_trend": prior_trend,
                    "trend_interval": self.config.trend_interval,
                    "entry_interval": self.config.entry_interval,
                    "exhaustion_score": exhaustion.score,
                    "exhaustion_reasons": tuple(exhaustion.reasons),
                    "structure_level": structure_break.level,
                    "structure_break_timestamp": structure_break.candle_ms,
                    "reversal_extreme": float(exhaustion.reversal_extreme),
                    "expected_rr": retest.expected_rr,
                    "runner_exit_on_sar_flip": bool(self.config.reversal_exit_on_sar_flip),
                    "tp_allocations": (
                        float(self.config.reversal_tp1_pct),
                        float(self.config.reversal_tp2_pct),
                        float(self.config.reversal_runner_pct),
                    ),
                },
            )

    def build_execution_plan(
        self,
        context: StrategyContext,
        signal: StrategySignal,
        size: float,
    ) -> StrategyExecutionPlan:
        return StrategyExecutionPlan(
            kind="reversal",
            coin=signal.coin,
            direction=signal.direction,
            size=size,
            expected_entry=signal.entry_price,
            stop_price=signal.stop_price,
            take_profit_prices=signal.take_profit_prices,
            metadata=dict(signal.metadata),
        )

    def rank_signal(self, signal: StrategySignal) -> tuple[float, int]:
        return signal.score, signal.signal_candle_ms

    def on_position_update(self, context: StrategyContext) -> None:
        setup = self._setups.get(context.coin.upper())
        if setup is None:
            return
        setup.state = ReversalState.POSITION_OPEN if context.current_position is not None else ReversalState.SEEKING_TREND


async def normalize_signal_prices(
    info: Any,
    signal: StrategySignal,
) -> StrategySignal:
    from utils.helpers import round_price_for_hyperliquid

    stop_price = await round_price_for_hyperliquid(info, signal.coin, signal.stop_price)
    tp_prices = tuple(await round_price_for_hyperliquid(info, signal.coin, price) for price in signal.take_profit_prices)
    entry_price = await round_price_for_hyperliquid(info, signal.coin, signal.entry_price)
    return StrategySignal(
        strategy=signal.strategy,
        coin=signal.coin,
        direction=signal.direction,
        signal_candle_ms=signal.signal_candle_ms,
        entry_price=entry_price,
        stop_price=stop_price,
        take_profit_prices=tp_prices,
        score=signal.score,
        reasons=signal.reasons,
        metadata=signal.metadata,
    )
