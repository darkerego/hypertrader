"""Confirmed-impulse Fibonacci pullbacks; execution belongs to the auto engine."""
from __future__ import annotations

import asyncio
import math
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Literal, Mapping, Sequence

from strategies.base import AutoStrategy, StrategyContext, StrategyExecutionPlan, StrategySignal
from utils.constants import INTERVAL_TO_MS

try:
    import numpy as np
    import talib
except ImportError:
    np = None
    talib = None

Direction = Literal["long", "short"]
RATIOS = (0.236, 0.382, 0.5, 0.618, 0.786)
FIB_DEFAULTS = {
    "fib_entry_levels": "0.382,0.5,0.618", "fib_pivot_width": 2,
    "fib_impulse_lookback": 120, "fib_min_impulse_atr": 2.0, "fib_min_impulse_bars": 5,
    "fib_zone_atr": 0.15, "fib_invalidation_atr": 0.10, "fib_stop_atr_buffer": 0.25,
    "fib_max_entry_distance_atr": 0.50, "fib_atr_period": 14,
    "fib_macd_fast": 12, "fib_macd_slow": 26, "fib_macd_signal": 9,
    "fib_adx_period": 14, "fib_min_adx": 18.0, "fib_strong_adx": 25.0,
    "fib_roc_period": 9, "fib_roc": True, "fib_trend_ema_fast": 20,
    "fib_trend_ema_slow": 50, "fib_confirm_timeout": 4, "fib_min_score": 5, "fib_min_rr": 1.5,
}


def parse_entry_levels(value: str | Sequence[float]) -> tuple[float, ...]:
    parts = re.split(r"[,\s]+", value.strip()) if isinstance(value, str) else value
    try:
        ratios = tuple(sorted(set(float(part) for part in parts)))
    except (TypeError, ValueError) as exc:
        raise ValueError("--fib-entry-levels must contain numeric ratios") from exc
    if not ratios or any(not math.isfinite(r) or not 0 < r < 1 for r in ratios):
        raise ValueError("--fib-entry-levels requires ratios strictly between 0 and 1")
    return ratios


def validate_fib_config(config: Any) -> None:
    parse_entry_levels(config.fib_entry_levels)
    for name, default in FIB_DEFAULTS.items():
        if isinstance(default, (str, bool)):
            continue
        value = getattr(config, name)
        if not math.isfinite(value) or (value < 0 if name == "fib_invalidation_atr" else value <= 0):
            raise ValueError(f"--{name.replace('_', '-')} must be finite and {'>= 0' if name == 'fib_invalidation_atr' else '> 0'}")
    for fast, slow in (("fib_macd_fast", "fib_macd_slow"), ("fib_trend_ema_fast", "fib_trend_ema_slow")):
        if getattr(config, fast) >= getattr(config, slow):
            raise ValueError(f"--{fast.replace('_', '-')} must be less than --{slow.replace('_', '-')}")
    if config.fib_strong_adx < config.fib_min_adx:
        raise ValueError("--fib-strong-adx must be >= --fib-min-adx")
    if config.fib_min_score > (10 if config.fib_roc else 9):
        raise ValueError("--fib-min-score exceeds the maximum achievable score")
    # TA-Lib rejects period 1 for these functions.
    for name in ("fib_adx_period", "fib_macd_fast", "fib_macd_slow", "fib_trend_ema_fast", "fib_trend_ema_slow"):
        if getattr(config, name) < 2:
            raise ValueError(f"--{name.replace('_', '-')} must be >= 2 for TA-Lib")


def require_talib_available() -> None:
    if np is None or talib is None:
        raise RuntimeError("The fib_reversal auto strategy requires numpy and TA-Lib. Install them with:\n  pip install numpy TA-Lib")


@dataclass(frozen=True)
class FibPivot:
    index: int
    timestamp_ms: int
    price: float
    kind: Literal["high", "low"]


@dataclass(frozen=True)
class FibImpulse:
    direction: Direction
    start_index: int
    end_index: int
    start_timestamp_ms: int
    end_timestamp_ms: int
    start_price: float
    end_price: float
    atr: float
    move_abs: float
    move_atr: float
    move_pct: float

    @property
    def impulse_id(self) -> tuple[str, int, int]:
        return self.direction, self.start_timestamp_ms, self.end_timestamp_ms


@dataclass(frozen=True)
class FibLevel:
    ratio: float
    price: float
    zone_low: float
    zone_high: float


@dataclass(frozen=True)
class FibConfirmation:
    score: int
    max_score: int
    reasons: tuple[str, ...]
    rejection: bool
    macd_hist_rotation: bool
    macd_crossover: bool
    dmi_aligned: bool
    adx_rising: bool
    adx_value: float
    plus_di: float
    minus_di: float
    roc_value: float
    roc_confirmed: bool
    htf_aligned: bool


class FibSetupState(str, Enum):
    WAITING_FOR_IMPULSE = "waiting_for_impulse"
    WAITING_FOR_RETRACE = "waiting_for_retrace"
    IN_FIB_ZONE = "in_fib_zone"
    WAITING_FOR_CONFIRMATION = "waiting_for_confirmation"
    SIGNALLED = "signalled"
    INVALIDATED = "invalidated"


@dataclass
class FibSetup:
    coin: str
    state: FibSetupState
    impulse: FibImpulse
    active_ratio: float | None = None
    active_level_price: float | None = None
    active_zone_low: float | None = None
    active_zone_high: float | None = None
    last_processed_candle_ms: int = 0
    entered_zone_candle_ms: int | None = None
    confirmation_deadline_ms: int | None = None
    invalidation_reason: str | None = None
    traded: bool = False
    last_confirmation: tuple[int, tuple[str, ...]] | None = None


def normalize_candles(candles: Sequence[Mapping[str, Any]], *, drop_incomplete: bool = True) -> list[dict[str, float]]:
    rows = {}
    for candle in candles:
        try:
            row = {key: float(candle[key]) for key in ("t", "o", "h", "l", "c", "v")}
        except (KeyError, ValueError, TypeError, OverflowError):
            continue
        if not all(math.isfinite(v) for v in row.values()):
            continue
        if min(row[k] for k in "ohlc") <= 0 or row["v"] < 0 or row["t"] < 0:
            continue
        if row["l"] > min(row["o"], row["c"]) or row["h"] < max(row["o"], row["c"]):
            continue
        rows[int(row["t"])] = row
    ordered = [rows[t] for t in sorted(rows)]
    return ordered[:-1] if drop_incomplete else ordered


def last_finite_index(*arrays: Any, rows: int = 1) -> int | None:
    if not arrays:
        return None
    for i in range(min(map(len, arrays)) - 1, rows - 2, -1):
        if all(math.isfinite(float(a[j])) for a in arrays for j in range(i - rows + 1, i + 1)):
            return i
    return None


def _arrays(candles: Sequence[Mapping[str, float]]) -> tuple[Any, Any, Any]:
    require_talib_available()
    return tuple(np.asarray([c[k] for c in candles], dtype=float) for k in "hlc")


def _atr(candles: Sequence[Mapping[str, float]], period: int) -> Any:
    require_talib_available()
    return talib.ATR(*_arrays(candles), timeperiod=period)


def find_confirmed_pivots(candles: Sequence[Mapping[str, float]], width: int = 2) -> tuple[tuple[FibPivot, ...], tuple[FibPivot, ...]]:
    if width <= 0:
        raise ValueError("pivot width must be positive")
    highs, lows = [], []
    for i in range(width, len(candles) - width):
        neighbors = [j for j in range(i - width, i + width + 1) if j != i]
        for key, kind, result in (("h", "high", highs), ("l", "low", lows)):
            price = candles[i][key]
            if all(price > candles[j][key] if kind == "high" else price < candles[j][key] for j in neighbors):
                result.append(FibPivot(i, int(candles[i]["t"]), price, kind))
    return tuple(highs), tuple(lows)


def detect_latest_impulse(candles: Sequence[Mapping[str, float]], *, atr_period: int, pivot_width: int,
                          lookback: int, min_impulse_atr: float, min_impulse_bars: int) -> FibImpulse | None:
    if len(candles) <= atr_period:
        return None
    atr = _atr(candles, atr_period)
    highs, lows = find_confirmed_pivots(candles, pivot_width)
    pivots = sorted((p for p in highs + lows if p.index >= len(candles) - lookback), key=lambda p: (p.index, p.kind))
    candidates = []
    # Consecutive same-kind candidates form one leg; retain all starts for tie-breaking.
    previous_run: list[FibPivot] = []
    run: list[FibPivot] = []
    for end in pivots:
        if run and end.kind != run[-1].kind:
            previous_run, run = run, []
        for start in previous_run:
            if end.index - start.index < min_impulse_bars:
                continue
            move = end.price - start.price if end.kind == "high" else start.price - end.price
            a = float(atr[end.index])
            if not math.isfinite(a) or not math.isfinite(move) or a <= 0 or move <= 0 or move / a < min_impulse_atr:
                continue
            candidates.append(FibImpulse("long" if end.kind == "high" else "short", start.index, end.index,
                                         start.timestamp_ms, end.timestamp_ms, start.price, end.price,
                                         a, move, move / a, move / start.price))
        run.append(end)
    return max(candidates, key=lambda p: (p.end_timestamp_ms, p.move_atr, p.start_timestamp_ms), default=None)


def calculate_fib_prices(impulse: FibImpulse) -> dict[float, float]:
    return {r: impulse.end_price + (impulse.start_price - impulse.end_price) * r for r in RATIOS}


def build_fib_levels(impulse: FibImpulse, *, entry_atr: float, zone_atr: float,
                     entry_ratios: Sequence[float] = ()) -> dict[float, FibLevel]:
    half = entry_atr * zone_atr
    return {r: FibLevel(r, p, p - half, p + half) for r in sorted(set(RATIOS) | set(entry_ratios))
            for p in [impulse.end_price + (impulse.start_price - impulse.end_price) * r]}


def find_touched_fib_level(candle: Mapping[str, float], levels: Mapping[float, FibLevel],
                           entry_ratios: Sequence[float]) -> FibLevel | None:
    return max((levels[r] for r in entry_ratios if candle["l"] <= levels[r].zone_high and candle["h"] >= levels[r].zone_low),
               key=lambda level: level.ratio, default=None)


def calculate_confirmation(*, candles: Sequence[Mapping[str, float]], direction: Direction, fib_level: FibLevel,
                           trend_candles: Sequence[Mapping[str, float]], config: Any) -> FibConfirmation | None:
    h, l, c = _arrays(candles)
    _, _, tc = _arrays(trend_candles)
    macd, signal, hist = talib.MACD(c, fastperiod=config.fib_macd_fast, slowperiod=config.fib_macd_slow, signalperiod=config.fib_macd_signal)
    adx = talib.ADX(h, l, c, timeperiod=config.fib_adx_period)
    plus = talib.PLUS_DI(h, l, c, timeperiod=config.fib_adx_period)
    minus = talib.MINUS_DI(h, l, c, timeperiod=config.fib_adx_period)
    roc = talib.ROC(c, timeperiod=config.fib_roc_period) if config.fib_roc else np.zeros(len(c))
    fast = talib.EMA(tc, timeperiod=config.fib_trend_ema_fast)
    slow = talib.EMA(tc, timeperiod=config.fib_trend_ema_slow)
    if last_finite_index(macd, signal, hist, adx, plus, minus, roc, rows=3) != len(c) - 1 or last_finite_index(fast, slow) != len(tc) - 1:
        return None
    sign = 1 if direction == "long" else -1
    label = "bullish" if sign == 1 else "bearish"
    candle = candles[-1]
    rejection = (candle["l"] <= fib_level.zone_high and candle["h"] >= fib_level.zone_low
                 and sign * (candle["c"] - fib_level.price) > 0 and sign * (candle["c"] - candle["o"]) > 0)
    rotation = bool(sign * hist[-3] < sign * hist[-2] < sign * hist[-1])
    cross = bool(sign * (macd[-2] - signal[-2]) <= 0 < sign * (macd[-1] - signal[-1]))
    aligned = bool(sign * (plus[-1] - minus[-1]) > 0)
    rising = bool(adx[-1] > adx[-2])
    strength = (2 if adx[-1] >= config.fib_strong_adx else 1 if adx[-1] >= config.fib_min_adx else 0) if rising else 0
    momentum = bool(config.fib_roc and sign * roc[-1] > 0 and sign * roc[-1] > sign * roc[-2])
    htf = bool(sign * (fast[-1] - slow[-1]) > 0 and sign * (tc[-1] - slow[-1]) > 0)
    awards = ((rejection, 1, f"{label} rejection from fib zone"), (rotation, 1, f"MACD histogram rotating {label}"),
              (cross, 2, f"MACD {label} crossover"), (aligned, 1, "+DI above -DI" if sign == 1 else "-DI above +DI"),
              (strength > 0, strength, f"ADX {adx[-1]:.1f} rising"),
              (momentum, 1, "ROC positive and accelerating" if sign == 1 else "ROC negative and accelerating"),
              (htf, 2, "higher-timeframe EMA trend aligned"))
    return FibConfirmation(sum(points for enabled, points, _ in awards if enabled), 10 if config.fib_roc else 9,
                           tuple(reason for enabled, _, reason in awards if enabled), rejection, rotation, cross, aligned,
                           rising, float(adx[-1]), float(plus[-1]), float(minus[-1]), float(roc[-1]), momentum, htf)


def structure_invalidated(impulse: FibImpulse, close: float, entry_atr: float, invalidation_atr: float,
                          entry_ratios: Sequence[float]) -> bool:
    anchor = impulse.start_price if 0.786 in entry_ratios else calculate_fib_prices(impulse)[0.786]
    sign = 1 if impulse.direction == "long" else -1
    return sign * (close - anchor) < -entry_atr * invalidation_atr


def calculate_trade_levels(*, impulse: FibImpulse, entry_price: float, entry_atr: float, active_ratio: float,
                           fib_786: float, stop_atr_buffer: float) -> tuple[float, tuple[float, ...], tuple[float, ...]]:
    sign = 1 if impulse.direction == "long" else -1
    stop = (impulse.start_price if active_ratio == 0.786 else fib_786) - sign * entry_atr * stop_atr_buffer
    targets = tuple(impulse.end_price + sign * impulse.move_abs * r for r in (0, 0.272, 0.618))
    risk = sign * (entry_price - stop)
    if not all(math.isfinite(v) and v > 0 for v in (entry_price, stop, entry_atr, *targets)) or not math.isfinite(risk) or risk <= 0:
        raise ValueError("invalid stop geometry")
    if not all(sign * (b - a) > 0 for a, b in zip((entry_price, *targets), targets)):
        raise ValueError("stale entry or invalid target geometry")
    return stop, targets, tuple(abs(tp - entry_price) / risk for tp in targets)


class FibReversalStrategy(AutoStrategy):
    name = "fib_reversal"

    def __init__(self, config: object) -> None:
        self.config = config
        self._lock = asyncio.Lock()
        self._setups: dict[str, FibSetup] = {}
        self._traded: set[tuple[str, str, int, int]] = set()
        self._last_candle: dict[str, int] = {}

    def _log(self, coin: str, message: str) -> None:
        print(f"[FIB][{coin}][{self.config.trend_interval}->{self.config.entry_interval}] {message}")

    def _invalidate(self, setup: FibSetup, reason: str) -> None:
        if setup.state != FibSetupState.INVALIDATED:
            setup.state = FibSetupState.INVALIDATED
            setup.invalidation_reason = reason
            self._log(setup.coin, f"Invalidated: {reason}")

    async def evaluate(self, context: StrategyContext) -> StrategySignal | None:
        require_talib_available()
        if context.current_position is not None:
            return None
        async with self._lock:
            return self._evaluate(context)

    def _evaluate(self, context: StrategyContext) -> StrategySignal | None:
        cfg = self.config
        coin = context.coin.upper()
        def closed(raw: Sequence[Mapping[str, Any]], interval: str) -> list[dict[str, float]]:
            rows = normalize_candles(raw, drop_incomplete=False)
            # Pivots must always be confirmed by closed trend candles, even in live-entry mode.
            if cfg.use_last_closed_candle or interval == cfg.trend_interval:
                rows = [r for r in rows if r["t"] + INTERVAL_TO_MS[interval] <= context.now_ms]
            return rows
        entry = closed(context.entry_candles, cfg.entry_interval)
        trend = closed(context.trend_candles, cfg.trend_interval)
        if len(entry) < max(cfg.fib_atr_period + 1, cfg.fib_macd_slow + cfg.fib_macd_signal + 2,
                            2 * cfg.fib_adx_period + 2, cfg.fib_roc_period + 3 if cfg.fib_roc else 0) or len(trend) < cfg.fib_trend_ema_slow:
            return None
        timestamp = int(entry[-1]["t"])
        trend = [r for r in trend if r["t"] + INTERVAL_TO_MS[cfg.trend_interval] <= min(context.now_ms, timestamp + INTERVAL_TO_MS[cfg.entry_interval])]
        if len(trend) < cfg.fib_trend_ema_slow:
            return None
        if timestamp <= self._last_candle.get(coin, -1):
            return None
        atr_values = _atr(entry, cfg.fib_atr_period)
        if last_finite_index(atr_values) != len(entry) - 1 or atr_values[-1] <= 0:
            return None
        atr = float(atr_values[-1])
        impulse = detect_latest_impulse(trend, atr_period=cfg.fib_atr_period, pivot_width=cfg.fib_pivot_width,
                                        lookback=cfg.fib_impulse_lookback, min_impulse_atr=cfg.fib_min_impulse_atr,
                                        min_impulse_bars=cfg.fib_min_impulse_bars)
        self._last_candle[coin] = timestamp
        setup = self._setups.get(coin)
        if impulse is None or timestamp - impulse.end_timestamp_ms > cfg.fib_impulse_lookback * INTERVAL_TO_MS[cfg.trend_interval]:
            if setup:
                self._invalidate(setup, "impulse no longer recent")
            return None
        if setup is None or setup.impulse.impulse_id != impulse.impulse_id:
            if setup:
                self._invalidate(setup, "superseded impulse")
            setup = FibSetup(coin, FibSetupState.WAITING_FOR_RETRACE, impulse)
            self._setups[coin] = setup
            self._log(coin, f"{impulse.direction.upper()} impulse {impulse.start_price:.8f} -> {impulse.end_price:.8f} | {impulse.move_atr:.2f} ATR")
        previous_candle_ms = setup.last_processed_candle_ms
        setup.last_processed_candle_ms = timestamp
        identity = (coin, *impulse.impulse_id)
        if setup.traded or identity in self._traded or setup.state == FibSetupState.INVALIDATED:
            return None
        ratios = parse_entry_levels(cfg.fib_entry_levels)
        prices = calculate_fib_prices(impulse)
        price = entry[-1]["c"]
        # Check every unseen close so a missed scan cannot erase a structure break.
        unseen = [(row, float(atr_values[i])) for i, row in enumerate(entry)
                  if previous_candle_ms < row["t"] <= timestamp and (previous_candle_ms > 0 or row["t"] == timestamp)]
        if any(math.isfinite(a) and structure_invalidated(impulse, row["c"], a, cfg.fib_invalidation_atr, ratios)
               for row, a in unseen):
            self._invalidate(setup, "close beyond buffered structure")
            return None
        if setup.confirmation_deadline_ms is not None and timestamp > setup.confirmation_deadline_ms:
            self._invalidate(setup, "confirmation timeout")
            return None
        if setup.active_ratio is None:
            levels = build_fib_levels(impulse, entry_atr=atr, zone_atr=cfg.fib_zone_atr, entry_ratios=ratios)
            level = find_touched_fib_level(entry[-1], levels, ratios)
            if level is None:
                return None
            setup.active_ratio, setup.active_level_price = level.ratio, level.price
            setup.active_zone_low, setup.active_zone_high = level.zone_low, level.zone_high
            setup.entered_zone_candle_ms = timestamp
            setup.confirmation_deadline_ms = timestamp + cfg.fib_confirm_timeout * INTERVAL_TO_MS[cfg.entry_interval]
            setup.state = FibSetupState.IN_FIB_ZONE
            self._log(coin, f"Entered {level.ratio:.3f} zone @ {level.price:.8f} | {level.zone_low:.8f}..{level.zone_high:.8f}")
        level = FibLevel(setup.active_ratio, setup.active_level_price, setup.active_zone_low, setup.active_zone_high)
        if abs(price - level.price) > atr * cfg.fib_max_entry_distance_atr:
            self._invalidate(setup, "entry too far from fib level")
            return None
        sign = 1 if impulse.direction == "long" else -1
        if sign * (price - impulse.end_price) >= 0:
            self._invalidate(setup, "price reached prior impulse extreme")
            return None
        setup.state = FibSetupState.WAITING_FOR_CONFIRMATION
        confirmation = calculate_confirmation(candles=entry, direction=impulse.direction, fib_level=level, trend_candles=trend, config=cfg)
        if confirmation is None:
            return None
        signature = (confirmation.score, confirmation.reasons)
        if setup.last_confirmation != signature:
            setup.last_confirmation = signature
            self._log(coin, f"Confirmation {confirmation.score}/{confirmation.max_score}: {'; '.join(confirmation.reasons)}")
        if confirmation.score < cfg.fib_min_score:
            return None
        try:
            stop, targets, rr = calculate_trade_levels(impulse=impulse, entry_price=price, entry_atr=atr,
                                                       active_ratio=level.ratio, fib_786=prices[0.786], stop_atr_buffer=cfg.fib_stop_atr_buffer)
        except ValueError as exc:
            self._invalidate(setup, str(exc))
            return None
        if rr[1] < cfg.fib_min_rr:
            self._invalidate(setup, "RR to TP2 below minimum")
            return None
        fraction = confirmation.score / confirmation.max_score
        metadata = {
            "impulse_id": impulse.impulse_id, "impulse_direction": impulse.direction,
            "impulse_start_ms": impulse.start_timestamp_ms, "impulse_end_ms": impulse.end_timestamp_ms,
            "impulse_start_price": impulse.start_price, "impulse_end_price": impulse.end_price,
            "impulse_move_atr": impulse.move_atr, "impulse_move_pct": impulse.move_pct,
            "fib_ratio": level.ratio, "fib_price": level.price, "fib_zone_low": level.zone_low, "fib_zone_high": level.zone_high,
            **{f"fib_{round(r * 1000):03d}": p for r, p in prices.items()},
            "confirmation_score": confirmation.score, "confirmation_max_score": confirmation.max_score,
            "adx": confirmation.adx_value, "plus_di": confirmation.plus_di, "minus_di": confirmation.minus_di,
            "roc": confirmation.roc_value, "rr_tp1": rr[0], "rr_tp2": rr[1], "rr_tp3": rr[2],
            "trend_interval": context.config.trend_interval, "entry_interval": context.config.entry_interval,
            "tp_allocations": (40.0, 35.0, 25.0), "trade_quality": "strong" if fraction >= .8 else "good" if fraction >= .6 else "qualified",
        }
        setup.state, setup.traded = FibSetupState.SIGNALLED, True
        self._traded.add(identity)
        self._log(coin, f"{impulse.direction.upper()} signal entry={price:.8f} stop={stop:.8f} TP={targets} RR2={rr[1]:.2f}")
        reasons = (f"fib {level.ratio:.3f} {'support' if impulse.direction == 'long' else 'resistance'} zone touched", *confirmation.reasons, f"RR to TP2 {rr[1]:.2f}")
        return StrategySignal(self.name, context.coin, impulse.direction, timestamp, price, stop, targets, float(confirmation.score), reasons, metadata)

    def build_execution_plan(self, context: StrategyContext, signal: StrategySignal, size: float) -> StrategyExecutionPlan:
        return StrategyExecutionPlan("reversal", signal.coin, signal.direction, size, signal.entry_price,
                                     signal.stop_price, signal.take_profit_prices, dict(signal.metadata))
