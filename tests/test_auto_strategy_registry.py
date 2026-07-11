import types
import unittest

from hypertrader import build_arg_parser
from strategies.default import DefaultAutoStrategy, evaluate_default_decision
from strategies.registry import available_strategies, create_strategy
from strategies.reversal import ReversalAutoStrategy


def _make_candle(ts_ms: int, o: float, h: float, l: float, c: float, v: float = 100.0) -> dict[str, float]:
    return {"t": float(ts_ms), "o": o, "h": h, "l": l, "c": c, "v": v}


def _uptrend_candles(count: int) -> list[dict[str, float]]:
    candles: list[dict[str, float]] = []
    px = 100.0
    for idx in range(count):
        candles.append(_make_candle(idx * 60_000, px, px + 2.0, px - 1.0, px + 1.25, 100.0 + idx))
        px += 0.75
    return candles


class AutoStrategyRegistryTests(unittest.TestCase):
    def test_available_strategies_are_deterministic(self) -> None:
        self.assertEqual(available_strategies(), ("default", "reversal"))

    def test_auto_parser_defaults_to_default_strategy(self) -> None:
        parser = build_arg_parser()
        args = parser.parse_args(["auto", "BTC", "--size", "1"])
        self.assertEqual(args.strategy, "default")

    def test_auto_parser_accepts_reversal_strategy(self) -> None:
        parser = build_arg_parser()
        args = parser.parse_args(["auto", "BTC", "--size", "1", "--strategy", "reversal"])
        self.assertEqual(args.strategy, "reversal")

    def test_registry_creates_expected_strategy_types(self) -> None:
        config = types.SimpleNamespace()
        self.assertIsInstance(create_strategy("default", config), DefaultAutoStrategy)
        self.assertIsInstance(create_strategy("reversal", config), ReversalAutoStrategy)

    def test_registry_rejects_unknown_strategy_name(self) -> None:
        with self.assertRaisesRegex(ValueError, "Available strategies: default, reversal"):
            create_strategy("defualt", object())

    def test_default_decision_is_deterministic(self) -> None:
        candles = _uptrend_candles(80)
        interval_candles = {"1h": candles, "15m": candles, "5m": candles}
        decision_a = evaluate_default_decision(
            interval_candles=interval_candles,
            current_px=160.0,
            intervals=["1h", "15m", "5m"],
            periods=80,
            min_agreement=0,
            adx_threshold=1.0,
            take_profit_pct_override=None,
            stop_loss_pct_override=None,
            min_take_profit_pct=0.002,
            max_take_profit_pct=0.03,
            macd_fast=12,
            macd_slow=26,
            macd_signal_period=9,
            sar_acceleration=0.02,
            sar_maximum=0.2,
            adx_timeperiod=14,
            bb_timeperiod=20,
            bb_dev=2.0,
            use_last_closed_candle=True,
            use_sar_stop_on_shortest_interval=False,
        )
        decision_b = evaluate_default_decision(
            interval_candles=interval_candles,
            current_px=160.0,
            intervals=["1h", "15m", "5m"],
            periods=80,
            min_agreement=0,
            adx_threshold=1.0,
            take_profit_pct_override=None,
            stop_loss_pct_override=None,
            min_take_profit_pct=0.002,
            max_take_profit_pct=0.03,
            macd_fast=12,
            macd_slow=26,
            macd_signal_period=9,
            sar_acceleration=0.02,
            sar_maximum=0.2,
            adx_timeperiod=14,
            bb_timeperiod=20,
            bb_dev=2.0,
            use_last_closed_candle=True,
            use_sar_stop_on_shortest_interval=False,
        )
        self.assertEqual(decision_a.direction, decision_b.direction)
        self.assertEqual(decision_a.reason, decision_b.reason)
        self.assertEqual(decision_a.take_profit_pct, decision_b.take_profit_pct)


if __name__ == "__main__":
    unittest.main()
