import types
import unittest
import sys
from types import ModuleType
from unittest.mock import patch

hyperliquid_pkg = ModuleType("hyperliquid")
hyperliquid_info = ModuleType("hyperliquid.info")
hyperliquid_exchange = ModuleType("hyperliquid.exchange")
hyperliquid_utils = ModuleType("hyperliquid.utils")
hyperliquid_utils_constants = ModuleType("hyperliquid.utils.constants")


class _FakeInfo:
    pass


class _FakeExchange:
    pass


hyperliquid_info.Info = _FakeInfo
hyperliquid_exchange.Exchange = _FakeExchange
hyperliquid_utils.constants = hyperliquid_utils_constants
hyperliquid_pkg.info = hyperliquid_info
hyperliquid_pkg.exchange = hyperliquid_exchange
hyperliquid_pkg.utils = hyperliquid_utils
sys.modules.setdefault("hyperliquid", hyperliquid_pkg)
sys.modules.setdefault("hyperliquid.info", hyperliquid_info)
sys.modules.setdefault("hyperliquid.exchange", hyperliquid_exchange)
sys.modules.setdefault("hyperliquid.utils", hyperliquid_utils)
sys.modules.setdefault("hyperliquid.utils.constants", hyperliquid_utils_constants)

eth_account_module = ModuleType("eth_account")
eth_account_signers = ModuleType("eth_account.signers")
eth_account_signers_local = ModuleType("eth_account.signers.local")


class _FakeLocalAccount:
    address = "0x0"


class _FakeAccountAPI:
    @staticmethod
    def from_key(_: str) -> _FakeLocalAccount:
        return _FakeLocalAccount()


eth_account_module.Account = _FakeAccountAPI
eth_account_signers_local.LocalAccount = _FakeLocalAccount
sys.modules.setdefault("eth_account", eth_account_module)
sys.modules.setdefault("eth_account.signers", eth_account_signers)
sys.modules.setdefault("eth_account.signers.local", eth_account_signers_local)

dotenv_module = ModuleType("dotenv")
dotenv_module.load_dotenv = lambda *args, **kwargs: None
sys.modules.setdefault("dotenv", dotenv_module)

from hypertrader import build_arg_parser
from strategies.default import BollingerState, DefaultAutoStrategy, confirm_signal_with_bollinger, evaluate_default_decision
from strategies.orderflow_pullback import OrderflowPullbackStrategy
from strategies.registry import available_strategies, available_strategy_names, create_strategy, normalize_strategy_name
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
    @staticmethod
    def _state(interval: str, percent_b: float, basis_slope: float = 1.0) -> BollingerState:
        return BollingerState(
            interval=interval,
            basis=100.0,
            upper=110.0,
            lower=90.0,
            percent_b=percent_b,
            basis_slope=basis_slope,
            bandwidth=0.2,
            previous_bandwidth=0.18,
            bandwidth_expanding=True,
            latest_close=90.0 + (20.0 * percent_b),
        )

    def test_available_strategies_are_deterministic(self) -> None:
        self.assertEqual(available_strategies(), ("default", "orderflow_pullback", "reversal"))

    def test_available_strategy_names_include_aliases(self) -> None:
        self.assertEqual(
            available_strategy_names(),
            ("default", "legacy", "of_pullback", "orderflow", "orderflow_pullback", "reversal"),
        )

    def test_auto_parser_defaults_to_default_strategy(self) -> None:
        parser = build_arg_parser()
        args = parser.parse_args(["auto", "BTC", "--size", "1"])
        self.assertEqual(args.strategy, "default")

    def test_auto_parser_accepts_reversal_strategy(self) -> None:
        parser = build_arg_parser()
        args = parser.parse_args(["auto", "BTC", "--size", "1", "--strategy", "reversal"])
        self.assertEqual(args.strategy, "reversal")

    def test_auto_parser_accepts_orderflow_aliases(self) -> None:
        parser = build_arg_parser()
        args = parser.parse_args(["auto", "BTC", "--size", "1", "--strategy", "orderflow"])
        self.assertEqual(args.strategy, "orderflow")
        args = parser.parse_args(["auto", "BTC", "--size", "1", "--strategy", "of_pullback"])
        self.assertEqual(args.strategy, "of_pullback")

    def test_auto_parser_exposes_orderflow_defaults(self) -> None:
        parser = build_arg_parser()
        args = parser.parse_args(["auto", "BTC", "--size", "1", "--strategy", "orderflow_pullback"])
        self.assertEqual(args.of_max_active_books, 8)
        self.assertEqual(args.of_max_spread_bps, 3.0)
        self.assertEqual(args.of_entry_timeout_seconds, 2.0)
        self.assertTrue(args.of_flow_scratch)

    def test_registry_creates_expected_strategy_types(self) -> None:
        config = types.SimpleNamespace()
        self.assertIsInstance(create_strategy("default", config), DefaultAutoStrategy)
        self.assertIsInstance(create_strategy("reversal", config), ReversalAutoStrategy)
        self.assertIsInstance(create_strategy("orderflow_pullback", config), OrderflowPullbackStrategy)
        self.assertIsInstance(create_strategy("orderflow", config), OrderflowPullbackStrategy)
        self.assertIsInstance(create_strategy("of_pullback", config), OrderflowPullbackStrategy)

    def test_strategy_aliases_normalize_to_canonical_name(self) -> None:
        self.assertEqual(normalize_strategy_name("orderflow"), "orderflow_pullback")
        self.assertEqual(normalize_strategy_name("of_pullback"), "orderflow_pullback")

    def test_registry_rejects_unknown_strategy_name(self) -> None:
        with self.assertRaisesRegex(ValueError, "Available strategies: default, orderflow_pullback, reversal"):
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

    def test_bollinger_confirmation_accepts_long_above_middle_band(self) -> None:
        with patch(
            "strategies.default.compute_bollinger_state",
            side_effect=[self._state("5m", 0.62), self._state("1h", 0.55)],
        ):
            allowed, reason, _ = confirm_signal_with_bollinger(
                side="long",
                interval_to_closes={"5m": [1.0] * 25, "1h": [1.0] * 25},
                active_intervals=["5m", "1h"],
                scalp=False,
            )

        self.assertTrue(allowed)
        self.assertIn("result=PASS", reason)

    def test_bollinger_confirmation_rejects_long_below_middle_band(self) -> None:
        with patch(
            "strategies.default.compute_bollinger_state",
            side_effect=[self._state("5m", 0.38), self._state("1h", 0.55)],
        ):
            allowed, reason, _ = confirm_signal_with_bollinger(
                side="long",
                interval_to_closes={"5m": [1.0] * 25, "1h": [1.0] * 25},
                active_intervals=["5m", "1h"],
                scalp=False,
            )

        self.assertFalse(allowed)
        self.assertIn("long rejected: entry interval below middle band", reason)

    def test_bollinger_confirmation_accepts_short_below_middle_band(self) -> None:
        with patch(
            "strategies.default.compute_bollinger_state",
            side_effect=[self._state("5m", 0.33, basis_slope=-1.0), self._state("1h", 0.45, basis_slope=-1.0)],
        ):
            allowed, reason, _ = confirm_signal_with_bollinger(
                side="short",
                interval_to_closes={"5m": [1.0] * 25, "1h": [1.0] * 25},
                active_intervals=["5m", "1h"],
                scalp=False,
            )

        self.assertTrue(allowed)
        self.assertIn("result=PASS", reason)


if __name__ == "__main__":
    unittest.main()
