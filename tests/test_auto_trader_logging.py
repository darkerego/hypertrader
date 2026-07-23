import io
import json
import logging
import sys
import types
import unittest
from types import ModuleType, SimpleNamespace

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

from modes.auto_trader import _build_auto_trade_log_context, log_auto_trade_event
from strategies.base import StrategyContext, StrategySignal


class AutoTraderLoggingTests(unittest.TestCase):
    def test_log_auto_trade_event_writes_json_record(self) -> None:
        stream = io.StringIO()
        logger = logging.getLogger("tests.auto_trader_logging")
        logger.handlers = []
        logger.setLevel(logging.INFO)
        logger.propagate = False
        handler = logging.StreamHandler(stream)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)

        log_auto_trade_event(
            logger,
            "auto_trade_launch",
            coin="btc",
            size=1.234567891,
            flags={"dry_run", "live"},
        )

        payload = json.loads(stream.getvalue().strip())
        self.assertEqual(payload["event"], "auto_trade_launch")
        self.assertEqual(payload["coin"], "BTC")
        self.assertEqual(payload["size"], 1.23456789)
        self.assertEqual(sorted(payload["flags"]), ["dry_run", "live"])

    def test_build_auto_trade_log_context_includes_reversal_fields(self) -> None:
        signal = StrategySignal(
            strategy="reversal",
            coin="BTC",
            direction="long",
            signal_candle_ms=1_700_000_000_000,
            entry_price=101.25,
            stop_price=99.5,
            take_profit_prices=(103.0, 105.0, 107.0),
            score=2.6,
            reasons=("divergence", "structure break", "retest confirmed"),
            metadata={"expected_rr": 2.2},
        )
        context = StrategyContext(
            coin="BTC",
            now_ms=1_700_000_050_000,
            config=SimpleNamespace(),
            market_metadata={"current_px": 101.5},
            trend_candles=(),
            entry_candles=(),
            current_position=None,
        )
        plan = SimpleNamespace(expected_entry=101.25)

        payload = _build_auto_trade_log_context(
            strategy_name="reversal",
            signal=signal,
            strategy_context=context,
            plan=plan,
            launch_size=0.75,
        )

        self.assertEqual(payload["strategy"], "reversal")
        self.assertEqual(payload["trade_quality"], "good")
        self.assertEqual(payload["entry_price"], 101.25)
        self.assertEqual(payload["signal_entry_price"], 101.25)
        self.assertEqual(payload["size"], 0.75)


if __name__ == "__main__":
    unittest.main()
