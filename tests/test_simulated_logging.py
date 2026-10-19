import asyncio
import io
import json
import logging
import unittest
from unittest.mock import AsyncMock, patch

from modes import auto_trader
from strategies.base import StrategyExecutionPlan, StrategySignal
from strategies.fib_reversal import FibReversalStrategy
from utils.simulated_logging import LoggedConsole


class SimulatedTradeTests(unittest.IsolatedAsyncioTestCase):
    def fib_plan(self, direction="long", allocations=(40, 35, 25)):
        sign = 1 if direction == "long" else -1
        signal = StrategySignal(
            "fib_reversal", "SOL", direction, 1, 100, 100 - sign * 10,
            tuple(100 + sign * distance for distance in (10, 20, 30)),
            7, (), {"tp_allocations": allocations},
        )
        return FibReversalStrategy(None).build_execution_plan(None, signal, 10)

    async def simulate(self, plan, prices, *, cancelled=False):
        stream = io.StringIO()
        logger = logging.Logger("simulation-test")
        logger.addHandler(logging.StreamHandler(stream))
        with (
            patch.object(auto_trader, "get_all_mids", new_callable=AsyncMock,
                         side_effect=[*({"SOL": p} for p in prices), asyncio.CancelledError()]),
            patch.object(auto_trader.asyncio, "sleep", new_callable=AsyncMock),
            patch.object(auto_trader, "run_bracket_entry", new_callable=AsyncMock) as live_entry,
        ):
            if cancelled:
                with self.assertRaises(asyncio.CancelledError):
                    await auto_trader.run_simulated_trade(object(), plan, logger, 1)
                result = None
            else:
                result = await auto_trader.run_simulated_trade(object(), plan, logger, 1)
        live_entry.assert_not_awaited()
        return result, [json.loads(line) for line in stream.getvalue().splitlines()]

    async def test_fib_ladder_realizes_each_allocation_once_in_both_directions(self):
        for direction, sign in (("long", 1), ("short", -1)):
            with self.subTest(direction=direction):
                result, records = await self.simulate(
                    self.fib_plan(direction),
                    [100 + sign * distance for distance in (0, 10, 10, 5, 10, 20, 30)],
                )
                fills = [r for r in records if r["event"] == "simulated_take_profit_filled"]
                self.assertEqual([r["level"] for r in fills], [1, 2, 3])
                self.assertEqual([r["exit_size"] for r in fills], [4, 3.5, 2.5])
                self.assertEqual([r["remaining_size"] for r in fills], [6, 2.5, 0])
                self.assertEqual([r["realized_pnl"] for r in fills], [40, 110, 185])
                marks = [r for r in records if r["event"] == "simulated_position_mark"]
                self.assertEqual([r["unrealized_pnl"] for r in marks], [0, 60, 60, 30, 60, 50])
                self.assertEqual(result.simulated_pnl, 185)
                self.assertEqual(result.log_context["realized_pnl"], 185)
                self.assertEqual(records[-1]["reason"], "take_profit")

    async def test_fib_stop_only_closes_remaining_size_and_keeps_realized_profit(self):
        for direction, sign in (("long", 1), ("short", -1)):
            for distances, expected, fill_count in (
                ((0, -10), -100, 0),
                ((0, 10, -10), -20, 1),
                ((0, 10, 20, -10), 85, 2),
            ):
                with self.subTest(direction=direction, distances=distances):
                    result, records = await self.simulate(
                        self.fib_plan(direction), [100 + sign * d for d in distances],
                    )
                    self.assertEqual(result.simulated_pnl, expected)
                    self.assertEqual(records[-1]["realized_pnl"], expected)
                    self.assertEqual(records[-1]["remaining_size"], 0)
                    self.assertEqual(records[-1]["reason"], "stop_loss")
                    self.assertEqual(sum(r["event"] == "simulated_take_profit_filled" for r in records),
                                     fill_count)

    async def test_fib_price_gap_fills_all_crossed_targets_at_observed_mid(self):
        for direction, price in (("long", 135), ("short", 65)):
            with self.subTest(direction=direction):
                result, records = await self.simulate(self.fib_plan(direction), [100, price])
                fills = [r for r in records if r["event"] == "simulated_take_profit_filled"]
                self.assertEqual([r["exit_price"] for r in fills], [price] * 3)
                self.assertEqual(sum(r["exit_size"] for r in fills), 10)
                self.assertEqual(result.simulated_pnl, 350)

    async def test_fib_shutdown_logs_partial_profit_and_remaining_exposure(self):
        _, records = await self.simulate(self.fib_plan(), [100, 110, 105], cancelled=True)
        self.assertEqual(records[-1]["event"], "simulated_position_open_at_shutdown")
        self.assertEqual(records[-1]["remaining_size"], 6)
        self.assertEqual(records[-1]["realized_pnl"], 40)
        self.assertEqual(records[-1]["unrealized_pnl"], 30)

    async def test_invalid_allocations_fail_before_simulating(self):
        for allocations in ((40, 35), (40, 35, 20), (100, 0, 0), (40, 35, float("nan"))):
            with self.subTest(allocations=allocations):
                with patch.object(auto_trader, "get_all_mids", new_callable=AsyncMock) as mids:
                    with self.assertRaisesRegex(ValueError, "allocations"):
                        await auto_trader.run_simulated_trade(
                            object(), self.fib_plan(allocations=allocations), logging.Logger("test"), 1,
                        )
                    mids.assert_not_awaited()

    async def test_plan_without_allocations_retains_final_target_exit(self):
        plan = StrategyExecutionPlan("bracket_entry", "SOL", "long", 10, 100, 90, (110, 120, 130))
        result, records = await self.simulate(plan, [100, 110, 120, 90])
        self.assertEqual(result.simulated_pnl, -100)
        self.assertFalse(any(r["event"] == "simulated_take_profit_filled" for r in records))

    async def test_long_and_short_profit_and_loss(self):
        for direction, exit_price, expected in [
            ("long", 111, 22), ("long", 89, -22),
            ("short", 89, 22), ("short", 111, -22),
        ]:
            with self.subTest(direction=direction, exit_price=exit_price):
                plan = StrategyExecutionPlan(
                    "bracket_entry", "SOL", direction, 2, 100,
                    90 if direction == "long" else 110,
                    (110 if direction == "long" else 90,),
                )
                stream = io.StringIO()
                logger = logging.Logger("simulation-test")
                logger.addHandler(logging.StreamHandler(stream))
                with (
                    patch.object(auto_trader, "get_all_mids", new_callable=AsyncMock,
                                 side_effect=[{"SOL": 100}, {"SOL": 102}, {"SOL": exit_price}]),
                    patch.object(auto_trader.asyncio, "sleep", new_callable=AsyncMock),
                    patch.object(auto_trader, "run_bracket_entry", new_callable=AsyncMock) as live_entry,
                ):
                    result = await auto_trader.run_simulated_trade(object(), plan, logger, 1)
                live_entry.assert_not_awaited()
                self.assertEqual(result.simulated_pnl, expected)
                records = [json.loads(line) for line in stream.getvalue().splitlines()]
                self.assertEqual(records[0]["event"], "simulated_position_entered")
                self.assertEqual(records[-1]["event"], "simulated_position_exited")
                self.assertEqual(records[-1]["realized_pnl"], expected)
                self.assertTrue(any(record.get("unrealized_pnl") for record in records))

    async def test_missing_price_does_not_create_false_exit(self):
        plan = StrategyExecutionPlan("bracket_entry", "SOL", "long", 2, 100, 90, (110,))
        logger = logging.Logger("simulation-test")
        with (
            patch.object(auto_trader, "get_all_mids", new_callable=AsyncMock,
                         side_effect=[{}, {"SOL": 100}, RuntimeError("offline"), {"SOL": 110}]),
            patch.object(auto_trader.asyncio, "sleep", new_callable=AsyncMock),
        ):
            result = await auto_trader.run_simulated_trade(object(), plan, logger, 1)
        self.assertEqual(result.simulated_pnl, 20)


class ConsoleLoggingTests(unittest.TestCase):
    def test_console_mirrors_lines_and_strips_color(self):
        console = io.StringIO()
        stream = LoggedConsole(console)
        with self.assertLogs("hypertrader.console", level="INFO") as logs:
            stream.write("\033[31m[ERROR] test\033[0m\n")
            stream.write("partial")
            stream.flush()
        self.assertIn("[ERROR] test", logs.output[0])
        self.assertNotIn("\033", logs.output[0])
        self.assertIn("partial", logs.output[1])
        self.assertIn("\033[31m", console.getvalue())

    def test_dry_events_use_runtime_log_without_live_trade_file(self):
        with patch.object(auto_trader.logging, "FileHandler") as handler:
            logger = auto_trader.get_auto_trades_logger(dry_run=True)
        self.assertEqual(logger.name, "hypertrader.simulated")
        self.assertTrue(logger.propagate)
        handler.assert_not_called()
