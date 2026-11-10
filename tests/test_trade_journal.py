import asyncio
import csv
import json
import logging
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from modes import auto_trader
from strategies.base import StrategyExecutionPlan
from utils.trade_journal import TradeJournal


class TradeJournalTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.info = SimpleNamespace(candles_snapshot=AsyncMock(return_value=[
            {"t": 1000, "c": "100"}, {"t": 2000, "c": "110"},
        ]))

    def journal(self, charts=False):
        return TradeJournal(self.temp.name, {"strategy_name": "default", "size": 2},
                            argv=["auto", "BTC", "--size", "2"], charts=charts)

    def result(self, **kwargs):
        return auto_trader.AutoManagedTradeResult(
            coin="xyz:TEST", direction="long", size=2, take_profit_pct=0.1,
            stop_loss_pct=0.05, started_ms=1000, closed_ms=2000, **kwargs,
        )

    def rows(self, journal):
        with journal.csv_path.open(newline="", encoding="utf-8") as stream:
            return list(csv.DictReader(stream))

    async def test_sessions_are_isolated_and_settings_survive_without_trades(self):
        first, second = self.journal(), self.journal()
        self.assertNotEqual(first.directory, second.directory)
        meta = json.loads((first.directory / "session.json").read_text())
        self.assertEqual(meta["argv"], ["auto", "BTC", "--size", "2"])
        self.assertEqual(meta["settings"]["size"], 2)
        self.assertEqual(self.rows(first), [])

    async def test_live_partial_exits_use_weighted_prices_and_append(self):
        journal = self.journal()
        fills = [
            {"dir": "Open Long", "px": "100", "sz": "2", "fee": "0.2"},
            {"dir": "Close Long", "px": "110", "sz": "0.5", "fee": "0.05"},
            {"dir": "Close Long", "px": "120", "sz": "1.5", "fee": "0.15"},
        ]
        await journal.record(self.result(), 34.6, fills, None, self.info)
        await journal.record(self.result(), 0, [], "pnl_unavailable: offline", self.info)
        rows = self.rows(journal)
        self.assertEqual(len(rows), 2)
        self.assertEqual(float(rows[0]["entry_price"]), 100)
        self.assertEqual(float(rows[0]["exit_price"]), 117.5)
        self.assertAlmostEqual(float(rows[0]["fees"]), 0.4)
        self.assertEqual(rows[1]["realized_pnl"], "")
        self.assertEqual(rows[1]["entry_price"], "")
        self.assertNotEqual(rows[0]["trade_id"], rows[1]["trade_id"])
        details = json.loads((journal.directory / rows[0]["details_file"]).read_text())
        self.assertEqual(details["fills"], fills)
        self.info.candles_snapshot.assert_not_awaited()

    async def test_simulated_ladder_csv_uses_all_exits_not_final_price(self):
        plan = StrategyExecutionPlan("bracket_entry", "SOL", "short", 2, 100, 110,
                                     (90, 80), metadata={"tp_allocations": (50, 50)})
        with patch.object(auto_trader, "get_all_mids", new_callable=AsyncMock,
                          side_effect=[{"SOL": p} for p in (100, 90, 80)]):
            result = await auto_trader.run_simulated_trade(object(), plan, logging.Logger("test"), 0)
        journal = self.journal()
        await journal.record(result, result.simulated_pnl, [], None, self.info)
        row = self.rows(journal)[0]
        self.assertEqual(float(row["exit_price"]), 85)
        self.assertEqual(float(row["realized_pnl"]), 30)
        self.assertEqual(row["exit_reason"], "take_profit")
        self.assertEqual(row["fees"], "")
        self.assertEqual(row["simulated"], "True")

    async def test_enabled_chart_is_valid_svg_and_uses_safe_filename(self):
        journal = self.journal(charts=True)
        await journal.record(self.result(log_context={"strategy": "A&B"}), 1, [], None, self.info)
        row = self.rows(journal)[0]
        path = journal.directory / row["chart_file"]
        self.assertEqual(path.parent, journal.directory)
        self.assertNotIn(":", path.name)
        root = ET.parse(path).getroot()
        self.assertEqual(root.tag, "{http://www.w3.org/2000/svg}svg")
        self.assertIn("A&amp;B", path.read_text())
        self.info.candles_snapshot.assert_awaited_once()

    async def test_chart_failure_or_cancellation_does_not_lose_closed_trade(self):
        for error, status in ((RuntimeError("offline"), "failed"),
                              (asyncio.CancelledError(), "interrupted")):
            with self.subTest(status=status):
                journal = self.journal(charts=True)
                self.info.candles_snapshot.side_effect = error
                if status == "interrupted":
                    with self.assertRaises(asyncio.CancelledError):
                        await journal.record(self.result(), 10, [], None, self.info)
                else:
                    await journal.record(self.result(), 10, [], None, self.info)
                rows = self.rows(journal)
                self.assertEqual(len(rows), 1)
                details = json.loads((journal.directory / rows[0]["details_file"]).read_text())
                self.assertEqual(details["chart_status"], status)

    async def test_neighboring_cycle_fills_are_excluded_from_pnl(self):
        info = SimpleNamespace(user_fills_by_time=AsyncMock(return_value=[
            {"coin": "BTC", "time": ts, "closedPnl": "10", "fee": "1"}
            for ts in (999, 1000, 1500, 2000, 2001)
        ]))
        pnl, fills, reason = await auto_trader.fetch_trade_cycle_net_pnl(info, "account", "BTC", 1000, 2000)
        self.assertEqual(pnl, 27)
        self.assertEqual(len(fills), 3)
        self.assertEqual(reason, "")

    async def test_cli_passes_arguments_and_defaults_to_auto_session(self):
        import hypertrader
        argv = ["auto", "BTC", "--size", "2", "--session-log-dir", self.temp.name, "--trade-charts"]
        with patch.object(auto_trader, "run_auto_trader", new_callable=AsyncMock) as run:
            await hypertrader.async_main(argv)
        kwargs = run.call_args.kwargs
        self.assertEqual(kwargs["session_argv"], argv)
        self.assertTrue(kwargs["trade_charts"])
        self.assertEqual(kwargs["session_cli_settings"]["size"], 2)
        self.assertIn("entry_retries", kwargs["session_cli_settings"])

    async def test_auto_run_records_one_closed_trade_and_no_open_trade_at_shutdown(self):
        import hypertrader
        from strategies.base import StrategySignal

        for prices, expected_rows in (((100, 110), 1), ((100,), 0)):
            with self.subTest(prices=prices), tempfile.TemporaryDirectory() as root:
                signal = StrategySignal("default", "SOL", "long", 1, 100, 90, (110,), 1, ())
                candidate = auto_trader.AutoScanCandidate("SOL", signal, SimpleNamespace(), None)
                plan = StrategyExecutionPlan("bracket_entry", "SOL", "long", 2, 100, 90, (110,))
                strategy = SimpleNamespace(name="default", start=AsyncMock(), shutdown=AsyncMock(),
                                           rank_signal=lambda signal: 1, build_execution_plan=lambda *args: plan)
                backfill = Mock(enabled=False, complete=True)
                prices_mock = AsyncMock(side_effect=[*({"SOL": p} for p in prices), asyncio.CancelledError()])
                snapshots = 0

                async def snapshot(**kwargs):
                    nonlocal snapshots
                    snapshots += 1
                    if snapshots > 3:
                        raise asyncio.CancelledError()
                    return auto_trader.AutoScanLoopSnapshot({"SOL": 100}, {}, 10000, 0)

                with (
                    patch.object(auto_trader, "require_talib_available"),
                    patch.object(auto_trader, "init_clients", new_callable=AsyncMock,
                                 return_value=("account", self.info, object())),
                    patch.object(auto_trader, "close_clients", new_callable=AsyncMock),
                    patch.object(auto_trader, "create_strategy", return_value=strategy),
                    patch.object(auto_trader, "AutoStartupBackfillState", return_value=backfill),
                    patch.object(auto_trader, "build_auto_scan_loop_snapshot", side_effect=snapshot),
                    patch.object(auto_trader, "scan_auto_trade_candidate", new_callable=AsyncMock, return_value=candidate),
                    patch.object(auto_trader, "get_all_mids", prices_mock),
                    patch.object(auto_trader, "_append_risk_event_log"),
                ):
                    argv = ["auto", "SOL", "--size", "2", "--dry-run", "--max-trades", "1",
                            "--scan-interval", "0.01", "--poll-interval", "0.001", "--session-log-dir", root]
                    if expected_rows:
                        await asyncio.wait_for(hypertrader.async_main(argv), timeout=2)
                    else:
                        with self.assertRaises(asyncio.CancelledError):
                            await asyncio.wait_for(hypertrader.async_main(argv), timeout=2)
                paths = list(Path(root).glob("*/closed_trades.csv"))
                self.assertEqual(len(paths), 1)
                with paths[0].open() as stream:
                    rows = list(csv.DictReader(stream))
                self.assertEqual(len(rows), expected_rows)
                if rows:
                    self.assertEqual(float(rows[0]["realized_pnl"]), 20)
                strategy.shutdown.assert_awaited_once()

    async def test_live_hidden_stop_waits_for_confirmed_close(self):
        from modes import position_management as management
        position = {"coin": "BTC", "szi": "2", "entryPx": "100"}
        exchange = SimpleNamespace(market_close=AsyncMock(return_value={"status": "ok"}))
        with (
            patch.object(management, "get_position_for_coin", new_callable=AsyncMock,
                         side_effect=[position, position, None]) as positions,
            patch.object(management, "get_all_mids", new_callable=AsyncMock, return_value={"BTC": "80"}),
            patch.object(management, "get_account_runtime_metrics", new_callable=AsyncMock, return_value={}),
            patch.object(management, "cancel_reduce_only_orders_for_coin", new_callable=AsyncMock),
            patch.object(management.asyncio, "sleep", new_callable=AsyncMock),
        ):
            await management.monitor_bracket_position(
                info=object(), exchange=exchange, account_address="account", coin="BTC", side="long",
                entry_px=100, take_profit_pct=0.1, tp_orders=[], tp_oids=[], stop_oid=None,
                managed_signed_size=2, poll_interval=0.01, reversal_pct=None, market_slippage=0.05,
                stop_loss_pct=0.1, stop_loss_trigger_px=90, take_profit_levels=1, tp_tif="Alo",
                tp_reversal_limit_exit=False, tp_reversal_stop_buffer_pct=None, hide_orders=True,
            )
        self.assertEqual(positions.await_count, 3)
        self.assertEqual(exchange.market_close.await_count, 2)
