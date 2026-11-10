import asyncio
import csv
import json
import logging
import tempfile
import unittest
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from modes import auto_trader, position_management
from strategies.base import StrategyExecutionPlan, StrategySignal, scale_take_profit_signal
from strategies.fib_reversal import FibReversalStrategy
from utils.session_state import SessionState
from utils.trade_journal import TradeJournal


class TargetScaleTests(unittest.TestCase):
    def test_scales_every_target_in_both_directions_and_preserves_stop_and_allocations(self):
        for side, targets, expected in (("long", (110, 120, 140), (105, 110, 120)),
                                         ("short", (90, 80, 60), (95, 90, 80))):
            signal = StrategySignal("fib_reversal", "BTC", side, 1, 100, 95 if side == "long" else 105,
                                    targets, 8, (), {"tp_allocations": (40, 35, 25), "take_profit_pct": .4})
            scaled = scale_take_profit_signal(signal, .5)
            self.assertEqual(scaled.take_profit_prices, expected)
            self.assertEqual(scaled.stop_price, signal.stop_price)
            self.assertEqual(scaled.metadata["tp_allocations"], (40, 35, 25))
            self.assertEqual(scaled.metadata["take_profit_pct"], .2)
            self.assertEqual(signal.metadata["take_profit_pct"], .4)
            self.assertIs(scale_take_profit_signal(signal, 1), signal)

    def test_invalid_multiplier_or_nonpositive_short_target_rejected(self):
        signal = StrategySignal("default", "BTC", "short", 1, 100, 110, (50,), 1, ())
        for scale in (0, -1, float("nan"), float("inf"), 2):
            with self.subTest(scale=scale), self.assertRaises(ValueError):
                scale_take_profit_signal(signal, scale)


class SessionRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

    def test_checkpoint_isolated_locked_and_can_be_reset(self):
        settings = {"dry_run": True, "size": 2}
        store = SessionState(self.temp.name, settings, "account")
        store.data = {"risk_session": asdict(auto_trader.AutoRiskSessionState(123, realized_pnl=7, peak_pnl=9)),
                      "coin_sessions": {"BTC": asdict(auto_trader.CoinTradeSessionState("BTC", cooldown_until_ms=999))}}
        store.save()
        with self.assertRaises(BlockingIOError):
            SessionState(self.temp.name, settings, "account")
        store.close()
        resumed = SessionState(self.temp.name, settings, "account")
        self.assertEqual(resumed.data["risk_session"]["realized_pnl"], 7)
        self.assertEqual(resumed.data["coin_sessions"]["BTC"]["cooldown_until_ms"], 999)
        resumed.close()
        for account, config in (("other", settings), ("account", {"dry_run": False, "size": 2})):
            separate = SessionState(self.temp.name, config, account)
            self.assertEqual(separate.data, {})
            separate.close()
        fresh = SessionState(self.temp.name, settings, "account", new_session=True)
        self.assertEqual(fresh.data, {})
        fresh.close()

    def test_failed_checkpoint_write_preserves_previous_state(self):
        store = SessionState(self.temp.name, {}, "account")
        self.addCleanup(store.close)
        store.data = {"completed_trades": 3}
        store.save()
        store.data["completed_trades"] = 4
        with patch("utils.session_state.os.replace", side_effect=OSError("disk error")):
            with self.assertRaises(OSError):
                store.save()
        self.assertEqual(json.loads(store.path.read_text())["completed_trades"], 3)
        self.assertEqual(list(Path(self.temp.name).glob(".checkpoint-*")), [])

    def test_fib_impulse_deduplication_survives_json_round_trip(self):
        strategy = FibReversalStrategy(SimpleNamespace())
        strategy._traded.add(("BTC", "long", 1000, 2000))
        resumed = FibReversalStrategy(SimpleNamespace())
        resumed.restore_session_state(json.loads(json.dumps(strategy.session_state())))
        self.assertEqual(resumed._traded, strategy._traded)

    async def test_simulation_resumes_partial_exit_and_completed_result_without_new_fill(self):
        plan = StrategyExecutionPlan("reversal", "SOL", "short", 10, 100, 110, (90, 80),
                                     {"tp_allocations": (50, 50)})
        state = {}
        with patch.object(auto_trader, "get_all_mids", new_callable=AsyncMock,
                          side_effect=[{"SOL": 100}, {"SOL": 90}, asyncio.CancelledError()]):
            with self.assertRaises(asyncio.CancelledError):
                await auto_trader.run_simulated_trade(None, plan, logging.Logger("test"), 0, state=state)
        state = json.loads(json.dumps(state))
        self.assertEqual(state["remaining_size"], 5)
        self.assertEqual(state["realized_pnl"], 50)
        with patch.object(auto_trader, "get_all_mids", new_callable=AsyncMock, return_value={"SOL": 80}) as mids:
            result = await auto_trader.run_simulated_trade(None, plan, logging.Logger("test"), 0, state=state)
            replay = await auto_trader.run_simulated_trade(None, plan, logging.Logger("test"), 0, state=state)
        self.assertEqual(result.simulated_pnl, 150)
        self.assertEqual(result, replay)
        mids.assert_awaited_once()

    async def test_journal_resume_appends_ids_and_deduplicates_replayed_completion(self):
        journal = TradeJournal(self.temp.name, {})
        result = auto_trader.AutoManagedTradeResult("BTC", "long", 1, .1, .05, 1000, 2000,
                                                    simulated_pnl=10, log_context={"entry_price": 100})
        await journal.record(result, 10, [], None, None, deduplicate=True)
        resumed = TradeJournal(self.temp.name, {}, resume_directory=journal.directory)
        await resumed.record(result, 10, [], None, None, deduplicate=True)
        result.started_ms = 3000
        result.closed_ms = 4000
        await resumed.record(result, 10, [], None, None, deduplicate=True)
        with resumed.csv_path.open() as stream:
            rows = list(csv.DictReader(stream))
        self.assertEqual([r["trade_id"] for r in rows], ["000001", "000002"])
        self.assertEqual(journal.session_id, resumed.session_id)

    async def test_cli_error_restart_restores_open_trade_journal_and_risk(self):
        import hypertrader
        signal = StrategySignal("default", "SOL", "long", 1, 100, 90, (120, 140), 1, ())
        candidate = auto_trader.AutoScanCandidate("SOL", signal, SimpleNamespace(), None)
        strategy = SimpleNamespace(name="default", start=AsyncMock(), shutdown=AsyncMock(), rank_signal=lambda s: 1,
                                   build_execution_plan=Mock(side_effect=lambda c, s, size: StrategyExecutionPlan(
                                       "bracket_entry", s.coin, s.direction, size, s.entry_price, s.stop_price,
                                       s.take_profit_prices, {"tp_allocations": (50, 50)})))
        partial = asyncio.Event()
        prices = iter((100, 110))

        async def mids(info):
            price = next(prices, None)
            if price is None:
                partial.set()
                await asyncio.Event().wait()
            return {"SOL": price}

        scans = 0

        async def snapshot(**kwargs):
            nonlocal scans
            scans += 1
            if scans > 1:
                await partial.wait()
                raise RuntimeError("simulated scan failure")
            return auto_trader.AutoScanLoopSnapshot({"SOL": 100}, {}, 10000, 0)

        argv = ["auto", "SOL", "--size", "10", "--dry-run", "--max-trades", "1",
                "--scan-interval", ".01", "--poll-interval", ".001", "--session-log-dir", self.temp.name,
                "--take-profit-scale", ".5"]
        with (
            patch.object(auto_trader, "require_talib_available"),
            patch.object(auto_trader, "init_clients", new_callable=AsyncMock, return_value=("account", object(), object())),
            patch.object(auto_trader, "close_clients", new_callable=AsyncMock),
            patch.object(auto_trader, "create_strategy", return_value=strategy),
            patch.object(auto_trader, "AutoStartupBackfillState", return_value=Mock(enabled=False, complete=True)),
            patch.object(auto_trader, "scan_auto_trade_candidate", new_callable=AsyncMock, return_value=candidate) as scan,
            patch.object(auto_trader, "_append_risk_event_log"),
        ):
            with (patch.object(auto_trader, "build_auto_scan_loop_snapshot", side_effect=snapshot),
                  patch.object(auto_trader, "get_all_mids", side_effect=mids)):
                with self.assertRaisesRegex(RuntimeError, "simulated scan failure"):
                    await asyncio.wait_for(hypertrader.async_main(argv), 3)
            checkpoint_path = next(Path(self.temp.name).glob(".active-*.json"))
            saved = json.loads(checkpoint_path.read_text())
            self.assertEqual(saved["pending_trades"]["SOL"]["state"]["remaining_size"], 5)
            journal_path = Path(saved["journal_directory"])
            scan.reset_mock()
            with (patch.object(auto_trader, "build_auto_scan_loop_snapshot", new_callable=AsyncMock,
                               return_value=auto_trader.AutoScanLoopSnapshot({"SOL": 120}, {}, 10000, 0)),
                  patch.object(auto_trader, "get_all_mids", new_callable=AsyncMock, return_value={"SOL": 120})):
                await asyncio.wait_for(hypertrader.async_main(argv), 3)
            strategy.build_execution_plan.assert_called_once()
        saved = json.loads(checkpoint_path.read_text())
        self.assertEqual(Path(saved["journal_directory"]), journal_path)
        self.assertTrue(saved["complete"])
        self.assertEqual(saved["completed_trades"], 1)
        self.assertEqual(saved["risk_session"]["realized_pnl"], 150)
        self.assertEqual(saved["coin_sessions"]["SOL"]["completed_cycles"], 1)
        self.assertGreater(saved["coin_sessions"]["SOL"]["post_trade_cooldown_until_ms"], 0)
        self.assertEqual(saved["pending_trades"], {})
        with (journal_path / "closed_trades.csv").open() as stream:
            rows = list(csv.DictReader(stream))
        self.assertEqual(len(rows), 1)
        self.assertEqual(float(rows[0]["realized_pnl"]), 150)

    async def test_live_resume_uses_saved_monitor_without_another_entry(self):
        kwargs = dict(coin="BTC", direction="long", size=2, take_profit_pct=.1, stop_loss_pct=.05,
                      stop_loss_trigger_px=95, take_profit_levels=3, use_trailing_tp=False,
                      trailing_tp_trigger_level=1, trailing_tp_profit_pct=.25, entry_retries=1,
                      entry_repost_interval=1, poll_interval=1, tp_reversal_pct=None, entry_tif="Alo",
                      tp_tif="Alo", market_fallback=False, market_slippage=.05, cancel_existing_tpsl=True,
                      tp_reversal_limit_exit=False, tp_reversal_stop_buffer_pct=None, use_testnet=True,
                      account_address="account", info=object(), exchange=object())
        recovery = {"monitor_kwargs": {"coin": "BTC"}, "monitor_state": {"trailing_tp_stop_px": 105}}
        with (patch.object(position_management, "monitor_bracket_position", new_callable=AsyncMock) as monitor,
              patch.object(position_management, "enter_position_with_reposting_limit", new_callable=AsyncMock) as enter,
              patch.object(position_management, "rebuild_bracket_orders", new_callable=AsyncMock) as rebuild):
            await position_management.run_bracket_entry(**kwargs, recovery_state=recovery)
        monitor.assert_awaited_once()
        self.assertIs(monitor.call_args.kwargs["recovery_state"], recovery)
        enter.assert_not_awaited()
        rebuild.assert_not_awaited()

    async def test_hidden_monitor_restores_trailing_stop_and_target_progress(self):
        position = {"coin": "BTC", "szi": "2", "entryPx": "100"}
        exchange = SimpleNamespace(market_close=AsyncMock(return_value={"status": "ok"}))
        recovery = {"monitor_state": {"trailing_tp_armed": True, "trailing_tp_stop_px": 105,
                                      "favorable_extreme": 110, "hidden_tp_placed_levels": [1]}}
        with (
            patch.object(position_management, "get_position_for_coin", new_callable=AsyncMock, side_effect=[position, None]),
            patch.object(position_management, "get_all_mids", new_callable=AsyncMock, return_value={"BTC": "104"}),
            patch.object(position_management, "get_account_runtime_metrics", new_callable=AsyncMock, return_value={}),
            patch.object(position_management, "cancel_reduce_only_orders_for_coin", new_callable=AsyncMock),
            patch.object(position_management.asyncio, "sleep", new_callable=AsyncMock),
        ):
            await position_management.monitor_bracket_position(
                info=object(), exchange=exchange, account_address="account", coin="BTC", side="long", entry_px=100,
                take_profit_pct=.1, tp_orders=[], tp_oids=[], stop_oid=None, managed_signed_size=2,
                poll_interval=.01, reversal_pct=None, market_slippage=.05, stop_loss_pct=.1, stop_loss_trigger_px=90,
                take_profit_levels=1, tp_tif="Alo", tp_reversal_limit_exit=False, tp_reversal_stop_buffer_pct=None,
                hide_orders=True, use_trailing_tp=True, recovery_state=recovery,
                checkpoint=lambda: json.dumps(recovery))
        exchange.market_close.assert_awaited_once()
        self.assertEqual(recovery["monitor_state"]["hidden_tp_placed_levels"], [1])
        self.assertGreaterEqual(recovery["monitor_state"]["trailing_tp_stop_px"], 105)
