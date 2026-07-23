from __future__ import annotations

import asyncio
import types
import unittest

from strategies.orderflow_pullback import _CoinState, OrderflowPullbackStrategy


class _FakeInfo:
    def __init__(self) -> None:
        self.subscribe_calls: list[dict[str, object]] = []
        self.unsubscribe_calls: list[tuple[dict[str, object], int]] = []
        self._next_sub_id = 0

    async def subscribe(self, subscription: dict[str, object], callback: object) -> int:
        self._next_sub_id += 1
        self.subscribe_calls.append(dict(subscription))
        return self._next_sub_id

    async def unsubscribe(self, subscription: dict[str, object], subscription_id: int) -> bool:
        self.unsubscribe_calls.append((dict(subscription), subscription_id))
        return True


class OrderflowPullbackLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_promotion_rotation_respects_warmup_hold_time(self) -> None:
        strategy = OrderflowPullbackStrategy(
            types.SimpleNamespace(
                of_max_active_books=1,
                of_warmup_seconds=60.0,
                scan_interval=1.0,
            )
        )
        strategy._loop = asyncio.get_running_loop()
        strategy._info = _FakeInfo()
        strategy._started = True
        strategy._global_generation = 1

        from strategies import orderflow_pullback as orderflow_module

        original_monotonic_ms = orderflow_module.monotonic_ms
        now_ms = 100_000
        orderflow_module.monotonic_ms = lambda: now_ms
        try:
            first = _CoinState(coin="BTC")
            first.last_rank_score = 0.50
            strategy._states["BTC"] = first
            await strategy._ensure_promoted(first)
            self.assertTrue(first.promoted)

            challenger = _CoinState(coin="ETH")
            challenger.last_rank_score = 0.90
            strategy._states["ETH"] = challenger

            now_ms += 30_000
            await strategy._ensure_promoted(challenger)
            self.assertFalse(challenger.promoted)
            self.assertTrue(first.promoted)

            now_ms += 31_000
            await strategy._ensure_promoted(challenger)
            self.assertTrue(challenger.promoted)
            self.assertFalse(first.promoted)
        finally:
            orderflow_module.monotonic_ms = original_monotonic_ms

    async def test_demote_invalidates_old_generation_and_live_feed_state(self) -> None:
        strategy = OrderflowPullbackStrategy(types.SimpleNamespace())
        strategy._loop = asyncio.get_running_loop()
        strategy._info = _FakeInfo()
        strategy._started = True
        strategy._global_generation = 1

        state = _CoinState(coin="BTC")
        strategy._states["BTC"] = state

        await strategy._ensure_promoted(state)
        promoted_generation = state.generation
        self.assertTrue(state.promoted)
        self.assertEqual(len(strategy._info.subscribe_calls), 2)

        strategy._handle_trade_message(
            "BTC",
            promoted_generation,
            {
                "data": [
                    {
                        "coin": "BTC",
                        "side": "B",
                        "px": 100.0,
                        "sz": 1.25,
                        "time": 1_000,
                        "tid": 7,
                    }
                ]
            },
        )
        self.assertEqual(state.last_trade_exchange_ts_ms, 1_000)
        self.assertEqual(len(state.trades.recent_trades()), 1)

        await strategy._demote_coin(state, reason="test")
        self.assertFalse(state.promoted)
        self.assertGreater(state.generation, promoted_generation)
        self.assertEqual(len(strategy._info.unsubscribe_calls), 2)
        self.assertIsNone(state.book)
        self.assertEqual(state.last_trade_exchange_ts_ms, 0)
        self.assertEqual(len(state.trades.recent_trades()), 0)

        strategy._handle_trade_message(
            "BTC",
            promoted_generation,
            {
                "data": [
                    {
                        "coin": "BTC",
                        "side": "B",
                        "px": 101.0,
                        "sz": 2.0,
                        "time": 2_000,
                        "tid": 8,
                    }
                ]
            },
        )
        self.assertEqual(state.last_trade_exchange_ts_ms, 0)
        self.assertEqual(len(state.trades.recent_trades()), 0)

    async def test_freshness_window_respects_scan_interval_floor(self) -> None:
        strategy = OrderflowPullbackStrategy(types.SimpleNamespace(scan_interval=30.0, of_max_data_age_seconds=1.5))
        state = _CoinState(coin="BTC")

        now_ms = 100_000
        state.last_book_received_mono_ms = now_ms - 20_000
        state.last_trade_received_mono_ms = now_ms - 20_000

        from strategies import orderflow_pullback as orderflow_module

        original_monotonic_ms = orderflow_module.monotonic_ms
        orderflow_module.monotonic_ms = lambda: now_ms
        try:
            self.assertTrue(strategy._fresh(state))
            state.last_book_received_mono_ms = now_ms - 100_000
            self.assertFalse(strategy._fresh(state))
        finally:
            orderflow_module.monotonic_ms = original_monotonic_ms

    async def test_freshness_window_tolerates_scan_loop_overhead(self) -> None:
        strategy = OrderflowPullbackStrategy(types.SimpleNamespace(scan_interval=1.5, of_max_data_age_seconds=1.5))
        state = _CoinState(coin="BTC")

        now_ms = 100_000
        state.last_book_received_mono_ms = now_ms - 2_200
        state.last_trade_received_mono_ms = now_ms - 2_200

        from strategies import orderflow_pullback as orderflow_module

        original_monotonic_ms = orderflow_module.monotonic_ms
        orderflow_module.monotonic_ms = lambda: now_ms
        try:
            self.assertTrue(strategy._fresh(state))
            state.last_trade_received_mono_ms = now_ms - 5_000
            self.assertTrue(strategy._fresh(state))
            state.last_book_received_mono_ms = now_ms - 5_000
            self.assertFalse(strategy._fresh(state))
        finally:
            orderflow_module.monotonic_ms = original_monotonic_ms

    async def test_freshness_window_adapts_to_observed_book_cadence(self) -> None:
        strategy = OrderflowPullbackStrategy(types.SimpleNamespace(scan_interval=1.5, of_max_data_age_seconds=1.5))
        state = _CoinState(coin="BTC")
        assert state.book_gap_history_mono_ms is not None
        state.book_gap_history_mono_ms.extend((2_400, 2_600, 2_500, 2_700))

        now_ms = 100_000
        state.last_book_received_mono_ms = now_ms - 7_000

        from strategies import orderflow_pullback as orderflow_module

        original_monotonic_ms = orderflow_module.monotonic_ms
        orderflow_module.monotonic_ms = lambda: now_ms
        try:
            self.assertEqual(strategy._book_staleness_limit_ms(state), 10_200)
            self.assertTrue(strategy._fresh(state))
            state.last_book_received_mono_ms = now_ms - 11_000
            self.assertFalse(strategy._fresh(state))
        finally:
            orderflow_module.monotonic_ms = original_monotonic_ms

    async def test_reset_live_feed_state_clears_book_gap_history(self) -> None:
        strategy = OrderflowPullbackStrategy(types.SimpleNamespace())
        state = _CoinState(coin="BTC")
        assert state.book_gap_history_mono_ms is not None
        state.book_gap_history_mono_ms.extend((1_000, 2_000, 3_000))

        strategy._reset_live_feed_state(state)

        assert state.book_gap_history_mono_ms is not None
        self.assertEqual(list(state.book_gap_history_mono_ms), [])

    async def test_book_handler_accepts_recently_received_snapshot_even_if_exchange_time_lags(self) -> None:
        strategy = OrderflowPullbackStrategy(types.SimpleNamespace())
        strategy._loop = asyncio.get_running_loop()
        state = _CoinState(coin="BTC", promoted=True, generation=7)
        strategy._states["BTC"] = state

        strategy._handle_book_message(
            "BTC",
            7,
            {
                "data": {
                    "coin": "BTC",
                    "time": 1_000,
                    "levels": [
                        [{"px": "100.0", "sz": "2.0", "n": 1}],
                        [{"px": "100.5", "sz": "3.0", "n": 1}],
                    ],
                }
            },
        )

        self.assertIsNotNone(state.book)
        assert state.book is not None
        self.assertEqual(state.book.exchange_ts_ms, 1_000)

    async def test_book_handler_tracks_observed_book_gap_history(self) -> None:
        strategy = OrderflowPullbackStrategy(types.SimpleNamespace())
        strategy._loop = asyncio.get_running_loop()
        state = _CoinState(coin="BTC", promoted=True, generation=7)
        strategy._states["BTC"] = state

        from strategies import orderflow_pullback as orderflow_module

        original_monotonic_ms = orderflow_module.monotonic_ms
        received_times = iter((10_000, 12_600))
        orderflow_module.monotonic_ms = lambda: next(received_times)
        try:
            strategy._handle_book_message(
                "BTC",
                7,
                {
                    "data": {
                        "coin": "BTC",
                        "time": 1_000,
                        "levels": [
                            [{"px": "100.0", "sz": "2.0", "n": 1}],
                            [{"px": "100.5", "sz": "3.0", "n": 1}],
                        ],
                    }
                },
            )
            strategy._handle_book_message(
                "BTC",
                7,
                {
                    "data": {
                        "coin": "BTC",
                        "time": 2_000,
                        "levels": [
                            [{"px": "100.1", "sz": "2.0", "n": 1}],
                            [{"px": "100.6", "sz": "3.0", "n": 1}],
                        ],
                    }
                },
            )
        finally:
            orderflow_module.monotonic_ms = original_monotonic_ms

    async def test_stale_promoted_feed_is_resubscribed(self) -> None:
        strategy = OrderflowPullbackStrategy(types.SimpleNamespace(scan_interval=1.0, of_max_data_age_seconds=1.5))
        strategy._loop = asyncio.get_running_loop()
        strategy._info = _FakeInfo()
        strategy._started = True
        strategy._global_generation = 3

        state = _CoinState(
            coin="BTC",
            promoted=True,
            generation=3,
            book_sub_id=11,
            trade_sub_id=12,
            promoted_at_mono_ms=10_000,
            warmup_started_mono_ms=10_000,
        )
        state.last_book_received_mono_ms = 10_000
        state.last_trade_received_mono_ms = 10_200
        strategy._states["BTC"] = state

        from strategies import orderflow_pullback as orderflow_module

        original_monotonic_ms = orderflow_module.monotonic_ms
        now_ms = 20_000
        orderflow_module.monotonic_ms = lambda: now_ms
        try:
            await strategy._recover_stale_feed(state)
        finally:
            orderflow_module.monotonic_ms = original_monotonic_ms

        self.assertEqual(
            strategy._info.unsubscribe_calls,
            [
                ({"type": "l2Book", "coin": "BTC"}, 11),
                ({"type": "trades", "coin": "BTC"}, 12),
            ],
        )
        self.assertEqual(
            strategy._info.subscribe_calls,
            [
                {"type": "l2Book", "coin": "BTC"},
                {"type": "trades", "coin": "BTC"},
            ],
        )
        self.assertTrue(state.promoted)
        self.assertEqual(state.recovery_count, 1)
        self.assertEqual(state.last_recovery_mono_ms, 20_000)
        self.assertEqual(state.promoted_at_mono_ms, 20_000)
        self.assertEqual(state.warmup_started_mono_ms, 20_000)
        self.assertEqual(state.book_sub_id, 1)
        self.assertEqual(state.trade_sub_id, 2)
        self.assertEqual(state.last_book_received_mono_ms, 0)
        self.assertEqual(state.last_trade_received_mono_ms, 0)


if __name__ == "__main__":
    unittest.main()
