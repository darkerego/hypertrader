import math
import unittest

from strategies.orderflow_metrics import (
    RollingTradeStore,
    compute_executable_depth_ratio,
    compute_microprice,
    compute_trade_window_metrics,
    estimate_round_trip_cost_bps,
    executable_depth_notional,
    parse_l2_book_snapshot,
    parse_trade_print,
    rolling_vwap,
    simple_atr,
    trend_efficiency,
    weighted_book_imbalance,
)
from strategies.orderflow_models import BookLevel


def _book_message() -> dict[str, object]:
    return {
        "data": {
            "coin": "BTC",
            "time": 1_000,
            "levels": [
                [
                    {"px": "100.0", "sz": "10", "n": 1},
                    {"px": "99.98", "sz": "8", "n": 1},
                    {"px": "99.96", "sz": "6", "n": 1},
                    {"px": "99.94", "sz": "4", "n": 1},
                    {"px": "99.92", "sz": "2", "n": 1},
                ],
                [
                    {"px": "100.02", "sz": "5", "n": 1},
                    {"px": "100.04", "sz": "6", "n": 1},
                    {"px": "100.06", "sz": "7", "n": 1},
                    {"px": "100.08", "sz": "8", "n": 1},
                    {"px": "100.10", "sz": "9", "n": 1},
                ],
            ],
        }
    }


class OrderflowMetricsTests(unittest.TestCase):
    def test_trade_side_b_is_aggressive_buy(self) -> None:
        trade = parse_trade_print(
            {"coin": "BTC", "side": "B", "px": "100", "sz": "2", "time": 1_000, "tid": 1},
            received_monotonic_ms=10,
        )
        self.assertIsNotNone(trade)
        metrics = compute_trade_window_metrics([trade], window_seconds=2, now_exchange_ts_ms=1_000)  # type: ignore[list-item]
        self.assertEqual(metrics.buy_volume, 200.0)
        self.assertEqual(metrics.sell_volume, 0.0)

    def test_trade_side_a_is_aggressive_sell(self) -> None:
        trade = parse_trade_print(
            {"coin": "BTC", "side": "A", "px": "100", "sz": "2", "time": 1_000, "tid": 1},
            received_monotonic_ms=10,
        )
        self.assertIsNotNone(trade)
        metrics = compute_trade_window_metrics([trade], window_seconds=2, now_exchange_ts_ms=1_000)  # type: ignore[list-item]
        self.assertEqual(metrics.buy_volume, 0.0)
        self.assertEqual(metrics.sell_volume, 200.0)

    def test_duplicate_trades_are_ignored(self) -> None:
        store = RollingTradeStore()
        trade = parse_trade_print(
            {"coin": "BTC", "side": "B", "px": "100", "sz": "1", "time": 1_000, "tid": 7},
            received_monotonic_ms=10,
        )
        self.assertTrue(store.add_trade(trade))  # type: ignore[arg-type]
        self.assertFalse(store.add_trade(trade))  # type: ignore[arg-type]

    def test_rolling_windows_prune_by_exchange_timestamp(self) -> None:
        store = RollingTradeStore(max_window_seconds=60)
        for ts_ms in (1, 30_000, 61_000):
            trade = parse_trade_print(
                {"coin": "BTC", "side": "B", "px": "100", "sz": "1", "time": ts_ms, "tid": ts_ms},
                received_monotonic_ms=10,
            )
            self.assertTrue(store.add_trade(trade))  # type: ignore[arg-type]
        self.assertEqual(len(store.recent_trades()), 2)
        self.assertEqual([trade.exchange_ts_ms for trade in store.recent_trades()], [30_000, 61_000])

    def test_out_of_order_trade_is_conservatively_ignored(self) -> None:
        store = RollingTradeStore()
        newer = parse_trade_print(
            {"coin": "BTC", "side": "B", "px": "100", "sz": "1", "time": 2_000, "tid": 2},
            received_monotonic_ms=10,
        )
        older = parse_trade_print(
            {"coin": "BTC", "side": "B", "px": "100", "sz": "1", "time": 1_000, "tid": 1},
            received_monotonic_ms=10,
        )
        self.assertTrue(store.add_trade(newer))  # type: ignore[arg-type]
        self.assertFalse(store.add_trade(older))  # type: ignore[arg-type]

    def test_weighted_book_imbalance_positive_zero_negative(self) -> None:
        positive = weighted_book_imbalance(
            [BookLevel(100.0, 5.0), BookLevel(99.0, 4.0)],
            [BookLevel(101.0, 2.0), BookLevel(102.0, 2.0)],
        )[2]
        neutral = weighted_book_imbalance(
            [BookLevel(100.0, 2.0)],
            [BookLevel(101.0, 2.0)],
        )[2]
        negative = weighted_book_imbalance(
            [BookLevel(100.0, 2.0), BookLevel(99.0, 2.0)],
            [BookLevel(101.0, 5.0), BookLevel(102.0, 4.0)],
        )[2]
        self.assertGreater(positive, 0.0)
        self.assertAlmostEqual(neutral, 0.0)
        self.assertLess(negative, 0.0)

    def test_microprice_bias_signs_match_top_of_book_sizes(self) -> None:
        positive_book = parse_l2_book_snapshot(_book_message(), received_monotonic_ms=100, max_data_age_ms=5_000, now_exchange_ts_ms=1_000)
        self.assertIsNotNone(positive_book)
        microprice, micro_bias = compute_microprice(positive_book)  # type: ignore[arg-type]
        self.assertGreater(microprice, positive_book.mid)  # type: ignore[union-attr]
        self.assertGreater(micro_bias, 0.0)

        message = _book_message()
        message["data"]["levels"][0][0]["sz"] = "3"  # type: ignore[index]
        message["data"]["levels"][1][0]["sz"] = "12"  # type: ignore[index]
        negative_book = parse_l2_book_snapshot(message, received_monotonic_ms=100, max_data_age_ms=5_000, now_exchange_ts_ms=1_000)
        _, negative_bias = compute_microprice(negative_book)  # type: ignore[arg-type]
        self.assertLess(negative_bias, 0.0)

    def test_executable_depth_uses_only_levels_inside_depth_band(self) -> None:
        book = parse_l2_book_snapshot(_book_message(), received_monotonic_ms=100, max_data_age_ms=5_000, now_exchange_ts_ms=1_000)
        bid_depth, ask_depth = executable_depth_notional(book, depth_bps=1.0)  # type: ignore[arg-type]
        self.assertAlmostEqual(bid_depth, 100.0 * 10.0)
        self.assertAlmostEqual(ask_depth, 100.02 * 5.0)
        self.assertGreater(compute_executable_depth_ratio(bid_depth, ask_depth, 50.0), 0.0)

    def test_crossed_one_sided_zero_size_and_stale_books_are_rejected(self) -> None:
        self.assertIsNone(parse_l2_book_snapshot({"data": {"coin": "BTC", "time": 1_000, "levels": [[], []]}}, received_monotonic_ms=10))
        self.assertIsNone(parse_l2_book_snapshot({"data": {"coin": "BTC", "time": 1_000, "levels": [[{"px": "100", "sz": "0"}], [{"px": "101", "sz": "1"}]]}}, received_monotonic_ms=10))
        self.assertIsNone(parse_l2_book_snapshot({"data": {"coin": "BTC", "time": 1_000, "levels": [[{"px": "101", "sz": "1"}], [{"px": "100", "sz": "1"}]]}}, received_monotonic_ms=10))
        self.assertIsNone(parse_l2_book_snapshot(_book_message(), received_monotonic_ms=10, max_data_age_ms=10, now_exchange_ts_ms=2_000))

    def test_spread_bps_calculation(self) -> None:
        book = parse_l2_book_snapshot(_book_message(), received_monotonic_ms=100, max_data_age_ms=5_000, now_exchange_ts_ms=1_000)
        self.assertIsNotNone(book)
        self.assertAlmostEqual(book.spread_bps, (0.02 / 100.01) * 10_000.0, places=6)  # type: ignore[union-attr]

    def test_trend_efficiency_vwap_atr_and_cost_estimate(self) -> None:
        self.assertEqual(trend_efficiency([1.0, 1.0, 1.0]), 0.0)
        self.assertAlmostEqual(trend_efficiency([1.0, 2.0, 3.0]), 1.0)
        self.assertLess(trend_efficiency([1.0, 2.0, 1.5, 2.5]), 1.0)

        candles = [
            {"h": 10.0, "l": 8.0, "c": 9.0, "v": 2.0},
            {"h": 11.0, "l": 9.0, "c": 10.0, "v": 3.0},
        ]
        self.assertAlmostEqual(rolling_vwap(candles), ((9.0 * 2.0) + (10.0 * 3.0)) / 5.0)

        atr_candles = []
        close = 10.0
        for idx in range(15):
            atr_candles.append({"h": close + 1.0, "l": close - 1.0, "c": close})
            close += 0.5
        atr_value = simple_atr(atr_candles, period=14)
        self.assertTrue(atr_value is not None and atr_value > 0.0)

        cost = estimate_round_trip_cost_bps(
            spread_bps=1.5,
            maker_fee_bps=0.5,
            taker_fee_bps=3.5,
            slippage_bps=0.25,
            entry_style="maker",
            exit_style="taker",
        )
        self.assertTrue(math.isclose(cost.estimated_round_trip_cost_bps, 0.5 + 3.5 + 1.5 + 0.25))


if __name__ == "__main__":
    unittest.main()
