import types
import unittest
from unittest import mock

from strategies.reversal import (
    FractalPoint,
    FractalSeries,
    ReversalAutoStrategy,
    StructureBreak,
    classify_prior_trend,
    detect_structure_break,
    evaluate_retest,
    find_confirmed_fractals,
)


def _make_candle(ts_ms: int, o: float, h: float, l: float, c: float, v: float = 100.0) -> dict[str, float]:
    return {"t": float(ts_ms), "o": o, "h": h, "l": l, "c": c, "v": v}


class FractalTests(unittest.TestCase):
    def test_detects_valid_fractal_high_and_low(self) -> None:
        candles = [
            _make_candle(0, 10, 11, 9, 10),
            _make_candle(1, 10, 12, 8, 11),
            _make_candle(2, 11, 15, 7, 12),
            _make_candle(3, 12, 13, 8, 11),
            _make_candle(4, 11, 12, 9, 10),
            _make_candle(5, 10, 11, 6, 9),
            _make_candle(6, 9, 10, 7, 8),
            _make_candle(7, 8, 9, 8, 8.5),
        ]
        fractals = find_confirmed_fractals(candles)
        self.assertEqual([point.index for point in fractals.highs], [2])
        self.assertEqual([point.index for point in fractals.lows], [2, 5])

    def test_does_not_mark_equal_highs_as_fractal(self) -> None:
        candles = [
            _make_candle(0, 10, 11, 9, 10),
            _make_candle(1, 10, 12, 8, 11),
            _make_candle(2, 11, 15, 7, 12),
            _make_candle(3, 12, 15, 8, 11),
            _make_candle(4, 11, 12, 9, 10),
        ]
        fractals = find_confirmed_fractals(candles)
        self.assertEqual(fractals.highs, ())
        self.assertEqual([point.index for point in fractals.lows], [2])

    def test_insufficient_candles_returns_no_fractals(self) -> None:
        fractals = find_confirmed_fractals([_make_candle(0, 1, 2, 1, 1), _make_candle(1, 1, 2, 1, 1)])
        self.assertEqual(fractals.highs, ())
        self.assertEqual(fractals.lows, ())


class TrendClassificationTests(unittest.TestCase):
    @mock.patch("strategies.reversal._adx")
    @mock.patch("strategies.reversal._ema")
    def test_valid_uptrend(self, mock_ema: mock.Mock, mock_adx: mock.Mock) -> None:
        candles = [_make_candle(idx, 100 + idx, 101 + idx, 99 + idx, 100 + idx) for idx in range(60)]
        fractals = FractalSeries(
            highs=(FractalPoint(4, 4, 110.0, "high"), FractalPoint(7, 7, 120.0, "high")),
            lows=(FractalPoint(3, 3, 90.0, "low"), FractalPoint(6, 6, 95.0, "low")),
        )
        mock_ema.side_effect = [
            [150.0] * 60,
            [80.0 + idx for idx in range(60)],
        ]
        mock_adx.return_value = [25.0] * 60
        self.assertEqual(classify_prior_trend(candles, fractals, 20, 50, 14, 18.0, 0.0005), "up")

    @mock.patch("strategies.reversal._adx")
    @mock.patch("strategies.reversal._ema")
    def test_flat_slope_returns_no_trend(self, mock_ema: mock.Mock, mock_adx: mock.Mock) -> None:
        candles = [_make_candle(idx, 100, 101, 99, 100) for idx in range(60)]
        fractals = FractalSeries(
            highs=(FractalPoint(4, 4, 110.0, "high"), FractalPoint(7, 7, 120.0, "high")),
            lows=(FractalPoint(3, 3, 90.0, "low"), FractalPoint(6, 6, 95.0, "low")),
        )
        mock_ema.side_effect = [[101.0] * 60, [100.0] * 60]
        mock_adx.return_value = [25.0] * 60
        self.assertEqual(classify_prior_trend(candles, fractals, 20, 50, 14, 18.0, 0.0005), "none")


class StructureAndRetestTests(unittest.TestCase):
    @mock.patch("strategies.reversal._sar")
    @mock.patch("strategies.reversal._ema")
    @mock.patch("strategies.reversal._atr")
    def test_wick_above_fractal_without_close_above_does_not_confirm(self, mock_atr: mock.Mock, mock_ema: mock.Mock, mock_sar: mock.Mock) -> None:
        candles = [
            _make_candle(0, 10, 11, 9, 10),
            _make_candle(1, 10, 12, 9, 11),
            _make_candle(2, 11, 13, 10, 12),
            _make_candle(3, 12, 14, 11, 13),
            _make_candle(4, 13, 15, 12, 14),
            _make_candle(5, 14, 16, 13, 14.8),
        ]
        fractals = FractalSeries(highs=(FractalPoint(3, 3, 15.5, "high"),), lows=())
        mock_atr.return_value = [1.0] * len(candles)
        mock_ema.side_effect = [[13.0] * len(candles), [12.0] * len(candles)]
        mock_sar.return_value = [10.0] * len(candles)
        result = detect_structure_break(
            candles=candles,
            fractals=fractals,
            direction="long",
            reversal_extreme=12.0,
            atr_period=14,
            breakout_body_atr=0.30,
            max_breakout_range_atr=2.50,
            ema_fast_period=9,
            ema_confirm_period=21,
            sar_acceleration=0.02,
            sar_maximum=0.2,
        )
        self.assertIsNone(result)

    @mock.patch("strategies.reversal._sar")
    @mock.patch("strategies.reversal._atr")
    def test_breakout_candle_cannot_count_as_retest(self, mock_atr: mock.Mock, mock_sar: mock.Mock) -> None:
        candles = [
            _make_candle(0, 10, 11, 9, 10),
            _make_candle(1, 10, 12, 9, 11),
            _make_candle(2, 11, 13, 10, 12),
            _make_candle(3, 12, 14, 11, 13),
        ]
        structure_break = StructureBreak("long", 12.0, 13.0, 1.0, 3, 0.5)
        fractals = FractalSeries(highs=(), lows=())
        mock_atr.return_value = [1.0] * len(candles)
        mock_sar.return_value = [9.0] * len(candles)
        result = evaluate_retest(
            candles=candles,
            structure_break=structure_break,
            reversal_extreme=9.0,
            timeout_candles=2,
            retest_atr_tolerance=0.15,
            retest_min_price_pct=0.001,
            stop_atr_buffer=0.25,
            max_stop_atr=2.5,
            min_rr=1.8,
            tp1_r=1.0,
            tp2_r=2.0,
            tp3_r=3.0,
            sar_acceleration=0.02,
            sar_maximum=0.2,
            fractals=fractals,
            entry_interval_ms=60_000,
        )
        self.assertFalse(result.confirmed)
        self.assertEqual(result.reason, "awaiting retest")

    @mock.patch("strategies.reversal._find_opposing_structure")
    @mock.patch("strategies.reversal._sar")
    @mock.patch("strategies.reversal._atr")
    def test_valid_bullish_retest_confirms(self, mock_atr: mock.Mock, mock_sar: mock.Mock, mock_opposing: mock.Mock) -> None:
        candles = [
            _make_candle(0, 10, 11, 9, 10),
            _make_candle(60_000, 10, 12, 9, 11),
            _make_candle(120_000, 11, 13, 10, 12.5),
            _make_candle(180_000, 12.0, 12.8, 11.95, 12.4),
        ]
        structure_break = StructureBreak("long", 12.0, 12.5, 1.0, 120_000, 0.5)
        fractals = FractalSeries(highs=(), lows=())
        mock_atr.return_value = [1.0] * len(candles)
        mock_sar.return_value = [9.0] * len(candles)
        mock_opposing.return_value = None
        result = evaluate_retest(
            candles=candles,
            structure_break=structure_break,
            reversal_extreme=11.5,
            timeout_candles=4,
            retest_atr_tolerance=0.15,
            retest_min_price_pct=0.001,
            stop_atr_buffer=0.25,
            max_stop_atr=2.5,
            min_rr=1.8,
            tp1_r=1.0,
            tp2_r=2.0,
            tp3_r=3.0,
            sar_acceleration=0.02,
            sar_maximum=0.2,
            fractals=fractals,
            entry_interval_ms=60_000,
        )
        self.assertTrue(result.confirmed)
        self.assertAlmostEqual(result.expected_entry or 0.0, 12.4)
        self.assertEqual(len(result.take_profit_prices), 3)


class ReversalStateIsolationTests(unittest.TestCase):
    def test_per_coin_setup_state_is_isolated(self) -> None:
        config = types.SimpleNamespace(
            entry_interval="15m",
            trend_interval="1h",
            reversal_exit_on_sar_flip=True,
        )
        strategy = ReversalAutoStrategy(config)
        strategy._setups["BTC"] = types.SimpleNamespace(coin="BTC")
        strategy._setups["ETH"] = types.SimpleNamespace(coin="ETH")
        self.assertIn("BTC", strategy._setups)
        self.assertIn("ETH", strategy._setups)
        self.assertNotEqual(strategy._setups["BTC"].coin, strategy._setups["ETH"].coin)


if __name__ == "__main__":
    unittest.main()
