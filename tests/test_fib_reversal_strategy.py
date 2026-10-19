import asyncio
import json
import math
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from strategies import fib_reversal as fib
from strategies.base import StrategyContext


def config(**changes):
    return SimpleNamespace(**{**fib.FIB_DEFAULTS, 'trend_interval': '1h', 'entry_interval': '15m',
                              'use_last_closed_candle': True, **changes})


def candle(i, price, *, step=900_000):
    return dict(t=i * step, o=price, h=price + .2, l=price - .2, c=price, v=100.)


def impulse(direction='long'):
    a, b = (100., 120.) if direction == 'long' else (120., 100.)
    return fib.FibImpulse(direction, 60, 70, 60 * 3600000, 70 * 3600000, a, b, 2., 20., 10., 20 / a)


def trend(direction='long'):
    prices = [110.] * 60 + [105, 102, 100.2, 103, 106, 109, 112, 116, 119.8, 117, 114, 113, 112]
    if direction == 'short':
        prices = [220 - p for p in prices]
    return [candle(i, p, step=3600000) for i, p in enumerate(prices)]


class FibMathTests(unittest.TestCase):
    def test_normalization(self):
        rows = [candle(2, 100), candle(0, 100), candle(1, 100), candle(1, 101),
                dict(candle(3, 100), c=float('nan')), dict(candle(4, 100), o=0),
                {'t': 'bad'}, dict(candle(5, 100), h=90)]
        original = [dict(r) for r in rows]
        result = fib.normalize_candles(rows)
        self.assertEqual([r['t'] for r in result], [0, 900000])
        self.assertEqual(result[-1]['c'], 101)
        self.assertEqual(rows, original)
        self.assertEqual(fib.normalize_candles([candle(1, 100)]), [])

    def test_finite_rows(self):
        self.assertIsNone(fib.last_finite_index([math.nan] * 3))
        self.assertEqual(fib.last_finite_index([math.nan, 1, 2, math.nan]), 2)
        self.assertIsNone(fib.last_finite_index([math.nan, 1, 2], rows=3))

    def test_pivots_and_edges(self):
        rows = [candle(i, p) for i, p in enumerate([5, 4, 2, 4, 5, 7, 5, 4, 1])]
        highs, lows = fib.find_confirmed_pivots(rows)
        self.assertEqual([p.index for p in highs], [5])
        self.assertEqual([p.index for p in lows], [2])
        self.assertNotIn(8, [p.index for p in lows])
        self.assertEqual(fib.find_confirmed_pivots(rows[:6])[0], ())
        self.assertEqual(fib.find_confirmed_pivots(rows[:7])[0], ())
        self.assertEqual(fib.find_confirmed_pivots(rows[:8])[0][0].index, 5)
        self.assertTrue(fib.find_confirmed_pivots(rows, 1)[0])

    def test_fibonacci_both_directions(self):
        for direction, expected in [('long', [115.28,112.36,110,107.64,104.28]), ('short',[104.72,107.64,110,112.36,115.72])]:
            prices = fib.calculate_fib_prices(impulse(direction))
            for r, value in zip(fib.RATIOS, expected):
                self.assertAlmostEqual(prices[r], value)

    def test_zones_and_custom_ratios(self):
        levels = fib.build_fib_levels(impulse(), entry_atr=2, zone_atr=.15, entry_ratios=(.7,))
        self.assertIn(.786, levels)
        self.assertIn(.7, levels)
        self.assertEqual(fib.find_touched_fib_level(dict(l=107.8,h=108), levels, (.618,)).ratio, .618)
        self.assertIsNone(fib.find_touched_fib_level(dict(l=108,h=109), levels, (.618,)))
        self.assertEqual(fib.find_touched_fib_level(dict(l=107,h=113), levels, (.382,.5,.618)).ratio, .618)
        self.assertEqual(fib.parse_entry_levels(' .618,0.5 .382,0.5 '), (.382,.5,.618))

    def test_stops_targets_rr(self):
        for direction in ('long','short'):
            imp = impulse(direction)
            prices = fib.calculate_fib_prices(imp)
            entry = prices[.618]
            stop, tp, rr = fib.calculate_trade_levels(impulse=imp, entry_price=entry, entry_atr=2,
                active_ratio=.618, fib_786=prices[.786], stop_atr_buffer=.25)
            sign = 1 if direction == 'long' else -1
            self.assertGreater(sign * (entry-stop),0)
            self.assertEqual(tp, (120,125.44,132.36) if direction=='long' else (100,94.56,87.64))
            for target, value in zip(tp,rr):
                self.assertAlmostEqual(value,abs(target-entry)/abs(entry-stop))
            stop786, _, _ = fib.calculate_trade_levels(impulse=imp, entry_price=prices[.786], entry_atr=2,
                active_ratio=.786, fib_786=prices[.786], stop_atr_buffer=.25)
            self.assertAlmostEqual(stop786,imp.start_price-sign*.5)
            for bad in (stop-sign, imp.end_price, math.nan):
                with self.assertRaises(ValueError):
                    fib.calculate_trade_levels(impulse=imp, entry_price=bad, entry_atr=2,
                        active_ratio=.618, fib_786=prices[.786], stop_atr_buffer=.25)

    def test_invalidation(self):
        for direction in ('long','short'):
            imp=impulse(direction); sign=1 if direction=='long' else -1
            anchor=fib.calculate_fib_prices(imp)[.786]
            self.assertTrue(fib.structure_invalidated(imp, anchor-sign*.21, 2,.1,(.618,)))
            self.assertFalse(fib.structure_invalidated(imp, anchor-sign*.19, 2,.1,(.618,)))
            self.assertFalse(fib.structure_invalidated(imp, anchor-sign*.21, 2,.1,(.618,.786)))
            self.assertTrue(fib.structure_invalidated(imp, imp.start_price-sign*.21, 2,.1,(.786,)))

    def test_dependency_error(self):
        with patch.object(fib,'talib',None), self.assertRaisesRegex(RuntimeError,'pip install numpy TA-Lib'):
            fib.require_talib_available()


@unittest.skipIf(fib.talib is None, 'TA-Lib unavailable')
class FibIndicatorTests(unittest.TestCase):
    def detect(self, rows, **changes):
        opts=dict(atr_period=14,pivot_width=2,lookback=120,min_impulse_atr=2,min_impulse_bars=5)
        return fib.detect_latest_impulse(rows, **{**opts,**changes})

    def test_impulses(self):
        for direction in ('long','short'):
            result=self.detect(trend(direction))
            self.assertEqual(result.direction,direction)
            self.assertGreaterEqual(result.move_atr,2)
            self.assertEqual(result.impulse_id,self.detect(trend(direction)).impulse_id)
            self.assertIsNone(self.detect(trend(direction),min_impulse_atr=1000))
            self.assertIsNone(self.detect(trend(direction),min_impulse_bars=20))
            self.assertIsNone(self.detect(trend(direction),lookback=4))
            self.assertIsNone(self.detect(trend(direction)[:70]))  # high still lacks two right candles
        rows=trend()+[candle(i,p,step=3600000) for i,p in enumerate([111,113,116,119,122,125,123,121],start=73)]
        self.assertGreater(self.detect(rows).end_timestamp_ms,self.detect(trend()).end_timestamp_ms)

    def confirmation(self, direction='long', **changes):
        cfg=config(**changes.pop('config_changes',{})); sign=1 if direction=='long' else -1
        rows=[candle(i,110) for i in range(60)]
        rows[-1].update(o=110-sign*.1,c=110+sign*.1)
        defaults=dict(MACD=([-1*sign,-1*sign,1*sign],[0,0,0],[-2*sign,-1*sign,1*sign]),
                      ADX=[20,21,27],PLUS_DI=[30]*3 if sign==1 else [10]*3,
                      MINUS_DI=[10]*3 if sign==1 else [30]*3,ROC=[-sign,sign,2*sign])
        defaults.update(changes)
        def array(values):
            return fib.np.asarray([values[0]]*57+list(values),dtype=float)
        from contextlib import ExitStack
        with ExitStack() as stack:
            for name, values in defaults.items():
                result=tuple(array(v) for v in values) if name=='MACD' else array(values)
                stack.enter_context(patch.object(fib.talib,name,return_value=result))
            stack.enter_context(patch.object(fib.talib,'EMA',side_effect=[array([110+sign*.05]*3),array([110-sign]*3)]))
            return fib.calculate_confirmation(candles=rows,direction=direction,fib_level=fib.FibLevel(.5,110,109.7,110.3),trend_candles=rows,config=cfg)

    def test_macd_dmi_rejection_roc_htf_both_directions(self):
        for direction in ('long','short'):
            result=self.confirmation(direction)
            self.assertEqual(result.score,10)
            for field in ('macd_hist_rotation','macd_crossover','dmi_aligned','rejection','roc_confirmed','htf_aligned'):
                self.assertTrue(getattr(result,field))
            hist_only=self.confirmation(direction,MACD=([0,0,0],[0,0,0],[-2,-1,1] if direction=='long' else [2,1,-1]))
            self.assertTrue(hist_only.macd_hist_rotation)
            self.assertFalse(hist_only.macd_crossover)
            self.assertEqual(hist_only.score,8)
            cross_only=self.confirmation(direction,MACD=([-1,-1,1] if direction=='long' else [1,1,-1],[0,0,0],[0,0,0]))
            self.assertEqual(cross_only.score,9)

    def test_adx_awards(self):
        for values, expected in [([20,26,27],10),([19,20,21],9),([29,28,27],8),([12,13,14],8)]:
            self.assertEqual(self.confirmation(ADX=values).score,expected)
        self.assertEqual(self.confirmation(PLUS_DI=[5]*3).score,9)

    def test_disabled_roc_and_nan_warmup(self):
        self.assertIsNone(self.confirmation(ROC=[math.nan]*3))
        result=self.confirmation(config_changes={'fib_roc':False},ROC=[math.nan]*3)
        self.assertEqual((result.score,result.max_score),(9,9))
        self.assertFalse(result.roc_confirmed)
        self.assertIsNone(self.confirmation(MACD=([math.nan,0,1],[0,0,0],[0,0,1])))


@unittest.skipIf(fib.talib is None, 'TA-Lib unavailable')
class FibLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def context(self, direction='long', offset=0, price=None, cfg=None):
        cfg=cfg or config(); prices=fib.calculate_fib_prices(impulse(direction))
        price=price if price is not None else prices[.618]+(.1 if direction=='long' else -.1)
        rows=[candle(300+i+offset,price) for i in range(60)]
        rows[-1]['o']=price-(.15 if direction=='long' else -.15)
        return StrategyContext('BTC',int(rows[-1]['t'])+900000,cfg,{},trend(direction),rows,None)

    def confirmation(self,score=7):
        return fib.FibConfirmation(score,10,('confirmation',),True,True,True,True,True,27,30,10,1,True,True)

    async def test_bullish_and_bearish_evaluation(self):
        for direction in ('long','short'):
            ctx=self.context(direction); strategy=fib.FibReversalStrategy(ctx.config)
            # Real confirmed HTF pivots and ATR, deterministic entry indicator score.
            with patch.object(fib,'calculate_confirmation',return_value=self.confirmation()):
                signal=await strategy.evaluate(ctx)
                self.assertIsNotNone(signal)
                self.assertEqual((signal.strategy,signal.direction),('fib_reversal',direction))
                sign=1 if direction=='long' else -1
                self.assertGreater(sign*(signal.entry_price-signal.stop_price),0)
                self.assertGreater(sign*(signal.take_profit_prices[0]-signal.entry_price),0)
                self.assertGreater(sign*(signal.take_profit_prices[1]-signal.take_profit_prices[0]),0)
                self.assertGreater(sign*(signal.take_profit_prices[2]-signal.take_profit_prices[1]),0)
                self.assertGreaterEqual(signal.score,ctx.config.fib_min_score)
                self.assertAlmostEqual(signal.metadata['fib_ratio'],.618)
                json.dumps(signal.metadata,allow_nan=False)
                plan=strategy.build_execution_plan(ctx,signal,.01)
                self.assertEqual(plan.kind,'reversal')
                self.assertEqual(plan.take_profit_prices,signal.take_profit_prices)
                self.assertEqual(plan.metadata['tp_allocations'],(40,35,25))
                self.assertIsNone(await strategy.evaluate(ctx))
                self.assertIsNone(await strategy.evaluate(self.context(direction,1)))

    async def test_wait_confirm_timeout(self):
        for offset,expected in [(4,fib.FibSetupState.SIGNALLED),(5,fib.FibSetupState.INVALIDATED)]:
            ctx=self.context(); strategy=fib.FibReversalStrategy(ctx.config)
            with patch.object(fib,'calculate_confirmation',return_value=self.confirmation(1)):
                self.assertIsNone(await strategy.evaluate(ctx))
            self.assertEqual(strategy._setups['BTC'].state,fib.FibSetupState.WAITING_FOR_CONFIRMATION)
            with patch.object(fib,'calculate_confirmation',return_value=self.confirmation()):
                result=await strategy.evaluate(self.context(offset=offset))
            self.assertEqual(strategy._setups['BTC'].state,expected)
            self.assertEqual(result is not None,offset==4)

    async def test_new_impulse_and_multi_market(self):
        ctx=self.context(); strategy=fib.FibReversalStrategy(ctx.config)
        first=impulse(); second=replace(first,start_timestamp_ms=first.start_timestamp_ms+1,end_timestamp_ms=first.end_timestamp_ms+1)
        with patch.object(fib,'calculate_confirmation',return_value=self.confirmation()), patch.object(fib,'detect_latest_impulse',return_value=first):
            signals=await asyncio.gather(strategy.evaluate(ctx),strategy.evaluate(ctx),strategy.evaluate(replace(ctx,coin='ETH')))
            self.assertEqual(sum(s is not None for s in signals),2)
        with patch.object(fib,'calculate_confirmation',return_value=self.confirmation()), patch.object(fib,'detect_latest_impulse',return_value=second):
            self.assertIsNotNone(await strategy.evaluate(self.context(offset=1)))
        with patch.object(fib,'detect_latest_impulse',return_value=first):
            self.assertIsNone(await strategy.evaluate(self.context(offset=2)))

    async def test_distance_rr_position_and_closed_candle(self):
        for cfg,price,reason in [(config(fib_min_rr=100),None,'RR'),(config(),100,'structure')]:
            ctx=self.context(cfg=cfg,price=price); strategy=fib.FibReversalStrategy(cfg)
            with patch.object(fib,'calculate_confirmation',return_value=self.confirmation()):
                self.assertIsNone(await strategy.evaluate(ctx))
            self.assertIn(reason,strategy._setups['BTC'].invalidation_reason)
        ctx=self.context(); strategy=fib.FibReversalStrategy(ctx.config)
        self.assertIsNone(await strategy.evaluate(replace(ctx,current_position=object())))
        with patch.object(fib,'calculate_confirmation',return_value=self.confirmation(1)):
            await strategy.evaluate(ctx)
        with patch.object(fib,'calculate_confirmation',return_value=self.confirmation()):
            self.assertIsNone(await strategy.evaluate(self.context(offset=1,price=110)))
            self.assertIn('too far',strategy._setups['BTC'].invalidation_reason)
        strategy=fib.FibReversalStrategy(ctx.config)
        live=candle(360,100)
        with patch.object(fib,'calculate_confirmation',return_value=self.confirmation()):
            signal=await strategy.evaluate(replace(ctx,entry_candles=[*ctx.entry_candles,live]))
            self.assertEqual(signal.signal_candle_ms,ctx.entry_candles[-1]['t'])


class FibSharedEngineTests(unittest.IsolatedAsyncioTestCase):
    async def test_explicit_ladder_and_hidden_orders(self):
        from modes import position_management as pm
        async def identity(info, coin, value):
            return value
        for side, targets in [('long',(120.,125.44,132.36)),('short',(100.,94.56,87.64))]:
            with patch.object(pm,'round_size_for_hyperliquid',side_effect=identity), patch.object(pm,'round_price_for_hyperliquid',side_effect=identity):
                orders=await pm.build_take_profit_ladder(None,'BTC',side,10,110,.2,3,
                    take_profit_prices=targets,tp_allocations=(40,35,25))
                self.assertEqual([o['px'] for o in orders],list(targets))
                self.assertEqual([o['sz'] for o in orders],[4,3.5,2.5])
                exchange=SimpleNamespace(bulk_orders=AsyncMock(),order=AsyncMock())
                result=await pm.rebuild_bracket_orders(None,exchange,'account','BTC',side,10,110,.2,.05,
                    104 if side=='long' else 116,3,'Gtc',.01,cancel_existing_reduce_only=False,
                    hide_orders=True,take_profit_prices=targets,tp_allocations=(40,35,25))
                self.assertEqual(result[1],orders)
                exchange.bulk_orders.assert_not_awaited()
                exchange.order.assert_not_awaited()
                with self.assertRaises(RuntimeError):
                    await pm.build_take_profit_ladder(None,'BTC',side,10,110,.2,3,
                        take_profit_prices=targets,tp_allocations=(40,35,20))

    async def test_shared_scan_uses_configured_timeframes_and_history(self):
        from modes import auto_trader as auto
        cfg=config(); cfg.fib_history_periods=187
        strategy=SimpleNamespace(name='fib_reversal',evaluate=AsyncMock(return_value=None))
        snapshot=auto.AutoScanLoopSnapshot({'BTC':110},{},1000,0)
        data=[candle(0,110)]
        with patch.object(auto,'fetch_recent_candles',new_callable=AsyncMock,return_value=data) as fetch:
            candidate=await auto.scan_auto_trade_candidate(None,'BTC',strategy,cfg,snapshot,use_websocket_candles=True)
        self.assertEqual([call.args[2:] for call in fetch.await_args_list],[('1h',187),('15m',187)])
        self.assertTrue(all(call.kwargs['use_websocket_candles'] for call in fetch.await_args_list))
        self.assertEqual(candidate.strategy_context.trend_candles,data)
        self.assertEqual(candidate.strategy_context.entry_candles,data)

    async def test_signal_price_normalization_preserves_metadata(self):
        from strategies.reversal import normalize_signal_prices
        from strategies.base import StrategySignal
        signal=StrategySignal('fib_reversal','BTC','long',1,107.64,104.18,(120,125.44,132.36),7,(),{'tp_allocations':(40,35,25)})
        with patch('utils.helpers.round_price_for_hyperliquid',new_callable=AsyncMock,side_effect=lambda i,c,p:round(p,1)):
            result=await normalize_signal_prices(None,signal)
        self.assertEqual(result.take_profit_prices,(120,125.4,132.4))
        self.assertEqual(result.metadata,signal.metadata)


class FibCliWiringTests(unittest.IsolatedAsyncioTestCase):
    async def test_cli_forwards_all_fib_options(self):
        from hypertrader import async_main
        with patch('modes.auto_trader.run_auto_trader',new_callable=AsyncMock) as run:
            await async_main(['auto','BTC','--size-pct','10','--strategy','fibonacci',
                              '--fib-entry-levels','.618 .786','--fib-min-score','6','--no-fib-roc'])
        kwargs=run.await_args.kwargs
        self.assertEqual(kwargs['strategy_name'],'fib_reversal')
        for name,value in fib.FIB_DEFAULTS.items():
            expected={'fib_entry_levels':'.618 .786','fib_min_score':6,'fib_roc':False}.get(name,value)
            self.assertEqual(kwargs[name],expected)

    async def test_cli_invalid_values_never_launch_auto(self):
        from hypertrader import async_main
        for options in (['--fib-min-score','11'],['--fib-entry-levels',''],['--fib-macd-fast','26'],
                        ['--trend-interval','nonsense'],['--entry-interval','1h']):
            with patch('modes.auto_trader.run_auto_trader',new_callable=AsyncMock) as run:
                with self.assertRaises(SystemExit):
                    await async_main(['auto','BTC','--size','1','--strategy','fib',*options])
                run.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
