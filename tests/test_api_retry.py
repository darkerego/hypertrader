import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call, patch

import aiohttp
from hyperliquid.utils.error import ClientError, ServerError

from modes.auto_trader import get_top_perp_markets_by_volume
from utils import helpers, retry


class ApiRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_repeated_gateway_errors_back_off_until_recovery(self):
        read = AsyncMock(side_effect=[ServerError(502, "Bad Gateway")] * 7 + ["ok"])
        with patch.object(retry.asyncio, "sleep", new_callable=AsyncMock) as sleep:
            result = await retry.retry_api_read(read, context="test read")
        self.assertEqual(result, "ok")
        self.assertEqual(read.await_count, 8)
        self.assertEqual(sleep.await_args_list, [call(n) for n in (5, 10, 20, 40, 60, 60, 60)])

    async def test_backoff_resets_for_next_operation(self):
        read = AsyncMock(side_effect=[ServerError(503, "unavailable"), 1] * 2)
        with patch.object(retry.asyncio, "sleep", new_callable=AsyncMock) as sleep:
            await retry.retry_api_read(read, context="first")
            await retry.retry_api_read(read, context="second")
        self.assertEqual(sleep.await_args_list, [call(5), call(5)])

    async def test_non_transient_errors_propagate_without_retry(self):
        for error in (ClientError(400, None, "bad request", None), ValueError("bad data")):
            with self.subTest(error=error):
                read = AsyncMock(side_effect=error)
                with patch.object(retry.asyncio, "sleep", new_callable=AsyncMock) as sleep:
                    with self.assertRaises(type(error)):
                        await retry.retry_api_read(read, context="test read")
                read.assert_awaited_once()
                sleep.assert_not_awaited()

    async def test_rate_limit_connection_and_timeout_errors_retry(self):
        for error in (
            ClientError(429, None, "rate limited", None),
            aiohttp.ClientConnectionError("disconnected"),
            asyncio.TimeoutError(),
        ):
            with self.subTest(error=error):
                read = AsyncMock(side_effect=[error, "ok"])
                with patch.object(retry.asyncio, "sleep", new_callable=AsyncMock) as sleep:
                    self.assertEqual(await retry.retry_api_read(read, context="read"), "ok")
                sleep.assert_awaited_once_with(5)

    async def test_cancellation_during_request_or_backoff_propagates(self):
        read = AsyncMock(side_effect=asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):
            await retry.retry_api_read(read, context="cancelled read")
        read.assert_awaited_once()

        read = AsyncMock(side_effect=ServerError(502, "Bad Gateway"))
        with patch.object(retry.asyncio, "sleep", new_callable=AsyncMock,
                          side_effect=asyncio.CancelledError()):
            with self.assertRaises(asyncio.CancelledError):
                await retry.retry_api_read(read, context="cancelled backoff")
        read.assert_awaited_once()

    async def test_market_ranking_recovers_from_logged_502(self):
        info = SimpleNamespace(meta_and_asset_ctxs=AsyncMock(side_effect=[
            ServerError(502, "<html>502 Bad Gateway</html>"),
            [{"universe": [{"name": "BTC"}, {"name": "ETH"}]},
             [{"dayNtlVlm": "100"}, {"dayNtlVlm": "200"}]],
        ]))
        with patch.object(retry.asyncio, "sleep", new_callable=AsyncMock) as sleep:
            result = await get_top_perp_markets_by_volume(info, 1)
        self.assertEqual(result, [("ETH", 200.0)])
        sleep.assert_awaited_once_with(5)
        self.assertEqual(info.meta_and_asset_ctxs.await_count, 2)

    async def test_startup_closes_failed_clients_before_backoff_then_recovers(self):
        for stage in ("info", "exchange", "cache"):
            with self.subTest(stage=stage):
                infos = [SimpleNamespace(initialize=AsyncMock(), aclose=AsyncMock()) for _ in range(2)]
                exchanges = [SimpleNamespace(initialize=AsyncMock(), aclose=AsyncMock()) for _ in range(2)]
                caches = [SimpleNamespace(start=AsyncMock(), stop=AsyncMock()) for _ in range(2)]
                failure = ServerError(502, "Bad Gateway")
                if stage == "info":
                    infos[0].initialize.side_effect = failure
                elif stage == "exchange":
                    exchanges[0].initialize.side_effect = failure
                else:
                    caches[0].start.side_effect = failure

                async def check_cleanup(delay):
                    self.assertEqual(delay, 5)
                    infos[0].aclose.assert_awaited_once()
                    if stage != "info":
                        exchanges[0].aclose.assert_awaited_once()
                    if stage == "cache":
                        caches[0].stop.assert_awaited_once()

                with (
                    patch.object(helpers, "load_credentials", return_value=("secret", "0xabc")),
                    patch.object(helpers.eth_account.Account, "from_key", return_value=object()),
                    patch.object(helpers, "Info", side_effect=infos),
                    patch.object(helpers, "Exchange", side_effect=exchanges),
                    patch.object(helpers, "HyperliquidWebsocketCache", side_effect=caches),
                    patch.object(helpers, "get_ws_cache", side_effect=lambda info: getattr(info, "ws_cache", None)),
                    patch.object(retry.asyncio, "sleep", new_callable=AsyncMock, side_effect=check_cleanup),
                ):
                    address, info, exchange = await helpers.init_clients(False, use_websocket=False)
                self.assertEqual(address, "0xabc")
                self.assertIs(info, infos[1])
                self.assertIs(exchange, exchanges[0 if stage == "info" else 1])
                info.aclose.assert_not_awaited()
                exchange.aclose.assert_not_awaited()

    async def test_startup_cancellation_closes_partial_client(self):
        info = SimpleNamespace(initialize=AsyncMock(side_effect=asyncio.CancelledError()), aclose=AsyncMock())
        with (
            patch.object(helpers, "load_credentials", return_value=("secret", "0xabc")),
            patch.object(helpers.eth_account.Account, "from_key", return_value=object()),
            patch.object(helpers, "Info", return_value=info),
            patch.object(helpers, "Exchange", new_callable=Mock) as exchange,
            patch.object(retry.asyncio, "sleep", new_callable=AsyncMock) as sleep,
        ):
            with self.assertRaises(asyncio.CancelledError):
                await helpers.init_clients(False)
        info.aclose.assert_awaited_once()
        exchange.assert_not_called()
        sleep.assert_not_awaited()
