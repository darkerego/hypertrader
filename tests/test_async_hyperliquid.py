from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from typing import Any

import pytest

import async_hyperliquid as ah


class FakeWebsocketManager:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.ws = object()
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True
        self.ws = object()

    async def stop(self) -> None:
        self.stopped = True
        self.ws = None


class FakeInfo:
    create_calls = 0

    def __init__(self, base_url: str, skip_ws: bool = False, timeout: float | None = None):
        self.base_url = base_url
        self.skip_ws = skip_ws
        self.timeout = timeout
        self.ws_manager = None if skip_ws else FakeWebsocketManager(base_url)
        self.coin_to_asset = {"BTC": 0, "ETH": 1, "SOL": 2}
        self.name_to_coin = {"BTC": "BTC", "ETH": "ETH", "SOL": "SOL"}
        self.asset_to_sz_decimals = {0: 3, 1: 2, 2: 1}
        self.subscribe_calls: list[dict[str, Any]] = []
        self.unsubscribe_calls: list[tuple[dict[str, Any], int]] = []
        self.callbacks: dict[int, Any] = {}
        self.sub_id = 0
        self.all_mids_calls = 0
        self.user_state_calls = 0
        self.frontend_open_orders_calls = 0
        self.user_fills_calls = 0
        self.order_status_calls = 0
        self.l2_snapshot_calls = 0
        self.candles_snapshot_calls = 0
        self._all_mids = {"BTC": "100.0", "ETH": "200.0", "SOL": "25.0"}
        self._user_state = {
            "marginSummary": {"accountValue": "1000", "withdrawable": "800"},
            "assetPositions": [
                {
                    "position": {
                        "coin": "BTC",
                        "szi": "1.5",
                        "entryPx": "95",
                        "markPx": "100",
                        "marginUsed": "50",
                        "unrealizedPnl": "7.5",
                        "liquidationPx": "70",
                        "leverage": {"value": "2"},
                    }
                }
            ],
        }
        self._frontend_orders = [
            {
                "coin": "BTC",
                "oid": 10,
                "isBuy": False,
                "sz": "0.5",
                "limitPx": "110",
                "reduceOnly": True,
                "clientOrderId": "tp-existing",
            }
        ]
        self._fills = [{"oid": 10, "coin": "BTC", "sz": "0.25", "closedPnl": "5.0", "fee": "0.1", "time": 1000}]
        self._order_status = {"oid": 10, "coin": "BTC", "status": "filled"}
        self._l2_book = {"levels": [[{"px": "99.0", "sz": "1"}], [{"px": "101.0", "sz": "1"}]]}
        self._candles = [
            {"s": "BTC", "i": "1m", "t": 1000, "T": 1999, "o": "90", "h": "101", "l": "89", "c": "100", "v": "10"},
            {"s": "BTC", "i": "1m", "t": 1000, "T": 1999, "o": "90", "h": "101", "l": "89", "c": "100", "v": "10"},
            {"s": "BTC", "i": "1m", "t": 2000, "T": 2999, "o": "100", "h": "102", "l": "99", "c": "101", "v": "11"},
        ]
        self.closed = False

    @classmethod
    async def create(cls, base_url: str, skip_ws: bool = False, timeout: float | None = None) -> "FakeInfo":
        cls.create_calls += 1
        return cls(base_url, skip_ws=skip_ws, timeout=timeout)

    async def initialize(self) -> "FakeInfo":
        return self

    async def subscribe(self, subscription: dict[str, Any], callback: Any) -> int:
        self.sub_id += 1
        self.subscribe_calls.append(dict(subscription))
        self.callbacks[self.sub_id] = callback
        return self.sub_id

    async def unsubscribe(self, subscription: dict[str, Any], subscription_id: int) -> bool:
        self.unsubscribe_calls.append((dict(subscription), subscription_id))
        self.callbacks.pop(subscription_id, None)
        return True

    async def all_mids(self) -> dict[str, str]:
        self.all_mids_calls += 1
        return dict(self._all_mids)

    async def user_state(self, address: str) -> dict[str, Any]:
        self.user_state_calls += 1
        return dict(self._user_state)

    async def frontend_open_orders(self, address: str) -> list[dict[str, Any]]:
        self.frontend_open_orders_calls += 1
        return [dict(order) for order in self._frontend_orders]

    async def user_fills_by_time(self, address: str, start_time: int, end_time: int | None = None, aggregate_by_time: bool = False) -> list[dict[str, Any]]:
        self.user_fills_calls += 1
        return [dict(fill) for fill in self._fills]

    async def query_order_by_oid(self, address: str, oid: int) -> dict[str, Any]:
        self.order_status_calls += 1
        return dict(self._order_status)

    async def l2_snapshot(self, coin: str) -> dict[str, Any]:
        self.l2_snapshot_calls += 1
        return dict(self._l2_book)

    async def candles_snapshot(self, coin: str, interval: str, start_time: int, end_time: int) -> list[dict[str, Any]]:
        self.candles_snapshot_calls += 1
        return [dict(row) for row in self._candles]

    async def aclose(self) -> None:
        self.closed = True


class FakeExchange:
    create_calls = 0

    def __init__(self) -> None:
        self.orders: list[dict[str, Any]] = []
        self.cancels: list[Any] = []
        self.leverages: list[Any] = []
        self.closed = False
        self.order_response = {"response": {"data": {"statuses": [{"resting": {"oid": 111}}]}}}
        self.cancel_response = {"response": {"data": {"statuses": [{"success": True}]}}}

    @classmethod
    async def create(cls, wallet: Any, base_url: str, vault_address: str | None = None, account_address: str | None = None, timeout: float | None = None) -> "FakeExchange":
        cls.create_calls += 1
        return cls()

    async def order(self, coin: str, is_buy: bool, sz: float, limit_px: float, order_type: dict[str, Any], reduce_only: bool = False, cloid: Any = None) -> dict[str, Any]:
        self.orders.append(
            {
                "coin": coin,
                "is_buy": is_buy,
                "sz": sz,
                "limit_px": limit_px,
                "order_type": order_type,
                "reduce_only": reduce_only,
                "cloid": cloid,
            }
        )
        return self.order_response

    async def bulk_cancel(self, requests: list[dict[str, Any]]) -> dict[str, Any]:
        self.cancels.append(requests)
        return self.cancel_response

    async def bulk_cancel_by_cloid(self, requests: list[dict[str, Any]]) -> dict[str, Any]:
        self.cancels.append(requests)
        return self.cancel_response

    async def update_leverage(self, leverage: int, coin: str, is_cross: bool = True) -> dict[str, Any]:
        self.leverages.append((leverage, coin, is_cross))
        return {"status": "ok"}

    async def aclose(self) -> None:
        self.closed = True


@pytest.fixture
def patched_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    FakeInfo.create_calls = 0
    FakeExchange.create_calls = 0
    monkeypatch.setattr(ah, "Info", FakeInfo)
    monkeypatch.setattr(ah, "Exchange", FakeExchange)
    monkeypatch.setattr(ah, "WebsocketManager", FakeWebsocketManager)
    monkeypatch.setattr(ah.eth_account.Account, "from_key", lambda key: SimpleNamespace(address="0xabc"))


@pytest.fixture
async def trading_client(patched_sdk: None) -> ah.AsyncHyperliquid:
    client = ah.AsyncHyperliquid("0x" + "1" * 40, secret_key="super-secret")
    await client.initialize()
    yield client
    await client.close()


@pytest.fixture
async def readonly_client(patched_sdk: None) -> ah.AsyncHyperliquid:
    client = ah.AsyncHyperliquid("0x" + "2" * 40)
    await client.initialize()
    yield client
    await client.close()


@pytest.mark.asyncio
async def test_constructor_performs_no_network_io(patched_sdk: None) -> None:
    ah.AsyncHyperliquid("0x" + "3" * 40, secret_key="secret")
    assert FakeInfo.create_calls == 0
    assert FakeExchange.create_calls == 0


@pytest.mark.asyncio
async def test_initialization_populates_metadata_and_state(trading_client: ah.AsyncHyperliquid) -> None:
    assert trading_client.normalize_coin("btc") == "BTC"
    assert await trading_client.get_account_balance() == 1000.0
    positions = await trading_client.get_open_positions()
    assert positions[0].coin == "BTC"


@pytest.mark.asyncio
async def test_rest_call_uses_async_sdk_callable(trading_client: ah.AsyncHyperliquid) -> None:
    result = await trading_client._rest_call("all_mids", trading_client.info.all_mids)  # type: ignore[arg-type]
    assert result["BTC"] == "100.0"


@pytest.mark.asyncio
async def test_websocket_callbacks_enqueue_safely(trading_client: ah.AsyncHyperliquid) -> None:
    callback = trading_client._subscriptions["allMids"].callback
    callback({"data": {"mids": {"BTC": "111.0"}}})
    await asyncio.sleep(0.05)
    assert await trading_client.get_mid("BTC") == 111.0


@pytest.mark.asyncio
async def test_duplicate_consumers_share_one_subscription(trading_client: ah.AsyncHyperliquid) -> None:
    info = trading_client.info
    assert isinstance(info, FakeInfo)
    before = len(info.subscribe_calls)
    q1 = await trading_client.subscribe_candles("BTC", "1m")
    q2 = await trading_client.subscribe_candles("BTC", "1m")
    assert len(info.subscribe_calls) == before + 1
    await trading_client.unsubscribe_candles("BTC", "1m", q1)
    await trading_client.unsubscribe_candles("BTC", "1m", q2)


@pytest.mark.asyncio
async def test_subscriptions_restore_after_reconnect(trading_client: ah.AsyncHyperliquid) -> None:
    await trading_client.get_best_bid_ask("BTC")
    info = trading_client.info
    assert isinstance(info, FakeInfo)
    before = len(info.subscribe_calls)
    await trading_client._restart_websocket()
    assert len(info.subscribe_calls) > before


@pytest.mark.asyncio
async def test_fresh_websocket_mid_avoids_rest(trading_client: ah.AsyncHyperliquid, monkeypatch: pytest.MonkeyPatch) -> None:
    trading_client._mids["BTC"] = 123.0
    trading_client._mids_ts_ms["BTC"] = ah._now_monotonic_ms()
    monkeypatch.setattr(trading_client, "_fetch_mid_rest", pytest.fail)
    assert await trading_client.get_mid("BTC") == 123.0


@pytest.mark.asyncio
async def test_stale_websocket_mid_falls_back_to_rest(trading_client: ah.AsyncHyperliquid, monkeypatch: pytest.MonkeyPatch) -> None:
    trading_client._mids["BTC"] = 123.0
    trading_client._mids_ts_ms["BTC"] = 0

    async def fake_fetch(coin: str) -> float:
        return 222.0

    monkeypatch.setattr(trading_client, "_fetch_mid_rest", fake_fetch)
    assert await trading_client.get_mid("BTC") == 222.0


@pytest.mark.asyncio
async def test_price_rounding_follows_metadata(trading_client: ah.AsyncHyperliquid) -> None:
    assert trading_client.round_price("BTC", 123.456789) == pytest.approx(123.457)


@pytest.mark.asyncio
async def test_size_rounding_follows_metadata(trading_client: ah.AsyncHyperliquid) -> None:
    assert trading_client.round_size("BTC", 1.23456) == pytest.approx(1.234)


@pytest.mark.asyncio
async def test_zero_rounded_size_is_rejected(trading_client: ah.AsyncHyperliquid) -> None:
    with pytest.raises(ah.HyperliquidValidationError):
        trading_client.round_size("BTC", 0.0001)


@pytest.mark.asyncio
async def test_market_orders_use_side_aware_ioc_protection_prices(trading_client: ah.AsyncHyperliquid) -> None:
    result = await trading_client.place_market_order("BTC", "buy", 1.0)
    assert result.success is True
    assert trading_client.exchange.orders[-1]["order_type"] == {"limit": {"tif": "Ioc"}}  # type: ignore[index]
    assert trading_client.exchange.orders[-1]["limit_px"] > 101.0  # type: ignore[index]


@pytest.mark.asyncio
async def test_post_only_orders_use_alo(trading_client: ah.AsyncHyperliquid) -> None:
    await trading_client.place_post_only_order("BTC", "sell", 1.0, price=120.0)
    assert trading_client.exchange.orders[-1]["order_type"] == {"limit": {"tif": "Alo"}}  # type: ignore[index]


@pytest.mark.asyncio
async def test_reduce_only_flag_is_preserved(trading_client: ah.AsyncHyperliquid) -> None:
    await trading_client.place_reduce_only_order("BTC", "sell", 0.5, price=110.0)
    assert trading_client.exchange.orders[-1]["reduce_only"] is True  # type: ignore[index]


@pytest.mark.asyncio
async def test_stop_market_payload_is_correct(trading_client: ah.AsyncHyperliquid) -> None:
    await trading_client.place_stop_market_order("BTC", "sell", 1.0, 90.0)
    payload = trading_client.exchange.orders[-1]["order_type"]  # type: ignore[index]
    assert payload["trigger"]["isMarket"] is True
    assert payload["trigger"]["tpsl"] == "sl"


@pytest.mark.asyncio
async def test_sdk_responses_normalize_into_order_result(trading_client: ah.AsyncHyperliquid) -> None:
    result = await trading_client.place_limit_order("BTC", "buy", 1.0, price=95.0)
    assert isinstance(result, ah.OrderResult)
    assert result.order_id == 111


@pytest.mark.asyncio
async def test_entry_chasing_replaces_only_unfilled_quantity(trading_client: ah.AsyncHyperliquid, monkeypatch: pytest.MonkeyPatch) -> None:
    placed: list[float] = []
    filled_map = {1: 0.4, 2: 1.0}
    order_ids = iter([1, 2])

    async def fake_place(*args: Any, **kwargs: Any) -> ah.OrderResult:
        placed.append(kwargs["size"] if "size" in kwargs else args[2])
        oid = next(order_ids)
        return ah.OrderResult(True, "BTC", "buy", placed[-1], kwargs["price"], oid, kwargs["client_order_id"], "resting", {})

    async def fake_cancel(coin: str, oid: int | str) -> bool:
        return True

    async def fake_status(coin: str, oid: int | str, refresh: bool = False) -> dict[str, Any]:
        return {"oid": oid, "status": "open"}

    monkeypatch.setattr(trading_client, "place_post_only_order", fake_place)
    monkeypatch.setattr(trading_client, "cancel_order", fake_cancel)
    monkeypatch.setattr(trading_client, "get_order_status", fake_status)
    monkeypatch.setattr(trading_client, "_filled_amount_for_order", lambda oid: filled_map.get(int(oid), 0.0))
    result = await trading_client.enter_position_chasing("BTC", "buy", 1.0, reprice_interval=0.01, max_reprices=2)
    assert placed[0] == pytest.approx(1.0)
    assert placed[1] == pytest.approx(0.6, rel=0, abs=0.001)
    assert result.size > 0


@pytest.mark.asyncio
async def test_fill_during_cancel_race_cannot_overfill(trading_client: ah.AsyncHyperliquid, monkeypatch: pytest.MonkeyPatch) -> None:
    placed: list[float] = []
    fills = {1: 0.7, 2: 1.0}
    ids = iter([1, 2])

    async def fake_place(*args: Any, **kwargs: Any) -> ah.OrderResult:
        placed.append(kwargs["size"] if "size" in kwargs else args[2])
        return ah.OrderResult(True, "BTC", "buy", placed[-1], kwargs["price"], next(ids), kwargs["client_order_id"], "resting", {})

    monkeypatch.setattr(trading_client, "place_post_only_order", fake_place)
    monkeypatch.setattr(trading_client, "cancel_order", lambda coin, oid: asyncio.sleep(0, result=True))
    monkeypatch.setattr(trading_client, "get_order_status", lambda *args, **kwargs: asyncio.sleep(0, result={"status": "open"}))
    monkeypatch.setattr(trading_client, "_filled_amount_for_order", lambda oid: fills.get(int(oid), 0.0))
    await trading_client.enter_position_chasing("BTC", "buy", 1.0, reprice_interval=0.01, max_reprices=2)
    assert sum(placed) >= 1.0
    assert placed[1] == pytest.approx(0.3, rel=0, abs=0.001)


@pytest.mark.asyncio
async def test_chase_timeout_cancels_remainder(trading_client: ah.AsyncHyperliquid, monkeypatch: pytest.MonkeyPatch) -> None:
    cancelled: list[int | str] = []

    async def fake_place(*args: Any, **kwargs: Any) -> ah.OrderResult:
        return ah.OrderResult(True, "BTC", "buy", 1.0, kwargs["price"], 5, kwargs["client_order_id"], "resting", {})

    monkeypatch.setattr(trading_client, "place_post_only_order", fake_place)
    monkeypatch.setattr(trading_client, "_filled_amount_for_order", lambda oid: 0.0)
    monkeypatch.setattr(trading_client, "cancel_order", lambda coin, oid: asyncio.sleep(0, result=cancelled.append(oid) is None or True))
    result = await trading_client.enter_position_chasing("BTC", "buy", 1.0, timeout=0.02, reprice_interval=0.01)
    assert result.status == "partial_fill"
    assert cancelled


@pytest.mark.asyncio
async def test_coroutine_cancellation_cleans_up_live_chase_order(trading_client: ah.AsyncHyperliquid, monkeypatch: pytest.MonkeyPatch) -> None:
    cancelled: list[int | str] = []

    async def fake_place(*args: Any, **kwargs: Any) -> ah.OrderResult:
        return ah.OrderResult(True, "BTC", "buy", 1.0, kwargs["price"], 9, kwargs["client_order_id"], "resting", {})

    monkeypatch.setattr(trading_client, "place_post_only_order", fake_place)
    monkeypatch.setattr(trading_client, "_filled_amount_for_order", lambda oid: 0.0)
    monkeypatch.setattr(trading_client, "cancel_order", lambda coin, oid: asyncio.sleep(0, result=cancelled.append(oid) is None or True))
    task = asyncio.create_task(trading_client.enter_position_chasing("BTC", "buy", 1.0, timeout=1.0, reprice_interval=0.5))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled == [9]


@pytest.mark.asyncio
async def test_market_close_uses_opposite_side_and_reduce_only(trading_client: ah.AsyncHyperliquid, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    async def fake_market_order(*args: Any, **kwargs: Any) -> ah.OrderResult:
        calls.append({"args": args, "kwargs": kwargs})
        return ah.OrderResult(True, "BTC", "sell", 1.5, 99.0, 1, None, "filled", {})

    monkeypatch.setattr(trading_client, "place_market_order", fake_market_order)
    result = await trading_client.close_position_market("BTC", verify_closed=False)
    assert result is not None
    assert calls[0]["args"][1] == ah.OrderSide.SELL
    assert calls[0]["kwargs"]["reduce_only"] is True


@pytest.mark.asyncio
async def test_market_close_cannot_reverse_position(trading_client: ah.AsyncHyperliquid, monkeypatch: pytest.MonkeyPatch) -> None:
    positions = [
        ah.Position("BTC", 1.0, 100.0, 100.0, None, None, 1.0, 0.0, None, {}),
        ah.Position("BTC", -0.1, 100.0, 100.0, None, None, 1.0, 0.0, None, {}),
    ]

    async def fake_get_position(*args: Any, **kwargs: Any) -> ah.Position | None:
        return positions.pop(0)

    monkeypatch.setattr(trading_client, "get_position", fake_get_position)
    monkeypatch.setattr(trading_client, "place_market_order", lambda *args, **kwargs: asyncio.sleep(0, result=ah.OrderResult(True, "BTC", "sell", 1.0, 99.0, 1, None, "filled", {})))
    with pytest.raises(ah.HyperliquidOrderError):
        await trading_client.close_position_market("BTC")


@pytest.mark.asyncio
async def test_position_sizing_uses_margin_pct_leverage_price_and_downward_rounding(trading_client: ah.AsyncHyperliquid) -> None:
    size = await trading_client.calculate_position_size("BTC", margin_pct=10.0, leverage=2.0, price=100.0, reserve_margin_pct=10.0)
    assert size == pytest.approx(1.8)


@pytest.mark.asyncio
async def test_candle_backfills_are_sorted_and_deduplicated(trading_client: ah.AsyncHyperliquid) -> None:
    candles = await trading_client.get_candles("BTC", "1m", limit=10, refresh=True)
    assert [c.open_time_ms for c in candles] == [1000, 2000]


@pytest.mark.asyncio
async def test_live_candles_merge_with_backfill_without_duplication(trading_client: ah.AsyncHyperliquid) -> None:
    await trading_client.get_candles("BTC", "1m", limit=10, refresh=True)
    await trading_client._handle_candle_message({"data": {"s": "BTC", "i": "1m", "t": 2000, "T": 2999, "o": "100", "h": "103", "l": "99", "c": "102", "v": "12"}})
    candles = await trading_client.get_candles("BTC", "1m", limit=10)
    assert [c.open_time_ms for c in candles] == [1000, 2000]
    assert candles[-1].close == 102.0


@pytest.mark.asyncio
async def test_multiple_candle_consumers_share_one_exchange_subscription(trading_client: ah.AsyncHyperliquid) -> None:
    info = trading_client.info
    assert isinstance(info, FakeInfo)
    count_before = len(info.subscribe_calls)
    q1 = await trading_client.subscribe_candles("ETH", "1m")
    q2 = await trading_client.subscribe_candles("ETH", "1m")
    assert len(info.subscribe_calls) == count_before + 1
    await trading_client.unsubscribe_candles("ETH", "1m", q1)
    await trading_client.unsubscribe_candles("ETH", "1m", q2)


@pytest.mark.asyncio
async def test_fill_waiters_resolve_from_websocket_events(trading_client: ah.AsyncHyperliquid) -> None:
    waiter = asyncio.create_task(trading_client.wait_for_fill(order_id=77, timeout=1.0))
    await asyncio.sleep(0)
    await trading_client._handle_user_fills_message({"data": {"fills": [{"oid": 77, "coin": "BTC", "sz": "1", "time": 1111}]}})
    fill = await waiter
    assert fill["oid"] == 77


@pytest.mark.asyncio
async def test_timeout_paths_perform_rest_reconciliation(trading_client: ah.AsyncHyperliquid, monkeypatch: pytest.MonkeyPatch) -> None:
    called = {"refresh": 0}

    async def fake_status(coin: str, oid: int | str, refresh: bool = False) -> dict[str, Any]:
        called["refresh"] += int(refresh)
        return {"oid": oid, "status": "filled"}

    monkeypatch.setattr(trading_client, "get_order_status", fake_status)
    status = await trading_client.wait_for_order_terminal("BTC", 101, timeout=0.01)
    assert status["status"] == "filled"
    assert called["refresh"] == 1


@pytest.mark.asyncio
async def test_tp_ladder_size_never_exceeds_position(trading_client: ah.AsyncHyperliquid) -> None:
    specs = trading_client._build_tp_order_specs(
        await trading_client.get_position("BTC"),
        [(1.0, 80.0), (2.0, 80.0)],
        target_mode="percent",
        size_mode="position_percent",
        min_profit_pct=0.0,
    )
    assert sum(spec["size"] for spec in specs) <= 1.5


@pytest.mark.asyncio
async def test_tp_prices_are_on_profitable_side_of_entry(trading_client: ah.AsyncHyperliquid) -> None:
    position = await trading_client.get_position("BTC")
    specs = trading_client._build_tp_order_specs(position, [(1.0, 50.0)], target_mode="percent", size_mode="position_percent", min_profit_pct=0.0)
    assert specs[0]["price"] > position.entry_price


@pytest.mark.asyncio
async def test_tp_reconciliation_is_idempotent(trading_client: ah.AsyncHyperliquid, monkeypatch: pytest.MonkeyPatch) -> None:
    placed: list[Any] = []

    async def fake_place(*args: Any, **kwargs: Any) -> ah.OrderResult:
        placed.append((args, kwargs))
        return ah.OrderResult(True, "BTC", "sell", 0.75, 101.0, 500 + len(placed), kwargs["client_order_id"], "resting", {})

    monkeypatch.setattr(trading_client, "_place_tp_order", fake_place)
    await trading_client.reconcile_take_profit_ladder("BTC", [(1.0, 50.0)], client_order_id_prefix="tp")
    trading_client._open_orders["501"] = {"coin": "BTC", "oid": 501, "isBuy": False, "sz": "0.75", "limitPx": "95.95", "reduceOnly": True, "clientOrderId": "tp-1"}
    await trading_client.reconcile_take_profit_ladder("BTC", [(1.0, 50.0)], client_order_id_prefix="tp")
    assert len(placed) == 1


@pytest.mark.asyncio
async def test_tp_reconciliation_preserves_unrelated_orders(trading_client: ah.AsyncHyperliquid, monkeypatch: pytest.MonkeyPatch) -> None:
    trading_client._open_orders["900"] = {"coin": "BTC", "oid": 900, "isBuy": True, "sz": "1", "limitPx": "90", "reduceOnly": False}
    cancelled: list[int | str] = []
    monkeypatch.setattr(trading_client, "cancel_order", lambda coin, oid: asyncio.sleep(0, result=cancelled.append(oid) is None or True))
    monkeypatch.setattr(trading_client, "_place_tp_order", lambda *args, **kwargs: asyncio.sleep(0, result=ah.OrderResult(True, "BTC", "sell", 0.75, 101.0, 700, kwargs["client_order_id"], "resting", {})))
    await trading_client.reconcile_take_profit_ladder("BTC", [(1.0, 50.0)], client_order_id_prefix="tp")
    assert 900 not in cancelled


@pytest.mark.asyncio
async def test_rest_reads_retry_transient_failures(trading_client: ah.AsyncHyperliquid) -> None:
    attempts = {"count": 0}

    async def flaky() -> str:
        attempts["count"] += 1
        if attempts["count"] < 2:
            raise RuntimeError("temporary connection issue")
        return "ok"

    result = await trading_client._rest_call("read", flaky, retries=2)
    assert result == "ok"
    assert attempts["count"] == 2


@pytest.mark.asyncio
async def test_signed_order_submissions_are_not_blindly_retried(trading_client: ah.AsyncHyperliquid, monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = {"count": 0}

    async def fail_order(*args: Any, **kwargs: Any) -> dict[str, Any]:
        attempts["count"] += 1
        raise RuntimeError("temporary connection issue")

    monkeypatch.setattr(trading_client.exchange, "order", fail_order)
    with pytest.raises(ah.HyperliquidOrderError):
        await trading_client.place_limit_order("BTC", "buy", 1.0, price=90.0)
    assert attempts["count"] == 1


@pytest.mark.asyncio
async def test_shutdown_is_idempotent(trading_client: ah.AsyncHyperliquid) -> None:
    await trading_client.close()
    await trading_client.close()
    assert trading_client.info is None


@pytest.mark.asyncio
async def test_shutdown_leaves_no_owned_tasks_running(patched_sdk: None) -> None:
    client = ah.AsyncHyperliquid("0x" + "4" * 40, secret_key="secret")
    await client.initialize()
    await client.close()
    assert not client._background_tasks


@pytest.mark.asyncio
async def test_secret_keys_never_appear_in_logs(patched_sdk: None, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    caplog.set_level(logging.DEBUG)
    client = ah.AsyncHyperliquid("0x" + "5" * 40, secret_key="my-very-secret-key")
    await client.initialize()

    async def fail(*args: Any, **kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("temporary connection issue")

    monkeypatch.setattr(client.exchange, "order", fail)
    with pytest.raises(ah.HyperliquidOrderError):
        await client.place_limit_order("BTC", "buy", 1.0, price=90.0)
    await client.close()
    assert "my-very-secret-key" not in caplog.text


@pytest.mark.asyncio
async def test_readonly_mode_blocks_trading(readonly_client: ah.AsyncHyperliquid) -> None:
    with pytest.raises(ah.HyperliquidReadOnlyError):
        await readonly_client.place_limit_order("BTC", "buy", 1.0, price=90.0)


@pytest.mark.asyncio
async def test_cancel_by_client_id_uses_cloid_mapping(trading_client: ah.AsyncHyperliquid) -> None:
    await trading_client.place_limit_order("BTC", "buy", 1.0, price=90.0, client_order_id="abc")
    assert await trading_client.cancel_order_by_client_id("BTC", "abc") is True


@pytest.mark.asyncio
async def test_set_leverage_forwards_cross_flag(trading_client: ah.AsyncHyperliquid) -> None:
    response = await trading_client.set_leverage("BTC", 3, cross=False)
    assert response["status"] == "ok"
    assert trading_client.exchange.leverages[-1] == (3, "BTC", False)  # type: ignore[index]
