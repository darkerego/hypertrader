import pytest

from utils.helpers import (
    get_open_orders_for_coin,
    get_position_for_coin,
    hyperliquid_market_ids_match,
    normalize_hyperliquid_market_id,
    post_active_asset_data,
)


class FakeDexInfo:
    def __init__(self) -> None:
        self.user_state_calls: list[tuple[str, str]] = []
        self.open_orders_calls: list[tuple[str, str]] = []
        self.post_calls: list[tuple[str, dict]] = []

    async def user_state(self, address: str, dex: str = "") -> dict:
        self.user_state_calls.append((address, dex))
        return {
            "assetPositions": [
                {"position": {"coin": "TESTDEX:SPY", "szi": "2", "entryPx": "620"}},
            ]
        }

    async def open_orders(self, address: str, dex: str = "") -> list[dict]:
        self.open_orders_calls.append((address, dex))
        return [
            {"coin": "TESTDEX:SPY", "oid": 1, "sz": "1"},
            {"coin": "BTC", "oid": 2, "sz": "1"},
        ]

    async def post(self, path: str, payload: dict) -> dict:
        self.post_calls.append((path, dict(payload)))
        return {"ok": True}


def test_normalize_hyperliquid_market_id_preserves_dex_format() -> None:
    assert normalize_hyperliquid_market_id(" testdex:spy ") == "TESTDEX:SPY"
    assert normalize_hyperliquid_market_id("eth") == "ETH"
    assert hyperliquid_market_ids_match("testdex:spy", "TESTDEX:SPY")


@pytest.mark.asyncio
async def test_get_position_for_coin_uses_dex_user_state() -> None:
    info = FakeDexInfo()

    position = await get_position_for_coin(info, "0xabc", "testdex:spy")

    assert position is not None
    assert info.user_state_calls == [("0xabc", "TESTDEX")]


@pytest.mark.asyncio
async def test_get_open_orders_for_coin_filters_with_dex_market_id() -> None:
    info = FakeDexInfo()

    orders = await get_open_orders_for_coin(info, "0xabc", "testdex:spy")

    assert [order["oid"] for order in orders] == [1]
    assert info.open_orders_calls == [("0xabc", "TESTDEX")]


@pytest.mark.asyncio
async def test_post_active_asset_data_sends_coin_and_dex_separately() -> None:
    info = FakeDexInfo()

    await post_active_asset_data(info, "0xabc", "testdex:spy")

    assert info.post_calls == [
        ("/info", {"type": "activeAssetData", "user": "0xabc", "coin": "SPY", "dex": "TESTDEX"})
    ]
