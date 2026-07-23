import asyncio

import pytest

import modes.trailing_stop as trailing_stop
from modes.trailing_stop import compute_trailing_stop_px


def test_compute_trailing_stop_px_long_uses_favorable_profit_not_raw_price_pct() -> None:
    stop_px = compute_trailing_stop_px("long", entry=100.0, favorable_extreme=120.0, trail_pct=0.33)

    assert stop_px == pytest.approx(113.4)


def test_compute_trailing_stop_px_short_uses_favorable_profit_not_raw_price_pct() -> None:
    stop_px = compute_trailing_stop_px("short", entry=100.0, favorable_extreme=80.0, trail_pct=0.33)

    assert stop_px == pytest.approx(86.6)


def test_compute_trailing_stop_px_never_moves_backwards() -> None:
    stop_px = compute_trailing_stop_px("long", entry=100.0, favorable_extreme=118.0, trail_pct=0.33, current_stop=113.4)

    assert stop_px == pytest.approx(113.4)


@pytest.mark.asyncio
async def test_trailing_manager_waits_for_positions_instead_of_exiting(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    poll_count = 0
    mids_called = False

    async def fake_get_all_open_positions(_info: object, _account_address: str) -> list[dict[str, object]]:
        nonlocal poll_count
        poll_count += 1
        if poll_count < 3:
            return []
        raise KeyboardInterrupt()

    async def fake_get_all_mids(_info: object) -> dict[str, str]:
        nonlocal mids_called
        mids_called = True
        return {}

    async def fake_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(trailing_stop, "get_all_open_positions", fake_get_all_open_positions)
    monkeypatch.setattr(trailing_stop, "get_all_mids", fake_get_all_mids)
    monkeypatch.setattr(trailing_stop.asyncio, "sleep", fake_sleep)

    await trailing_stop.trailing_stop_for_all_positions(
        trail_pct=0.1,
        poll_interval=0.01,
        use_testnet=True,
        account_address="0xabc",
        info=object(),
        exchange=object(),
    )

    captured = capsys.readouterr().out
    assert poll_count == 3
    assert not mids_called
    assert captured.count("[WAIT] No open perp positions found. Waiting for positions to appear...") == 1
    assert "Trailing manager will keep running until interrupted." in captured
    assert "[!] Caught Ctrl+C, exiting without closing remaining positions." in captured


@pytest.mark.asyncio
async def test_trailing_manager_keeps_state_when_market_close_returns_nested_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    positions = [{"coin": "ZEC", "szi": "2.69", "entryPx": "547.1384"}]
    mids_sequence = iter([
        {"ZEC": "548.605"},
        {"ZEC": "548.205"},
        {"ZEC": "548.205"},
    ])

    async def fake_get_all_open_positions(_info: object, _account_address: str) -> list[dict[str, object]]:
        return positions

    async def fake_get_all_mids(_info: object) -> dict[str, str]:
        try:
            return next(mids_sequence)
        except StopIteration:
            raise KeyboardInterrupt()

    async def fake_get_account_runtime_metrics(*args: object, **kwargs: object) -> object:
        return None

    monkeypatch.setattr(trailing_stop, "get_all_open_positions", fake_get_all_open_positions)
    monkeypatch.setattr(trailing_stop, "get_all_mids", fake_get_all_mids)
    monkeypatch.setattr(trailing_stop, "get_account_runtime_metrics", fake_get_account_runtime_metrics)
    monkeypatch.setattr(trailing_stop, "compute_position_unrealized_pnl", lambda pos, price: 0.0)
    monkeypatch.setattr(trailing_stop, "format_account_metrics", lambda metrics: "metrics=N/A")
    monkeypatch.setattr(trailing_stop, "get_best_bid_ask", lambda *_args, **_kwargs: asyncio.sleep(0, result=(548.18, 548.22)))
    monkeypatch.setattr(trailing_stop, "round_price_for_hyperliquid", lambda _info, _coin, px: asyncio.sleep(0, result=px))
    monkeypatch.setattr(
        trailing_stop,
        "cancel_reduce_only_orders_for_coin",
        lambda *_args, **_kwargs: asyncio.sleep(0),
    )
    monkeypatch.setattr(trailing_stop.asyncio, "sleep", lambda _seconds: asyncio.sleep(0))

    class FakeInfo:
        async def user_state(self, _account_address: str) -> dict[str, object]:
            return {
                "assetPositions": [
                    {"position": {"coin": "ZEC", "szi": "2.69"}},
                ]
            }

    class FakeExchange:
        def __init__(self) -> None:
            self.calls = 0

        async def order(
            self,
            coin: str,
            is_buy: bool,
            sz: float,
            limit_px: float,
            order_type: dict[str, object],
            reduce_only: bool = False,
        ) -> dict[str, object]:
            self.calls += 1
            assert coin == "ZEC"
            assert is_buy is False
            assert sz == 2.69
            assert limit_px == 542.6982
            assert order_type == {"limit": {"tif": "Ioc"}}
            assert reduce_only is True
            return {
                "status": "ok",
                "response": {
                    "type": "order",
                    "data": {
                        "statuses": [
                            {"error": "Reduce only order would increase position. asset=214"},
                        ]
                    },
                },
            }

    exchange = FakeExchange()

    await trailing_stop.trailing_stop_for_all_positions(
        trail_pct=0.25,
        poll_interval=0.01,
        use_testnet=True,
        account_address="0xabc",
        info=FakeInfo(),
        exchange=exchange,
    )

    captured = capsys.readouterr().out
    assert exchange.calls == 2
    assert captured.count("[TRACK] Coin:      ZEC") == 1
    assert "Reduce only order would increase position. asset=214" in captured


@pytest.mark.asyncio
async def test_trailing_manager_cancels_reduce_only_orders_and_uses_market_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    positions_by_cycle = iter([
        [
            {"coin": "BTC", "szi": "1.0", "entryPx": "100.0"},
            {"coin": "ETH", "szi": "-2.0", "entryPx": "200.0"},
        ],
        [
            {"coin": "BTC", "szi": "1.0", "entryPx": "100.0"},
            {"coin": "ETH", "szi": "-2.0", "entryPx": "200.0"},
        ],
    ])
    mids_by_cycle = iter([
        {"BTC": "105.0", "ETH": "195.0"},
        {"BTC": "102.0", "ETH": "198.0"},
    ])
    cancel_calls: list[dict[str, object]] = []
    market_close_calls: list[dict[str, object]] = []

    class FakeInfo:
        async def user_state(self, _account_address: str) -> dict[str, object]:
            return {
                "assetPositions": [
                    {"position": {"coin": "BTC", "szi": "1.0"}},
                    {"position": {"coin": "ETH", "szi": "-2.0"}},
                ]
            }

    async def fake_get_all_open_positions(_info: object, _account_address: str) -> list[dict[str, object]]:
        try:
            return next(positions_by_cycle)
        except StopIteration:
            raise KeyboardInterrupt()

    async def fake_get_all_mids(_info: object) -> dict[str, str]:
        try:
            return next(mids_by_cycle)
        except StopIteration:
            raise KeyboardInterrupt()

    async def fake_get_account_runtime_metrics(*args: object, **kwargs: object) -> object:
        return None

    async def fake_cancel_reduce_only_orders_for_coin(
        _info: object,
        _exchange: object,
        _account_address: str,
        coin: str,
        only_tpsl: bool = False,
    ) -> None:
        cancel_calls.append({"coin": coin, "only_tpsl": only_tpsl})

    monkeypatch.setattr(trailing_stop, "get_all_open_positions", fake_get_all_open_positions)
    monkeypatch.setattr(trailing_stop, "get_all_mids", fake_get_all_mids)
    monkeypatch.setattr(trailing_stop, "get_account_runtime_metrics", fake_get_account_runtime_metrics)
    monkeypatch.setattr(trailing_stop, "compute_position_unrealized_pnl", lambda pos, price: 0.0)
    monkeypatch.setattr(trailing_stop, "format_account_metrics", lambda metrics: "metrics=N/A")
    monkeypatch.setattr(trailing_stop, "cancel_reduce_only_orders_for_coin", fake_cancel_reduce_only_orders_for_coin)
    monkeypatch.setattr(trailing_stop, "get_best_bid_ask", lambda *_args, **_kwargs: asyncio.sleep(0, result=(100.0, 101.0)))
    monkeypatch.setattr(trailing_stop, "round_price_for_hyperliquid", lambda _info, _coin, px: asyncio.sleep(0, result=px))
    monkeypatch.setattr(trailing_stop.asyncio, "sleep", lambda _seconds: asyncio.sleep(0))

    class FakeExchange:
        async def order(
            self,
            coin: str,
            is_buy: bool,
            sz: float,
            limit_px: float,
            order_type: dict[str, object],
            reduce_only: bool = False,
        ) -> dict[str, object]:
            market_close_calls.append(
                {
                    "coin": coin,
                    "is_buy": is_buy,
                    "sz": sz,
                    "limit_px": limit_px,
                    "order_type": order_type,
                    "reduce_only": reduce_only,
                }
            )
            return {"status": "ok", "response": {"type": "order", "data": {"statuses": [{"filled": {}}]}}}

    await trailing_stop.trailing_stop_for_all_positions(
        trail_pct=0.5,
        poll_interval=0.01,
        use_testnet=True,
        account_address="0xabc",
        info=FakeInfo(),
        exchange=FakeExchange(),
    )

    assert cancel_calls == [
        {"coin": "BTC", "only_tpsl": False},
        {"coin": "ETH", "only_tpsl": False},
    ]
    assert market_close_calls == [
        {
            "coin": "BTC",
            "is_buy": False,
            "sz": 1.0,
            "limit_px": 99.0,
            "order_type": {"limit": {"tif": "Ioc"}},
            "reduce_only": True,
        },
        {
            "coin": "ETH",
            "is_buy": True,
            "sz": 2.0,
            "limit_px": 102.01,
            "order_type": {"limit": {"tif": "Ioc"}},
            "reduce_only": True,
        },
    ]


@pytest.mark.asyncio
async def test_close_position_at_market_skips_when_live_position_is_flat(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cancel_calls: list[str] = []

    async def fake_cancel_reduce_only_orders_for_coin(
        _info: object,
        _exchange: object,
        _account_address: str,
        coin: str,
        only_tpsl: bool = False,
    ) -> None:
        assert only_tpsl is False
        cancel_calls.append(coin)

    class FakeInfo:
        async def user_state(self, _account_address: str) -> dict[str, object]:
            return {"assetPositions": [{"position": {"coin": "BTC", "szi": "0"}}]}

    class FakeExchange:
        def __init__(self) -> None:
            self.calls = 0

        async def order(
            self,
            coin: str,
            is_buy: bool,
            sz: float,
            limit_px: float,
            order_type: dict[str, object],
            reduce_only: bool = False,
        ) -> dict[str, object]:
            self.calls += 1
            return {
                "coin": coin,
                "is_buy": is_buy,
                "sz": sz,
                "limit_px": limit_px,
                "order_type": order_type,
                "reduce_only": reduce_only,
            }

    monkeypatch.setattr(trailing_stop, "cancel_reduce_only_orders_for_coin", fake_cancel_reduce_only_orders_for_coin)
    monkeypatch.setattr(trailing_stop, "get_best_bid_ask", lambda *_args, **_kwargs: asyncio.sleep(0, result=(100.0, 101.0)))
    monkeypatch.setattr(trailing_stop, "round_price_for_hyperliquid", lambda _info, _coin, px: asyncio.sleep(0, result=px))

    exchange = FakeExchange()
    result = await trailing_stop.close_position_at_market(
        FakeInfo(),
        exchange,
        "0xabc",
        "BTC",
    )

    captured = capsys.readouterr().out
    assert cancel_calls == ["BTC"]
    assert exchange.calls == 0
    assert result is None
    assert "[BTC] Position is already flat; skipping reduce-only close." in captured


@pytest.mark.asyncio
async def test_close_position_at_market_places_explicit_reduce_only_exit_order(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cancel_calls: list[str] = []

    async def fake_cancel_reduce_only_orders_for_coin(
        _info: object,
        _exchange: object,
        _account_address: str,
        coin: str,
        only_tpsl: bool = False,
    ) -> None:
        assert only_tpsl is False
        cancel_calls.append(coin)

    class FakeInfo:
        async def user_state(self, _account_address: str) -> dict[str, object]:
            return {"assetPositions": [{"position": {"coin": "BTC", "szi": "1.0"}}]}

    class FakeExchange:
        def __init__(self) -> None:
            self.calls = 0

        async def order(
            self,
            coin: str,
            is_buy: bool,
            sz: float,
            limit_px: float,
            order_type: dict[str, object],
            reduce_only: bool = False,
        ) -> dict[str, object]:
            self.calls += 1
            return {
                "coin": coin,
                "is_buy": is_buy,
                "sz": sz,
                "limit_px": limit_px,
                "order_type": order_type,
                "reduce_only": reduce_only,
            }

    monkeypatch.setattr(trailing_stop, "cancel_reduce_only_orders_for_coin", fake_cancel_reduce_only_orders_for_coin)
    monkeypatch.setattr(trailing_stop, "get_best_bid_ask", lambda *_args, **_kwargs: asyncio.sleep(0, result=(100.0, 101.0)))
    monkeypatch.setattr(trailing_stop, "round_price_for_hyperliquid", lambda _info, _coin, px: asyncio.sleep(0, result=px))

    exchange = FakeExchange()
    result = await trailing_stop.close_position_at_market(
        FakeInfo(),
        exchange,
        "0xabc",
        "BTC",
    )

    captured = capsys.readouterr().out
    assert cancel_calls == ["BTC"]
    assert exchange.calls == 1
    assert result == {
        "coin": "BTC",
        "is_buy": False,
        "sz": 1.0,
        "limit_px": 99.0,
        "order_type": {"limit": {"tif": "Ioc"}},
        "reduce_only": True,
    }
    assert "[BTC] Closing LONG position: size=1.0, side=SELL" in captured
