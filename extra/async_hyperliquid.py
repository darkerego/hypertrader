from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import random
import sys
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP
from enum import Enum
from typing import Any, AsyncIterator, Awaitable, Callable, Deque, Iterable, Literal, Sequence, TypeVar, Tuple

import dotenv
import eth_account
from eth_account.signers.local import LocalAccount
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants
from hyperliquid.utils.types import Cloid
from hyperliquid.websocket_manager import WebsocketManager

try:
    from utils.constants import INTERVAL_TO_MS
except Exception:  # pragma: no cover - defensive fallback
    INTERVAL_TO_MS = {
        "1m": 60_000,
        "3m": 180_000,
        "5m": 300_000,
        "15m": 900_000,
        "30m": 1_800_000,
        "1h": 3_600_000,
        "2h": 7_200_000,
        "4h": 14_400_000,
        "8h": 28_800_000,
        "12h": 43_200_000,
        "1d": 86_400_000,
        "3d": 259_200_000,
        "1w": 604_800_000,
        "1M": 2_592_000_000,
    }


T = TypeVar("T")

_ADDRESS_HEX_LEN = 42
_QUEUE_MAXSIZE = 2_048
_CONSUMER_QUEUE_MAXSIZE = 256
_RECENT_FILLS_MAXLEN = 2_000
_CANDLE_CACHE_MAXLEN = 2_000
_DEFAULT_WS_STALE_SECS = 20.0


class HyperliquidClientError(Exception):
    pass


class HyperliquidNotInitializedError(HyperliquidClientError):
    pass


class HyperliquidReadOnlyError(HyperliquidClientError):
    pass


class HyperliquidValidationError(HyperliquidClientError):
    pass


class HyperliquidTimeoutError(HyperliquidClientError):
    pass


class HyperliquidOrderError(HyperliquidClientError):
    pass


class HyperliquidConnectionError(HyperliquidClientError):
    pass


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    LIMIT = "limit"
    MARKET = "market"
    STOP_MARKET = "stop_market"


@dataclass(slots=True)
class OrderResult:
    success: bool
    coin: str
    side: str
    size: float
    price: float | None
    order_id: int | str | None
    client_order_id: str | None
    status: str
    raw: dict[str, Any]


@dataclass(slots=True)
class Position:
    coin: str
    size: float
    entry_price: float
    mark_price: float
    liquidation_price: float | None
    leverage: float | None
    margin_used: float
    unrealized_pnl: float
    return_on_equity: float | None
    raw: dict[str, Any]


@dataclass(slots=True)
class Candle:
    coin: str
    interval: str
    open_time_ms: int
    close_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: int | None = None
    closed: bool = False


@dataclass(slots=True)
class SubscriptionRecord:
    key: str
    subscription: dict[str, Any]
    callback: Callable[[dict[str, Any]], None]
    handler: Callable[[dict[str, Any]], Awaitable[None]]
    subscription_id: int | None = None
    ref_count: int = 0


def _now_monotonic_ms() -> int:
    return int(time.monotonic() * 1000)


def _decimal_to_float(value: Decimal) -> float:
    return float(value.normalize()) if value != 0 else 0.0


class AsyncHyperliquid:
    def __init__(
        self,
        account_address: str,
        secret_key: str | None = None,
        *,
        testnet: bool = False,
        vault_address: str | None = None,
        request_timeout: float = 10.0,
        rest_retries: int = 3,
        reconnect_min_delay: float = 1.0,
        reconnect_max_delay: float = 30.0,
        default_slippage: float = 0.01,
        logger: logging.Logger | None = None,
    ) -> None:
        self.account_address = self._normalize_address(account_address)
        self.secret_key = secret_key
        self.testnet = testnet
        self.vault_address = vault_address
        self.request_timeout = float(request_timeout)
        self.rest_retries = int(rest_retries)
        self.reconnect_min_delay = float(reconnect_min_delay)
        self.reconnect_max_delay = float(reconnect_max_delay)
        self.default_slippage = float(default_slippage)
        self.logger = logger or logging.getLogger("async_hyperliquid")
        self.base_url = constants.TESTNET_API_URL if testnet else constants.MAINNET_API_URL

        self.info: Info | None = None
        self.exchange: Exchange | None = None
        self._wallet: LocalAccount | None = None

        self._initialized = False
        self._init_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()
        self._ready_event = asyncio.Event()
        self._closing = False
        self._event_loop: asyncio.AbstractEventLoop | None = None

        self._market_lock = asyncio.Lock()
        self._account_lock = asyncio.Lock()
        self._orders_lock = asyncio.Lock()
        self._candles_lock = asyncio.Lock()
        self._coin_locks: dict[str, asyncio.Lock] = {}

        self._message_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=_QUEUE_MAXSIZE)
        self._subscriptions: dict[str, SubscriptionRecord] = {}
        self._background_tasks: set[asyncio.Task[Any]] = set()

        self._fill_handlers: set[Callable[[dict[str, Any]], Awaitable[None]]] = set()
        self._fill_waiters: dict[str, set[asyncio.Future[dict[str, Any]]]] = defaultdict(set)
        self._terminal_waiters: dict[str, set[asyncio.Future[dict[str, Any]]]] = defaultdict(set)

        self._client_order_id_to_cloid: dict[str, Cloid] = {}
        self._cloid_raw_to_client_order_id: dict[str, str] = {}
        self._order_id_to_client_order_id: dict[str, str] = {}

        self._coin_to_asset: dict[str, int] = {}
        self._name_to_coin: dict[str, str] = {}
        self._asset_to_sz_decimals: dict[int, int] = {}
        self._coin_to_sz_decimals: dict[str, int] = {}

        self._mids: dict[str, float] = {}
        self._mids_ts_ms: dict[str, int] = {}
        self._bbo: dict[str, tuple[float, float]] = {}
        self._bbo_ts_ms: dict[str, int] = {}
        self._l2_books: dict[str, dict[str, Any]] = {}
        self._l2_ts_ms: dict[str, int] = {}

        self._account_state: dict[str, Any] | None = None
        self._account_state_ts_ms: int | None = None
        self._account_value: float | None = None
        self._available_margin: float | None = None
        self._margin_used: float | None = None

        self._positions: dict[str, Position] = {}
        self._positions_ts_ms: int | None = None
        self._open_orders: dict[str, dict[str, Any]] = {}
        self._open_orders_ts_ms: int | None = None
        self._order_statuses: dict[str, dict[str, Any]] = {}
        self._recent_fills: Deque[dict[str, Any]] = deque(maxlen=_RECENT_FILLS_MAXLEN)
        self._recent_fill_keys: set[tuple[Any, ...]] = set()
        self._recent_fills_ts_ms: int | None = None

        self._candle_history: dict[tuple[str, str], Deque[Candle]] = {}
        self._current_candles: dict[tuple[str, str], Candle] = {}
        self._candle_subscribers: dict[tuple[str, str], set[asyncio.Queue[Candle]]] = defaultdict(set)

        self._last_ws_message_monotonic: float | None = None
        self._last_reconnect_monotonic: float | None = None

    async def __aenter__(self) -> AsyncHyperliquid:
        return await self.initialize()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def initialize(self) -> AsyncHyperliquid:
        if self._initialized:
            return self

        async with self._init_lock:
            if self._initialized:
                return self

            self._event_loop = asyncio.get_running_loop()
            self._closing = False
            self.info = await Info.create(self.base_url, skip_ws=False, timeout=self.request_timeout)
            if self.secret_key is not None:
                self._wallet = eth_account.Account.from_key(self.secret_key)
                self.exchange = await Exchange.create(
                    self._wallet,
                    self.base_url,
                    vault_address=self.vault_address,
                    account_address=self.account_address,
                    timeout=self.request_timeout,
                )

            await self.refresh_metadata(force=True)
            await self._reconcile_account_state()
            await self._reconcile_orders_and_fills()
            await self._subscribe_default_streams()
            self._start_background_task(self._consume_ws_messages(), "async-hl-ws-consumer")
            self._start_background_task(self._ws_health_loop(), "async-hl-ws-health")

            self._initialized = True
            self._ready_event.set()
            return self

    async def wait_until_ready(self, timeout: float | None = None) -> None:
        if timeout is None:
            await self._ready_event.wait()
            return
        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout=timeout)
        except asyncio.TimeoutError as exc:
            raise HyperliquidTimeoutError("Timed out waiting for AsyncHyperliquid initialization.") from exc

    async def close(self) -> None:
        async with self._close_lock:
            if self._closing:
                return
            self._closing = True
            self._ready_event.clear()
            self._initialized = False

            sub_keys = list(self._subscriptions)
            for key in sub_keys:
                try:
                    await self._unsubscribe(key, force=True)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.logger.exception("Subscription shutdown failed for %s", key)

            tasks = list(self._background_tasks)
            for task in tasks:
                task.cancel()
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, asyncio.CancelledError):
                        continue
                    if isinstance(result, BaseException):
                        self.logger.debug("Background task ended during shutdown: %r", result)

            self._background_tasks.clear()

            for client in (self.exchange, self.info):
                if client is None:
                    continue
                close = getattr(client, "aclose", None)
                if close is not None:
                    await close()

            self.exchange = None
            self.info = None
            self._wallet = None

    async def refresh_metadata(self, force: bool = False) -> None:
        self._require_info()
        if self._coin_to_asset and not force:
            return

        info = self.info
        assert info is not None
        await info.initialize()
        async with self._market_lock:
            self._coin_to_asset = dict(getattr(info, "coin_to_asset", {}))
            self._name_to_coin = dict(getattr(info, "name_to_coin", {}))
            self._asset_to_sz_decimals = dict(getattr(info, "asset_to_sz_decimals", {}))
            self._coin_to_sz_decimals = {}
            for coin, asset in self._coin_to_asset.items():
                decimals = self._asset_to_sz_decimals.get(int(asset))
                if decimals is not None:
                    self._coin_to_sz_decimals[str(coin).upper()] = int(decimals)

    def normalize_coin(self, coin: str) -> str:
        self._require_ready()
        coin_key = str(coin).strip()
        if not coin_key:
            raise HyperliquidValidationError("Coin must be non-empty.")
        coin_upper = coin_key.upper()
        if coin_upper in self._coin_to_asset:
            return coin_upper
        mapped = self._name_to_coin.get(coin_key) or self._name_to_coin.get(coin_upper)
        if mapped:
            return str(mapped).upper()
        return coin_upper

    def round_price(
        self,
        coin: str,
        price: float | Decimal,
        *,
        direction: Literal["nearest", "up", "down"] = "nearest",
    ) -> float:
        coin_norm = self.normalize_coin(coin)
        value = Decimal(str(price))
        if value <= 0:
            raise HyperliquidValidationError(f"Price must be positive for {coin_norm}.")

        asset = self._coin_to_asset.get(coin_norm)
        if asset is None:
            raise HyperliquidValidationError(f"Unknown coin: {coin_norm}")
        sz_decimals = self._coin_to_sz_decimals[coin_norm]
        max_decimals = 8 if asset >= 10_000 else 6
        decimals_allowed = max(0, max_decimals - sz_decimals)

        sig_value = Decimal(f"{float(value):.5g}")
        exponent = Decimal("1").scaleb(-decimals_allowed)
        rounding = {
            "nearest": ROUND_HALF_UP,
            "up": ROUND_UP,
            "down": ROUND_DOWN,
        }[direction]
        if value > Decimal("100000"):
            return float(sig_value.to_integral_value(rounding=rounding))
        return _decimal_to_float(sig_value.quantize(exponent, rounding=rounding))

    def round_size(
        self,
        coin: str,
        size: float | Decimal,
        *,
        direction: Literal["nearest", "up", "down"] = "down",
    ) -> float:
        coin_norm = self.normalize_coin(coin)
        value = Decimal(str(size))
        if value <= 0:
            raise HyperliquidValidationError("Order size must be positive.")
        decimals = self._coin_to_sz_decimals.get(coin_norm)
        if decimals is None:
            raise HyperliquidValidationError(f"Unknown coin: {coin_norm}")
        exponent = Decimal("1").scaleb(-decimals)
        rounding = {
            "nearest": ROUND_HALF_UP,
            "up": ROUND_UP,
            "down": ROUND_DOWN,
        }[direction]
        rounded = value.quantize(exponent, rounding=rounding)
        if rounded <= 0:
            raise HyperliquidValidationError(
                f"Rounded size for {coin_norm} became zero from input {size}."
            )
        return _decimal_to_float(rounded)

    async def get_mid(
        self,
        coin: str,
        *,
        max_age: float = 2.0,
        verify_rest: bool = False,
    ) -> float:
        coin_norm = self.normalize_coin(coin)
        async with self._market_lock:
            ts_ms = self._mids_ts_ms.get(coin_norm)
            cached = self._mids.get(coin_norm)
        if cached is not None and self._is_fresh(ts_ms, max_age):
            if verify_rest:
                rest = await self._fetch_mid_rest(coin_norm)
                return rest
            return cached
        self.logger.debug("REST fallback for mid %s", coin_norm)
        return await self._fetch_mid_rest(coin_norm)

    async def get_best_bid_ask(
        self,
        coin: str,
        *,
        max_age: float = 2.0,
        subscribe: bool = True,
    ) -> tuple[float, float]:
        coin_norm = self.normalize_coin(coin)
        if subscribe:
            await self._ensure_bbo_subscription(coin_norm)
        async with self._market_lock:
            ts_ms = self._bbo_ts_ms.get(coin_norm)
            cached = self._bbo.get(coin_norm)
        if cached is not None and self._is_fresh(ts_ms, max_age):
            return cached
        book = await self.get_l2_book(coin_norm, max_age=max_age, rest_fallback=True)
        levels = book.get("levels") or []
        bids = levels[0] if len(levels) > 0 else []
        asks = levels[1] if len(levels) > 1 else []
        if not bids or not asks:
            raise HyperliquidConnectionError(f"No L2 bids/asks available for {coin_norm}.")
        best_bid = float(bids[0]["px"])
        best_ask = float(asks[0]["px"])
        async with self._market_lock:
            self._bbo[coin_norm] = (best_bid, best_ask)
            self._bbo_ts_ms[coin_norm] = self._now_ms()
        return best_bid, best_ask

    async def get_l2_book(
        self,
        coin: str,
        *,
        max_age: float = 2.0,
        rest_fallback: bool = True,
    ) -> dict[str, Any]:
        coin_norm = self.normalize_coin(coin)
        async with self._market_lock:
            ts_ms = self._l2_ts_ms.get(coin_norm)
            cached = self._l2_books.get(coin_norm)
        if cached is not None and self._is_fresh(ts_ms, max_age):
            return dict(cached)
        if not rest_fallback:
            raise HyperliquidConnectionError(f"Stale L2 book for {coin_norm}.")
        self._require_info()
        info = self.info
        assert info is not None
        book = await self._rest_call("l2_snapshot", info.l2_snapshot, coin_norm)
        async with self._market_lock:
            self._l2_books[coin_norm] = dict(book)
            self._l2_ts_ms[coin_norm] = self._now_ms()
        return dict(book)

    async def _market_protection_price(
        self,
        coin: str,
        side: OrderSide,
        slippage: float,
    ) -> float:
        best_bid, best_ask = await self.get_best_bid_ask(coin)
        if side == OrderSide.BUY:
            raw = Decimal(str(best_ask)) * (Decimal("1") + Decimal(str(slippage)))
            return self.round_price(coin, raw, direction="up")
        raw = Decimal(str(best_bid)) * (Decimal("1") - Decimal(str(slippage)))
        return self.round_price(coin, raw, direction="down")

    async def place_order(
        self,
        coin: str,
        side: OrderSide | str,
        size: float | Decimal,
        *,
        order_type: OrderType | str = OrderType.LIMIT,
        price: float | Decimal | None = None,
        reduce_only: bool = False,
        post_only: bool = False,
        time_in_force: Literal["Gtc", "Ioc", "Alo"] | None = None,
        client_order_id: str | None = None,
        slippage: float | None = None,
    ) -> OrderResult:
        self._require_trading()
        coin_norm = self.normalize_coin(coin)
        order_side = self._normalize_side(side)
        order_type_norm = order_type if isinstance(order_type, OrderType) else OrderType(str(order_type))
        rounded_size = self.round_size(coin_norm, size, direction="down")
        tif = time_in_force

        if order_type_norm == OrderType.MARKET:
            if post_only:
                raise HyperliquidValidationError("Market orders cannot be post-only.")
            tif = "Ioc"
        elif order_type_norm == OrderType.LIMIT:
            if price is None:
                raise HyperliquidValidationError("Limit orders require a price.")
            if post_only:
                tif = "Alo"
            elif tif is None:
                tif = "Gtc"
        else:
            raise HyperliquidValidationError("Use place_stop_market_order for stop-market orders.")

        if tif not in {"Gtc", "Ioc", "Alo"}:
            raise HyperliquidValidationError("time_in_force must be Gtc, Ioc, or Alo.")

        limit_px: float
        if order_type_norm == OrderType.MARKET:
            limit_px = await self._market_protection_price(
                coin_norm,
                order_side,
                self.default_slippage if slippage is None else slippage,
            )
        else:
            assert price is not None
            direction = "up" if order_side == OrderSide.BUY else "down"
            limit_px = self.round_price(coin_norm, price, direction=direction if post_only else "nearest")

        if tif == "Alo":
            best_bid, best_ask = await self.get_best_bid_ask(coin_norm)
            if order_side == OrderSide.BUY and limit_px >= best_ask:
                limit_px = self.round_price(coin_norm, best_bid, direction="down")
            if order_side == OrderSide.SELL and limit_px <= best_bid:
                limit_px = self.round_price(coin_norm, best_ask, direction="up")

        cloid = self._to_cloid(client_order_id) if client_order_id else None
        order_payload = {"limit": {"tif": tif}}
        exchange = self.exchange
        assert exchange is not None

        try:
            response = await self._rest_call(
                "order",
                exchange.order,
                coin_norm,
                order_side == OrderSide.BUY,
                rounded_size,
                limit_px,
                order_payload,
                reduce_only=reduce_only,
                cloid=cloid,
                retry_writes=False,
            )
        except Exception as exc:
            raise HyperliquidOrderError(f"Order submission failed for {coin_norm}.") from exc

        result = self._parse_order_result(
            coin=coin_norm,
            side=order_side,
            size=rounded_size,
            price=limit_px,
            raw=response,
            client_order_id=client_order_id,
        )
        if result.order_id is not None and client_order_id is not None:
            self._order_id_to_client_order_id[str(result.order_id)] = client_order_id
        return result

    async def place_limit_order(self, *args: Any, **kwargs: Any) -> OrderResult:
        kwargs["order_type"] = OrderType.LIMIT
        return await self.place_order(*args, **kwargs)

    async def place_market_order(self, *args: Any, **kwargs: Any) -> OrderResult:
        kwargs["order_type"] = OrderType.MARKET
        return await self.place_order(*args, **kwargs)

    async def place_reduce_only_order(self, *args: Any, **kwargs: Any) -> OrderResult:
        kwargs["reduce_only"] = True
        return await self.place_order(*args, **kwargs)

    async def place_post_only_order(self, *args: Any, **kwargs: Any) -> OrderResult:
        kwargs["post_only"] = True
        kwargs.setdefault("time_in_force", "Alo")
        return await self.place_order(*args, **kwargs)

    async def place_stop_market_order(
        self,
        coin: str,
        side: OrderSide | str,
        size: float | Decimal,
        trigger_price: float | Decimal,
        *,
        reduce_only: bool = True,
        trigger_kind: Literal["sl", "tp"] = "sl",
        client_order_id: str | None = None,
    ) -> OrderResult:
        self._require_trading()
        coin_norm = self.normalize_coin(coin)
        order_side = self._normalize_side(side)
        rounded_size = self.round_size(coin_norm, size, direction="down")
        rounded_trigger = self.round_price(coin_norm, trigger_price)
        limit_px = await self._market_protection_price(coin_norm, order_side, self.default_slippage)
        cloid = self._to_cloid(client_order_id) if client_order_id else None
        payload = {"trigger": {"triggerPx": rounded_trigger, "isMarket": True, "tpsl": trigger_kind}}
        exchange = self.exchange
        assert exchange is not None
        response = await self._rest_call(
            "stop_market_order",
            exchange.order,
            coin_norm,
            order_side == OrderSide.BUY,
            rounded_size,
            limit_px,
            payload,
            reduce_only=reduce_only,
            cloid=cloid,
            retry_writes=False,
        )
        result = self._parse_order_result(
            coin=coin_norm,
            side=order_side,
            size=rounded_size,
            price=limit_px,
            raw=response,
            client_order_id=client_order_id,
        )
        if result.order_id is not None and client_order_id is not None:
            self._order_id_to_client_order_id[str(result.order_id)] = client_order_id
        return result

    async def cancel_order(self, coin: str, order_id: int | str) -> bool:
        self._require_trading()
        coin_norm = self.normalize_coin(coin)
        exchange = self.exchange
        assert exchange is not None
        response = await self._rest_call(
            "cancel_order",
            exchange.bulk_cancel,
            [{"coin": coin_norm, "oid": int(order_id)}],
            retry_writes=False,
        )
        success = self._cancel_response_success(response)
        if success:
            async with self._orders_lock:
                self._open_orders.pop(str(order_id), None)
                self._order_statuses[str(order_id)] = {"status": "cancelled", "oid": order_id, "coin": coin_norm}
        return success

    async def cancel_order_by_client_id(self, coin: str, client_order_id: str) -> bool:
        cloid = self._client_order_id_to_cloid.get(client_order_id)
        if cloid is None:
            orders = await self.get_open_orders(coin, refresh=False)
            for order in orders:
                if order.get("clientOrderId") == client_order_id or order.get("cloid") == cloid:
                    oid = order.get("oid")
                    if oid is not None:
                        return await self.cancel_order(coin, oid)
            return False
        exchange = self.exchange
        assert exchange is not None
        coin_norm = self.normalize_coin(coin)
        response = await self._rest_call(
            "cancel_order_by_client_id",
            exchange.bulk_cancel_by_cloid,
            [{"coin": coin_norm, "cloid": cloid}],
            retry_writes=False,
        )
        return self._cancel_response_success(response)

    async def cancel_orders(
        self,
        orders: Sequence[tuple[str, int | str]],
    ) -> dict[tuple[str, int | str], bool]:
        self._require_trading()
        grouped: dict[str, list[int]] = defaultdict(list)
        for coin, oid in orders:
            grouped[self.normalize_coin(coin)].append(int(oid))
        exchange = self.exchange
        assert exchange is not None
        results: dict[tuple[str, int | str], bool] = {}
        for coin_norm, oids in grouped.items():
            payload = [{"coin": coin_norm, "oid": oid} for oid in oids]
            response = await self._rest_call("bulk_cancel", exchange.bulk_cancel, payload, retry_writes=False)
            success = self._cancel_response_success(response)
            for oid in oids:
                results[(coin_norm, oid)] = success
                if success:
                    async with self._orders_lock:
                        self._open_orders.pop(str(oid), None)
                        self._order_statuses[str(oid)] = {"status": "cancelled", "oid": oid, "coin": coin_norm}
        return results

    async def get_open_orders(
        self,
        coin: str | None = None,
        *,
        refresh: bool = False,
    ) -> list[dict[str, Any]]:
        self._require_ready()
        if refresh or not self._open_orders:
            await self._reconcile_orders_and_fills()
        async with self._orders_lock:
            orders = [dict(v) for v in self._open_orders.values()]
        if coin is not None:
            coin_norm = self.normalize_coin(coin)
            return [order for order in orders if str(order.get("coin", "")).upper() == coin_norm]
        return orders

    async def get_order_status(
        self,
        coin: str,
        order_id: int | str,
        *,
        refresh: bool = False,
    ) -> dict[str, Any] | None:
        self._require_ready()
        order_key = str(order_id)
        async with self._orders_lock:
            cached = self._order_statuses.get(order_key)
        if cached is not None and not refresh:
            return dict(cached)
        info = self.info
        assert info is not None
        status = await self._rest_call("order_status", info.query_order_by_oid, self.account_address, int(order_id))
        if isinstance(status, dict):
            order = dict(status)
            order.setdefault("coin", self.normalize_coin(coin))
            async with self._orders_lock:
                self._order_statuses[order_key] = order
            return order
        return None

    async def enter_position_chasing(
        self,
        coin: str,
        side: OrderSide | str,
        size: float | Decimal,
        *,
        timeout: float = 30.0,
        reprice_interval: float = 0.75,
        max_reprices: int | None = None,
        price_offset_ticks: int = 1,
        cancel_on_timeout: bool = True,
        allow_partial_fill: bool = True,
        client_order_id_prefix: str | None = None,
    ) -> OrderResult:
        self._require_trading()
        coin_norm = self.normalize_coin(coin)
        order_side = self._normalize_side(side)
        async with self._coin_lock(coin_norm):
            total = self.round_size(coin_norm, size, direction="down")
            remaining = total
            filled = Decimal("0")
            active_order_id: int | str | None = None
            active_client_id: str | None = None
            reprices = 0
            deadline = time.monotonic() + timeout
            last_price: float | None = None
            last_result: OrderResult | None = None

            try:
                while remaining > 0:
                    if time.monotonic() >= deadline:
                        break
                    if max_reprices is not None and reprices > max_reprices:
                        break

                    best_bid, best_ask = await self.get_best_bid_ask(coin_norm, subscribe=True)
                    tick = Decimal("1").scaleb(-self._coin_to_sz_decimals[coin_norm])
                    if order_side == OrderSide.BUY:
                        candidate = Decimal(str(best_bid)) - (tick * price_offset_ticks)
                        passive_price = self.round_price(coin_norm, candidate, direction="down")
                    else:
                        candidate = Decimal(str(best_ask)) + (tick * price_offset_ticks)
                        passive_price = self.round_price(coin_norm, candidate, direction="up")

                    if active_order_id is not None and last_price is not None and passive_price == last_price:
                        await asyncio.sleep(reprice_interval)
                        continue

                    if active_order_id is not None:
                        status = await self.get_order_status(coin_norm, active_order_id, refresh=False)
                        if status is not None:
                            filled = max(filled, Decimal(str(self._filled_amount_for_order(active_order_id))))
                            remainder = max(0.0, total - float(filled))
                            remaining = 0.0 if remainder <= 0.0 else self.round_size(coin_norm, remainder, direction="down")
                        if remaining <= 0:
                            break
                        await self.cancel_order(coin_norm, active_order_id)
                        try:
                            await self.wait_for_order_terminal(coin_norm, active_order_id, timeout=min(2.0, reprice_interval))
                        except HyperliquidTimeoutError:
                            await self._reconcile_orders_and_fills()
                        active_order_id = None

                    active_client_id = self._new_client_order_id(client_order_id_prefix)
                    last_result = await self.place_post_only_order(
                        coin_norm,
                        order_side,
                        remaining,
                        price=passive_price,
                        reduce_only=False,
                        client_order_id=active_client_id,
                    )
                    active_order_id = last_result.order_id
                    last_price = passive_price
                    reprices += 1
                    if active_order_id is None and last_result.success:
                        filled = Decimal(str(total))
                        remaining = 0.0
                        break
                    await asyncio.sleep(reprice_interval)
                    filled = Decimal(str(self._filled_amount_for_order(active_order_id)))
                    remaining = max(0.0, total - float(filled))

                if active_order_id is not None and cancel_on_timeout and remaining > 0:
                    await self.cancel_order(coin_norm, active_order_id)

            except asyncio.CancelledError:
                if active_order_id is not None:
                    try:
                        await self.cancel_order(coin_norm, active_order_id)
                    except Exception:
                        self.logger.exception("Failed to cancel chase order during cancellation for %s", coin_norm)
                raise

            filled_size = max(0.0, total - remaining)
            if filled_size <= 0 and (last_result is None or not allow_partial_fill):
                raise HyperliquidOrderError(f"Post-only entry chase did not fill any {coin_norm}.")
            if remaining > 0 and not allow_partial_fill:
                raise HyperliquidOrderError(f"Post-only entry chase only filled {filled_size} of {total} {coin_norm}.")
            return OrderResult(
                success=filled_size > 0,
                coin=coin_norm,
                side=order_side.value,
                size=filled_size,
                price=last_price,
                order_id=active_order_id,
                client_order_id=active_client_id,
                status="filled" if remaining <= 0 else "partial_fill",
                raw=last_result.raw if last_result is not None else {},
            )

    async def close_position_market(
        self,
        coin: str,
        *,
        size: float | Decimal | None = None,
        slippage: float | None = None,
        verify_closed: bool = True,
        timeout: float = 10.0,
    ) -> OrderResult | None:
        self._require_trading()
        coin_norm = self.normalize_coin(coin)
        async with self._coin_lock(coin_norm):
            position = await self.get_position(coin_norm, refresh=True)
            if position is None:
                return None
            current_abs = abs(position.size)
            if size is None:
                close_size = current_abs
            else:
                close_size = min(current_abs, self.round_size(coin_norm, size, direction="down"))
            if close_size <= 0:
                return None
            side = OrderSide.SELL if position.size > 0 else OrderSide.BUY
            result = await self.place_market_order(
                coin_norm,
                side,
                close_size,
                reduce_only=True,
                slippage=self.default_slippage if slippage is None else slippage,
            )
            if not verify_closed:
                return result

            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                await asyncio.sleep(0.2)
                refreshed = await self.get_position(coin_norm, refresh=True)
                if refreshed is None:
                    return result
                remainder = abs(refreshed.size)
                if remainder <= 0:
                    return result
                if refreshed.size * position.size < 0:
                    raise HyperliquidOrderError(f"Close attempt reversed the position for {coin_norm}.")
                if remainder < current_abs:
                    current_abs = remainder
                    if current_abs <= 0:
                        return result
            refreshed = await self.get_position(coin_norm, refresh=True)
            if refreshed is not None and refreshed.size * position.size < 0:
                raise HyperliquidOrderError(f"Close attempt reversed the position for {coin_norm}.")
            return result

    async def get_account_state(
        self,
        *,
        refresh: bool = False,
        max_age: float = 3.0,
    ) -> dict[str, Any]:
        self._require_ready()
        async with self._account_lock:
            if not refresh and self._account_state is not None and self._is_fresh(self._account_state_ts_ms, max_age):
                return dict(self._account_state)
        await self._reconcile_account_state()
        async with self._account_lock:
            return dict(self._account_state or {})

    async def get_account_balance(
        self,
        *,
        refresh: bool = False,
    ) -> float:
        if refresh or self._account_value is None:
            await self._reconcile_account_state()
        if self._account_value is None:
            raise HyperliquidConnectionError("Account value is unavailable.")
        return self._account_value

    async def get_available_margin(
        self,
        *,
        refresh: bool = False,
    ) -> float:
        if refresh or self._available_margin is None:
            await self._reconcile_account_state()
        if self._available_margin is None:
            raise HyperliquidConnectionError("Available margin is unavailable.")
        return self._available_margin

    async def calculate_position_size(
        self,
        coin: str,
        margin_pct: float,
        *,
        leverage: float = 1.0,
        price: float | Decimal | None = None,
        reserve_margin_pct: float = 0.0,
        max_notional: float | Decimal | None = None,
        min_notional: float | Decimal | None = None,
        refresh_balance: bool = False,
    ) -> float:
        if not (0 < margin_pct <= 100):
            raise HyperliquidValidationError("margin_pct must be between 0 and 100.")
        if not (0 < leverage):
            raise HyperliquidValidationError("leverage must be > 0.")
        if not (0 <= reserve_margin_pct < 100):
            raise HyperliquidValidationError("reserve_margin_pct must be between 0 and 100.")
        coin_norm = self.normalize_coin(coin)
        available = Decimal(str(await self.get_available_margin(refresh=refresh_balance)))
        usable_margin = available * (Decimal("1") - (Decimal(str(reserve_margin_pct)) / Decimal("100")))
        allocated_margin = usable_margin * (Decimal(str(margin_pct)) / Decimal("100"))
        notional = allocated_margin * Decimal(str(leverage))
        if max_notional is not None:
            notional = min(notional, Decimal(str(max_notional)))
        if min_notional is not None and notional < Decimal(str(min_notional)):
            raise HyperliquidValidationError("Calculated notional is below min_notional.")
        if price is None:
            px = Decimal(str(await self.get_mid(coin_norm)))
        else:
            px = Decimal(str(price))
        if px <= 0:
            raise HyperliquidValidationError("Price must be positive.")
        raw_size = notional / px
        return self.round_size(coin_norm, raw_size, direction="down")

    async def set_leverage(
        self,
        coin: str,
        leverage: int,
        *,
        cross: bool = True,
    ) -> dict[str, Any]:
        self._require_trading()
        if leverage <= 0:
            raise HyperliquidValidationError("leverage must be > 0.")
        exchange = self.exchange
        assert exchange is not None
        coin_norm = self.normalize_coin(coin)
        response = await self._rest_call(
            "set_leverage",
            exchange.update_leverage,
            leverage,
            coin_norm,
            is_cross=cross,
            retry_writes=False,
        )
        return dict(response) if isinstance(response, dict) else {"raw": response}

    async def get_open_positions(
        self,
        coin: str | None = None,
        *,
        refresh: bool = False,
        max_age: float = 3.0,
    ) -> list[Position]:
        self._require_ready()
        needs_refresh = refresh or self._positions_ts_ms is None or not self._is_fresh(self._positions_ts_ms, max_age)
        if needs_refresh:
            await self._reconcile_account_state()
        async with self._account_lock:
            positions = list(self._positions.values())
        if coin is not None:
            coin_norm = self.normalize_coin(coin)
            positions = [pos for pos in positions if pos.coin == coin_norm]
        return [
            Position(
                coin=pos.coin,
                size=pos.size,
                entry_price=pos.entry_price,
                mark_price=pos.mark_price,
                liquidation_price=pos.liquidation_price,
                leverage=pos.leverage,
                margin_used=pos.margin_used,
                unrealized_pnl=pos.unrealized_pnl,
                return_on_equity=pos.return_on_equity,
                raw=dict(pos.raw),
            )
            for pos in positions
        ]

    async def get_position(
        self,
        coin: str,
        *,
        refresh: bool = False,
    ) -> Position | None:
        positions = await self.get_open_positions(coin, refresh=refresh)
        return positions[0] if positions else None

    async def get_candles(
        self,
        coin: str,
        interval: str,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        limit: int | None = None,
        refresh: bool = False,
    ) -> list[Candle]:
        self._require_ready()
        coin_norm = self.normalize_coin(coin)
        self._validate_interval(interval)
        key = (coin_norm, interval)

        if not refresh:
            async with self._candles_lock:
                cached = list(self._candle_history.get(key, deque()))
                live = self._current_candles.get(key)
            if cached and start_time_ms is None and end_time_ms is None and limit is not None and len(cached) >= limit:
                candles = cached[-limit:]
                if live is not None and (not candles or live.open_time_ms != candles[-1].open_time_ms):
                    candles.append(live)
                return candles

        interval_ms = INTERVAL_TO_MS[interval]
        now_ms = self._now_ms()
        if end_time_ms is None:
            end_time_ms = now_ms
        if start_time_ms is None:
            if limit is None:
                limit = 500
            start_time_ms = max(0, end_time_ms - (limit * interval_ms * 2))

        info = self.info
        assert info is not None
        all_rows: dict[int, Candle] = {}
        batch_start = start_time_ms
        while batch_start < end_time_ms:
            batch_end = min(end_time_ms, batch_start + (interval_ms * 5000))
            rows = await self._rest_call(
                "candles_snapshot",
                info.candles_snapshot,
                coin_norm,
                interval,
                batch_start,
                batch_end,
            )
            for row in rows or []:
                candle = self._parse_candle(row, coin=coin_norm, interval=interval)
                candle.closed = True
                all_rows[candle.open_time_ms] = candle
            if batch_end >= end_time_ms:
                break
            batch_start = batch_end + interval_ms

        candles = [all_rows[k] for k in sorted(all_rows)]
        if limit is not None:
            candles = candles[-limit:]
        async with self._candles_lock:
            self._candle_history[key] = deque(candles[-_CANDLE_CACHE_MAXLEN:], maxlen=_CANDLE_CACHE_MAXLEN)
            live = self._current_candles.get(key)
        if live is not None:
            if not candles or candles[-1].open_time_ms != live.open_time_ms:
                candles.append(live)
            else:
                candles[-1] = live
        return candles

    async def subscribe_candles(
        self,
        coin: str,
        interval: str,
        *,
        backfill: int = 0,
    ) -> asyncio.Queue[Candle]:
        coin_norm = self.normalize_coin(coin)
        self._validate_interval(interval)
        key = (coin_norm, interval)
        queue: asyncio.Queue[Candle] = asyncio.Queue(maxsize=_CONSUMER_QUEUE_MAXSIZE)
        async with self._candles_lock:
            self._candle_subscribers[key].add(queue)
        await self._subscribe(
            f"candle:{coin_norm},{interval}",
            {"type": "candle", "coin": coin_norm, "interval": interval},
            self._handle_candle_message,
        )
        if backfill > 0:
            candles = await self.get_candles(coin_norm, interval, limit=backfill)
            for candle in candles[-backfill:]:
                await self._consumer_put(queue, candle)
        return queue

    async def unsubscribe_candles(
        self,
        coin: str,
        interval: str,
        queue: asyncio.Queue[Candle] | None = None,
    ) -> None:
        coin_norm = self.normalize_coin(coin)
        key = (coin_norm, interval)
        async with self._candles_lock:
            if queue is None:
                self._candle_subscribers.pop(key, None)
            else:
                self._candle_subscribers[key].discard(queue)
                if not self._candle_subscribers[key]:
                    self._candle_subscribers.pop(key, None)
        if key not in self._candle_subscribers:
            await self._unsubscribe(f"candle:{coin_norm},{interval}")

    async def candle_stream(
        self,
        coin: str,
        interval: str,
        *,
        backfill: int = 0,
    ) -> AsyncIterator[Candle]:
        queue = await self.subscribe_candles(coin, interval, backfill=backfill)
        try:
            while True:
                yield await queue.get()
        finally:
            await self.unsubscribe_candles(coin, interval, queue)

    async def wait_for_fill(
        self,
        *,
        coin: str | None = None,
        order_id: int | str | None = None,
        client_order_id: str | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        keys = self._waiter_keys(coin=coin, order_id=order_id, client_order_id=client_order_id)
        for key in keys:
            self._fill_waiters[key].add(future)
        try:
            if timeout is None:
                return await future
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            await self._reconcile_orders_and_fills()
            raise HyperliquidTimeoutError("Timed out waiting for fill.")
        finally:
            for key in keys:
                self._fill_waiters[key].discard(future)

    async def wait_for_order_terminal(
        self,
        coin: str,
        order_id: int | str,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        order_key = self._order_waiter_key(self.normalize_coin(coin), order_id)
        self._terminal_waiters[order_key].add(future)
        try:
            if timeout is None:
                return await future
            return await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            status = await self.get_order_status(coin, order_id, refresh=True)
            if status is not None and self._is_order_terminal(str(status.get("status", ""))):
                return status
            raise HyperliquidTimeoutError("Timed out waiting for order terminal state.")
        finally:
            self._terminal_waiters[order_key].discard(future)

    def add_fill_handler(
        self,
        handler: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> Callable[[], None]:
        self._fill_handlers.add(handler)

        def remove() -> None:
            self._fill_handlers.discard(handler)

        return remove

    async def get_realized_pnl(
        self,
        *,
        coin: str | None = None,
        since_ms: int | None = None,
        refresh: bool = False,
    ) -> float:
        if refresh:
            await self._reconcile_orders_and_fills()
        fills = list(self._recent_fills)
        total = Decimal("0")
        coin_norm = self.normalize_coin(coin) if coin else None
        for fill in fills:
            if coin_norm is not None and str(fill.get("coin", "")).upper() != coin_norm:
                continue
            fill_time = self._extract_fill_time_ms(fill)
            if since_ms is not None and fill_time < since_ms:
                continue
            pnl = self._safe_float(
                fill.get("closedPnl")
                or fill.get("closedPnL")
                or fill.get("realizedPnl")
                or fill.get("realizedPnL")
            )
            fee = self._safe_float(fill.get("fee"))
            total += Decimal(str(pnl)) - Decimal(str(fee))
        return float(total)

    async def monitor_account(
        self,
        *,
        interval: float = 1.0,
        clear_screen: bool = False,
        color: bool | None = None,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        use_color = sys.stdout.isatty() if color is None else color
        while True:
            if stop_event is not None and stop_event.is_set():
                return
            state = await self.get_account_state(refresh=False, max_age=max(interval * 2, 2.0))
            positions = await self.get_open_positions(refresh=False)
            orders = await self.get_open_orders(refresh=False)
            realized = await self.get_realized_pnl(refresh=False)
            if clear_screen and sys.stdout.isatty():
                print("\033[2J\033[H", end="")
            lines = self._format_monitor_lines(
                state=state,
                positions=positions,
                orders=orders,
                realized_pnl=realized,
                color=use_color,
            )
            print("\n".join(lines))
            try:
                await asyncio.wait_for(asyncio.sleep(interval), timeout=interval + 0.1)
            except asyncio.CancelledError:
                raise

    async def build_take_profit_ladder(
        self,
        coin: str,
        targets: Sequence[tuple[float, float]],
        *,
        target_mode: Literal["price", "percent"] = "percent",
        size_mode: Literal["position_percent", "absolute"] = "position_percent",
        post_only: bool = False,
        replace_existing: bool = True,
        min_profit_pct: float = 0.0,
        client_order_id_prefix: str | None = None,
    ) -> list[OrderResult]:
        coin_norm = self.normalize_coin(coin)
        async with self._coin_lock(coin_norm):
            position = await self.get_position(coin_norm, refresh=True)
            if position is None:
                return []
            if replace_existing:
                await self.cancel_take_profit_ladder(coin_norm, client_order_id_prefix=client_order_id_prefix)
            return await self._place_tp_ladder_for_position(
                position,
                targets,
                target_mode=target_mode,
                size_mode=size_mode,
                post_only=post_only,
                min_profit_pct=min_profit_pct,
                client_order_id_prefix=client_order_id_prefix,
            )

    async def reconcile_take_profit_ladder(
        self,
        coin: str,
        desired_targets: Sequence[tuple[float, float]],
        *,
        target_mode: Literal["price", "percent"] = "percent",
        size_mode: Literal["position_percent", "absolute"] = "position_percent",
        post_only: bool = False,
        min_profit_pct: float = 0.0,
        client_order_id_prefix: str | None = None,
    ) -> list[OrderResult]:
        coin_norm = self.normalize_coin(coin)
        async with self._coin_lock(coin_norm):
            position = await self.get_position(coin_norm, refresh=True)
            if position is None:
                await self.cancel_take_profit_ladder(coin_norm, client_order_id_prefix=client_order_id_prefix)
                return []
            desired_specs = self._build_tp_order_specs(
                position,
                desired_targets,
                target_mode=target_mode,
                size_mode=size_mode,
                min_profit_pct=min_profit_pct,
            )
            existing = await self.get_open_orders(coin_norm, refresh=True)
            ladder_orders = [o for o in existing if self._belongs_to_tp_ladder(o, client_order_id_prefix)]
            keep_keys = {
                (
                    self.round_price(coin_norm, spec["price"]),
                    self.round_size(coin_norm, spec["size"], direction="down"),
                    spec["side"].value,
                )
                for spec in desired_specs
            }

            for order in ladder_orders:
                key = (
                    self._safe_float(order.get("limitPx") or order.get("px")),
                    abs(self._safe_float(order.get("sz"))),
                    "buy" if bool(order.get("isBuy")) else "sell",
                )
                if key not in keep_keys:
                    oid = order.get("oid")
                    if oid is not None:
                        await self.cancel_order(coin_norm, oid)

            existing = await self.get_open_orders(coin_norm, refresh=True)
            existing_keys = {
                (
                    self._safe_float(order.get("limitPx") or order.get("px")),
                    abs(self._safe_float(order.get("sz"))),
                    "buy" if bool(order.get("isBuy")) else "sell",
                )
                for order in existing
                if self._belongs_to_tp_ladder(order, client_order_id_prefix)
            }

            placed: list[OrderResult] = []
            for idx, spec in enumerate(desired_specs, start=1):
                key = (
                    self.round_price(coin_norm, spec["price"]),
                    self.round_size(coin_norm, spec["size"], direction="down"),
                    spec["side"].value,
                )
                if key in existing_keys:
                    continue
                placed.append(
                    await self._place_tp_order(
                        coin_norm,
                        spec["side"],
                        spec["size"],
                        spec["price"],
                        post_only=post_only,
                        client_order_id=self._new_client_order_id(client_order_id_prefix or f"tp{idx}"),
                    )
                )
            return placed

    async def cancel_take_profit_ladder(
        self,
        coin: str,
        *,
        client_order_id_prefix: str | None = None,
    ) -> int:
        coin_norm = self.normalize_coin(coin)
        existing = await self.get_open_orders(coin_norm, refresh=True)
        cancelled = 0
        for order in existing:
            if not self._belongs_to_tp_ladder(order, client_order_id_prefix):
                continue
            oid = order.get("oid")
            if oid is not None and await self.cancel_order(coin_norm, oid):
                cancelled += 1
        return cancelled

    def _require_ready(self) -> None:
        if not self._initialized or self.info is None:
            raise HyperliquidNotInitializedError("AsyncHyperliquid is not initialized.")

    def _require_trading(self) -> None:
        self._require_ready()
        if self.exchange is None:
            raise HyperliquidReadOnlyError("Trading requires a configured secret key.")

    def _require_info(self) -> None:
        if self.info is None:
            raise HyperliquidNotInitializedError("Info client is not initialized.")

    def _normalize_side(self, side: OrderSide | str) -> OrderSide:
        if isinstance(side, OrderSide):
            return side
        value = str(side).strip().lower()
        if value in {"buy", "bid", "b", "long"}:
            return OrderSide.BUY
        if value in {"sell", "ask", "s", "short"}:
            return OrderSide.SELL
        raise HyperliquidValidationError(f"Unsupported side: {side}")

    def _opposite_side(self, side: OrderSide) -> OrderSide:
        return OrderSide.SELL if side == OrderSide.BUY else OrderSide.BUY

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _new_client_order_id(self, prefix: str | None = None) -> str:
        suffix = uuid.uuid4().hex[:12]
        return f"{prefix}-{suffix}" if prefix else suffix

    def _parse_order_result(
        self,
        *,
        coin: str,
        side: OrderSide,
        size: float,
        price: float | None,
        raw: Any,
        client_order_id: str | None = None,
    ) -> OrderResult:
        response = dict(raw) if isinstance(raw, dict) else {"raw": raw}
        statuses = response.get("response", {}).get("data", {}).get("statuses", [])
        status = "unknown"
        success = False
        order_id = self._extract_order_id(response)
        if statuses:
            first = statuses[0]
            if isinstance(first, dict):
                if "resting" in first or "filled" in first:
                    success = True
                if "error" in first:
                    status = str(first["error"])
                elif "filled" in first:
                    status = "filled"
                elif "resting" in first:
                    status = "resting"
        return OrderResult(
            success=success,
            coin=coin,
            side=side.value,
            size=size,
            price=price,
            order_id=order_id,
            client_order_id=client_order_id,
            status=status,
            raw=response,
        )

    def _parse_position(self, raw_position: dict[str, Any]) -> Position | None:
        if "position" in raw_position and isinstance(raw_position["position"], dict):
            raw_position = raw_position["position"]
        coin = str(raw_position.get("coin", "")).upper()
        size = self._safe_float(raw_position.get("szi"))
        if not coin or size == 0:
            return None
        entry_price = self._safe_float(raw_position.get("entryPx"))
        mark_price = self._safe_float(raw_position.get("markPx") or raw_position.get("markPrice"))
        margin_used = self._safe_float(
            raw_position.get("marginUsed")
            or raw_position.get("margin")
            or raw_position.get("positionValue")
        )
        unrealized = self._safe_float(
            raw_position.get("unrealizedPnl")
            or raw_position.get("unrealizedPnL")
            or raw_position.get("unrealized_pnl")
        )
        liquidation = raw_position.get("liquidationPx") or raw_position.get("liquidationPrice")
        leverage = None
        leverage_value = raw_position.get("leverage")
        if isinstance(leverage_value, dict):
            leverage = self._safe_float(leverage_value.get("value"), default=0.0) or None
        elif leverage_value is not None:
            leverage = self._safe_float(leverage_value, default=0.0) or None
        roe = raw_position.get("returnOnEquity") or raw_position.get("roe")
        return Position(
            coin=coin,
            size=size,
            entry_price=entry_price,
            mark_price=mark_price,
            liquidation_price=None if liquidation is None else self._safe_float(liquidation),
            leverage=leverage,
            margin_used=margin_used,
            unrealized_pnl=unrealized if unrealized != 0 else (size * (mark_price - entry_price)),
            return_on_equity=None if roe is None else self._safe_float(roe),
            raw=dict(raw_position),
        )

    def _parse_candle(
        self,
        raw: dict[str, Any],
        *,
        coin: str | None = None,
        interval: str | None = None,
    ) -> Candle:
        candle_coin = str(raw.get("s") or coin or "").upper()
        candle_interval = str(raw.get("i") or interval or "")
        open_time_ms = int(raw.get("t") or raw.get("openTime") or 0)
        close_time_ms = int(raw.get("T") or raw.get("closeTime") or (open_time_ms + INTERVAL_TO_MS[candle_interval]))
        return Candle(
            coin=candle_coin,
            interval=candle_interval,
            open_time_ms=open_time_ms,
            close_time_ms=close_time_ms,
            open=self._safe_float(raw.get("o")),
            high=self._safe_float(raw.get("h")),
            low=self._safe_float(raw.get("l")),
            close=self._safe_float(raw.get("c")),
            volume=self._safe_float(raw.get("v")),
            trades=int(raw["n"]) if raw.get("n") is not None else None,
            closed=bool(raw.get("closed", False)),
        )

    def _extract_order_id(self, response: dict[str, Any]) -> int | str | None:
        statuses = response.get("response", {}).get("data", {}).get("statuses", [])
        for status in statuses:
            if not isinstance(status, dict):
                continue
            resting = status.get("resting")
            if isinstance(resting, dict) and resting.get("oid") is not None:
                return resting["oid"]
            filled = status.get("filled")
            if isinstance(filled, dict) and filled.get("oid") is not None:
                return filled["oid"]
        return None

    def _is_transient_error(self, exc: BaseException) -> bool:
        text = str(exc).lower()
        return any(token in text for token in ("timeout", "tempor", "connection", "429", "rate limit", "reset"))

    def _is_post_only_rejection(self, response_or_exc: Any) -> bool:
        text = str(response_or_exc).lower()
        return "post" in text and ("reject" in text or "cross" in text)

    def _is_order_terminal(self, status: str) -> bool:
        status_norm = status.strip().lower()
        return status_norm in {"filled", "cancelled", "canceled", "rejected", "expired"}

    async def _reconcile_account_state(self) -> None:
        self._require_info()
        info = self.info
        assert info is not None
        state = await self._rest_call("user_state", info.user_state, self.account_address)
        account_value = self._extract_account_value(state)
        available_margin = self._extract_available_margin(state)
        positions: dict[str, Position] = {}
        for item in state.get("assetPositions", []):
            if not isinstance(item, dict):
                continue
            position = self._parse_position(item)
            if position is not None:
                positions[position.coin] = position
        async with self._account_lock:
            self._account_state = dict(state)
            self._account_state_ts_ms = self._now_ms()
            self._account_value = account_value
            self._available_margin = available_margin
            self._margin_used = sum(pos.margin_used for pos in positions.values())
            self._positions = positions
            self._positions_ts_ms = self._account_state_ts_ms

    async def _reconcile_orders_and_fills(self) -> None:
        self._require_info()
        info = self.info
        assert info is not None
        open_orders = await self._rest_call("open_orders", info.frontend_open_orders, self.account_address)
        orders_map: dict[str, dict[str, Any]] = {}
        for order in open_orders or []:
            if not isinstance(order, dict):
                continue
            oid = order.get("oid")
            if oid is None:
                continue
            orders_map[str(oid)] = dict(order)
            orders_map[str(oid)]["coin"] = str(order.get("coin", "")).upper()
        start_time = max(0, self._now_ms() - (24 * 60 * 60 * 1000))
        fills = await self._rest_call("user_fills_by_time", info.user_fills_by_time, self.account_address, start_time)
        async with self._orders_lock:
            self._open_orders = orders_map
            self._open_orders_ts_ms = self._now_ms()
            for fill in fills or []:
                if isinstance(fill, dict):
                    self._merge_fill(fill)

    async def _rest_call(
        self,
        operation: str,
        func: Callable[..., T],
        *args: Any,
        timeout: float | None = None,
        retries: int | None = None,
        retry_writes: bool = False,
        **kwargs: Any,
    ) -> T:
        attempts = (self.rest_retries if retries is None else retries) + 1
        timeout_value = self.request_timeout if timeout is None else timeout
        last_exc: BaseException | None = None
        is_write = retry_writes or operation in {
            "order",
            "stop_market_order",
            "bulk_cancel",
            "cancel_order",
            "cancel_order_by_client_id",
            "set_leverage",
        }

        for attempt in range(1, attempts + 1):
            try:
                return await asyncio.wait_for(func(*args, **kwargs), timeout=timeout_value)
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError as exc:
                last_exc = exc
                if is_write or attempt >= attempts:
                    raise HyperliquidTimeoutError(f"{operation} timed out.") from exc
            except BaseException as exc:
                last_exc = exc
                if is_write and not retry_writes:
                    raise HyperliquidClientError(f"{operation} failed.") from exc
                if not self._is_transient_error(exc) or attempt >= attempts:
                    raise HyperliquidClientError(f"{operation} failed.") from exc
            await asyncio.sleep(min(self.reconnect_max_delay, (2 ** (attempt - 1)) * 0.25) + random.random() * 0.1)

        assert last_exc is not None
        raise HyperliquidClientError(f"{operation} failed.") from last_exc

    def _is_fresh(
        self,
        timestamp_ms: int | None,
        max_age: float,
    ) -> bool:
        if timestamp_ms is None:
            return False
        elapsed_ms = _now_monotonic_ms() - timestamp_ms
        return elapsed_ms <= int(max_age * 1000)

    async def _subscribe(
        self,
        key: str,
        subscription: dict[str, Any],
        handler: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        self._require_info()
        record = self._subscriptions.get(key)
        if record is not None:
            record.ref_count += 1
            return

        callback = self._make_ws_callback(key)
        info = self.info
        assert info is not None
        subscription_id = await info.subscribe(dict(subscription), callback)
        self._subscriptions[key] = SubscriptionRecord(
            key=key,
            subscription=dict(subscription),
            callback=callback,
            handler=handler,
            subscription_id=subscription_id,
            ref_count=1,
        )

    async def _unsubscribe(self, key: str, force: bool = False) -> None:
        record = self._subscriptions.get(key)
        if record is None:
            return
        if not force and record.ref_count > 1:
            record.ref_count -= 1
            return
        info = self.info
        if info is not None and record.subscription_id is not None:
            await info.unsubscribe(dict(record.subscription), record.subscription_id)
        self._subscriptions.pop(key, None)

    async def _restore_subscriptions(self) -> None:
        info = self.info
        if info is None:
            return
        current = list(self._subscriptions.values())
        self._subscriptions = {}
        for record in current:
            subscription_id = await info.subscribe(dict(record.subscription), record.callback)
            record.subscription_id = subscription_id
            self._subscriptions[record.key] = record

    def _coin_lock(self, coin: str) -> asyncio.Lock:
        coin_norm = str(coin).upper()
        lock = self._coin_locks.get(coin_norm)
        if lock is None:
            lock = asyncio.Lock()
            self._coin_locks[coin_norm] = lock
        return lock

    def _start_background_task(self, coro: Awaitable[Any], name: str) -> None:
        task = asyncio.create_task(coro, name=name)
        self._background_tasks.add(task)
        task.add_done_callback(self._handle_background_task_done)

    def _handle_background_task_done(self, task: asyncio.Task[Any]) -> None:
        self._background_tasks.discard(task)
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        if exc is not None:
            self.logger.exception("Background task %s failed", task.get_name(), exc_info=exc)

    async def _subscribe_default_streams(self) -> None:
        await self._subscribe("allMids", {"type": "allMids"}, self._handle_all_mids_message)
        await self._subscribe(
            "userEvents",
            {"type": "userEvents", "user": self.account_address},
            self._handle_user_events_message,
        )
        await self._subscribe(
            f"userFills:{self.account_address.lower()}",
            {"type": "userFills", "user": self.account_address},
            self._handle_user_fills_message,
        )
        await self._subscribe(
            "orderUpdates",
            {"type": "orderUpdates", "user": self.account_address},
            self._handle_order_updates_message,
        )

    async def _ensure_bbo_subscription(self, coin: str) -> None:
        await self._subscribe(
            f"bbo:{coin.lower()}",
            {"type": "bbo", "coin": coin},
            self._handle_bbo_message,
        )

    def _make_ws_callback(self, key: str) -> Callable[[dict[str, Any]], None]:
        def callback(message: dict[str, Any]) -> None:
            loop = self._event_loop
            if loop is None:
                return

            def _put() -> None:
                try:
                    self._message_queue.put_nowait({"key": key, "message": message})
                except asyncio.QueueFull:
                    if key.startswith(("allMids", "bbo:", "candle:")):
                        try:
                            self._message_queue.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                        self._message_queue.put_nowait({"key": key, "message": message})
                        return
                    self.logger.warning("Websocket queue full for %s; retaining last reliable message only.", key)
                    raise HyperliquidConnectionError(f"Websocket queue overflow for {key}")

            loop.call_soon_threadsafe(_put, )

        return callback

    async def _consume_ws_messages(self) -> None:
        while True:
            envelope = await self._message_queue.get()
            self._last_ws_message_monotonic = time.monotonic()
            key = envelope["key"]
            message = envelope["message"]
            record = self._subscriptions.get(key)
            if record is None:
                continue
            try:
                await record.handler(message)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception("Malformed websocket message for %s: %r", key, message)

    async def _ws_health_loop(self) -> None:
        delay = self.reconnect_min_delay
        while True:
            await asyncio.sleep(self.reconnect_min_delay)
            if self._closing or self.info is None:
                continue
            ws_manager = getattr(self.info, "ws_manager", None)
            stale = self._last_ws_message_monotonic is None or (
                time.monotonic() - self._last_ws_message_monotonic >= _DEFAULT_WS_STALE_SECS
            )
            disconnected = ws_manager is None or getattr(ws_manager, "ws", None) is None
            if not stale and not disconnected:
                delay = self.reconnect_min_delay
                continue
            self.logger.warning("Websocket stale/disconnected; reconnecting subscriptions.")
            try:
                await self._restart_websocket()
                delay = self.reconnect_min_delay
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger.exception("Websocket reconnect failed")
                await asyncio.sleep(min(self.reconnect_max_delay, delay + random.random()))
                delay = min(self.reconnect_max_delay, max(delay * 2, self.reconnect_min_delay))

    async def _restart_websocket(self) -> None:
        self._require_info()
        info = self.info
        assert info is not None
        if info.ws_manager is not None:
            await info.ws_manager.stop()
        info.ws_manager = WebsocketManager(info.base_url)
        await info.ws_manager.start()
        self._last_reconnect_monotonic = time.monotonic()
        await self._restore_subscriptions()
        await self._reconcile_account_state()
        await self._reconcile_orders_and_fills()

    async def _handle_all_mids_message(self, message: dict[str, Any]) -> None:
        data = message.get("data", {})
        mids_raw = data.get("mids") if isinstance(data, dict) else None
        if mids_raw is None and isinstance(data, dict):
            mids_raw = data
        if not isinstance(mids_raw, dict):
            return
        ts_ms = _now_monotonic_ms()
        async with self._market_lock:
            for coin, px in mids_raw.items():
                try:
                    coin_norm = str(coin).upper()
                    self._mids[coin_norm] = float(px)
                    self._mids_ts_ms[coin_norm] = ts_ms
                except (TypeError, ValueError):
                    continue

    async def _handle_bbo_message(self, message: dict[str, Any]) -> None:
        data = message.get("data", {})
        if not isinstance(data, dict):
            return
        coin = str(data.get("coin", "")).upper()
        raw_bbo = data.get("bbo") or []
        if len(raw_bbo) < 2:
            return
        bid_px = self._extract_level_px(raw_bbo[0])
        ask_px = self._extract_level_px(raw_bbo[1])
        if bid_px is None or ask_px is None:
            return
        async with self._market_lock:
            self._bbo[coin] = (bid_px, ask_px)
            self._bbo_ts_ms[coin] = _now_monotonic_ms()

    async def _handle_user_events_message(self, message: dict[str, Any]) -> None:
        data = message.get("data", {})
        if isinstance(data, dict):
            fills = data.get("fills")
            if isinstance(fills, list):
                for fill in fills:
                    if isinstance(fill, dict):
                        self._merge_fill(fill)

    async def _handle_user_fills_message(self, message: dict[str, Any]) -> None:
        data = message.get("data", {})
        fills = data.get("fills") if isinstance(data, dict) else None
        if isinstance(fills, list):
            for fill in fills:
                if isinstance(fill, dict):
                    self._merge_fill(fill)

    async def _handle_order_updates_message(self, message: dict[str, Any]) -> None:
        data = message.get("data", {})
        updates: Iterable[dict[str, Any]]
        if isinstance(data, list):
            updates = [u for u in data if isinstance(u, dict)]
        elif isinstance(data, dict) and isinstance(data.get("orders"), list):
            updates = [u for u in data["orders"] if isinstance(u, dict)]
        elif isinstance(data, dict):
            updates = [data]
        else:
            updates = []
        async with self._orders_lock:
            for update in updates:
                oid = update.get("oid")
                if oid is None:
                    continue
                key = str(oid)
                status = str(update.get("status") or update.get("orderStatus") or "")
                self._order_statuses[key] = dict(update)
                self._order_statuses[key].setdefault("status", status)
                if self._is_order_terminal(status):
                    self._open_orders.pop(key, None)
                else:
                    self._open_orders[key] = dict(update)
                for waiter in list(self._terminal_waiters[self._order_waiter_key(str(update.get("coin", "")).upper(), oid)]):
                    if not waiter.done() and self._is_order_terminal(status):
                        waiter.set_result(dict(update))

    async def _handle_candle_message(self, message: dict[str, Any]) -> None:
        data = message.get("data", {})
        if not isinstance(data, dict):
            return
        candle = self._parse_candle(data)
        key = (candle.coin, candle.interval)
        async with self._candles_lock:
            previous = self._current_candles.get(key)
            if previous is not None and previous.open_time_ms != candle.open_time_ms:
                previous.closed = True
                history = self._candle_history.setdefault(key, deque(maxlen=_CANDLE_CACHE_MAXLEN))
                history.append(previous)
            self._current_candles[key] = candle
            subscribers = list(self._candle_subscribers.get(key, set()))
        for queue in subscribers:
            await self._consumer_put(queue, candle)

    async def _consumer_put(self, queue: asyncio.Queue[Candle], candle: Candle) -> None:
        try:
            queue.put_nowait(candle)
        except asyncio.QueueFull:
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            queue.put_nowait(candle)

    async def _fetch_mid_rest(self, coin: str) -> float:
        info = self.info
        assert info is not None
        _mids, pending = await self._rest_call("all_mids", info.all_mids)

        value = None
        for key, px in _mids.items():
            if str(key).upper() == coin:
                value = float(px)
                break
        if value is None:
            raise HyperliquidConnectionError(f"No mid price found for {coin}.")
        async with self._market_lock:
            self._mids[coin] = value
            self._mids_ts_ms[coin] = _now_monotonic_ms()
        return value

    def _normalize_address(self, address: str) -> str:
        normalized = str(address).strip().lower()
        if len(normalized) != _ADDRESS_HEX_LEN or not normalized.startswith("0x"):
            raise HyperliquidValidationError("Account address must be a 0x-prefixed 20-byte hex string.")
        if any(ch not in "0123456789abcdef" for ch in normalized[2:]):
            raise HyperliquidValidationError("Account address contains non-hex characters.")
        return normalized

    def _extract_level_px(self, level: Any) -> float | None:
        try:
            if isinstance(level, dict):
                return float(level["px"])
            if isinstance(level, (list, tuple)) and level:
                first = level[0]
                if isinstance(first, dict):
                    return float(first["px"])
                return float(first)
        except (KeyError, TypeError, ValueError):
            return None
        return None

    def _extract_account_value(self, state: dict[str, Any]) -> float | None:
        for outer, inner in (
            ("marginSummary", "accountValue"),
            ("crossMarginSummary", "accountValue"),
            ("portfolio", "accountValue"),
        ):
            section = state.get(outer)
            if isinstance(section, dict) and section.get(inner) is not None:
                return self._safe_float(section.get(inner))
        for key in ("accountValue", "totalAccountValue"):
            if state.get(key) is not None:
                return self._safe_float(state.get(key))
        return None

    def _extract_available_margin(self, state: dict[str, Any]) -> float | None:
        for outer, inner in (
            ("marginSummary", "withdrawable"),
            ("crossMarginSummary", "withdrawable"),
            ("marginSummary", "available"),
            ("crossMarginSummary", "available"),
        ):
            section = state.get(outer)
            if isinstance(section, dict) and section.get(inner) is not None:
                return self._safe_float(section.get(inner))
        for key in ("withdrawable", "available", "availableMargin"):
            if state.get(key) is not None:
                return self._safe_float(state.get(key))
        return None

    def _to_cloid(self, client_order_id: str) -> Cloid:
        existing = self._client_order_id_to_cloid.get(client_order_id)
        if existing is not None:
            return existing
        raw = "0x" + hashlib.md5(client_order_id.encode("utf-8")).hexdigest()
        cloid = Cloid.from_str(raw)
        self._client_order_id_to_cloid[client_order_id] = cloid
        self._cloid_raw_to_client_order_id[cloid.to_raw()] = client_order_id
        return cloid

    def _cancel_response_success(self, response: Any) -> bool:
        if not isinstance(response, dict):
            return False
        statuses = response.get("response", {}).get("data", {}).get("statuses", [])
        if not statuses:
            return False
        return not any(isinstance(item, dict) and item.get("error") for item in statuses)

    def _merge_fill(self, fill: dict[str, Any]) -> None:
        fill_copy = dict(fill)
        key = (
            fill_copy.get("tid"),
            fill_copy.get("hash"),
            fill_copy.get("oid"),
            fill_copy.get("coin"),
            fill_copy.get("px"),
            fill_copy.get("sz"),
            self._extract_fill_time_ms(fill_copy),
        )
        if key in self._recent_fill_keys:
            return
        self._recent_fill_keys.add(key)
        self._recent_fills.append(fill_copy)
        while len(self._recent_fill_keys) > (self._recent_fills.maxlen or _RECENT_FILLS_MAXLEN):
            break
        self._recent_fills_ts_ms = self._now_ms()
        self._resolve_fill_waiters(fill_copy)
        for handler in list(self._fill_handlers):
            self._start_background_task(self._guard_fill_handler(handler, fill_copy), "async-hl-fill-handler")

    async def _guard_fill_handler(self, handler: Callable[[dict[str, Any]], Awaitable[None]], fill: dict[str, Any]) -> None:
        try:
            await handler(dict(fill))
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger.exception("Fill handler failed")

    def _resolve_fill_waiters(self, fill: dict[str, Any]) -> None:
        for key in self._waiter_keys(
            coin=str(fill.get("coin")) if fill.get("coin") is not None else None,
            order_id=fill.get("oid"),
            client_order_id=self._order_id_to_client_order_id.get(str(fill.get("oid"))),
        ):
            for waiter in list(self._fill_waiters[key]):
                if not waiter.done():
                    waiter.set_result(dict(fill))

    def _waiter_keys(
        self,
        *,
        coin: str | None,
        order_id: int | str | None,
        client_order_id: str | None,
    ) -> list[str]:
        keys = ["any"]
        if coin:
            keys.append(f"coin:{str(coin).upper()}")
        if order_id is not None:
            keys.append(f"oid:{order_id}")
        if client_order_id:
            keys.append(f"cloid:{client_order_id}")
        return keys

    def _order_waiter_key(self, coin: str, order_id: int | str) -> str:
        return f"terminal:{coin}:{order_id}"

    def _extract_fill_time_ms(self, fill: dict[str, Any]) -> int:
        for key in ("time", "timestamp"):
            if fill.get(key) is not None:
                try:
                    return int(float(fill[key]))
                except (TypeError, ValueError):
                    continue
        return 0

    def _filled_amount_for_order(self, order_id: int | str | None) -> float:
        if order_id is None:
            return 0.0
        oid = str(order_id)
        total = 0.0
        for fill in self._recent_fills:
            if str(fill.get("oid")) == oid:
                total += abs(self._safe_float(fill.get("sz")))
        return total

    def _validate_interval(self, interval: str) -> None:
        if interval not in INTERVAL_TO_MS:
            raise HyperliquidValidationError(f"Unsupported interval: {interval}")

    def _format_monitor_lines(
        self,
        *,
        state: dict[str, Any],
        positions: Sequence[Position],
        orders: Sequence[dict[str, Any]],
        realized_pnl: float,
        color: bool,
    ) -> list[str]:
        account_value = self._extract_account_value(state)
        available = self._extract_available_margin(state)
        margin_used = sum(position.margin_used for position in positions)
        upnl = sum(position.unrealized_pnl for position in positions)
        lines = [
            "AsyncHyperliquid Account Monitor",
            f"Account value: {account_value if account_value is not None else 'N/A'}",
            f"Available margin: {available if available is not None else 'N/A'}",
            f"Margin used: {margin_used:.8f}",
            f"Unrealized PnL: {upnl:.8f}",
            f"Realized PnL: {realized_pnl:.8f}",
            "Positions:",
        ]
        for position in positions:
            lines.append(
                f"  {position.coin} side={'LONG' if position.size > 0 else 'SHORT'} "
                f"size={position.size:.8f} entry={position.entry_price:.8f} mark={position.mark_price:.8f} "
                f"liq={position.liquidation_price if position.liquidation_price is not None else 'N/A'} "
                f"lev={position.leverage if position.leverage is not None else 'N/A'} "
                f"upnl={position.unrealized_pnl:.8f} roe={position.return_on_equity if position.return_on_equity is not None else 'N/A'}"
            )
        lines.append("Orders:")
        for order in orders:
            reduce_only = bool(order.get("reduceOnly", False))
            lines.append(
                f"  {str(order.get('coin', '')).upper()} oid={order.get('oid')} "
                f"side={'BUY' if bool(order.get('isBuy')) else 'SELL'} "
                f"size={self._safe_float(order.get('sz')):.8f} "
                f"px={self._safe_float(order.get('limitPx') or order.get('px')):.8f} "
                f"reduce_only={reduce_only}"
            )
        return lines

    def _build_tp_order_specs(
        self,
        position: Position,
        targets: Sequence[tuple[float, float]],
        *,
        target_mode: Literal["price", "percent"],
        size_mode: Literal["position_percent", "absolute"],
        min_profit_pct: float,
    ) -> list[dict[str, Any]]:
        if not targets:
            return []
        position_abs = abs(position.size)
        if position_abs <= 0:
            return []
        side = OrderSide.SELL if position.size > 0 else OrderSide.BUY
        specs: list[dict[str, Any]] = []
        remaining = Decimal(str(position_abs))
        for idx, (target, size_value) in enumerate(targets, start=1):
            if target_mode == "percent":
                target_px = (
                    Decimal(str(position.entry_price)) * (Decimal("1") + (Decimal(str(max(target, min_profit_pct))) / Decimal("100")))
                    if position.size > 0
                    else Decimal(str(position.entry_price)) * (Decimal("1") - (Decimal(str(max(target, min_profit_pct))) / Decimal("100")))
                )
            else:
                target_px = Decimal(str(target))

            if position.size > 0 and target_px <= Decimal(str(position.entry_price)):
                raise HyperliquidValidationError("Long TP price must be above entry.")
            if position.size < 0 and target_px >= Decimal(str(position.entry_price)):
                raise HyperliquidValidationError("Short TP price must be below entry.")

            if size_mode == "position_percent":
                raw_size = Decimal(str(position_abs)) * (Decimal(str(size_value)) / Decimal("100"))
            else:
                raw_size = Decimal(str(size_value))
            if idx == len(targets):
                raw_size = remaining
            rounded_size = Decimal(str(self.round_size(position.coin, raw_size, direction="down")))
            rounded_size = min(rounded_size, remaining)
            if rounded_size <= 0:
                continue
            remaining -= rounded_size
            specs.append({"side": side, "price": float(target_px), "size": float(rounded_size)})
        return specs

    async def _place_tp_ladder_for_position(
        self,
        position: Position,
        targets: Sequence[tuple[float, float]],
        *,
        target_mode: Literal["price", "percent"],
        size_mode: Literal["position_percent", "absolute"],
        post_only: bool,
        min_profit_pct: float,
        client_order_id_prefix: str | None,
    ) -> list[OrderResult]:
        specs = self._build_tp_order_specs(
            position,
            targets,
            target_mode=target_mode,
            size_mode=size_mode,
            min_profit_pct=min_profit_pct,
        )
        results: list[OrderResult] = []
        for index, spec in enumerate(specs, start=1):
            results.append(
                await self._place_tp_order(
                    position.coin,
                    spec["side"],
                    spec["size"],
                    spec["price"],
                    post_only=post_only,
                    client_order_id=self._new_client_order_id(client_order_id_prefix or f"tp{index}"),
                )
            )
        return results

    async def _place_tp_order(
        self,
        coin: str,
        side: OrderSide,
        size: float,
        price: float,
        *,
        post_only: bool,
        client_order_id: str,
    ) -> OrderResult:
        kwargs: dict[str, Any] = {
            "coin": coin,
            "side": side,
            "size": size,
            "price": price,
            "reduce_only": True,
            "client_order_id": client_order_id,
        }
        if post_only:
            return await self.place_post_only_order(**kwargs)
        return await self.place_limit_order(**kwargs)

    def _belongs_to_tp_ladder(self, order: dict[str, Any], prefix: str | None) -> bool:
        if not bool(order.get("reduceOnly", False)):
            return False
        if bool(order.get("isTrigger", False)):
            return False
        client_id = order.get("clientOrderId") or order.get("client_order_id")
        if prefix is None:
            return True
        return isinstance(client_id, str) and client_id.startswith(prefix)

    async def _place_tp_order_with_retry(
        self,
        coin: str,
        side: OrderSide,
        size: float,
        price: float,
        *,
        post_only: bool,
        client_order_id: str,
    ) -> OrderResult:
        try:
            return await self._place_tp_order(coin, side, size, price, post_only=post_only, client_order_id=client_order_id)
        except HyperliquidOrderError as exc:
            if post_only and self._is_post_only_rejection(exc):
                best_bid, best_ask = await self.get_best_bid_ask(coin)
                repriced = best_bid if side == OrderSide.BUY else best_ask
                return await self._place_tp_order(coin, side, size, repriced, post_only=True, client_order_id=client_order_id)
            raise


async def _example_main() -> None:
    dotenv.load_dotenv()
    account_address = os.environ["HYPERLIQUID_ACCOUNT_ADDRESS"]
    secret_key = os.environ.get("HYPERLIQUID_SECRET_KEY")

    async with AsyncHyperliquid(
        account_address=account_address,
        secret_key=secret_key,
        testnet=False,
    ) as client:
        while True:
            balance = await client.get_account_balance()
            available_margin = await client.get_available_margin()
            positions = await client.get_open_positions()

            print(f"Account value: {balance}")
            print(f"Available margin: {available_margin}")
            print(f"Open positions: {positions}")

            size = await client.calculate_position_size(
                "ZEC",
                margin_pct=1.0,
                leverage=1.0,
                reserve_margin_pct=10.0,
            )
            print(f"Calculated BTC size: {size}")

            candles = await client.get_candles(
                "BTC",
                "1m",
                limit=100,
            )
            print(f"Backfilled candles: {len(candles)}")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(_example_main())
