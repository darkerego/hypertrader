import asyncio
import decimal
import logging
import os
import time
from dataclasses import dataclass
from typing import List, Any, Tuple, Optional, Dict, Awaitable

import eth_account
from dotenv import load_dotenv
from eth_account.signers.local import LocalAccount
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants

from utils.constants import INTERVAL_TO_MS, WATCH_RETRY_SLEEP_SECONDS
from utils.worker import AsyncTaskQueue

decimal.getcontext().prec = 4


def normalize_hyperliquid_market_id(coin: str) -> str:
    """Normalize a market id, preserving optional dex-qualified perps."""
    raw = str(coin).strip()
    if not raw:
        return ""
    if ":" not in raw:
        return raw.upper()
    dex, symbol = raw.split(":", 1)
    dex = dex.strip().upper()
    symbol = symbol.strip().upper()
    return f"{dex}:{symbol}" if dex else symbol


def split_hyperliquid_market_id(coin: str) -> Tuple[str, str]:
    """Return ``(dex, symbol)`` for native or HIP-3 perp identifiers."""
    normalized = normalize_hyperliquid_market_id(coin)
    if ":" not in normalized:
        return "", normalized
    dex, symbol = normalized.split(":", 1)
    return dex, symbol


def hyperliquid_market_ids_match(left: str, right: str) -> bool:
    """Case-insensitive market-id comparison with consistent dex handling."""
    return normalize_hyperliquid_market_id(left) == normalize_hyperliquid_market_id(right)


async def _info_method_with_optional_dex(info: Info, method_name: str, *args: Any, dex: str = "") -> Any:
    """Call an Info method, tolerating SDK revisions with or without ``dex`` support."""
    method = getattr(info, method_name)
    if not dex:
        return await method(*args)
    try:
        return await method(*args, dex=dex)
    except TypeError:
        return await method(*args, dex)


async def _info_meta_with_optional_dex(info: Info, dex: str = "") -> Any:
    """Fetch market metadata, trying dex-aware SDK variants first when needed."""
    if not dex:
        return await info.meta()
    try:
        return await info.meta(dex=dex)
    except TypeError:
        return await info.meta(dex)


async def post_active_asset_data(info: Info, account_address: str, coin: str) -> Any:
    """Fetch ``activeAssetData`` for native or dex-qualified perp markets."""
    dex, symbol = split_hyperliquid_market_id(coin)
    payload: Dict[str, Any] = {"type": "activeAssetData", "user": account_address, "coin": symbol}
    if dex:
        payload["dex"] = dex
    return await info.post("/info", payload)





def parse_interval_list(intervals_value: str) -> List[str]:
    """Parse comma/space separated candle intervals."""
    intervals = [item.strip() for item in intervals_value.replace(",", " ").split() if item.strip()]
    if not intervals:
        raise RuntimeError("At least one interval must be specified.")
    unknown = [interval for interval in intervals if interval not in INTERVAL_TO_MS]
    if unknown:
        raise RuntimeError(f"Unsupported interval(s) {unknown}. Valid: {sorted(INTERVAL_TO_MS.keys())}")
    return intervals


def parse_fractional_pct(value: Any, *, field_name: str) -> float:
    """Accept 10, 10%, and 0.10 style inputs and normalize to a fraction."""
    if value is None:
        raise RuntimeError(f"{field_name} is required.")

    raw = str(value).strip()
    if not raw:
        raise RuntimeError(f"{field_name} cannot be empty.")

    had_percent = raw.endswith("%")
    if had_percent:
        raw = raw[:-1].strip()

    try:
        parsed = decimal.Decimal(raw)
    except decimal.InvalidOperation as exc:
        raise RuntimeError(f"{field_name} must be a number or percent string. Got: {value!r}") from exc

    if parsed <= 0:
        raise RuntimeError(f"{field_name} must be > 0.")

    if had_percent or parsed > decimal.Decimal("1"):
        parsed = parsed / decimal.Decimal("100")

    if parsed <= 0 or parsed > 1:
        raise RuntimeError(f"{field_name} must resolve to a fraction between 0 and 1.")

    return float(parsed)


# ---------------------------------------------------------------------------
# Credentials / clients
# ---------------------------------------------------------------------------

def load_credentials(testnet: bool =False) -> Tuple[str, str]:
    """Load Hyperliquid credentials from environment variables."""
    load_dotenv()
    if testnet:
        secret_key = os.getenv("HYPERLIQUID_TESTNET_SECRET_KEY")
        account_address = os.getenv("HYPERLIQUID_TESTNET_ACCOUNT_ADDRESS")
    else:
        secret_key = os.getenv("HYPERLIQUID_SECRET_KEY")
        account_address = os.getenv("HYPERLIQUID_ACCOUNT_ADDRESS")

    if not secret_key:
        raise RuntimeError(
            "HYPERLIQUID_SECRET_KEY is not set. Add it to your environment or .env file."
        )
    if not account_address:
        raise RuntimeError(
            "HYPERLIQUID_ACCOUNT_ADDRESS is not set. Add it to your environment or .env file."
        )

    return secret_key, account_address


# ---------------------------------------------------------------------------
# Websocket market-data cache
# ---------------------------------------------------------------------------

class HyperliquidWebsocketCache:
    """Small websocket-backed cache with HTTP fallback at call sites.

    The async SDK starts the WebsocketManager when Info is created with
    skip_ws=False. This cache subscribes to:
      * allMids       -> fast mid-price reads for monitors/trailing logic
      * bbo per coin  -> fast top-of-book reads for entry/repost/market maker logic
      * userEvents    -> cached for diagnostics / future event-driven position refresh
      * orderUpdates  -> cached for diagnostics / future order-state refresh

    There is no native websocket subscription for clearinghouseState or openOrders in
    the async SDK. For those, this class keeps REST-backed snapshots that are marked
    dirty by userEvents / userFills / orderUpdates and refreshed on demand.
    """

    def __init__(
            self,
            info: Info,
            account_address: str,
            enabled: bool = True,
            max_age_seconds: float = 5.0,
            warmup_timeout: float = 0.5,
    ) -> None:
        self.info = info
        self.account_address = account_address
        self.enabled = enabled
        self.max_age_seconds = max_age_seconds
        self.warmup_timeout = warmup_timeout

        self._mids: Dict[str, float] = {}
        self._mids_updated_at = 0.0
        self._mids_ready = asyncio.Event()
        self._mids_sub_id: Optional[int] = None

        self._bbo: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
        self._bbo_updated_at: Dict[str, float] = {}
        self._bbo_ready: Dict[str, asyncio.Event] = {}
        self._bbo_sub_ids: Dict[str, int] = {}

        self._user_events_sub_id: Optional[int] = None
        self._user_fills_sub_id: Optional[int] = None
        self._order_updates_sub_id: Optional[int] = None
        self.last_user_event: Optional[Dict[str, Any]] = None
        self.last_user_fills: Optional[Dict[str, Any]] = None
        self.last_order_update: Optional[Dict[str, Any]] = None

        self._user_state: Optional[Dict[str, Any]] = None
        self._user_state_updated_at = 0.0
        self._user_state_ready = asyncio.Event()
        self._user_state_lock = asyncio.Lock()
        self._user_state_dirty = True

        self._open_orders: List[Dict[str, Any]] = []
        self._open_orders_updated_at = 0.0
        self._open_orders_ready = asyncio.Event()
        self._open_orders_lock = asyncio.Lock()
        self._open_orders_dirty = True

        self._frontend_open_orders: List[Dict[str, Any]] = []
        self._frontend_open_orders_updated_at = 0.0
        self._frontend_open_orders_ready = asyncio.Event()
        self._frontend_open_orders_lock = asyncio.Lock()
        self._frontend_open_orders_dirty = True

        self._user_fills: List[Dict[str, Any]] = []
        self._user_fill_keys: set[Tuple[Any, ...]] = set()
        self._user_fills_updated_at = 0.0
        self._user_fills_ready = asyncio.Event()
        self._user_fills_lock = asyncio.Lock()
        self._user_fills_seed_start_ms: Optional[int] = None
        self._candle_buffers: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        self._candle_requested_periods: Dict[Tuple[str, str], int] = {}
        self._candle_updated_at: Dict[Tuple[str, str], float] = {}
        self._candle_ready: Dict[Tuple[str, str], asyncio.Event] = {}
        self._candle_sub_ids: Dict[Tuple[str, str], int] = {}
        self._candle_locks: Dict[Tuple[str, str], asyncio.Lock] = {}

    @staticmethod
    def _coin_keys(coin: str) -> Tuple[str, str]:
        raw = str(coin)
        return raw, raw.upper()

    @staticmethod
    def _is_fresh(updated_at: float, max_age_seconds: float) -> bool:
        return updated_at > 0.0 and (time.monotonic() - updated_at) <= max_age_seconds

    @staticmethod
    def _extract_level_px(level: Any) -> Optional[float]:
        if level is None:
            return None
        try:
            if isinstance(level, dict):
                px = level.get("px")
                return None if px is None else float(px)
            if isinstance(level, (list, tuple)) and level:
                first = level[0]
                if isinstance(first, dict):
                    px = first.get("px")
                    return None if px is None else float(px)
                return float(first)
        except (TypeError, ValueError):
            return None
        return None

    @staticmethod
    def _normalize_candle_key(coin: str, interval: str) -> Tuple[str, str]:
        return str(coin).upper(), str(interval)

    @staticmethod
    def _extract_candle_start_ms(candle: Dict[str, Any]) -> Optional[int]:
        try:
            return int(candle["t"])
        except (KeyError, TypeError, ValueError):
            return None

    def _merge_candles(
        self,
        key: Tuple[str, str],
        candles: List[Dict[str, Any]],
    ) -> None:
        if not candles:
            return

        merged: Dict[int, Dict[str, Any]] = {}
        for existing in self._candle_buffers.get(key, []):
            start_ms = self._extract_candle_start_ms(existing)
            if start_ms is not None:
                merged[start_ms] = existing

        for candle in candles:
            start_ms = self._extract_candle_start_ms(candle)
            if start_ms is not None:
                merged[start_ms] = dict(candle)

        keep = max(1, self._candle_requested_periods.get(key, len(merged)))
        ordered = [merged[start_ms] for start_ms in sorted(merged)]
        self._candle_buffers[key] = ordered[-keep:]
        self._candle_updated_at[key] = time.monotonic()
        self._candle_ready.setdefault(key, asyncio.Event()).set()

    async def start(self) -> None:
        """Start default websocket subscriptions. Safe to call even when disabled."""
        if not self.enabled:
            return
        if getattr(self.info, "ws_manager", None) is None:
            print("[WS] Info client has no websocket manager; websocket cache disabled.")
            self.enabled = False
            return

        try:
            self._mids_sub_id = await self.info.subscribe({"type": "allMids"}, self._on_all_mids)
            print(f"[WS] Subscribed to allMids (subscription id {self._mids_sub_id}).")
        except Exception as exc:
            print(f"[WS-WARN] allMids subscription failed; HTTP mid fallback will be used: {exc}")

        # These are cached for observability / future event-driven refreshes. The
        # authoritative position and order reconciliation still uses HTTP snapshots.
        for sub, attr_name, label, callback in (
                ({"type": "userEvents", "user": self.account_address}, "_user_events_sub_id", "userEvents",
                 self._on_user_event),
                ({"type": "userFills", "user": self.account_address}, "_user_fills_sub_id", "userFills",
                 self._on_user_fills),
                ({"type": "orderUpdates", "user": self.account_address}, "_order_updates_sub_id", "orderUpdates",
                 self._on_order_update),
        ):
            try:
                sub_id = await self.info.subscribe(dict(sub), callback)
                setattr(self, attr_name, sub_id)
                print(f"[WS] Subscribed to {label} (subscription id {sub_id}).")
            except Exception as exc:
                print(f"[WS-WARN] {label} subscription failed; continuing without it: {exc}")

    async def stop(self) -> None:
        """Best-effort unsubscribe. Closing the Info client also closes the websocket."""
        if not self.enabled or getattr(self.info, "ws_manager", None) is None:
            return

        unsubscribe_specs: List[Tuple[Dict[str, Any], Optional[int], str]] = [
            ({"type": "allMids"}, self._mids_sub_id, "allMids"),
            ({"type": "userEvents", "user": self.account_address}, self._user_events_sub_id, "userEvents"),
            ({"type": "userFills", "user": self.account_address}, self._user_fills_sub_id, "userFills"),
            ({"type": "orderUpdates", "user": self.account_address}, self._order_updates_sub_id, "orderUpdates"),
        ]
        for coin, sub_id in list(self._bbo_sub_ids.items()):
            unsubscribe_specs.append(({"type": "bbo", "coin": coin}, sub_id, f"bbo:{coin}"))
        for (coin, interval), sub_id in list(self._candle_sub_ids.items()):
            unsubscribe_specs.append(
                ({"type": "candle", "coin": coin, "interval": interval}, sub_id, f"candle:{coin},{interval}")
            )

        for subscription, sub_id, label in unsubscribe_specs:
            if sub_id is None:
                continue
            try:
                await self.info.unsubscribe(subscription, sub_id)
                print(f"[WS] Unsubscribed from {label}.")
            except Exception:
                # Shutdown should be quiet and best effort; Info.aclose closes the socket.
                pass

    def _on_all_mids(self, msg: Dict[str, Any]) -> None:
        data = msg.get("data", {}) if isinstance(msg, dict) else {}
        mids_raw: Any = data.get("mids") if isinstance(data, dict) else None
        if mids_raw is None and isinstance(data, dict):
            mids_raw = data
        if not isinstance(mids_raw, dict):
            return

        now = time.monotonic()
        for coin, px in mids_raw.items():
            try:
                value = float(px)
            except (TypeError, ValueError):
                continue
            raw_key, upper_key = self._coin_keys(str(coin))
            self._mids[raw_key] = value
            self._mids[upper_key] = value
        self._mids_updated_at = now
        self._mids_ready.set()

    def _on_bbo(self, requested_coin: str, msg: Dict[str, Any]) -> None:
        data = msg.get("data", {}) if isinstance(msg, dict) else {}
        if not isinstance(data, dict):
            return

        coin = str(data.get("coin") or requested_coin)
        raw_bbo = data.get("bbo")
        bid_px: Optional[float] = None
        ask_px: Optional[float] = None
        if isinstance(raw_bbo, (list, tuple)):
            if len(raw_bbo) > 0:
                bid_px = self._extract_level_px(raw_bbo[0])
            if len(raw_bbo) > 1:
                ask_px = self._extract_level_px(raw_bbo[1])

        raw_key, upper_key = self._coin_keys(coin)
        requested_raw, requested_upper = self._coin_keys(requested_coin)
        now = time.monotonic()
        for key in {raw_key, upper_key, requested_raw, requested_upper}:
            self._bbo[key] = (bid_px, ask_px)
            self._bbo_updated_at[key] = now
            self._bbo_ready.setdefault(key, asyncio.Event()).set()

    def _on_user_event(self, msg: Dict[str, Any]) -> None:
        self.last_user_event = msg
        data = msg.get("data", {}) if isinstance(msg, dict) else {}
        if isinstance(data, dict):
            fills = data.get("fills")
            if isinstance(fills, list) and fills:
                self._merge_user_fills(fills, replace=False)
        self._user_state_dirty = True
        self._open_orders_dirty = True
        self._frontend_open_orders_dirty = True

    def _on_user_fills(self, msg: Dict[str, Any]) -> None:
        self.last_user_fills = msg
        data = msg.get("data", {}) if isinstance(msg, dict) else {}
        if not isinstance(data, dict):
            return
        fills = data.get("fills")
        if isinstance(fills, list):
            self._merge_user_fills(fills, replace=bool(data.get("isSnapshot")))
        self._user_state_dirty = True

    def _on_order_update(self, msg: Dict[str, Any]) -> None:
        self.last_order_update = msg
        self._open_orders_dirty = True
        self._frontend_open_orders_dirty = True
        self._user_state_dirty = True

    def _on_candle(self, requested_coin: str, requested_interval: str, msg: Dict[str, Any]) -> None:
        data = msg.get("data", {}) if isinstance(msg, dict) else {}
        if not isinstance(data, dict):
            return
        interval = str(data.get("i") or requested_interval)
        if interval != requested_interval:
            return
        candle = dict(data)
        candle["s"] = str(data.get("s") or requested_coin).upper()
        candle["i"] = interval
        key = self._normalize_candle_key(requested_coin, requested_interval)
        self._merge_candles(key, [candle])

    async def get_all_mids(self) -> Optional[Dict[str, float]]:
        if not self.enabled:
            return None
        if self._is_fresh(self._mids_updated_at, self.max_age_seconds) and self._mids:
            return dict(self._mids)
        if not self._mids_ready.is_set():
            try:
                await asyncio.wait_for(self._mids_ready.wait(), timeout=self.warmup_timeout)
            except asyncio.TimeoutError:
                return None
        if self._is_fresh(self._mids_updated_at, self.max_age_seconds) and self._mids:
            return dict(self._mids)
        return None

    async def ensure_bbo_subscription(self, coin: str) -> None:
        if not self.enabled:
            return
        raw_key, upper_key = self._coin_keys(coin)
        if raw_key in self._bbo_sub_ids or upper_key in self._bbo_sub_ids:
            return
        self._bbo_ready.setdefault(raw_key, asyncio.Event())
        self._bbo_ready.setdefault(upper_key, asyncio.Event())
        try:
            sub_id = await self.info.subscribe({"type": "bbo", "coin": coin}, lambda msg, c=coin: self._on_bbo(c, msg))
            self._bbo_sub_ids[raw_key] = sub_id
            self._bbo_sub_ids[upper_key] = sub_id
            print(f"[WS] Subscribed to bbo:{coin} (subscription id {sub_id}).")
        except Exception as exc:
            print(f"[WS-WARN] bbo subscription failed for {coin}; HTTP L2 fallback will be used: {exc}")

    async def get_bbo(self, coin: str) -> Optional[Tuple[Optional[float], Optional[float]]]:
        if not self.enabled:
            return None
        await self.ensure_bbo_subscription(coin)
        raw_key, upper_key = self._coin_keys(coin)
        for key in (raw_key, upper_key):
            updated_at = self._bbo_updated_at.get(key, 0.0)
            if self._is_fresh(updated_at, self.max_age_seconds) and key in self._bbo:
                return self._bbo[key]

        event = self._bbo_ready.setdefault(upper_key, asyncio.Event())
        if not event.is_set():
            try:
                await asyncio.wait_for(event.wait(), timeout=self.warmup_timeout)
            except asyncio.TimeoutError:
                return None

        for key in (raw_key, upper_key):
            updated_at = self._bbo_updated_at.get(key, 0.0)
            if self._is_fresh(updated_at, self.max_age_seconds) and key in self._bbo:
                return self._bbo[key]
        return None

    async def ensure_candle_subscription(self, coin: str, interval: str, periods: int) -> None:
        if not self.enabled:
            return

        key = self._normalize_candle_key(coin, interval)
        self._candle_requested_periods[key] = max(periods, self._candle_requested_periods.get(key, 0))
        self._candle_ready.setdefault(key, asyncio.Event())
        lock = self._candle_locks.setdefault(key, asyncio.Lock())
        async with lock:
            if key not in self._candle_sub_ids:
                try:
                    sub_id = await self.info.subscribe(
                        {"type": "candle", "coin": coin, "interval": interval},
                        lambda msg, c=coin, i=interval: self._on_candle(c, i, msg),
                    )
                    self._candle_sub_ids[key] = sub_id
                    print(f"[WS] Subscribed to candle:{coin},{interval} (subscription id {sub_id}).")
                except Exception as exc:
                    print(
                        f"[WS-WARN] candle subscription failed for {coin} {interval}; "
                        f"HTTP candle fallback will be used: {exc}"
                    )
                    return

            if len(self._candle_buffers.get(key, [])) >= periods:
                return

            now_ms = int(time.time() * 1000)
            interval_ms = INTERVAL_TO_MS[interval]
            window_ms = interval_ms * periods * 2
            start_time = now_ms - window_ms
            end_time = now_ms
            data = await self.info.candles_snapshot(coin, interval, start_time, end_time)
            if not isinstance(data, list) or not data:
                raise RuntimeError(f"No candle data returned for {coin} {interval}.")
            self._merge_candles(key, data[-periods:])

    async def get_recent_candles(self, coin: str, interval: str, periods: int) -> Optional[List[Dict[str, Any]]]:
        if not self.enabled:
            return None
        await self.ensure_candle_subscription(coin, interval, periods)
        key = self._normalize_candle_key(coin, interval)
        candles = self._candle_buffers.get(key, [])
        if len(candles) < periods:
            return None
        return [dict(candle) for candle in candles[-periods:]]

    @staticmethod
    def _extract_fill_time_ms(fill: Dict[str, Any]) -> int:
        for key in ("time", "timestamp"):
            try:
                value = fill.get(key)
                if value is not None:
                    return int(float(value))
            except (AttributeError, TypeError, ValueError):
                continue
        return 0

    @classmethod
    def _fill_cache_key(cls, fill: Dict[str, Any]) -> Tuple[Any, ...]:
        return (
            fill.get("tid"),
            fill.get("hash"),
            fill.get("oid"),
            fill.get("coin"),
            fill.get("px"),
            fill.get("sz"),
            cls._extract_fill_time_ms(fill),
        )

    def _merge_user_fills(self, fills: List[Dict[str, Any]], replace: bool) -> None:
        if replace:
            self._user_fills = []
            self._user_fill_keys.clear()

        changed = False
        for fill in fills:
            if not isinstance(fill, dict):
                continue
            fill_copy = dict(fill)
            cache_key = self._fill_cache_key(fill_copy)
            if cache_key in self._user_fill_keys:
                continue
            self._user_fill_keys.add(cache_key)
            self._user_fills.append(fill_copy)
            changed = True

        if changed or replace:
            self._user_fills.sort(key=self._extract_fill_time_ms)
            self._user_fills_updated_at = time.monotonic()
            self._user_fills_ready.set()

    async def get_user_state(self, *, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None
        if (
            not force_refresh
            and not self._user_state_dirty
            and self._is_fresh(self._user_state_updated_at, self.max_age_seconds)
            and self._user_state is not None
        ):
            return dict(self._user_state)

        async with self._user_state_lock:
            if (
                not force_refresh
                and not self._user_state_dirty
                and self._is_fresh(self._user_state_updated_at, self.max_age_seconds)
                and self._user_state is not None
            ):
                return dict(self._user_state)

            user_state = await self.info.user_state(self.account_address)
            if not isinstance(user_state, dict):
                return None
            self._user_state = dict(user_state)
            self._user_state_updated_at = time.monotonic()
            self._user_state_dirty = False
            self._user_state_ready.set()
            return dict(self._user_state)

    async def get_open_orders(self, *, force_refresh: bool = False) -> Optional[List[Dict[str, Any]]]:
        if not self.enabled:
            return None
        if (
            not force_refresh
            and not self._open_orders_dirty
            and self._is_fresh(self._open_orders_updated_at, self.max_age_seconds)
        ):
            return [dict(order) for order in self._open_orders]

        async with self._open_orders_lock:
            if (
                not force_refresh
                and not self._open_orders_dirty
                and self._is_fresh(self._open_orders_updated_at, self.max_age_seconds)
            ):
                return [dict(order) for order in self._open_orders]

            open_orders = await self.info.open_orders(self.account_address)
            if not isinstance(open_orders, list):
                return None
            self._open_orders = [dict(order) for order in open_orders if isinstance(order, dict)]
            self._open_orders_updated_at = time.monotonic()
            self._open_orders_dirty = False
            self._open_orders_ready.set()
            return [dict(order) for order in self._open_orders]

    async def get_frontend_open_orders(self, *, force_refresh: bool = False) -> Optional[List[Dict[str, Any]]]:
        if not self.enabled:
            return None
        if (
            not force_refresh
            and not self._frontend_open_orders_dirty
            and self._is_fresh(self._frontend_open_orders_updated_at, self.max_age_seconds)
        ):
            return [dict(order) for order in self._frontend_open_orders]

        async with self._frontend_open_orders_lock:
            if (
                not force_refresh
                and not self._frontend_open_orders_dirty
                and self._is_fresh(self._frontend_open_orders_updated_at, self.max_age_seconds)
            ):
                return [dict(order) for order in self._frontend_open_orders]

            frontend_orders = await self.info.frontend_open_orders(self.account_address)
            if not isinstance(frontend_orders, list):
                return None
            self._frontend_open_orders = [dict(order) for order in frontend_orders if isinstance(order, dict)]
            self._frontend_open_orders_updated_at = time.monotonic()
            self._frontend_open_orders_dirty = False
            self._frontend_open_orders_ready.set()
            return [dict(order) for order in self._frontend_open_orders]

    async def get_user_fills_since(self, start_time_ms: int) -> Optional[List[Dict[str, Any]]]:
        if not self.enabled:
            return None

        async with self._user_fills_lock:
            need_seed = (
                self._user_fills_seed_start_ms is None
                or start_time_ms < self._user_fills_seed_start_ms
                or not self._user_fills_ready.is_set()
            )
            if need_seed:
                fills = await self.info.user_fills_by_time(self.account_address, start_time_ms)
                if not isinstance(fills, list):
                    return None
                self._user_fills_seed_start_ms = start_time_ms
                self._merge_user_fills([dict(fill) for fill in fills if isinstance(fill, dict)], replace=True)

            return [
                dict(fill)
                for fill in self._user_fills
                if self._extract_fill_time_ms(fill) >= start_time_ms
            ]


def get_ws_cache(info: Info) -> Optional[HyperliquidWebsocketCache]:
    cache = getattr(info, "ws_cache", None)
    if isinstance(cache, HyperliquidWebsocketCache):
        return cache
    return None


async def init_clients(use_testnet: bool, use_websocket: bool = True) -> Tuple[str, Info, Exchange]:
    """Initialize async Info and Exchange clients for Hyperliquid."""
    secret_key, account_address = load_credentials(use_testnet)
    api_url = constants.TESTNET_API_URL if use_testnet else constants.MAINNET_API_URL
    account: LocalAccount = eth_account.Account.from_key(secret_key)

    info = await Info.create(api_url, skip_ws=(not use_websocket))
    exchange = await Exchange.create(account, api_url, account_address=account_address)

    ws_cache = HyperliquidWebsocketCache(info, account_address, enabled=use_websocket)
    setattr(info, "ws_cache", ws_cache)
    await ws_cache.start()
    return account_address, info, exchange


async def close_clients(info: Optional[Info], exchange: Optional[Exchange]) -> None:
    """Best-effort close for async SDK HTTP/websocket sessions."""
    if info is not None:
        cache = get_ws_cache(info)
        if cache is not None:
            await cache.stop()
    for client in (exchange, info):
        if client is None:
            continue
        close = getattr(client, "aclose", None)
        if close is None:
            continue
        try:
            await close()
        except Exception as exc:
            print(f"[WARN] Failed to close async client cleanly: {exc}")


def _try_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None





# ---------------------------------------------------------------------------
# Order / precision helpers
# ---------------------------------------------------------------------------

async def get_asset_id(info: Info, coin: str) -> int:
    """Return Hyperliquid asset id for a coin/symbol name."""
    normalized_coin = normalize_hyperliquid_market_id(coin)
    try:
        return int(await info.name_to_asset(normalized_coin))  # type: ignore[attr-defined]
    except Exception as exc:
        dex, symbol = split_hyperliquid_market_id(normalized_coin)
        if dex:
            try:
                return int(await info.name_to_asset(symbol))  # type: ignore[attr-defined]
            except Exception:
                pass
        raise RuntimeError(f"Could not resolve Hyperliquid asset id for {normalized_coin}: {exc}") from exc


async def get_size_decimals(info: Info, coin: str) -> int:
    """Return allowed size decimals for the given Hyperliquid asset."""
    asset_id = await get_asset_id(info, coin)
    try:
        return int(info.asset_to_sz_decimals[asset_id])
    except Exception as exc:
        raise RuntimeError(f"Could not resolve size decimals for {coin}: {exc}") from exc


async def round_size_for_hyperliquid(info: Info, coin: str, size: float) -> float:
    """Round order size to the precision accepted by Hyperliquid."""
    if size <= 0.0:
        return 0.0
    decimals_count = await get_size_decimals(info, coin)
    rounded = round(float(size), decimals_count)
    if rounded <= 0.0:
        raise RuntimeError(
            f"Rounded size for {coin} became zero. Input size={size}, szDecimals={decimals_count}."
        )
    return rounded


async def round_price_for_hyperliquid(info: Info, coin: str, price: float) -> float:
    """Round price to Hyperliquid's accepted precision."""
    if price <= 0.0:
        raise RuntimeError(f"Price must be positive for {coin}. Got: {price}")

    asset_id = await get_asset_id(info, coin)
    sz_decimals = await get_size_decimals(info, coin)
    max_decimals = 8 if asset_id >= 10_000 else 6

    if price > 100_000:
        return float(round(price))

    decimals_allowed = max(0, max_decimals - sz_decimals)
    return round(float(f"{float(price):.5g}"), decimals_allowed)


def extract_order_error(resp: Any) -> Optional[str]:
    """Extract error string from an exchange.order response, if any."""
    try:
        if not isinstance(resp, dict):
            return None
        response = resp.get("response") or {}
        data = response.get("data") or {}
        statuses = data.get("statuses") or []
        if not statuses:
            return None
        status0 = statuses[0]
        if isinstance(status0, dict) and "error" in status0:
            return str(status0["error"])
    except Exception:
        return None
    return None






async def get_best_bid_ask(info: Info, coin: str) -> Tuple[Optional[float], Optional[float]]:
    """Return (best_bid, best_ask), preferring websocket bbo and falling back to HTTP L2."""
    cache = get_ws_cache(info)
    if cache is not None:
        cached_bbo = await cache.get_bbo(coin)
        if cached_bbo is not None:
            return cached_bbo

    try:
        snap = await info.l2_snapshot(coin)
    except Exception as exc:
        print(f"[WARN] Failed to fetch l2 snapshot for {coin}: {exc}")
        return None, None

    levels = snap.get("levels", [])
    if not isinstance(levels, list) or not levels:
        return None, None

    bids = levels[0] if len(levels) > 0 else []
    asks = levels[1] if len(levels) > 1 else []
    best_bid = None
    best_ask = None

    if bids:
        try:
            best_bid = float(bids[0]["px"])
        except (KeyError, TypeError, ValueError):
            best_bid = None
    if asks:
        try:
            best_ask = float(asks[0]["px"])
        except (KeyError, TypeError, ValueError):
            best_ask = None

    return best_bid, best_ask


async def get_open_orders_for_coin(
        info: Info,
        account_address: str,
        coin: str,
        *,
        force_http: bool = False,
) -> List[Dict[str, Any]]:
    """Return open orders for a specific coin."""
    normalized_coin = normalize_hyperliquid_market_id(coin)
    dex, _ = split_hyperliquid_market_id(normalized_coin)
    try:
        open_orders: Any
        cache = get_ws_cache(info)
        if cache is not None and not force_http and not dex:
            cached_orders = await cache.get_open_orders(force_refresh=False)
            open_orders = (
                cached_orders
                if cached_orders is not None
                else await _info_method_with_optional_dex(info, "open_orders", account_address, dex=dex)
            )
        else:
            open_orders = await _info_method_with_optional_dex(info, "open_orders", account_address, dex=dex)
    except Exception:
        logging.getLogger("hypertrader").exception(
            "[WARN] Failed to fetch open orders for %s.",
            normalized_coin,
        )
        return []

    coin_orders: List[Dict[str, Any]] = []
    for order in open_orders:
        try:
            if hyperliquid_market_ids_match(str(order.get("coin", "")), normalized_coin):
                coin_orders.append(order)
        except Exception:
            continue
    return coin_orders




def is_rate_limit_error(exc: BaseException) -> bool:
    """Return True when the SDK surfaced a 429/rate-limit style response."""
    for arg in getattr(exc, "args", ()):
        if str(arg) == '429':
            return True
        if isinstance(arg, str) and "429" in arg:
            return True
        if isinstance(arg, tuple) and arg and arg[0] == 429:
            return True
    return "429" in str(exc)


async def get_user_state_with_retry(
        info: Info,
        account_address: str,
        *,
        context_label: str,
        coin: Optional[str] = None,
        retry_sleep: float = WATCH_RETRY_SLEEP_SECONDS,
        force_http: bool = False,
) -> Dict[str, Any]:
    """Fetch user_state and keep retrying on Hyperliquid rate limits."""
    normalized_coin = normalize_hyperliquid_market_id(coin) if coin else None
    dex, _ = split_hyperliquid_market_id(normalized_coin) if normalized_coin else ("", "")
    coin_label = normalized_coin if normalized_coin else "ALL"
    attempt = 0
    while True:
        attempt += 1
        try:
            cache = get_ws_cache(info)
            if cache is not None and not force_http and not dex:
                cached_state = await cache.get_user_state(force_refresh=False)
                user_state = (
                    cached_state
                    if cached_state is not None
                    else await _info_method_with_optional_dex(info, "user_state", account_address, dex=dex)
                )
            else:
                user_state = await _info_method_with_optional_dex(info, "user_state", account_address, dex=dex)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not is_rate_limit_error(exc):
                raise
            print(
                f"[RATE-LIMIT] {context_label} coin={coin_label} attempt={attempt} "
                f"hit Hyperliquid rate limit ({exc}). Sleeping {retry_sleep:.1f}s before retry."
            )
            await asyncio.sleep(retry_sleep)
            continue

        if not isinstance(user_state, dict):
            raise RuntimeError(f"{context_label} user_state response was not a dictionary.")
        return user_state


# ---------------------------------------------------------------------------
# Position helpers
# ---------------------------------------------------------------------------

async def get_all_open_positions(
        info: Info,
        account_address: str,
        *,
        force_http: bool = False,
) -> List[Dict[str, Any]]:
    """Return a list of open perp positions for the account."""
    user_state = await get_user_state_with_retry(
        info,
        account_address,
        context_label="get_all_open_positions",
        force_http=force_http,
    )
    asset_positions = user_state.get("assetPositions", [])
    open_positions: List[Dict[str, Any]] = []

    for asset_pos in asset_positions:
        position = asset_pos.get("position", {})
        try:
            size = float(position.get("szi", "0"))
        except (TypeError, ValueError):
            continue
        if size != 0.0:
            open_positions.append(position)

    return open_positions


async def get_position_size_for_coin(
        info: Info,
        account_address: str,
        coin: str,
        *,
        force_http: bool = False,
) -> float:
    """Return signed position size for a coin, or 0.0 if flat/not found."""
    normalized_coin = normalize_hyperliquid_market_id(coin)
    user_state = await get_user_state_with_retry(
        info,
        account_address,
        context_label="get_position_size_for_coin",
        coin=normalized_coin,
        force_http=force_http,
    )
    asset_positions = user_state.get("assetPositions", [])
    for asset_pos in asset_positions:
        position = asset_pos.get("position", {})
        if hyperliquid_market_ids_match(str(position.get("coin", "")), normalized_coin):
            try:
                return float(position.get("szi", "0"))
            except (TypeError, ValueError):
                return 0.0
    return 0.0


async def get_position_for_coin(
        info: Info,
        account_address: str,
        coin: str,
        *,
        force_http: bool = False,
) -> Optional[Dict[str, Any]]:
    """Return full position dict for a coin if the account has a non-zero position."""
    normalized_coin = normalize_hyperliquid_market_id(coin)
    user_state = await get_user_state_with_retry(
        info,
        account_address,
        context_label="get_position_for_coin",
        coin=normalized_coin,
        force_http=force_http,
    )
    for asset_pos in user_state.get("assetPositions", []):
        pos = asset_pos.get("position", {})
        if not hyperliquid_market_ids_match(str(pos.get("coin", "")), normalized_coin):
            continue
        try:
            if float(pos.get("szi", "0")) != 0.0:
                return pos
        except (TypeError, ValueError):
            return None
    return None


async def get_all_mids(info: Info) -> Dict[str, float]:
    """Fetch mid prices, preferring websocket allMids and falling back to HTTP."""
    cache = get_ws_cache(info)
    if cache is not None:
        cached_mids = await cache.get_all_mids()
        if cached_mids is not None:
            return cached_mids

    mids_raw = await info.all_mids()
    mids: Dict[str, float] = {}
    for coin, px in mids_raw.items():
        try:
            value = float(px)
        except (TypeError, ValueError):
            continue
        coin_str = normalize_hyperliquid_market_id(str(coin))
        mids[coin_str] = value
        mids[coin_str.upper()] = value
    return mids


# ---------------------------------------------------------------------------
# Candle / volatility helpers
# ---------------------------------------------------------------------------

async def fetch_recent_candles(
        info: Info,
        coin: str,
        interval: str,
        periods: int,
        use_websocket_candles: bool = False,
) -> List[Dict[str, Any]]:
    """Fetch the last `periods` candles using the async SDK candleSnapshot helper."""
    if interval not in INTERVAL_TO_MS:
        raise RuntimeError(f"Unsupported interval {interval}. Valid: {sorted(INTERVAL_TO_MS.keys())}")
    if periods <= 0:
        raise RuntimeError("periods must be > 0")

    normalized_coin = normalize_hyperliquid_market_id(coin)
    if use_websocket_candles:
        cache = get_ws_cache(info)
        if cache is not None and cache.enabled:
            cached_candles = await cache.get_recent_candles(normalized_coin, interval, periods)
            if cached_candles is not None:
                return cached_candles

    now_ms = int(time.time() * 1000)
    interval_ms = INTERVAL_TO_MS[interval]
    window_ms = interval_ms * periods * 2
    start_time = now_ms - window_ms
    end_time = now_ms

    data = await info.candles_snapshot(normalized_coin, interval, start_time, end_time)
    if not isinstance(data, list) or not data:
        raise RuntimeError(f"No candle data returned for {normalized_coin} {interval}.")

    return data[-periods:]





def parse_position_snapshot(position: Dict[str, Any]) -> Tuple[str, float, float, str, float]:
    """Parse live position fields needed for management loops."""
    try:
        coin = normalize_hyperliquid_market_id(str(position["coin"]))
        signed_size = float(position["szi"])
        entry_px = float(position["entryPx"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"Could not parse position snapshot: {position}") from exc

    if signed_size == 0.0:
        raise RuntimeError(f"Position snapshot is flat and cannot be managed: {position}")

    side = "long" if signed_size > 0.0 else "short"
    return coin, signed_size, entry_px, side, abs(signed_size)


def position_is_directional_add(
        previous_signed_size: float,
        current_signed_size: float,
        size_epsilon: float = 1e-12,
) -> bool:
    """Return True when the live position increased in the same direction."""
    if previous_signed_size == 0.0 or current_signed_size == 0.0:
        return False
    if previous_signed_size * current_signed_size <= 0.0:
        return False
    return abs(current_signed_size) > abs(previous_signed_size) + size_epsilon


def compute_position_unrealized_pnl(position: Dict[str, Any], mid_price: float) -> Optional[float]:
    """Return current unrealized PnL, preferring exchange-reported fields when available."""
    for key in ("unrealizedPnl", "unrealized_pnl"):
        value = position.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            pass

    try:
        signed_size = float(position["szi"])
        entry_px = float(position["entryPx"])
    except (KeyError, TypeError, ValueError):
        return None

    return signed_size * (mid_price - entry_px)


@dataclass
class AccountRuntimeMetrics:
    """Runtime account metrics displayed by active management loops."""

    account_balance: Optional[float]
    realized_pnl: Optional[float]


def extract_closed_pnl_from_fill(fill: Dict[str, Any]) -> float:
    """Extract realized/closed PnL from a user fill response, if present."""
    for key in ("closedPnl", "closedPnL", "realizedPnl", "realizedPnL", "realized_pnl"):
        value = _try_float(fill.get(key))
        if value is not None:
            return value
    return 0.0


async def get_realized_pnl_since(
        info: Info,
        account_address: str,
        start_time_ms: Optional[int],
        coin: Optional[str] = None,
) -> Optional[float]:
    """Return closed/realized PnL since command start when available from fills."""
    if start_time_ms is None:
        return None

    try:
        fills = await get_user_fills_since(info, account_address, start_time_ms)
    except Exception:
        logging.getLogger("hypertrader").exception(
            "[METRICS] Failed to fetch user fills since %s for coin=%s.",
            start_time_ms,
            coin.upper() if coin else "ALL",
        )
        return None

    if not isinstance(fills, list):
        return None

    coin_upper = coin.upper() if coin else None
    total = 0.0
    found = False
    for fill in fills:
        if not isinstance(fill, dict):
            continue
        if coin_upper is not None and not hyperliquid_market_ids_match(str(fill.get("coin", "")), coin_upper):
            continue
        pnl = extract_closed_pnl_from_fill(fill)
        total += pnl
        if pnl != 0.0 or any(
                k in fill for k in ("closedPnl", "closedPnL", "realizedPnl", "realizedPnL", "realized_pnl")):
            found = True
    return total if found or fills else 0.0


async def get_user_fills_since(
        info: Info,
        account_address: str,
        start_time_ms: int,
        *,
        end_time_ms: Optional[int] = None,
        aggregate_by_time: bool = False,
        force_http: bool = False,
) -> Any:
    """Return fills since start_time_ms, using the websocket-backed cache when safe."""
    cache = get_ws_cache(info)
    if cache is not None and not force_http and end_time_ms is None and not aggregate_by_time:
        cached_fills = await cache.get_user_fills_since(start_time_ms)
        if cached_fills is not None:
            return cached_fills
    return await info.user_fills_by_time(
        account_address,
        start_time_ms,
        end_time_ms,
        aggregate_by_time=aggregate_by_time,
    )

def extract_account_balance_from_user_state(user_state: Dict[str, Any]) -> Optional[float]:
    """Extract current Hyperliquid account value from a clearinghouseState response."""
    candidate_paths = (
        ("marginSummary", "accountValue"),
        ("crossMarginSummary", "accountValue"),
        ("portfolio", "accountValue"),
    )
    for outer, inner in candidate_paths:
        section = user_state.get(outer)
        if isinstance(section, dict):
            value = _try_float(section.get(inner))
            if value is not None:
                return value

    for key in ("accountValue", "totalAccountValue", "balance", "withdrawable"):
        value = _try_float(user_state.get(key))
        if value is not None:
            return value
    return None


async def get_account_runtime_metrics(
        info: Info,
        account_address: str,
        start_time_ms: Optional[int],
        coin: Optional[str] = None,
) -> AccountRuntimeMetrics:
    """Fetch current balance and command-session realized PnL for display."""
    balance: Optional[float] = None
    try:
        user_state = await get_user_state_with_retry(
            info,
            account_address,
            context_label="get_account_runtime_metrics",
            coin=coin,
        )
        balance = extract_account_balance_from_user_state(user_state)
    except Exception:
        logging.getLogger("hypertrader").exception(
            "[METRICS] Failed to fetch user_state for coin=%s.",
            coin.upper() if coin else "ALL",
        )
        balance = None

    realized_pnl = await get_realized_pnl_since(info, account_address, start_time_ms, coin=coin)
    return AccountRuntimeMetrics(account_balance=balance, realized_pnl=realized_pnl)


def fmt_optional_float(value: Optional[float], decimals: int = 8) -> str:
    return f"{value:.{decimals}f}" if value is not None else "N/A"


def format_account_metrics(metrics: AccountRuntimeMetrics) -> str:
    return (
        f"rpnl={fmt_optional_float(metrics.realized_pnl)} "
        f"balance={fmt_optional_float(metrics.account_balance)}"
    )


def compute_default_stop_loss_pct(
        take_profit_pct: Optional[float],
        stop_loss_pct: Optional[float],
) -> Optional[float]:
    """Apply default SL rule: TP without SL => SL = TP * 0.5."""
    if stop_loss_pct is not None:
        return stop_loss_pct
    if take_profit_pct is not None:
        return take_profit_pct * 0.5
    return None
