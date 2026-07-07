#!/usr/bin/env python3
"""Hyperliquid margin percentage sizing helpers.

This module fixes the common mistakes when computing "use X% of available
margin" for Hyperliquid perps:

1. Hyperliquid numeric fields are strings. Convert them with Decimal.
2. CLI percentages may arrive as 5, "5", 0.05, or "5%". Normalize once.
3. Margin dollars and position notional are different:
       margin_to_use = available_margin_usd * percent
       order_notional_usd = margin_to_use * leverage
       order_size_base = order_notional_usd / mark_price
4. For coin-specific order capacity, activeAssetData is safer because it already
   accounts for current leverage/margin limits and returns long/short capacity.

The functions are intentionally dependency-light so they can be pasted directly
into an existing bot. The optional CLI requires the official hyperliquid-python-sdk
package to be installed/importable.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, Literal, Mapping, Optional, Tuple

SideName = Literal["long", "short"]

ZERO = Decimal("0")
ONE = Decimal("1")
HUNDRED = Decimal("100")


class MarginSizingError(ValueError):
    """Raised when a margin sizing input is invalid."""


def dec(value: Any, *, field_name: str = "value") -> Decimal:
    """Convert Hyperliquid string/number fields to Decimal safely."""
    if value is None:
        raise MarginSizingError(f"{field_name} is missing")

    if isinstance(value, Decimal):
        return value

    text = str(value).strip()
    if text.endswith("%"):
        text = text[:-1].strip()

    if text == "" or text.lower() in {"nan", "none", "null"}:
        raise MarginSizingError(f"{field_name} is not a usable number: {value!r}")

    try:
        return Decimal(text)
    except InvalidOperation as exc:
        raise MarginSizingError(f"{field_name} is not a valid decimal: {value!r}") from exc


def percent_to_fraction(percent: Any) -> Decimal:
    """Normalize 5, "5", "5%", 0.05, or "0.05" into Decimal("0.05").

    Values greater than 1 are treated as human percentages.
    Examples:
        5      -> 0.05
        "5%"   -> 0.05
        0.05   -> 0.05
        "0.5"  -> 0.5
    """
    pct = dec(percent, field_name="percent")
    if pct < ZERO:
        raise MarginSizingError(f"percent must be >= 0, got {percent!r}")
    if pct > HUNDRED:
        raise MarginSizingError(f"percent must be <= 100, got {percent!r}")
    if pct > ONE:
        pct = pct / HUNDRED
    return pct


def clamp_decimal(value: Decimal, lower: Decimal = ZERO, upper: Optional[Decimal] = None) -> Decimal:
    """Clamp a Decimal to [lower, upper] when upper is provided."""
    if value < lower:
        return lower
    if upper is not None and value > upper:
        return upper
    return value


def round_down(value: Decimal, decimals: int) -> Decimal:
    """Round down to a fixed number of decimal places for base size."""
    if decimals < 0:
        raise MarginSizingError(f"decimals must be >= 0, got {decimals}")
    quantum = Decimal("1") if decimals == 0 else Decimal("1e-" + str(decimals))
    return value.quantize(quantum, rounding=ROUND_DOWN)


def get_available_margin_usd(
    user_state: Mapping[str, Any],
    *,
    prefer_withdrawable: bool = True,
    safety_buffer_usd: Any = "0",
) -> Decimal:
    """Return available cross-margin dollars from clearinghouseState.

    Prefer top-level `withdrawable` when present because that is Hyperliquid's
    directly reported available amount. If missing, fall back to:
        crossMarginSummary.accountValue - crossMarginSummary.totalMarginUsed

    `safety_buffer_usd` leaves a small amount unused to reduce marginal-order
    rejections from fees, funding, or fast price movement.
    """
    buffer = dec(safety_buffer_usd, field_name="safety_buffer_usd")
    if buffer < ZERO:
        raise MarginSizingError("safety_buffer_usd must be >= 0")

    if prefer_withdrawable and user_state.get("withdrawable") is not None:
        available = dec(user_state["withdrawable"], field_name="withdrawable")
    else:
        summary = user_state.get("crossMarginSummary") or user_state.get("marginSummary") or {}
        account_value = dec(summary.get("accountValue"), field_name="crossMarginSummary.accountValue")
        total_margin_used = dec(summary.get("totalMarginUsed"), field_name="crossMarginSummary.totalMarginUsed")
        available = account_value - total_margin_used

    return clamp_decimal(available - buffer, ZERO)


def margin_percent_usd(
    user_state: Mapping[str, Any],
    percent: Any,
    *,
    safety_buffer_usd: Any = "0",
) -> Decimal:
    """Compute margin dollars equal to percent of available margin."""
    available = get_available_margin_usd(user_state, safety_buffer_usd=safety_buffer_usd)
    return available * percent_to_fraction(percent)


def size_from_margin_percent(
    user_state: Mapping[str, Any],
    percent: Any,
    mark_price: Any,
    leverage: Any,
    *,
    sz_decimals: Optional[int] = None,
    min_base_size: Any = "0",
    safety_buffer_usd: Any = "0",
) -> Dict[str, str]:
    """Compute base order size from a percent of available margin.

    This is the manual formula:
        margin_to_use_usd = available_margin_usd * percent
        order_notional_usd = margin_to_use_usd * leverage
        order_size_base = order_notional_usd / mark_price
    """
    price = dec(mark_price, field_name="mark_price")
    lev = dec(leverage, field_name="leverage")
    min_size = dec(min_base_size, field_name="min_base_size")

    if price <= ZERO:
        raise MarginSizingError(f"mark_price must be > 0, got {mark_price!r}")
    if lev <= ZERO:
        raise MarginSizingError(f"leverage must be > 0, got {leverage!r}")

    available = get_available_margin_usd(user_state, safety_buffer_usd=safety_buffer_usd)
    pct = percent_to_fraction(percent)
    margin_to_use = available * pct
    notional = margin_to_use * lev
    base_size = notional / price

    if sz_decimals is not None:
        base_size = round_down(base_size, sz_decimals)

    if base_size < min_size:
        base_size = ZERO

    return {
        "available_margin_usd": str(available),
        "percent_fraction": str(pct),
        "margin_to_use_usd": str(margin_to_use),
        "leverage": str(lev),
        "order_notional_usd": str(notional),
        "mark_price": str(price),
        "order_size_base": str(base_size),
    }


def side_index(side: str) -> int:
    """Return Hyperliquid long/short array index."""
    normalized = side.strip().lower()
    if normalized in {"long", "buy", "b", "bid"}:
        return 0
    if normalized in {"short", "sell", "s", "ask"}:
        return 1
    raise MarginSizingError(f"side must be long/buy or short/sell, got {side!r}")


async def size_from_active_asset_data(
    active_asset_data: Mapping[str, Any],
    percent: Any,
    side: str,
    *,
    sz_decimals: Optional[int] = None,
    min_base_size: Any = "0",
) -> Dict[str, str]:
    """Compute base order size using Hyperliquid activeAssetData.

    This is usually the best sizing path for entries. Hyperliquid's
    activeAssetData response contains side-specific `availableToTrade`,
    `maxTradeSzs`, and `markPx`. The returned notional is clamped to the smaller
    of available capacity and max trade size, then multiplied by `percent`.
    """
    idx = side_index(side)
    pct = percent_to_fraction(percent)
    min_size = dec(min_base_size, field_name="min_base_size")

    available_to_trade = active_asset_data.get("availableToTrade")
    max_trade_szs = active_asset_data.get("maxTradeSzs")
    if not isinstance(available_to_trade, (list, tuple)) or len(available_to_trade) < 2:
        raise MarginSizingError("activeAssetData.availableToTrade must be a 2-item list/tuple")
    if not isinstance(max_trade_szs, (list, tuple)) or len(max_trade_szs) < 2:
        raise MarginSizingError("activeAssetData.maxTradeSzs must be a 2-item list/tuple")

    available_notional = dec(available_to_trade[idx], field_name=f"availableToTrade[{idx}]")
    max_notional = dec(max_trade_szs[idx], field_name=f"maxTradeSzs[{idx}]")
    mark_price = dec(active_asset_data.get("markPx"), field_name="markPx")

    if mark_price <= ZERO:
        raise MarginSizingError(f"markPx must be > 0, got {active_asset_data.get('markPx')!r}")

    capacity_notional = min(available_notional, max_notional)
    target_notional = clamp_decimal(capacity_notional * pct, ZERO, capacity_notional)
    base_size = target_notional / mark_price

    if sz_decimals is not None:
        base_size = round_down(base_size, sz_decimals)

    if base_size < min_size:
        base_size = ZERO

    return {
        "coin": str(active_asset_data.get("coin", "")),
        "side": "long" if idx == 0 else "short",
        "percent_fraction": str(pct),
        "available_notional_usd": str(available_notional),
        "max_trade_notional_usd": str(max_notional),
        "capacity_notional_usd": str(capacity_notional),
        "order_notional_usd": str(target_notional),
        "mark_price": str(mark_price),
        "order_size_base": str(base_size),
    }


async def fetch_active_asset_data(info: Any, user: str, coin: str, *, dex: str = "") -> Dict[str, Any]:
    """Fetch activeAssetData using hyperliquid-python-sdk's inherited post method.

    Some SDK versions expose no typed wrapper for activeAssetData, but Info.post()
    can still call the info endpoint directly.
    """
    payload: Dict[str, Any] = {"type": "activeAssetData", "user": user, "coin": coin}
    if dex:
        payload["dex"] = dex
    return await info.post("/info", payload)


async def fetch_user_state(info: Any, user: str, *, dex: str = "") -> Dict[str, Any]:
    """Fetch clearinghouseState with the SDK wrapper when available."""
    return await info.user_state(user, dex=dex)


async def cli() -> int:
    parser = argparse.ArgumentParser(description="Compute Hyperliquid order size from available margin percentage.")
    parser.add_argument("--address", required=True, help="Actual trading account address, not the API/agent wallet address.")
    parser.add_argument("--coin", required=True, help="Perp coin symbol, e.g. BTC, ETH, HYPE.")
    parser.add_argument("--side", required=True, choices=("long", "short", "buy", "sell"), help="Entry side.")
    parser.add_argument("--percent", required=True, help="Percent of available capacity to use, e.g. 5, 5%%, 0.05, or 50.")
    parser.add_argument("--testnet", action="store_true", help="Use Hyperliquid testnet API URL.")
    parser.add_argument("--dex", default="", help="Optional perp DEX name. Default is native perp DEX.")
    parser.add_argument("--sz-decimals", type=int, default=None, help="Optional base-size decimals to round down to.")
    parser.add_argument("--min-base-size", default="0", help="Optional minimum base size; returns zero below this.")
    parser.add_argument("--manual-leverage", default=None, help="Optional leverage for manual clearinghouseState calculation.")
    parser.add_argument("--manual-mark-price", default=None, help="Optional mark price for manual clearinghouseState calculation.")
    parser.add_argument("--safety-buffer-usd", default="0", help="Only used for manual mode; leaves this much margin unused.")
    args = parser.parse_args()

    try:
        from hyperliquid.info import Info
        from hyperliquid.utils import constants
    except ImportError as exc:
        raise SystemExit(
            "Could not import hyperliquid-python-sdk. Install it or run this from your bot venv. "
            "Example: pip install hyperliquid-python-sdk"
        ) from exc

    base_url = constants.TESTNET_API_URL if args.testnet else constants.MAINNET_API_URL
    info = Info(base_url, skip_ws=True)

    active = await fetch_active_asset_data(info, args.address, args.coin.upper(), dex=args.dex)
    active_result = await size_from_active_asset_data(
        active,
        args.percent,
        args.side,
        sz_decimals=args.sz_decimals,
        min_base_size=args.min_base_size,
    )

    output: Dict[str, Any] = {"active_asset_data_result": active_result}

    if args.manual_leverage is not None and args.manual_mark_price is not None:
        state = await fetch_user_state(info, args.address, dex=args.dex)
        manual_result = size_from_margin_percent(
            state,
            args.percent,
            args.manual_mark_price,
            args.manual_leverage,
            sz_decimals=args.sz_decimals,
            min_base_size=args.min_base_size,
            safety_buffer_usd=args.safety_buffer_usd,
        )
        output["manual_clearinghouse_state_result"] = manual_result

    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(cli()))
