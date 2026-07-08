import asyncio
import logging
import time
from typing import Optional, List, Dict, Any, Tuple

from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

from utils.helpers import compute_default_stop_loss_pct, get_open_orders_for_coin
from utils.constants import WATCH_RETRY_SLEEP_SECONDS, PRICE_EPS
from utils.helpers import init_clients, get_position_for_coin, \
    parse_position_snapshot, get_all_mids, compute_position_unrealized_pnl, get_account_runtime_metrics, \
    position_is_directional_add, \
    fmt_optional_float, format_account_metrics, is_rate_limit_error, \
    round_size_for_hyperliquid, get_position_size_for_coin, AccountRuntimeMetrics, get_best_bid_ask, \
    round_price_for_hyperliquid, extract_order_error, close_clients, fetch_recent_candles

try:
    import numpy as np
    import talib
except Exception:
    np = None  # type: ignore[assignment]
    talib = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Bracket entry / TP / SL
# ---------------------------------------------------------------------------

def extract_resting_oids(resp: Any) -> List[int]:
    """Return resting order ids from an exchange.order/bulk_orders response."""
    oids: List[int] = []
    try:
        statuses = resp.get("response", {}).get("data", {}).get("statuses", [])
        for status in statuses:
            resting = status.get("resting") if isinstance(status, dict) else None
            if isinstance(resting, dict) and isinstance(resting.get("oid"), int):
                oids.append(int(resting["oid"]))
    except Exception:
        return oids
    return oids

def extract_filled_qty(resp: Any) -> float:
    """Return filled quantity from an exchange.order/bulk_orders response if present."""
    total = 0.0
    try:
        statuses = resp.get("response", {}).get("data", {}).get("statuses", [])
        for status in statuses:
            filled = status.get("filled") if isinstance(status, dict) else None
            if isinstance(filled, dict):
                total += float(filled.get("totalSz", "0"))
    except Exception:
        return total
    return total

async def get_frontend_open_orders_for_coin(
        info: Info,
        account_address: str,
        coin: str,
) -> List[Dict[str, Any]]:
    """Return frontend open orders for a coin, including reduceOnly/isTrigger fields."""
    try:
        orders = await info.frontend_open_orders(account_address)
    except Exception:
        logging.getLogger("hypertrader").exception(
            "[WARN] Failed to fetch frontend open orders for %s.",
            coin,
        )
        return []

    out: List[Dict[str, Any]] = []
    for order in orders:
        try:
            if str(order.get("coin", "")).lower() == coin.lower():
                out.append(order)
        except Exception:
            continue
    return out


def clamp_tp_reversal_trigger_px(side: str, entry_px: float, candidate_px: float) -> float:
    """Clamp TP-reversal triggers so they never cross the average entry.

    For a long, the TP-reversal trigger is never allowed below entry.
    For a short, the TP-reversal trigger is never allowed above entry.
    """
    if side == "long":
        return max(float(entry_px), float(candidate_px))
    if side == "short":
        return min(float(entry_px), float(candidate_px))
    raise RuntimeError(f"Invalid side: {side}")

def compute_tp_reversal_trigger_px(side: str, entry_px: float, favorable_extreme: float, reversal_pct: float) -> float:
    """Return the TP-reversal trigger using favorable-extreme logic.

    The reversal threshold is a percentage move back from the best favorable
    price reached after the first TP level was touched. The result is always
    entry-clamped: long triggers cannot be below entry, and short triggers
    cannot be above entry. This prevents TP reversal from turning into a loss
    exit level merely because the configured reversal percentage is larger than
    the current unrealized gain.
    """
    if side not in ("long", "short"):
        raise RuntimeError(f"Invalid side: {side}")
    if not (0.0 < reversal_pct < 1.0):
        raise RuntimeError("reversal_pct must be a decimal fraction between 0 and 1.")

    if side == "long":
        if favorable_extreme <= entry_px:
            return float(entry_px)
        candidate_px = favorable_extreme * (1.0 - reversal_pct)
    else:
        if favorable_extreme >= entry_px:
            return float(entry_px)
        candidate_px = favorable_extreme * (1.0 + reversal_pct)

    return clamp_tp_reversal_trigger_px(side, entry_px, candidate_px)


def resolve_tp_reversal_stop_buffer_pct(
        reversal_pct: float,
        configured_stop_buffer_pct: Optional[float],
) -> float:
    """Resolve the TP-reversal stop buffer; default to 20% of the reversal threshold."""
    stop_buffer_pct = configured_stop_buffer_pct
    if stop_buffer_pct is None:
        stop_buffer_pct = reversal_pct * 0.2
    if not (0.0 < stop_buffer_pct < 1.0):
        raise RuntimeError("tp_reversal_stop_buffer_pct must be a decimal fraction between 0 and 1.")
    return stop_buffer_pct

def compute_stop_loss_trigger_px(side: str, entry_px: float, stop_loss_pct: float) -> float:
    """Compute the private stop-loss trigger from entry and pct."""
    if not (0.0 < stop_loss_pct < 1.0):
        raise RuntimeError("stop_loss_pct must be a decimal fraction between 0 and 1.")
    if side == "long":
        return entry_px * (1.0 - stop_loss_pct)
    if side == "short":
        return entry_px * (1.0 + stop_loss_pct)
    raise RuntimeError(f"Invalid side: {side}")

def stop_loss_trigger_px_is_valid(side: str, entry_px: float, trigger_px: Optional[float]) -> bool:
    """Return True when an absolute stop trigger is side-correct relative to entry."""
    if trigger_px is None or trigger_px <= 0.0:
        return False
    if side == "long":
        return trigger_px < entry_px
    if side == "short":
        return trigger_px > entry_px
    raise RuntimeError(f"Invalid side: {side}")


def resolve_stop_loss_trigger_px(
        side: str,
        entry_px: float,
        stop_loss_pct: Optional[float],
        stop_loss_trigger_px: Optional[float],
) -> Tuple[Optional[float], str]:
    """Resolve the active stop trigger, preferring an absolute trigger when valid."""
    if stop_loss_trigger_px is not None:
        if stop_loss_trigger_px_is_valid(side, entry_px, stop_loss_trigger_px):
            return float(stop_loss_trigger_px), "absolute"
        print(
            f"[SL-WARN] Ignoring invalid absolute stop trigger for {side}: "
            f"entry={entry_px:.8f} trigger={float(stop_loss_trigger_px):.8f}"
        )
    if stop_loss_pct is not None:
        return compute_stop_loss_trigger_px(side, entry_px, stop_loss_pct), "pct"
    return None, "none"


def format_tp_targets(tp_orders: List[Dict[str, Any]], placed_levels: Optional[set[int]] = None) -> str:
    if not tp_orders:
        return "N/A"
    placed_levels = placed_levels or set()
    chunks = []
    for order in tp_orders:
        level = int(order.get("level", 0))
        status = "placed" if level in placed_levels else "hidden"
        chunks.append(f"L{level}:{float(order['px']):.8f}/{float(order['sz']):.8f}/{status}")
    return ",".join(chunks)


def build_tp_level_to_oid_map(tp_orders: List[Dict[str, Any]], tp_oids: List[int]) -> Dict[int, int]:
    """Best-effort mapping from TP ladder level to exchange order id."""
    mapping: Dict[int, int] = {}
    for order, oid in zip(tp_orders, tp_oids):
        if isinstance(oid, int):
            mapping[int(order.get("level", 0))] = oid
    return mapping

def price_hit_take_profit(side: str, price: float, target_px: float) -> bool:
    if side == "long":
        return price >= target_px
    if side == "short":
        return price <= target_px
    raise RuntimeError(f"Invalid side: {side}")

def price_hit_stop(side: str, price: float, stop_px: Optional[float]) -> bool:
    if stop_px is None:
        return False
    if side == "long":
        return price <= stop_px
    if side == "short":
        return price >= stop_px
    raise RuntimeError(f"Invalid side: {side}")


def compute_trailing_take_profit_stop_px(
        side: str,
        entry_px: float,
        favorable_extreme: Optional[float],
        trail_profit_pct: float,
        current_stop_px: Optional[float] = None,
) -> Optional[float]:
    """Ratchet a local TP stop by a fraction of favorable unrealized profit."""
    if favorable_extreme is None:
        return current_stop_px
    if not (0.0 < trail_profit_pct < 1.0):
        raise RuntimeError("trail_profit_pct must be a decimal fraction between 0 and 1.")

    retain_profit_fraction = 1.0 - trail_profit_pct
    if side == "long":
        favorable_profit = favorable_extreme - entry_px
        if favorable_profit <= 0.0:
            return current_stop_px
        candidate = entry_px + (favorable_profit * retain_profit_fraction)
        return max(entry_px, current_stop_px if current_stop_px is not None else entry_px, candidate)

    if side == "short":
        favorable_profit = entry_px - favorable_extreme
        if favorable_profit <= 0.0:
            return current_stop_px
        candidate = entry_px - (favorable_profit * retain_profit_fraction)
        return min(entry_px, current_stop_px if current_stop_px is not None else entry_px, candidate)

    raise RuntimeError(f"Invalid side: {side}")

def signed_position_delta(initial_pos: float, current_pos: float, is_buy: bool) -> float:
    """Return how much of the desired directional entry has been filled."""
    direction = 1.0 if is_buy else -1.0
    return max(0.0, (current_pos - initial_pos) * direction)

def order_side_matches(order: Dict[str, Any], is_buy: bool) -> bool:
    """Best-effort side check across Hyperliquid open-order response variants."""
    for key in ("isBuy", "is_buy"):
        value = order.get(key)
        if isinstance(value, bool):
            return value == is_buy

    side_value = order.get("side")
    if side_value is None:
        # Some response shapes omit side. For entry cleanup we treat that as a
        # possible match so a stale entry order is not left behind before/after
        # market fallback.
        return True

    side_str = str(side_value).strip().lower()
    buy_values = {"b", "bid", "buy"}
    sell_values = {"a", "ask", "s", "sell"}
    if side_str in buy_values:
        return is_buy
    if side_str in sell_values:
        return not is_buy

    # Unknown side encoding: treat as possible match for cleanup.
    return True

async def stop_limit_px_for_trigger(
        info: Info,
        coin: str,
        is_exit_buy: bool,
        trigger_px: float,
        slippage: float,
) -> float:
    """Compute aggressive limit px for a market-style TP/SL trigger order."""
    if slippage < 0.0:
        raise RuntimeError("slippage must be >= 0")
    raw_px = trigger_px * (1.0 + slippage) if is_exit_buy else trigger_px * (1.0 - slippage)
    return await round_price_for_hyperliquid(info, coin, raw_px)

async def get_entry_limit_price(info: Info, coin: str, is_buy: bool) -> float:
    """Return a top-of-book maker limit price for entry."""
    best_bid, best_ask = await get_best_bid_ask(info, coin)
    if is_buy and best_bid is not None:
        return await round_price_for_hyperliquid(info, coin, best_bid)
    if (not is_buy) and best_ask is not None:
        return await round_price_for_hyperliquid(info, coin, best_ask)

    mids = await get_all_mids(info)
    if coin not in mids:
        raise RuntimeError(f"No BBO or mid available for {coin}; cannot price entry order.")
    return await round_price_for_hyperliquid(info, coin, float(mids[coin]))

async def cancel_entry_orders_for_coin(
        info: Info,
        exchange: Exchange,
        account_address: str,
        coin: str,
        is_buy: Optional[bool] = None,
        label: str = "entry orders",
) -> None:
    """Cancel remaining non-reduce-only entry orders for a coin.

    This is used before and after market fallback. The tracked active oid can
    become stale when an order is modified, partially filled, or returned in a
    different response shape, so this reconciles against the authoritative open
    order snapshot and removes any leftover non-reduce-only orders for the same
    coin/side. Reduce-only TP orders are intentionally left alone.
    """
    orders = await get_frontend_open_orders_for_coin(info, account_address, coin)
    oids: List[int] = []
    for order in orders:
        try:
            if bool(order.get("reduceOnly", False)):
                continue
            if is_buy is not None and not order_side_matches(order, is_buy):
                continue
            oid = order.get("oid")
            if isinstance(oid, int):
                oids.append(oid)
        except Exception:
            continue

    # frontendOpenOrders is preferred for reduceOnly filtering, but fall back to
    # openOrders if it returned nothing. openOrders usually represents active
    # limit orders, so anything found here before market fallback is treated as
    # an entry-order candidate.
    if not oids:
        fallback_orders = await get_open_orders_for_coin(info, account_address, coin)
        for order in fallback_orders:
            try:
                if is_buy is not None and not order_side_matches(order, is_buy):
                    continue
                oid = order.get("oid")
                if isinstance(oid, int):
                    oids.append(oid)
            except Exception:
                continue

    # Preserve order but deduplicate.
    deduped_oids = list(dict.fromkeys(oids))
    await cancel_oids(exchange, coin, deduped_oids, label)


async def cancel_oids(exchange: Exchange, coin: str, oids: List[int], label: str = "orders") -> None:
    """Bulk-cancel a list of order ids for one coin."""
    clean_oids = [int(oid) for oid in oids if isinstance(oid, int)]
    if not clean_oids:
        return

    reqs = [{"coin": coin, "oid": oid} for oid in clean_oids]
    try:
        print(f"[CANCEL] Canceling {len(reqs)} {label} for {coin}: {clean_oids}")
        resp = await exchange.bulk_cancel(reqs)
        print(f"[CANCEL] {coin} {label} cancel response: {resp}")
    except Exception as exc:
        print(f"[WARN] Failed to cancel {label} for {coin}: {exc}")


def update_hidden_stop_from_favorable_extreme(
        side: str,
        entry_px: float,
        current_stop_px: Optional[float],
        favorable_extreme: Optional[float],
        stop_loss_pct: Optional[float],
) -> Optional[float]:
    """Ratchet a private stop when the trade is in profit; never loosen it."""
    if stop_loss_pct is None:
        return current_stop_px

    base_stop = compute_stop_loss_trigger_px(side, entry_px, stop_loss_pct)
    if favorable_extreme is None:
        return current_stop_px if current_stop_px is not None else base_stop

    if side == "long":
        candidate = favorable_extreme * (1.0 - stop_loss_pct)
        if favorable_extreme <= entry_px:
            candidate = base_stop
        return max(base_stop, current_stop_px if current_stop_px is not None else base_stop, candidate)

    if side == "short":
        candidate = favorable_extreme * (1.0 + stop_loss_pct)
        if favorable_extreme >= entry_px:
            candidate = base_stop
        return min(base_stop, current_stop_px if current_stop_px is not None else base_stop, candidate)

    raise RuntimeError(f"Invalid side: {side}")

async def cancel_reduce_only_orders_for_coin(
        info: Info,
        exchange: Exchange,
        account_address: str,
        coin: str,
        only_tpsl: bool = False,
) -> None:
    """Cancel reduce-only orders for a coin, optionally limiting to TP/SL trigger orders."""
    orders = await get_frontend_open_orders_for_coin(info, account_address, coin)
    oids: List[int] = []
    for order in orders:
        try:
            if not bool(order.get("reduceOnly", False)):
                continue
            if only_tpsl and not bool(order.get("isTrigger", False)):
                continue
            oid = order.get("oid")
            if isinstance(oid, int):
                oids.append(oid)
        except Exception:
            continue
    label = "reduce-only TP/SL orders" if only_tpsl else "reduce-only orders"
    await cancel_oids(exchange, coin, oids, label)


async def get_open_reduce_only_take_profit_orders_for_coin(
        info: Info,
        account_address: str,
        coin: str,
) -> List[Dict[str, Any]]:
    """Return open reduce-only non-trigger TP limit orders for a coin."""
    orders = await get_frontend_open_orders_for_coin(info, account_address, coin)
    tp_orders: List[Dict[str, Any]] = []
    for order in orders:
        try:
            if not bool(order.get("reduceOnly", False)):
                continue
            if bool(order.get("isTrigger", False)):
                continue
            tp_orders.append(order)
        except Exception:
            continue
    return tp_orders


async def place_reduce_only_stop_market_order(
        info: Info,
        exchange: Exchange,
        coin: str,
        side: str,
        position_size_abs: float,
        trigger_px: float,
        slippage: float,
        label: str,
) -> Tuple[Optional[int], float]:
    """Place a reduce-only stop-market order for the current position size at a given trigger."""
    size = await round_size_for_hyperliquid(info, coin, position_size_abs)
    is_exit_buy = side == "short"
    trigger_px = await round_price_for_hyperliquid(info, coin, trigger_px)
    limit_px = await stop_limit_px_for_trigger(info, coin, is_exit_buy, trigger_px, slippage)
    order_type = {"trigger": {"triggerPx": trigger_px, "isMarket": True, "tpsl": "sl"}}
    side_label = "BUY" if is_exit_buy else "SELL"

    print(
        f"[{label}] Placing reduce-only stop-market {side_label} {size:.8f} {coin}: "
        f"trigger={trigger_px:.8f}, limit_px={limit_px:.8f}"
    )
    resp = await exchange.order(coin, is_exit_buy, size, limit_px, order_type, reduce_only=True)
    print(f"[{label}] response: {resp}")
    oids = extract_resting_oids(resp)
    return (oids[0] if oids else None), trigger_px


async def exit_on_tp_reversal(
        info: Info,
        exchange: Exchange,
        account_address: str,
        coin: str,
        side: str,
        live_signed_size: float,
        reference_px: float,
        reversal_pct: float,
        market_slippage: float,
        poll_interval: float,
        limit_exit_first: bool,
        stop_buffer_pct: Optional[float],
) -> bool:
    """Exit after a TP reversal, optionally trying limit+protective-stop first."""
    if not limit_exit_first:
        try:
            resp = await exchange.market_close(coin, slippage=market_slippage)
            print(f"[MARKET EXIT] response: {resp}")
            return True
        except Exception as exc:
            print(f"[ERROR] Failed market close after TP reversal: {exc}")
            return False

    size_abs = abs(live_signed_size)
    if size_abs <= 0.0:
        print(f"[TP-REVERSAL EXIT] {coin} is already flat before exit placement.")
        return True

    is_exit_buy = side == "short"
    side_label = "BUY" if is_exit_buy else "SELL"
    best_bid, best_ask = await get_best_bid_ask(info, coin)

    # Use a marketable limit: buy exits lift the ask, sell exits hit the bid.
    # This is still a limit order, but it should fill immediately when book data is fresh.
    if is_exit_buy and best_ask is not None:
        raw_limit_px = best_ask
    elif (not is_exit_buy) and best_bid is not None:
        raw_limit_px = best_bid
    else:
        raw_limit_px = reference_px

    limit_px = await round_price_for_hyperliquid(info, coin, raw_limit_px)
    rounded_size = await round_size_for_hyperliquid(info, coin, size_abs)
    stop_buffer_pct = resolve_tp_reversal_stop_buffer_pct(reversal_pct, stop_buffer_pct)
    stop_trigger_raw = (
        reference_px * (1.0 + stop_buffer_pct)
        if is_exit_buy
        else reference_px * (1.0 - stop_buffer_pct)
    )

    print(
        f"[TP-REVERSAL EXIT] Attempting reduce-only limit {side_label} "
        f"{rounded_size:.8f} {coin} @ {limit_px:.8f}; protective stop buffer "
        f"{stop_buffer_pct * 100:.4f}% from ref={reference_px:.8f}"
    )
    try:
        limit_resp = await exchange.order(
            coin,
            is_exit_buy,
            rounded_size,
            limit_px,
            {"limit": {"tif": "Gtc"}},
            reduce_only=True,
        )
    except Exception as exc:
        print(f"[ERROR] Failed TP-reversal limit exit placement for {coin}: {exc}")
        return False
    print(f"[TP-REVERSAL LIMIT] response: {limit_resp}")
    limit_oids = extract_resting_oids(limit_resp)

    await asyncio.sleep(min(max(poll_interval * 0.25, 0.1), 0.5))
    live_pos = await get_position_for_coin(info, account_address, coin)
    if live_pos is None:
        print(f"[TP-REVERSAL EXIT] {coin} fully closed on the limit order.")
        return True

    try:
        _, remaining_signed_size, _, remaining_side, remaining_abs = parse_position_snapshot(live_pos)
    except RuntimeError as exc:
        print(f"[WARN] {exc}")
        remaining_signed_size = live_signed_size
        remaining_side = side
        remaining_abs = size_abs

    stop_oid: Optional[int] = None
    stop_trigger_px: Optional[float] = None
    if remaining_abs > 0.0:
        try:
            stop_oid, stop_trigger_px = await place_reduce_only_stop_market_order(
                info=info,
                exchange=exchange,
                coin=coin,
                side=remaining_side,
                position_size_abs=remaining_abs,
                trigger_px=stop_trigger_raw,
                slippage=market_slippage,
                label="TP-REVERSAL STOP",
            )
        except Exception as exc:
            print(f"[ERROR] Failed TP-reversal stop placement for {coin}: {exc}")
            await cancel_oids(exchange, coin, limit_oids, "tp reversal limit orders")
            return False

    wait_seconds = max(1.0, poll_interval * 3.0)
    deadline = time.time() + wait_seconds
    while time.time() < deadline:
        await asyncio.sleep(min(max(poll_interval, 0.2), 1.0))
        live_pos = await get_position_for_coin(info, account_address, coin)
        if live_pos is None:
            print(f"[TP-REVERSAL EXIT] {coin} flat after paired limit/stop exit.")
            return True
        try:
            _, remaining_signed_size, _, _, remaining_abs = parse_position_snapshot(live_pos)
        except RuntimeError:
            print(f"[TP-REVERSAL EXIT] {coin} is no longer manageable; treating as closed.")
            return True
        print(
            f"[TP-REVERSAL EXIT] Waiting for {coin} exit fill; remaining signed size={remaining_signed_size:.8f} "
            f"limit_oids={limit_oids} stop_oid={stop_oid} stop_trigger="
            f"{f'{stop_trigger_px:.8f}' if stop_trigger_px is not None else 'N/A'}"
        )

    await cancel_oids(exchange, coin, limit_oids, "tp reversal limit orders")
    if stop_oid is not None:
        await cancel_oids(exchange, coin, [stop_oid], "tp reversal stop orders")

    live_pos = await get_position_for_coin(info, account_address, coin)
    if live_pos is None:
        print(f"[TP-REVERSAL EXIT] {coin} flat after canceling remaining exit orders.")
        return True

    print(f"[TP-REVERSAL EXIT] Limit/stop exit did not complete in time for {coin}; falling back to market close.")
    try:
        resp = await exchange.market_close(coin, slippage=market_slippage)
        print(f"[MARKET EXIT] response: {resp}")
        return True
    except Exception as exc:
        print(f"[ERROR] Failed market close after TP reversal limit fallback: {exc}")
        return False


async def close_position_remainder_with_market_retries(
        info: Info,
        exchange: Exchange,
        account_address: str,
        coin: str,
        side: str,
        market_slippage: float,
        poll_interval: float,
        label: str = "TP-REMAINDER",
        max_attempts: int = 3,
) -> bool:
    """Repeatedly reconcile and market-close a residual same-side position until flat."""
    settle_before_close = min(max(poll_interval * 0.25, 0.15), 0.5)
    settle_after_close = min(max(poll_interval * 0.5, 0.25), 1.0)

    for attempt in range(1, max_attempts + 1):
        await cancel_reduce_only_orders_for_coin(info, exchange, account_address, coin, only_tpsl=False)
        await asyncio.sleep(settle_before_close)

        remainder_pos = await get_position_for_coin(info, account_address, coin)
        if remainder_pos is None:
            print(f"[{label}] {coin} is already flat after cleanup.")
            return True

        try:
            _, remainder_signed_size, _, remainder_side, remainder_abs = parse_position_snapshot(remainder_pos)
        except RuntimeError as exc:
            print(f"[{label}] {coin} remainder check could not parse live position: {exc}")
            return False

        if remainder_side != side or remainder_abs <= 0.0:
            print(f"[{label}] {coin} no longer has a managed residual position.")
            return True

        print(
            f"[{label}] attempt {attempt}/{max_attempts} closing residual {coin} position: "
            f"signed={remainder_signed_size:.8f} abs={remainder_abs:.8f}"
        )
        try:
            resp = await exchange.market_close(coin, slippage=market_slippage)
            print(f"[{label}] market_close response: {resp}")
        except Exception as exc:
            print(f"[ERROR] {label} market_close failed for {coin} on attempt {attempt}/{max_attempts}: {exc}")
            if attempt >= max_attempts:
                return False
            await asyncio.sleep(settle_after_close)
            continue

        await asyncio.sleep(settle_after_close)
        final_remainder_pos = await get_position_for_coin(info, account_address, coin)
        if final_remainder_pos is None:
            print(f"[{label}] {coin} fully closed after residual market exit.")
            return True

        try:
            _, final_signed_size, _, final_side, final_abs = parse_position_snapshot(final_remainder_pos)
        except RuntimeError as exc:
            print(f"[{label}-WARN] {coin} post-close position parse failed: {exc}")
            if attempt >= max_attempts:
                return False
            continue

        if final_side != side or final_abs <= 0.0:
            print(f"[{label}] {coin} no longer has a managed residual position after market exit.")
            return True

        print(
            f"[{label}-WARN] {coin} still open after attempt {attempt}/{max_attempts}: "
            f"signed={final_signed_size:.8f} abs={final_abs:.8f}. Retrying cleanup."
        )

    print(f"[{label}-WARN] {coin} residual position remained open after {max_attempts} market-close attempts.")
    return False


async def place_hidden_take_profit_order(
        info: Info,
        exchange: Exchange,
        coin: str,
        side: str,
        order: Dict[str, Any],
        tp_tif: str,
) -> List[int]:
    """Place one hidden TP level as a post-only reduce-only limit once touched."""
    if tp_tif not in ("Alo", "Gtc"):
        raise RuntimeError("tp_tif must be Alo or Gtc.")

    is_exit_buy = bool(order["is_buy"])
    target_px = float(order["px"])
    size = await round_size_for_hyperliquid(info, coin, float(order["sz"]))
    best_bid, best_ask = await get_best_bid_ask(info, coin)

    # Keep it maker/post-only. For long exits this is a sell resting just above ask;
    # for short exits this is a buy resting just below bid. The target remains the
    # trigger condition; placement price may be nudged to avoid taker execution.
    if side == "long":
        px = target_px
        if best_ask is not None:
            px = max(px, best_ask + PRICE_EPS)
    elif side == "short":
        px = target_px
        if best_bid is not None:
            px = min(px, best_bid - PRICE_EPS)
    else:
        raise RuntimeError(f"Invalid side: {side}")

    px = await round_price_for_hyperliquid(info, coin, px)
    side_label = "BUY" if is_exit_buy else "SELL"
    print(
        f"[HIDDEN-TP] Target touched; placing reduce-only post-only {side_label} "
        f"{size:.8f} {coin} @ {px:.8f} for L{int(order.get('level', 0)):02d} "
        f"target={target_px:.8f}"
    )
    resp = await exchange.order(
        coin,
        is_exit_buy,
        size,
        px,
        {"limit": {"tif": "Alo" if tp_tif == "Alo" else tp_tif}},
        reduce_only=True,
    )
    print(f"[HIDDEN-TP] response: {resp}")
    err = extract_order_error(resp)
    if err is not None:
        print(f"[HIDDEN-TP-WARN] TP level was not placed: {err}")
        return []
    return extract_resting_oids(resp)


async def enter_position_with_reposting_limit(
        info: Info,
        exchange: Exchange,
        account_address: str,
        coin: str,
        is_buy: bool,
        size: float,
        retries: int,
        repost_interval: float,
        entry_tif: str,
        market_fallback: bool,
        market_slippage: float,
        metrics_start_time_ms: Optional[int] = None,
) -> Dict[str, Any]:
    """Enter with one resting top-of-book limit order, modifying it instead of cancel/repost.

    The previous implementation canceled the resting entry order every loop and
    submitted a fresh one. This version places one order, keeps its oid, and on
    subsequent iterations sends a single modify_order request with the updated
    top-of-book price and remaining size. A cancel is only sent when the entry is
    done, the order becomes unusable, or the bot is about to fall back to market.
    """
    if size <= 0.0:
        raise RuntimeError("Entry size must be > 0.")
    if retries < 0:
        raise RuntimeError("Entry retries must be >= 0.")
    if repost_interval <= 0.0:
        raise RuntimeError("Entry repost interval must be > 0.")
    if entry_tif not in ("Alo", "Gtc"):
        raise RuntimeError("entry_tif must be Alo or Gtc.")

    size = await round_size_for_hyperliquid(info, coin, size)
    initial_pos = await get_position_size_for_coin(info, account_address, coin)
    direction_label = "LONG" if is_buy else "SHORT"

    if initial_pos != 0.0:
        print(
            f"[WARN] Existing {coin} position is {initial_pos:.8f}. "
            "Entry fill will be measured as directional delta from that starting position."
        )

    print("============================================================")
    print(" Hyperliquid Async Smart Entry")
    print("============================================================")
    print(f"Coin:            {coin}")
    print(f"Direction:       {direction_label}")
    print(f"Requested size:  {size:.8f}")
    print(f"Entry TIF:       {entry_tif}")
    print(f"Limit attempts:  {retries}")
    print(f"Modify interval: {repost_interval:.2f}s")
    print(f"Market fallback: {market_fallback}")
    print("============================================================")

    active_oid: Optional[int] = None
    side = "BUY" if is_buy else "SELL"

    async def get_entry_progress() -> Tuple[float, float, float, str, AccountRuntimeMetrics]:
        """Return current_pos, filled_delta, remaining_size, formatted uPnL, account metrics."""
        _current_pos = await get_position_size_for_coin(info, account_address, coin)
        filled_delta = signed_position_delta(initial_pos, _current_pos, is_buy)
        remaining_size = max(0.0, size - filled_delta)

        pos_snapshot = await get_position_for_coin(info, account_address, coin)
        mids = await get_all_mids(info)
        mid_price_raw = mids.get(coin)
        _upnl_str = "N/A"
        if pos_snapshot is not None and mid_price_raw is not None:
            try:
                unrealized_pnl = compute_position_unrealized_pnl(pos_snapshot, float(mid_price_raw))
            except (TypeError, ValueError):
                unrealized_pnl = None
            if unrealized_pnl is not None:
                _upnl_str = f"{unrealized_pnl:.8f}"
        _metrics = await get_account_runtime_metrics(info, account_address, metrics_start_time_ms, coin=coin)
        return _current_pos, filled_delta, remaining_size, _upnl_str, _metrics

    async def cancel_active_entry_order(label: str, reconcile: bool = False) -> None:
        nonlocal active_oid
        if active_oid is not None:
            await cancel_oids(exchange, coin, [active_oid], label)
            active_oid = None
        if reconcile:
            await cancel_entry_orders_for_coin(info, exchange, account_address, coin, is_buy=is_buy, label=label)

    for attempt in range(1, retries + 1):
        current_pos, filled, remaining, upnl_str, metrics = await get_entry_progress()
        print(
            f"[ENTRY {attempt}/{retries}] status filled_delta={filled:.8f} "
            f"remaining={remaining:.8f} position_now={current_pos:.8f} upnl={upnl_str} "
            f"{format_account_metrics(metrics)} active_oid={active_oid if active_oid is not None else 'N/A'}"
        )
        if remaining <= 0.0:
            print(f"[ENTRY] Target position filled before attempt {attempt}.")
            await cancel_active_entry_order("excess entry order after target fill", reconcile=True)
            break

        remaining = await round_size_for_hyperliquid(info, coin, remaining)
        limit_px = await get_entry_limit_price(info, coin, is_buy)
        order_type = {"limit": {"tif": entry_tif}}

        if active_oid is None:
            print(
                f"[ENTRY {attempt}/{retries}] placing {side} {remaining:.8f} {coin} @ {limit_px:.8f} "
                f"({entry_tif}, top-of-book)"
            )
            try:
                resp = await exchange.order(
                    coin,
                    is_buy,
                    remaining,
                    limit_px,
                    order_type,
                    reduce_only=False,
                )
                print(f"[ENTRY {attempt}] place response: {resp}")
            except Exception as exc:
                print(f"[WARN] Entry order placement attempt {attempt} failed: {exc}")
                await asyncio.sleep(repost_interval)
                continue

            err = extract_order_error(resp)
            if err is not None:
                print(f"[WARN] Entry placement rejected: {err}")
                await asyncio.sleep(repost_interval)
                continue

            resting_oids = extract_resting_oids(resp)
            if resting_oids:
                active_oid = resting_oids[0]
                print(f"[ENTRY {attempt}] active entry oid={active_oid}")
            else:
                filled_now = extract_filled_qty(resp)
                if filled_now > 0.0:
                    print(f"[ENTRY {attempt}] order filled immediately for {filled_now:.8f}; no resting oid.")
                else:
                    print("[ENTRY] No resting oid returned; will re-check position before another placement.")
        else:
            print(
                f"[ENTRY {attempt}/{retries}] modifying oid={active_oid} -> "
                f"{side} {remaining:.8f} {coin} @ {limit_px:.8f} ({entry_tif}, top-of-book)"
            )
            try:
                resp = await exchange.modify_order(
                    active_oid,
                    coin,
                    is_buy,
                    remaining,
                    limit_px,
                    order_type,
                    reduce_only=False,
                )
                print(f"[ENTRY {attempt}] modify response: {resp}")
            except Exception as exc:
                print(f"[WARN] Entry order modify failed for oid={active_oid}: {exc}")
                active_oid = None
                await asyncio.sleep(repost_interval)
                continue

            err = extract_order_error(resp)
            if err is not None:
                print(f"[WARN] Entry modify rejected for oid={active_oid}: {err}")
                lowered = err.lower()
                if any(token in lowered for token in
                       ("does not exist", "not found", "already filled", "not open", "canceled")):
                    active_oid = None
                await asyncio.sleep(repost_interval)
                continue

        await asyncio.sleep(repost_interval)

    current_pos, filled, remaining, upnl_str, metrics = await get_entry_progress()
    print(
        f"[ENTRY FINAL] filled_delta={filled:.8f}, remaining={remaining:.8f}, "
        f"position_now={current_pos:.8f}, upnl={upnl_str}, {format_account_metrics(metrics)}, "
        f"active_oid={active_oid if active_oid is not None else 'N/A'}"
    )

    if remaining <= 0.0:
        await cancel_active_entry_order("excess entry order after complete fill", reconcile=True)
    elif remaining > 0.0:
        if market_fallback:
            await cancel_active_entry_order("entry order before market fallback", reconcile=True)
            await asyncio.sleep(0.15)
            await cancel_entry_orders_for_coin(
                info,
                exchange,
                account_address,
                coin,
                is_buy=is_buy,
                label="entry order reconcile before market fallback",
            )
            current_pos, filled, remaining, upnl_str, metrics = await get_entry_progress()
            print(
                f"[ENTRY] After cancel reconcile: filled_delta={filled:.8f}, "
                f"remaining={remaining:.8f}, position_now={current_pos:.8f}, upnl={upnl_str}, "
                f"{format_account_metrics(metrics)}"
            )
            if remaining <= 0.0:
                print("[ENTRY] Target filled while canceling/reconciling the limit order; skipping market fallback.")
                await cancel_entry_orders_for_coin(
                    info,
                    exchange,
                    account_address,
                    coin,
                    is_buy=is_buy,
                    label="entry order final cleanup after cancel reconcile",
                )
            else:
                remaining = await round_size_for_hyperliquid(info, coin, remaining)
                print(
                    f"[ENTRY] Limit modify attempts exhausted with {remaining:.8f} {coin} remaining. "
                    f"Falling back to market_open with slippage={market_slippage:.4f}."
                )
                try:
                    resp = await exchange.market_open(coin, is_buy, remaining, slippage=market_slippage)
                    print(f"[MARKET ENTRY] response: {resp}")
                    await asyncio.sleep(0.15)
                    await cancel_entry_orders_for_coin(
                        info,
                        exchange,
                        account_address,
                        coin,
                        is_buy=is_buy,
                        label="entry order reconcile after market fallback",
                    )
                except Exception as exc:
                    raise RuntimeError(f"Market fallback failed: {exc}") from exc
        else:
            await cancel_active_entry_order("unfilled entry order after limit attempts", reconcile=True)
            raise RuntimeError(
                f"Limit entry did not fully fill. Filled {filled:.8f} of {size:.8f}; market fallback disabled."
            )

    await asyncio.sleep(max(0.25, repost_interval))
    final_pos = await get_position_for_coin(info, account_address, coin)
    if final_pos is None:
        raise RuntimeError(f"No open {coin} position found after entry attempts.")

    final_size = float(final_pos.get("szi", "0"))
    if is_buy and final_size <= initial_pos:
        raise RuntimeError(f"Expected larger/longer {coin} position after buy entry, got {final_size}.")
    if (not is_buy) and final_size >= initial_pos:
        raise RuntimeError(f"Expected smaller/shorter {coin} position after sell entry, got {final_size}.")

    return final_pos


async def place_stop_market_order(
        info: Info,
        exchange: Exchange,
        coin: str,
        side: str,
        position_size_abs: float,
        trigger_px: float,
        slippage: float,
        label: str = "SL",
) -> Tuple[Optional[int], float]:
    """Place a reduce-only stop-market order for the full managed position."""
    size = await round_size_for_hyperliquid(info, coin, position_size_abs)
    is_exit_buy = side == "short"
    if side not in ("long", "short"):
        raise RuntimeError(f"Invalid side: {side}")

    trigger_px = await round_price_for_hyperliquid(info, coin, trigger_px)
    limit_px = await stop_limit_px_for_trigger(info, coin, is_exit_buy, trigger_px, slippage)
    order_type = {"trigger": {"triggerPx": trigger_px, "isMarket": True, "tpsl": "sl"}}
    side_label = "BUY" if is_exit_buy else "SELL"

    print(
        f"[{label}] Placing reduce-only stop-market {side_label} {size:.8f} {coin}: "
        f"trigger={trigger_px:.8f}, limit_px={limit_px:.8f}"
    )
    resp = await exchange.order(coin, is_exit_buy, size, limit_px, order_type, reduce_only=True)
    print(f"[{label}] response: {resp}")
    oids = extract_resting_oids(resp)
    return (oids[0] if oids else None), trigger_px


async def compute_auto_sar_stop_trigger_px(
        info: Info,
        coin: str,
        side: str,
        interval: str,
        periods: int,
        sar_acceleration: float,
        sar_maximum: float,
        use_last_closed_candle: bool,
        use_websocket_candles: bool = False,
) -> Optional[float]:
    """Compute the latest protective SAR stop for an active auto-managed position."""
    sar_stop_px, _, _, _ = await compute_auto_sar_stop_state(
        info=info,
        coin=coin,
        side=side,
        interval=interval,
        periods=periods,
        sar_acceleration=sar_acceleration,
        sar_maximum=sar_maximum,
        use_last_closed_candle=use_last_closed_candle,
        use_websocket_candles=use_websocket_candles,
    )
    return sar_stop_px


async def compute_auto_sar_stop_state(
        info: Info,
        coin: str,
        side: str,
        interval: str,
        periods: int,
        sar_acceleration: float,
        sar_maximum: float,
        use_last_closed_candle: bool,
        use_websocket_candles: bool = False,
) -> Tuple[Optional[float], Optional[float], Optional[float], bool]:
    """Return the latest SAR stop candidate and whether the SAR flipped against the position."""
    if np is None or talib is None:
        raise RuntimeError("Dynamic auto SAR stop updates require numpy and TA-Lib.")

    candles = await fetch_recent_candles(
        info,
        coin,
        interval,
        periods,
        use_websocket_candles=use_websocket_candles,
    )
    usable_candles = candles[:-1] if use_last_closed_candle and len(candles) > 1 else candles
    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []
    for candle in usable_candles:
        try:
            highs.append(float(candle["h"]))
            lows.append(float(candle["l"]))
            closes.append(float(candle["c"]))
        except (KeyError, TypeError, ValueError):
            continue

    if not highs or not lows or not closes:
        raise RuntimeError(f"No valid candles available to compute SAR stop for {coin} {interval}.")

    high_arr = np.asarray(highs, dtype=float)  # type: ignore[union-attr]
    low_arr = np.asarray(lows, dtype=float)  # type: ignore[union-attr]
    close_arr = np.asarray(closes, dtype=float)  # type: ignore[union-attr]
    sar_arr = talib.SAR(  # type: ignore[union-attr]
        high_arr,
        low_arr,
        acceleration=sar_acceleration,
        maximum=sar_maximum,
    )

    for idx in range(len(sar_arr) - 1, -1, -1):
        sar_value = sar_arr[idx]
        close_value = close_arr[idx]
        if not np.isfinite(sar_value) or not np.isfinite(close_value):  # type: ignore[union-attr]
            continue

        candidate = float(sar_value)
        close_px = float(close_value)
        if side == "long" and candidate < close_px:
            return candidate, candidate, close_px, False
        if side == "short" and candidate > close_px:
            return candidate, candidate, close_px, False
        if side == "long":
            return None, candidate, close_px, candidate >= close_px
        if side == "short":
            return None, candidate, close_px, candidate <= close_px
        raise RuntimeError(f"Invalid side: {side}")

    return None, None, None, False


async def monitor_bracket_position(
        info: Info,
        exchange: Exchange,
        account_address: str,
        coin: str,
        side: str,
        entry_px: float,
        take_profit_pct: Optional[float],
        tp_orders: List[Dict[str, Any]],
        tp_oids: List[int],
        stop_oid: Optional[int],
        managed_signed_size: float,
        poll_interval: float,
        reversal_pct: Optional[float],
        market_slippage: float,
        stop_loss_pct: Optional[float],
        stop_loss_trigger_px: Optional[float],
        take_profit_levels: int,
        tp_tif: str,
        tp_reversal_limit_exit: bool,
        tp_reversal_stop_buffer_pct: Optional[float],
        hide_orders: bool = False,
        use_trailing_tp: bool = False,
        trailing_tp_trigger_level: int = 1,
        trailing_tp_profit_pct: float = 0.25,
        metrics_start_time_ms: Optional[int] = None,
        auto_sar_stop_interval: Optional[str] = None,
        auto_sar_stop_periods: Optional[int] = None,
        auto_sar_acceleration: Optional[float] = None,
        auto_sar_maximum: Optional[float] = None,
        auto_use_last_closed_candle: bool = True,
        auto_use_websocket_candles: bool = False,
) -> None:
    """Monitor bracket orders, display PnL/account metrics, and manage hidden/public TP/SL."""
    if poll_interval <= 0.0:
        raise RuntimeError("poll_interval must be > 0")
    tp_reversal_enabled = reversal_pct is not None
    if tp_reversal_enabled and not (0.0 < reversal_pct < 1.0):
        raise RuntimeError("reversal_pct must be a decimal fraction between 0 and 1.")
    if trailing_tp_trigger_level <= 0:
        raise RuntimeError("trailing_tp_trigger_level must be > 0.")

    side_is_long = side == "long"
    tp_trigger_px: Optional[float] = None
    if tp_orders:
        prices = [float(order["px"]) for order in tp_orders]
        tp_trigger_px = min(prices) if side_is_long else max(prices)

    hidden_tp_placed_levels: set[int] = set()
    hidden_tp_oids: Dict[int, List[int]] = {}
    tp_level_to_oid = build_tp_level_to_oid_map(tp_orders, tp_oids)
    local_stop_px: Optional[float] = None
    stop_source = "none"
    trailing_tp_armed = False
    trailing_tp_stop_px: Optional[float] = None
    dynamic_auto_sar_stop_enabled = (
        auto_sar_stop_interval is not None
        and auto_sar_stop_periods is not None
        and auto_sar_acceleration is not None
        and auto_sar_maximum is not None
    )
    if hide_orders:
        local_stop_px, stop_source = resolve_stop_loss_trigger_px(side, entry_px, stop_loss_pct, stop_loss_trigger_px)

    print("============================================================")
    print(" Async Bracket Position Monitor")
    print("============================================================")
    print(f"Coin:             {coin}")
    print(f"Side:             {side}")
    print(f"Entry:            {entry_px:.8f}")
    print(f"Hide orders:      {hide_orders}")
    print(f"First TP px:       {tp_trigger_px if tp_trigger_px is not None else 'N/A'}")
    print(f"Trailing TP:       {use_trailing_tp}")
    if use_trailing_tp:
        print(f"Trailing TP level: {trailing_tp_trigger_level}")
        print(f"Trailing TP pct:   {trailing_tp_profit_pct * 100:.4f}%")
    print(
        f"Local stop px:     {fmt_optional_float(local_stop_px)} ({stop_source})"
        if hide_orders
        else f"Stop oid:          {stop_oid if stop_oid is not None else 'N/A'}"
    )
    if hide_orders and not use_trailing_tp:
        print(f"Hidden TP targets: {format_tp_targets(tp_orders, hidden_tp_placed_levels)}")
    if tp_reversal_enabled:
        print(f"TP reversal pct:   {reversal_pct * 100:.4f}%")
    else:
        print("TP reversal pct:   DISABLED")
    if tp_reversal_enabled and tp_reversal_limit_exit:
        stop_buffer_pct = resolve_tp_reversal_stop_buffer_pct(reversal_pct, tp_reversal_stop_buffer_pct)
        print("TP rev exit mode:  LIMIT+STOP then MARKET")
        print(f"TP rev stop pct:   {stop_buffer_pct * 100:.4f}%")
    elif tp_reversal_enabled:
        print("TP rev exit mode:  MARKET")
    else:
        print("TP rev exit mode:  DISABLED")
    print(f"TP resting oids:   {tp_oids}")
    print(f"Poll interval:     {poll_interval:.2f}s")
    if dynamic_auto_sar_stop_enabled:
        print(
            f"Auto SAR stop:     {auto_sar_stop_interval} "
            f"(periods={auto_sar_stop_periods}, closed_only={auto_use_last_closed_candle})"
        )
        print(f"Auto SAR WS:       {auto_use_websocket_candles}")
    print("------------------------------------------------------------")
    print("Press Ctrl+C to stop monitoring without closing the position.")
    print("============================================================")

    favorable_extreme: Optional[float] = None
    tp_zone_seen = False
    tp_remainder_close_in_progress = False

    try:
        while True:
            await asyncio.sleep(poll_interval)
            try:
                pos = await get_position_for_coin(info, account_address, coin)
                if pos is None:
                    print(f"[DONE] No open {coin} position remains. Canceling leftover reduce-only orders.")
                    await cancel_reduce_only_orders_for_coin(info, exchange, account_address, coin, only_tpsl=False)
                    return

                try:
                    _, live_signed_size, live_entry_px, live_side, live_pos_abs = parse_position_snapshot(pos)
                except RuntimeError as exc:
                    print(f"[WARN] {exc}")
                    continue

                if side_is_long and live_signed_size <= 0.0:
                    print(f"[DONE] Managed long {coin} is no longer open. Canceling leftovers.")
                    await cancel_reduce_only_orders_for_coin(info, exchange, account_address, coin, only_tpsl=False)
                    return
                if (not side_is_long) and live_signed_size >= 0.0:
                    print(f"[DONE] Managed short {coin} is no longer open. Canceling leftovers.")
                    await cancel_reduce_only_orders_for_coin(info, exchange, account_address, coin, only_tpsl=False)
                    return

                mids = await get_all_mids(info)
                if coin not in mids:
                    print(f"[WARN] No mid price for {coin}; skipping this monitor tick.")
                    continue
                price = float(mids[coin])
                unrealized_pnl = compute_position_unrealized_pnl(pos, price)
                metrics = await get_account_runtime_metrics(info, account_address, metrics_start_time_ms, coin=coin)

                size_increased = position_is_directional_add(managed_signed_size, live_signed_size)
                if size_increased:
                    print(
                        f"[REBASE] {coin} position increased from {managed_signed_size:.8f} to {live_signed_size:.8f}. "
                        f"Rebuilding {'hidden ' if hide_orders else ''}TP/SL from avg entry {live_entry_px:.8f}."
                    )
                    stop_oid, tp_orders, tp_oids, tp_trigger_px = await rebuild_bracket_orders(
                        info=info,
                        exchange=exchange,
                        account_address=account_address,
                        coin=coin,
                        side=live_side,
                        position_size_abs=live_pos_abs,
                        entry_px=live_entry_px,
                        take_profit_pct=take_profit_pct,
                        stop_loss_pct=stop_loss_pct,
                        stop_loss_trigger_px=stop_loss_trigger_px,
                        take_profit_levels=take_profit_levels,
                        tp_tif=tp_tif,
                        market_slippage=market_slippage,
                        cancel_existing_reduce_only=True,
                        hide_orders=hide_orders,
                    )
                    side = live_side
                    side_is_long = side == "long"
                    entry_px = live_entry_px
                    managed_signed_size = live_signed_size
                    favorable_extreme = price
                    tp_zone_seen = tp_trigger_px is not None and (
                        price >= tp_trigger_px if side_is_long else price <= tp_trigger_px
                    )
                    trailing_tp_armed = False
                    trailing_tp_stop_px = None
                    hidden_tp_placed_levels.clear()
                    hidden_tp_oids.clear()
                    tp_level_to_oid = build_tp_level_to_oid_map(tp_orders, tp_oids)
                    tp_remainder_close_in_progress = False
                    if hide_orders:
                        local_stop_px, stop_source = resolve_stop_loss_trigger_px(
                            side,
                            entry_px,
                            stop_loss_pct,
                            stop_loss_trigger_px,
                        )
                else:
                    managed_signed_size = live_signed_size
                    entry_px = live_entry_px

                if dynamic_auto_sar_stop_enabled:
                    try:
                        updated_sar_stop_px, latest_sar_px, latest_sar_close_px, sar_flip_exit = await compute_auto_sar_stop_state(
                            info=info,
                            coin=coin,
                            side=side,
                            interval=auto_sar_stop_interval,
                            periods=auto_sar_stop_periods,
                            sar_acceleration=auto_sar_acceleration,
                            sar_maximum=auto_sar_maximum,
                            use_last_closed_candle=auto_use_last_closed_candle,
                            use_websocket_candles=auto_use_websocket_candles,
                        )
                    except Exception as exc:
                        print(f"[SL-SAR-WARN] Failed to refresh SAR stop for {coin}: {exc}")
                    else:
                        if sar_flip_exit:
                            print(
                                f"[SL-SAR] {coin} SAR flipped against {side} position: "
                                f"sar={fmt_optional_float(latest_sar_px)} close={fmt_optional_float(latest_sar_close_px)}. "
                                "Canceling reduce-only orders and market-closing."
                            )
                            await cancel_reduce_only_orders_for_coin(
                                info,
                                exchange,
                                account_address,
                                coin,
                                only_tpsl=False,
                            )
                            try:
                                resp = await exchange.market_close(coin, slippage=market_slippage)
                                print(f"[SL-SAR] market_close response: {resp}")
                            except Exception as exc:
                                print(f"[ERROR] Auto SAR market_close failed for {coin}: {exc}")
                                continue
                            return
                        if updated_sar_stop_px is not None:
                            rounded_sar_stop_px = await round_price_for_hyperliquid(info, coin, updated_sar_stop_px)
                            if hide_orders:
                                if local_stop_px is None or abs(rounded_sar_stop_px - local_stop_px) > PRICE_EPS:
                                    print(
                                        f"[SL-SAR] Updating hidden SAR stop for {coin}: "
                                        f"{fmt_optional_float(local_stop_px)} -> {rounded_sar_stop_px:.8f}"
                                    )
                                    local_stop_px = rounded_sar_stop_px
                                    stop_source = "sar-dynamic"
                            else:
                                current_stop_px = None if stop_oid is None else stop_loss_trigger_px
                                if current_stop_px is None or abs(rounded_sar_stop_px - current_stop_px) > PRICE_EPS:
                                    print(
                                        f"[SL-SAR] Updating exchange stop for {coin}: "
                                        f"{fmt_optional_float(current_stop_px)} -> {rounded_sar_stop_px:.8f}"
                                    )
                                    await cancel_reduce_only_orders_for_coin(
                                        info,
                                        exchange,
                                        account_address,
                                        coin,
                                        only_tpsl=True,
                                    )
                                    stop_oid, stop_loss_trigger_px = await place_stop_market_order(
                                        info=info,
                                        exchange=exchange,
                                        coin=coin,
                                        side=side,
                                        position_size_abs=live_pos_abs,
                                        trigger_px=rounded_sar_stop_px,
                                        slippage=market_slippage,
                                        label="SL-SAR",
                                    )
                        elif hide_orders:
                            stop_source = "sar-dynamic"

                if side_is_long:
                    if favorable_extreme is None or price > favorable_extreme:
                        favorable_extreme = price
                    if tp_trigger_px is not None and price >= tp_trigger_px:
                        tp_zone_seen = True
                    current_tp_reversal_px = (
                        compute_tp_reversal_trigger_px(side, entry_px, favorable_extreme, reversal_pct)
                        if tp_reversal_enabled and reversal_pct is not None and tp_zone_seen and favorable_extreme is not None
                        else None
                    )
                    if current_tp_reversal_px is not None:
                        current_tp_reversal_px = clamp_tp_reversal_trigger_px(side, entry_px, current_tp_reversal_px)
                    reversal_hit = current_tp_reversal_px is not None and price <= current_tp_reversal_px
                else:
                    if favorable_extreme is None or price < favorable_extreme:
                        favorable_extreme = price
                    if tp_trigger_px is not None and price <= tp_trigger_px:
                        tp_zone_seen = True
                    current_tp_reversal_px = (
                        compute_tp_reversal_trigger_px(side, entry_px, favorable_extreme, reversal_pct)
                        if tp_reversal_enabled and reversal_pct is not None and tp_zone_seen and favorable_extreme is not None
                        else None
                    )
                    if current_tp_reversal_px is not None:
                        current_tp_reversal_px = clamp_tp_reversal_trigger_px(side, entry_px, current_tp_reversal_px)
                    reversal_hit = current_tp_reversal_px is not None and price >= current_tp_reversal_px

                if hide_orders:
                    if stop_source == "pct":
                        local_stop_px = update_hidden_stop_from_favorable_extreme(
                            side=side,
                            entry_px=entry_px,
                            current_stop_px=local_stop_px,
                            favorable_extreme=favorable_extreme,
                            stop_loss_pct=stop_loss_pct,
                        )

                    if price_hit_stop(side, price, local_stop_px):
                        print(
                            f"[HIDDEN-SL] {coin} local stop hit at mid={price:.8f}; "
                            f"stop={local_stop_px:.8f}. Canceling TP orders and market-closing."
                        )
                        await cancel_reduce_only_orders_for_coin(info, exchange, account_address, coin, only_tpsl=False)
                        try:
                            resp = await exchange.market_close(coin, slippage=market_slippage)
                            print(f"[HIDDEN-SL] market_close response: {resp}")
                        except Exception as exc:
                            print(f"[ERROR] Hidden stop market_close failed for {coin}: {exc}")
                            continue
                        return

                    if not use_trailing_tp:
                        for order in tp_orders:
                            level = int(order.get("level", 0))
                            if level in hidden_tp_placed_levels:
                                continue
                            target_px = float(order["px"])
                            if not price_hit_take_profit(side, price, target_px):
                                continue
                            oids = await place_hidden_take_profit_order(info, exchange, coin, side, order, tp_tif)
                            if oids:
                                hidden_tp_placed_levels.add(level)
                                hidden_tp_oids[level] = oids
                                tp_oids.extend(oids)

                if ((use_trailing_tp and hide_orders and tp_trigger_px is not None) or trailing_tp_armed):
                    if not trailing_tp_armed and tp_zone_seen:
                        trailing_tp_armed = True
                        trailing_tp_stop_px = compute_trailing_take_profit_stop_px(
                            side=side,
                            entry_px=entry_px,
                            favorable_extreme=favorable_extreme,
                            trail_profit_pct=trailing_tp_profit_pct,
                            current_stop_px=trailing_tp_stop_px,
                        )
                        print(
                            f"[TRAILING-TP] {coin} first TP zone reached at mid={price:.8f}; "
                            f"initial trailing TP stop={fmt_optional_float(trailing_tp_stop_px)}"
                        )

                    if trailing_tp_armed:
                        new_trailing_tp_stop_px = compute_trailing_take_profit_stop_px(
                            side=side,
                            entry_px=entry_px,
                            favorable_extreme=favorable_extreme,
                            trail_profit_pct=trailing_tp_profit_pct,
                            current_stop_px=trailing_tp_stop_px,
                        )
                        if new_trailing_tp_stop_px is not None and (
                            trailing_tp_stop_px is None or abs(new_trailing_tp_stop_px - trailing_tp_stop_px) > PRICE_EPS
                        ):
                            print(
                                f"[TRAILING-TP] {coin} ratcheting trailing TP stop "
                                f"{fmt_optional_float(trailing_tp_stop_px)} -> {new_trailing_tp_stop_px:.8f}"
                            )
                        trailing_tp_stop_px = new_trailing_tp_stop_px

                        if price_hit_stop(side, price, trailing_tp_stop_px):
                            print(
                                f"[TRAILING-TP] {coin} trailing TP hit at mid={price:.8f}; "
                                f"stop={fmt_optional_float(trailing_tp_stop_px)}. Canceling reduce-only orders and market-closing."
                            )
                            await cancel_reduce_only_orders_for_coin(info, exchange, account_address, coin, only_tpsl=False)
                            try:
                                resp = await exchange.market_close(coin, slippage=market_slippage)
                                print(f"[TRAILING-TP] market_close response: {resp}")
                            except Exception as exc:
                                print(f"[ERROR] Trailing TP market_close failed for {coin}: {exc}")
                                continue
                            return

                if tp_orders and not use_trailing_tp:
                    open_tp_orders = await get_open_reduce_only_take_profit_orders_for_coin(
                        info,
                        account_address,
                        coin,
                    )
                    tp_levels_exhausted = False
                    if hide_orders:
                        tp_levels_exhausted = (
                            len(hidden_tp_placed_levels) >= len(tp_orders)
                            and len(open_tp_orders) == 0
                        )
                    else:
                        tp_levels_exhausted = len(open_tp_orders) == 0 and not trailing_tp_armed

                    if tp_levels_exhausted and not tp_remainder_close_in_progress:
                        tp_remainder_close_in_progress = True
                        print(
                            f"[TP-REMAINDER] {coin} take-profit ladder is exhausted; "
                            "checking whether any position remains open."
                        )
                        close_ok = await close_position_remainder_with_market_retries(
                            info=info,
                            exchange=exchange,
                            account_address=account_address,
                            coin=coin,
                            side=side,
                            market_slippage=market_slippage,
                            poll_interval=poll_interval,
                            label="TP-REMAINDER",
                        )
                        if close_ok:
                            return
                        tp_remainder_close_in_progress = False

                if tp_orders and use_trailing_tp and not hide_orders and not trailing_tp_armed:
                    open_tp_orders = await get_open_reduce_only_take_profit_orders_for_coin(
                        info,
                        account_address,
                        coin,
                    )
                    open_tp_oids = {
                        int(order["oid"])
                        for order in open_tp_orders
                        if isinstance(order, dict) and isinstance(order.get("oid"), int)
                    }
                    trigger_tp_oid = tp_level_to_oid.get(trailing_tp_trigger_level)
                    if trigger_tp_oid is not None and trigger_tp_oid not in open_tp_oids:
                        remaining_tp_oids = sorted(open_tp_oids)
                        print(
                            f"[TRAILING-TP] {coin} TP level {trailing_tp_trigger_level} filled. "
                            "Canceling remaining TP ladder orders and switching to a local trailing TP."
                        )
                        if remaining_tp_oids:
                            await cancel_oids(
                                exchange,
                                coin,
                                remaining_tp_oids,
                                "remaining TP orders before trailing conversion",
                            )
                            await asyncio.sleep(min(max(poll_interval * 0.25, 0.15), 0.5))
                        trailing_tp_armed = True
                        trailing_tp_stop_px = compute_trailing_take_profit_stop_px(
                            side=side,
                            entry_px=entry_px,
                            favorable_extreme=favorable_extreme,
                            trail_profit_pct=trailing_tp_profit_pct,
                            current_stop_px=trailing_tp_stop_px,
                        )
                        print(
                            f"[TRAILING-TP] {coin} trailing TP armed after TP level "
                            f"{trailing_tp_trigger_level} fill; initial stop={fmt_optional_float(trailing_tp_stop_px)}"
                        )

                extreme_str = f"{favorable_extreme:.8f}" if favorable_extreme is not None else "N/A"
                pnl_str = f"{unrealized_pnl:.8f}" if unrealized_pnl is not None else "N/A"
                tp_reversal_str = f"{current_tp_reversal_px:.8f}" if current_tp_reversal_px is not None else "N/A"
                stop_str = fmt_optional_float(local_stop_px) if hide_orders else (
                    str(stop_oid) if stop_oid is not None else "N/A")
                if use_trailing_tp or trailing_tp_armed:
                    tp_targets_str = (
                        f"trail-armed:{fmt_optional_float(trailing_tp_stop_px)}"
                        if trailing_tp_armed
                        else f"trail-pending:L{trailing_tp_trigger_level}"
                    )
                else:
                    tp_targets_str = (
                        format_tp_targets(tp_orders, hidden_tp_placed_levels)
                        if hide_orders
                        else "exchange-resting"
                    )
                print(
                    f"[MONITOR] {coin} side={side} pos={managed_signed_size:.8f} mid={price:.8f} "
                    f"entry={entry_px:.8f} upnl={pnl_str} {format_account_metrics(metrics)} "
                    f"extreme={extreme_str} local_stop={stop_str} tp_zone_seen={tp_zone_seen} "
                    f"tp_rev_px={tp_reversal_str} tp_targets={tp_targets_str}"
                )

                if reversal_hit:
                    print(
                        f"[TP-REVERSAL] {coin} moved into TP zone and reversed by "
                        f"{reversal_pct * 100:.4f}%. Canceling TP/SL orders and exiting."
                    )
                    await cancel_reduce_only_orders_for_coin(info, exchange, account_address, coin, only_tpsl=False)
                    exit_ok = await exit_on_tp_reversal(
                        info=info,
                        exchange=exchange,
                        account_address=account_address,
                        coin=coin,
                        side=side,
                        live_signed_size=managed_signed_size,
                        reference_px=price,
                        reversal_pct=reversal_pct,
                        market_slippage=market_slippage,
                        poll_interval=poll_interval,
                        limit_exit_first=(tp_reversal_limit_exit and not hide_orders),
                        stop_buffer_pct=tp_reversal_stop_buffer_pct,
                    )
                    if not exit_ok:
                        continue
                    await asyncio.sleep(max(0.25, poll_interval))
                    await cancel_reduce_only_orders_for_coin(info, exchange, account_address, coin, only_tpsl=False)
                    return
            except Exception as exc:
                if is_rate_limit_error(exc):
                    print(
                        f"[WATCH-RETRY] {coin} monitor hit Hyperliquid rate limit ({exc}). "
                        f"Sleeping {WATCH_RETRY_SLEEP_SECONDS:.1f}s before retry."
                    )
                    await asyncio.sleep(WATCH_RETRY_SLEEP_SECONDS)
                    continue
                raise
    except KeyboardInterrupt:
        print("\n[!] Caught Ctrl+C, leaving current open orders/position untouched and exiting monitor.")


async def place_take_profit_ladder(
        exchange: Exchange,
        coin: str,
        tp_orders: List[Dict[str, Any]],
        tp_tif: str,
) -> List[int]:
    """Place reduce-only TP limit orders and return any resting oids."""
    if tp_tif not in ("Alo", "Gtc"):
        raise RuntimeError("tp_tif must be Alo or Gtc.")
    if not tp_orders:
        return []

    requests_payload: List[Dict[str, Any]] = []
    print("[TP] Planned take-profit ladder:")
    for order in tp_orders:
        side = "BUY" if order["is_buy"] else "SELL"
        print(f"  L{order['level']:02d} {side:4} {order['sz']:.8f} {coin} @ {order['px']:.8f}")
        requests_payload.append(
            {
                "coin": coin,
                "is_buy": bool(order["is_buy"]),
                "sz": float(order["sz"]),
                "limit_px": float(order["px"]),
                "order_type": {"limit": {"tif": tp_tif}},
                "reduce_only": True,
            }
        )

    resp = await exchange.bulk_orders(requests_payload)
    print(f"[TP] bulk_orders response: {resp}")
    return extract_resting_oids(resp)


async def build_take_profit_ladder(
        info: Info,
        coin: str,
        side: str,
        position_size_abs: float,
        entry_px: float,
        take_profit_pct: float,
        levels: int,
) -> List[Dict[str, Any]]:
    """Build weighted TP limit orders that sum to the full position."""
    if not (0.0 < take_profit_pct < 1.0):
        raise RuntimeError("take_profit_pct must be a decimal fraction between 0 and 1.")
    if levels <= 0:
        raise RuntimeError("take-profit levels must be > 0")

    exit_is_buy = side == "short"
    weights = list(range(1, levels + 1))
    weight_sum = float(sum(weights))
    raw_sizes = [position_size_abs * (weight / weight_sum) for weight in weights]

    orders: List[Dict[str, Any]] = []
    remaining = await round_size_for_hyperliquid(info, coin, position_size_abs)

    for idx, raw_sz in enumerate(raw_sizes, start=1):
        if idx == levels:
            sz = remaining
        else:
            sz = min(remaining, await round_size_for_hyperliquid(info, coin, raw_sz))
        remaining = max(0.0, remaining - sz)
        if sz <= 0.0:
            continue

        fraction = idx / levels
        raw_px = entry_px * (1.0 + take_profit_pct * fraction) if side == "long" else entry_px * (
                    1.0 - take_profit_pct * fraction)
        px = await round_price_for_hyperliquid(info, coin, raw_px)
        orders.append({"coin": coin, "is_buy": exit_is_buy, "sz": sz, "px": px, "level": idx})

    return orders


async def rebuild_bracket_orders(
        info: Info,
        exchange: Exchange,
        account_address: str,
        coin: str,
        side: str,
        position_size_abs: float,
        entry_px: float,
        take_profit_pct: Optional[float],
        stop_loss_pct: Optional[float],
        stop_loss_trigger_px: Optional[float],
        take_profit_levels: int,
        tp_tif: str,
        market_slippage: float,
        cancel_existing_reduce_only: bool = True,
        hide_orders: bool = False,
        use_trailing_tp: bool = False,
        trailing_tp_profit_pct: float = 0.25,
) -> Tuple[Optional[int], List[Dict[str, Any]], List[int], Optional[float]]:
    """Place or prepare reduce-only TP/SL orders for the current live position.

    Default behavior places exchange-side stop-market and TP limit orders.
    With hide_orders=True, no TP/SL orders are placed during rebuild; targets are
    calculated and returned so monitor_bracket_position can hold them in memory
    and trigger them opportunistically.
    """
    if cancel_existing_reduce_only:
        await cancel_reduce_only_orders_for_coin(info, exchange, account_address, coin, only_tpsl=False)

    tp_orders: List[Dict[str, Any]] = []
    tp_oids: List[int] = []
    tp_trigger_px: Optional[float] = None

    if take_profit_pct is not None:
        tp_orders = await build_take_profit_ladder(
            info=info,
            coin=coin,
            side=side,
            position_size_abs=position_size_abs,
            entry_px=entry_px,
            take_profit_pct=take_profit_pct,
            levels=take_profit_levels,
        )
        prices = [float(order["px"]) for order in tp_orders]
        if prices:
            tp_trigger_px = min(prices) if side == "long" else max(prices)

    if hide_orders:
        resolved_stop_px, stop_source = resolve_stop_loss_trigger_px(side, entry_px, stop_loss_pct,
                                                                     stop_loss_trigger_px)
        stop_desc = f"{resolved_stop_px:.8f}" if resolved_stop_px is not None else "N/A"
        print("[HIDE-ORDERS] TP/SL orders are private; no exchange-side bracket orders placed.")
        print(f"[HIDE-ORDERS] Local stop target: {stop_desc} ({stop_source})")
        if use_trailing_tp and tp_trigger_px is not None:
            print(
                f"[TRAILING-TP] Hidden TP ladder suppressed; trailing TP will arm after first TP at "
                f"{tp_trigger_px:.8f} with trail {trailing_tp_profit_pct * 100:.4f}% of favorable unrealized profit."
            )
        else:
            print(f"[HIDE-ORDERS] Hidden TP targets: {format_tp_targets(tp_orders)}")
        return None, tp_orders, [], tp_trigger_px

    stop_oid: Optional[int] = None
    resolved_stop_px, stop_source = resolve_stop_loss_trigger_px(side, entry_px, stop_loss_pct, stop_loss_trigger_px)
    if resolved_stop_px is not None:
        stop_oid, _ = await place_stop_market_order(
            info=info,
            exchange=exchange,
            coin=coin,
            side=side,
            position_size_abs=position_size_abs,
            trigger_px=resolved_stop_px,
            slippage=market_slippage,
            label="SL" if stop_source == "pct" else "SL-SAR",
        )

    if use_trailing_tp and hide_orders and tp_trigger_px is not None:
        print(
            f"[TRAILING-TP] TP ladder disabled; arming local trailing take-profit after first TP at "
            f"{tp_trigger_px:.8f} with trail {trailing_tp_profit_pct * 100:.4f}% of favorable unrealized profit."
        )
    elif tp_orders:
        tp_oids = await place_take_profit_ladder(exchange, coin, tp_orders, tp_tif)

    return stop_oid, tp_orders, tp_oids, tp_trigger_px


async def run_bracket_entry(
        coin: str,
        direction: str,
        size: float,
        take_profit_pct: Optional[float],
        stop_loss_pct: Optional[float],
        stop_loss_trigger_px: Optional[float],
        take_profit_levels: int,
        use_trailing_tp: bool,
        trailing_tp_trigger_level: int,
        trailing_tp_profit_pct: float,
        entry_retries: int,
        entry_repost_interval: float,
        poll_interval: float,
        tp_reversal_pct: Optional[float],
        entry_tif: str,
        tp_tif: str,
        market_fallback: bool,
        market_slippage: float,
        cancel_existing_tpsl: bool,
        tp_reversal_limit_exit: bool,
        tp_reversal_stop_buffer_pct: Optional[float],
        use_testnet: bool,
        use_websocket: bool = True,
        hide_orders: bool = False,
        auto_sar_stop_interval: Optional[str] = None,
        auto_sar_stop_periods: Optional[int] = None,
        auto_sar_acceleration: Optional[float] = None,
        auto_sar_maximum: Optional[float] = None,
        auto_use_last_closed_candle: bool = True,
        auto_use_websocket_candles: bool = False,
        account_address: Optional[str] = None,
        info: Optional[Info] = None,
        exchange: Optional[Exchange] = None,
) -> None:
    """Enter a position and manage bracket protection/exit logic."""
    direction = direction.lower().strip()
    if direction not in ("long", "short"):
        raise RuntimeError("direction must be 'long' or 'short'.")
    if size <= 0.0:
        raise RuntimeError("size must be > 0.")
    if take_profit_pct is None and stop_loss_pct is None and stop_loss_trigger_px is None:
        raise RuntimeError("Specify --take-profit-pct, --stop-loss-pct, or both.")
    if take_profit_pct is not None and not (0.0 < take_profit_pct < 1.0):
        raise RuntimeError("--take-profit-pct must be a decimal fraction between 0 and 1.")

    stop_loss_pct = compute_default_stop_loss_pct(take_profit_pct, stop_loss_pct)
    if stop_loss_pct is not None and not (0.0 < stop_loss_pct < 1.0):
        raise RuntimeError("--stop-loss-pct must be a decimal fraction between 0 and 1.")
    if take_profit_levels <= 0:
        raise RuntimeError("--take-profit-levels must be > 0.")
    if trailing_tp_trigger_level <= 0:
        raise RuntimeError("--trailing-tp-trigger-level must be > 0.")
    if trailing_tp_trigger_level > take_profit_levels:
        raise RuntimeError("--trailing-tp-trigger-level cannot exceed --take-profit-levels.")
    if not (0.0 < trailing_tp_profit_pct < 1.0):
        raise RuntimeError("--trailing-tp-profit-pct must be a decimal fraction between 0 and 1.")
    if entry_tif not in ("Alo", "Gtc"):
        raise RuntimeError("--entry-tif must be Alo or Gtc.")
    if tp_tif not in ("Alo", "Gtc"):
        raise RuntimeError("--tp-tif must be Alo or Gtc.")

    owns_clients = account_address is None and info is None and exchange is None
    if not owns_clients and (account_address is None or info is None or exchange is None):
        raise RuntimeError("Pass account_address, info, and exchange together when reusing initialized clients.")
    try:
        if owns_clients:
            account_address, info, exchange = await init_clients(use_testnet, use_websocket=use_websocket)
        metrics_start_time_ms = int(time.time() * 1000)
        coin = coin.upper()
        is_buy = direction == "long"

        print("============================================================")
        print(" Hyperliquid Async Bracket Entry Bot")
        print("============================================================")
        print(f"Account:           {account_address}")
        print(f"Network:           {'TESTNET' if use_testnet else 'MAINNET'}")
        print(f"Websocket:         {'ENABLED' if use_websocket else 'DISABLED'}")
        print(f"WS candles:        {'ENABLED' if auto_use_websocket_candles else 'DISABLED'}")
        print(f"Hide orders:       {hide_orders}")
        print(f"Coin:              {coin}")
        print(f"Direction:         {direction}")
        print(f"Size:              {size}")
        if take_profit_pct is not None:
            print(f"Take profit pct:   {take_profit_pct * 100:.4f}%")
        else:
            print("Take profit pct:   N/A")
        print(f"Trailing TP:       {use_trailing_tp}")
        if use_trailing_tp:
            print(f"Trailing TP level: {trailing_tp_trigger_level}")
            print(f"Trailing TP pct:   {trailing_tp_profit_pct * 100:.4f}%")
        if stop_loss_pct is not None:
            print(f"Stop loss pct:     {stop_loss_pct * 100:.4f}%")
        else:
            print("Stop loss pct:     N/A")
        if stop_loss_trigger_px is not None:
            print(f"Stop trigger px:   {stop_loss_trigger_px:.8f}")
        print(f"TP levels:         {take_profit_levels}")
        print("============================================================")

        if cancel_existing_tpsl:
            await cancel_reduce_only_orders_for_coin(info, exchange, account_address, coin, only_tpsl=False)

        pos = await enter_position_with_reposting_limit(
            info=info,
            exchange=exchange,
            account_address=account_address,
            coin=coin,
            is_buy=is_buy,
            size=size,
            retries=entry_retries,
            repost_interval=entry_repost_interval,
            entry_tif=entry_tif,
            market_fallback=market_fallback,
            market_slippage=market_slippage,
            metrics_start_time_ms=metrics_start_time_ms,
        )

        try:
            current_size = float(pos["szi"])
            entry_px = float(pos["entryPx"])
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(f"Could not parse opened position: {pos}. Error: {exc}") from exc

        side = "long" if current_size > 0.0 else "short"
        pos_abs = abs(current_size)
        print(f"[OPEN] {coin} {side.upper()} size={pos_abs:.8f}, signed={current_size:.8f}, entry={entry_px:.8f}")

        stop_oid, tp_orders, tp_oids, _ = await rebuild_bracket_orders(
            info=info,
            exchange=exchange,
            account_address=account_address,
            coin=coin,
            side=side,
            position_size_abs=pos_abs,
            entry_px=entry_px,
            take_profit_pct=take_profit_pct,
            stop_loss_pct=stop_loss_pct,
            stop_loss_trigger_px=stop_loss_trigger_px,
            take_profit_levels=take_profit_levels,
            tp_tif=tp_tif,
            market_slippage=market_slippage,
            cancel_existing_reduce_only=cancel_existing_tpsl,
            hide_orders=hide_orders,
            use_trailing_tp=use_trailing_tp,
            trailing_tp_profit_pct=trailing_tp_profit_pct,
        )

        await monitor_bracket_position(
            info=info,
            exchange=exchange,
            account_address=account_address,
            coin=coin,
            side=side,
            entry_px=entry_px,
            take_profit_pct=take_profit_pct,
            tp_orders=tp_orders,
            tp_oids=tp_oids,
            stop_oid=stop_oid,
            managed_signed_size=current_size,
            poll_interval=poll_interval,
            reversal_pct=tp_reversal_pct,
            market_slippage=market_slippage,
            stop_loss_pct=stop_loss_pct,
            stop_loss_trigger_px=stop_loss_trigger_px,
            take_profit_levels=take_profit_levels,
            tp_tif=tp_tif,
            tp_reversal_limit_exit=tp_reversal_limit_exit,
            tp_reversal_stop_buffer_pct=tp_reversal_stop_buffer_pct,
            hide_orders=hide_orders,
            use_trailing_tp=use_trailing_tp,
            trailing_tp_trigger_level=trailing_tp_trigger_level,
            trailing_tp_profit_pct=trailing_tp_profit_pct,
            metrics_start_time_ms=metrics_start_time_ms,
            auto_sar_stop_interval=auto_sar_stop_interval,
            auto_sar_stop_periods=auto_sar_stop_periods,
            auto_sar_acceleration=auto_sar_acceleration,
            auto_sar_maximum=auto_sar_maximum,
            auto_use_last_closed_candle=auto_use_last_closed_candle,
            auto_use_websocket_candles=auto_use_websocket_candles,
        )
    finally:
        if owns_clients:
            await close_clients(info, exchange)
