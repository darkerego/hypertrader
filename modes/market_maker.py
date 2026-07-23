# ---------------------------------------------------------------------------
# Event-driven market maker
# ---------------------------------------------------------------------------
import asyncio
import statistics
from typing import Optional, List, Dict, Any, Tuple

from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

from utils.helpers import init_clients, close_clients, extract_order_error
from utils.constants import INTERVAL_TO_MS, PRICE_EPS
from utils.helpers import get_all_mids, get_open_orders_for_coin, get_position_size_for_coin, \
    fetch_recent_candles, get_best_bid_ask, round_price_for_hyperliquid, \
    round_size_for_hyperliquid, hyperliquid_market_ids_match, normalize_hyperliquid_market_id

def extract_order_px(order: Dict[str, Any]) -> float:
    """Extract order price across API variants."""
    for key in ("px", "limitPx", "limit_px"):
        value = order.get(key)
        if value is not None:
            return float(value)
    raise KeyError(f"Order has no recognizable price key: {order}")

def compute_center_and_sigma_from_candles(candles: List[Dict[str, Any]]) -> Tuple[float, float]:
    """Compute center price and standard deviation from candle closes."""
    closes: List[float] = []
    highs: List[float] = []
    lows: List[float] = []

    for candle in candles:
        try:
            closes.append(float(candle["c"]))
            highs.append(float(candle["h"]))
            lows.append(float(candle["l"]))
        except (KeyError, TypeError, ValueError):
            continue

    if not closes:
        raise RuntimeError("No valid close prices in candles.")

    center = statistics.mean(closes)
    try:
        sigma = statistics.pstdev(closes)
    except statistics.StatisticsError:
        sigma = 0.0

    if sigma <= 0.0:
        ranges = [high - low for high, low in zip(highs, lows) if high >= low]
        sigma = statistics.mean(ranges) if ranges else 0.0

    if sigma <= 0.0:
        raise RuntimeError("Standard deviation and average range are zero; market appears flat.")

    return center, sigma

async def cancel_open_orders_for_coin(
    info: Info,
    exchange: Exchange,
    account_address: str,
    coin: str,
    existing_orders: Optional[List[Dict[str, Any]]] = None,
    mid: Optional[float] = None,
    protect_close_pct: float = 0.0,
) -> List[Dict[str, Any]]:
    """Cancel open orders for `coin`, optionally protecting orders near current mid."""
    if existing_orders is None:
        coin_orders = await get_open_orders_for_coin(info, account_address, coin)
    else:
        coin_orders = [
            order for order in existing_orders
            if hyperliquid_market_ids_match(str(order.get("coin", "")), coin)
        ]

    protected: List[Dict[str, Any]] = []
    cancel_requests: List[Dict[str, Any]] = []

    for order in coin_orders:
        try:
            px = float(extract_order_px(order))
        except (TypeError, ValueError, KeyError):
            continue

        if mid is not None and protect_close_pct > 0.0 and mid > 0.0:
            dev = abs(px - mid) / mid
            if dev <= protect_close_pct:
                protected.append(order)
                continue

        try:
            sz = float(order.get("sz", "0"))
            oid = order.get("oid")
        except (TypeError, ValueError):
            continue

        if sz > 0.0 and isinstance(oid, int):
            cancel_requests.append({"coin": coin, "oid": oid})

    if not cancel_requests:
        if protected:
            print(f"[MM] No orders to cancel for {coin}; {len(protected)} orders protected near mid.")
        else:
            print(f"[MM] No existing open orders to cancel for {coin}.")
        return protected

    print(
        f"[MM] Canceling {len(cancel_requests)} existing open orders for {coin} in bulk; "
        f"protecting {len(protected)} orders near mid."
    )
    try:
        res = await exchange.bulk_cancel(cancel_requests)
        print(f"[CANCEL] {coin} -> {res}")
    except Exception as exc:
        print(f"[ERROR] Bulk cancel for {coin} failed: {exc}")

    return protected

async def place_order_with_retry(
        info: Info,
        exchange: Exchange,
        coin: str,
        is_buy: bool,
        size: float,
        initial_px: float,
        max_retries: int = 3,
) -> None:
    """Place a post-only order, retrying slightly adjusted prices if ALO fails."""
    side = "BUY" if is_buy else "SELL"
    px = initial_px

    for attempt in range(max_retries):
        try:
            rounded_size = await round_size_for_hyperliquid(info, coin, size)
            rounded_px = await round_price_for_hyperliquid(info, coin, px)
            resp = await exchange.order(
                coin,
                is_buy,
                rounded_size,
                rounded_px,
                {"limit": {"tif": "Alo"}},
            )
        except Exception as exc:
            print(f"[ERROR] {side} {size:.8f} {coin} @ {px:.8f} exception: {exc}")
            return

        err = extract_order_error(resp)
        if err is None:
            print(f"[OK] {side:4} {size:.8f} {coin} @ {px:.8f} -> {resp}")
            return

        print(f"[WARN] {side} {size:.8f} {coin} @ {px:.8f} rejected: {err}")

        if "Post only order would have immediately matched" in err:
            best_bid, best_ask = await get_best_bid_ask(info, coin)
            eps = PRICE_EPS * (attempt + 1)
            if is_buy and best_bid is not None:
                px = best_bid - eps
                print(f"[RETRY] Adjusting BUY below best bid: new px={px:.8f}")
                continue
            if (not is_buy) and best_ask is not None:
                px = best_ask + eps
                print(f"[RETRY] Adjusting SELL above best ask: new px={px:.8f}")
                continue

            print("[RETRY] No valid BBO to adjust against; giving up.")
            return

        print("[WARN] Non post-only error, not retrying.")
        return

    print(f"[WARN] Max retries reached for {side} {size:.8f} {coin} (last px={px:.8f}).")

async def run_market_maker(
    coin: str,
    interval: str,
    periods: int,
    levels: int,
    base_size: float,
    use_testnet: bool,
    loop_sleep: Optional[float],
    min_edge_pct: float,
    rebalance_threshold_pct: float,
    protect_close_pct: float,
    use_websocket: bool = True,
    account_address: Optional[str] = None,
    info: Optional[Info] = None,
    exchange: Optional[Exchange] = None,
) -> None:
    """Market maker that only rebalances when price drifts away from current ladder."""
    if levels <= 0:
        raise RuntimeError("levels must be > 0")
    if base_size <= 0.0:
        raise RuntimeError("base-size must be > 0")
    if interval not in INTERVAL_TO_MS:
        raise RuntimeError(f"Unsupported interval {interval}. Valid: {sorted(INTERVAL_TO_MS.keys())}")
    if min_edge_pct <= 0.0:
        raise RuntimeError("min-edge-pct must be > 0")
    if rebalance_threshold_pct <= 0.0:
        raise RuntimeError("rebalance-threshold-pct must be > 0")
    if protect_close_pct < 0.0:
        raise RuntimeError("protect-close-pct must be >= 0")
    if loop_sleep is None or loop_sleep <= 0.0:
        loop_sleep = 1.0

    owns_clients = account_address is None and info is None and exchange is None
    if not owns_clients and (account_address is None or info is None or exchange is None):
        raise RuntimeError("Pass account_address, info, and exchange together when reusing initialized clients.")
    try:
        if owns_clients:
            account_address, info, exchange = await init_clients(use_testnet, use_websocket=use_websocket)
        coin = normalize_hyperliquid_market_id(coin)

        print("============================================================")
        print(" Hyperliquid Async Market Maker")
        print("============================================================")
        print(f"Account:        {account_address}")
        print(f"Network:        {'TESTNET' if use_testnet else 'MAINNET'}")
        print(f"Websocket:      {'ENABLED' if use_websocket else 'DISABLED'}")
        print(f"Coin:           {coin}")
        print(f"Interval:       {interval}")
        print(f"Lookback N:     {periods} candles")
        print(f"Levels/side:    {levels}")
        print(f"Base size:      {base_size} contracts")
        print(f"Poll interval:  {loop_sleep:.2f} seconds")
        print(f"Min edge:       {min_edge_pct * 100:.4f}% vs mid")
        print(f"Rebalance thr:  {rebalance_threshold_pct * 100:.4f}% mid vs ladder center")
        print("============================================================")

        loop_idx = 0
        while True:
            loop_idx += 1
            await asyncio.sleep(loop_sleep)

            try:
                mids = await get_all_mids(info)
                mid = float(mids.get(coin)) if coin in mids else None
            except Exception as exc:
                print(f"[WARN] Failed to fetch mids for {coin}: {exc}")
                mid = None

            coin_orders = await get_open_orders_for_coin(info, account_address, coin)
            ladder_prices: List[float] = []
            for order in coin_orders:
                try:
                    ladder_prices.append(float(extract_order_px(order)))
                except (TypeError, ValueError, KeyError):
                    continue

            need_rebalance = False
            if mid is None or not ladder_prices:
                need_rebalance = True
                if mid is None and not ladder_prices:
                    print(f"[LOOP {loop_idx}] No mid price and no ladder -> forcing rebalance.")
                elif mid is None:
                    print(f"[LOOP {loop_idx}] No mid price -> forcing rebalance.")
                else:
                    print(f"[LOOP {loop_idx}] No existing ladder for {coin} -> forcing rebalance.")
            else:
                ladder_min = min(ladder_prices)
                ladder_max = max(ladder_prices)
                ladder_center = 0.5 * (ladder_min + ladder_max)
                deviation = abs(mid - ladder_center) / ladder_center if ladder_center > 0 else None
                dev_str = "N/A" if deviation is None else f"{deviation * 100:.4f}%"
                print(
                    f"[LOOP {loop_idx}] mid={mid:.8f}, ladder_center={ladder_center:.8f}, "
                    f"min={ladder_min:.8f}, max={ladder_max:.8f}, dev={dev_str}"
                )
                if deviation is None or deviation >= rebalance_threshold_pct:
                    print("[MM] Rebalance condition met (price drifted from ladder).")
                    need_rebalance = True
                else:
                    print("[MM] No rebalance needed this loop.")
                    continue

            if not need_rebalance:
                continue

            try:
                pos_size = await get_position_size_for_coin(info, account_address, coin)
            except Exception as exc:
                print(f"[WARN] Failed to fetch current position for {coin}: {exc}")
                pos_size = 0.0

            pos_abs = abs(pos_size)
            exit_chunk = pos_abs / levels if pos_abs > 0 else 0.0
            if pos_size > 0:
                print(f"[MM] Current position: LONG {pos_size:.8f}, exit chunk per level: {exit_chunk:.8f}")
            elif pos_size < 0:
                print(f"[MM] Current position: SHORT {pos_size:.8f}, exit chunk per level: {exit_chunk:.8f}")
            else:
                print("[MM] Current position: FLAT")

            try:
                candles = await fetch_recent_candles(info, coin, interval, periods)
            except Exception as exc:
                print(f"[ERROR] Failed to fetch candles for {coin}: {exc}")
                continue

            print(f"[MM] Fetched {len(candles)} candles for {coin} {interval}.")
            try:
                center, sigma = compute_center_and_sigma_from_candles(candles)
            except Exception as exc:
                print(f"[ERROR] Failed to compute center/sigma: {exc}")
                continue

            print(f"[MM] New center:  {center:.8f}")
            print(f"[MM] New sigma:   {sigma:.8f}")

            try:
                mids = await get_all_mids(info)
                mid = float(mids.get(coin)) if coin in mids else None
            except Exception as exc:
                print(f"[WARN] Failed to fetch mids for edge check: {exc}")
                mid = None

            if mid is not None:
                print(f"[MM] Current mid: {mid:.8f}")
            else:
                print("[MM] Current mid: N/A")

            best_bid, best_ask = await get_best_bid_ask(info, coin)
            print(f"[MM] Best bid/ask:  {best_bid} / {best_ask}")

            step = sigma / (levels + 1)
            orders_to_place: List[Dict[str, Any]] = []

            edge_buy_limit = mid * (1.0 - min_edge_pct) if mid is not None else None
            base_buy_px = center - step
            if edge_buy_limit is not None:
                base_buy_px = min(base_buy_px, edge_buy_limit)

            last_buy_px = None
            for k in range(levels):
                buy_px = base_buy_px - k * step
                if best_bid is not None and buy_px >= best_bid:
                    buy_px = best_bid - PRICE_EPS
                if last_buy_px is not None and buy_px >= last_buy_px:
                    buy_px = last_buy_px - PRICE_EPS
                buy_size = exit_chunk if pos_size < 0 and exit_chunk > 0.0 else base_size * (k + 1)
                if buy_px > 0 and buy_size > 0:
                    buy_px_rounded = await round_price_for_hyperliquid(info, coin, buy_px)
                    buy_size_rounded = await round_size_for_hyperliquid(info, coin, buy_size)
                    orders_to_place.append({"coin": coin, "is_buy": True, "px": buy_px_rounded, "sz": buy_size_rounded})
                    last_buy_px = buy_px_rounded

            edge_sell_limit = mid * (1.0 + min_edge_pct) if mid is not None else None
            base_sell_px = center + step
            if edge_sell_limit is not None:
                base_sell_px = max(base_sell_px, edge_sell_limit)

            last_sell_px = None
            for k in range(levels):
                sell_px = base_sell_px + k * step
                if best_ask is not None and sell_px <= best_ask:
                    sell_px = best_ask + PRICE_EPS
                if last_sell_px is not None and sell_px <= last_sell_px:
                    sell_px = last_sell_px + PRICE_EPS
                sell_size = exit_chunk if pos_size > 0 and exit_chunk > 0.0 else base_size * (k + 1)
                if sell_px > 0 and sell_size > 0:
                    sell_px_rounded = await round_price_for_hyperliquid(info, coin, sell_px)
                    sell_size_rounded = await round_size_for_hyperliquid(info, coin, sell_size)
                    orders_to_place.append({"coin": coin, "is_buy": False, "px": sell_px_rounded, "sz": sell_size_rounded})
                    last_sell_px = sell_px_rounded

            if not orders_to_place:
                print("[MM] No valid prices generated for orders after spacing/edge checks.")
                continue

            print("[MM] Planned limit orders (post-only / ALO, spaced ladder, position-based sizing):")
            for order in orders_to_place:
                side = "BUY" if order["is_buy"] else "SELL"
                print(f"  {side:4} {order['sz']:.8f} {coin} @ {order['px']:.8f}")
            print("------------------------------------------------------------")

            await cancel_open_orders_for_coin(
                info,
                exchange,
                account_address,
                coin,
                existing_orders=coin_orders,
                mid=mid,
                protect_close_pct=protect_close_pct,
            )

            print(f"[MM] Placing {len(orders_to_place)} new orders for {coin}...")
            for order in orders_to_place:
                await place_order_with_retry(
                    info,
                    exchange,
                    order["coin"],
                    bool(order["is_buy"]),
                    float(order["sz"]),
                    float(order["px"]),
                    max_retries=3,
                )
    except KeyboardInterrupt:
        print("\n[!] Caught Ctrl+C, stopping market maker loop.")
    finally:
        if owns_clients:
            await close_clients(info, exchange)
