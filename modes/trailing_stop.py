# ---------------------------------------------------------------------------
# Trailing stop manager
# ---------------------------------------------------------------------------
import asyncio
import time
from typing import Optional, Dict, Any, List, Tuple

from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

from utils.helpers import init_clients, get_all_open_positions, get_all_mids, \
    parse_position_snapshot, compute_position_unrealized_pnl, get_account_runtime_metrics, position_is_directional_add, \
    format_account_metrics, close_clients



def compute_trailing_stop_px(
    side: str,
    entry: float,
    favorable_extreme: float,
    trail_pct: float,
    current_stop: Optional[float] = None,
) -> float:
    """Return a profit-based trailing stop from the best favorable price seen."""
    if not (0.0 < trail_pct < 1.0):
        raise ValueError(f"trail_pct must be between 0 and 1. Got: {trail_pct}")

    retain_profit_fraction = 1.0 - trail_pct

    if side == "long":
        favorable_profit = favorable_extreme - entry
        if favorable_profit <= 0.0:
            return current_stop if current_stop is not None else entry
        candidate = entry + (favorable_profit * retain_profit_fraction)
        return max(entry, current_stop if current_stop is not None else entry, candidate)

    if side == "short":
        favorable_profit = entry - favorable_extreme
        if favorable_profit <= 0.0:
            return current_stop if current_stop is not None else entry
        candidate = entry - (favorable_profit * retain_profit_fraction)
        return min(entry, current_stop if current_stop is not None else entry, candidate)

    raise ValueError(f"Invalid side: {side}")



TRAILING_CLOSE_SLIPPAGE = 0.05
TRAILING_CLOSE_MAX_ATTEMPTS = 3
TRAILING_CLOSE_CANCEL_TIMEOUT = 4.0
TRAILING_CLOSE_VERIFY_TIMEOUT = 3.0
TRAILING_CLOSE_POLL_INTERVAL = 0.10

_CLOSE_LOCKS: Dict[Tuple[str, str], asyncio.Lock] = {}


def _coin_dex(coin: str) -> str:
    """Return the HIP-3 DEX prefix or an empty string for native perps."""
    return coin.split(":", 1)[0] if ":" in coin else ""


def _effective_trading_address(exchange: Exchange, account_address: str) -> str:
    """Return the account against which Exchange.market_close resolves positions."""
    vault_address = getattr(exchange, "vault_address", None)
    if vault_address:
        return str(vault_address)

    exchange_account_address = getattr(exchange, "account_address", None)
    if exchange_account_address:
        return str(exchange_account_address)

    if account_address:
        return str(account_address)

    wallet = getattr(exchange, "wallet", None)
    wallet_address = getattr(wallet, "address", None)
    if wallet_address:
        return str(wallet_address)

    raise RuntimeError("Unable to determine the Hyperliquid trading account address.")


def _close_lock(address: str, coin: str) -> asyncio.Lock:
    key = (address.lower(), coin.upper())
    lock = _CLOSE_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _CLOSE_LOCKS[key] = lock
    return lock


def _extract_exchange_error(response: Any) -> Optional[str]:
    """Return the first nested Hyperliquid order/cancel error."""
    if not isinstance(response, dict):
        return None

    outer_status = response.get("status")
    if outer_status == "err":
        return str(response.get("response") or response)

    nested = response.get("response")
    if not isinstance(nested, dict):
        return None

    data = nested.get("data")
    if not isinstance(data, dict):
        return None

    statuses = data.get("statuses")
    if not isinstance(statuses, list):
        return None

    for status in statuses:
        if isinstance(status, dict) and status.get("error") is not None:
            return str(status["error"])

    return None


def _extract_filled_size(response: Any) -> float:
    """Return total filled size from a Hyperliquid order response."""
    total = 0.0
    try:
        statuses = response["response"]["data"]["statuses"]
        for status in statuses:
            if not isinstance(status, dict):
                continue
            filled = status.get("filled")
            if isinstance(filled, dict):
                total += float(filled.get("totalSz", "0"))
    except (KeyError, TypeError, ValueError):
        return total
    return total


def _order_oid(order: Dict[str, Any]) -> Optional[int]:
    value = order.get("oid")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


async def _fetch_live_position_size(
    info: Info,
    account_address: str,
    coin: str,
) -> float:
    """Fetch the authoritative signed position size directly over HTTP."""
    dex = _coin_dex(coin)
    user_state = await info.user_state(account_address, dex)
    if not isinstance(user_state, dict):
        raise RuntimeError(
            f"Unexpected user_state response for {account_address}: "
            f"{type(user_state).__name__}"
        )

    for asset_position in user_state.get("assetPositions", []):
        if not isinstance(asset_position, dict):
            continue
        position = asset_position.get("position")
        if not isinstance(position, dict):
            continue
        if str(position.get("coin", "")).upper() != coin.upper():
            continue
        try:
            return float(position.get("szi", "0"))
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Invalid live position size for {coin}: {position.get('szi')!r}"
            ) from exc

    return 0.0


async def _fetch_open_orders_for_close(
    info: Info,
    account_address: str,
    coin: str,
) -> List[Dict[str, Any]]:
    """Fetch every live order for a coin from both authoritative order endpoints."""
    dex = _coin_dex(coin)
    frontend_orders, open_orders = await asyncio.gather(
        info.frontend_open_orders(account_address, dex),
        info.open_orders(account_address, dex),
    )

    merged: Dict[int, Dict[str, Any]] = {}
    unkeyed: List[Dict[str, Any]] = []

    for collection in (frontend_orders, open_orders):
        if not isinstance(collection, list):
            continue
        for raw_order in collection:
            if not isinstance(raw_order, dict):
                continue
            if str(raw_order.get("coin", "")).upper() != coin.upper():
                continue
            order = dict(raw_order)
            oid = _order_oid(order)
            if oid is None:
                unkeyed.append(order)
            else:
                existing = merged.get(oid, {})
                existing.update(order)
                existing["oid"] = oid
                merged[oid] = existing

    return list(merged.values()) + unkeyed


async def _cancel_all_orders_and_wait(
    info: Info,
    exchange: Exchange,
    account_address: str,
    coin: str,
) -> None:
    """Cancel all coin orders and wait until both order endpoints confirm cleanup."""
    orders = await _fetch_open_orders_for_close(info, account_address, coin)
    oids = list(
        dict.fromkeys(
            oid
            for oid in (_order_oid(order) for order in orders)
            if oid is not None
        )
    )

    if oids:
        details = []
        for order in orders:
            oid = _order_oid(order)
            if oid is None:
                continue
            details.append(
                f"oid={oid} side={order.get('side', order.get('isBuy', 'N/A'))} "
                f"reduceOnly={order.get('reduceOnly', order.get('reduce_only', 'N/A'))} "
                f"isTrigger={order.get('isTrigger', 'N/A')}"
            )
        print(
            f"[CLOSE-CLEANUP] {coin} canceling {len(oids)} open order(s) before "
            f"market close: {'; '.join(details)}"
        )

        cancel_response = await exchange.bulk_cancel(
            [{"coin": coin, "oid": oid} for oid in oids]
        )
        print(f"[CLOSE-CLEANUP] {coin} cancel response: {cancel_response}")

        cancel_error = _extract_exchange_error(cancel_response)
        if cancel_error is not None and "already canceled" not in cancel_error.lower():
            print(f"[CLOSE-CLEANUP-WARN] {coin} cancel response error: {cancel_error}")

    deadline = time.monotonic() + TRAILING_CLOSE_CANCEL_TIMEOUT
    last_remaining: List[Dict[str, Any]] = []

    while True:
        last_remaining = await _fetch_open_orders_for_close(
            info,
            account_address,
            coin,
        )
        remaining_oids = [
            oid
            for oid in (_order_oid(order) for order in last_remaining)
            if oid is not None
        ]

        if not remaining_oids:
            if oids:
                print(
                    f"[CLOSE-CLEANUP] {coin} cleanup confirmed; no open orders remain."
                )
            return

        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"{coin} close aborted because open orders still remain after "
                f"cancellation: {remaining_oids}. This prevents another full-size "
                "reduce-only close from being rejected."
            )

        await asyncio.sleep(TRAILING_CLOSE_POLL_INTERVAL)


async def _print_close_identity_diagnostics(
    info: Info,
    exchange: Exchange,
    configured_account_address: str,
    effective_account_address: str,
    coin: str,
) -> None:
    """Print signer/account diagnostics after a deterministic reduce-only rejection."""
    wallet = getattr(exchange, "wallet", None)
    signer_address = str(getattr(wallet, "address", "UNKNOWN"))
    exchange_account = getattr(exchange, "account_address", None)
    vault_address = getattr(exchange, "vault_address", None)

    print(
        f"[CLOSE-DIAG] coin={coin} signer={signer_address} "
        f"configured_account={configured_account_address} "
        f"exchange.account_address={exchange_account} "
        f"exchange.vault_address={vault_address} "
        f"effective_account={effective_account_address}"
    )

    try:
        signer_role = await info.user_role(signer_address)
        print(f"[CLOSE-DIAG] signer userRole: {signer_role}")
    except Exception as exc:
        print(f"[CLOSE-DIAG-WARN] Could not query signer userRole: {exc}")

    try:
        agents = await info.extra_agents(effective_account_address)
        print(f"[CLOSE-DIAG] effective account extraAgents: {agents}")
    except Exception as exc:
        print(f"[CLOSE-DIAG-WARN] Could not query effective account agents: {exc}")


async def close_position_at_market_verified(
    info: Info,
    exchange: Exchange,
    account_address: str,
    coin: str,
) -> Tuple[bool, Any, Optional[str]]:
    """Cancel conflicting orders, close the live position, and verify it is flat.

    Hyperliquid can return top-level status='ok' while an individual order status
    contains an error. This function treats the nested status as authoritative.
    """
    coin = coin.upper()
    effective_address = _effective_trading_address(exchange, account_address)
    exchange_account = getattr(exchange, "account_address", None)

    if (
        exchange_account
        and not getattr(exchange, "vault_address", None)
        and str(exchange_account).lower() != str(account_address).lower()
    ):
        raise RuntimeError(
            "Trailing manager account mismatch: "
            f"function account_address={account_address}, "
            f"exchange.account_address={exchange_account}."
        )

    async with _close_lock(effective_address, coin):
        last_response: Any = None
        last_error: Optional[str] = None

        for attempt in range(1, TRAILING_CLOSE_MAX_ATTEMPTS + 1):
            live_size_before = await _fetch_live_position_size(
                info,
                effective_address,
                coin,
            )
            if live_size_before == 0.0:
                print(f"[CLOSE] {coin} is already flat.")
                return True, last_response, None

            expected_side = "BUY" if live_size_before < 0.0 else "SELL"
            print(
                f"[CLOSE {attempt}/{TRAILING_CLOSE_MAX_ATTEMPTS}] {coin} "
                f"live_size={live_size_before:.8f} expected_exit_side={expected_side} "
                f"account={effective_address}"
            )

            # Existing TP, SL, and entry orders can conflict with a full-position
            # reduce-only close. Do not merely send cancellations; wait until the
            # exchange's order endpoints confirm that they are gone.
            await _cancel_all_orders_and_wait(
                info,
                exchange,
                effective_address,
                coin,
            )

            live_size_after_cancel = await _fetch_live_position_size(
                info,
                effective_address,
                coin,
            )
            if live_size_after_cancel == 0.0:
                print(f"[CLOSE] {coin} became flat during order cleanup.")
                return True, last_response, None

            if (live_size_after_cancel < 0.0) != (live_size_before < 0.0):
                print(
                    f"[CLOSE-RETRY] {coin} direction changed during cleanup: "
                    f"before={live_size_before:.8f}, after={live_size_after_cancel:.8f}."
                )
                await asyncio.sleep(TRAILING_CLOSE_POLL_INTERVAL)
                continue

            # Leave size unset so the SDK performs its own final authoritative
            # position read and closes exactly that live size.
            last_response = await exchange.market_close(
                coin,
                slippage=TRAILING_CLOSE_SLIPPAGE,
            )
            print(f"[RESULT] {coin} market_close response: {last_response}")

            last_error = _extract_exchange_error(last_response)
            if last_error is not None:
                print(
                    f"[CLOSE-ERROR] {coin} attempt "
                    f"{attempt}/{TRAILING_CLOSE_MAX_ATTEMPTS}: {last_error}"
                )

                live_size_after_error = await _fetch_live_position_size(
                    info,
                    effective_address,
                    coin,
                )
                remaining_orders = await _fetch_open_orders_for_close(
                    info,
                    effective_address,
                    coin,
                )
                remaining_oids = [
                    oid
                    for oid in (_order_oid(order) for order in remaining_orders)
                    if oid is not None
                ]
                print(
                    f"[CLOSE-ERROR] {coin} post-rejection live_size="
                    f"{live_size_after_error:.8f}, remaining_order_oids={remaining_oids}"
                )

                if "reduce only order would increase position" in last_error.lower():
                    await _print_close_identity_diagnostics(
                        info,
                        exchange,
                        account_address,
                        effective_address,
                        coin,
                    )

                if live_size_after_error == 0.0:
                    return True, last_response, None

                if attempt < TRAILING_CLOSE_MAX_ATTEMPTS:
                    await asyncio.sleep(TRAILING_CLOSE_POLL_INTERVAL)
                    continue

                return False, last_response, last_error

            filled_size = _extract_filled_size(last_response)
            deadline = time.monotonic() + TRAILING_CLOSE_VERIFY_TIMEOUT
            live_size_after_close = live_size_after_cancel

            while time.monotonic() < deadline:
                live_size_after_close = await _fetch_live_position_size(
                    info,
                    effective_address,
                    coin,
                )
                if live_size_after_close == 0.0:
                    print(
                        f"[CLOSE-VERIFIED] {coin} is flat; "
                        f"reported_filled_size={filled_size:.8f}."
                    )
                    return True, last_response, None
                await asyncio.sleep(TRAILING_CLOSE_POLL_INTERVAL)

            print(
                f"[CLOSE-PARTIAL] {coin} remains open after IOC: "
                f"before={live_size_after_cancel:.8f}, "
                f"after={live_size_after_close:.8f}, "
                f"reported_filled_size={filled_size:.8f}."
            )

            if attempt < TRAILING_CLOSE_MAX_ATTEMPTS:
                await asyncio.sleep(TRAILING_CLOSE_POLL_INTERVAL)
                continue

            last_error = (
                f"{coin} position was not flat after "
                f"{TRAILING_CLOSE_MAX_ATTEMPTS} verified market-close attempts; "
                f"remaining size={live_size_after_close:.8f}"
            )
            return False, last_response, last_error

        return False, last_response, last_error or f"Unable to close {coin}."


async def trailing_stop_for_all_positions(
    trail_pct: float,
    poll_interval: float,
    use_testnet: bool,
    only_coin: Optional[str] = None,
    use_websocket: bool = True,
    hide_orders: bool = False,
    account_address: Optional[str] = None,
    info: Optional[Info] = None,
    exchange: Optional[Exchange] = None,
) -> None:
    """Trailing stop manager for all open positions or one coin.

    If a managed position is increased in the same direction, the trailing
    anchor and entry are rebased to the new average entry. If the add makes the
    position no longer profitable, trailing is paused until price is back beyond
    the new entry; this avoids closing immediately after scaling in.

    `trail_pct` is the fraction of favorable unrealized profit that may be
    surrendered before exit. Example: 0.33 means retain 67% of the best profit
    observed since arming.
    """
    if not (0.0 < trail_pct < 1.0):
        raise ValueError(f"trail_pct must be between 0 and 1. Got: {trail_pct}")
    if poll_interval <= 0:
        raise ValueError(f"poll_interval must be greater than 0. Got: {poll_interval}")

    def in_profit(side: str, price: float, entry: float) -> bool:
        return price > entry if side == "long" else price < entry

    def reset_trailing_state(st: Dict[str, Any], price: float) -> None:
        side = str(st["side"])
        entry = float(st["entry"])
        st["highest"] = price
        st["lowest"] = price
        st["armed"] = in_profit(side, price, entry)
        if st["armed"]:
            st["stop"] = compute_trailing_stop_px(side, entry, price, trail_pct, entry)
        else:
            st["stop"] = entry

    owns_clients = account_address is None and info is None and exchange is None
    if not owns_clients and (account_address is None or info is None or exchange is None):
        raise RuntimeError("Pass account_address, info, and exchange together when reusing initialized clients.")
    try:
        if owns_clients:
            account_address, info, exchange = await init_clients(use_testnet, use_websocket=use_websocket)
        metrics_start_time_ms = int(time.time() * 1000)

        state: Dict[str, Dict[str, Any]] = {}
        only_coin_upper = only_coin.upper() if only_coin is not None else None
        idle_logged = False

        def build_position_map(open_positions: list[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
            filtered_positions = open_positions
            if only_coin_upper is not None:
                filtered_positions = [
                    pos for pos in open_positions if str(pos.get("coin", "")).upper() == only_coin_upper
                ]
            positions_by_coin: Dict[str, Dict[str, Any]] = {}
            for pos in filtered_positions:
                coin = str(pos.get("coin", "")).upper()
                if not coin:
                    continue
                positions_by_coin[coin] = pos
            return positions_by_coin

        print("============================================================")
        print(" Hyperliquid Async Trailing Stop Manager")
        print("============================================================")
        print(f"Account:       {account_address}")
        print(f"Network:       {'TESTNET' if use_testnet else 'MAINNET'}")
        print(f"Websocket:     {'ENABLED' if use_websocket else 'DISABLED'}")
        print(f"Hide orders:   {hide_orders} (trailing is always local; no TP/SL orders are placed)")
        print(f"Profit giveback: {trail_pct * 100:.4f}%")
        print(f"Poll interval: {poll_interval:.2f} s")
        if only_coin_upper is not None:
            print(f"Scope:         {only_coin_upper}")
        else:
            print("Scope:         ALL")
        print("------------------------------------------------------------")
        print("Trailing manager will keep running until interrupted.")
        print("Press Ctrl+C to exit without closing remaining positions.")
        print("============================================================")

        while True:
            try:
                open_positions = await get_all_open_positions(info, account_address)
            except Exception as exc:
                print(f"[WARN] Failed to fetch open positions: {exc}. Retrying...")
                await asyncio.sleep(poll_interval)
                continue

            positions_by_coin = build_position_map(open_positions)

            if not positions_by_coin and not state:
                if not idle_logged:
                    if only_coin_upper is not None:
                        print(f"[WAIT] No open positions found for {only_coin_upper}. Waiting for one to appear...")
                    else:
                        print("[WAIT] No open perp positions found. Waiting for positions to appear...")
                    idle_logged = True
                await asyncio.sleep(poll_interval)
                continue

            try:
                mids = await get_all_mids(info)
            except Exception as exc:
                print(f"[WARN] Failed to fetch mids: {exc}. Retrying...")
                await asyncio.sleep(poll_interval)
                continue

            if idle_logged and positions_by_coin:
                print("[WATCH] Position detected. Starting trailing management.")
                idle_logged = False

            for coin in list(state.keys()):
                if coin not in positions_by_coin:
                    print(f"[DONE] {coin} is now flat; trailing state cleared.")
                    del state[coin]

            for coin, pos in positions_by_coin.items():
                if coin in state:
                    continue
                if coin not in mids:
                    print(f"[WARN] No mid price for {coin}, skipping initialization until a mid is available.")
                    continue

                if not coin:
                    print(f"[WARN] Position missing coin field, skipping: {pos}")
                    continue

                try:
                    size = float(pos["szi"])
                    entry_px = float(pos["entryPx"])
                    current_price = float(mids[coin])
                except KeyError as exc:
                    print(f"[WARN] Position for {coin} missing field {exc}, skipping: {pos}")
                    continue
                except (TypeError, ValueError) as exc:
                    print(f"[WARN] Could not parse position/mid values for {coin}: {exc}. Position: {pos}")
                    continue

                if size == 0.0:
                    continue

                side = "long" if size > 0.0 else "short"
                entry_px_str = pos.get("entryPx", "N/A")
                st = {
                    "coin": coin,
                    "size": size,
                    "side": side,
                    "entry": entry_px,
                    "entryPx": entry_px_str,
                    "highest": current_price,
                    "lowest": current_price,
                    "stop": entry_px,
                    "armed": False,
                }
                reset_trailing_state(st, current_price)
                state[coin] = st

                armed = "YES" if st.get("armed") else "NO"
                print(
                    f"[TRACK] Coin: {coin:>8} | Side: {st['side']:>5} | Size: {st['size']:.8f} | "
                    f"Entry: {st['entryPx']} | Initial mid: {current_price:.8f} | "
                    f"Initial stop: {st['stop']:.8f} | Armed: {armed}"
                )
                if not st["armed"]:
                    print(
                        f"[!] {side.upper()} {coin} is not in profit yet: current={current_price:.8f}, "
                        f"entry={entry_px:.8f}. It will be armed once price is profitable."
                    )

            for coin, st in list(state.items()):
                pos = positions_by_coin.get(coin)
                if pos is None:
                    continue

                try:
                    _, live_size, live_entry_px, live_side, _ = parse_position_snapshot(pos)
                except RuntimeError as exc:
                    print(f"[WARN] {exc}")
                    continue

                if live_side != st["side"]:
                    print(
                        f"[DONE] {coin} position direction changed from {st['side']} to {live_side}; "
                        "resetting trailing state."
                    )
                    del state[coin]
                    continue

                try:
                    price = float(mids[coin])
                except (TypeError, ValueError) as exc:
                    print(f"[WARN] Could not parse mid price for {coin}: {mids[coin]!r}. Error: {exc}")
                    continue

                unrealized_pnl = compute_position_unrealized_pnl(pos, price)
                metrics = await get_account_runtime_metrics(info, account_address, metrics_start_time_ms, coin=coin)
                if position_is_directional_add(float(st["size"]), live_size):
                    st["size"] = live_size
                    st["entry"] = live_entry_px
                    st["entryPx"] = f"{live_entry_px:.8f}"
                    reset_trailing_state(st, price)
                    print(
                        f"[TRAIL-REBASE] {coin} position increased to {live_size:.8f}; "
                        f"entry rebased to {live_entry_px:.8f}, trailing anchor reset to {price:.8f}, "
                        f"stop recalculated to {st['stop']:.8f}, armed={st['armed']}."
                    )
                else:
                    st["size"] = live_size
                    st["entry"] = live_entry_px
                    st["entryPx"] = f"{live_entry_px:.8f}"

                side = st["side"]
                if not st.get("armed", True):
                    if in_profit(side, price, float(st["entry"])):
                        reset_trailing_state(st, price)
                        print(
                            f"[TRAIL-ARM] {coin} is profitable again; trailing armed with stop {st['stop']:.8f}."
                        )
                    else:
                        pnl_str = f"{unrealized_pnl:.8f}" if unrealized_pnl is not None else "N/A"
                        print(
                            f"[INFO] {coin} | Side: {side} | Size: {st['size']:.8f} | Last: {price:.8f} | "
                            f"Entry: {st['entry']:.8f} | uPnL: {pnl_str} | {format_account_metrics(metrics)} | Stop: PAUSED"
                        )
                        continue

                if side == "long":
                    if price > st["highest"]:
                        st["highest"] = price
                        st["stop"] = compute_trailing_stop_px(side, st["entry"], st["highest"], trail_pct, st["stop"])
                        print(f"[LONG] {coin} new high {st['highest']:.8f}, moved stop to {st['stop']:.8f}")

                    if price <= st["stop"]:
                        print(f"[LONG] {coin} stop hit! Price {price:.8f} <= stop {st['stop']:.8f}. Closing...")
                        try:
                            close_ok, result, close_error = await close_position_at_market_verified(
                                info,
                                exchange,
                                account_address,
                                coin,
                            )
                            if close_ok:
                                state.pop(coin, None)
                                continue
                            print(
                                f"[ERROR] Failed to close {coin} position: "
                                f"{close_error or result}"
                            )
                        except Exception as exc:
                            print(f"[ERROR] Failed to close {coin} position: {exc}")
                else:
                    if price < st["lowest"]:
                        st["lowest"] = price
                        st["stop"] = compute_trailing_stop_px(side, st["entry"], st["lowest"], trail_pct, st["stop"])
                        print(f"[SHORT] {coin} new low {st['lowest']:.8f}, moved stop to {st['stop']:.8f}")

                    if price >= st["stop"]:
                        print(f"[SHORT] {coin} stop hit! Price {price:.8f} >= stop {st['stop']:.8f}. Closing...")
                        try:
                            close_ok, result, close_error = await close_position_at_market_verified(
                                info,
                                exchange,
                                account_address,
                                coin,
                            )
                            if close_ok:
                                state.pop(coin, None)
                                continue
                            print(
                                f"[ERROR] Failed to close {coin} position: "
                                f"{close_error or result}"
                            )
                        except Exception as exc:
                            print(f"[ERROR] Failed to close {coin} position: {exc}")

                pnl_str = f"{unrealized_pnl:.8f}" if unrealized_pnl is not None else "N/A"
                print(
                    f"[INFO] {coin} | Side: {side} | Size: {st['size']:.8f} | Last: {price:.8f} | "
                    f"Entry: {st['entry']:.8f} | uPnL: {pnl_str} | {format_account_metrics(metrics)} | Stop: {st['stop']:.8f}"
                )

            await asyncio.sleep(poll_interval)
    except KeyboardInterrupt:
        print("\n[!] Caught Ctrl+C, exiting without closing remaining positions.")
    finally:
        if owns_clients:
            await close_clients(info, exchange)