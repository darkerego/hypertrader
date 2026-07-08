# ---------------------------------------------------------------------------
# Trailing stop manager
# ---------------------------------------------------------------------------
import asyncio
import time
from typing import Optional, Dict, Any

from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

from utils.helpers import init_clients, get_all_open_positions, get_all_mids, get_position_for_coin, \
    parse_position_snapshot, compute_position_unrealized_pnl, get_account_runtime_metrics, position_is_directional_add, \
    format_account_metrics, close_clients


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
            if side == "long":
                st["stop"] = max(entry, price * (1.0 - trail_pct))
            else:
                st["stop"] = min(entry, price * (1.0 + trail_pct))
        else:
            st["stop"] = entry

    owns_clients = account_address is None and info is None and exchange is None
    if not owns_clients and (account_address is None or info is None or exchange is None):
        raise RuntimeError("Pass account_address, info, and exchange together when reusing initialized clients.")
    try:
        if owns_clients:
            account_address, info, exchange = await init_clients(use_testnet, use_websocket=use_websocket)
        metrics_start_time_ms = int(time.time() * 1000)
        open_positions = await get_all_open_positions(info, account_address)
        if only_coin is not None:
            only_coin_upper = only_coin.upper()
            open_positions = [
                pos for pos in open_positions if str(pos.get("coin", "")).upper() == only_coin_upper
            ]

        if not open_positions:
            if only_coin:
                print(f"[!] No open positions found for {only_coin} on this account.")
            else:
                print("[!] No open perp positions found on this account.")
            return

        try:
            mids = await get_all_mids(info)
        except Exception as exc:
            print(f"[ERROR] Failed to fetch mids before starting trailing stop manager: {exc}")
            return

        state: Dict[str, Dict[str, Any]] = {}

        for pos in open_positions:
            coin = pos.get("coin")
            if not coin:
                print(f"[WARN] Position missing coin field, skipping: {pos}")
                continue
            coin = str(coin)

            if coin not in mids:
                print(f"[WARN] No mid price for {coin}, skipping.")
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
                "active": True,
                "armed": False,
            }
            reset_trailing_state(st, current_price)
            if not st["armed"]:
                print(
                    f"[!] {side.upper()} {coin} is not in profit yet: current={current_price:.8f}, "
                    f"entry={entry_px:.8f}. It will be armed once price is profitable."
                )
            state[coin] = st

        if not state:
            print("[!] No valid positions to manage trailing stops for.")
            return

        print("============================================================")
        print(" Hyperliquid Async Trailing Stop Manager")
        print("============================================================")
        print(f"Account:       {account_address}")
        print(f"Network:       {'TESTNET' if use_testnet else 'MAINNET'}")
        print(f"Websocket:     {'ENABLED' if use_websocket else 'DISABLED'}")
        print(f"Hide orders:   {hide_orders} (trailing is always local; no TP/SL orders are placed)")
        print(f"Trail percent: {trail_pct * 100:.4f}%")
        print(f"Poll interval: {poll_interval:.2f} s")
        print("------------------------------------------------------------")

        for coin, st in state.items():
            armed = "YES" if st.get("armed") else "NO"
            print(
                f"Coin: {coin:>8} | Side: {st['side']:>5} | Size: {st['size']:.8f} | "
                f"Entry: {st['entryPx']} | Initial mid: {float(mids.get(coin, 0.0)):.8f} | "
                f"Initial stop: {st['stop']:.8f} | Armed: {armed}"
            )

        print("------------------------------------------------------------")
        print("Press Ctrl+C to exit without closing remaining positions.")
        print("============================================================")

        while True:
            if not any(st["active"] for st in state.values()):
                print("[DONE] All managed positions have been closed.")
                break

            await asyncio.sleep(poll_interval)

            try:
                mids = await get_all_mids(info)
            except Exception as exc:
                print(f"[WARN] Failed to fetch mids: {exc}. Retrying...")
                continue

            for coin, st in list(state.items()):
                if not st["active"]:
                    continue
                if coin not in mids:
                    print(f"[WARN] Mid price for {coin} missing in this poll, skipping.")
                    continue

                pos = await get_position_for_coin(info, account_address, coin)
                if pos is None:
                    print(f"[DONE] {coin} is now flat; trailing state cleared.")
                    st["active"] = False
                    continue

                try:
                    _, live_size, live_entry_px, live_side, _ = parse_position_snapshot(pos)
                except RuntimeError as exc:
                    print(f"[WARN] {exc}")
                    continue

                if live_side != st["side"]:
                    print(
                        f"[DONE] {coin} position direction changed from {st['side']} to {live_side}; "
                        "stopping this trailing state."
                    )
                    st["active"] = False
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
                            f"Entry: {st['entry']:.8f} | uPnL: {pnl_str} | {format_account_metrics(metrics)} | Stop: PAUSED | Active: {st['active']}"
                        )
                        continue

                if side == "long":
                    if price > st["highest"]:
                        st["highest"] = price
                        st["stop"] = max(st["entry"], st["highest"] * (1.0 - trail_pct))
                        print(f"[LONG] {coin} new high {st['highest']:.8f}, moved stop to {st['stop']:.8f}")

                    if price <= st["stop"]:
                        print(f"[LONG] {coin} stop hit! Price {price:.8f} <= stop {st['stop']:.8f}. Closing...")
                        try:
                            result = await exchange.market_close(coin)
                            print(f"[RESULT] {coin} market_close response: {result}")
                            st["active"] = False
                        except Exception as exc:
                            print(f"[ERROR] Failed to close {coin} position: {exc}")
                            st["active"] = True
                else:
                    if price < st["lowest"]:
                        st["lowest"] = price
                        st["stop"] = min(st["entry"], st["lowest"] * (1.0 + trail_pct))
                        print(f"[SHORT] {coin} new low {st['lowest']:.8f}, moved stop to {st['stop']:.8f}")

                    if price >= st["stop"]:
                        print(f"[SHORT] {coin} stop hit! Price {price:.8f} >= stop {st['stop']:.8f}. Closing...")
                        try:
                            result = await exchange.market_close(coin)
                            print(f"[RESULT] {coin} market_close response: {result}")
                            st["active"] = False
                        except Exception as exc:
                            print(f"[ERROR] Failed to close {coin} position: {exc}")
                            st["active"] = True

                pnl_str = f"{unrealized_pnl:.8f}" if unrealized_pnl is not None else "N/A"
                print(
                    f"[INFO] {coin} | Side: {side} | Size: {st['size']:.8f} | Last: {price:.8f} | "
                    f"Entry: {st['entry']:.8f} | uPnL: {pnl_str} | {format_account_metrics(metrics)} | Stop: {st['stop']:.8f} | Active: {st['active']}"
                )
    except KeyboardInterrupt:
        print("\n[!] Caught Ctrl+C, exiting without closing remaining positions.")
    finally:
        if owns_clients:
            await close_clients(info, exchange)
