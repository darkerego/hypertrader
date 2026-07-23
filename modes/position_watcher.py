import asyncio
import logging
import time
from typing import Optional, List, Dict, Any

from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

from modes.position_management import monitor_bracket_position, rebuild_bracket_orders
from utils.constants import WATCH_RETRY_SLEEP_SECONDS
from utils.helpers import init_clients, get_all_open_positions, get_all_mids, compute_position_unrealized_pnl, \
    get_account_runtime_metrics, format_account_metrics, is_rate_limit_error, close_clients, \
    compute_default_stop_loss_pct, hyperliquid_market_ids_match, normalize_hyperliquid_market_id


async def attach_bracket_to_existing_position(
    info: Info,
    exchange: Exchange,
    account_address: str,
    position: Dict[str, Any],
    take_profit_pct: Optional[float],
    stop_loss_pct: Optional[float],
    take_profit_levels: int,
    use_trailing_tp: bool,
    trailing_tp_trigger_level: int,
    trailing_tp_profit_pct: float,
    poll_interval: float,
    tp_reversal_pct: Optional[float],
    tp_tif: str,
    market_slippage: float,
    cancel_existing_tpsl: bool,
    tp_reversal_limit_exit: bool,
    tp_reversal_stop_buffer_pct: Optional[float],
    hide_orders: bool = False,
    metrics_start_time_ms: Optional[int] = None,
) -> None:
    """Attach TP/SL orders to an already-open position and monitor it."""
    coin = normalize_hyperliquid_market_id(str(position.get("coin", "")))
    if not coin:
        raise RuntimeError(f"Cannot manage position without coin field: {position}")

    try:
        current_size = float(position["szi"])
        entry_px = float(position["entryPx"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"Could not parse open position for bracket attachment: {position}") from exc

    if current_size == 0.0:
        print(f"[WATCH] {coin} is already flat; nothing to attach.")
        return

    side = "long" if current_size > 0.0 else "short"
    pos_abs = abs(current_size)

    if take_profit_pct is None and stop_loss_pct is None:
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
    if tp_tif not in ("Alo", "Gtc"):
        raise RuntimeError("--tp-tif must be Alo or Gtc.")
    if market_slippage < 0.0:
        raise RuntimeError("--market-slippage must be >= 0.")

    print("============================================================")
    print(" Async Bracket Attach")
    print("============================================================")
    print(f"Account:          {account_address}")
    print(f"Coin:             {coin}")
    print(f"Side:             {side}")
    print(f"Signed size:      {current_size:.8f}")
    print(f"Managed size:     {pos_abs:.8f}")
    print(f"Entry:            {entry_px:.8f}")
    print(f"Hide orders:      {hide_orders}")
    print(f"Take profit pct:  {take_profit_pct * 100:.4f}%" if take_profit_pct is not None else "Take profit pct:  N/A")
    print(f"Trailing TP:      {use_trailing_tp}")
    if use_trailing_tp:
        print(f"Trailing TP level: {trailing_tp_trigger_level}")
        print(f"Trailing TP pct:  {trailing_tp_profit_pct * 100:.4f}%")
    print(f"Stop loss pct:    {stop_loss_pct * 100:.4f}%" if stop_loss_pct is not None else "Stop loss pct:    N/A")
    print(f"TP levels:        {take_profit_levels}")
    print("============================================================")

    if not cancel_existing_tpsl:
        print("[WATCH] Existing reduce-only TP/SL orders will be left untouched before placing this bracket.")

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
        stop_loss_trigger_px=None,
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
        stop_loss_trigger_px=None,
        take_profit_levels=take_profit_levels,
        tp_tif=tp_tif,
        tp_reversal_limit_exit=tp_reversal_limit_exit,
        tp_reversal_stop_buffer_pct=tp_reversal_stop_buffer_pct,
        hide_orders=hide_orders,
        use_trailing_tp=use_trailing_tp,
        trailing_tp_trigger_level=trailing_tp_trigger_level,
        trailing_tp_profit_pct=trailing_tp_profit_pct,
        metrics_start_time_ms=metrics_start_time_ms,
    )

async def _watched_position_task(
    info: Info,
    exchange: Exchange,
    account_address: str,
    position: Dict[str, Any],
    take_profit_pct: Optional[float],
    stop_loss_pct: Optional[float],
    take_profit_levels: int,
    use_trailing_tp: bool,
    trailing_tp_trigger_level: int,
    trailing_tp_profit_pct: float,
    poll_interval: float,
    tp_reversal_pct: Optional[float],
    tp_tif: str,
    market_slippage: float,
    cancel_existing_tpsl: bool,
    tp_reversal_limit_exit: bool,
    tp_reversal_stop_buffer_pct: Optional[float],
    hide_orders: bool = False,
    metrics_start_time_ms: Optional[int] = None,
) -> None:
    """Task wrapper for one watched position so watcher loop keeps running after errors."""
    coin = str(position.get("coin", "UNKNOWN")).upper()
    try:
        while True:
            try:
                await attach_bracket_to_existing_position(
                    info=info,
                    exchange=exchange,
                    account_address=account_address,
                    position=position,
                    take_profit_pct=take_profit_pct,
                    stop_loss_pct=stop_loss_pct,
                    take_profit_levels=take_profit_levels,
                    use_trailing_tp=use_trailing_tp,
                    trailing_tp_trigger_level=trailing_tp_trigger_level,
                    trailing_tp_profit_pct=trailing_tp_profit_pct,
                    poll_interval=poll_interval,
                    tp_reversal_pct=tp_reversal_pct,
                    tp_tif=tp_tif,
                    market_slippage=market_slippage,
                    cancel_existing_tpsl=cancel_existing_tpsl,
                    tp_reversal_limit_exit=tp_reversal_limit_exit,
                    tp_reversal_stop_buffer_pct=tp_reversal_stop_buffer_pct,
                    hide_orders=hide_orders,
                    metrics_start_time_ms=metrics_start_time_ms,
                )
                print(f"[WATCH] Management task for {coin} finished.")
                return
            except Exception as exc:
                if not is_rate_limit_error(exc):
                    raise
                print(
                    f"[WATCH-RETRY] Management task for {coin} hit Hyperliquid rate limit ({exc}). "
                    f"Sleeping {WATCH_RETRY_SLEEP_SECONDS:.1f}s before retry."
                )
                await asyncio.sleep(WATCH_RETRY_SLEEP_SECONDS)
    except asyncio.CancelledError:
        print(f"[WATCH] Management task for {coin} canceled; leaving position/orders untouched.")
        raise
    except Exception:
        logging.getLogger("hypertrader").exception("[WATCH-ERROR] Management task for %s failed.", coin)


async def run_position_watcher(
    only_coin: Optional[str],
    take_profit_pct: Optional[float],
    stop_loss_pct: Optional[float],
    take_profit_levels: int,
    use_trailing_tp: bool,
    trailing_tp_trigger_level: int,
    trailing_tp_profit_pct: float,
    poll_interval: float,
    tp_reversal_pct: Optional[float],
    tp_tif: str,
    market_slippage: float,
    cancel_existing_tpsl: bool,
    manage_existing: bool,
    tp_reversal_limit_exit: bool,
    tp_reversal_stop_buffer_pct: Optional[float],
    use_testnet: bool,
    use_websocket: bool = True,
    hide_orders: bool = False,
    account_address: Optional[str] = None,
    info: Optional[Info] = None,
    exchange: Optional[Exchange] = None,
) -> None:
    """Watch account positions and attach TP/SL management to new open positions."""
    if take_profit_pct is None and stop_loss_pct is None:
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
    if poll_interval <= 0.0:
        raise RuntimeError("--poll-interval must be > 0.")
    if tp_tif not in ("Alo", "Gtc"):
        raise RuntimeError("--tp-tif must be Alo or Gtc.")
    if market_slippage < 0.0:
        raise RuntimeError("--market-slippage must be >= 0.")

    normalized_only_coin = normalize_hyperliquid_market_id(only_coin) if only_coin else None
    owns_clients = account_address is None and info is None and exchange is None
    if not owns_clients and (account_address is None or info is None or exchange is None):
        raise RuntimeError("Pass account_address, info, and exchange together when reusing initialized clients.")
    managed_tasks: Dict[str, asyncio.Task[None]] = {}
    ignored_initial_coins: set[str] = set()

    try:
        if owns_clients:
            account_address, info, exchange = await init_clients(use_testnet, use_websocket=use_websocket)
        metrics_start_time_ms = int(time.time() * 1000)
        initial_positions = await get_all_open_positions(info, account_address)
        if normalized_only_coin is not None:
            initial_positions = [
                pos for pos in initial_positions
                if hyperliquid_market_ids_match(str(pos.get("coin", "")), normalized_only_coin)
            ]
        ignored_initial_coins = {
            normalize_hyperliquid_market_id(str(pos.get("coin", "")))
            for pos in initial_positions
            if str(pos.get("coin", "")).strip()
        }

        print("============================================================")
        print(" Hyperliquid Async Position Watcher")
        print("============================================================")
        print(f"Account:           {account_address}")
        print(f"Network:           {'TESTNET' if use_testnet else 'MAINNET'}")
        print(f"Websocket:         {'ENABLED' if use_websocket else 'DISABLED'}")
        print(f"Hide orders:       {hide_orders}")
        print(f"Coin filter:       {normalized_only_coin if normalized_only_coin else 'ALL'}")
        print(f"Take profit pct:   {take_profit_pct * 100:.4f}%" if take_profit_pct is not None else "Take profit pct:   N/A")
        print(f"Trailing TP:       {use_trailing_tp}")
        if use_trailing_tp:
            print(f"Trailing TP level: {trailing_tp_trigger_level}")
            print(f"Trailing TP pct:   {trailing_tp_profit_pct * 100:.4f}%")
        print(f"Stop loss pct:     {stop_loss_pct * 100:.4f}%" if stop_loss_pct is not None else "Stop loss pct:     N/A")
        print(f"TP levels:         {take_profit_levels}")
        print(f"Poll interval:     {poll_interval:.2f}s")
        print(f"Manage existing:   {manage_existing}")
        print("------------------------------------------------------------")
        if ignored_initial_coins and not manage_existing:
            print(
                "[WATCH] Existing positions at startup will be ignored until they are flat and reopened: "
                + ", ".join(sorted(ignored_initial_coins))
            )
        elif ignored_initial_coins and manage_existing:
            print("[WATCH] Existing positions will be managed immediately: " + ", ".join(sorted(ignored_initial_coins)))
        else:
            print("[WATCH] No matching open positions at startup.")
        print("Press Ctrl+C to stop watcher. Active positions/orders are left untouched.")
        print("============================================================")

        while True:
            finished = [coin for coin, task in managed_tasks.items() if task.done()]
            for coin in finished:
                managed_tasks.pop(coin, None)

            try:
                positions = await get_all_open_positions(info, account_address)
            except Exception as exc:
                print(f"[WATCH-WARN] Failed to fetch open positions: {exc}")
                await asyncio.sleep(poll_interval)
                continue

            mids: Dict[str, Any] = {}
            try:
                mids = await get_all_mids(info)
            except Exception as exc:
                print(f"[WATCH-WARN] Failed to fetch mids for uPnL display: {exc}")

            current_open_coins: set[str] = set()
            for pos in positions:
                coin = normalize_hyperliquid_market_id(str(pos.get("coin", "")))
                if not coin:
                    continue
                if normalized_only_coin is not None and not hyperliquid_market_ids_match(coin, normalized_only_coin):
                    continue
                current_open_coins.add(coin)

                try:
                    signed_size = float(pos.get("szi", "0"))
                    entry_px = float(pos.get("entryPx", "0"))
                except (TypeError, ValueError):
                    print(f"[WATCH-WARN] Could not parse position for {coin}: {pos}")
                    continue
                side = "LONG" if signed_size > 0.0 else "SHORT"
                upnl_str = "N/A"
                mid_price_raw = mids.get(coin)
                if mid_price_raw is not None:
                    try:
                        unrealized_pnl = compute_position_unrealized_pnl(pos, float(mid_price_raw))
                    except (TypeError, ValueError):
                        unrealized_pnl = None
                    if unrealized_pnl is not None:
                        upnl_str = f"{unrealized_pnl:.8f}"
                metrics = await get_account_runtime_metrics(info, account_address, metrics_start_time_ms, coin=coin)
                print(
                    f"[WATCH] {coin} {side} signed_size={signed_size:.8f} "
                    f"entry={entry_px:.8f} upnl={upnl_str} {format_account_metrics(metrics)}"
                )

                existing_task = managed_tasks.get(coin)
                if existing_task is not None and not existing_task.done():
                    continue

                if not manage_existing and coin in ignored_initial_coins:
                    continue

                print(
                    f"[WATCH] New unmanaged position detected: {coin} {side} "
                    f"signed_size={signed_size:.8f}, entry={entry_px:.8f}. Attaching TP/SL."
                )
                managed_tasks[coin] = asyncio.create_task(
                    _watched_position_task(
                        info=info,
                        exchange=exchange,
                        account_address=account_address,
                        position=dict(pos),
                        take_profit_pct=take_profit_pct,
                        stop_loss_pct=stop_loss_pct,
                        take_profit_levels=take_profit_levels,
                        use_trailing_tp=use_trailing_tp,
                        trailing_tp_trigger_level=trailing_tp_trigger_level,
                        trailing_tp_profit_pct=trailing_tp_profit_pct,
                        poll_interval=poll_interval,
                        tp_reversal_pct=tp_reversal_pct,
                        tp_tif=tp_tif,
                        market_slippage=market_slippage,
                        cancel_existing_tpsl=cancel_existing_tpsl,
                        tp_reversal_limit_exit=tp_reversal_limit_exit,
                        tp_reversal_stop_buffer_pct=tp_reversal_stop_buffer_pct,
                        hide_orders=hide_orders,
                        metrics_start_time_ms=metrics_start_time_ms,
                    ),
                    name=f"bracket-watch-{coin}",
                )

            for coin in list(ignored_initial_coins):
                if coin not in current_open_coins:
                    ignored_initial_coins.remove(coin)
                    print(f"[WATCH] Startup position {coin} is now flat; a future reopened position will be managed.")

            managed_label = ", ".join(sorted(managed_tasks)) if managed_tasks else "none"
            ignored_label = ", ".join(sorted(ignored_initial_coins)) if ignored_initial_coins else "none"
            print(f"[WATCH] open={sorted(current_open_coins)} managed={managed_label} ignored_startup={ignored_label}")
            await asyncio.sleep(poll_interval)
    except KeyboardInterrupt:
        print("\n[!] Caught Ctrl+C, stopping position watcher.")
    finally:
        if managed_tasks:
            print(f"[WATCH] Canceling {len(managed_tasks)} local monitor task(s); exchange orders are left untouched.")
            for task in managed_tasks.values():
                task.cancel()
            await asyncio.gather(*managed_tasks.values(), return_exceptions=True)
        if owns_clients:
            await close_clients(info, exchange)
