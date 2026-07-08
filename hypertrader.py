#!/usr/bin/env python3
"""Async Hyperliquid trading helper.

Modes:
  trailing      Manage trailing stops for currently open perp positions.
  market_maker  Event-driven maker ladder that rebalances on drift.
  enter         Smart limit-entry bracket bot with stop-loss and take-profit logic.
  watch         Watch for newly opened positions and automatically attach TP/SL logic.
  auto          TA-Lib multi-timeframe signal scanner with automatic entry and TP/SL.

Environment:
  HYPERLIQUID_SECRET_KEY      Private key for the trading wallet or API wallet.
  HYPERLIQUID_ACCOUNT_ADDRESS Main wallet address / account address.

This version is written for darkerego/hyperliquid-python-sdk-async.
Websocket market data is enabled by default; pass --no-websocket to force HTTP polling.
"""
import argparse
import asyncio
import decimal
import logging
import os
import sys
from typing import Any, Dict, List, Optional

import uvloop
from hyperliquid.info import Info

from modes.auto_trader import run_auto_trader
from modes.market_maker import run_market_maker
from modes.position_management import run_bracket_entry
from modes.position_watcher import run_position_watcher
from modes.trailing_stop import trailing_stop_for_all_positions
from utils.constants import DEFAULT_LOG_FILE, INTERVAL_TO_MS
from utils.helpers import parse_fractional_pct
from utils.style import install_pretty_stdout

try:
    import numpy as np
    import talib
except Exception:  # TA-Lib is only required for the auto command.
    np = None  # type: ignore[assignment]
    talib = None  # type: ignore[assignment]

try:
    os.mkdir("logs")
except FileExistsError:
    pass

decimal.getcontext().prec = 4
install_pretty_stdout()


def configure_runtime_logging(log_path: Optional[str] = None) -> str:
    """Configure file-only runtime logging for exceptions and warnings."""
    resolved_path = os.path.abspath(log_path or DEFAULT_LOG_FILE)
    os.makedirs(os.path.dirname(resolved_path), exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(resolved_path, encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    root_logger.addHandler(file_handler)

    runtime_logger = logging.getLogger("hypertrader")
    runtime_logger.setLevel(logging.INFO)
    runtime_logger.propagate = True
    logging.captureWarnings(True)

    def _log_uncaught_exception(exc_type: type[BaseException], exc_value: BaseException, exc_traceback: Any) -> None:
        if issubclass(exc_type, KeyboardInterrupt):
            runtime_logger.info("[INTERRUPTED] KeyboardInterrupt received.")
            return
        runtime_logger.exception(
            "[UNCAUGHT] Top-level exception",
            exc_info=(exc_type, exc_value, exc_traceback),
        )

    sys.excepthook = _log_uncaught_exception
    return resolved_path


def install_asyncio_exception_logging(loop: asyncio.AbstractEventLoop) -> None:
    """Log unhandled asyncio task/transport exceptions with traceback."""

    def _handle_asyncio_exception(loop: asyncio.AbstractEventLoop, context: Dict[str, Any]) -> None:
        logger = logging.getLogger("hypertrader")
        exc = context.get("exception")
        message = context.get("message", "Unhandled asyncio exception")
        if exc is not None:
            logger.exception("[ASYNCIO] %s", message, exc_info=(type(exc), exc, exc.__traceback__))
            return
        logger.error("[ASYNCIO] %s | context=%r", message, context)

    loop.set_exception_handler(_handle_asyncio_exception)


# ---------------------------------------------------------------------------
# Async SDK compatibility
# ---------------------------------------------------------------------------

async def _compat_name_to_asset(self: Info, name: str) -> int:
    """Compatibility shim for async SDK revisions missing Info.name_to_asset."""
    if hasattr(self, "_ensure_initialized"):
        await self._ensure_initialized()  # type: ignore[attr-defined]

    name_to_coin = getattr(self, "name_to_coin", {})
    coin_to_asset = getattr(self, "coin_to_asset", {})
    coin = name_to_coin.get(name, name)
    if coin not in coin_to_asset:
        raise KeyError(f"No Hyperliquid asset id found for {name!r}; known names include {list(name_to_coin)[:20]}")
    return int(coin_to_asset[coin])


def install_async_sdk_compat_shims() -> None:
    """Install tiny runtime shims for async SDK branches that changed helper methods."""
    if not hasattr(Info, "name_to_asset"):
        setattr(Info, "name_to_asset", _compat_name_to_asset)


install_async_sdk_compat_shims()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_optional_pct(value: Optional[str]) -> Optional[float]:
    """argparse helper accepting omitted optional decimal fractions."""
    if value is None:
        return None
    return float(value)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Async Hyperliquid CLI: trailing stop manager, bracket entry bot, and event-driven market maker.\n\n"
            "Subcommands:\n"
            "  trailing      Manage trailing stops for open perp positions.\n"
            "  enter         Enter a position, place stop-market protection, and manage TP ladder.\n"
            "  market_maker  Event-driven MM: build a pyramiding ladder around recent volatility."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    trailing_parser = subparsers.add_parser("trailing", help="Trailing stop manager for open perp positions.")
    trailing_parser.add_argument("coin", type=str, nargs="?", default=None, help="Optional coin symbol, e.g. BTC, ETH.")
    trailing_parser.add_argument("--trail-pct", type=float, default=0.01,
                                 help="Trailing distance fraction, e.g. 0.01 = 1pct.")
    trailing_parser.add_argument("--poll-interval", type=float, default=2.0,
                                 help="Polling interval in seconds. Default: 2.0.")
    trailing_parser.add_argument("--testnet", action="store_true", help="Use Hyperliquid testnet instead of mainnet.")
    trailing_parser.add_argument("--no-websocket", action="store_true",
                                 help="Disable websocket market-data cache and use HTTP polling only.")
    trailing_parser.add_argument("--hide-orders", "-ho", action="store_true",
                                 help="Accepted for consistency; trailing mode already keeps stops local and hidden.")

    enter_parser = subparsers.add_parser(
        "enter",
        help="Enter a position with limit reposting, market fallback, stop-loss, and TP ladder.",
    )
    enter_parser.add_argument("coin", type=str, help="Perpetual coin symbol, e.g. BTC, ETH, HYPE.")
    enter_parser.add_argument("direction", type=str, choices=["long", "short"], help="Direction to enter.")
    enter_parser.add_argument("--size", type=float, required=True, help="Contract size to enter.")
    enter_parser.add_argument("--take-profit-pct", type=float, default=None,
                              help="Take-profit target fraction, e.g. 0.01 = 1pct.")
    enter_parser.add_argument("--stop-loss-pct", type=float, default=None,
                              help="Stop-loss fraction. If omitted with TP, defaults to TP * 0.5.")
    enter_parser.add_argument("--take-profit-levels", type=int, default=4,
                              help="Number of weighted TP limit levels. Default: 4.")
    enter_parser.add_argument("--trailing-tp", action="store_true",
                              help="Place the TP ladder, then after the configured TP level fills cancel the remaining ladder and switch to a local trailing take-profit.")
    enter_parser.add_argument("--trailing-tp-trigger-level", type=int, default=1,
                              help="TP ladder level that must fill before trailing TP activates. Default: 1.")
    enter_parser.add_argument("--trailing-tp-profit-pct", type=float, default=0.25,
                              help="Trailing distance as a fraction of favorable unrealized profit after the first TP level is hit. Default: 0.25.")
    enter_parser.add_argument("--entry-retries", type=int, default=5,
                              help="Number of top-of-book limit repost attempts. Default: 5.")
    enter_parser.add_argument("--entry-repost-interval", type=float, default=0.3,
                              help="Seconds between entry reposts. Default: 1.0.")
    enter_parser.add_argument("--poll-interval", type=float, default=1.0,
                              help="Seconds between bracket monitor polls. Default: 1.0.")
    enter_parser.add_argument("--tp-reversal-pct", type=float, default=None,
                              help="Exit if price reverses this fraction after first TP zone.")
    enter_parser.add_argument("--tp-reversal-limit-exit", action="store_true",
                              help="On TP reversal, try a reduce-only limit exit plus protective stop before falling back to market.")
    enter_parser.add_argument("--tp-reversal-stop-buffer-pct", type=float, default=None,
                              help="Buffer fraction for the TP-reversal stop trigger; default is tp-reversal-pct * 0.2.")
    enter_parser.add_argument("--entry-tif", type=str, default="Alo", choices=["Alo", "Gtc"],
                              help="Entry limit TIF. Default: Alo.")
    enter_parser.add_argument("--tp-tif", type=str, default="Alo", choices=["Alo", "Gtc"],
                              help="Take-profit limit TIF. Default: Alo.")
    enter_parser.add_argument("--no-market-fallback", action="store_true",
                              help="Do not fall back to market entry after limit retries.")
    enter_parser.add_argument("--market-slippage", type=float, default=0.05,
                              help="Slippage fraction for market-style entry/exit. Default: 0.05.")
    enter_parser.add_argument("--keep-existing-tpsl", action="store_true",
                              help="Do not cancel existing reduce-only TP/SL orders before entry.")
    enter_parser.add_argument("--testnet", action="store_true", help="Use Hyperliquid testnet instead of mainnet.")
    enter_parser.add_argument("--no-websocket", action="store_true",
                              help="Disable websocket market-data cache and use HTTP polling only.")
    enter_parser.add_argument("--hide-orders", "-ho", action="store_true",
                              help="Keep TP/SL targets private; no exchange-side bracket orders are placed until targets trigger.")

    watch_parser = subparsers.add_parser(
        "watch",
        help="Watch for newly opened positions and automatically attach stop-loss and take-profit management.",
    )
    watch_parser.add_argument("coin", type=str, nargs="?", default=None,
                              help="Optional coin symbol to watch, e.g. BTC. Omit to watch all coins.")
    watch_parser.add_argument("--take-profit-pct", type=float, default=None,
                              help="Take-profit target fraction, e.g. 0.01 = 1pct.")
    watch_parser.add_argument("--stop-loss-pct", type=float, default=None,
                              help="Stop-loss fraction. If omitted with TP, defaults to TP * 0.5.")
    watch_parser.add_argument("--take-profit-levels", type=int, default=4,
                              help="Number of weighted TP limit levels. Default: 4.")
    watch_parser.add_argument("--trailing-tp", action="store_true",
                              help="Place the TP ladder, then after the configured TP level fills cancel the remaining ladder and switch to a local trailing take-profit.")
    watch_parser.add_argument("--trailing-tp-trigger-level", type=int, default=1,
                              help="TP ladder level that must fill before trailing TP activates. Default: 1.")
    watch_parser.add_argument("--trailing-tp-profit-pct", type=float, default=0.25,
                              help="Trailing distance as a fraction of favorable unrealized profit after the first TP level is hit. Default: 0.25.")
    watch_parser.add_argument("--poll-interval", type=float, default=1.0,
                              help="Seconds between watcher/position monitor polls. Default: 1.0.")
    watch_parser.add_argument("--tp-reversal-pct", type=float, default=None,
                              help="Exit if price reverses this fraction after first TP zone.")
    watch_parser.add_argument("--tp-reversal-limit-exit", action="store_true",
                              help="On TP reversal, try a reduce-only limit exit plus protective stop before falling back to market.")
    watch_parser.add_argument("--tp-reversal-stop-buffer-pct", type=float, default=None,
                              help="Buffer fraction for the TP-reversal stop trigger; default is tp-reversal-pct * 0.2.")
    watch_parser.add_argument("--tp-tif", type=str, default="Alo", choices=["Alo", "Gtc"],
                              help="Take-profit limit TIF. Default: Alo.")
    watch_parser.add_argument("--market-slippage", type=float, default=0.05,
                              help="Slippage fraction for market-style exits. Default: 0.05.")
    watch_parser.add_argument("--keep-existing-tpsl", action="store_true",
                              help="Do not cancel existing reduce-only TP/SL orders before attaching new ones.")
    watch_parser.add_argument("--manage-existing", action="store_true",
                              help="Also attach TP/SL to matching positions already open at startup.")
    watch_parser.add_argument("--testnet", action="store_true", help="Use Hyperliquid testnet instead of mainnet.")
    watch_parser.add_argument("--no-websocket", action="store_true",
                              help="Disable websocket market-data cache and use HTTP polling only.")
    watch_parser.add_argument("--hide-orders", "-ho", action="store_true",
                              help="Keep TP/SL targets private; no exchange-side bracket orders are placed until targets trigger.")

    auto_parser = subparsers.add_parser(
        "auto",
        help="TA-Lib multi-timeframe auto trader using MACD, SAR, ADX, and Bollinger targets.",
    )
    auto_parser.add_argument("coin", type=str, nargs="?", default=None,
                             help="Optional perpetual coin symbol, e.g. BTC, ETH, HYPE. Omit to scan the top perp markets by volume.")
    auto_size_group = auto_parser.add_mutually_exclusive_group(required=True)
    auto_size_group.add_argument("--size", type=float, help="Contract size to enter when a signal appears.")
    auto_size_group.add_argument("--size-pct", type=str,
                                 help="Percent of available collateral to use for sizing. Accepts 10, 10%%, or 0.10.")
    auto_parser.add_argument("--top-markets", type=int, default=10,
                             help="When auto coin is omitted, scan the top N perp markets by day notional volume. Default: 10.")
    auto_parser.add_argument("--intervals", type=str, default="1h,15m,5m,1m",
                             help="Comma/space separated intervals. Default: 1h,15m,5m,1m.")
    auto_parser.add_argument("--auto-periods", type=int, default=200,
                             help="Candles per interval for TA-Lib indicators. Default: 200.")
    auto_parser.add_argument("--scan-interval", type=float, default=30,
                             help="Seconds between signal scans. Default: 30.0.")
    auto_parser.add_argument("--max-concurrent-scans", type=int, default=3,
                             help="Maximum markets to scan concurrently per auto loop. Default: 3.")
    auto_parser.add_argument("--max-positions", type=int, default=3,
                             help="Maximum concurrently open or actively managed auto positions. Default: 3.")
    auto_parser.add_argument("--min-agreement", type=int, default=0,
                             help="Intervals required to agree. 0 means all configured intervals. Default: 0.")
    auto_parser.add_argument("--adx-threshold", type=float, default=20.0,
                             help="Minimum ADX required for a trend signal. Default: 20.0.")
    auto_parser.add_argument("--macd-fast", type=int, default=12, help="MACD fast period. Default: 12.")
    auto_parser.add_argument("--macd-slow", type=int, default=26, help="MACD slow period. Default: 26.")
    auto_parser.add_argument("--macd-signal", type=int, default=9, help="MACD signal period. Default: 9.")
    auto_parser.add_argument("--sar-acceleration", type=float, default=0.02,
                             help="Parabolic SAR acceleration. Default: 0.02.")
    auto_parser.add_argument("--sar-maximum", type=float, default=0.2,
                             help="Parabolic SAR maximum acceleration. Default: 0.2.")
    auto_parser.add_argument("--auto-sar-stop-on-shortest-interval", action="store_true",
                             help="Use the Parabolic SAR from the shortest configured interval as the stop trigger for auto entries. Default keeps pct-based stop behavior.")
    auto_parser.add_argument("--adx-timeperiod", type=int, default=14, help="ADX timeperiod. Default: 14.")
    auto_parser.add_argument("--bb-timeperiod", type=int, default=20, help="Bollinger Bands timeperiod. Default: 20.")
    auto_parser.add_argument("--bb-dev", type=float, default=2.0,
                             help="Bollinger Bands standard-deviation multiplier. Default: 2.0.")
    auto_parser.add_argument("--scalp", action="store_true",
                             help="Use scalp-mode Bollinger confirmation on the shortest active interval before auto entry.")
    auto_parser.add_argument("--use-live-candle", action="store_true",
                             help="Use the latest candle even if it may still be forming. Default uses last closed candle.")
    auto_parser.add_argument("--take-profit-pct", type=float, default=None,
                             help="Override Bollinger-derived TP fraction, e.g. 0.01 = 1pct.")
    auto_parser.add_argument("--min-take-profit-pct", type=float, default=0.002,
                             help="Minimum Bollinger-derived TP fraction. Default: 0.002.")
    auto_parser.add_argument("--max-take-profit-pct", type=float, default=0.03,
                             help="Maximum Bollinger-derived TP fraction. Default: 0.03.")
    auto_parser.add_argument("--stop-loss-pct", type=float, default=None,
                             help="Stop-loss fraction. If omitted, defaults to TP * 0.5.")
    auto_parser.add_argument("--take-profit-levels", type=int, default=4,
                             help="Number of weighted TP limit levels. Default: 4.")
    auto_parser.add_argument("--trailing-tp", action="store_true",
                             help="Place the TP ladder, then after the configured TP level fills cancel the remaining ladder and switch to a local trailing take-profit.")
    auto_parser.add_argument("--trailing-tp-trigger-level", type=int, default=1,
                             help="TP ladder level that must fill before trailing TP activates. Default: 1.")
    auto_parser.add_argument("--trailing-tp-profit-pct", type=float, default=0.25,
                             help="Trailing distance as a fraction of favorable unrealized profit after the first TP level is hit. Default: 0.25.")
    auto_parser.add_argument("--entry-retries", type=int, default=5,
                             help="Number of top-of-book modify attempts. Default: 5.")
    auto_parser.add_argument("--entry-repost-interval", type=float, default=0.25,
                             help="Seconds between entry modify attempts. Default: 1.0.")
    auto_parser.add_argument("--poll-interval", type=float, default=1.0,
                             help="Seconds between bracket monitor polls after entry. Default: 1.0.")
    auto_parser.add_argument("--tp-reversal-pct", type=float, default=None,
                             help="Exit if price reverses this fraction after first TP zone.")
    auto_parser.add_argument("--tp-reversal-limit-exit", action="store_true",
                             help="On TP reversal, try a reduce-only limit exit plus protective stop before falling back to market.")
    auto_parser.add_argument("--tp-reversal-stop-buffer-pct", type=float, default=None,
                             help="Buffer fraction for the TP-reversal stop trigger; default is tp-reversal-pct * 0.2.")
    auto_parser.add_argument("--entry-tif", type=str, default="Alo", choices=["Alo", "Gtc"],
                             help="Entry limit TIF. Default: Alo.")
    auto_parser.add_argument("--tp-tif", type=str, default="Alo", choices=["Alo", "Gtc"],
                             help="Take-profit limit TIF. Default: Alo.")
    auto_parser.add_argument("--no-market-fallback", action="store_true",
                             help="Do not fall back to market entry after limit modify attempts.")
    auto_parser.add_argument("--market-slippage", type=float, default=0.05,
                             help="Slippage fraction for market-style entry/exit. Default: 0.05.")
    auto_parser.add_argument("--keep-existing-tpsl", action="store_true",
                             help="Do not cancel existing reduce-only TP/SL orders before entry.")
    auto_parser.add_argument("--dry-run", action="store_true", help="Print auto signals but do not enter trades.")
    auto_parser.add_argument("--max-trades", type=int, default=0,
                             help="Maximum completed auto trades before exit. 0 means unlimited. Default: 0.")
    auto_parser.add_argument("--cooldown-after-trade", type=float, default=60.0,
                             help="Seconds to wait after a managed trade exits before scanning again. Default: 60.0.")
    auto_parser.add_argument("--loop-after-trade", dest="loop_after_trade", action="store_true",
                             help="After a managed trade finishes, resume scanning for the next signal. Default: enabled.")
    auto_parser.add_argument("--exit-after-trade", dest="loop_after_trade", action="store_false",
                             help="Exit auto mode after the first completed managed trade.")
    auto_parser.set_defaults(loop_after_trade=True)
    auto_parser.add_argument("--max-coin-trades-per-session", type=int, default=0,
                             help="Per-coin completed trade-cycle limit before cooldown. 0 disables. Default: 0.")
    auto_parser.add_argument("--coin-session-cooldown-seconds", type=float, default=0.0,
                             help="Cooldown duration after a per-coin stop condition triggers. 0 disables timed cooldown. Default: 0.")
    auto_parser.add_argument("--coin-session-profit-target", type=float, default=0.0,
                             help="Per-coin realized PnL target before cooldown. 0 disables. Default: 0.")
    auto_parser.add_argument("--coin-session-min-profit-to-lock", type=float, default=0.0,
                             help="Minimum per-coin peak realized PnL before giveback protection activates. Default: 0.")
    auto_parser.add_argument("--coin-session-giveback-pct", type=float, default=0.0,
                             help="Per-coin giveback fraction from peak realized PnL before cooldown. 0 disables. Default: 0.")
    auto_parser.add_argument("--cooldown-after-loss-following-wins", type=int, default=0,
                             help="Cooldown a coin after a loss that follows at least N consecutive winning cycles. 0 disables. Default: 0.")
    auto_parser.add_argument("--session-profit-target", type=float, default=0.0,
                             help="Stop the entire auto session after total realized PnL reaches this target. 0 disables. Default: 0.")
    auto_parser.add_argument("--session-max-loss", type=float, default=0.0,
                             help="Stop the entire auto session if total realized PnL falls to -N or below. 0 disables. Default: 0.")
    auto_parser.add_argument("--session-giveback-pct", type=float, default=0.0,
                             help="Stop the entire auto session after giving back this fraction from peak realized PnL. 0 disables. Default: 0.")
    auto_parser.add_argument("--risk-session-log", type=str, default="",
                             help="Optional JSONL path for auto risk-session events.")
    auto_parser.add_argument("--testnet", action="store_true", help="Use Hyperliquid testnet instead of mainnet.")
    auto_parser.add_argument("--no-websocket", action="store_true",
                             help="Disable websocket market-data cache and use HTTP polling only.")
    auto_parser.add_argument("--disable-ws-candles", action="store_true",
                             help="Disable ws for kline data (not recommended): ")
    auto_parser.add_argument("--hide-orders", "-ho", action="store_true",
                             help="Keep auto-mode TP/SL targets private; no exchange-side bracket orders are placed until targets trigger.")

    mm_parser = subparsers.add_parser(
        "market_maker",
        help="Event-driven market maker using async SDK calls.",
    )
    mm_parser.add_argument("coin", type=str, help="Perpetual coin symbol, e.g. BTC, ETH, kPEPE.")
    mm_parser.add_argument("--interval", type=str, default="1m", choices=sorted(INTERVAL_TO_MS.keys()),
                           help="Candle interval. Default: 1m.")
    mm_parser.add_argument("--periods", type=int, default=12, help="Recent candle count for stddev. Default: 12.")
    mm_parser.add_argument("--levels", type=int, default=4, help="Number of pyramid levels per side. Default: 4.")
    mm_parser.add_argument("--base-size", type=float, default=0.01,
                           help="Base contract size for closest level. Default: 0.01.")
    mm_parser.add_argument("--loop-sleep", type=float, default=1.0, help="Seconds between MM checks. Default: 1.0.")
    mm_parser.add_argument("--min-edge-pct", type=float, default=0.0005,
                           help="Minimum edge vs mid as fraction. Default: 0.0005.")
    mm_parser.add_argument("--rebalance-threshold-pct", type=float, default=0.001,
                           help="Mid-vs-ladder-center drift threshold. Default: 0.001.")
    mm_parser.add_argument("--protect-close-pct", type=float, default=0.0005,
                           help="Do not cancel orders within this mid band. Default: 0.0005.")
    mm_parser.add_argument("--testnet", action="store_true", help="Use Hyperliquid testnet instead of mainnet.")
    mm_parser.add_argument("--no-websocket", action="store_true",
                           help="Disable websocket market-data cache and use HTTP polling only.")

    return parser


async def async_main(argv: Optional[List[str]] = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    if args.command == "trailing":
        if args.trail_pct <= 0.0:
            print("[ERROR] --trail-pct must be > 0.")
            sys.exit(1)
        if args.poll_interval <= 0.0:
            print("[ERROR] --poll-interval must be > 0.")
            sys.exit(1)
        await trailing_stop_for_all_positions(
            trail_pct=args.trail_pct,
            poll_interval=args.poll_interval,
            use_testnet=args.testnet,
            only_coin=(args.coin.upper() if args.coin is not None else None),
            use_websocket=(not args.no_websocket),
            hide_orders=args.hide_orders,
        )
        return

    if args.command == "enter":
        if args.size <= 0.0:
            print("[ERROR] --size must be > 0.")
            sys.exit(1)
        if args.take_profit_pct is None and args.stop_loss_pct is None:
            print("[ERROR] Specify --take-profit-pct, --stop-loss-pct, or both.")
            sys.exit(1)
        if args.take_profit_levels <= 0:
            print("[ERROR] --take-profit-levels must be > 0.")
            sys.exit(1)
        if args.trailing_tp_trigger_level <= 0:
            print("[ERROR] --trailing-tp-trigger-level must be > 0.")
            sys.exit(1)
        if args.trailing_tp_trigger_level > args.take_profit_levels:
            print("[ERROR] --trailing-tp-trigger-level cannot exceed --take-profit-levels.")
            sys.exit(1)
        if not (0.0 < args.trailing_tp_profit_pct < 1.0):
            print("[ERROR] --trailing-tp-profit-pct must be between 0 and 1.")
            sys.exit(1)
        if args.entry_retries < 0:
            print("[ERROR] --entry-retries must be >= 0.")
            sys.exit(1)
        if args.entry_repost_interval <= 0.0:
            print("[ERROR] --entry-repost-interval must be > 0.")
            sys.exit(1)
        if args.poll_interval <= 0.0:
            print("[ERROR] --poll-interval must be > 0.")
            sys.exit(1)
        if args.market_slippage < 0.0:
            print("[ERROR] --market-slippage must be >= 0.")
            sys.exit(1)
        if args.tp_reversal_stop_buffer_pct is not None and not (0.0 < args.tp_reversal_stop_buffer_pct < 1.0):
            print("[ERROR] --tp-reversal-stop-buffer-pct must be between 0 and 1.")
            sys.exit(1)

        await run_bracket_entry(
            coin=args.coin.upper(),
            direction=args.direction,
            size=args.size,
            take_profit_pct=args.take_profit_pct,
            stop_loss_pct=args.stop_loss_pct,
            stop_loss_trigger_px=None,
            take_profit_levels=args.take_profit_levels,
            use_trailing_tp=args.trailing_tp,
            trailing_tp_trigger_level=args.trailing_tp_trigger_level,
            trailing_tp_profit_pct=args.trailing_tp_profit_pct,
            entry_retries=args.entry_retries,
            entry_repost_interval=args.entry_repost_interval,
            poll_interval=args.poll_interval,
            tp_reversal_pct=args.tp_reversal_pct,
            entry_tif=args.entry_tif,
            tp_tif=args.tp_tif,
            market_fallback=(not args.no_market_fallback),
            market_slippage=args.market_slippage,
            cancel_existing_tpsl=(not args.keep_existing_tpsl),
            tp_reversal_limit_exit=args.tp_reversal_limit_exit,
            tp_reversal_stop_buffer_pct=args.tp_reversal_stop_buffer_pct,
            use_testnet=args.testnet,
            use_websocket=(not args.no_websocket),
            hide_orders=args.hide_orders,
        )
        return

    if args.command == "watch":
        if args.take_profit_pct is None and args.stop_loss_pct is None:
            print("[ERROR] Specify --take-profit-pct, --stop-loss-pct, or both.")
            sys.exit(1)
        if args.take_profit_levels <= 0:
            print("[ERROR] --take-profit-levels must be > 0.")
            sys.exit(1)
        if args.trailing_tp_trigger_level <= 0:
            print("[ERROR] --trailing-tp-trigger-level must be > 0.")
            sys.exit(1)
        if args.trailing_tp_trigger_level > args.take_profit_levels:
            print("[ERROR] --trailing-tp-trigger-level cannot exceed --take-profit-levels.")
            sys.exit(1)
        if not (0.0 < args.trailing_tp_profit_pct < 1.0):
            print("[ERROR] --trailing-tp-profit-pct must be between 0 and 1.")
            sys.exit(1)
        if args.poll_interval <= 0.0:
            print("[ERROR] --poll-interval must be > 0.")
            sys.exit(1)
        if args.market_slippage < 0.0:
            print("[ERROR] --market-slippage must be >= 0.")
            sys.exit(1)
        if args.tp_reversal_stop_buffer_pct is not None and not (0.0 < args.tp_reversal_stop_buffer_pct < 1.0):
            print("[ERROR] --tp-reversal-stop-buffer-pct must be between 0 and 1.")
            sys.exit(1)

        await run_position_watcher(
            only_coin=(args.coin.upper() if args.coin is not None else None),
            take_profit_pct=args.take_profit_pct,
            stop_loss_pct=args.stop_loss_pct,
            take_profit_levels=args.take_profit_levels,
            use_trailing_tp=args.trailing_tp,
            trailing_tp_trigger_level=args.trailing_tp_trigger_level,
            trailing_tp_profit_pct=args.trailing_tp_profit_pct,
            poll_interval=args.poll_interval,
            tp_reversal_pct=args.tp_reversal_pct,
            tp_tif=args.tp_tif,
            market_slippage=args.market_slippage,
            cancel_existing_tpsl=(not args.keep_existing_tpsl),
            manage_existing=args.manage_existing,
            tp_reversal_limit_exit=args.tp_reversal_limit_exit,
            tp_reversal_stop_buffer_pct=args.tp_reversal_stop_buffer_pct,
            use_testnet=args.testnet,
            use_websocket=(not args.no_websocket),
            hide_orders=args.hide_orders,
        )
        return

    if args.command == "auto":
        if args.size is not None and args.size <= 0.0:
            print("[ERROR] --size must be > 0.")
            sys.exit(1)
        if args.size_pct is not None:
            try:
                parse_fractional_pct(args.size_pct, field_name="--size-pct")
            except RuntimeError as exc:
                print(f"[ERROR] {exc}")
                sys.exit(1)

        if args.disable_ws_candles:
            setattr(args, 'ws_candles', False)
        else:
            setattr(args, 'ws_candles', True)

        if args.top_markets <= 0:
            print("[ERROR] --top-markets must be > 0.")
            sys.exit(1)
        if args.auto_periods <= 0:
            print("[ERROR] --auto-periods must be > 0.")
            sys.exit(1)
        if args.scan_interval <= 0.0:
            print("[ERROR] --scan-interval must be > 0.")
            sys.exit(1)
        if args.max_concurrent_scans <= 0:
            print("[ERROR] --max-concurrent-scans must be > 0.")
            sys.exit(1)
        if args.max_positions <= 0:
            print("[ERROR] --max-positions must be > 0.")
            sys.exit(1)
        if args.min_agreement < 0:
            print("[ERROR] --min-agreement must be >= 0.")
            sys.exit(1)
        if args.take_profit_levels <= 0:
            print("[ERROR] --take-profit-levels must be > 0.")
            sys.exit(1)
        if args.trailing_tp_trigger_level <= 0:
            print("[ERROR] --trailing-tp-trigger-level must be > 0.")
            sys.exit(1)
        if args.trailing_tp_trigger_level > args.take_profit_levels:
            print("[ERROR] --trailing-tp-trigger-level cannot exceed --take-profit-levels.")
            sys.exit(1)
        if not (0.0 < args.trailing_tp_profit_pct < 1.0):
            print("[ERROR] --trailing-tp-profit-pct must be between 0 and 1.")
            sys.exit(1)
        if args.entry_retries < 0:
            print("[ERROR] --entry-retries must be >= 0.")
            sys.exit(1)
        if args.entry_repost_interval <= 0.0:
            print("[ERROR] --entry-repost-interval must be > 0.")
            sys.exit(1)
        if args.poll_interval <= 0.0:
            print("[ERROR] --poll-interval must be > 0.")
            sys.exit(1)
        if args.market_slippage < 0.0:
            print("[ERROR] --market-slippage must be >= 0.")
            sys.exit(1)
        if args.max_trades < 0:
            print("[ERROR] --max-trades must be >= 0.")
            sys.exit(1)
        if args.cooldown_after_trade < 0.0:
            print("[ERROR] --cooldown-after-trade must be >= 0.")
            sys.exit(1)
        if args.max_coin_trades_per_session < 0:
            print("[ERROR] --max-coin-trades-per-session must be >= 0.")
            sys.exit(1)
        if args.coin_session_cooldown_seconds < 0.0:
            print("[ERROR] --coin-session-cooldown-seconds must be >= 0.")
            sys.exit(1)
        if args.coin_session_profit_target < 0.0:
            print("[ERROR] --coin-session-profit-target must be >= 0.")
            sys.exit(1)
        if args.coin_session_min_profit_to_lock < 0.0:
            print("[ERROR] --coin-session-min-profit-to-lock must be >= 0.")
            sys.exit(1)
        if not (0.0 <= args.coin_session_giveback_pct < 1.0):
            print("[ERROR] --coin-session-giveback-pct must be >= 0 and < 1.")
            sys.exit(1)
        if args.cooldown_after_loss_following_wins < 0:
            print("[ERROR] --cooldown-after-loss-following-wins must be >= 0.")
            sys.exit(1)
        if args.session_profit_target < 0.0:
            print("[ERROR] --session-profit-target must be >= 0.")
            sys.exit(1)
        if args.session_max_loss < 0.0:
            print("[ERROR] --session-max-loss must be >= 0.")
            sys.exit(1)
        if not (0.0 <= args.session_giveback_pct < 1.0):
            print("[ERROR] --session-giveback-pct must be >= 0 and < 1.")
            sys.exit(1)
        if args.tp_reversal_stop_buffer_pct is not None and not (0.0 < args.tp_reversal_stop_buffer_pct < 1.0):
            print("[ERROR] --tp-reversal-stop-buffer-pct must be between 0 and 1.")
            sys.exit(1)
        if args.ws_candles and args.no_websocket:
            print("[ERROR] --ws-candles requires websocket market data; remove --no-websocket.")
            sys.exit(1)

        await run_auto_trader(
            coin=(args.coin.upper() if args.coin is not None else None),
            size=args.size,
            size_pct=args.size_pct,
            top_markets=args.top_markets,
            intervals_value=args.intervals,
            periods=args.auto_periods,
            scan_interval=args.scan_interval,
            max_concurrent_scans=args.max_concurrent_scans,
            max_positions=args.max_positions,
            min_agreement=args.min_agreement,
            adx_threshold=args.adx_threshold,
            take_profit_pct=args.take_profit_pct,
            stop_loss_pct=args.stop_loss_pct,
            min_take_profit_pct=args.min_take_profit_pct,
            max_take_profit_pct=args.max_take_profit_pct,
            take_profit_levels=args.take_profit_levels,
            use_trailing_tp=args.trailing_tp,
            trailing_tp_trigger_level=args.trailing_tp_trigger_level,
            trailing_tp_profit_pct=args.trailing_tp_profit_pct,
            entry_retries=args.entry_retries,
            entry_repost_interval=args.entry_repost_interval,
            poll_interval=args.poll_interval,
            tp_reversal_pct=args.tp_reversal_pct,
            entry_tif=args.entry_tif,
            tp_tif=args.tp_tif,
            market_fallback=(not args.no_market_fallback),
            market_slippage=args.market_slippage,
            cancel_existing_tpsl=(not args.keep_existing_tpsl),
            tp_reversal_limit_exit=args.tp_reversal_limit_exit,
            tp_reversal_stop_buffer_pct=args.tp_reversal_stop_buffer_pct,
            macd_fast=args.macd_fast,
            macd_slow=args.macd_slow,
            macd_signal_period=args.macd_signal,
            sar_acceleration=args.sar_acceleration,
            sar_maximum=args.sar_maximum,
            adx_timeperiod=args.adx_timeperiod,
            bb_timeperiod=args.bb_timeperiod,
            bb_dev=args.bb_dev,
            scalp=args.scalp,
            use_last_closed_candle=(not args.use_live_candle),
            use_sar_stop_on_shortest_interval=args.auto_sar_stop_on_shortest_interval,
            dry_run=args.dry_run,
            max_trades=args.max_trades,
            cooldown_after_trade=args.cooldown_after_trade,
            loop_after_trade=args.loop_after_trade,
            max_coin_trades_per_session=args.max_coin_trades_per_session,
            coin_session_cooldown_seconds=args.coin_session_cooldown_seconds,
            coin_session_profit_target=args.coin_session_profit_target,
            coin_session_min_profit_to_lock=args.coin_session_min_profit_to_lock,
            coin_session_giveback_pct=args.coin_session_giveback_pct,
            cooldown_after_loss_following_wins=args.cooldown_after_loss_following_wins,
            session_profit_target=args.session_profit_target,
            session_max_loss=args.session_max_loss,
            session_giveback_pct=args.session_giveback_pct,
            use_testnet=args.testnet,
            use_websocket=(not args.no_websocket),
            use_websocket_candles=args.ws_candles,
            hide_orders=args.hide_orders,
            risk_session_log=args.risk_session_log,
        )
        return

    if args.command == "market_maker":
        if args.periods <= 0:
            print("[ERROR] --periods must be > 0.")
            sys.exit(1)
        if args.levels <= 0:
            print("[ERROR] --levels must be > 0.")
            sys.exit(1)
        if args.base_size <= 0:
            print("[ERROR] --base-size must be > 0.")
            sys.exit(1)
        if args.min_edge_pct <= 0:
            print("[ERROR] --min-edge-pct must be > 0.")
            sys.exit(1)
        if args.rebalance_threshold_pct <= 0:
            print("[ERROR] --rebalance-threshold-pct must be > 0.")
            sys.exit(1)
        await run_market_maker(
            coin=args.coin.upper(),
            interval=args.interval,
            periods=args.periods,
            levels=args.levels,
            base_size=args.base_size,
            use_testnet=args.testnet,
            loop_sleep=args.loop_sleep,
            min_edge_pct=args.min_edge_pct,
            rebalance_threshold_pct=args.rebalance_threshold_pct,
            protect_close_pct=args.protect_close_pct,
            use_websocket=(not args.no_websocket),
        )
        return

    parser.print_help()
    sys.exit(1)


def main(argv: Optional[List[str]] = None) -> None:
    log_path = configure_runtime_logging()
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        install_asyncio_exception_logging(loop)
        print(f"[LOG] Writing runtime logs to {log_path}")
        print(f"[START] argv={argv if argv is not None else sys.argv[1:]}")
        loop.run_until_complete(async_main(argv))
    except KeyboardInterrupt:
        print("\n[!] Interrupted.")
    except Exception:
        logging.getLogger("hypertrader").exception("[FATAL] Unhandled main() exception.")
        sys.exit(1)
    finally:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = None
        if loop is not None and not loop.is_closed():
            loop.close()


if __name__ == "__main__":
    uvloop.install()
    main()
