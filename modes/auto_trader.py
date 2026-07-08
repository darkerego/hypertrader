import asyncio
import decimal
import json
import logging
import math
import os
import time
import numpy as np
import talib
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Tuple

from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

from modes.position_management import run_bracket_entry
from utils.constants import AUTO_TRADES_LOG_FILE, INTERVAL_TO_MS, cp
from utils.helpers import parse_interval_list, init_clients, _try_float, parse_fractional_pct, \
    extract_account_balance_from_user_state, round_size_for_hyperliquid, get_user_state_with_retry, get_all_mids, \
    fetch_recent_candles, compute_position_unrealized_pnl, close_clients, compute_default_stop_loss_pct, \
    extract_closed_pnl_from_fill

logger = logging.getLogger(__name__)
_AUTO_TRADES_LOGGER: Optional[logging.Logger] = None


def get_auto_trades_logger() -> logging.Logger:
    """Return the dedicated auto-trade completion logger."""
    global _AUTO_TRADES_LOGGER

    if _AUTO_TRADES_LOGGER is not None:
        return _AUTO_TRADES_LOGGER

    resolved_path = os.path.abspath(AUTO_TRADES_LOG_FILE)
    os.makedirs(os.path.dirname(resolved_path), exist_ok=True)

    auto_logger = logging.getLogger("hypertrader.auto_trades")
    auto_logger.setLevel(logging.INFO)
    auto_logger.propagate = False

    if not any(
        isinstance(handler, logging.FileHandler) and getattr(handler, "baseFilename", None) == resolved_path
        for handler in auto_logger.handlers
    ):
        file_handler = logging.FileHandler(resolved_path, encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        auto_logger.addHandler(file_handler)

    _AUTO_TRADES_LOGGER = auto_logger
    return auto_logger

def require_talib_available() -> None:
    """Fail early with an actionable message when auto-mode dependencies are missing."""
    if np is None or talib is None:
        raise RuntimeError(
            "The auto command requires numpy and TA-Lib. Install them with:\n"
            "  pip install numpy TA-Lib\n\n"
            "On Debian/Ubuntu, if the wheel is unavailable, install the TA-Lib C library first, for example:\n"
            "  sudo apt-get update && sudo apt-get install -y build-essential ta-lib\n"
        )

async def get_top_perp_markets_by_volume(info: Info, limit: int) -> List[Tuple[str, float]]:
    """Return the top perp markets ranked by reported day notional volume."""
    if limit <= 0:
        raise RuntimeError("--top-markets must be > 0.")

    meta_and_ctxs = await info.meta_and_asset_ctxs()
    if not isinstance(meta_and_ctxs, (list, tuple)) or len(meta_and_ctxs) < 2:
        raise RuntimeError(f"Unexpected metaAndAssetCtxs response shape: {type(meta_and_ctxs).__name__}")

    meta = meta_and_ctxs[0]
    asset_ctxs = meta_and_ctxs[1]
    universe = meta.get("universe", []) if isinstance(meta, dict) else []
    if not isinstance(universe, list) or not isinstance(asset_ctxs, list):
        raise RuntimeError("metaAndAssetCtxs response did not include perp universe and asset contexts lists.")

    ranked: List[Tuple[str, float]] = []
    for asset_info, ctx in zip(universe, asset_ctxs):
        if not isinstance(asset_info, dict) or not isinstance(ctx, dict):
            continue
        coin = str(asset_info.get("name") or ctx.get("coin") or "").upper()
        if not coin:
            continue
        volume = _try_float(ctx.get("dayNtlVlm"))
        if volume is None or volume <= 0.0:
            continue
        ranked.append((coin, volume))

    ranked.sort(key=lambda item: item[1], reverse=True)
    return ranked[:limit]


# ---------------------------------------------------------------------------
# TA-Lib auto trader
# ---------------------------------------------------------------------------

@dataclass
class AutoIntervalSignal:
    """TA-Lib indicator snapshot for one interval."""
    interval: str
    closes: List[float]
    close: float
    macd: float
    macd_signal: float
    macd_hist: float
    sar: float
    adx: float
    bb_upper: float
    bb_middle: float
    bb_lower: float
    direction: str
    reason: str


@dataclass
class AutoTradeDecision:
    """Aggregated multi-timeframe trade decision."""
    direction: Optional[str]
    current_px: float
    target_px: Optional[float]
    take_profit_pct: Optional[float]
    stop_loss_pct: Optional[float]
    stop_loss_trigger_px: Optional[float]
    long_votes: int
    short_votes: int
    required_votes: int
    snapshots: List[AutoIntervalSignal]
    reason: str


@dataclass
class BollingerState:
    """Computed Bollinger confirmation state for one selected interval."""
    interval: str
    basis: float
    upper: float
    lower: float
    percent_b: float
    basis_slope: float
    bandwidth: float
    previous_bandwidth: float
    bandwidth_expanding: bool
    latest_close: float


@dataclass
class CoinTradeSessionState:
    coin: str
    realized_pnl: float = 0.0
    peak_pnl: float = 0.0
    completed_cycles: int = 0
    winning_cycles: int = 0
    losing_cycles: int = 0
    consecutive_wins: int = 0
    consecutive_losses: int = 0
    cooldown_until_ms: int = 0
    cooldown_reason: str = ""
    post_trade_cooldown_until_ms: int = 0
    last_cycle_pnl: float = 0.0
    last_cycle_started_ms: int = 0
    last_cycle_closed_ms: int = 0


@dataclass
class AutoRiskSessionState:
    started_ms: int
    realized_pnl: float = 0.0
    peak_pnl: float = 0.0
    completed_cycles: int = 0
    stopped: bool = False
    stop_reason: str = ""


@dataclass
class AutoScanLoopSnapshot:
    """Per-scan shared state reused across all market candidates."""
    mids: Dict[str, float]
    positions_by_coin: Dict[str, Dict[str, Any]]
    account_balance: Optional[float]
    realized_pnl: Optional[float]


@dataclass
class AutoScanCandidate:
    """Outcome of scanning one market during a shared scan pass."""
    coin: str
    decision: Optional["AutoTradeDecision"]
    existing_position: Optional[Dict[str, Any]]
    rejection_reason: Optional[str] = None


@dataclass
class AutoManagedTradeResult:
    """Completed auto-managed trade metadata returned by a background task."""
    coin: str
    direction: str
    size: float
    take_profit_pct: float
    stop_loss_pct: Optional[float]
    started_ms: int
    closed_ms: int


def now_ms() -> int:
    return int(time.time() * 1000)


def get_or_create_coin_session(
    coin_sessions: Dict[str, CoinTradeSessionState],
    coin: str,
) -> CoinTradeSessionState:
    coin_key = str(coin).upper()
    existing = coin_sessions.get(coin_key)
    if existing is not None:
        return existing
    created = CoinTradeSessionState(coin=coin_key)
    coin_sessions[coin_key] = created
    return created


def _append_risk_event_log(path: str, payload: Dict[str, Any]) -> None:
    if not path:
        return
    resolved_path = os.path.abspath(path)
    os.makedirs(os.path.dirname(resolved_path) or ".", exist_ok=True)
    with open(resolved_path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def reset_coin_session_after_cooldown_if_needed(
    coin_state: CoinTradeSessionState,
    current_time_ms: int,
    risk_session_log: str = "",
    session_pnl: Optional[float] = None,
) -> bool:
    if coin_state.cooldown_until_ms <= 0 or current_time_ms < coin_state.cooldown_until_ms:
        return False

    print(
        f"[AUTO-RISK] {coin_state.coin} cooldown expired; resetting coin session state "
        f'(previous_reason="{coin_state.cooldown_reason}")'
    )
    _append_risk_event_log(
        risk_session_log,
        {
            "ts_ms": current_time_ms,
            "event": "coin_cooldown_expired",
            "coin": coin_state.coin,
            "reason": coin_state.cooldown_reason,
            "coin_pnl": coin_state.realized_pnl,
            "session_pnl": session_pnl,
        },
    )
    coin_state.realized_pnl = 0.0
    coin_state.peak_pnl = 0.0
    coin_state.completed_cycles = 0
    coin_state.winning_cycles = 0
    coin_state.losing_cycles = 0
    coin_state.consecutive_wins = 0
    coin_state.consecutive_losses = 0
    coin_state.cooldown_until_ms = 0
    coin_state.cooldown_reason = ""
    coin_state.last_cycle_pnl = 0.0
    coin_state.last_cycle_started_ms = 0
    coin_state.last_cycle_closed_ms = 0
    return True


def coin_is_blocked_by_risk(
    coin_state: CoinTradeSessionState,
    current_time_ms: int,
) -> Tuple[bool, str]:
    if coin_state.cooldown_until_ms > current_time_ms:
        remaining_seconds = (coin_state.cooldown_until_ms - current_time_ms) / 1000.0
        return (
            True,
            f'cooldown active {remaining_seconds:.1f}s remaining reason="{coin_state.cooldown_reason}"',
        )
    return False, ""


def coin_is_in_post_trade_cooldown(
    coin_state: CoinTradeSessionState,
    current_time_ms: int,
) -> Tuple[bool, str]:
    if coin_state.post_trade_cooldown_until_ms > current_time_ms:
        remaining_seconds = (coin_state.post_trade_cooldown_until_ms - current_time_ms) / 1000.0
        return True, f"post-trade cooldown active {remaining_seconds:.1f}s remaining"
    return False, ""


def set_coin_cooldown(
    coin_state: CoinTradeSessionState,
    cooldown_seconds: float,
    reason: str,
) -> None:
    coin_state.cooldown_reason = reason
    if cooldown_seconds > 0.0:
        coin_state.cooldown_until_ms = now_ms() + int(cooldown_seconds * 1000)
    else:
        coin_state.cooldown_until_ms = now_ms()


async def fetch_trade_cycle_net_pnl(
    info: Info,
    account_address: str,
    coin: str,
    start_time_ms: int,
    end_time_ms: int,
) -> Tuple[float, List[Dict[str, Any]], str]:
    request_start_ms = start_time_ms - 5000
    request_end_ms = end_time_ms + 5000
    fills: Any

    try:
        try:
            fills = await info.user_fills_by_time(
                account_address,
                request_start_ms,
                request_end_ms,
                aggregate_by_time=False,
            )
        except TypeError:
            fills = await info.post(
                "/info",
                {
                    "type": "userFillsByTime",
                    "user": account_address,
                    "startTime": request_start_ms,
                    "endTime": request_end_ms,
                    "aggregateByTime": False,
                },
            )
    except Exception as exc:
        return 0.0, [], f"pnl_unavailable: {exc}"

    if not isinstance(fills, list):
        return 0.0, [], f"pnl_unavailable: unexpected fills response type {type(fills).__name__}"

    coin_upper = str(coin).upper()
    filtered_fills: List[Dict[str, Any]] = []
    net_pnl = 0.0
    for fill in fills:
        if not isinstance(fill, dict):
            continue
        if str(fill.get("coin", "")).upper() != coin_upper:
            continue
        fill_time_ms = int(_try_float(fill.get("time")) or _try_float(fill.get("timestamp")) or 0.0)
        if fill_time_ms < request_start_ms or fill_time_ms > request_end_ms:
            continue
        closed_pnl = _try_float(fill.get("closedPnl"))
        if closed_pnl is None:
            closed_pnl = _try_float(fill.get("closedPnL"))
        if closed_pnl is None:
            closed_pnl = _try_float(fill.get("realizedPnl"))
        if closed_pnl is None:
            closed_pnl = _try_float(fill.get("realizedPnL"))
        if closed_pnl is None:
            closed_pnl = _try_float(fill.get("realized_pnl"))
        fee = _try_float(fill.get("fee")) or 0.0
        net_pnl += (closed_pnl or 0.0) - fee
        filtered_fills.append(fill)

    if not filtered_fills:
        return 0.0, [], "pnl_unavailable: no matching fills in trade cycle window"

    return net_pnl, filtered_fills, ""


def update_risk_after_trade_cycle(
    coin_state: CoinTradeSessionState,
    session_state: AutoRiskSessionState,
    cycle_pnl: float,
    cycle_started_ms: int,
    cycle_closed_ms: int,
    max_coin_trades_per_session: int,
    coin_session_cooldown_seconds: float,
    coin_session_profit_target: float,
    coin_session_min_profit_to_lock: float,
    coin_session_giveback_pct: float,
    cooldown_after_loss_following_wins: int,
    session_profit_target: float,
    session_max_loss: float,
    session_giveback_pct: float,
) -> Tuple[Optional[str], Optional[str]]:
    previous_consecutive_wins = coin_state.consecutive_wins

    coin_state.realized_pnl += cycle_pnl
    coin_state.peak_pnl = max(coin_state.peak_pnl, coin_state.realized_pnl)
    coin_state.completed_cycles += 1
    coin_state.last_cycle_pnl = cycle_pnl
    coin_state.last_cycle_started_ms = cycle_started_ms
    coin_state.last_cycle_closed_ms = cycle_closed_ms

    if cycle_pnl > 0.0:
        coin_state.winning_cycles += 1
        coin_state.consecutive_wins += 1
        coin_state.consecutive_losses = 0
    elif cycle_pnl < 0.0:
        coin_state.losing_cycles += 1
        coin_state.consecutive_losses += 1
        coin_state.consecutive_wins = 0
    else:
        coin_state.consecutive_wins = 0
        coin_state.consecutive_losses = 0

    session_state.realized_pnl += cycle_pnl
    session_state.peak_pnl = max(session_state.peak_pnl, session_state.realized_pnl)
    session_state.completed_cycles += 1

    coin_reason: Optional[str] = None
    if 0 < max_coin_trades_per_session <= coin_state.completed_cycles:
        coin_reason = (
            "max_coin_trades_per_session reached: "
            f"completed_cycles={coin_state.completed_cycles} limit={max_coin_trades_per_session}"
        )
    elif 0.0 < coin_session_profit_target <= coin_state.realized_pnl:
        coin_reason = (
            "coin profit target reached: "
            f"pnl={coin_state.realized_pnl:.6f} target={coin_session_profit_target:.6f}"
        )
    elif (
            0 < cooldown_after_loss_following_wins <= previous_consecutive_wins
            and cycle_pnl < 0.0
    ):
        coin_reason = (
            "loss after win streak: "
            f"previous_wins={previous_consecutive_wins} loss={cycle_pnl:.6f}"
        )
    elif (
        coin_session_giveback_pct > 0.0
        and coin_state.peak_pnl > 0.0
        and coin_state.peak_pnl >= coin_session_min_profit_to_lock
    ):
        threshold = coin_state.peak_pnl * (1.0 - coin_session_giveback_pct)
        if coin_state.realized_pnl <= threshold:
            coin_reason = (
                "coin giveback stop: "
                f"pnl={coin_state.realized_pnl:.6f} peak={coin_state.peak_pnl:.6f} "
                f"giveback_pct={coin_session_giveback_pct:.6f} threshold={threshold:.6f}"
            )

    if coin_reason is not None:
        set_coin_cooldown(
            coin_state=coin_state,
            cooldown_seconds=coin_session_cooldown_seconds,
            reason=coin_reason,
        )

    session_reason: Optional[str] = None
    if session_max_loss > 0.0 and session_state.realized_pnl <= -abs(session_max_loss):
        session_reason = (
            "session max loss reached "
            f"pnl={session_state.realized_pnl:.6f} limit=-{abs(session_max_loss):.6f}"
        )
    elif 0.0 < session_profit_target <= session_state.realized_pnl:
        session_reason = (
            "session profit target reached "
            f"pnl={session_state.realized_pnl:.6f} target={session_profit_target:.6f}"
        )
    elif (
        session_giveback_pct > 0.0
        and session_state.peak_pnl > 0.0
    ):
        threshold = session_state.peak_pnl * (1.0 - session_giveback_pct)
        if session_state.realized_pnl <= threshold:
            session_reason = (
                "session giveback stop "
                f"pnl={session_state.realized_pnl:.6f} peak={session_state.peak_pnl:.6f} "
                f"giveback_pct={session_giveback_pct:.6f} threshold={threshold:.6f}"
            )

    if session_reason is not None:
        session_state.stopped = True
        session_state.stop_reason = session_reason

    return coin_reason, session_reason

def _normalize_state_key(key: Any) -> str:
    """Normalize account-state keys so minor API casing/name shifts still match."""
    return "".join(ch for ch in str(key).lower() if ch.isalnum())

def _decimal_from_margin_value(value: Any, *, field_name: str) -> decimal.Decimal:
    """Convert Hyperliquid numeric payload fields into Decimal safely."""
    if value is None:
        raise RuntimeError(f"{field_name} is missing.")

    if isinstance(value, decimal.Decimal):
        return value

    raw = str(value).strip()
    if raw.endswith("%"):
        raw = raw[:-1].strip()
    if not raw or raw.lower() in {"nan", "none", "null"}:
        raise RuntimeError(f"{field_name} is not a usable number: {value!r}")

    try:
        return decimal.Decimal(raw)
    except decimal.InvalidOperation as exc:
        raise RuntimeError(f"{field_name} is not a valid decimal: {value!r}") from exc

def _find_first_numeric_field(
    payload: Any,
    candidate_keys: Tuple[str, ...],
    path: Tuple[str, ...] = (),
) -> Tuple[Optional[float], Optional[str]]:
    """Depth-first search for the first matching numeric field in nested account state."""
    if isinstance(payload, dict):
        normalized_map = {_normalize_state_key(key): key for key in payload.keys()}
        for candidate_key in candidate_keys:
            actual_key = normalized_map.get(_normalize_state_key(candidate_key))
            if actual_key is None:
                continue
            value = _try_float(payload.get(actual_key))
            if value is not None and value >= 0.0:
                return value, ".".join(path + (str(actual_key),))

        for key, value in payload.items():
            nested_value, nested_path = _find_first_numeric_field(value, candidate_keys, path + (str(key),))
            if nested_value is not None and nested_path is not None:
                return nested_value, nested_path
        return None, None

    if isinstance(payload, list):
        for idx, value in enumerate(payload):
            nested_value, nested_path = _find_first_numeric_field(value, candidate_keys, path + (f"[{idx}]",))
            if nested_value is not None and nested_path is not None:
                return nested_value, nested_path
    return None, None


def extract_available_collateral_from_user_state(user_state: Dict[str, Any]) -> Tuple[Optional[float], str]:
    """Extract available collateral with the same priority order as testpct.py."""
    with decimal.localcontext() as ctx:
        ctx.prec = 28

        withdrawable = user_state.get("withdrawable")
        if withdrawable is not None:
            try:
                available = _decimal_from_margin_value(withdrawable, field_name="withdrawable")
            except RuntimeError:
                available = None
            if available is not None and available >= 0:
                return float(available), "withdrawable"

        summary = user_state.get("crossMarginSummary")
        summary_name = "crossMarginSummary"
        if not isinstance(summary, dict):
            summary = user_state.get("marginSummary")
            summary_name = "marginSummary"
        if isinstance(summary, dict):
            account_value_raw = summary.get("accountValue")
            total_margin_used_raw = summary.get("totalMarginUsed")
            if account_value_raw is not None and total_margin_used_raw is not None:
                try:
                    account_value = _decimal_from_margin_value(
                        account_value_raw,
                        field_name=f"{summary_name}.accountValue",
                    )
                    total_margin_used = _decimal_from_margin_value(
                        total_margin_used_raw,
                        field_name=f"{summary_name}.totalMarginUsed",
                    )
                    available = max(decimal.Decimal("0"), account_value - total_margin_used)
                    return float(available), f"{summary_name}.accountValue-totalMarginUsed"
                except RuntimeError:
                    pass

    candidate_paths = (
        ("availableToWithdraw",),
        ("availableBalance",),
        ("availableCollateral",),
        ("freeCollateral",),
        ("usableBalance",),
        ("totalWithdrawable",),
        ("crossMarginSummary", "withdrawable"),
        ("crossMarginSummary", "availableToWithdraw"),
        ("crossMarginSummary", "availableBalance"),
        ("crossMarginSummary", "availableCollateral"),
        ("crossMarginSummary", "freeCollateral"),
        ("marginSummary", "withdrawable"),
        ("marginSummary", "availableToWithdraw"),
        ("marginSummary", "availableBalance"),
        ("marginSummary", "availableCollateral"),
        ("marginSummary", "freeCollateral"),
        ("marginSummary", "accountValue"),
        ("crossMarginSummary", "accountValue"),
        ("portfolio", "accountValue"),
        ("accountValue",),
        ("totalAccountValue",),
        ("balance",),
    )
    for path in candidate_paths:
        current: Any = user_state
        valid = True
        for key in path:
            if not isinstance(current, dict):
                valid = False
                break
            current = current.get(key)
        if not valid:
            continue
        value = _try_float(current)
        if value is not None and value >= 0.0:
            return value, ".".join(path)

    recursive_available, recursive_available_path = _find_first_numeric_field(
        user_state,
        (
            "withdrawable",
            "availableToWithdraw",
            "availableBalance",
            "availableCollateral",
            "freeCollateral",
            "usableBalance",
            "totalWithdrawable",
        ),
    )
    if recursive_available is not None and recursive_available_path is not None:
        return recursive_available, recursive_available_path

    recursive_balance, recursive_balance_path = _find_first_numeric_field(
        user_state,
        ("accountValue", "totalAccountValue", "balance"),
    )
    if recursive_balance is not None and recursive_balance_path is not None:
        return recursive_balance, recursive_balance_path

    fallback_balance = extract_account_balance_from_user_state(user_state)
    if fallback_balance is not None and fallback_balance >= 0.0:
        return fallback_balance, "account_balance_fallback"
    return None, "unknown"


def extract_open_positions_by_coin(user_state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Build a coin->position map from one user_state snapshot."""
    positions_by_coin: Dict[str, Dict[str, Any]] = {}
    asset_positions = user_state.get("assetPositions", [])
    if not isinstance(asset_positions, list):
        return positions_by_coin

    for asset_pos in asset_positions:
        if not isinstance(asset_pos, dict):
            continue
        position = asset_pos.get("position", {})
        if not isinstance(position, dict):
            continue
        coin = str(position.get("coin", "")).upper()
        if not coin:
            continue
        try:
            if float(position.get("szi", "0")) == 0.0:
                continue
        except (TypeError, ValueError):
            continue
        positions_by_coin[coin] = position
    return positions_by_coin


def extract_realized_pnl_by_coin_since(fills: Any) -> Dict[str, float]:
    """Aggregate closed PnL by coin from one fills response."""
    realized_by_coin: Dict[str, float] = {}
    if not isinstance(fills, list):
        return realized_by_coin

    for fill in fills:
        if not isinstance(fill, dict):
            continue
        coin = str(fill.get("coin", "")).upper()
        if not coin:
            continue
        realized_by_coin[coin] = realized_by_coin.get(coin, 0.0) + extract_closed_pnl_from_fill(fill)
    return realized_by_coin


async def build_auto_scan_loop_snapshot(
    info: Info,
    account_address: str,
    metrics_start_time_ms: int,
) -> AutoScanLoopSnapshot:
    """Fetch shared scan-loop state once to avoid per-market REST duplication."""
    user_state = await get_user_state_with_retry(
        info,
        account_address,
        context_label="auto_scan_loop_snapshot",
    )
    positions_by_coin = extract_open_positions_by_coin(user_state)
    account_balance = extract_account_balance_from_user_state(user_state)

    mids_task = asyncio.create_task(get_all_mids(info))
    fills_task = asyncio.create_task(info.user_fills_by_time(account_address, metrics_start_time_ms))

    mids = await mids_task

    try:
        fills = await fills_task
    except Exception as exc:
        cp.warning(f"[AUTO-WARN] Failed to fetch user fills for session metrics: {exc}")
        fills = []
    realized_by_coin = extract_realized_pnl_by_coin_since(fills)

    return AutoScanLoopSnapshot(
        mids=mids,
        positions_by_coin=positions_by_coin,
        account_balance=account_balance,
        realized_pnl=sum(realized_by_coin.values()) if realized_by_coin or isinstance(fills, list) else None,
    )


def format_auto_scan_metrics(snapshot: AutoScanLoopSnapshot) -> str:
    """Format the shared auto-scan account metrics display."""
    realized = f"{snapshot.realized_pnl:.8f}" if snapshot.realized_pnl is not None else "N/A"
    balance = f"{snapshot.account_balance:.8f}" if snapshot.account_balance is not None else "N/A"
    return f"rpnl={realized} balance={balance}"


def _active_asset_side_index(side: str) -> int:
    normalized = str(side).strip().lower()
    if normalized in {"long", "buy", "b", "bid"}:
        return 0
    if normalized in {"short", "sell", "s", "ask"}:
        return 1
    raise RuntimeError(f"Unsupported side for activeAssetData sizing: {side!r}")


def normalize_bollinger_side(side: str) -> str:
    """Map project side aliases onto long/short confirmation rules."""
    normalized = str(side).strip().lower()
    if normalized in {"long", "buy", "b", "bid"}:
        return "long"
    if normalized in {"short", "sell", "s", "a", "ask"}:
        return "short"
    raise RuntimeError(f"Unsupported side for Bollinger confirmation: {side!r}")


def interval_to_seconds(interval: str) -> int:
    """Convert supported interval strings to seconds for dynamic ordering."""
    normalized = str(interval).strip()
    if normalized in INTERVAL_TO_MS:
        return int(INTERVAL_TO_MS[normalized] / 1000)

    lower = normalized.lower()
    if lower in INTERVAL_TO_MS:
        return int(INTERVAL_TO_MS[lower] / 1000)
    if lower.endswith("m"):
        return int(lower[:-1]) * 60
    if lower.endswith("h"):
        return int(lower[:-1]) * 60 * 60
    if lower.endswith("d"):
        return int(lower[:-1]) * 24 * 60 * 60
    if lower.endswith("w"):
        return int(lower[:-1]) * 7 * 24 * 60 * 60
    raise ValueError(f"Unsupported interval: {interval}")


def select_bollinger_intervals(active_intervals: List[str], scalp: bool) -> Dict[str, str]:
    """Select dynamic Bollinger confirmation intervals from the current auto run."""
    if not active_intervals:
        raise RuntimeError("No active intervals were configured for Bollinger confirmation.")

    ordered = sorted(active_intervals, key=interval_to_seconds)
    if scalp:
        return {"entry": ordered[0]}

    if len(ordered) == 1:
        return {"entry": ordered[0], "regime": ordered[0]}
    if len(ordered) == 2:
        return {"entry": ordered[0], "regime": ordered[1]}
    if len(ordered) == 3:
        return {"entry": ordered[0], "setup": ordered[1], "regime": ordered[2]}

    entry_index = 1 if ordered[0] == "1m" else 0
    return {
        "entry": ordered[entry_index],
        "setup": ordered[len(ordered) // 2],
        "regime": ordered[-1],
    }


def compute_bollinger_state(
    interval: str,
    closes: List[float],
    period: int = 20,
    stddev_multiplier: float = 2.0,
) -> BollingerState:
    """Compute current and previous Bollinger state for one interval."""
    require_talib_available()
    if period <= 1:
        raise RuntimeError(f"Bollinger period must be > 1, got {period}.")
    if len(closes) < period + 1:
        raise RuntimeError(
            f"{interval} has insufficient close data for Bollinger confirmation: "
            f"got {len(closes)}, need at least {period + 1}."
        )

    close_arr = np.asarray(closes, dtype=float)  # type: ignore[union-attr]
    upper_arr, basis_arr, lower_arr = talib.BBANDS(  # type: ignore[union-attr]
        close_arr,
        timeperiod=period,
        nbdevup=stddev_multiplier,
        nbdevdn=stddev_multiplier,
        matype=0,
    )

    current_idx = last_finite_index(close_arr, upper_arr, basis_arr, lower_arr)
    previous_idx: Optional[int] = None
    for idx in range(current_idx - 1, -1, -1):
        values = (close_arr[idx], upper_arr[idx], basis_arr[idx], lower_arr[idx])
        if all(math.isfinite(float(value)) for value in values):
            previous_idx = idx
            break
    if previous_idx is None:
        raise RuntimeError(f"{interval} does not have enough completed Bollinger windows for slope confirmation.")

    latest_close = float(close_arr[current_idx])
    upper = float(upper_arr[current_idx])
    basis = float(basis_arr[current_idx])
    lower = float(lower_arr[current_idx])
    previous_upper = float(upper_arr[previous_idx])
    previous_basis = float(basis_arr[previous_idx])
    previous_lower = float(lower_arr[previous_idx])

    band_span = upper - lower
    if abs(band_span) <= 0.0:
        raise RuntimeError(f"{interval} Bollinger band span is zero; skipping confirmation safely.")
    if basis == 0.0:
        raise RuntimeError(f"{interval} Bollinger basis is zero; skipping confirmation safely.")
    previous_band_span = previous_upper - previous_lower
    if previous_basis == 0.0:
        raise RuntimeError(f"{interval} previous Bollinger basis is zero; skipping confirmation safely.")

    percent_b = (latest_close - lower) / band_span
    basis_slope = basis - previous_basis
    bandwidth = band_span / basis
    previous_bandwidth = previous_band_span / previous_basis

    return BollingerState(
        interval=interval,
        basis=basis,
        upper=upper,
        lower=lower,
        percent_b=percent_b,
        basis_slope=basis_slope,
        bandwidth=bandwidth,
        previous_bandwidth=previous_bandwidth,
        bandwidth_expanding=bandwidth > previous_bandwidth,
        latest_close=latest_close,
    )


def _scalp_quality_label(side: str, percent_b: float) -> str:
    """Return scalp-mode quality label for logs."""
    if side == "long":
        if percent_b <= 0.25:
            return "ideal"
        if percent_b <= 0.35:
            return "good"
        if percent_b <= 0.50:
            return "valid"
        return "extended"
    if percent_b >= 0.75:
        return "ideal"
    if percent_b >= 0.65:
        return "good"
    if percent_b >= 0.50:
        return "valid"
    return "extended"


def confirm_signal_with_bollinger(
    side: str,
    interval_to_closes: Dict[str, List[float]],
    active_intervals: List[str],
    scalp: bool,
    period: int = 20,
    stddev_multiplier: float = 2.0,
) -> Tuple[bool, str, Dict[str, BollingerState]]:
    """Confirm an already-generated directional auto signal with Bollinger context."""
    try:
        normalized_side = normalize_bollinger_side(side)
        selected_intervals = select_bollinger_intervals(active_intervals, scalp)
    except Exception as exc:
        return False, str(exc), {}

    states: Dict[str, BollingerState] = {}
    for role, interval in selected_intervals.items():
        closes = interval_to_closes.get(interval)
        if not closes:
            return False, f"{role} interval {interval} has no usable close data for Bollinger confirmation.", states
        try:
            states[role] = compute_bollinger_state(
                interval=interval,
                closes=closes,
                period=period,
                stddev_multiplier=stddev_multiplier,
            )
        except Exception as exc:
            return False, f"{role} interval {interval} Bollinger confirmation failed: {exc}", states

    if scalp:
        entry_state = states["entry"]
        quality = _scalp_quality_label(normalized_side, entry_state.percent_b)
        if normalized_side == "long":
            allowed = entry_state.percent_b <= 0.50
            if not allowed:
                return False, (
                    f"mode=scalp side=long entry_tf={entry_state.interval} %B={entry_state.percent_b:.3f} "
                    f"result=REJECT reason=\"long rejected: entry interval above middle band\" quality={quality}"
                ), states
        else:
            allowed = entry_state.percent_b >= 0.50
            if not allowed:
                return False, (
                    f"mode=scalp side=short entry_tf={entry_state.interval} %B={entry_state.percent_b:.3f} "
                    f"result=REJECT reason=\"short rejected: entry interval below middle band\" quality={quality}"
                ), states

        return True, (
            f"mode=scalp side={normalized_side} entry_tf={entry_state.interval} %B={entry_state.percent_b:.3f} "
            f"basis={entry_state.basis:.8f} upper={entry_state.upper:.8f} lower={entry_state.lower:.8f} "
            f"result=PASS quality={quality}"
        ), states

    regime_state = states.get("regime")
    setup_state = states.get("setup")
    entry_state = states["entry"]

    if normalized_side == "long":
        if regime_state is not None:
            if regime_state.percent_b < 0.25 and regime_state.basis_slope < 0.0:
                return False, (
                    f"mode=non_scalp side=long regime_tf={regime_state.interval} setup_tf="
                    f"{setup_state.interval if setup_state is not None else 'N/A'} entry_tf={entry_state.interval} "
                    f"result=REJECT reason=\"long rejected: regime interval is in bearish breakdown\""
                ), states
            if regime_state.percent_b < 0.0 and regime_state.bandwidth_expanding:
                return False, (
                    f"mode=non_scalp side=long regime_tf={regime_state.interval} setup_tf="
                    f"{setup_state.interval if setup_state is not None else 'N/A'} entry_tf={entry_state.interval} "
                    f"result=REJECT reason=\"long rejected: regime interval is below lower band with expanding bandwidth\""
                ), states
        if setup_state is not None and setup_state.percent_b > 0.90:
            return False, (
                f"mode=non_scalp side=long regime_tf={regime_state.interval if regime_state is not None else 'N/A'} "
                f"setup_tf={setup_state.interval} entry_tf={entry_state.interval} "
                f"result=REJECT reason=\"long rejected: setup interval is already overextended high\""
            ), states
        if entry_state.percent_b > 0.50:
            return False, (
                f"mode=non_scalp side=long regime_tf={regime_state.interval if regime_state is not None else 'N/A'} "
                f"setup_tf={setup_state.interval if setup_state is not None else 'N/A'} entry_tf={entry_state.interval} "
                f"result=REJECT reason=\"long rejected: entry interval above middle band\""
            ), states
    else:
        if regime_state is not None:
            if regime_state.percent_b > 0.75 and regime_state.basis_slope > 0.0:
                return False, (
                    f"mode=non_scalp side=short regime_tf={regime_state.interval} setup_tf="
                    f"{setup_state.interval if setup_state is not None else 'N/A'} entry_tf={entry_state.interval} "
                    f"result=REJECT reason=\"short rejected: regime interval is in bullish breakout\""
                ), states
            if regime_state.percent_b > 1.0 and regime_state.bandwidth_expanding:
                return False, (
                    f"mode=non_scalp side=short regime_tf={regime_state.interval} setup_tf="
                    f"{setup_state.interval if setup_state is not None else 'N/A'} entry_tf={entry_state.interval} "
                    f"result=REJECT reason=\"short rejected: regime interval is above upper band with expanding bandwidth\""
                ), states
        if setup_state is not None and setup_state.percent_b < 0.10:
            return False, (
                f"mode=non_scalp side=short regime_tf={regime_state.interval if regime_state is not None else 'N/A'} "
                f"setup_tf={setup_state.interval} entry_tf={entry_state.interval} "
                f"result=REJECT reason=\"short rejected: setup interval is already overextended low\""
            ), states
        if entry_state.percent_b < 0.50:
            return False, (
                f"mode=non_scalp side=short regime_tf={regime_state.interval if regime_state is not None else 'N/A'} "
                f"setup_tf={setup_state.interval if setup_state is not None else 'N/A'} entry_tf={entry_state.interval} "
                f"result=REJECT reason=\"short rejected: entry interval below middle band\""
            ), states

    reason = (
        f"mode=non_scalp side={normalized_side} "
        f"regime_tf={regime_state.interval if regime_state is not None else 'N/A'} "
        f"setup_tf={setup_state.interval if setup_state is not None else 'N/A'} "
        f"entry_tf={entry_state.interval} result=PASS "
    )
    if regime_state is not None:
        reason += f"regime_b={regime_state.percent_b:.3f} "
    if setup_state is not None:
        reason += f"setup_b={setup_state.percent_b:.3f} "
    reason += f"entry_b={entry_state.percent_b:.3f}"
    return True, reason, states


async def resolve_size_pct_from_active_asset_data(
    info: Info,
    account_address: str,
    coin: str,
    side: str,
    size_pct_fraction: float,
) -> Tuple[Optional[float], str]:
    """Resolve size from Hyperliquid activeAssetData when available.

    Hyperliquid's side-specific `maxTradeSzs` value already reflects the
    leveraged trade-size ceiling for the asset. `availableToTrade` is useful
    telemetry, but using `min(availableToTrade, maxTradeSzs)` here incorrectly
    turns `--size-pct` into a percent of the unlevered side capacity.
    """
    try:
        active_asset_data = await info.post("/info", {"type": "activeAssetData", "user": account_address, "coin": coin})
    except Exception as exc:
        return None, f"activeAssetData request failed: {exc}"

    if not isinstance(active_asset_data, dict):
        return None, f"activeAssetData returned {type(active_asset_data).__name__}, expected dict"

    side_idx = _active_asset_side_index(side)
    available_to_trade = active_asset_data.get("availableToTrade")
    # available_to_trade = active_asset_data.get("max_trade_notional_usd")
    max_trade_szs = active_asset_data.get("maxTradeSzs")
    if not isinstance(available_to_trade, (list, tuple)) or len(available_to_trade) < 2:
        return None, "activeAssetData.availableToTrade missing long/short capacity"
    if not isinstance(max_trade_szs, (list, tuple)) or len(max_trade_szs) < 2:
        return None, "activeAssetData.maxTradeSzs missing long/short capacity"

    try:
        available_size = _decimal_from_margin_value(
            available_to_trade[side_idx],
            field_name=f"activeAssetData.availableToTrade[{side_idx}]",
        )
        max_trade_size = _decimal_from_margin_value(
            max_trade_szs[side_idx],
            field_name=f"activeAssetData.maxTradeSzs[{side_idx}]",
        )
        mark_price = _decimal_from_margin_value(
            active_asset_data.get("markPx"),
            field_name="activeAssetData.markPx",
        )
    except RuntimeError as exc:
        return None, f"activeAssetData numeric parse failed: {exc}"

    if mark_price <= 0:
        return None, f"activeAssetData.markPx must be > 0, got {mark_price}"

    size_pct_dec = decimal.Decimal(str(size_pct_fraction))
    capacity_size = max_trade_size
    derived_size = capacity_size * size_pct_dec
    rounded_size = await round_size_for_hyperliquid(info, coin, float(derived_size))
    if rounded_size <= 0.0:
        raise RuntimeError(
            f"--size-pct resolved to {float(derived_size):.12f} {coin} from activeAssetData, "
            "which rounds to zero for Hyperliquid precision."
        )
    return (
        rounded_size,
        (
            f"size_pct={size_pct_fraction:.4f} source=activeAssetData side={side} "
            f"available_size={available_size:.8f} max_trade_size={max_trade_size:.8f} "
            f"capacity_size={capacity_size:.8f} derived_size={derived_size:.8f} mark={mark_price:.8f}"
        ),
    )


def _extract_active_asset_data_leverage(active_asset_data: Dict[str, Any]) -> Tuple[Optional[decimal.Decimal], str]:
    """Extract the currently configured leverage from activeAssetData when present."""
    leverage = active_asset_data.get("leverage")
    if not isinstance(leverage, dict):
        return None, "activeAssetData.leverage missing"
    try:
        leverage_value = _decimal_from_margin_value(
            leverage.get("value"),
            field_name="activeAssetData.leverage.value",
        )
    except RuntimeError as exc:
        return None, f"activeAssetData leverage parse failed: {exc}"
    if leverage_value <= 0:
        return None, f"activeAssetData leverage must be > 0, got {leverage_value}"
    return leverage_value, "activeAssetData.leverage.value"


async def get_instrument_leverage_for_size_pct(
    info: Info,
    account_address: str,
    coin: str,
) -> Tuple[Optional[decimal.Decimal], str]:
    """Return leverage for manual --size-pct fallback sizing."""
    try:
        active_asset_data = await info.post("/info", {"type": "activeAssetData", "user": account_address, "coin": coin})
    except Exception as exc:
        active_asset_data = None
        active_asset_reason = f"activeAssetData request failed: {exc}"
    else:
        if isinstance(active_asset_data, dict):
            leverage_value, leverage_source = _extract_active_asset_data_leverage(active_asset_data)
            if leverage_value is not None:
                return leverage_value, leverage_source
            active_asset_reason = leverage_source
        else:
            active_asset_reason = f"activeAssetData returned {type(active_asset_data).__name__}, expected dict"

    try:
        meta = await info.meta()
    except Exception as exc:
        return None, f"{active_asset_reason}; meta request failed: {exc}"

    universe = meta.get("universe", []) if isinstance(meta, dict) else []
    if not isinstance(universe, list):
        return None, f"{active_asset_reason}; meta.universe missing"

    normalized_coin = str(coin).upper()
    for asset_info in universe:
        if not isinstance(asset_info, dict):
            continue
        asset_name = str(asset_info.get("name") or "").upper()
        if asset_name != normalized_coin:
            continue
        for key in ("maxLeverage", "maxLev", "max_leverage", "leverage"):
            raw_value = asset_info.get(key)
            if raw_value is None:
                continue
            try:
                leverage_value = _decimal_from_margin_value(raw_value, field_name=f"meta.universe[{normalized_coin}].{key}")
            except RuntimeError:
                continue
            if leverage_value > 0:
                return leverage_value, f"meta.universe.{key}"
        return None, f"{active_asset_reason}; no leverage field found in meta for {normalized_coin}"

    return None, f"{active_asset_reason}; {normalized_coin} missing from meta universe"





async def resolve_auto_trade_size(
    info: Info,
    account_address: str,
    coin: str,
    side: str,
    size: Optional[float],
    size_pct: Optional[Any],
) -> Tuple[float, str]:
    """Resolve auto-trade size from explicit size or available-collateral percentage."""
    if size is not None:
        if size <= 0.0:
            raise RuntimeError("--size must be > 0.")
        return size, f"fixed size={size:.8f}"

    if size_pct is None:
        raise RuntimeError("Specify either --size or --size-pct.")
    size_pct_fraction = parse_fractional_pct(size_pct, field_name="--size-pct")

    active_asset_size, active_asset_reason = await resolve_size_pct_from_active_asset_data(
        info=info,
        account_address=account_address,
        coin=coin,
        side=side,
        size_pct_fraction=size_pct_fraction,
    )
    if active_asset_size is not None:
        return active_asset_size, active_asset_reason

    user_state = await get_user_state_with_retry(
        info,
        account_address,
        context_label="resolve_auto_trade_size",
        coin=coin,
    )
    available_collateral, collateral_source = extract_available_collateral_from_user_state(user_state)
    if available_collateral is None or available_collateral <= 0.0:
        raise RuntimeError("Could not determine available collateral for --size-pct sizing.")

    mids = await get_all_mids(info)
    current_px = _try_float(mids.get(coin))
    if current_px is None or current_px <= 0.0:
        raise RuntimeError(f"No mid price available for {coin}; cannot derive size from --size-pct.")

    available_collateral_dec = decimal.Decimal(str(available_collateral))
    size_pct_dec = decimal.Decimal(str(size_pct_fraction))
    current_px_dec = decimal.Decimal(str(current_px))
    leverage_dec, leverage_source = await get_instrument_leverage_for_size_pct(
        info=info,
        account_address=account_address,
        coin=coin,
    )
    if leverage_dec is None or leverage_dec <= 0:
        raise RuntimeError(
            f"Could not determine leverage for {coin} during --size-pct sizing. "
            f"fallback_reason={active_asset_reason}; leverage_reason={leverage_source}"
        )

    usd_notional = available_collateral_dec * leverage_dec * size_pct_dec
    derived_size = usd_notional / current_px_dec
    rounded_size = await round_size_for_hyperliquid(info, coin, float(derived_size))
    if rounded_size <= 0.0:
        raise RuntimeError(
            f"--size-pct resolved to {float(derived_size):.12f} {coin}, which rounds to zero for Hyperliquid precision."
        )
    return (
        rounded_size,
        (
            f"size_pct={size_pct_fraction:.4f} collateral={available_collateral_dec:.8f} "
            f"source={collateral_source} leverage={leverage_dec:.8f} leverage_source={leverage_source} "
            f"notional={usd_notional:.8f} mid={current_px_dec:.8f} fallback_reason={active_asset_reason}"
        ),
    )


def last_finite_index(*arrays: Any) -> int:
    """Return the latest index where every TA-Lib array has a finite value."""
    if not arrays:
        raise RuntimeError("No arrays supplied for finite-index scan.")
    length = min(len(array) for array in arrays)
    for idx in range(length - 1, -1, -1):
        ok = True
        for array in arrays:
            try:
                value = float(array[idx])
            except (TypeError, ValueError):
                ok = False
                break
            if not math.isfinite(value):
                ok = False
                break
        if ok:
            return idx
    raise RuntimeError("No finite TA-Lib indicator row is available yet; fetch more candles.")


async def compute_auto_interval_signal(
    info: Info,
    coin: str,
    interval: str,
    periods: int,
    adx_threshold: float,
    macd_fast: int,
    macd_slow: int,
    macd_signal: int,
    sar_acceleration: float,
    sar_maximum: float,
    adx_timeperiod: int,
    bb_timeperiod: int,
    bb_dev: float,
    use_last_closed_candle: bool,
    use_websocket_candles: bool = False,
) -> AutoIntervalSignal:
    """Fetch candles for one interval and compute MACD/SAR/ADX/Bollinger signal state."""
    require_talib_available()
    minimum_periods = max(macd_slow + macd_signal + 5, adx_timeperiod + 5, bb_timeperiod + 5, 40)
    if periods < minimum_periods:
        raise RuntimeError(f"--auto-periods must be at least {minimum_periods} for the chosen indicator settings.")
    # TODO: Determine - is it possible to retrieve candles for multiple coins with one API call? Or is this data \
    # TODO: that is available over the websocket?
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

    if len(closes) < minimum_periods:
        raise RuntimeError(
            f"Not enough valid candles for {coin} {interval}: got {len(closes)}, need at least {minimum_periods}."
        )

    high_arr = np.asarray(highs, dtype=float)  # type: ignore[union-attr]
    low_arr = np.asarray(lows, dtype=float)  # type: ignore[union-attr]
    close_arr = np.asarray(closes, dtype=float)  # type: ignore[union-attr]

    macd_arr, macd_sig_arr, macd_hist_arr = talib.MACD(  # type: ignore[union-attr]
        close_arr,
        fastperiod=macd_fast,
        slowperiod=macd_slow,
        signalperiod=macd_signal,
    )
    sar_arr = talib.SAR(  # type: ignore[union-attr]
        high_arr,
        low_arr,
        acceleration=sar_acceleration,
        maximum=sar_maximum,
    )
    adx_arr = talib.ADX(  # type: ignore[union-attr]
        high_arr,
        low_arr,
        close_arr,
        timeperiod=adx_timeperiod,
    )
    bb_upper_arr, bb_middle_arr, bb_lower_arr = talib.BBANDS(  # type: ignore[union-attr]
        close_arr,
        timeperiod=bb_timeperiod,
        nbdevup=bb_dev,
        nbdevdn=bb_dev,
        matype=0,
    )

    idx = last_finite_index(close_arr, macd_arr, macd_sig_arr, macd_hist_arr, sar_arr, adx_arr, bb_upper_arr, bb_middle_arr, bb_lower_arr)
    close_px = float(close_arr[idx])
    macd_value = float(macd_arr[idx])
    macd_signal_value = float(macd_sig_arr[idx])
    macd_hist_value = float(macd_hist_arr[idx])
    sar_value = float(sar_arr[idx])
    adx_value = float(adx_arr[idx])
    bb_upper = float(bb_upper_arr[idx])
    bb_middle = float(bb_middle_arr[idx])
    bb_lower = float(bb_lower_arr[idx])

    macd_bullish = macd_value > macd_signal_value and macd_hist_value > 0.0
    macd_bearish = macd_value < macd_signal_value and macd_hist_value < 0.0
    sar_bullish = close_px > sar_value
    sar_bearish = close_px < sar_value
    trend_ok = adx_value >= adx_threshold

    if trend_ok and macd_bullish and sar_bullish:
        direction = "long"
    elif trend_ok and macd_bearish and sar_bearish:
        direction = "short"
    else:
        direction = "neutral"

    reason = (
        f"macd={'bull' if macd_bullish else 'bear' if macd_bearish else 'flat'} "
        f"sar={'bull' if sar_bullish else 'bear' if sar_bearish else 'flat'} "
        f"adx={adx_value:.2f}/{adx_threshold:.2f}"
    )

    return AutoIntervalSignal(
        interval=interval,
        closes=list(closes),
        close=close_px,
        macd=macd_value,
        macd_signal=macd_signal_value,
        macd_hist=macd_hist_value,
        sar=sar_value,
        adx=adx_value,
        bb_upper=bb_upper,
        bb_middle=bb_middle,
        bb_lower=bb_lower,
        direction=direction,
        reason=reason,
    )


def clamp_pct(value: float, minimum: float, maximum: float) -> float:
    """Clamp a percentage fraction to configured bounds."""
    return min(max(value, minimum), maximum)


def get_shortest_interval_snapshot(snapshots: List[AutoIntervalSignal]) -> Optional[AutoIntervalSignal]:
    """Return the snapshot for the shortest configured interval."""
    if not snapshots:
        return None
    return min(snapshots, key=lambda snapshot: INTERVAL_TO_MS.get(snapshot.interval, math.inf))




async def evaluate_auto_trade_decision(
    info: Info,
    coin: str,
    intervals: List[str],
    periods: int,
    min_agreement: int,
    adx_threshold: float,
    take_profit_pct_override: Optional[float],
    stop_loss_pct_override: Optional[float],
    min_take_profit_pct: float,
    max_take_profit_pct: float,
    macd_fast: int,
    macd_slow: int,
    macd_signal_period: int,
    sar_acceleration: float,
    sar_maximum: float,
    adx_timeperiod: int,
    bb_timeperiod: int,
    bb_dev: float,
    use_last_closed_candle: bool,
    use_sar_stop_on_shortest_interval: bool,
    use_websocket_candles: bool = False,
    current_px: Optional[float] = None,
) -> AutoTradeDecision:
    """Evaluate all configured intervals and return an aggregated trade decision."""
    snapshots: List[AutoIntervalSignal] = []
    errors: List[str] = []
    for interval in intervals:
        try:
            snapshots.append(
                await compute_auto_interval_signal(
                    info=info,
                    coin=coin,
                    interval=interval,
                    periods=periods,
                    adx_threshold=adx_threshold,
                    macd_fast=macd_fast,
                    macd_slow=macd_slow,
                    macd_signal=macd_signal_period,
                    sar_acceleration=sar_acceleration,
                    sar_maximum=sar_maximum,
                    adx_timeperiod=adx_timeperiod,
                    bb_timeperiod=bb_timeperiod,
                    bb_dev=bb_dev,
                    use_last_closed_candle=use_last_closed_candle,
                    use_websocket_candles=use_websocket_candles,
                )
            )
        except Exception as exc:
            errors.append(f"{interval}: {exc}")
            cp.warning(f"[AUTO-WARN] Failed to compute signal for {coin} {interval}: {exc}")

    if not snapshots:
        return AutoTradeDecision(
            direction=None,
            current_px=0.0,
            target_px=None,
            take_profit_pct=None,
            stop_loss_pct=None,
            stop_loss_trigger_px=None,
            long_votes=0,
            short_votes=0,
            required_votes=max(1, min_agreement),
            snapshots=[],
            reason="No usable interval snapshots. " + "; ".join(errors),
        )

    required_votes = len(intervals) if min_agreement <= 0 else min_agreement
    long_votes = sum(1 for snapshot in snapshots if snapshot.direction == "long")
    short_votes = sum(1 for snapshot in snapshots if snapshot.direction == "short")

    if current_px is None or current_px <= 0.0:
        current_px = float(snapshots[-1].close)

    if len(snapshots) < required_votes:
        return AutoTradeDecision(
            direction=None,
            current_px=current_px,
            target_px=None,
            take_profit_pct=None,
            stop_loss_pct=None,
            stop_loss_trigger_px=None,
            long_votes=long_votes,
            short_votes=short_votes,
            required_votes=required_votes,
            snapshots=snapshots,
            reason=f"Only {len(snapshots)} usable snapshots; required {required_votes}.",
        )

    direction: Optional[str]
    if long_votes >= required_votes and long_votes > short_votes:
        direction = "long"
    elif short_votes >= required_votes and short_votes > long_votes:
        direction = "short"
    else:
        direction = None

    if direction is None:
        return AutoTradeDecision(
            direction=None,
            current_px=current_px,
            target_px=None,
            take_profit_pct=None,
            stop_loss_pct=None,
            stop_loss_trigger_px=None,
            long_votes=long_votes,
            short_votes=short_votes,
            required_votes=required_votes,
            snapshots=snapshots,
            reason=f"No trade: long_votes={long_votes}, short_votes={short_votes}, required={required_votes}.",
        )

    if direction == "long":
        candidate_targets = sorted(snapshot.bb_upper for snapshot in snapshots if snapshot.bb_upper > current_px)
        raw_target_px = candidate_targets[0] if candidate_targets else current_px * (1.0 + min_take_profit_pct)
        raw_take_profit_pct = max(0.0, (raw_target_px - current_px) / current_px)
    else:
        candidate_targets = sorted(
            (snapshot.bb_lower for snapshot in snapshots if snapshot.bb_lower < current_px),
            reverse=True,
        )
        raw_target_px = candidate_targets[0] if candidate_targets else current_px * (1.0 - min_take_profit_pct)
        raw_take_profit_pct = max(0.0, (current_px - raw_target_px) / current_px)

    if take_profit_pct_override is not None:
        take_profit_pct = take_profit_pct_override
    else:
        take_profit_pct = clamp_pct(raw_take_profit_pct, min_take_profit_pct, max_take_profit_pct)

    if direction == "long":
        target_px = current_px * (1.0 + take_profit_pct)
    else:
        target_px = current_px * (1.0 - take_profit_pct)

    stop_loss_pct = compute_default_stop_loss_pct(take_profit_pct, stop_loss_pct_override)
    stop_loss_trigger_px: Optional[float] = None
    shortest_snapshot = get_shortest_interval_snapshot(snapshots)
    if use_sar_stop_on_shortest_interval and shortest_snapshot is not None:
        candidate_stop = float(shortest_snapshot.sar)
        if direction == "long" and candidate_stop < current_px:
            stop_loss_trigger_px = candidate_stop
        elif direction == "short" and candidate_stop > current_px:
            stop_loss_trigger_px = candidate_stop
    return AutoTradeDecision(
        direction=direction,
        current_px=current_px,
        target_px=target_px,
        take_profit_pct=take_profit_pct,
        stop_loss_pct=stop_loss_pct,
        stop_loss_trigger_px=stop_loss_trigger_px,
        long_votes=long_votes,
        short_votes=short_votes,
        required_votes=required_votes,
        snapshots=snapshots,
        reason=(
            f"{direction.upper()} signal: long_votes={long_votes}, short_votes={short_votes}, "
            f"required={required_votes}, bb_target={raw_target_px:.8f}, tp_pct={take_profit_pct * 100:.4f}%"
        ),
    )


def print_auto_decision(decision: AutoTradeDecision, instrument: str) -> None:
    """Print a compact multi-timeframe signal summary."""
    cp.normal(f"{instrument.upper()} Interval signals:", 'AUTO')
    if not decision.snapshots:
        print("  no usable snapshots")
    for snapshot in decision.snapshots:
        print(
            f"  {snapshot.interval:>4} | {snapshot.direction:>7} | close={snapshot.close:.8f} "
            f"macd={snapshot.macd:.8f}/{snapshot.macd_signal:.8f} hist={snapshot.macd_hist:.8f} "
            f"sar={snapshot.sar:.8f} adx={snapshot.adx:.2f} "
            f"bb=({snapshot.bb_lower:.8f}, {snapshot.bb_middle:.8f}, {snapshot.bb_upper:.8f}) "
            f"{snapshot.reason}"
        )
    target = f"{decision.target_px:.8f}" if decision.target_px is not None else "N/A"
    tp_pct = f"{decision.take_profit_pct * 100:.4f}%" if decision.take_profit_pct is not None else "N/A"
    sl_pct = f"{decision.stop_loss_pct * 100:.4f}%" if decision.stop_loss_pct is not None else "N/A"
    sl_trigger = f"{decision.stop_loss_trigger_px:.8f}" if decision.stop_loss_trigger_px is not None else "N/A"
    data = f"decision={decision.direction or 'none'} current={decision.current_px:.8f} "
    f"target={target} tp={tp_pct} sl={sl_pct} sl_trigger={sl_trigger} reason={decision.reason}"
    if decision.direction is None:
        cp.warning(data, 'AUTO')
    else:
        if decision.direction.lower() == "long":
            cp.good(
                data, 'AUTO'
            )
        elif decision.direction.lower() == "short":
            cp.error(
                data, 'AUTO'
            )


async def scan_auto_trade_candidate(
    info: Info,
    scan_coin: str,
    intervals: List[str],
    periods: int,
    min_agreement: int,
    adx_threshold: float,
    take_profit_pct: Optional[float],
    stop_loss_pct: Optional[float],
    min_take_profit_pct: float,
    max_take_profit_pct: float,
    macd_fast: int,
    macd_slow: int,
    macd_signal_period: int,
    sar_acceleration: float,
    sar_maximum: float,
    adx_timeperiod: int,
    bb_timeperiod: int,
    bb_dev: float,
    scalp: bool,
    use_last_closed_candle: bool,
    use_sar_stop_on_shortest_interval: bool,
    snapshot: AutoScanLoopSnapshot,
    use_websocket_candles: bool = False,
) -> AutoScanCandidate:
    """Scan one market using shared loop snapshots to avoid redundant REST calls."""
    existing_pos = snapshot.positions_by_coin.get(scan_coin.upper())
    if existing_pos is not None:
        return AutoScanCandidate(coin=scan_coin, decision=None, existing_position=existing_pos)

    decision = await evaluate_auto_trade_decision(
        info=info,
        coin=scan_coin,
        intervals=intervals,
        periods=periods,
        min_agreement=min_agreement,
        adx_threshold=adx_threshold,
        take_profit_pct_override=take_profit_pct,
        stop_loss_pct_override=stop_loss_pct,
        min_take_profit_pct=min_take_profit_pct,
        max_take_profit_pct=max_take_profit_pct,
        macd_fast=macd_fast,
        macd_slow=macd_slow,
        macd_signal_period=macd_signal_period,
        sar_acceleration=sar_acceleration,
        sar_maximum=sar_maximum,
        adx_timeperiod=adx_timeperiod,
        bb_timeperiod=bb_timeperiod,
        bb_dev=bb_dev,
        use_last_closed_candle=use_last_closed_candle,
        use_sar_stop_on_shortest_interval=use_sar_stop_on_shortest_interval,
        use_websocket_candles=use_websocket_candles,
        current_px=snapshot.mids.get(scan_coin.upper()),
    )
    print_auto_decision(decision, scan_coin)

    if decision.direction is None or decision.take_profit_pct is None:
        return AutoScanCandidate(coin=scan_coin, decision=decision, existing_position=None)

    interval_to_closes = {interval_snapshot.interval: list(interval_snapshot.closes) for interval_snapshot in decision.snapshots}
    bb_allowed, bb_reason, _bb_states = confirm_signal_with_bollinger(
        side=decision.direction,
        interval_to_closes=interval_to_closes,
        active_intervals=intervals,
        scalp=scalp,
        period=bb_timeperiod,
        stddev_multiplier=bb_dev,
    )
    print(f"[BB] {scan_coin} {bb_reason}")
    if not bb_allowed:
        print(f"[AUTO] {scan_coin} Bollinger confirmation rejected; skipping entry this loop.")
        return AutoScanCandidate(
            coin=scan_coin,
            decision=decision,
            existing_position=None,
            rejection_reason="bollinger_confirmation_rejected",
        )

    return AutoScanCandidate(coin=scan_coin, decision=decision, existing_position=None)


async def run_auto_trader(
    coin: Optional[str],
    size: Optional[float],
    size_pct: Optional[Any],
    top_markets: int,
    intervals_value: str,
    periods: int,
    scan_interval: float,
    max_concurrent_scans: int,
    max_positions: int,
    min_agreement: int,
    adx_threshold: float,
    take_profit_pct: Optional[float],
    stop_loss_pct: Optional[float],
    min_take_profit_pct: float,
    max_take_profit_pct: float,
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
    macd_fast: int,
    macd_slow: int,
    macd_signal_period: int,
    sar_acceleration: float,
    sar_maximum: float,
    adx_timeperiod: int,
    bb_timeperiod: int,
    bb_dev: float,
    scalp: bool,
    use_last_closed_candle: bool,
    use_sar_stop_on_shortest_interval: bool,
    dry_run: bool,
    max_trades: int,
    cooldown_after_trade: float,
    loop_after_trade: bool,
    max_coin_trades_per_session: int,
    coin_session_cooldown_seconds: float,
    coin_session_profit_target: float,
    coin_session_min_profit_to_lock: float,
    coin_session_giveback_pct: float,
    cooldown_after_loss_following_wins: int,
    session_profit_target: float,
    session_max_loss: float,
    session_giveback_pct: float,
    use_testnet: bool,
    use_websocket: bool = True,
    use_websocket_candles: bool = False,
    hide_orders: bool = False,
    risk_session_log: str = "",
    account_address: Optional[str] = None,
    info: Optional[Info] = None,
    exchange: Optional[Exchange] = None,
) -> None:
    """Automatically scan TA-Lib signals, enter positions, and hand off to bracket management."""

    require_talib_available()
    coin = coin.upper() if coin is not None else None
    intervals = parse_interval_list(intervals_value)
    if (size is None) == (size_pct is None):
        raise RuntimeError("Specify exactly one of --size or --size-pct.")
    if size is not None and size <= 0.0:
        raise RuntimeError("--size must be > 0.")
    size_pct_fraction: Optional[float] = None
    if size_pct is not None:
        size_pct_fraction = parse_fractional_pct(size_pct, field_name="--size-pct")
    if top_markets <= 0:
        raise RuntimeError("--top-markets must be > 0.")
    if periods <= 0:
        raise RuntimeError("--auto-periods must be > 0.")
    if scan_interval <= 0.0:
        raise RuntimeError("--scan-interval must be > 0.")
    if max_concurrent_scans <= 0:
        raise RuntimeError("--max-concurrent-scans must be > 0.")
    if max_positions <= 0:
        raise RuntimeError("--max-positions must be > 0.")
    if min_agreement < 0:
        raise RuntimeError("--min-agreement must be >= 0. Use 0 to require all configured intervals.")
    if adx_threshold < 0.0:
        raise RuntimeError("--adx-threshold must be >= 0.")
    if take_profit_pct is not None and not (0.0 < take_profit_pct < 1.0):
        raise RuntimeError("--take-profit-pct must be between 0 and 1.")
    if stop_loss_pct is not None and not (0.0 < stop_loss_pct < 1.0):
        raise RuntimeError("--stop-loss-pct must be between 0 and 1.")
    if not (0.0 < min_take_profit_pct < 1.0):
        raise RuntimeError("--min-take-profit-pct must be between 0 and 1.")
    if not (0.0 < max_take_profit_pct < 1.0):
        raise RuntimeError("--max-take-profit-pct must be between 0 and 1.")
    if min_take_profit_pct > max_take_profit_pct:
        raise RuntimeError("--min-take-profit-pct cannot be greater than --max-take-profit-pct.")
    if take_profit_levels <= 0:
        raise RuntimeError("--take-profit-levels must be > 0.")
    if trailing_tp_trigger_level <= 0:
        raise RuntimeError("--trailing-tp-trigger-level must be > 0.")
    if trailing_tp_trigger_level > take_profit_levels:
        raise RuntimeError("--trailing-tp-trigger-level cannot exceed --take-profit-levels.")
    if not (0.0 < trailing_tp_profit_pct < 1.0):
        raise RuntimeError("--trailing-tp-profit-pct must be between 0 and 1.")
    if entry_retries < 0:
        raise RuntimeError("--entry-retries must be >= 0.")
    if entry_repost_interval <= 0.0:
        raise RuntimeError("--entry-repost-interval must be > 0.")
    if poll_interval <= 0.0:
        raise RuntimeError("--poll-interval must be > 0.")
    if max_trades < 0:
        raise RuntimeError("--max-trades must be >= 0.")
    if cooldown_after_trade < 0.0:
        raise RuntimeError("--cooldown-after-trade must be >= 0.")
    if max_coin_trades_per_session < 0:
        raise RuntimeError("--max-coin-trades-per-session must be >= 0.")
    if coin_session_cooldown_seconds < 0.0:
        raise RuntimeError("--coin-session-cooldown-seconds must be >= 0.")
    if coin_session_profit_target < 0.0:
        raise RuntimeError("--coin-session-profit-target must be >= 0.")
    if coin_session_min_profit_to_lock < 0.0:
        raise RuntimeError("--coin-session-min-profit-to-lock must be >= 0.")
    if not (0.0 <= coin_session_giveback_pct < 1.0):
        raise RuntimeError("--coin-session-giveback-pct must be >= 0 and < 1.")
    if cooldown_after_loss_following_wins < 0:
        raise RuntimeError("--cooldown-after-loss-following-wins must be >= 0.")
    if session_profit_target < 0.0:
        raise RuntimeError("--session-profit-target must be >= 0.")
    if session_max_loss < 0.0:
        raise RuntimeError("--session-max-loss must be >= 0.")
    if not (0.0 <= session_giveback_pct < 1.0):
        raise RuntimeError("--session-giveback-pct must be >= 0 and < 1.")
    if use_websocket_candles and not use_websocket:
        raise RuntimeError("--ws-candles requires websocket market data. Remove --no-websocket to enable it.")

    owns_clients = account_address is None and info is None and exchange is None
    if not owns_clients and (account_address is None or info is None or exchange is None):
        raise RuntimeError("Pass account_address, info, and exchange together when reusing initialized clients.")
    completed_trades = 0
    auto_trades_logger = get_auto_trades_logger()

    try:
        if owns_clients:
            account_address, info, exchange = await init_clients(use_testnet, use_websocket=use_websocket)
        metrics_start_time_ms = int(time.time() * 1000)
        risk_session = AutoRiskSessionState(started_ms=metrics_start_time_ms)
        coin_sessions: Dict[str, CoinTradeSessionState] = {}
        active_trade_tasks: Dict[str, asyncio.Task[AutoManagedTradeResult]] = {}
        stop_requested = False
        stop_reason = ""
        print("============================================================")
        print(" Hyperliquid Async Auto Trader")
        print("============================================================")
        print(f"Account:            {account_address}")
        print(f"Network:            {'TESTNET' if use_testnet else 'MAINNET'}")
        print(f"Websocket:          {'ENABLED' if use_websocket else 'DISABLED'}")
        print(f"WS candles:         {'ENABLED' if use_websocket_candles else 'DISABLED'}")
        print(f"Hide orders:        {hide_orders}")
        print(f"Coin scope:         {coin if coin is not None else f'TOP {top_markets} PERPS BY VOLUME'}")
        print(f"Size mode:          {'fixed contracts' if size is not None else 'available collateral pct'}")
        if size is not None:
            print(f"Size:               {size:.8f}")
        else:
            print(f"Size pct:           {size_pct_fraction:.4f}")
        print(f"Intervals:          {', '.join(intervals)}")
        print(f"Bollinger confirm:  {'SCALP' if scalp else 'NON-SCALP'}")
        print(f"Required agreement: {'ALL' if min_agreement == 0 else min_agreement}")
        print(f"ADX threshold:      {adx_threshold:.2f}")
        print(f"Bollinger:          timeperiod={bb_timeperiod}, dev={bb_dev}")
        print(f"SAR stop mode:      {use_sar_stop_on_shortest_interval}")
        print(f"Trailing TP:        {use_trailing_tp}")
        if use_trailing_tp:
            print(f"Trailing TP level:  {trailing_tp_trigger_level}")
            print(f"Trailing TP pct:    {trailing_tp_profit_pct * 100:.4f}% of favorable unrealized profit")
        print(f"Scan interval:      {scan_interval:.2f}s")
        print(f"Scan concurrency:   {max_concurrent_scans}")
        print(f"Max positions:      {max_positions}")
        print(f"Dry run:            {dry_run}")
        print(f"Max trades:         {'unlimited' if max_trades == 0 else max_trades}")
        print(f"Loop after trade:   {loop_after_trade}")
        print("Coin risk controls:")
        print(f"  max_coin_trades_per_session={max_coin_trades_per_session}")
        print(f"  coin_session_cooldown_seconds={coin_session_cooldown_seconds}")
        print(f"  coin_session_profit_target={coin_session_profit_target}")
        print(f"  coin_session_min_profit_to_lock={coin_session_min_profit_to_lock}")
        print(f"  coin_session_giveback_pct={coin_session_giveback_pct}")
        print(f"  cooldown_after_loss_following_wins={cooldown_after_loss_following_wins}")
        print("Session risk controls:")
        print(f"  session_profit_target={session_profit_target}")
        print(f"  session_max_loss={session_max_loss}")
        print(f"  session_giveback_pct={session_giveback_pct}")
        print("============================================================")

        async def _run_managed_auto_trade(
            managed_coin: str,
            managed_direction: str,
            managed_size: float,
            managed_tp_pct: float,
            managed_sl_pct: Optional[float],
            managed_sl_trigger_px: Optional[float],
            managed_shortest_snapshot: Optional[AutoIntervalSignal],
        ) -> AutoManagedTradeResult:
            trade_cycle_started_ms = now_ms()
            await run_bracket_entry(
                coin=managed_coin,
                direction=managed_direction,
                size=managed_size,
                take_profit_pct=managed_tp_pct,
                stop_loss_pct=managed_sl_pct,
                stop_loss_trigger_px=managed_sl_trigger_px,
                take_profit_levels=take_profit_levels,
                use_trailing_tp=use_trailing_tp,
                trailing_tp_trigger_level=trailing_tp_trigger_level,
                trailing_tp_profit_pct=trailing_tp_profit_pct,
                entry_retries=entry_retries,
                entry_repost_interval=entry_repost_interval,
                poll_interval=poll_interval,
                tp_reversal_pct=tp_reversal_pct,
                entry_tif=entry_tif,
                tp_tif=tp_tif,
                market_fallback=market_fallback,
                market_slippage=market_slippage,
                cancel_existing_tpsl=cancel_existing_tpsl,
                tp_reversal_limit_exit=tp_reversal_limit_exit,
                tp_reversal_stop_buffer_pct=tp_reversal_stop_buffer_pct,
                use_testnet=use_testnet,
                use_websocket=use_websocket,
                hide_orders=hide_orders,
                auto_sar_stop_interval=(
                    managed_shortest_snapshot.interval
                    if use_sar_stop_on_shortest_interval and managed_shortest_snapshot is not None
                    else None
                ),
                auto_sar_stop_periods=periods if use_sar_stop_on_shortest_interval else None,
                auto_sar_acceleration=sar_acceleration if use_sar_stop_on_shortest_interval else None,
                auto_sar_maximum=sar_maximum if use_sar_stop_on_shortest_interval else None,
                auto_use_last_closed_candle=use_last_closed_candle,
                auto_use_websocket_candles=use_websocket_candles,
                account_address=account_address,
                info=info,
                exchange=exchange,
            )
            return AutoManagedTradeResult(
                coin=managed_coin,
                direction=managed_direction,
                size=managed_size,
                take_profit_pct=managed_tp_pct,
                stop_loss_pct=managed_sl_pct,
                started_ms=trade_cycle_started_ms,
                closed_ms=now_ms(),
            )

        async def _drain_completed_trade_tasks() -> None:
            nonlocal completed_trades, stop_requested, stop_reason
            finished_coins = [task_coin for task_coin, task in active_trade_tasks.items() if task.done()]
            for finished_coin in finished_coins:
                task = active_trade_tasks.pop(finished_coin)
                try:
                    trade_result = task.result()
                except asyncio.CancelledError:
                    print(f"[AUTO] Managed trade task for {finished_coin} was cancelled.")
                    continue
                except Exception as exc:
                    print(f"[AUTO-WARN] Managed trade task failed for {finished_coin}: {exc}")
                    logger.exception("[AUTO] Managed trade task failed for %s", finished_coin)
                    continue

                completed_trades += 1
                auto_trades_logger.info(
                    "[AUTO-TRADE-COMPLETE] coin=%s direction=%s size=%.8f tp_pct=%.8f sl_pct=%s completed_trades=%d",
                    trade_result.coin,
                    trade_result.direction,
                    trade_result.size,
                    trade_result.take_profit_pct,
                    f"{trade_result.stop_loss_pct:.8f}" if trade_result.stop_loss_pct is not None else "N/A",
                    completed_trades,
                )

                if info is None or exchange is None:
                    raise RuntimeError("Initialized clients became unavailable while draining auto trade tasks.")

                cycle_pnl, cycle_fills, pnl_reason = await fetch_trade_cycle_net_pnl(
                    info=info,
                    account_address=account_address,
                    coin=trade_result.coin,
                    start_time_ms=trade_result.started_ms,
                    end_time_ms=trade_result.closed_ms,
                )
                if pnl_reason:
                    print(
                        f"[AUTO-WARN] {trade_result.coin} post-trade PnL unavailable; "
                        f"risk session not updated for this cycle: {pnl_reason}"
                    )
                    _append_risk_event_log(
                        risk_session_log,
                        {
                            "ts_ms": trade_result.closed_ms,
                            "event": "cycle_pnl_unavailable",
                            "coin": trade_result.coin,
                            "reason": pnl_reason,
                            "session_pnl": risk_session.realized_pnl,
                        },
                    )
                else:
                    coin_state = get_or_create_coin_session(coin_sessions, trade_result.coin)
                    coin_reason, session_reason = update_risk_after_trade_cycle(
                        coin_state=coin_state,
                        session_state=risk_session,
                        cycle_pnl=cycle_pnl,
                        cycle_started_ms=trade_result.started_ms,
                        cycle_closed_ms=trade_result.closed_ms,
                        max_coin_trades_per_session=max_coin_trades_per_session,
                        coin_session_cooldown_seconds=coin_session_cooldown_seconds,
                        coin_session_profit_target=coin_session_profit_target,
                        coin_session_min_profit_to_lock=coin_session_min_profit_to_lock,
                        coin_session_giveback_pct=coin_session_giveback_pct,
                        cooldown_after_loss_following_wins=cooldown_after_loss_following_wins,
                        session_profit_target=session_profit_target,
                        session_max_loss=session_max_loss,
                        session_giveback_pct=session_giveback_pct,
                    )
                    print(
                        f"[AUTO-RISK] {trade_result.coin} cycle_pnl={cycle_pnl:.6f} "
                        f"coin_pnl={coin_state.realized_pnl:.6f} coin_peak={coin_state.peak_pnl:.6f} "
                        f"coin_cycles={coin_state.completed_cycles} session_pnl={risk_session.realized_pnl:.6f} "
                        f"session_peak={risk_session.peak_pnl:.6f}"
                    )
                    _append_risk_event_log(
                        risk_session_log,
                        {
                            "ts_ms": trade_result.closed_ms,
                            "event": "cycle_complete",
                            "coin": trade_result.coin,
                            "cycle_pnl": cycle_pnl,
                            "coin_pnl": coin_state.realized_pnl,
                            "coin_peak_pnl": coin_state.peak_pnl,
                            "coin_cycles": coin_state.completed_cycles,
                            "session_pnl": risk_session.realized_pnl,
                            "session_peak_pnl": risk_session.peak_pnl,
                            "fills_count": len(cycle_fills),
                        },
                    )
                    if coin_reason is not None:
                        print(
                            f"[AUTO-RISK] {trade_result.coin} entering cooldown for "
                            f"{coin_session_cooldown_seconds:.1f}s reason=\"{coin_reason}\""
                        )
                        _append_risk_event_log(
                            risk_session_log,
                            {
                                "ts_ms": trade_result.closed_ms,
                                "event": "coin_cooldown",
                                "coin": trade_result.coin,
                                "reason": coin_reason,
                                "coin_pnl": coin_state.realized_pnl,
                                "session_pnl": risk_session.realized_pnl,
                            },
                        )
                    if session_reason is not None:
                        print(f"[AUTO-RISK] Session stop triggered: {session_reason}")
                        _append_risk_event_log(
                            risk_session_log,
                            {
                                "ts_ms": trade_result.closed_ms,
                                "event": "session_stop",
                                "coin": trade_result.coin,
                                "reason": session_reason,
                                "coin_pnl": coin_state.realized_pnl,
                                "session_pnl": risk_session.realized_pnl,
                            },
                        )

                if cooldown_after_trade > 0.0:
                    coin_state = get_or_create_coin_session(coin_sessions, trade_result.coin)
                    coin_state.post_trade_cooldown_until_ms = max(
                        coin_state.post_trade_cooldown_until_ms,
                        trade_result.closed_ms + int(cooldown_after_trade * 1000),
                    )
                    cooldown_now_ms = now_ms()
                    remaining_seconds = max(
                        0.0,
                        (coin_state.post_trade_cooldown_until_ms - cooldown_now_ms) / 1000.0,
                    )
                    print(
                        f"[AUTO] {trade_result.coin} post-trade cooldown active for "
                        f"{remaining_seconds:.2f}s after close."
                    )

                if risk_session.stopped:
                    stop_requested = True
                    stop_reason = risk_session.stop_reason or "session risk stop"
                elif 0 < max_trades <= completed_trades:
                    stop_requested = True
                    stop_reason = f"max trades reached ({completed_trades})"
                elif not loop_after_trade:
                    stop_requested = True
                    stop_reason = f"completed {completed_trades} trade(s) and auto looping is disabled"

        while True:
            await _drain_completed_trade_tasks()
            if stop_requested:
                if active_trade_tasks:
                    print(
                        f"[AUTO] Stop requested ({stop_reason}); waiting for "
                        f"{len(active_trade_tasks)} active managed trade(s) to finish."
                    )
                    done, _pending = await asyncio.wait(
                        list(active_trade_tasks.values()),
                        timeout=min(scan_interval, 5.0),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if not done:
                        await asyncio.sleep(min(scan_interval, 1.0))
                    continue
                print(f"[AUTO] {stop_reason}; exiting auto mode.")
                return

            if coin is not None:
                scan_coins = [coin]
            else:
                ranked_markets = await get_top_perp_markets_by_volume(info, top_markets)
                scan_coins = [market_coin for market_coin, _ in ranked_markets]
                market_labels = ", ".join(f"{market_coin}({volume:.0f})" for market_coin, volume in ranked_markets)
                print(f"[AUTO] Top volume scan set: {market_labels}")
                if not scan_coins:
                    print("[AUTO-WARN] No perp markets with usable volume data were returned.")
                    await asyncio.sleep(scan_interval)
                    continue

            eligible_scan_coins: List[str] = []
            current_time_ms = now_ms()
            for scan_coin in scan_coins:
                coin_state = get_or_create_coin_session(coin_sessions, scan_coin)
                reset_coin_session_after_cooldown_if_needed(
                    coin_state,
                    current_time_ms,
                    risk_session_log=risk_session_log,
                    session_pnl=risk_session.realized_pnl,
                )
                in_post_trade_cooldown, post_trade_reason = coin_is_in_post_trade_cooldown(
                    coin_state,
                    current_time_ms,
                )
                if in_post_trade_cooldown:
                    print(f"[AUTO] Skipping {scan_coin}: {post_trade_reason}")
                    continue
                is_blocked, block_reason = coin_is_blocked_by_risk(coin_state, current_time_ms)
                if is_blocked:
                    print(f"[AUTO-RISK] Skipping {scan_coin}: {block_reason}")
                    _append_risk_event_log(
                        risk_session_log,
                        {
                            "ts_ms": current_time_ms,
                            "event": "coin_skipped",
                            "coin": coin_state.coin,
                            "reason": coin_state.cooldown_reason,
                            "blocked_detail": block_reason,
                            "coin_pnl": coin_state.realized_pnl,
                            "session_pnl": risk_session.realized_pnl,
                        },
                    )
                    continue
                eligible_scan_coins.append(scan_coin)

            if not eligible_scan_coins:
                print("[AUTO] All scan candidates are temporarily blocked by cooldown or coin session controls.")
                await asyncio.sleep(scan_interval)
                continue

            shared_snapshot = await build_auto_scan_loop_snapshot(
                info=info,
                account_address=account_address,
                metrics_start_time_ms=metrics_start_time_ms,
            )
            reserved_coins = set(active_trade_tasks.keys()) | set(shared_snapshot.positions_by_coin.keys())
            available_slots = max_positions - len(reserved_coins)
            if available_slots <= 0:
                print(
                    f"[AUTO] Max position capacity reached "
                    f"({len(reserved_coins)}/{max_positions}); delaying new entries."
                )
                await asyncio.sleep(scan_interval)
                continue

            scan_concurrency = min(max_concurrent_scans, max(1, len(eligible_scan_coins)))
            scan_semaphore = asyncio.Semaphore(scan_concurrency)

            async def _scan_with_limit(scan_coin: str) -> AutoScanCandidate:
                async with scan_semaphore:
                    return await scan_auto_trade_candidate(
                        info=info,
                        scan_coin=scan_coin,
                        intervals=intervals,
                        periods=periods,
                        min_agreement=min_agreement,
                        adx_threshold=adx_threshold,
                        take_profit_pct=take_profit_pct,
                        stop_loss_pct=stop_loss_pct,
                        min_take_profit_pct=min_take_profit_pct,
                        max_take_profit_pct=max_take_profit_pct,
                        macd_fast=macd_fast,
                        macd_slow=macd_slow,
                        macd_signal_period=macd_signal_period,
                        sar_acceleration=sar_acceleration,
                        sar_maximum=sar_maximum,
                        adx_timeperiod=adx_timeperiod,
                        bb_timeperiod=bb_timeperiod,
                        bb_dev=bb_dev,
                        scalp=scalp,
                        use_last_closed_candle=use_last_closed_candle,
                        use_sar_stop_on_shortest_interval=use_sar_stop_on_shortest_interval,
                        snapshot=shared_snapshot,
                        use_websocket_candles=use_websocket_candles,
                    )

            scan_tasks = [asyncio.create_task(_scan_with_limit(scan_coin)) for scan_coin in eligible_scan_coins]
            launch_specs: List[Tuple[str, AutoTradeDecision, float]] = []
            try:
                for completed_task in asyncio.as_completed(scan_tasks):
                    try:
                        candidate = await completed_task
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        cp.warning(f"[AUTO-WARN] Market scan task failed: {exc}")
                        continue

                    if candidate.existing_position is not None:
                        mid = float(shared_snapshot.mids.get(candidate.coin.upper(), 0.0))
                        upnl = compute_position_unrealized_pnl(candidate.existing_position, mid) if mid > 0.0 else None
                        upnl_str = f"{upnl:.8f}" if upnl is not None else "N/A"
                        print(
                            f"[AUTO] Existing {candidate.coin} position detected; skipping new auto trade on that market. "
                            f"uPnL={upnl_str} {format_auto_scan_metrics(shared_snapshot)}"
                        )
                        continue

                    if candidate.decision is None or candidate.decision.direction is None or candidate.decision.take_profit_pct is None:
                        continue
                    if candidate.coin in reserved_coins:
                        print(
                            f"[AUTO] {candidate.coin} is already reserved by an active position or managed task; "
                            f"skipping duplicate auto entry."
                        )
                        continue

                    try:
                        resolved_size, size_reason = await resolve_auto_trade_size(
                            info=info,
                            account_address=account_address,
                            coin=candidate.coin,
                            side=candidate.decision.direction,
                            size=size,
                            size_pct=size_pct_fraction,
                        )
                    except Exception as exc:
                        print(f"[AUTO-WARN] {candidate.coin} size resolution failed; skipping trade candidate: {exc}")
                        continue
                    print(f"[AUTO] {candidate.coin} size resolved to {resolved_size:.8f} ({size_reason})")
                    launch_specs.append((candidate.coin, candidate.decision, resolved_size))
                    reserved_coins.add(candidate.coin)
                    if len(launch_specs) >= available_slots:
                        continue
            except Exception as exc:
                logger.error(exc)

            await asyncio.gather(*scan_tasks, return_exceptions=True)

            if not launch_specs:
                print('=' * 40 + f'Sleeping {scan_interval} seconds .. ' + '=' * 40)
                await asyncio.sleep(scan_interval)
                continue

            if dry_run:
                for launch_coin, _launch_decision, _launch_size in launch_specs:
                    print(f"[AUTO] Dry run enabled; {launch_coin} signal will not be traded.")
                await asyncio.sleep(scan_interval)
                continue

            for launch_coin, launch_decision, launch_size in launch_specs:
                direction = launch_decision.direction
                if direction is None or launch_decision.take_profit_pct is None:
                    continue
                auto_tp_pct = launch_decision.take_profit_pct
                auto_sl_pct = stop_loss_pct if stop_loss_pct is not None else launch_decision.stop_loss_pct
                auto_sl_trigger_px = (
                    launch_decision.stop_loss_trigger_px
                    if use_sar_stop_on_shortest_interval
                    else None
                )
                shortest_snapshot = get_shortest_interval_snapshot(launch_decision.snapshots)
                sl_display = f"{auto_sl_pct * 100:.4f}%" if auto_sl_pct is not None else "N/A"
                print(
                    f"[AUTO] Launching managed task for {direction.upper()} {launch_coin}: size={launch_size:.8f}, "
                    f"tp_pct={auto_tp_pct * 100:.4f}%, sl_pct={sl_display}, "
                    f"sl_trigger={f'{auto_sl_trigger_px:.8f}' if auto_sl_trigger_px is not None else 'N/A'}"
                )
                active_trade_tasks[launch_coin] = asyncio.create_task(
                    _run_managed_auto_trade(
                        managed_coin=launch_coin,
                        managed_direction=direction,
                        managed_size=launch_size,
                        managed_tp_pct=auto_tp_pct,
                        managed_sl_pct=auto_sl_pct,
                        managed_sl_trigger_px=auto_sl_trigger_px,
                        managed_shortest_snapshot=shortest_snapshot,
                    )
                )

            metrics_start_time_ms = int(time.time() * 1000)
            await asyncio.sleep(scan_interval)
    except KeyboardInterrupt:
        print("\n[!] Caught Ctrl+C, stopping auto trader.")
    finally:
        if owns_clients:
            await close_clients(info, exchange)
