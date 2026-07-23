# 2026-07-10 22:59:15 EDT

- Updated `README.md` so the new auto strategy functionality is documented more completely: the shared execution-path behavior for all strategies is now explicit, and the `reversal` strategy section now covers the main reversal-only CLI parameters and trade-planning behavior.

# 2026-07-10 20:27:54 EDT

- Updated `modes/auto_trader.py` so the `reversal` auto strategy now excludes unknown/non-functional markets from startup candle backfill tracking the same way the default strategy already does.
- Prevented reversal auto runs from staying stuck behind `[AUTO] Startup candle backfill still in progress` when a top-volume market has no usable candle feed and would otherwise leave its startup coin/interval pairs permanently incomplete.

# 2026-07-10 16:16:18 EDT

- Updated `modes/auto_trader.py` auto-trade completion logging so each completed auto trade now writes a structured payload containing the launch-time trade quality label, expected entry price, current market snapshot, and strategy indicator/context values alongside the existing coin/side/size/TP/SL metadata.
- Preserved the existing completion-time logging point by capturing the signal snapshot when the managed task is launched and carrying that context through the background trade lifecycle until the trade closes.

# 2026-07-10 16:03:13 EDT

- Added a new top-level `strategies/` package with a typed strategy contract, deterministic registry, extracted `default` auto strategy, and a new `reversal` auto strategy module.
- Updated `hypertrader.py` and `modes/auto_trader.py` so `auto` now accepts `--strategy {default,reversal}`, logs the selected strategy at startup, instantiates the strategy once per auto session, and keeps shared scan/sizing/risk/execution infrastructure outside the strategy implementations.
- Added reversal-strategy CLI parameters, reversal helper tests, registry/parser tests, and README documentation for the new strategy selection and reversal workflow.

# 2026-07-09 22:56:00 EDT

- Extended `utils/helpers.py` websocket cache coverage with a first-pass hybrid account cache: `user_state` / `clearinghouseState`, `openOrders`, `frontendOpenOrders`, and `userFills` are now cached in memory, with `userEvents`, `userFills`, and `orderUpdates` marking REST-backed snapshots dirty and websocket fills incrementally merged into the session fill cache.
- Added explicit cache-aware helpers for open orders, user state, and fills, plus `force_http` escape hatches so high-frequency monitors can reuse cached snapshots while authoritative REST validation remains available at call sites that are about to trade.
- Updated `modes/position_management.py` to keep live REST reconciliation immediately before order-management actions such as entry cleanup, market fallback cleanup, and post-exit position checks, while `modes/auto_trader.py` now uses the cached session fills helper for scan-loop realized-PnL metrics.

# 2026-07-09 21:45:22 EDT

- Updated `hypertrader.py` to lazy-import `run_auto_trader(...)` only inside the `auto` command branch, so `enter`, `watch`, `trailing`, and `market_maker` no longer pull TA-Lib dependencies during module import.
- Removed unconditional `numpy` and `talib` imports from `utils/helpers.py`, and switched `modes/position_watcher.py` to import `compute_default_stop_loss_pct(...)` from `utils.helpers` instead of `modes.auto_trader`, closing the remaining non-`auto` dependency leak.
- Wrapped `modes/auto_trader.py` TA-Lib imports in the intended optional guard, preserving the existing explicit `require_talib_available()` failure only when the `auto` path actually executes.
- Fixed `modes/position_management.py` modify-order entry handling so successful `exchange.modify_order(...)` responses now re-run `extract_resting_oids(resp)` and `extract_filled_qty(resp)`, adopt replacement resting order ids, and clear `active_oid` when a modify fills instead of leaving a stale entry oid tracked.

# 2026-07-09 03:07:36 EDT

- Updated `modes/position_management.py` so bracket monitoring now tracks the managed take-profit ladder by the specific TP order ids this bot placed, instead of inferring fills only from a reduced live position size or from the absence of any reduce-only TP order on the coin.
- Added managed-TP oid disappearance detection gated by that TP level having actually been reached, which lets the bot confirm full managed-ladder fills and handle the remaining managed position without confusing unrelated open orders or other positions.
- Tightened TP exhaustion and trailing-TP conversion to act only on tracked managed TP order ids for the current coin, preventing unrelated reduce-only orders from being treated as part of the active bracket.

# 2026-07-09 02:36:11 EDT

- Reverted the watcher-specific `close_remainder_after_tp_exhausted=True` opt-in in `modes/position_watcher.py`, so `watch` mode no longer force-flattens a live watched position just because its current TP ladder appears exhausted after a fill.
- Restored the prior per-position watch behavior where the shared bracket monitor rebuilds TP/SL management for a still-open watched remainder instead of market-closing it, preventing one watched trade lifecycle from being treated as a full forced exit.

# 2026-07-09 01:54:47 EDT

- Updated `modes/position_management.py` watch/enter bracket monitoring so if a confirmed TP fill reduces the position and the exchange-side TP ladder disappears, the bot now rebuilds TP/SL management for the live remainder instead of canceling all reduce-only orders and market-closing the position.
- Preserved the existing TP-remainder cleanup only for effectively flat dust leftovers, preventing a first filled TP limit from being misclassified as a full exit.

# 2026-07-09 00:57:32 EDT

- Updated `modes/position_management.py` TP remainder handling so watch/enter monitors now require a confirmed managed-position reduction before treating an empty TP ladder as an exhausted take-profit sequence that should market-close the remainder.
- Prevented spontaneous or manual TP-order disappearance from flattening a still-open position; if reduce-only TP orders are missing but no TP fill was observed, the bot now logs `[TP-REMAINDER-WARN]` and leaves the position open.

# 2026-07-08 20:20:57 EDT

- Updated `modes/auto_trader.py` startup auto-entry gating so once startup backfill has encountered REST candle rate limits, the bot now unlocks normal auto entries after the first full scan iteration that finishes without any new rate-limit errors, even if some original startup coin/interval pairs are still pending.
- Preserved the existing startup pacing and pair-tracking behavior for true ongoing rate-limit pressure, while preventing auto mode from staying stuck on the `Startup candle backfill still in progress` block after a clean recovery iteration.

# 2026-07-08 20:12:13 EDT

- Updated `modes/auto_trader.py` startup candle backfill handling so unknown-market candle failures such as `KeyError('KBONK')` now exclude that coin from future auto scans instead of leaving startup backfill permanently blocked.
- Added explicit unknown-market detection for auto candle signal evaluation and treated excluded coin/interval pairs as completed for startup gating only, while preserving retry behavior for other non-rate-limit failures.

# 2026-07-08 20:02:54 EDT

- Updated `modes/auto_trader.py` startup candle warmup so auto mode now blocks all new entries until every coin/interval pair in the initial eligible scan set has successfully backfilled at least once.
- Fixed startup backfill completion tracking so non-rate-limit candle fetch failures no longer incorrectly unlock trading before the missing history has actually loaded.

# 2026-07-08 02:05:00 EDT

- Updated `modes/auto_trader.py` startup scanning so large top-market auto runs now detect REST candle `429` rate limits, enable a temporary `3.0s` pause between scan batches, and automatically return to the normal scan cadence once the initial candle backfill completes.
- Scoped the new pacing to early REST backfill only, leaving single-coin auto runs and websocket-candle scans unchanged after startup warms.

# 2026-07-08 01:48:23 EDT

- Updated `modes/auto_trader.py` so `--cooldown-after-trade` is now tracked per coin instead of as a single global auto-scan pause, which lets other markets continue scanning and opening trades while only the just-closed market is cooling down.
- Added per-coin post-trade cooldown filtering in the auto scan loop, preserving existing per-coin risk-session cooldown behavior while preventing concurrent positions on unrelated markets from being blocked by another market's close.

# 2026-07-08 01:41:07 EDT

- Updated `hypertrader.py` auto-mode CLI with `--max-positions`, defaulting to `3`, and validated that the new limit must be greater than zero before `run_auto_trader(...)` starts.
- Reworked `modes/auto_trader.py` so auto entries launch `run_bracket_entry(...)` in background `asyncio.Task` instances, allowing the scanner to keep evaluating other markets while existing auto-managed positions are being entered and monitored.
- Added active managed-trade tracking and slot reservation in auto mode so new entries are skipped once the combined set of live positions plus active auto tasks reaches `--max-positions`, while completed background trades still flow through the existing realized-PnL and risk-session accounting.
- Validation: `python3 -m py_compile hypertrader.py` and `python3 -m py_compile modes/auto_trader.py` passed. CLI help checks are still blocked in this environment because `uvloop` is not installed (`ModuleNotFoundError: No module named 'uvloop'`).

# 2026-07-08 01:09:41 EDT

- Hardened `modes/position_management.py` residual TP close handling so when the auto-managed take-profit ladder is exhausted, the bot now cancels leftover reduce-only orders, rechecks the live position, and retries `exchange.market_close(...)` up to three times until the same-side remainder is flat.
- Replaced the old single-shot TP remainder `market_close` path with explicit post-close verification and retry logging, which prevents partially closed auto trades from being left open after the limit TP ladder finishes.

# 2026-07-07 21:00:00 EDT

- Removed the remaining-level trailing-TP path from `hypertrader.py`, `modes/position_management.py`, `modes/position_watcher.py`, and `modes/auto_trader.py`; trailing take-profit now keeps only the trigger-level activation flow.
# 2026-07-09 02:07:18 EDT

- Updated `modes/position_watcher.py` so watcher-managed brackets now tell the shared monitor to flatten any confirmed residual same-side position after the full take-profit ladder has been exhausted, instead of rebuilding TP/SL on the leftover size.
- Updated `modes/position_management.py` to keep the existing anti-premature-close guard in place by only triggering that watcher-specific remainder close after the monitored position size has actually decreased from the tracked baseline, while leaving `enter`/`auto` remainder rebuild behavior unchanged.

# 2026-07-10 00:01:54 EDT

- Fixed auto-mode max-position enforcement in `modes/auto_trader.py` so once a scan batch fills the currently available slot count, the launcher stops queueing additional startup candidates instead of continuing past `--max-positions`.

- Deleted the old ladder-to-trailing conversion branch that armed trailing TP when a configured number of TP levels remained open.
- Hardened auto-mode stop selection so when `--auto-sar-stop-on-shortest-interval` is not enabled, auto entries explicitly use the normal pct-based stop-loss path instead of any SAR-derived absolute stop trigger.

# 2026-07-07 19:58:46 EDT

- Updated `hypertrader.py` auto-mode CLI defaults so `--trailing-tp-remaining-levels` no longer defaults to `1`; auto mode now keeps trailing take-profit fully disabled unless `--trailing-tp` is explicitly supplied.
- Hardened `modes/auto_trader.py` so auto mode forces `trailing_tp_remaining_levels=0` whenever `use_trailing_tp` is false, preventing the last-TP-level ladder-to-trailing switch from activating implicitly.

# 2026-07-07 17:25:50 EDT

- Reworked `utils/style.py` into a shared stdout formatter that classifies banners, section headers, indented detail rows, and common log prefixes such as `[AUTO]`, `[WATCH]`, `[ENTRY]`, `[MM]`, `[WARN]`, and `[ERROR]` into distinct color treatments.
- Updated `hypertrader.py` to install the styled stdout hook at startup so existing `print(...)` calls across the canonical bot and its modes now render through the shared colored output path without changing trading behavior.
- Validation: `python3 -m py_compile utils/style.py hypertrader.py` passed.

# 2026-07-07 02:15:33 EDT

- Updated `auto_trader.py` to use the websocket for kline data.
- Refactored bot so that the clients only need to be initialized at run time to speed up execution.

# 2026-07-07 17:07:29 EDT

- Updated `README.md` to document the newer `auto` mode CLI parameters, including concurrent scan limits, SAR-stop/scalp options, per-coin risk controls, session-wide risk limits, optional JSONL risk-event logging, and experimental websocket candle support.
- Corrected stale `auto` option names in the README so the docs now match the current `hypertrader.py` CLI surface.

# 2026-07-07 17:08:00 EDT

- Added `setup.py` so the repository can be built and published as a PyPI package, with `hypertrader` exposed as a `console_scripts` entry point to `hypertrader:main`.
- Declared base runtime dependencies in packaging metadata (`eth-account`, `hyperliquid-python-sdk-async`, `python-dotenv`, `uvloop`) and added an optional `auto` extra for `numpy` and `TA-Lib`.

# 2026-07-07 16:36:24 EDT

- Added auto-trader session risk controls in `hypertrader.py` and `modes/auto_trader.py`, including per-coin cycle caps, coin cooldowns, per-coin profit/giveback locks, loss-after-win-streak cooldowns, session profit/max-loss/giveback kill switches, and optional JSONL risk event logging.
- Integrated pre-scan coin blocking and cooldown reset behavior before auto signal evaluation, while preserving existing MACD/SAR/ADX/Bollinger decision flow and the existing `run_bracket_entry(...)` execution path.
- Added post-trade fill-window reconciliation using net realized PnL (`closedPnl - fee`) for the selected coin after each completed bracket lifecycle, with compact `[AUTO-RISK]` logging and session-stop enforcement.
- Validation: `python3 -m py_compile hypertrader.py` and `python3 -m py_compile modes/auto_trader.py` passed. CLI help commands are currently blocked in this environment because `uvloop` is not installed (`ModuleNotFoundError: No module named 'uvloop'`).

# 2026-07-07 17:01:00 EDT

- Updated auto-mode SAR stop handling in `modes/position_management.py` so a live Parabolic SAR flip now exits the remaining position immediately: long positions close when the latest SAR is above or equal to the latest close, and short positions close when the latest SAR is below or equal to the latest close.
- Preserved the existing dynamic SAR stop-price refresh behavior when the SAR dot remains on the protective side, so auto-managed positions still ratchet their stop trigger without changing non-auto bracket behavior.

# 2026-07-10 16:38:25 EDT

- Added `async_hyperliquid.py` with a standalone `AsyncHyperliquid` client that extracts reusable Hyperliquid exchange, account-state, websocket cache, market-data, candles, order placement, position sizing, fill waiting, position closing, account monitoring, and take-profit ladder logic out of the bot into a strategy-independent async library.
- Added `tests/test_async_hyperliquid.py` with fake async SDK coverage for initialization, websocket callback bridging, cache/REST fallback behavior, rounding, order normalization, chase-entry behavior, market-close guards, candle streaming, fill waiters, TP reconciliation, retries, read-only safety, and shutdown behavior.
- Validation in this environment: `python3 -m py_compile hypertrader.py` passed, `python3 -m compileall async_hyperliquid.py tests/test_async_hyperliquid.py` passed, `.venv/bin/python hypertrader.py --help` passed, `.venv/bin/python hypertrader.py enter --help` passed, and a manual fake-SDK smoke run of `AsyncHyperliquid.initialize()`, `get_account_balance()`, `get_mid()`, and `place_limit_order()` passed. `pytest`, `pytest-asyncio`, `ruff`, and `mypy` are not installed here, so those requested validation commands could not be executed.

# 2026-07-23 13:54:45 EDT

- Fixed trailing stop calculation
- enhanced default auto strategy
- various bugfixes and optimizations
- Note: `reversal` strategy may need more debugging