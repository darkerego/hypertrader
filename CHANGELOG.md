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
