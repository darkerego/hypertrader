# Hypertrader

Async Hyperliquid trading helper built around the async SDK from `darkerego/hyperliquid-python-sdk-async`.

The canonical bot file in this repo is [`hypertrader.py`](/media/anon/development/anon/PycharmProjects/hypertrader/hypertrader.py). It provides five modes:

- `enter`: top-of-book limit entry with modify-order repricing, optional market fallback, and TP/SL management
- `watch`: attach TP/SL management to newly opened or existing positions
- `trailing`: manage local trailing stops for open positions
- `auto`: TA-Lib multi-timeframe signal scanner that routes through the same bracket-entry path as `enter`
- `market_maker`: event-driven maker ladder that stays separate from the bracket and hidden-order logic

## Behavior Summary

- Async SDK only: `Info` and `Exchange` are created and used asynchronously end-to-end.
- Websocket market data is enabled by default. Use `--no-websocket` to force HTTP polling only.
- HTTP remains the fallback and authoritative path for account state, reconciliation, candles, and trading actions.
- Entry logic uses `exchange.modify_order(...)` to move one working limit order instead of cancel/repost on every loop.
- Market fallback cleans up stale non-reduce-only entry orders before and after any market entry.
- TP reversal never crosses back through entry into loss territory.
- `--hide-orders` is available for `enter`, `watch`, `trailing`, and `auto`. It does not apply to `market_maker`.
- Runtime monitors print current uPnL, session realized PnL, and account balance.

## Requirements

- Python 3.10+
- Hyperliquid account credentials
- Network access to Hyperliquid APIs
- Account created with referral code [DARKEREGO](https://app.hyperliquid.xyz/join/DARKEREGO) (see account creation [*])

Base dependencies:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install python-dotenv eth-account hyperliquid-python-sdk-async
```

Optional `auto` mode dependencies:

```bash
pip install numpy TA-Lib
```

`auto` imports `numpy` and `talib` defensively, so the other modes can still run without TA-Lib installed.

Please see TA-Lib documentation if you need help installing the required binary library:

https://ta-lib.org/install/#linux


## Configuration

1) Create a hyperliquid account with referral code `DARKEREGO` with this URL: 

    [https://app.hyperliquid.xyz/join/DARKEREGO](https://app.hyperliquid.xyz/join/DARKEREGO)
   
2) Once in HyperLiquid panel, click more -> API and create an API key.

    - The API key is a private Ethereum key. Copy to clipboard and continue: 

3) Set credentials in the environment or a local `.env` file:

```text
HYPERLIQUID_SECRET_KEY=...
HYPERLIQUID_ACCOUNT_ADDRESS=...
```

- `HYPERLIQUID_SECRET_KEY`: private key for the trading wallet or API wallet (the API key you just created)
- `HYPERLIQUID_ACCOUNT_ADDRESS`: main account address used for the account state

Do not commit real credentials.

## Quick Start

Show global help:

```bash
python3 hypertrader.py --help
```

Enter a long with a 1% TP and default half-TP stop:

```bash
python3 hypertrader.py enter HYPE long --size 1 --take-profit-pct 0.01
```

Enter with hidden local TP/SL targets:

```bash
python3 hypertrader.py enter ETH short --size 0.25 --take-profit-pct 0.012 --hide-orders
```

Enter, let TP level 1 fill, then cancel the rest of the ladder and trail the remainder:

```bash
python3 hypertrader.py enter BTC long --size 0.1 --take-profit-pct 0.01 --trailing-tp
```

Watch an existing position and attach management:

```bash
python3 hypertrader.py watch BTC --take-profit-pct 0.01 --manage-existing
```

Watch a position and switch from the TP ladder to a trailing TP after TP level 1 fills:

```bash
python3 hypertrader.py watch BTC --take-profit-pct 0.01 --manage-existing --trailing-tp
```

Run local trailing management:

```bash
python3 hypertrader.py trailing BTC --trail-pct 0.01
```

Dry-run auto mode across short intervals:

```bash
python3 hypertrader.py auto SOL --intervals "1m 3m 5m" --size 10 --dry-run
```

Run the market maker on testnet:

```bash
python3 hypertrader.py market_maker HYPE --base-size 0.01 --testnet
```

## Commands

### `enter`

Opens a position with a top-of-book limit order, reprices the same working order with `modify_order`, optionally falls back to market for the remainder, then manages TP/SL.

```bash
python3 hypertrader.py enter COIN long|short --size SIZE [options]
```

Key options:

- `--take-profit-pct`: TP target fraction
- `--stop-loss-pct`: SL fraction; defaults to `TP * 0.5` if TP is supplied
- `--take-profit-levels`: number of weighted TP ladder levels
- `--trailing-tp`: keep the TP ladder live, then switch to a local trailing TP after the configured TP level fills
- `--trailing-tp-trigger-level`: TP ladder level that must fill before trailing TP activates
- `--trailing-tp-remaining-levels`: switch the remaining ladder to a local trailing TP when this many TP levels are left
- `--trailing-tp-profit-pct`: trailing distance as a fraction of favorable unrealized profit
- `--entry-retries`: limit modify attempts before fallback
- `--entry-repost-interval`: seconds between modify attempts
- `--entry-tif`: entry limit TIF, `Alo` or `Gtc`
- `--tp-tif`: TP limit TIF, `Alo` or `Gtc`
- `--no-market-fallback`: disable remaining-size market entry
- `--market-slippage`: slippage fraction for market-style helpers
- `--keep-existing-tpsl`: preserve existing reduce-only TP/SL orders before entry
- `--tp-reversal-pct`: enable TP-reversal exits after the first TP zone is reached
- `--tp-reversal-limit-exit`: try a reduce-only limit exit plus protective stop before market fallback
- `--tp-reversal-stop-buffer-pct`: optional TP-reversal stop buffer
- `--hide-orders` or `-ho`: keep bracket targets local until they trigger
- `--testnet`
- `--no-websocket`

### `watch`

Watches for matching positions and attaches TP/SL management. It can also manage positions that already exist at startup.

```bash
python3 hypertrader.py watch [COIN] [options]
```

Key options:

- `--take-profit-pct`
- `--stop-loss-pct`
- `--take-profit-levels`
- `--trailing-tp`
- `--trailing-tp-trigger-level`
- `--trailing-tp-remaining-levels`
- `--trailing-tp-profit-pct`
- `--poll-interval`
- `--manage-existing`
- `--tp-reversal-pct`
- `--tp-reversal-limit-exit`
- `--tp-reversal-stop-buffer-pct`
- `--tp-tif`
- `--market-slippage`
- `--keep-existing-tpsl`
- `--hide-orders` or `-ho`
- `--testnet`
- `--no-websocket`

### `trailing`

Runs local trailing-stop management for open positions.

```bash
python3 hypertrader.py trailing [COIN] [options]
```

Key options:

- `--trail-pct`
- `--poll-interval`
- `--hide-orders` or `-ho`
- `--testnet`
- `--no-websocket`

`trailing` already keeps stops local. The hidden-order flag is accepted for CLI consistency.

### `auto`

Scans one coin or the top perp markets using TA-Lib signals from:

- MACD
- Parabolic SAR
- ADX
- Bollinger Bands

Default intervals are `1h,15m,5m,1m`. With `--min-agreement 0`, all configured intervals must agree.

```bash
python3 hypertrader.py auto [COIN] (--size SIZE | --size-pct SIZE_PCT) [options]
```

Key options:

- `--size` or `--size-pct`
- `--top-markets`
- `--intervals`
- `--auto-periods`
- `--scan-interval`
- `--scan-batch-size`
- `--max-concurrency`
- `--min-agreement`
- `--adx-threshold`
- `--macd-fast`
- `--macd-slow`
- `--macd-signal`
- `--sar-acceleration`
- `--sar-maximum`
- `--adx-timeperiod`
- `--bb-timeperiod`
- `--bb-dev`
- `--use-live-candle`
- `--take-profit-pct`: override Bollinger-derived TP
- `--min-take-profit-pct`
- `--max-take-profit-pct`
- `--stop-loss-pct`
- `--stop-loss-sar`
- `--take-profit-levels`
- `--entry-retries`
- `--entry-repost-interval`
- `--poll-interval`
- `--tp-reversal-pct`
- `--tp-reversal-limit-exit`
- `--tp-reversal-stop-buffer-pct`
- `--no-market-fallback`
- `--market-slippage`
- `--keep-existing-tpsl`
- `--dry-run`
- `--max-trades`
- `--cooldown-after-trade`
- `--loop-after-trade`
- `--exit-after-trade`
- `--hide-orders` or `-ho`
- `--testnet`
- `--no-websocket`

`auto` derives TP from Bollinger Bands unless `--take-profit-pct` overrides it. If TP is set and SL is omitted, SL defaults to half TP. Executions route through the same bracket-entry path used by `enter`.

### `market_maker`

Runs an event-driven maker ladder around a computed center price.

```bash
python3 hypertrader.py market_maker COIN [options]
```

Key options:

- `--interval`
- `--periods`
- `--levels`
- `--base-size`
- `--loop-sleep`
- `--min-edge-pct`
- `--rebalance-threshold-pct`
- `--protect-close-pct`
- `--testnet`
- `--no-websocket`

`market_maker` is intentionally separate from hidden-order bracket behavior.

## Hidden Orders

`--hide-orders` keeps TP/SL targets in memory instead of placing exchange-side bracket orders immediately.

When enabled:

- no exchange-side stop-loss order is placed when management starts
- no exchange-side TP ladder is placed when management starts
- hidden targets are still calculated and displayed
- a hidden stop hit cancels any reduce-only TP orders and closes the position with `exchange.market_close(...)`
- a hidden TP level places its reduce-only post-only TP order only when the target is hit
- scale-ins rebase the hidden TP/SL targets to the new average entry and current size

This flag applies to `enter`, `watch`, `trailing`, and `auto`, but not `market_maker`.

## Websocket And HTTP

The bot creates `Info` with websocket support enabled unless `--no-websocket` is supplied:

```python
info = await Info.create(api_url, skip_ws=(not use_websocket))
```

The websocket cache is used as the fast path for market data such as mids, best bid/offer, and account event streams. HTTP calls remain in place for authoritative snapshots, reconciliation, candles, and trade actions.

## Logging

Runtime logs are written to [`logs/hypertrader.log`](/media/anon/development/anon/PycharmProjects/hypertrader/logs/hypertrader.log).
Completed auto-trades are appended to [`logs/auto_trades.log`](/media/anon/development/anon/PycharmProjects/hypertrader/logs/auto_trades.log).

## Validation

Minimum validation after changes:

```bash
python3 -m py_compile hypertrader.py
python3 hypertrader.py --help
python3 hypertrader.py enter --help
python3 hypertrader.py watch --help
python3 hypertrader.py trailing --help
python3 hypertrader.py auto --help
python3 hypertrader.py market_maker --help
```

Suggested non-live auto check:

```bash
python3 hypertrader.py auto SOL --intervals "1m 3m 5m" --size 10 --dry-run
```

Do not run live trading commands unless you intend to trade.
