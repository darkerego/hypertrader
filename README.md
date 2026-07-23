# Hypertrader

Async Hyperliquid trading helper built around my async SDK: [hyperliquid-python-sdk-async](https://github.com/darkerego/hyperliquid-python-sdk-async).

## Warning

This is alpha software currently in development. Trade carefully, and if you encounter 
any problems, please open an issue so that I can fic them.

## About

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

Install from pip:

<pre>
pip install hypertrader[auto] --upgrade
</pre>

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


##### [*] Account creation:

1) Create a hyperliquid account with referral code `DARKEREGO` with this URL: 

    [https://app.hyperliquid.xyz/join/DARKEREGO](https://app.hyperliquid.xyz/join/DARKEREGO)
   
2) Once in HyperLiquid panel, click more -> API and create an API key.

    - The API key is a private Ethereum key. Copy to clipboard and continue: 

3) Set credentials in the environment or a local `.env` file:

```text
HYPERLIQUID_SECRET_KEY=...
HYPERLIQUID_ACCOUNT_ADDRESS=...
HYPERLIQUID_TESTNET_SECRET_KEY # optional, for testnet
```

- `HYPERLIQUID_SECRET_KEY`: private key for the trading wallet or API wallet (the API key you just created)
- `HYPERLIQUID_ACCOUNT_ADDRESS`: main account address used for the account state
- `HYPERLIQUID_TESTNET_SECRET_KEY`: optional testnet API key

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

Watch all perpetual markets for new and currently opened positions and attach management.

```bash
python3 hypertrader.py watch BTC --take-profit-pct 0.01 --manage-existing
```

Watch an existing position on a specific market and attach management:

```bash
python3 hypertrader.py watch BTC --take-profit-pct 0.01 --manage-existing
```

Watch a position and switch from the TP ladder to a trailing TP after TP level 1 fills:

```bash
python3 hypertrader.py watch BTC --take-profit-pct 0.01 --manage-existing --trailing-tp
```

Run local trailing management for all perpetual markets:

```bash
python3 hypertrader.py trailing --trail-pct 0.01
```


Run local trailing management for a specific market:

```bash
python3 hypertrader.py trailing BTC --trail-pct 0.01
```

Dry-run auto mode across the top 15 markets by volume concurrently, open positions with 10% of accounts' collateral:

```bash
python3 hypertrader.py auto --intervals "1m 3m 5m" --top-markets 15 --size-pct 10 --dry-run
```

Dry-run auto mode across short intervals on a specific market only with a static position size:

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

`auto` now supports selectable strategies:

- `default`: the existing MACD/SAR/ADX/Bollinger auto trader and the implicit default when `--strategy` is omitted
- `orderflow_pullback`: order-flow-confirmed trend pullback scalp strategy, also available as `of_pullback` and `orderflow`
- `reversal`: a closed-candle trend-exhaustion, structure-break, and retest strategy

The strategy selection changes the signal engine only. Shared sizing, risk controls, modify-order entry, market-fallback cleanup, hidden-order behavior, and bracket management still run through the same common auto execution path.

The default strategy scans one coin or the top perp markets using TA-Lib signals from:

- MACD
- Parabolic SAR
- ADX
- Bollinger Bands

Default intervals are `1h,15m,5m,1m`. With `--min-agreement 0`, all configured intervals must agree.

```bash
python3 hypertrader.py auto [COIN] (--size SIZE | --size-pct SIZE_PCT) [options]
```

Key options:

- `--strategy {default,reversal}`
- `--strategy {default,orderflow_pullback,reversal}` with aliases `of_pullback` and `orderflow`
- `--size` or `--size-pct`
- `--top-markets`
- `--intervals`
- `--auto-periods`
- `--scan-interval`
- `--max-concurrent-scans`
- `--min-agreement`
- `--adx-threshold`
- `--macd-fast`
- `--macd-slow`
- `--macd-signal`
- `--sar-acceleration`
- `--sar-maximum`
- `--auto-sar-stop-on-shortest-interval`
- `--adx-timeperiod`
- `--bb-timeperiod`
- `--bb-dev`
- `--scalp`
- `--use-live-candle`
- `--take-profit-pct`: override Bollinger-derived TP
- `--min-take-profit-pct`
- `--max-take-profit-pct`
- `--stop-loss-pct`
- `--take-profit-levels`
- `--trailing-tp`
- `--trailing-tp-trigger-level`
- `--trailing-tp-profit-pct`
- `--entry-retries`
- `--entry-repost-interval`
- `--entry-tif`
- `--tp-tif`
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
- `--max-coin-trades-per-session`
- `--coin-session-cooldown-seconds`
- `--coin-session-profit-target`
- `--coin-session-min-profit-to-lock`
- `--coin-session-giveback-pct`
- `--cooldown-after-loss-following-wins`
- `--session-profit-target`
- `--session-max-loss`
- `--session-giveback-pct`
- `--risk-session-log`
- `--disable-ws-candles`
- `--hide-orders` or `-ho`
- `--testnet`
- `--no-websocket`

`auto` derives TP from Bollinger Bands unless `--take-profit-pct` overrides it. If TP is set and SL is omitted, SL defaults to half TP. Executions route through the same bracket-entry path used by `enter`.

Reversal strategy notes:

- All confirmations use closed candles only.
- SAR alone never opens a trade.
- A trade requires: prior trend, exhaustion, confirmed Williams-fractal structure break, then a confirmed retest.
- Default reversal intervals are `--trend-interval 1h` and `--entry-interval 15m`.
- Reversal entries build explicit entry, stop, and multi-target plans before handing execution to the shared auto engine.
- Hidden-order mode still applies: `--hide-orders` keeps reversal TP/SL targets local until trigger conditions are met.

Important reversal-specific options:

- `--trend-interval`: higher timeframe used for prior-trend classification
- `--entry-interval`: lower timeframe used for structure-break and retest confirmation
- `--reversal-min-adx`: minimum higher-timeframe ADX required before a reversal setup is considered
- `--reversal-exhaustion-score`: minimum exhaustion confirmations required before structure-break tracking starts
- `--reversal-retest-timeout`: number of entry candles allowed for the retest to confirm before the setup expires
- `--reversal-stop-atr-buffer`: ATR buffer added beyond the invalidation point when computing the stop
- `--reversal-max-stop-atr`: caps the stop width in ATR terms
- `--reversal-min-rr`: minimum reward-to-risk ratio required for a valid execution plan
- `--reversal-tp1-r`, `--reversal-tp2-r`, `--reversal-tp3-r`: target distances in multiples of initial risk
- `--reversal-tp1-pct`, `--reversal-tp2-pct`, `--reversal-runner-pct`: allocation percentages for the three reversal exits; they must sum to `100`
- `--reversal-exit-on-sar-flip` / `--no-reversal-exit-on-sar-flip`: enable or disable SAR-flip managed exits after entry
- `--reversal-min-ema50-slope`: minimum absolute higher-timeframe EMA slope required to qualify the prior trend

Example single-coin reversal scan:

```bash
python3 hypertrader.py auto --strategy reversal \
  BTC \
  --size-pct 5 \
  --trend-interval 1h \
  --entry-interval 15m \
  --reversal-min-adx 18 \
  --reversal-exhaustion-score 2 \
  --reversal-min-rr 1.8 \
  --reversal-exit-on-sar-flip
```

Order-flow pullback strategy notes:

- Uses closed `1m`/`5m` candles for regime context and live `l2Book` plus `trades` websocket feeds for entry confirmation.
- Indicators classify context only; executed trade flow, weighted book imbalance, and microprice authorize entries.
- Entries still route through the shared modify-order post-only entry path.
- Hidden-order mode still applies: `--hide-orders` keeps TP/SL targets local instead of placing exchange-visible brackets.

Important order-flow options:

- `--of-max-active-books`: maximum promoted markets with live book/trade monitoring
- `--of-max-spread-bps` and `--of-max-spread-ratio`: live spread gates
- `--of-min-depth-ratio`: executable depth gate versus intended order notional
- `--of-warmup-seconds`: warm-up period after promotion before entries are allowed
- `--of-min-edge-cost-multiple`: expected-move versus estimated round-trip-cost gate
- `--of-pullback-min` and `--of-pullback-max`: retracement zone bounds
- `--of-book-imbalance-min`, `--of-micro-bias-min`, `--of-trade-imbalance-2s-min`, and `--of-trade-imbalance-10s-min`: mandatory confirmation thresholds
- `--of-entry-timeout-seconds` and `--of-max-chase-ticks`: entry aggressiveness controls layered onto the shared entry helper

Example order-flow scan:

```bash
python3 hypertrader.py auto \
  --strategy orderflow_pullback \
  --top-markets 30 \
  --size-pct 5 \
  --scan-interval 1 \
  --of-max-active-books 8 \
  --of-max-spread-bps 3 \
  --of-min-depth-ratio 10 \
  --of-entry-timeout-seconds 2 \
  --of-max-chase-ticks 2
```

Example top-market reversal scan:

```bash
python3 hypertrader.py auto --strategy reversal \
  --top-markets 30 \
  --size-pct 5 \
  --trend-interval 1h \
  --entry-interval 15m \
  --scan-interval 30 \
  --loop-after-trade \
  --max-coin-trades-per-session 3
```

Additional `auto` parameter notes:

- `--max-concurrent-scans`: limits how many markets are scanned in parallel in each auto loop.
- `--auto-sar-stop-on-shortest-interval`: uses the Parabolic SAR from the shortest configured interval as the live stop trigger for auto-managed positions instead of the default pct-based stop behavior.
- `--scalp`: requires additional shortest-interval Bollinger confirmation before entering.
- `--max-coin-trades-per-session`: caps completed trade cycles per coin before that coin is cooled down.
- `--coin-session-cooldown-seconds`: duration of the per-coin cooldown window after a coin-level stop condition triggers.
- `--coin-session-profit-target`: pauses a coin after it reaches the configured realized PnL target for the current auto session.
- `--coin-session-min-profit-to-lock`: enables per-coin giveback protection only after realized PnL first reaches this minimum lock threshold.
- `--coin-session-giveback-pct`: pauses a coin after it gives back the configured fraction from its peak realized PnL.
- `--cooldown-after-loss-following-wins`: pauses a coin after a losing trade that follows at least N consecutive winning cycles.
- `--session-profit-target`: stops the entire auto session once total realized PnL reaches the target.
- `--session-max-loss`: stops the entire auto session once total realized PnL falls to `-N` or below.
- `--session-giveback-pct`: stops the entire auto session after giving back the configured fraction from peak realized PnL.
- `--risk-session-log`: writes auto risk-session events to a JSONL file path you provide.
- `--ws-candles`: experimental candle mode that seeds each `(coin, interval)` via `candleSnapshot`, then keeps them updated from websocket candle messages.

Example with newer auto risk controls:

```bash
python3 hypertrader.py auto SOL --intervals "1m 5m 15m" --size 10 --dry-run --max-concurrent-scans 2 --coin-session-profit-target 25 --coin-session-giveback-pct 0.3 --session-max-loss 50
```

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

`--hide-orders` keeps TP/SL targets in memory instead of placing exchange-side bracket orders immediately. This prevents 
exchange market maker bots from being able to see where your stops and take profit orders are.

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
