"""Per-run analysis artifacts. No exchange writes or plotting dependencies."""
import asyncio
import csv
import json
import logging
import math
import re
import sys
import uuid
from datetime import datetime, timezone
from html import escape
from pathlib import Path

from utils.constants import INTERVAL_TO_MS

logger = logging.getLogger(__name__)


def utc_time(milliseconds):
    return datetime.fromtimestamp(milliseconds / 1000, timezone.utc).isoformat()


def number(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (ValueError, TypeError):
        return None


def fill_average(fills, direction):
    matching = [f for f in fills if f.get("dir") == direction
                and number(f.get("px")) is not None and (number(f.get("sz")) or 0) > 0]
    size = sum(float(f["sz"]) for f in matching)
    return (sum(float(f["px"]) * float(f["sz"]) for f in matching) / size, size) if size else (None, None)


class TradeJournal:
    fields = (
        "session_id", "trade_id", "coin", "direction", "strategy", "simulated",
        "started_at_utc", "closed_at_utc", "duration_seconds", "requested_size",
        "filled_entry_size", "closed_size", "entry_price", "exit_price", "planned_entry_price",
        "realized_pnl", "fees", "pnl_basis", "pnl_status", "exit_reason",
        "take_profit_pct", "stop_loss_pct", "details_file", "chart_file",
    )

    def __init__(self, root, settings, *, argv=None, charts=False, resume_directory=None):
        if resume_directory is not None:
            self.directory = Path(resume_directory).resolve()
            metadata = json.loads((self.directory / "session.json").read_text())
            self.session_id = metadata["session_id"]
            self.charts = charts
            self.strategy = settings.get("strategy_name", "")
            self.csv_path = self.directory / "closed_trades.csv"
            with self.csv_path.open(newline="", encoding="utf-8") as stream:
                rows = list(csv.DictReader(stream))
            self.sequence = max((int(row["trade_id"]) for row in rows), default=0)
            self.recorded_cycles = {(row["coin"], row["started_at_utc"]) for row in rows}
            return
        self.session_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ") + "-" + uuid.uuid4().hex[:8]
        self.directory = Path(root).expanduser().resolve() / self.session_id
        self.directory.mkdir(parents=True, exist_ok=False)
        self.charts = charts
        self.strategy = settings.get("strategy_name", "")
        self.sequence = 0
        self.recorded_cycles = set()
        metadata = {
            "schema_version": 1, "session_id": self.session_id,
            "started_at_utc": datetime.now(timezone.utc).isoformat(),
            "argv": list(argv) if argv is not None else None,
            "settings": settings, "python_version": sys.version,
        }
        self._json("session.json", metadata)
        self.csv_path = self.directory / "closed_trades.csv"
        with self.csv_path.open("x", newline="", encoding="utf-8") as stream:
            csv.DictWriter(stream, fieldnames=self.fields).writeheader()

    def _json(self, filename, payload):
        (self.directory / filename).write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8",
        )

    async def record(self, result, pnl, fills, pnl_error, info, *, deduplicate=False):
        """Append only completed results; flush every trade by closing the file."""
        cycle_key = (result.coin, utc_time(result.started_ms))
        if deduplicate and cycle_key in self.recorded_cycles:
            return
        self.sequence += 1
        trade_id = f"{self.sequence:06d}"
        symbol = re.sub(r"[^A-Za-z0-9_-]", "_", result.coin)
        stem = f"{trade_id}_{symbol}"
        context = result.log_context
        simulated = result.simulated_pnl is not None
        side = result.direction.title()
        entry, entry_size = fill_average(fills, f"Open {side}")
        exit_price, closed_size = fill_average(fills, f"Close {side}")
        if simulated:
            entry, entry_size = context.get("entry_price"), result.size
            exit_price, closed_size = context.get("average_exit_price", context.get("exit_price")), result.size
        elif closed_size is None:
            pnl_error = pnl_error or "pnl_unavailable: no matching closing fills"
        row = dict(
            session_id=self.session_id, trade_id=trade_id, coin=result.coin,
            direction=result.direction, strategy=context.get("strategy", self.strategy), simulated=simulated,
            started_at_utc=utc_time(result.started_ms), closed_at_utc=utc_time(result.closed_ms),
            duration_seconds=(result.closed_ms - result.started_ms) / 1000,
            requested_size=result.size, filled_entry_size=entry_size, closed_size=closed_size,
            entry_price=entry, exit_price=exit_price,
            planned_entry_price=context.get("signal_entry_price", context.get("entry_price")) if not simulated else context.get("planned_entry_price"),
            realized_pnl=None if pnl_error else pnl,
            fees=None if simulated or pnl_error else sum(number(f.get("fee")) or 0 for f in fills),
            pnl_basis="gross_excluding_fees_funding_slippage" if simulated else "closed_pnl_less_fill_fees_excluding_funding",
            pnl_status=pnl_error or "available", exit_reason=context.get("exit_reason", "position_closed"),
            take_profit_pct=result.take_profit_pct, stop_loss_pct=result.stop_loss_pct,
            details_file=stem + ".json", chart_file=stem + ".svg" if self.charts else "",
        )
        # Commit the row before any optional network I/O, including on cancellation.
        with self.csv_path.open("a", newline="", encoding="utf-8") as stream:
            csv.DictWriter(stream, fieldnames=self.fields).writerow(row)
        self.recorded_cycles.add(cycle_key)
        details = {"trade": row, "fills": fills, "signal_snapshot": context,
                   "chart_status": "pending" if self.charts else "disabled"}
        self._json(row["details_file"], details)
        if self.charts:
            try:
                duration = max(0, result.closed_ms - result.started_ms)
                interval, step = next(
                    ((name, ms) for name, ms in sorted(INTERVAL_TO_MS.items(), key=lambda item: item[1])
                     if duration / ms <= 450),
                    max(INTERVAL_TO_MS.items(), key=lambda item: item[1]),
                )
                candles = await asyncio.wait_for(info.candles_snapshot(
                    result.coin, interval, result.started_ms - 5 * step, result.closed_ms,
                ), timeout=10)
                self._chart(row["chart_file"], row, result, candles, interval)
                details["chart_status"] = "created"
            except asyncio.CancelledError:
                details["chart_status"] = "interrupted"
                raise
            except Exception as exc:
                details["chart_status"] = "failed"
                details["chart_error"] = str(exc)
                logger.warning("Trade chart failed for %s: %s", result.coin, exc)
            finally:
                self._json(row["details_file"], details)
        return row

    def _chart(self, filename, row, result, candles, interval):
        points = sorted((int(c["t"]), float(c["c"])) for c in candles
                        if number(c.get("t")) is not None and number(c.get("c")) is not None)
        if not points:
            raise ValueError("No candle data available")
        prices = [p for _, p in points] + [p for p in (row["entry_price"], row["exit_price"]) if p is not None]
        low, high = min(prices), max(prices)
        padding = max((high - low) * 0.1, abs(high) * 0.001, 1e-8)
        low, high = low - padding, high + padding
        start, end = min(points[0][0], result.started_ms), max(points[-1][0], result.closed_ms)
        x = lambda timestamp: 100 + 850 * (timestamp - start) / max(1, end - start)
        y = lambda price: 440 - 340 * (price - low) / (high - low)
        title = escape(f'{result.coin} {result.direction} | {row["strategy"]} | PnL: {row["realized_pnl"] if row["realized_pnl"] is not None else "unavailable"}')
        parts = ['<svg xmlns="http://www.w3.org/2000/svg" width="1000" height="540" viewBox="0 0 1000 540">',
                 '<rect width="1000" height="540" fill="#111827"/>',
                 '<g font-family="sans-serif" fill="#e5e7eb" font-size="13">',
                 f'<text x="40" y="35" font-size="20">{title}</text>',
                 f'<text x="40" y="62">{escape(interval)} candle prices | entry / exit: size-weighted fills | {"SIMULATED" if row["simulated"] else "LIVE"}</text>']
        for i in range(5):
            price = low + (high - low) * i / 4
            py = y(price)
            parts += [f'<line x1="100" y1="{py}" x2="950" y2="{py}" stroke="#374151"/>',
                      f'<text x="8" y="{py + 4}">{price:.6g}</text>']
        coords = " ".join(f"{x(t):.2f},{y(p):.2f}" for t, p in points)
        parts.append(f'<polyline points="{coords}" fill="none" stroke="#60a5fa" stroke-width="2"/>')
        for label, timestamp, price, color in (
            ("Entry", result.started_ms, row["entry_price"], "#34d399"),
            ("Exit", result.closed_ms, row["exit_price"], "#fbbf24"),
        ):
            px = x(timestamp)
            parts.append(f'<line x1="{px}" y1="100" x2="{px}" y2="440" stroke="{color}" stroke-dasharray="4 4"/>')
            if price is not None:
                parts.append(f'<circle cx="{px}" cy="{y(price)}" r="5" fill="{color}"/>')
            parts.append(f'<text x="{min(px, 880)}" y="{85 if label == "Entry" else 465}" fill="{color}">{label}</text>')
        parts += [f'<text x="100" y="495">{escape(utc_time(start))}</text>',
                  f'<text x="950" y="495" text-anchor="end">{escape(utc_time(end))}</text>',
                  '<text x="100" y="520">UTC | Markers use cycle start and closure detection times; latest candle may be incomplete.</text>',
                  '</g></svg>']
        (self.directory / filename).write_text("\n".join(parts), encoding="utf-8")
