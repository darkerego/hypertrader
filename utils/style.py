import builtins
import sys
from typing import Any, Optional

try:
    import colored
except Exception:  # pragma: no cover - color output should degrade cleanly.
    colored = None  # type: ignore[assignment]


_ORIGINAL_PRINT = builtins.print


def _color(name: str) -> str:
    if colored is None:
        return ""
    return str(getattr(colored.Fore, name, ""))


def _style(name: str) -> str:
    if colored is None:
        return ""
    return str(getattr(colored.Style, name, ""))


RESET = _style("RESET")
BOLD = _style("BOLD")
DIM = _style("DIM")

PALETTE = {
    "banner": _color("deep_sky_blue_2"),
    "header": _color("cyan_3"),
    "normal": _color("LIGHT_BLUE"),
    "muted": _color("grey_70"),
    "detail": _color("light_cyan_3"),
    "success": _color("spring_green_3b"),
    "good": _color("yellow_1"),
    "warning": _color("VIOLET"),
    "error": _color("RED_1"),
    "accent": _color("orange_1"),
    "auto": _color("gold_1"),
    "watch": _color("light_slate_blue"),
    "entry": _color("deep_sky_blue_1"),
    "mm": _color("plum_2"),
    "hidden": _color("medium_spring_green"),
}

PREFIX_STYLES = {
    "[ERROR]": "error",
    "[FATAL]": "error",
    "[WARN]": "warning",
    "[AUTO-WARN]": "warning",
    "[WATCH-WARN]": "warning",
    "[WS-WARN]": "warning",
    "[HIDDEN-TP-WARN]": "warning",
    "[TP-REMAINDER-WARN]": "warning",
    "[DONE]": "success",
    "[OK]": "success",
    "[RESULT]": "success",
    "[SUCCESS]": "success",
    "[OPEN]": "good",
    "[START]": "header",
    "[LOG]": "detail",
    "[AUTO]": "auto",
    "[AUTO-RISK]": "accent",
    "[WATCH]": "watch",
    "[ENTRY]": "entry",
    "[MM]": "mm",
    "[HIDDEN-TP]": "hidden",
    "[HIDDEN-SL]": "hidden",
    "[HIDE-ORDERS]": "hidden",
    "[TP-REVERSAL]": "accent",
    "[TP-REVERSAL EXIT]": "accent",
    "[TP-REVERSAL LIMIT]": "accent",
    "[MARKET ENTRY]": "accent",
    "[MARKET EXIT]": "accent",
    "[TP]": "good",
    "[SL-SAR]": "accent",
    "[SL-SAR-WARN]": "warning",
    "[TRAILING-TP]": "accent",
    "[TP-REMAINDER]": "accent",
    "[CANCEL]": "warning",
    "[WS]": "detail",
}


def _emit(text: str, color_key: str = "normal", pretext: Optional[str] = None) -> None:
    if pretext:
        prefix = f"[{pretext}] "
        _ORIGINAL_PRINT(f"{PALETTE.get(color_key, '')}{BOLD}{prefix}{RESET}{text}")
        return
    _ORIGINAL_PRINT(f"{PALETTE.get(color_key, '')}{text}{RESET}")


def _classify_line(line: str) -> str:
    stripped = line.strip()
    if not stripped:
        return line

    if stripped and len(set(stripped)) == 1 and stripped[0] in {"=", "-"} and len(stripped) >= 12:
        return f"{PALETTE['banner']}{BOLD}{line}{RESET}"

    if stripped.startswith("[!]"):
        return f"{PALETTE['warning']}{BOLD}{line}{RESET}"

    if stripped.startswith(" Hyperliquid Async") or stripped.startswith(" Async "):
        return f"{PALETTE['header']}{BOLD}{line}{RESET}"

    for prefix, color_key in sorted(PREFIX_STYLES.items(), key=lambda item: len(item[0]), reverse=True):
        if stripped.startswith(prefix):
            return f"{PALETTE[color_key]}{BOLD}{line}{RESET}"

    if line.startswith("  "):
        return f"{PALETTE['detail']}{line}{RESET}"

    if ":" in line and not stripped.startswith("{"):
        return f"{PALETTE['normal']}{line}{RESET}"

    return f"{PALETTE['muted']}{line}{RESET}"


def emit_styled_print(*values: Any, sep: str = " ", end: str = "\n", file: Any = None, flush: bool = False) -> None:
    target = sys.stdout if file is None else file
    if target is not sys.stdout:
        _ORIGINAL_PRINT(*values, sep=sep, end=end, file=file, flush=flush)
        return

    rendered = sep.join(str(value) for value in values)
    trailing_newline = rendered.endswith("\n")
    segments = rendered.split("\n")
    for index, segment in enumerate(segments):
        is_last = index == len(segments) - 1
        segment_end = "" if not is_last else end
        _ORIGINAL_PRINT(_classify_line(segment), end=segment_end, file=target, flush=False)
        if index < len(segments) - 1:
            _ORIGINAL_PRINT("", file=target, flush=False)
    if trailing_newline and end == "\n":
        target.flush()
    if flush:
        target.flush()


def install_pretty_stdout() -> None:
    builtins.print = emit_styled_print


class PrettyText:
    @classmethod
    def normal(cls, data: Any, pretext: str = "+") -> None:
        _emit(str(data), "normal", pretext)

    @classmethod
    def error(cls, data: Any, pretext: str = "!") -> None:
        _emit(str(data), "error", pretext)

    @classmethod
    def good(cls, data: Any, pretext: str = "~") -> None:
        _emit(str(data), "good", pretext)

    @classmethod
    def success(cls, data: Any, pretext: str = "~") -> None:
        _emit(str(data), "success", pretext)

    @classmethod
    def warning(cls, data: Any, pretext: str = "*") -> None:
        _emit(str(data), "warning", pretext)

    @classmethod
    def print(cls, data: Any, color: str) -> None:
        if colored is None:
            _ORIGINAL_PRINT(str(data))
            return
        _ORIGINAL_PRINT(_color(color) + str(data) + RESET)
