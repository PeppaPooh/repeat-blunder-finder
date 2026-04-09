# utils/cli.py
import argparse
import shlex
from typing import Any, Dict, List, Optional, Set
from config import DEFAULT_CONFIG_PATH

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find recurring early error positions for a Lichess user."
    )
    parser.add_argument("--username", default=None, help="Lichess username")
    parser.add_argument(
        "--since",
        default=None,
        help="Start date in YYYY-MM-DD (inclusive, interpreted as 00:00:00 UTC)",
    )
    parser.add_argument(
        "--max-fullmoves",
        type=int,
        default=10,
        help="Only inspect errors within the first N full moves (default: 10)",
    )
    parser.add_argument(
        "--from-cpl",
        type=int,
        default=0,
        help=(
            "If 0, use Lichess judgment text (Inaccuracy/Mistake/Blunder). "
            "If nonzero, use this centipawn loss threshold instead."
        ),
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help="Path to config YAML file (default: config.yaml)",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Optional Lichess API token. Public user games usually work without it.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="HTTP timeout in seconds (default: 60)",
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=25,
        help="Emit a progress log every N processed games (default: 25)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--use-save",
        default=None,
        help=(
            "Path to a prior blunder_positions_*.jsonl file to reuse if generation "
            "params match. If fully covering the requested time range, no new JSONL "
            "is written. If partially covering, only missing time range is fetched."
        ),
    )
    return parser.parse_args()

def build_command_string(argv: List[str]) -> str:
    return " ".join(shlex.quote(arg) for arg in argv)


def build_effective_params(
    username: str,
    since_input: Any,
    max_fullmoves: int,
    from_cpl: int,
    ignore_fens: Set[str],
    config_path: str,
    token_used: bool,
    use_save: Optional[str],
) -> Dict[str, Any]:
    return {
        "username": username,
        "since": str(since_input),
        "max_fullmoves": max_fullmoves,
        "from_cpl": from_cpl,
        "ignore_positions_normalized": sorted(ignore_fens),
        "config_path": config_path,
        "token_used": token_used,
        "use_save": use_save,
    }


def params_for_cache_comparison(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fields that must match for cached JSONL reuse to be safe, excluding time coverage.
    """
    return {
        "username": params.get("username"),
        "max_fullmoves": params.get("max_fullmoves"),
        "from_cpl": params.get("from_cpl"),
        "ignore_positions_normalized": params.get("ignore_positions_normalized", []),
    }
