# io_utils/config_loader.py
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple
import argparse

import yaml

from core.fen_utils import normalize_fen_string

def parse_ignore_positions_fallback(raw_text: str) -> List[str]:
    """
    Supports the exact loose template style the user provided, where ignore-positions
    is followed by indented FEN lines without YAML list dashes.
    """
    lines = raw_text.splitlines()
    in_block = False
    results: List[str] = []

    for line in lines:
        if re.match(r"^\s*ignore-positions\s*:\s*$", line):
            in_block = True
            continue

        if in_block:
            if not line.strip():
                continue
            if re.match(r"^\S", line):
                break
            fen = line.strip()
            if fen:
                results.append(fen)

    return results


def load_config(config_path: str) -> Dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        logging.info("Config file not found at %s; continuing without config defaults.", config_path)
        return {}

    raw_text = path.read_text(encoding="utf-8")

    loaded: Dict[str, Any] = {}
    yaml_ok = False

    try:
        parsed = yaml.safe_load(raw_text)
        if isinstance(parsed, dict):
            loaded = parsed
            yaml_ok = True
    except yaml.YAMLError:
        yaml_ok = False

    if not yaml_ok:
        logging.warning(
            "Config YAML parsing failed or was not a mapping. Falling back to lenient parser for ignore-positions."
        )

    config: Dict[str, Any] = {}
    config["default-lichess-username"] = loaded.get("default-lichess-username")
    config["default-start-date"] = loaded.get("default-start-date")

    ignore_positions = loaded.get("ignore-positions", None)

    if isinstance(ignore_positions, list):
        config["ignore-positions"] = [str(x).strip() for x in ignore_positions if str(x).strip()]
    elif isinstance(ignore_positions, str):
        config["ignore-positions"] = [line.strip() for line in ignore_positions.splitlines() if line.strip()]
    else:
        config["ignore-positions"] = parse_ignore_positions_fallback(raw_text)

    return config


def resolve_runtime_settings(args: argparse.Namespace, config: Dict[str, Any]) -> Tuple[str, str, Set[str]]:
    username = args.username or config.get("default-lichess-username")
    since = args.since or config.get("default-start-date")

    if not username:
        raise ValueError("No username provided. Use --username or set default-lichess-username in config.yaml.")
    if not since:
        raise ValueError("No start date provided. Use --since or set default-start-date in config.yaml.")

    ignore_fens_raw = config.get("ignore-positions", []) or []
    ignore_fens_normalized: Set[str] = set()

    for fen in ignore_fens_raw:
        try:
            ignore_fens_normalized.add(normalize_fen_string(fen))
        except Exception as exc:
            logging.warning("Skipping invalid ignore-position FEN from config: %s (%s)", fen, exc)

    return username, since, ignore_fens_normalized

