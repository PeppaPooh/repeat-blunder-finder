#!/usr/bin/env python3
"""
Find recurring early-game error positions for a Lichess user.

What it does:
- Downloads analyzed games for a Lichess user since a YYYY-MM-DD date.
- Keeps only standard chess games.
- For each game, finds the user's first qualifying error within the first N full moves.
- By default, qualifying errors are: Inaccuracy, Mistake, or Blunder.
- Optionally, use --from-cpl N to detect the first move with centipawn loss >= N instead.
- Stores each found position locally in JSONL.
- Exports repeated positions to a TXT file keyed by normalized FEN.
- Emits progress logs while running.
- Supports config.yaml defaults and an ignore list of FEN positions.

Install:
    pip install requests python-chess pyyaml
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import re
import sys
import time
from collections import defaultdict, Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone, date
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple

import shlex
from dataclasses import asdict, dataclass, fields

import chess
import chess.pgn
import requests
import yaml


API_URL_TEMPLATE = "https://lichess.org/api/games/user/{username}"
QUALIFYING_JUDGMENTS = {"Inaccuracy", "Mistake", "Blunder"}
DEFAULT_CONFIG_PATH = "config.yaml"


@dataclass
class GenerationMetadata:
    generated_at_utc: str
    generated_at_human: str
    command: str
    params: Dict[str, Any]



@dataclass
class ErrorRecord:
    username: str
    game_id: str
    game_url: str
    played_at_utc: Optional[str]
    color: str
    opponent: str
    event: str
    white: str
    black: str
    result: str
    move_number: int
    ply_index: int
    san_played: str
    fen_before: str
    normalized_fen: str
    pgn_until_error: str
    opening_name: Optional[str]
    comment: Optional[str]
    judgment_name: Optional[str]
    source: str
    cpl: Optional[int]
    threshold_used: Optional[int]


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def human_timestamp(dt: datetime) -> str:
    return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


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


def yyyy_mm_dd_to_epoch_ms(date_input) -> int:
    """
    Accepts:
    - "YYYY-MM-DD" string
    - datetime.date
    - datetime.datetime
    """
    if isinstance(date_input, datetime):
        dt = date_input
    elif isinstance(date_input, date):
        dt = datetime.combine(date_input, datetime.min.time())
    elif isinstance(date_input, str):
        dt = datetime.strptime(date_input, "%Y-%m-%d")
    else:
        raise TypeError(f"Unsupported date type: {type(date_input)}")

    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def epoch_ms_to_iso_utc(epoch_ms: Optional[int]) -> Optional[str]:
    if epoch_ms is None:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat()


def normalized_fen(board: chess.Board) -> str:
    ep = chess.square_name(board.ep_square) if board.ep_square is not None else "-"
    return f"{board.board_fen()} {'w' if board.turn else 'b'} {board.castling_xfen()} {ep} 0 1"


def normalize_fen_string(fen: str) -> str:
    """
    Normalize incoming FEN strings so config ignores match the same normalized form
    used internally for repetition matching.
    """
    board = chess.Board(fen)
    return normalized_fen(board)


def lichess_headers(token: Optional[str]) -> Dict[str, str]:
    headers = {
        "Accept": "application/x-ndjson",
        "User-Agent": "lichess-blunder-positions/1.2",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def stream_user_games(
    username: str,
    since_ms: int,
    token: Optional[str],
    timeout: int,
) -> Iterator[Dict[str, Any]]:
    url = API_URL_TEMPLATE.format(username=username)
    params = {
        "since": since_ms,
        "analysed": "true",
        "finished": "true",
        "pgnInJson": "true",
        "evals": "true",
        "opening": "true",
        "clocks": "false",
    }

    session = requests.Session()
    headers = lichess_headers(token)

    while True:
        logging.info("Requesting games from Lichess for %s since %s ms", username, since_ms)
        with session.get(url, headers=headers, params=params, stream=True, timeout=timeout) as resp:
            if resp.status_code == 429:
                logging.warning("Received HTTP 429 from Lichess. Sleeping 60 seconds before retry.")
                time.sleep(60)
                continue

            try:
                resp.raise_for_status()
            except requests.HTTPError as exc:
                raise RuntimeError(
                    f"Lichess API request failed: HTTP {resp.status_code} - {resp.text[:500]}"
                ) from exc

            for raw_line in resp.iter_lines(decode_unicode=True):
                if not raw_line:
                    continue
                try:
                    yield json.loads(raw_line)
                except json.JSONDecodeError as exc:
                    logging.error("Skipping malformed NDJSON line: %s", raw_line[:300])
                    raise RuntimeError("Could not decode NDJSON from Lichess.") from exc
            break


def parse_pgn_game(game_json: Dict[str, Any]) -> Optional[chess.pgn.Game]:
    pgn_text = game_json.get("pgn")
    if not pgn_text:
        return None
    try:
        return chess.pgn.read_game(StringIO(pgn_text))
    except Exception as exc:
        logging.warning("Failed to parse PGN for game %s: %s", game_json.get("id"), exc)
        return None


def get_user_color_and_opponent(
    game_json: Dict[str, Any],
    username: str,
) -> Optional[Tuple[str, str]]:
    players = game_json.get("players", {})
    white_name = players.get("white", {}).get("user", {}).get("name")
    black_name = players.get("black", {}).get("user", {}).get("name")

    if white_name and white_name.lower() == username.lower():
        return "white", (black_name or "unknown")
    if black_name and black_name.lower() == username.lower():
        return "black", (white_name or "unknown")
    return None


def eval_item_to_pawns(eval_item: Optional[Dict[str, Any]]) -> Optional[float]:
    if not isinstance(eval_item, dict):
        return None

    if "cp" in eval_item and eval_item["cp"] is not None:
        try:
            return float(eval_item["cp"]) / 100.0
        except (TypeError, ValueError):
            return None

    if "mate" in eval_item and eval_item["mate"] is not None:
        try:
            mate = int(eval_item["mate"])
        except (TypeError, ValueError):
            return None
        return math.inf if mate > 0 else -math.inf

    return None


def compute_centipawn_loss_for_move(
    analysis_before: Optional[Dict[str, Any]],
    analysis_after: Optional[Dict[str, Any]],
    mover_is_white: bool,
) -> Optional[int]:
    before = eval_item_to_pawns(analysis_before)
    after = eval_item_to_pawns(analysis_after)

    if before is None or after is None:
        return None
    if math.isinf(before) or math.isinf(after):
        return None

    if mover_is_white:
        loss_pawns = before - after
    else:
        loss_pawns = after - before

    if loss_pawns <= 0:
        return 0

    return int(round(loss_pawns * 100))


def extract_first_error_from_game(
    game_json: Dict[str, Any],
    username: str,
    max_fullmoves: int,
    from_cpl: int,
) -> Optional[ErrorRecord]:
    variant = (game_json.get("variant") or "").lower()
    if variant != "standard":
        return None

    color_and_opp = get_user_color_and_opponent(game_json, username)
    if color_and_opp is None:
        return None
    user_color, opponent = color_and_opp
    user_is_white = user_color == "white"

    pgn_game = parse_pgn_game(game_json)
    if pgn_game is None:
        logging.debug("Skipping game %s because PGN is unavailable.", game_json.get("id"))
        return None

    analysis = game_json.get("analysis")
    has_structured_analysis = isinstance(analysis, list) and len(analysis) > 0

    board = pgn_game.board()
    node = pgn_game
    ply_index = 0

    white_name = pgn_game.headers.get("White", "")
    black_name = pgn_game.headers.get("Black", "")
    result = pgn_game.headers.get("Result", "")
    event = pgn_game.headers.get("Event", "")
    opening_name = opening_name_from_game_json_or_pgn(game_json, pgn_game)

    while node.variations:
        next_node = node.variation(0)
        move = next_node.move
        ply_index += 1

        fen_before = board.fen()
        norm_before = normalized_fen(board)
        move_number = board.fullmove_number
        is_user_move = (board.turn == chess.WHITE and user_is_white) or (
            board.turn == chess.BLACK and not user_is_white
        )

        if is_user_move and move_number <= max_fullmoves:
            san_played = board.san(move)
            mover_is_white = board.turn == chess.WHITE

            if from_cpl > 0 and has_structured_analysis:
                analysis_before = analysis[ply_index - 2] if 0 <= (ply_index - 2) < len(analysis) else None
                analysis_after = analysis[ply_index - 1] if (ply_index - 1) < len(analysis) else None
                cpl = compute_centipawn_loss_for_move(
                    analysis_before=analysis_before,
                    analysis_after=analysis_after,
                    mover_is_white=mover_is_white,
                )
                if cpl is not None and cpl >= from_cpl:
                    judgment = (analysis_after or {}).get("judgment") or {}
                    judgment_name = judgment.get("name")
                    comment = judgment.get("comment")
                    return ErrorRecord(
                        username=username,
                        game_id=game_json.get("id", ""),
                        game_url=f"https://lichess.org/{game_json.get('id', '')}",
                        played_at_utc=epoch_ms_to_iso_utc(game_json.get("lastMoveAt")),
                        color=user_color,
                        opponent=opponent,
                        event=event,
                        white=white_name,
                        black=black_name,
                        result=result,
                        move_number=move_number,
                        ply_index=ply_index,
                        san_played=san_played,
                        fen_before=fen_before,
                        normalized_fen=norm_before,
                        comment=comment,
                        judgment_name=judgment_name,
                        source="analysis_cpl",
                        cpl=cpl,
                        threshold_used=from_cpl,
                        pgn_until_error=build_pgn_until_ply(pgn_game, ply_index),
                        opening_name=opening_name,
                    )

            elif from_cpl == 0:
                if has_structured_analysis and len(analysis) >= ply_index:
                    analysis_item = analysis[ply_index - 1] or {}
                    judgment = analysis_item.get("judgment") or {}
                    judgment_name = judgment.get("name")
                    comment = judgment.get("comment")

                    if judgment_name in QUALIFYING_JUDGMENTS:
                        return ErrorRecord(
                            username=username,
                            game_id=game_json.get("id", ""),
                            game_url=f"https://lichess.org/{game_json.get('id', '')}",
                            played_at_utc=epoch_ms_to_iso_utc(game_json.get("lastMoveAt")),
                            color=user_color,
                            opponent=opponent,
                            event=event,
                            white=white_name,
                            black=black_name,
                            result=result,
                            move_number=move_number,
                            ply_index=ply_index,
                            san_played=san_played,
                            fen_before=fen_before,
                            normalized_fen=norm_before,
                            comment=comment,
                            judgment_name=judgment_name,
                            source="analysis",
                            cpl=None,
                            threshold_used=None,
                            opening_name=opening_name,
                            pgn_until_error=build_pgn_until_ply(pgn_game, ply_index),
                        )

                comment_text = (next_node.comment or "").strip()
                nags = set(next_node.nags or set())

                is_pgn_error = (
                    chess.pgn.NAG_DUBIOUS_MOVE in nags
                    or chess.pgn.NAG_MISTAKE in nags
                    or chess.pgn.NAG_BLUNDER in nags
                    or "inaccuracy" in comment_text.lower()
                    or "mistake" in comment_text.lower()
                    or "blunder" in comment_text.lower()
                )

                if is_pgn_error:
                    derived_name = None
                    if chess.pgn.NAG_BLUNDER in nags or "blunder" in comment_text.lower():
                        derived_name = "Blunder"
                    elif chess.pgn.NAG_MISTAKE in nags or "mistake" in comment_text.lower():
                        derived_name = "Mistake"
                    elif chess.pgn.NAG_DUBIOUS_MOVE in nags or "inaccuracy" in comment_text.lower():
                        derived_name = "Inaccuracy"

                    return ErrorRecord(
                        username=username,
                        game_id=game_json.get("id", ""),
                        game_url=f"https://lichess.org/{game_json.get('id', '')}",
                        played_at_utc=epoch_ms_to_iso_utc(game_json.get("lastMoveAt")),
                        color=user_color,
                        opponent=opponent,
                        event=event,
                        white=white_name,
                        black=black_name,
                        result=result,
                        move_number=move_number,
                        ply_index=ply_index,
                        san_played=san_played,
                        fen_before=fen_before,
                        normalized_fen=norm_before,
                        comment=comment_text or None,
                        judgment_name=derived_name,
                        source="pgn_nag" if nags else "pgn_comment",
                        cpl=None,
                        threshold_used=None,
                        opening_name=opening_name,
                        pgn_until_error=build_pgn_until_ply(pgn_game, ply_index),
                    )

        board.push(move)
        node = next_node

        if board.fullmove_number > max_fullmoves and board.turn == chess.WHITE:
            break

    return None


def write_jsonl(path: Path, metadata: GenerationMetadata, records: Iterable[ErrorRecord]) -> None:
    with path.open("w", encoding="utf-8") as f:
        f.write(json.dumps({"_meta": asdict(metadata)}, ensure_ascii=False) + "\n")
        for record in records:
            f.write(json.dumps(asdict(record), ensure_ascii=False) + "\n")

def most_common_pgn(records: List[ErrorRecord]) -> Tuple[str, int]:
    """
    Returns (most_common_pgn, count). Ties are broken lexicographically for stability.
    """
    counter = Counter(rec.pgn_until_error for rec in records)
    if not counter:
        return "", 0

    best_count = max(counter.values())
    best_pgns = sorted(pgn for pgn, count in counter.items() if count == best_count)
    best_pgn = best_pgns[0]
    return best_pgn, best_count

def build_pgn_until_ply(game: chess.pgn.Game, target_ply: int) -> str:
    """
    Return SAN move text up to and including target_ply.
    Example: '1. e4 e5 2. Nf3'
    """
    if target_ply <= 0:
        return ""

    board = game.board()
    node = game
    parts: List[str] = []
    ply_index = 0

    while node.variations and ply_index < target_ply:
        next_node = node.variation(0)
        move = next_node.move
        san = board.san(move)

        if board.turn == chess.WHITE:
            parts.append(f"{board.fullmove_number}. {san}")
        else:
            parts.append(san)

        board.push(move)
        node = next_node
        ply_index += 1

    return " ".join(parts)


def opening_name_from_game_json_or_pgn(game_json: Dict[str, Any], pgn_game: chess.pgn.Game) -> Optional[str]:
    """
    Prefer JSON opening field if present, fall back to PGN Opening tag.
    """
    opening = game_json.get("opening")
    if isinstance(opening, dict):
        name = opening.get("name")
        if name:
            return str(name)

    tag_name = pgn_game.headers.get("Opening")
    if tag_name:
        return tag_name

    return None

def write_repeated_txt(
    path: Path,
    grouped: Dict[str, List[ErrorRecord]],
    metadata: GenerationMetadata,
) -> None:
    repeated_items = [(fen, recs) for fen, recs in grouped.items() if len(recs) > 1]
    repeated_items.sort(key=lambda item: (-len(item[1]), item[0]))

    with path.open("w", encoding="utf-8") as f:
        f.write("=== Generation Metadata ===\n")
        f.write(f"Generated at (human): {metadata.generated_at_human}\n")
        f.write(f"Generated at (UTC): {metadata.generated_at_utc}\n")
        f.write(f"Command: {metadata.command}\n")
        f.write("Params:\n")
        for key, value in metadata.params.items():
            f.write(f"  {key}: {value}\n")
        f.write("\n")

        if not repeated_items:
            f.write("No repeated early error positions were found.\n")
            return

        for idx, (fen, recs) in enumerate(repeated_items, start=1):
            common_pgn, common_pgn_count = most_common_pgn(recs)

            common_pgn_records = [rec for rec in recs if rec.pgn_until_error == common_pgn]
            common_opening_counter = Counter(
                rec.opening_name for rec in common_pgn_records if rec.opening_name
            )
            common_opening_text = ""
            if common_opening_counter:
                opening_name, opening_count = common_opening_counter.most_common(1)[0]
                common_opening_text = f" | opening={opening_name} ({opening_count}x)"

            f.write(f"=== Repeated Position #{idx} ===\n")
            f.write(f"Count: {len(recs)}\n")
            f.write(f"FEN: {fen}\n")
            f.write(
                f"Most common game PGN ({common_pgn_count}x): "
                f"{common_pgn}{common_opening_text}\n"
            )
            f.write("Occurrences:\n")

            pgn_buckets: Dict[str, List[ErrorRecord]] = defaultdict(list)
            for rec in recs:
                pgn_buckets[rec.pgn_until_error].append(rec)

            for pgn_line in sorted(pgn_buckets.keys(), key=lambda p: (-len(pgn_buckets[p]), p)):
                bucket = pgn_buckets[pgn_line]
                opening_counter = Counter(
                    rec.opening_name for rec in bucket if rec.opening_name
                )
                opening_summary = ""
                if opening_counter:
                    opening_name, opening_count = opening_counter.most_common(1)[0]
                    opening_summary = f" | opening={opening_name} ({opening_count}x)"

                f.write(f"  PGN group ({len(bucket)}x): {pgn_line}{opening_summary}\n")

                for rec in bucket:
                    extra = ""
                    if rec.cpl is not None:
                        extra = f" cpl={rec.cpl}"
                    elif rec.judgment_name:
                        extra = f" judgment={rec.judgment_name}"

                    opening_part = f" opening={rec.opening_name}" if rec.opening_name else ""
                    f.write(
                        f"    - game={rec.game_id} date={rec.played_at_utc or 'unknown'} "
                        f"color={rec.color} move={rec.move_number} san={rec.san_played} "
                        f"opponent={rec.opponent}{extra}{opening_part} url={rec.game_url}\n"
                    )

            f.write("\n")

def load_saved_jsonl(path: Path) -> Tuple[Optional[GenerationMetadata], List[ErrorRecord]]:
    if not path.exists():
        raise FileNotFoundError(f"Saved JSONL not found: {path}")

    metadata: Optional[GenerationMetadata] = None
    records: List[ErrorRecord] = []

    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            obj = json.loads(line)

            if line_no == 1 and isinstance(obj, dict) and "_meta" in obj:
                raw_meta = obj["_meta"]
                metadata = GenerationMetadata(
                    generated_at_utc=raw_meta["generated_at_utc"],
                    generated_at_human=raw_meta["generated_at_human"],
                    command=raw_meta["command"],
                    params=raw_meta["params"],
                )
                continue

            record_field_names = {field.name for field in fields(ErrorRecord)}
            filtered = {k: v for k, v in obj.items() if k in record_field_names}
            records.append(ErrorRecord(**filtered))

    return metadata, records

def parse_iso_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


def saved_file_can_be_reused(
    saved_meta: Optional[GenerationMetadata],
    current_params: Dict[str, Any],
) -> bool:
    if saved_meta is None:
        return False

    return params_for_cache_comparison(saved_meta.params) == params_for_cache_comparison(current_params)


def get_saved_since(saved_meta: GenerationMetadata) -> Any:
    return saved_meta.params.get("since")


def filter_records_by_since(records: List[ErrorRecord], since_ms: int) -> List[ErrorRecord]:
    kept: List[ErrorRecord] = []
    for rec in records:
        if not rec.played_at_utc:
            continue
        try:
            rec_dt = datetime.fromisoformat(rec.played_at_utc)
        except ValueError:
            continue
        if int(rec_dt.timestamp() * 1000) >= since_ms:
            kept.append(rec)
    return kept


def dedupe_records(records: List[ErrorRecord]) -> List[ErrorRecord]:
    seen = set()
    out: List[ErrorRecord] = []
    for rec in records:
        key = (
            rec.game_id,
            rec.ply_index,
            rec.normalized_fen,
            rec.pgn_until_error,
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(rec)
    return out

def stream_user_games(
    username: str,
    since_ms: int,
    token: Optional[str],
    timeout: int,
    until_ms: Optional[int] = None,
) -> Iterator[Dict[str, Any]]:
    url = API_URL_TEMPLATE.format(username=username)
    params = {
        "since": since_ms,
        "analysed": "true",
        "finished": "true",
        "pgnInJson": "true",
        "evals": "true",
        "opening": "true",
        "clocks": "false",
    }
    if until_ms is not None:
        params["until"] = until_ms

    session = requests.Session()
    headers = lichess_headers(token)

    while True:
        logging.info(
            "Requesting games from Lichess for %s since %s ms%s",
            username,
            since_ms,
            f" until {until_ms} ms" if until_ms is not None else "",
        )
        with session.get(url, headers=headers, params=params, stream=True, timeout=timeout) as resp:
            if resp.status_code == 429:
                logging.warning("Received HTTP 429 from Lichess. Sleeping 60 seconds before retry.")
                time.sleep(60)
                continue

            try:
                resp.raise_for_status()
            except requests.HTTPError as exc:
                raise RuntimeError(
                    f"Lichess API request failed: HTTP {resp.status_code} - {resp.text[:500]}"
                ) from exc

            for raw_line in resp.iter_lines(decode_unicode=True):
                if not raw_line:
                    continue
                yield json.loads(raw_line)
            break

def collect_records_from_stream(
    games_iter: Iterable[Dict[str, Any]],
    username: str,
    max_fullmoves: int,
    from_cpl: int,
    ignore_fens: Set[str],
    log_every: int,
    initial_processed: int = 0,
    initial_standard: int = 0,
    initial_found: int = 0,
    initial_ignored: int = 0,
    initial_duplicates: int = 0,
    seen_game_ids: Optional[Set[str]] = None,
) -> Tuple[List[ErrorRecord], Dict[str, int], Set[str]]:
    processed = initial_processed
    standard_games = initial_standard
    found_errors = initial_found
    ignored_matches = initial_ignored
    skipped_duplicates = initial_duplicates
    seen_game_ids = seen_game_ids or set()

    records: List[ErrorRecord] = []

    for game_json in games_iter:
        processed += 1
        game_id = game_json.get("id")

        if game_id in seen_game_ids:
            skipped_duplicates += 1
            logging.debug("Skipping duplicate game id %s", game_id)
            continue
        seen_game_ids.add(game_id)

        if (game_json.get("variant") or "").lower() != "standard":
            if processed % log_every == 0:
                logging.info(
                    "Progress: processed=%d standard=%d found=%d ignored=%d duplicates=%d",
                    processed,
                    standard_games,
                    found_errors,
                    ignored_matches,
                    skipped_duplicates,
                )
            continue

        standard_games += 1

        record = extract_first_error_from_game(
            game_json=game_json,
            username=username,
            max_fullmoves=max_fullmoves,
            from_cpl=from_cpl,
        )

        if record is not None:
            if record.normalized_fen in ignore_fens:
                ignored_matches += 1
                logging.info(
                    "Ignored configured position: game=%s move=%d fen=%s",
                    record.game_id,
                    record.move_number,
                    record.normalized_fen,
                )
            else:
                found_errors += 1
                logging.info(
                    "Found early error: game=%s move=%d color=%s san=%s source=%s judgment=%s cpl=%s fen=%s",
                    record.game_id,
                    record.move_number,
                    record.color,
                    record.san_played,
                    record.source,
                    record.judgment_name,
                    record.cpl,
                    record.normalized_fen,
                )
                records.append(record)

        if processed % log_every == 0:
            logging.info(
                "Progress: processed=%d standard=%d found=%d ignored=%d duplicates=%d",
                processed,
                standard_games,
                found_errors,
                ignored_matches,
                skipped_duplicates,
            )

    stats = {
        "processed": processed,
        "standard_games": standard_games,
        "found_errors": found_errors,
        "ignored_matches": ignored_matches,
        "skipped_duplicates": skipped_duplicates,
    }
    return records, stats, seen_game_ids

def timestamp_affix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def ensure_output_dirs() -> Tuple[Path, Path]:
    jsonl_dir = Path("out") / "blunder_positions"
    txt_dir = Path("out") / "txt_reports"
    jsonl_dir.mkdir(parents=True, exist_ok=True)
    txt_dir.mkdir(parents=True, exist_ok=True)
    return jsonl_dir, txt_dir


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


def main() -> int:
    args = parse_args()
    setup_logging(args.verbose)

    config = load_config(args.config)
    username, since_input, ignore_fens = resolve_runtime_settings(args, config)

    current_run_dt = now_utc()
    since_ms = yyyy_mm_dd_to_epoch_ms(since_input)
    jsonl_dir, txt_dir = ensure_output_dirs()
    stamp = timestamp_affix()

    jsonl_path = jsonl_dir / f"blunder_positions_{stamp}.jsonl"
    repeated_txt_path = txt_dir / f"repeated_blunder_positions_{stamp}.txt"

    effective_params = build_effective_params(
        username=username,
        since_input=since_input,
        max_fullmoves=args.max_fullmoves,
        from_cpl=args.from_cpl,
        ignore_fens=ignore_fens,
        config_path=args.config,
        token_used=bool(args.token),
        use_save=args.use_save,
    )

    metadata = GenerationMetadata(
        generated_at_utc=iso_utc(current_run_dt),
        generated_at_human=human_timestamp(current_run_dt),
        command=build_command_string(sys.argv),
        params=effective_params,
    )

    logging.info(
        "Starting scan for user=%s since=%s (epoch_ms=%d), max_fullmoves=%d, from_cpl=%d",
        username,
        str(since_input),
        since_ms,
        args.max_fullmoves,
        args.from_cpl,
    )

    if args.from_cpl == 0:
        logging.info("Mode: judgment text (Inaccuracy/Mistake/Blunder)")
    else:
        logging.info("Mode: centipawn loss threshold >= %d", args.from_cpl)

    logging.info("Ignoring %d configured position(s).", len(ignore_fens))
    logging.info("TXT report will be written to: %s", repeated_txt_path.resolve())

    all_records: List[ErrorRecord] = []
    write_new_jsonl = True
    stats = {
        "processed": 0,
        "standard_games": 0,
        "found_errors": 0,
        "ignored_matches": 0,
        "skipped_duplicates": 0,
    }
    seen_game_ids: Set[str] = set()

    try:
        reused_records: List[ErrorRecord] = []
        saved_meta: Optional[GenerationMetadata] = None

        if args.use_save:
            save_path = Path(args.use_save)
            logging.info("Attempting to reuse saved JSONL: %s", save_path.resolve())
            saved_meta, saved_records = load_saved_jsonl(save_path)

            if not saved_file_can_be_reused(saved_meta, effective_params):
                logging.info("Saved JSONL params do not match current run. Ignoring cache.")
            else:
                saved_since_input = get_saved_since(saved_meta)
                saved_since_ms = yyyy_mm_dd_to_epoch_ms(saved_since_input)
                saved_generated_until_ms = int(parse_iso_datetime(saved_meta.generated_at_utc).timestamp() * 1000)

                if saved_since_ms <= since_ms:
                    # Saved file starts earlier or equal. Filter it down to requested lower bound.
                    reused_records = filter_records_by_since(saved_records, since_ms)

                    # Full coverage only if we accept saved upper bound as current upper bound.
                    # In practice, to avoid stale silent reuse, only fully reuse if generated "now enough".
                    # Here we define full reuse strictly as: no additional upper bound requested beyond saved run time.
                    # Since this script always means "up to now", a saved file can never fully cover a later current time.
                    # So we still fetch from saved_generated_until_ms onward.
                    logging.info(
                        "Saved JSONL matches params and covers requested lower bound. "
                        "Will reuse %d records and fetch newer games since saved generation time.",
                        len(reused_records),
                    )

                    new_records, stats, seen_game_ids = collect_records_from_stream(
                        games_iter=stream_user_games(
                            username=username,
                            since_ms=saved_generated_until_ms,
                            token=args.token,
                            timeout=args.timeout,
                            until_ms=None,
                        ),
                        username=username,
                        max_fullmoves=args.max_fullmoves,
                        from_cpl=args.from_cpl,
                        ignore_fens=ignore_fens,
                        log_every=args.log_every,
                        seen_game_ids=set(rec.game_id for rec in reused_records),
                    )
                    all_records = dedupe_records(reused_records + new_records)

                elif saved_since_ms > since_ms:
                    logging.info(
                        "Saved JSONL is a subset of requested range. "
                        "Will reuse %d saved records and fetch older missing range [%s, %s).",
                        len(saved_records),
                        str(since_input),
                        str(saved_since_input),
                    )

                    reused_records = saved_records

                    older_records, stats, seen_game_ids = collect_records_from_stream(
                        games_iter=stream_user_games(
                            username=username,
                            since_ms=since_ms,
                            until_ms=saved_since_ms,
                            token=args.token,
                            timeout=args.timeout,
                        ),
                        username=username,
                        max_fullmoves=args.max_fullmoves,
                        from_cpl=args.from_cpl,
                        ignore_fens=ignore_fens,
                        log_every=args.log_every,
                        seen_game_ids=set(rec.game_id for rec in reused_records),
                    )
                    all_records = dedupe_records(older_records + reused_records)

                if not all_records and reused_records:
                    all_records = dedupe_records(reused_records)

                # Decide whether to write a new JSONL.
                # If nothing new was fetched and saved file fully satisfied current request, skip writing.
                # Because the request's implicit upper bound is "now", true full coverage is only possible
                # if the saved file was generated in this same instant, which is unrealistic.
                # So here: skip writing only when current command explicitly chooses to trust saved file as-is
                # and no fetch was necessary. That happens when current "now" is effectively accepted as saved upper bound.
                #
                # For practical behavior, if saved_since_ms == since_ms and the user reuses save immediately,
                # you may still want no new JSONL. We use a small freshness window.
                if saved_meta and saved_file_can_be_reused(saved_meta, effective_params):
                    saved_generated_dt = parse_iso_datetime(saved_meta.generated_at_utc)
                    age_seconds = (current_run_dt - saved_generated_dt).total_seconds()

                    if saved_since_ms == since_ms and age_seconds <= 60:
                        logging.info(
                            "Saved JSONL fully reused (fresh within 60s). No new blunder_positions JSONL will be written."
                        )
                        write_new_jsonl = False
                        all_records = dedupe_records(reused_records if reused_records else saved_records)

        if not args.use_save or not all_records:
            logging.info("Fetching all data from Lichess.")
            fetched_records, stats, seen_game_ids = collect_records_from_stream(
                games_iter=stream_user_games(
                    username=username,
                    since_ms=since_ms,
                    token=args.token,
                    timeout=args.timeout,
                    until_ms=None,
                ),
                username=username,
                max_fullmoves=args.max_fullmoves,
                from_cpl=args.from_cpl,
                ignore_fens=ignore_fens,
                log_every=args.log_every,
                seen_game_ids=set(),
            )
            all_records = dedupe_records(fetched_records)

    except KeyboardInterrupt:
        logging.warning("Interrupted by user. Writing partial results.")
    except Exception as exc:
        logging.exception("Fatal error: %s", exc)
        return 1

    grouped: Dict[str, List[ErrorRecord]] = defaultdict(list)
    for rec in all_records:
        grouped[rec.normalized_fen].append(rec)

    if write_new_jsonl:
        logging.info("JSONL output will be written to: %s", jsonl_path.resolve())
        write_jsonl(jsonl_path, metadata, all_records)
    else:
        logging.info("Skipping new JSONL generation because saved JSONL fully satisfied the request.")

    write_repeated_txt(repeated_txt_path, grouped, metadata)

    repeated_count = sum(1 for recs in grouped.values() if len(recs) > 1)

    logging.info("Done.")
    logging.info("Processed total games: %d", stats["processed"])
    logging.info("Standard games checked: %d", stats["standard_games"])
    logging.info("Error positions stored: %d", len(all_records))
    logging.info("Ignored configured matches: %d", stats["ignored_matches"])
    logging.info("Repeated error positions found: %d", repeated_count)
    if write_new_jsonl:
        logging.info("JSONL output: %s", jsonl_path.resolve())
    logging.info("TXT output: %s", repeated_txt_path.resolve())

    return 0


if __name__ == "__main__":
    sys.exit(main())