#!/usr/bin/env python3
"""
Find recurring early-game blunder positions for a Lichess user.

What it does:
- Downloads analyzed games for a Lichess user since a YYYY-MM-DD date.
- Keeps only standard chess games.
- For each game, finds the user's first blunder within the first N full moves.
- Stores each found blunder position locally in JSONL.
- Exports repeated blunder positions to a TXT file keyed by normalized FEN.
- Emits progress logs while running.

Install:
    pip install requests python-chess

Examples:
    python lichess_blunder_positions.py \
        --username some_user \
        --since 2025-01-01 \
        --max-fullmoves 10 \
        --out-dir out

    python lichess_blunder_positions.py \
        --username some_user \
        --since 2025-01-01 \
        --token lip_your_token_here \
        --log-every 25
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple


import chess
import chess.pgn
import requests



API_URL_TEMPLATE = "https://lichess.org/api/games/user/{username}"


@dataclass
class BlunderRecord:
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
    move_number: int               # fullmove number, e.g. 6 means move 6
    ply_index: int                 # 1-based ply index
    san_played: str                # SAN of the blunder move
    fen_before: str                # full FEN before the blunder move
    normalized_fen: str            # FEN normalized for repetition matching
    comment: Optional[str]         # analysis comment if present
    judgment_name: Optional[str]   # "Blunder" if directly available
    source: str                    # "analysis", "pgn_nag", or "pgn_comment"


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find recurring early blunder positions for a Lichess user."
    )
    parser.add_argument("--username", required=True, help="Lichess username")
    parser.add_argument(
        "--since",
        required=True,
        help="Start date in YYYY-MM-DD (inclusive, interpreted as 00:00:00 UTC)",
    )
    parser.add_argument(
        "--max-fullmoves",
        type=int,
        default=10,
        help="Only inspect blunders within the first N full moves (default: 10)",
    )
    parser.add_argument(
        "--out-dir",
        default="lichess_blunder_output",
        help="Directory for output files",
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
    return parser.parse_args()


def yyyy_mm_dd_to_epoch_ms(date_str: str) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def epoch_ms_to_iso_utc(epoch_ms: Optional[int]) -> Optional[str]:
    if epoch_ms is None:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat()


def normalized_fen(board: chess.Board) -> str:
    """
    Normalize FEN so repeated-position matching ignores halfmove/fullmove counters.
    This preserves:
    - piece placement
    - side to move
    - castling rights
    - en passant square
    """
    ep = chess.square_name(board.ep_square) if board.ep_square is not None else "-"
    return f"{board.board_fen()} {'w' if board.turn else 'b'} {board.castling_xfen()} {ep} 0 1"


def lichess_headers(token: Optional[str]) -> Dict[str, str]:
    headers = {
        "Accept": "application/x-ndjson",
        "User-Agent": "lichess-blunder-positions/1.0",
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
    """
    Streams NDJSON game exports from Lichess.

    Retries politely on 429 according to Lichess guidance.
    """
    url = API_URL_TEMPLATE.format(username=username)
    params = {
        "since": since_ms,
        "analysed": "true",
        "finished": "true",
        "pgnInJson": "true",
        "evals": "true",
        "opening": "false",
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


def extract_first_blunder_from_game(
    game_json: Dict[str, Any],
    username: str,
    max_fullmoves: int,
) -> Optional[BlunderRecord]:
    """
    Prefer structured JSON analysis if present.
    Fall back to PGN NAG/comment detection if necessary.
    """
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

    while node.variations:
        next_node = node.variation(0)
        move = next_node.move
        ply_index += 1

        # Position before the move is the decision point.
        fen_before = board.fen()
        norm_before = normalized_fen(board)
        move_number = board.fullmove_number
        is_user_move = (board.turn == chess.WHITE and user_is_white) or (
            board.turn == chess.BLACK and not user_is_white
        )

        if is_user_move and move_number <= max_fullmoves:
            san_played = board.san(move)

            # Path 1: structured analysis from JSON
            if has_structured_analysis and len(analysis) >= ply_index:
                analysis_item = analysis[ply_index - 1] or {}
                judgment = analysis_item.get("judgment") or {}
                judgment_name = judgment.get("name")
                comment = judgment.get("comment")

                if judgment_name == "Blunder":
                    return BlunderRecord(
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
                    )

            # Path 2: PGN annotations / NAG fallback
            comment_text = (next_node.comment or "").strip()
            nags = set(next_node.nags or set())

            is_pgn_blunder = (
                chess.pgn.NAG_BLUNDER in nags
                or "blunder" in comment_text.lower()
            )

            if is_pgn_blunder:
                return BlunderRecord(
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
                    judgment_name="Blunder" if "blunder" in comment_text.lower() else None,
                    source="pgn_nag" if chess.pgn.NAG_BLUNDER in nags else "pgn_comment",
                )

        board.push(move)
        node = next_node

        # Once we've moved past the search window for both sides, stop early.
        if board.fullmove_number > max_fullmoves and board.turn == chess.WHITE:
            break

    return None


def write_jsonl(path: Path, records: Iterable[BlunderRecord]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(asdict(record), ensure_ascii=False) + "\n")


def write_repeated_txt(path: Path, grouped: Dict[str, List[BlunderRecord]]) -> None:
    repeated_items = [(fen, recs) for fen, recs in grouped.items() if len(recs) > 1]
    repeated_items.sort(key=lambda item: (-len(item[1]), item[0]))

    with path.open("w", encoding="utf-8") as f:
        if not repeated_items:
            f.write("No repeated early blunder positions were found.\n")
            return

        for idx, (fen, recs) in enumerate(repeated_items, start=1):
            f.write(f"=== Repeated Position #{idx} ===\n")
            f.write(f"Count: {len(recs)}\n")
            f.write(f"FEN: {fen}\n")
            f.write("Occurrences:\n")
            for rec in recs:
                f.write(
                    f"  - game={rec.game_id} date={rec.played_at_utc or 'unknown'} "
                    f"color={rec.color} move={rec.move_number} san={rec.san_played} "
                    f"opponent={rec.opponent} url={rec.game_url}\n"
                )
            f.write("\n")


def main() -> int:
    args = parse_args()
    setup_logging(args.verbose)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    since_ms = yyyy_mm_dd_to_epoch_ms(args.since)
    logging.info(
        "Starting scan for user=%s since=%s (epoch_ms=%d), max_fullmoves=%d",
        args.username,
        args.since,
        since_ms,
        args.max_fullmoves,
    )

    processed = 0
    standard_games = 0
    found_blunders = 0
    skipped_duplicates = 0
    seen_game_ids = set()

    records: List[BlunderRecord] = []

    try:
        for game_json in stream_user_games(
            username=args.username,
            since_ms=since_ms,
            token=args.token,
            timeout=args.timeout,
        ):
            processed += 1
            game_id = game_json.get("id")

            if game_id in seen_game_ids:
                skipped_duplicates += 1
                logging.debug("Skipping duplicate game id %s", game_id)
                continue
            seen_game_ids.add(game_id)

            if (game_json.get("variant") or "").lower() != "standard":
                if processed % args.log_every == 0:
                    logging.info(
                        "Progress: processed=%d standard=%d found=%d duplicates=%d",
                        processed,
                        standard_games,
                        found_blunders,
                        skipped_duplicates,
                    )
                continue

            standard_games += 1

            record = extract_first_blunder_from_game(
                game_json=game_json,
                username=args.username,
                max_fullmoves=args.max_fullmoves,
            )
            if record is not None:
                found_blunders += 1
                records.append(record)
                logging.info(
                    "Found early blunder: game=%s move=%d color=%s san=%s fen=%s",
                    record.game_id,
                    record.move_number,
                    record.color,
                    record.san_played,
                    record.normalized_fen,
                )

            if processed % args.log_every == 0:
                logging.info(
                    "Progress: processed=%d standard=%d found=%d duplicates=%d",
                    processed,
                    standard_games,
                    found_blunders,
                    skipped_duplicates,
                )

    except KeyboardInterrupt:
        logging.warning("Interrupted by user. Writing partial results.")
    except Exception as exc:
        logging.exception("Fatal error: %s", exc)
        return 1

    grouped: Dict[str, List[BlunderRecord]] = defaultdict(list)
    for rec in records:
        grouped[rec.normalized_fen].append(rec)

    jsonl_path = out_dir / "blunder_positions.jsonl"
    repeated_txt_path = out_dir / "repeated_blunder_positions.txt"

    write_jsonl(jsonl_path, records)
    write_repeated_txt(repeated_txt_path, grouped)

    repeated_count = sum(1 for recs in grouped.values() if len(recs) > 1)

    logging.info("Done.")
    logging.info("Processed total games: %d", processed)
    logging.info("Standard games checked: %d", standard_games)
    logging.info("Blunder positions stored: %d", len(records))
    logging.info("Repeated blunder positions found: %d", repeated_count)
    logging.info("JSONL output: %s", jsonl_path.resolve())
    logging.info("TXT output: %s", repeated_txt_path.resolve())

    return 0


if __name__ == "__main__":
    sys.exit(main())