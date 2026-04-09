# core/grouping.py
import logging
from collections import Counter
from typing import Dict, Iterable, List, Optional, Set, Tuple, Any

from core.analysis import extract_first_error_from_game
from models.records import ErrorRecord

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

