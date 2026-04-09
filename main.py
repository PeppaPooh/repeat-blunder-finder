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

import logging
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set

from models.records import ErrorRecord, GenerationMetadata
from utils.cli import build_command_string, build_effective_params, parse_args
from utils.logging_utils import setup_logging
from utils.time_utils import human_timestamp, iso_utc, now_utc, parse_iso_datetime, timestamp_affix, yyyy_mm_dd_to_epoch_ms
from core.grouping import collect_records_from_stream
from lichess.api import stream_user_games
from io_utils.cache import dedupe_records, filter_records_by_since, get_saved_since, load_saved_jsonl, saved_file_can_be_reused
from io_utils.config_loader import load_config, resolve_runtime_settings
from io_utils.writers import ensure_output_dirs, write_jsonl, write_repeated_txt


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