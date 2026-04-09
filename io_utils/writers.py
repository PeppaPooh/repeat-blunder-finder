# io_utils/writers.py
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from core.grouping import most_common_pgn
from models.records import ErrorRecord, GenerationMetadata
from utils.time_utils import timestamp_affix

from dataclasses import asdict

def write_jsonl(path: Path, metadata: GenerationMetadata, records: Iterable[ErrorRecord]) -> None:
    with path.open("w", encoding="utf-8") as f:
        f.write(json.dumps({"_meta": asdict(metadata)}, ensure_ascii=False) + "\n")
        for record in records:
            f.write(json.dumps(asdict(record), ensure_ascii=False) + "\n")




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




def ensure_output_dirs() -> Tuple[Path, Path]:
    jsonl_dir = Path("out") / "blunder_positions"
    txt_dir = Path("out") / "txt_reports"
    jsonl_dir.mkdir(parents=True, exist_ok=True)
    txt_dir.mkdir(parents=True, exist_ok=True)
    return jsonl_dir, txt_dir

