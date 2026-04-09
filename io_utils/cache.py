# io_utils/cache.py
import json
from dataclasses import fields
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from models.records import ErrorRecord, GenerationMetadata
from utils.cli import params_for_cache_comparison

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

