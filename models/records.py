#!/usr/bin/env python3
# models/records.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

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
