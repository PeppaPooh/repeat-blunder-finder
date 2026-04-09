# lichess/parsing.py
import logging
from io import StringIO
from typing import Any, Dict, Optional, Tuple

import chess.pgn

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
