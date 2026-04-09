# core/analysis.py
import math
import logging
from typing import Any, Dict, Optional

import chess
import chess.pgn

from config import QUALIFYING_JUDGMENTS
from core.fen_utils import normalized_fen
from lichess.parsing import build_pgn_until_ply, get_user_color_and_opponent, opening_name_from_game_json_or_pgn, parse_pgn_game
from models.records import ErrorRecord
from utils.time_utils import epoch_ms_to_iso_utc

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
