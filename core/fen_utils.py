import chess
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