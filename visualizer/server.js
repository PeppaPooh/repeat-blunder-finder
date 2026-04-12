const express = require('express');
const fs = require('fs');
const path = require('path');
const { Chess } = require('chess.js');
const { parseBlunders } = require('./parseBlunders');

const app = express();
const PORT = 3000;

function resolveBlunderFilePath() {
  if (process.env.BLUNDERS_FILE) {
    return path.resolve(process.env.BLUNDERS_FILE);
  }

  const reportsDir = path.resolve(process.cwd(), '../out/txt_reports');
  const reportFiles = fs
    .readdirSync(reportsDir)
    .filter((name) => /^repeated_blunder_positions_.*\.txt$/.test(name))
    .sort();
  const latest = reportFiles[reportFiles.length - 1];
  if (!latest) {
    throw new Error(`No repeated blunder report found in ${reportsDir}`);
  }
  return path.join(reportsDir, latest);
}

const blunderFilePath = resolveBlunderFilePath();
const blunders = parseBlunders(blunderFilePath);

function buildReplayData(pgn, fallbackFen) {
  const game = new Chess();
  try {
    game.loadPgn(pgn);
    const sanMoves = game.history();
    const replay = new Chess();
    const fens = [replay.fen()];
    for (const move of sanMoves) {
      replay.move(move);
      fens.push(replay.fen());
    }
    return { moves: sanMoves, fens };
  } catch (err) {
    const safeFen = fallbackFen || 'start';
    return { moves: [], fens: [safeFen] };
  }
}

app.use(express.static(path.join(__dirname, 'public')));
app.use('/vendor/chess.js', express.static(path.join(__dirname, 'node_modules/chess.js/dist')));
app.use('/vendor/chessground', express.static(path.join(__dirname, 'node_modules/chessground')));

app.get('/blunders', (req, res) => {
  const page = parseInt(req.query.page, 10) || 0;
  const perPage = parseInt(req.query.perPage, 10) || 6;
  const start = page * perPage;
  const end = start + perPage;
  const totalPages = Math.ceil(blunders.length / perPage);
  const pageItems = blunders.slice(start, end).map((b) => {
    const replay = buildReplayData(b.pgn, b.fen);
    
    return {
      ...b,
      replayMoves: replay.moves,
      replayFens: replay.fens,
    };
  });

  res.json({
    blunders: pageItems,
    total: blunders.length,
    page,
    perPage,
    totalPages,
    sourceFile: blunderFilePath,
  });
});

app.listen(PORT, () => {
  console.log(`Loaded ${blunders.length} blunders from ${blunderFilePath}`);
  console.log(`Server running at http://localhost:${PORT}`);
});
