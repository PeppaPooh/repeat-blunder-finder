import express from 'express';
import fs from 'fs';
import path from 'path';
import { parseBlunders } from './parseBlunders';

const app = express();
const PORT = 3000;

function resolveBlunderFilePath(): string {
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

app.use(express.static(path.join(__dirname, 'public')));
app.use('/vendor/chess.js', express.static(path.join(__dirname, 'node_modules/chess.js/dist')));
app.use('/vendor/chessground', express.static(path.join(__dirname, 'node_modules/chessground')));

app.get('/blunders', (req, res) => {
    const page = parseInt(req.query.page as string) || 0;
    const perPage = parseInt(req.query.perPage as string) || 6;
    const start = page * perPage;
    const end = start + perPage;
    const totalPages = Math.ceil(blunders.length / perPage);

    res.json({
        blunders: blunders.slice(start, end),
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
