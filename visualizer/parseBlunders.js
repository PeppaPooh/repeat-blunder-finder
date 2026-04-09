const fs = require('fs');

function parseBlunders(filePath) {
  const text = fs.readFileSync(filePath, 'utf-8');
  const positions = text.split(/=== Repeated Position #[0-9]+ ===/).slice(1);

  const blunders = positions.map((posText, idx) => {
    const fenMatch = posText.match(/FEN:\s*(.*)/);
    const pgnLineMatch = posText.match(/Most common game PGN.*?:\s*(.*)/);
    const countMatch = posText.match(/Count:\s*(\d+)/);
    const pgnWithMeta = pgnLineMatch && pgnLineMatch[1] ? pgnLineMatch[1].trim() : '';
    const pgn = pgnWithMeta.split('|')[0].trim();
    const openingMatch = pgnWithMeta.match(/opening=(.*?)(?:\s*\(\d+x\))?$/);
    const dateMatches = [...posText.matchAll(/date=(\d{4}-\d{2}-\d{2}T[\d:.]+)\+00:00/g)];
    const allDates = dateMatches.map((m) => new Date(`${m[1]}+00:00`));
    const latestDate =
      allDates.length > 0
        ? new Date(Math.max(...allDates.map((d) => d.getTime())))
        : new Date(0);
    const urlMatch = posText.match(/url=(https?:\/\/[^\s]+)/);

    return {
      index: idx + 1,
      fen: fenMatch ? fenMatch[1] : '',
      pgn,
      latestDate,
      latestDateIso: latestDate.toISOString(),
      count: countMatch ? Number(countMatch[1]) : 0,
      opening: openingMatch ? openingMatch[1].trim() : undefined,
      url: urlMatch ? urlMatch[1] : undefined,
    };
  });

  return blunders.sort((a, b) => b.latestDate.getTime() - a.latestDate.getTime());
}

module.exports = { parseBlunders };
