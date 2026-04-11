# Opening Blunder Finder

> There's an old saying in Tennessee -- I know it's in Texas, probably in Tennessee -- that says, fool me once, shame on... shame on you. Fool me -- you can't get fooled again.

-- George W. Bush

**Do you want to get better at chess openings, but don't know where to start? Do you hate staring at opening theory? Are you frustrated that your opponents don't play into the mainline that you learned on Youtube?** This project aims to help. This project is an automated script that looks through your games and finds opening mistakes that you've made more than once. Improve by fixing your mistakes one by one.

![[Visualizer-demo.png]]

# Example Usages
### Get help
~~~bash
python main.py --help
~~~


### Find repeat blunders for EricRosen since Jan 1, 2026,  (default settings)
~~~bash
python main.py --username EricRosen --since 2026-01-01
~~~


### Use settings in config.yaml
~~~bash
python main.py
~~~



## Visualize your repeat blunders
~~~bash
cd visualizer
npm start
~~~
Go to https://localhost:3000 to see your repeat blunders 


# Directory Guide
What each file does：

main.py
- Entry point and orchestration only.
- Parses args, loads config, resolves runtime settings, coordinates cache reuse, fetching, grouping, and output writing.
- Keeps the control flow in one place without low-level logic.

config.py
- Project-wide constants.
- Stores API_URL_TEMPLATE, QUALIFYING_JUDGMENTS, and DEFAULT_CONFIG_PATH.

models/records.py
- Data structures only.
- Holds GenerationMetadata and ErrorRecord dataclasses.

core/analysis.py
- Chess analysis and error-detection logic.
- Decides whether a move qualifies as the first early error.
- Contains centipawn-loss helpers and the main extraction routine.

core/grouping.py
- Record aggregation and deduplication helpers.
- Keeps repeated-position grouping and repeated-line summarization logic separate from I/O.

core/fen_utils.py
- FEN normalization logic.
- Keeps board/FEN transformations in one small, testable module.

io_utils/cache.py
- Saved JSONL reuse and dedupe support.
- Handles loading prior runs, cache compatibility checks, since filtering, and record deduplication.

io_utils/config_loader.py
- Config parsing and runtime setting resolution.
- Handles YAML + lenient fallback parsing and ignore-position normalization.

io_utils/writers.py
- Output writing only.
- Writes JSONL and repeated-position TXT reports, plus output-directory creation.

lichess/api.py
- Lichess HTTP interaction only.
- Builds headers and streams user games from the API.
- Keep only the newer stream_user_games version here (the one that supports until_ms).

lichess/parsing.py
- PGN/game metadata extraction helpers.
- Converts raw API game JSON into parsed objects and user/opponent/opening context.

utils/cli.py
- argparse setup only.

utils/logging_utils.py
- Logging setup only.

utils/time_utils.py
- Time/date formatting and conversion helpers.

Subdirectory purpose

models/
- Pure data containers. No business logic.

core/
- Domain logic for chess position/error analysis and grouping.

io_utils/
- File system reads/writes, cache handling, and config ingestion.

lichess/
- Network/API integration and raw game parsing helpers.

utils/
- Small generic helpers reused across the app.
