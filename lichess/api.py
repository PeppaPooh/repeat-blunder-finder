# lichess/api.py
import json
import logging
import time
from typing import Any, Dict, Iterator, Optional

import requests

from config import API_URL_TEMPLATE

def lichess_headers(token: Optional[str]) -> Dict[str, str]:
    headers = {
        "Accept": "application/x-ndjson",
        "User-Agent": "lichess-blunder-positions/1.2",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def stream_user_games(
    username: str,
    since_ms: int,
    token: Optional[str],
    timeout: int,
    until_ms: Optional[int] = None,
) -> Iterator[Dict[str, Any]]:
    url = API_URL_TEMPLATE.format(username=username)
    params = {
        "since": since_ms,
        "analysed": "true",
        "finished": "true",
        "pgnInJson": "true",
        "evals": "true",
        "opening": "true",
        "clocks": "false",
    }
    if until_ms is not None:
        params["until"] = until_ms

    session = requests.Session()
    headers = lichess_headers(token)

    while True:
        logging.info(
            "Requesting games from Lichess for %s since %s ms%s",
            username,
            since_ms,
            f" until {until_ms} ms" if until_ms is not None else "",
        )
        with session.get(url, headers=headers, params=params, stream=True, timeout=timeout) as resp:
            if resp.status_code == 429:
                logging.warning("Received HTTP 429 from Lichess. Sleeping 60 seconds before retry.")
                time.sleep(60)
                continue

            try:
                resp.raise_for_status()
            except requests.HTTPError as exc:
                raise RuntimeError(
                    f"Lichess API request failed: HTTP {resp.status_code} - {resp.text[:500]}"
                ) from exc

            for raw_line in resp.iter_lines(decode_unicode=True):
                if not raw_line:
                    continue
                yield json.loads(raw_line)
            break
