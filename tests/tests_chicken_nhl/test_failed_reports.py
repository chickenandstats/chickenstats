"""What a failed HTML report must do: record the game as failed, warn, and come back on a retry.

Each report the scraper fetches for a game (play-by-play PL, the two shift reports TH and TV, and the
rosters RO) is failed three ways through the shared network guard: a connection error, a 404 and a 500.
Today most of these return a partial or empty game with no entry in failed_games and no warning, and
Scraper.refresh does not exist yet, so every case is a known bug for block 3.1.
"""

import logging
import warnings
from unittest.mock import patch

import pytest
import requests

from chickenstats.chicken_nhl import Scraper
from test_known_bugs import known_bug

GAME_ID = 2023020001
REPORTS = ["PL", "TH", "TV", "RO"]
FAILURES = ["connection error", 404, 500]


def failing(report: str, failure: str | int):
    """A Session.get that fails one report of the saved game and serves every other request."""
    served_get = requests.Session.get

    def get(self, url, *args, **kwargs):
        if f"/{report}020001.HTM" in str(url):
            if failure == "connection error":
                raise requests.exceptions.ConnectionError("down")
            response = requests.Response()
            response.status_code = int(failure)
            response._content = b""
            response.url = str(url)
            return response
        return served_get(self, url, *args, **kwargs)

    return get


@pytest.mark.parametrize("failure", FAILURES)
@pytest.mark.parametrize("report", REPORTS)
@known_bug(
    "a failed report returns a partial or empty game, often with no failed_games entry or warning, "
    "and there is no Scraper.refresh to retry it",
    raises=(AssertionError, AttributeError),
)
def test_a_failed_report_fails_the_game_and_a_retry_recovers_it(caplog, report, failure):
    with patch.object(requests.Session, "get", failing(report, failure)), warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        with caplog.at_level(logging.WARNING, logger="chickenstats"):
            scraper = Scraper([GAME_ID], disable_progress_bar=True)
            play_by_play = scraper.play_by_play

    assert scraper.failed_games == [GAME_ID]
    assert play_by_play.height == 0
    assert w or caplog.records

    scraper.refresh([GAME_ID])  # ty: ignore[unresolved-attribute]

    assert scraper.failed_games == []
    assert scraper.play_by_play.height == 781
