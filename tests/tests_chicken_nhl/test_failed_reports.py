"""What a failed HTML report must do: record the game as failed, warn, and come back on a retry.

Each report the scraper fetches for a game (play-by-play PL, the two shift reports TH and TV, and the
rosters RO) is failed four ways through the shared network guard: a connection error, a 404, a 500, and
a report cut off before its end.
Each must record the game in failed_games with a warning that names the report. The retry through
Scraper.refresh is a known bug until block 3.3 adds it.
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
FAILURES = ["connection error", 404, 500, "cut off"]


def failing(report: str, failure: str | int):
    """A Session.get that fails one report of the saved game and serves every other request."""
    served_get = requests.Session.get

    def get(self, url, *args, **kwargs):
        if f"/{report}020001.HTM" in str(url):
            if failure == "connection error":
                raise requests.exceptions.ConnectionError("down")
            if failure == "cut off":
                response = served_get(self, url, *args, **kwargs)
                response._content = response.content[: int(len(response.content) * 0.6)]
                return response
            response = requests.Response()
            response.status_code = int(failure)
            response._content = b""
            response.url = str(url)
            return response
        return served_get(self, url, *args, **kwargs)

    return get


def scrape_with_a_failed_report(caplog, report: str, failure: str | int):
    """Scrape the saved game with one report failing, and return the Scraper, its play-by-play and the warnings."""
    with patch.object(requests.Session, "get", failing(report, failure)), warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        with caplog.at_level(logging.WARNING, logger="chickenstats"):
            scraper = Scraper([GAME_ID], disable_progress_bar=True)
            play_by_play = scraper.play_by_play

    return scraper, play_by_play, list(w) + list(caplog.records)


@pytest.mark.parametrize("failure", FAILURES)
@pytest.mark.parametrize("report", REPORTS)
def test_a_failed_report_fails_the_game(caplog, report, failure):
    """A finished game with a report that cannot be read is recorded as failed, with a warning, not returned partial."""
    scraper, play_by_play, warned = scrape_with_a_failed_report(caplog, report, failure)

    assert scraper.failed_games == [GAME_ID]
    assert play_by_play.height == 0
    assert any(report in str(getattr(record, "message", record)) for record in warned)


@pytest.mark.parametrize("failure", FAILURES)
@pytest.mark.parametrize("report", REPORTS)
@known_bug("there is no Scraper.refresh to retry a failed game (block 3.3)", raises=AttributeError)
def test_a_retry_recovers_a_failed_game(caplog, report, failure):
    scraper, _, _ = scrape_with_a_failed_report(caplog, report, failure)

    scraper.refresh([GAME_ID])

    assert scraper.failed_games == []
    assert scraper.play_by_play.height == 781


def test_a_finished_game_whose_events_stop_before_the_game_end_fails(caplog):
    """An events report with status 200 that has no game end row stopped early, so the game is not complete."""
    served_get = requests.Session.get

    def without_game_end(self, url, *args, **kwargs):
        response = served_get(self, url, *args, **kwargs)
        if "/PL020001.HTM" in str(url):
            html = response.content.decode("utf-8")
            start = html.rfind("<tr", 0, html.find(">GEND<"))
            end = html.find("</tr>", start) + len("</tr>")
            response._content = (html[:start] + html[end:]).encode()
        return response

    with patch.object(requests.Session, "get", without_game_end), warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        with caplog.at_level(logging.WARNING, logger="chickenstats"):
            scraper = Scraper([GAME_ID], disable_progress_bar=True)
            _ = scraper.play_by_play

    assert scraper.failed_games == [GAME_ID]
    assert any("no game end" in record.getMessage() for record in caplog.records)


def test_a_defect_in_the_play_by_play_pipeline_reaches_the_error_log(caplog):
    """A bug while building play-by-play is not relabeled as a data mismatch: it is logged as an unexpected error."""
    from chickenstats.chicken_nhl import Game
    from chickenstats.chicken_nhl._game_pbp import _GamePBPMixin

    def broken(self, merged_events, actives):
        raise ZeroDivisionError("a defect")

    with patch.object(_GamePBPMixin, "_track_pbp_state", broken):
        with pytest.raises(ZeroDivisionError):
            _ = Game(GAME_ID).play_by_play

        with warnings.catch_warnings(), caplog.at_level(logging.ERROR, logger="chickenstats"):
            warnings.simplefilter("ignore")
            scraper = Scraper([GAME_ID], disable_progress_bar=True)
            _ = scraper.play_by_play

    assert scraper.failed_games == [GAME_ID]
    assert any("Unexpected error" in record.getMessage() for record in caplog.records)
