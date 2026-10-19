"""What a failed HTML report must do: record the game as failed, warn, and come back on a retry.

Each report the scraper fetches for a game (play-by-play PL, the two shift reports TH and TV, and the
rosters RO) is failed four ways through the shared network guard: a connection error, a 404, a 500, and
a report cut off before its end.
Each must record the game in failed_games with a warning that names the report, and Scraper.refresh must
recover it once the report can be read.
"""

import logging
import warnings
from unittest.mock import patch

import pytest
import requests

from chickenstats.chicken_nhl import Scraper

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
def test_a_retry_recovers_a_failed_game(caplog, report, failure):
    """Once the report can be read, refresh() fetches the game again and it leaves failed_games."""
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


def test_a_refresh_that_fails_again_keeps_the_earlier_rows(caplog):
    """A game whose report fails on refresh keeps the rows it had, and is listed as failed."""
    scraper = Scraper([GAME_ID], disable_progress_bar=True)
    before = scraper.play_by_play

    with patch.object(requests.Session, "get", failing("PL", 500)), pytest.warns(UserWarning, match="Failed"):
        scraper.refresh([GAME_ID])

    assert scraper.failed_games == [GAME_ID]
    assert scraper.play_by_play.equals(before)


def test_refresh_with_no_games_fetches_only_those_not_final(caplog):
    """refresh() fetches failed and provisional games, and leaves a final game alone."""
    with patch.object(requests.Session, "get", failing("PL", 404)), pytest.warns(UserWarning):
        scraper = Scraper([GAME_ID, 2023020015], disable_progress_bar=True)
        _ = scraper.play_by_play
    assert scraper.failed_games == [GAME_ID]

    served_get = requests.Session.get
    requested: list[str] = []

    def recording_get(self, url, *args, **kwargs):
        requested.append(str(url))
        return served_get(self, url, *args, **kwargs)

    with patch.object(requests.Session, "get", recording_get):
        scraper.refresh()

    assert scraper.failed_games == []
    assert requested and not any("2023020015" in url or "020015.HTM" in url for url in requested)
    assert scraper.play_by_play["game_id"].unique().sort().to_list() == [GAME_ID, 2023020015]


def test_refresh_rejects_a_game_not_in_the_scraper():
    from chickenstats.exceptions import InvalidInputError

    scraper = Scraper([GAME_ID], disable_progress_bar=True)

    with pytest.raises(InvalidInputError, match="not in this Scraper"):
        scraper.refresh([2023020015])
