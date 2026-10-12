"""Shared offline fixtures for the chicken_nhl tests."""

import json
import os
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import requests

from chickenstats.chicken_nhl import Scraper

MOCK_DATA_DIR = os.path.join(os.path.dirname(__file__), "mock_data")
FIXTURE_GAME_ID = 2023020001

REPORT_FILES = {"PL": "events.html", "RO": "rosters.html", "TH": "shifts_home.html", "TV": "shifts_away.html"}


def fixture_session_get(self, url, *args, **kwargs):
    """Serve the saved responses for game 2023020001; every other URL returns a 404."""
    response = requests.Response()
    response.url = url
    response.status_code = 404

    filename = None
    if f"gamecenter/{FIXTURE_GAME_ID}/play-by-play" in url:
        filename = "play_by_play.json"
    else:
        for report, report_file in REPORT_FILES.items():
            if f"/{report}{str(FIXTURE_GAME_ID)[4:]}.HTM" in url:
                filename = report_file

    if filename is not None:
        response.status_code = 200
        with open(os.path.join(MOCK_DATA_DIR, filename), "rb") as f:
            response._content = f.read()

    return response


@pytest.fixture(scope="session")
def fixture_game():
    """Every scraped frame for game 2023020001 (NSH at TBL, TBL won 5-3), built once with no network access."""
    with patch("requests.Session.get", fixture_session_get):
        scraper = Scraper([FIXTURE_GAME_ID], disable_progress_bar=True)
        frames = SimpleNamespace(
            play_by_play=scraper.play_by_play,
            play_by_play_ext=scraper.play_by_play_ext,
            rosters=scraper.rosters,
            html_rosters=scraper.html_rosters,
            api_rosters=scraper.api_rosters,
            shifts=scraper.shifts,
            changes=scraper.changes,
            html_events=scraper.html_events,
            api_events=scraper.api_events,
            failed_games=scraper.failed_games,
        )
    return frames


@pytest.fixture(scope="session")
def fixture_api_response():
    """The raw NHL API play-by-play response for game 2023020001."""
    with open(os.path.join(MOCK_DATA_DIR, "play_by_play.json")) as f:
        return json.load(f)
