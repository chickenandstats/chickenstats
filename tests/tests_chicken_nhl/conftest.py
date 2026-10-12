"""Shared offline fixtures for the chicken_nhl tests."""

import json
import os
from types import SimpleNamespace

import pytest

from chickenstats.chicken_nhl import Scraper

MOCK_DATA_DIR = os.path.join(os.path.dirname(__file__), "mock_data")
FIXTURE_GAME_ID = 2023020001


@pytest.fixture(scope="session")
def fixture_game():
    """Every scraped frame for game 2023020001 (NSH at TBL, TBL won 5-3), built once from the saved responses."""
    scraper = Scraper([FIXTURE_GAME_ID], disable_progress_bar=True)

    return SimpleNamespace(
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


@pytest.fixture(scope="session")
def fixture_api_response():
    """The raw NHL API play-by-play response for game 2023020001."""
    with open(os.path.join(MOCK_DATA_DIR, "play_by_play.json")) as f:
        return json.load(f)
