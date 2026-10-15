"""Shared offline fixtures for the chicken_nhl tests."""

import gzip
import json
import os
from types import SimpleNamespace

import pytest

from chickenstats.chicken_nhl import Scraper

MOCK_DATA_DIR = os.path.join(os.path.dirname(__file__), "mock_data")
FIXTURE_GAME_ID = 2023020001


SHOOTOUT_GAME_ID = 2023020125
PLAYOFF_OVERTIME_GAME_ID = 2023030223
LONG_SHOOTOUT_GAME_ID = 2023020411


def scrape_saved_game(game_id: int) -> SimpleNamespace:
    """Scrape one game from the saved responses and return every frame."""
    scraper = Scraper([game_id], disable_progress_bar=True)

    return SimpleNamespace(
        game_id=game_id,
        play_by_play=scraper.play_by_play,
        play_by_play_ext=scraper.play_by_play_ext,
        xg_fields=scraper.xg_fields,
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
def saved_api_response():
    """A function that returns the raw NHL API play-by-play response saved for a game."""

    def load(game_id: int) -> dict:
        path = os.path.join(MOCK_DATA_DIR, "saved", f"gamecenter_{game_id}_play-by-play.gz")
        with gzip.open(path, "rb") as f:
            return json.load(f)

    return load


@pytest.fixture(scope="session")
def fixture_game():
    """Every scraped frame for game 2023020001 (NSH at TBL, TBL won 5-3), built once from the saved responses."""
    return scrape_saved_game(FIXTURE_GAME_ID)


@pytest.fixture(scope="session")
def shootout_game():
    """Every scraped frame for game 2023020125 (VGK at LAK, VGK won 4-3 in a shootout)."""
    return scrape_saved_game(SHOOTOUT_GAME_ID)


@pytest.fixture(scope="session")
def long_shootout_game():
    """Every scraped frame for game 2023020411 (MTL at BUF, MTL won 3-2 in a shootout with five shootout goals)."""
    return scrape_saved_game(LONG_SHOOTOUT_GAME_ID)


@pytest.fixture(scope="session")
def playoff_overtime_game():
    """Every scraped frame for game 2023030223 (NYR at CAR, NYR won 3-2 in overtime)."""
    return scrape_saved_game(PLAYOFF_OVERTIME_GAME_ID)


@pytest.fixture(scope="session")
def fixture_api_response():
    """The raw NHL API play-by-play response for game 2023020001."""
    with open(os.path.join(MOCK_DATA_DIR, "play_by_play.json")) as f:
        return json.load(f)
