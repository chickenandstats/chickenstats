"""Tests for behavior that is known to be wrong today.

Each test states what the correct result is and is marked as an expected failure, with the
exception the bug causes. The marks are strict: when a bug is fixed its test passes, pytest
reports that as a failure, and the test moves to test_fixed_bugs.py in the same commit as the
fix. A test that fails with any other exception is reported, so it cannot pass as expected
for the wrong reason.

All of them run on the saved games, with no network access.
"""

import warnings
from unittest.mock import patch

import numpy as np
import polars as pl
import pytest
import requests

from chickenstats.chicken_nhl import (
    Game,
    Scraper,
    Season,
    prep_ind,
    prep_lines,
    prep_stats,
    prep_stints,
    prep_team_stats,
)
from chickenstats.chicken_nhl.identity import identity_columns
from chickenstats.exceptions import InvalidInputError

GAME_ID = 2023020001


def known_bug(reason: str, raises: type[BaseException] | tuple[type[BaseException], ...] = AssertionError):
    """Mark a test as failing today because of a known bug, with the exception that bug causes."""
    return pytest.mark.xfail(strict=True, reason=reason, raises=raises)


# -----------------------------------------------------------------------------
# Scraper state
# -----------------------------------------------------------------------------


def test_access_order_does_not_duplicate_rows():
    scraper = Scraper([GAME_ID], disable_progress_bar=True)
    _ = scraper.rosters
    _ = scraper.html_events
    _ = scraper.shifts
    _ = scraper.play_by_play

    heights = (scraper.rosters.height, scraper.html_events.height, scraper.shifts.height, scraper.changes.height)
    assert heights == (46, 331, 743, 450)


def test_repeated_and_string_game_ids_are_scraped_once():
    scraper = Scraper([GAME_ID, GAME_ID, str(GAME_ID)], disable_progress_bar=True)

    assert scraper.game_ids == [GAME_ID]
    assert scraper.play_by_play.height == 781


@known_bug("add_games leaves the aggregated stats from before the new games")
def test_add_games_clears_the_aggregated_stats():
    scraper = Scraper([GAME_ID], disable_progress_bar=True)
    scraper.prep_stats(level="game", disable_progress_bar=True)
    assert scraper.stats["game_id"].unique().to_list() == [GAME_ID]

    scraper.add_games([2023020125])

    assert sorted(scraper.stats["game_id"].unique().to_list()) == [GAME_ID, 2023020125]


@known_bug("building play_by_play changes the cached line changes")
def test_changes_are_the_same_before_and_after_play_by_play():
    game = Game(GAME_ID)
    keys_before = len(game.changes[0])
    _ = game.play_by_play

    assert len(game.changes[0]) == keys_before


UNKNOWN_LEVEL_CALLS = {
    "Scraper.prep_stats": lambda scraper, level: scraper.prep_stats(level=level, disable_progress_bar=True),
    "Scraper.prep_lines": lambda scraper, level: scraper.prep_lines(level=level, disable_progress_bar=True),
    "Scraper.prep_team_stats": lambda scraper, level: scraper.prep_team_stats(level=level, disable_progress_bar=True),
    "prep_ind": lambda scraper, level: prep_ind(scraper.play_by_play, level=level),
    "prep_stats": lambda scraper, level: prep_stats(scraper.play_by_play, scraper.play_by_play_ext, level=level),
    "prep_lines": lambda scraper, level: prep_lines(scraper.play_by_play, scraper.play_by_play_ext, level=level),
    "prep_team_stats": lambda scraper, level: prep_team_stats(
        scraper.play_by_play, scraper.play_by_play_ext, level=level
    ),
}


@pytest.mark.parametrize("level", ["Game", "seasons", None])
@pytest.mark.parametrize("call", UNKNOWN_LEVEL_CALLS)
@known_bug(
    "an unrecognized level is aggregated as if it were season, or crashes in prep_lines",
    raises=(pytest.fail.Exception, UnboundLocalError),
)
def test_unknown_level_raises(call, level):
    scraper = Scraper([GAME_ID], disable_progress_bar=True)

    with pytest.raises(InvalidInputError):
        UNKNOWN_LEVEL_CALLS[call](scraper, level)


# -----------------------------------------------------------------------------
# Other aggregation
# -----------------------------------------------------------------------------


@known_bug("a stint takes its strength state from its first event, which is relative to that event's team")
def test_stint_strength_state_is_relative_to_the_home_team(fixture_game):
    stints = prep_stints(fixture_game.play_by_play)
    labels = stints.group_by("home_skater_count", "away_skater_count").agg(pl.col("strength_state").n_unique())

    assert labels["strength_state"].max() == 1


@known_bug("the standalone aggregation functions fail on a pandas frame", raises=AttributeError)
def test_standalone_aggregation_accepts_pandas(fixture_game):
    pytest.importorskip("pandas")

    team_stats = prep_team_stats(fixture_game.play_by_play.to_pandas(), level="game", strength_state=False)

    assert len(team_stats) == 2


# -----------------------------------------------------------------------------
# Charts
# -----------------------------------------------------------------------------


def rolling_stats_for_two_teams() -> pl.DataFrame:
    """Three games of a rolling stat for each of two teams."""
    return pl.DataFrame(
        {
            "team": ["NSH", "NSH", "NSH", "TBL", "TBL", "TBL"],
            "game_id": [1, 2, 3, 1, 2, 3],
            "rolling_cf_p60": [50.0, 55.0, 60.0, 40.0, 42.0, 44.0],
        }
    )


@known_bug("plot_rolling_stats joins every team left after filtering into one line")
def test_rolling_chart_draws_one_line_per_team():
    pytest.importorskip("matplotlib")
    pytest.importorskip("seaborn")
    from chickenstats.chicken_nhl.viz import plot_rolling_stats

    ax = plot_rolling_stats(rolling_stats_for_two_teams(), stat="cf_p60")

    assert sorted(np.asarray(line.get_ydata()).tolist() for line in ax.lines) == [
        [40.0, 42.0, 44.0],
        [50.0, 55.0, 60.0],
    ]


@known_bug(
    "plot_rolling_stats ignores a player filter when the frame has no player column", raises=pytest.fail.Exception
)
def test_rolling_chart_rejects_a_filter_it_cannot_apply():
    pytest.importorskip("matplotlib")
    pytest.importorskip("seaborn")
    from chickenstats.chicken_nhl.viz import plot_rolling_stats

    with pytest.raises(InvalidInputError):
        plot_rolling_stats(rolling_stats_for_two_teams(), stat="cf_p60", player="FILIP FORSBERG")


# -----------------------------------------------------------------------------
# Season
# -----------------------------------------------------------------------------


@known_bug("the schedule is cached per team, so a second call with other sessions returns the first result")
def test_schedule_second_call_with_other_sessions():
    season = Season(2023)
    regular_season = season.schedule("NSH", sessions="R", disable_progress_bar=True)
    playoffs = season.schedule("NSH", sessions="P", disable_progress_bar=True)

    assert regular_season.height == 82
    assert playoffs.height == 6
