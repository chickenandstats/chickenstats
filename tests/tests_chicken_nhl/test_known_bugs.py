"""Tests for behavior that is known to be wrong today.

Each test states what the correct result is and is marked as an expected failure. The marks
are strict: when a bug is fixed its test passes, pytest reports that as a failure, and the
mark is removed in the same commit as the fix.

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


def known_bug(reason: str):
    """Mark a test as failing today because of a known bug."""
    return pytest.mark.xfail(strict=True, reason=reason)


# -----------------------------------------------------------------------------
# Shootouts
# -----------------------------------------------------------------------------


@known_bug("shootout attempts are counted as shots on goal when stats are not split by strength state")
def test_team_shots_leave_out_shootout_attempts(shootout_game, saved_api_response):
    api_response = saved_api_response(shootout_game.game_id)
    team_stats = prep_team_stats(
        shootout_game.play_by_play, shootout_game.play_by_play_ext, level="game", strength_state=False
    )
    shots = dict(team_stats.select("team", "sf").rows())

    assert shots == {"LAK": api_response["homeTeam"]["sog"], "VGK": api_response["awayTeam"]["sog"]}


@known_bug("a shootout attempt is credited to the shooter as an individual shot")
def test_individual_shots_leave_out_shootout_attempts(shootout_game):
    play_by_play = shootout_game.play_by_play
    ind_stats = prep_ind(play_by_play, level="game", strength_state=False)

    assert ind_stats["isf"].sum() == play_by_play.filter(pl.col("period") <= 4)["shot"].sum()


@known_bug("shots differ between the split and unsplit aggregations because of shootout attempts")
def test_team_shots_are_the_same_with_and_without_a_strength_split(shootout_game):
    play_by_play, play_by_play_ext = shootout_game.play_by_play, shootout_game.play_by_play_ext
    unsplit = prep_team_stats(play_by_play, play_by_play_ext, level="game", strength_state=False)
    split = prep_team_stats(play_by_play, play_by_play_ext, level="game")

    assert dict(split.group_by("team").agg(pl.col("sf").sum()).rows()) == dict(unsplit.select("team", "sf").rows())


# -----------------------------------------------------------------------------
# Events lost or misplaced by a strength split
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("teammates", [False, True])
@known_bug("a penalty-shot goal has no ice time, and rows with no ice time are dropped")
def test_prep_stats_keeps_every_individual_event(fixture_game, teammates):
    stats = prep_stats(fixture_game.play_by_play, fixture_game.play_by_play_ext, level="game", teammates=teammates)

    assert stats.select(pl.col("g", "a1", "isf", "iff", "icf").sum()).row(0) == (8, 7, 65, 96, 126)


@known_bug("the penalty-shot goal is dropped from team stats when they are split by strength state")
def test_team_goals_by_strength_add_up_to_the_final_score(fixture_game):
    team_stats = prep_team_stats(fixture_game.play_by_play, fixture_game.play_by_play_ext)

    assert dict(team_stats.group_by("team").agg(pl.col("gf").sum()).rows()) == {"TBL": 5, "NSH": 3}


@known_bug("a penalty shot is 1v0 for the shooter's team but 5v5 for the other team")
def test_penalty_shot_has_the_mirrored_state_for_the_other_team(fixture_game):
    penalty_shot = fixture_game.play_by_play.filter(pl.col("strength_state") == "1v0")

    assert penalty_shot["opp_strength_state"].to_list() == ["0v1"]


@known_bug("the penalty-shot goal is counted against the other team at 5v5")
def test_team_for_and_against_mirror_by_strength_state(fixture_game):
    team_stats = prep_team_stats(fixture_game.play_by_play, fixture_game.play_by_play_ext)
    rows = {(row["team"], row["strength_state"]): row for row in team_stats.to_dicts()}
    mirrored_state = {"5v5": "5v5", "5v4": "4v5", "4v5": "5v4", "5vE": "Ev5", "Ev5": "5vE"}
    other_team = {"TBL": "NSH", "NSH": "TBL"}

    for (team, state), row in rows.items():
        mirrored = rows[(other_team[team], mirrored_state[state])]
        for stat in ("g", "s", "f", "c"):
            assert row[f"{stat}f"] == mirrored[f"{stat}a"], (team, state, stat)


@known_bug("zone starts are dropped for players with no ice time in a split, and for some name spellings")
def test_zone_starts_are_all_counted(fixture_game):
    play_by_play, play_by_play_ext = fixture_game.play_by_play, fixture_game.play_by_play_ext
    changes = play_by_play.filter(pl.col("event") == "CHANGE")
    expected = dict(changes.group_by("zone_start").agg(pl.col("change_on_count").sum()).rows())

    for strength_state in (False, True):
        stats = prep_stats(play_by_play, play_by_play_ext, level="game", strength_state=strength_state)
        totals = stats.select(pl.col("ozs", "nzs", "dzs", "otf").sum()).row(0)
        assert totals == (expected["OFF"], expected["NEU"], expected["DEF"], expected["OTF"]), strength_state


# -----------------------------------------------------------------------------
# Faceoff zones
# -----------------------------------------------------------------------------


@known_bug("a lost faceoff is counted in the winner's zone, so offensive and defensive zone faceoffs are swapped")
def test_zone_faceoffs_mirror_between_the_teams(fixture_game):
    team_stats = prep_team_stats(
        fixture_game.play_by_play, fixture_game.play_by_play_ext, level="game", strength_state=False
    )
    tbl = team_stats.filter(pl.col("team") == "TBL").to_dicts()[0]
    nsh = team_stats.filter(pl.col("team") == "NSH").to_dicts()[0]

    assert (tbl["ozf"], tbl["dzf"]) == (nsh["dzf"], nsh["ozf"])
    assert (tbl["ozf"], tbl["dzf"]) == (24, 19)


@known_bug("individual faceoff losses are counted in the winner's zone")
def test_individual_zone_faceoff_losses(fixture_game):
    ind_stats = prep_ind(fixture_game.play_by_play, level="game", strength_state=False)

    # NSH lost 12 draws in its offensive zone, which are TBL's 12 defensive-zone wins; TBL lost 9
    assert dict(ind_stats.group_by("team").agg(pl.col("iozfl").sum()).rows()) == {"NSH": 12, "TBL": 9}


# -----------------------------------------------------------------------------
# Scraper state
# -----------------------------------------------------------------------------


@known_bug("scraping one data type after another appends the same game's rows twice")
def test_access_order_does_not_duplicate_rows():
    scraper = Scraper([GAME_ID], disable_progress_bar=True)
    _ = scraper.rosters
    _ = scraper.html_events
    _ = scraper.shifts
    _ = scraper.play_by_play

    stored = (scraper._rosters, scraper._html_events, scraper._shifts, scraper._changes)
    assert tuple(sum(frame.height for frame in frames) for frames in stored) == (46, 331, 743, 450)


@known_bug("game IDs are not normalized, so a repeated or string ID is scraped again")
def test_repeated_and_string_game_ids_are_scraped_once():
    scraper = Scraper([GAME_ID, GAME_ID, str(GAME_ID)], disable_progress_bar=True)

    assert scraper.game_ids == [GAME_ID]
    assert scraper.play_by_play.height == 781


@known_bug("add_games leaves the aggregated stats from before the new games")
def test_add_games_clears_the_aggregated_stats():
    scraper = Scraper([GAME_ID], disable_progress_bar=True)
    scraper.prep_stats(disable_progress_bar=True)
    assert scraper._stats.height == 120

    scraper.add_games([2023020125])

    assert scraper._stats.is_empty()


@known_bug("building play_by_play changes the cached line changes")
def test_changes_are_the_same_before_and_after_play_by_play():
    game = Game(GAME_ID)
    keys_before = len(game.changes[0])
    _ = game.play_by_play

    assert len(game.changes[0]) == keys_before


@pytest.mark.parametrize("level", ["Game", "seasons", None])
@known_bug("an unrecognized level is aggregated as if it were season")
def test_unknown_level_raises(level):
    scraper = Scraper([GAME_ID], disable_progress_bar=True)

    with pytest.raises(InvalidInputError):
        scraper.prep_stats(level=level, disable_progress_bar=True)


@known_bug("a failed shift report for one team is ignored and the game is returned without it")
def test_a_failed_shift_report_fails_the_game():
    served_get = requests.Session.get

    def away_shifts_down(self, url, *args, **kwargs):
        if "/TV020001.HTM" in str(url):
            raise requests.exceptions.ConnectionError("down")
        return served_get(self, url, *args, **kwargs)

    with patch.object(requests.Session, "get", away_shifts_down), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        scraper = Scraper([GAME_ID], disable_progress_bar=True)
        play_by_play = scraper.play_by_play

    assert scraper.failed_games == [GAME_ID]
    assert play_by_play.height == 0


# -----------------------------------------------------------------------------
# Names and identity
# -----------------------------------------------------------------------------


@known_bug("API roster names skip a spelling correction that HTML names get, so one player has two names")
def test_a_player_has_one_name_across_sources(fixture_game):
    alex_carrier = 8478851
    names = set(fixture_game.rosters.filter(pl.col("api_id") == alex_carrier)["player_name"]) | set(
        fixture_game.shifts.filter(pl.col("api_id") == alex_carrier)["player_name"]
    )

    assert len(names) == 1
    assert set(fixture_game.html_rosters["eh_id"]) == set(fixture_game.rosters["eh_id"])


@known_bug("identity_columns for lines leaves out the line itself unless teammates is on")
def test_identity_columns_is_a_key_of_lines(fixture_game):
    lines = prep_lines(
        fixture_game.play_by_play,
        fixture_game.play_by_play_ext,
        level="game",
        strength_state=True,
        score=False,
        teammates=False,
        opposition=False,
    )
    key = identity_columns("lines", level="game", strength_state=True, score=False, teammates=False, opposition=False)

    assert set(key) <= set(lines.columns)
    assert lines.select(key).n_unique() == lines.height


@known_bug("identity_columns for team stats names opp_team at season level, where the frame has no such column")
def test_identity_columns_is_a_key_of_season_team_stats(fixture_game):
    team_stats = prep_team_stats(
        fixture_game.play_by_play,
        fixture_game.play_by_play_ext,
        level="season",
        strength_state=True,
        score=False,
        opposition=False,
    )
    key = identity_columns("team_stats", level="season", strength_state=True, score=False, opposition=False)

    assert set(key) <= set(team_stats.columns)
    assert team_stats.select(key).n_unique() == team_stats.height


# -----------------------------------------------------------------------------
# Other aggregation
# -----------------------------------------------------------------------------


@known_bug("xG columns are reported as 0.0 when the play-by-play has no predicted goals")
def test_no_xg_columns_without_predicted_goals(fixture_game):
    assert "pred_goal" not in fixture_game.play_by_play.columns

    stats = prep_stats(fixture_game.play_by_play, fixture_game.play_by_play_ext)

    assert [column for column in stats.columns if "xg" in column] == []


@known_bug("a stint takes its strength state from its first event, which is relative to that event's team")
def test_stint_strength_state_is_relative_to_the_home_team(fixture_game):
    stints = prep_stints(fixture_game.play_by_play)
    labels = stints.group_by("home_skater_count", "away_skater_count").agg(pl.col("strength_state").n_unique())

    assert labels["strength_state"].max() == 1


@known_bug("a blocked power-play shot is score-adjusted with the blocking team's weight")
def test_blocked_power_play_shot_uses_the_shooting_teams_weight(fixture_game):
    play_by_play = fixture_game.play_by_play.filter(pl.col("home_score_diff") == 1)
    shot = play_by_play.filter(
        (pl.col("event_team") == "TBL") & (pl.col("strength_state") == "5v4") & (pl.col("event") == "SHOT")
    )["corsi_adj"][0]
    block = play_by_play.filter(
        (pl.col("event_team") == "NSH") & (pl.col("strength_state") == "4v5") & (pl.col("event") == "BLOCK")
    )["corsi_adj"][0]

    assert block == pytest.approx(shot)


@known_bug("count columns stored as floats are replaced with 0 during validation")
def test_float_count_columns_give_the_same_totals(fixture_game):
    play_by_play = fixture_game.play_by_play.with_columns(pl.col("goal", "shot", "fenwick", "hit").cast(pl.Float64))
    ind_stats = prep_ind(play_by_play, level="game", strength_state=False)

    assert ind_stats.select(pl.col("g", "isf", "iff", "ihf").sum()).row(0) == (8, 65, 96, 45)


@known_bug("a shot with no description is dropped from individual stats")
def test_shots_without_a_description_are_counted(fixture_game):
    play_by_play = fixture_game.play_by_play.with_columns(
        pl.when(pl.col("event") == "SHOT").then(None).otherwise(pl.col("description")).alias("description")
    )

    assert prep_ind(play_by_play, level="game", strength_state=False)["isf"].sum() == 65


@known_bug("the standalone aggregation functions fail on a pandas frame")
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


@known_bug("plot_rolling_stats ignores a player filter when the frame has no player column")
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
