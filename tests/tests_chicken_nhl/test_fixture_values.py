"""Value tests against the offline fixture for game 2023020001 (NSH at TBL, TBL won 5-3).

The expected numbers come from the NHL API response and HTML reports saved in ``mock_data``.
Where a number can be read from the raw API response it is compared with that response; the
rest are pinned. A deliberate change to an output should update the number here in the same commit.
"""

import json
import os
from unittest.mock import patch

import polars as pl
import pytest

from chickenstats.chicken_nhl import Scraper, prep_ind, prep_lines, prep_oi, prep_stats, prep_stints, prep_team_stats

from test_live_games import LIVE_DATA_DIR, snapshot_session_get

# How one team's strength state reads from the other bench
MIRRORED_STATE = {"5v5": "5v5", "5v4": "4v5", "4v5": "5v4", "5vE": "Ev5", "Ev5": "5vE"}


# -----------------------------------------------------------------------------
# Scraped frames
# -----------------------------------------------------------------------------


def test_frame_heights(fixture_game):
    """Each scraped frame has the number of rows the reports contain."""
    assert fixture_game.failed_games == []
    assert fixture_game.play_by_play.height == 781
    assert fixture_game.play_by_play_ext.height == 781
    assert fixture_game.rosters.height == 46
    assert fixture_game.html_rosters.height == 46
    assert fixture_game.api_rosters.height == 40
    assert fixture_game.shifts.height == 743
    assert fixture_game.changes.height == 450
    assert fixture_game.html_events.height == 331
    assert fixture_game.api_events.height == 328

    assert dict(fixture_game.rosters["status"].value_counts().rows()) == {"ACTIVE": 40, "SCRATCH": 6}
    assert dict(fixture_game.shifts["team"].value_counts().rows()) == {"TBL": 359, "NSH": 384}


def test_final_score_and_shots_match_api_box_score(fixture_game, fixture_api_response):
    """The running score and the shots on goal agree with the box score in the API response."""
    play_by_play = fixture_game.play_by_play
    home, away = fixture_api_response["homeTeam"], fixture_api_response["awayTeam"]

    assert (home["abbrev"], home["score"], home["sog"]) == ("TBL", 5, 34)
    assert (away["abbrev"], away["score"], away["sog"]) == ("NSH", 3, 31)

    last_event = play_by_play.tail(1)
    assert last_event["event"].item() == "GEND"
    assert (last_event["home_score"].item(), last_event["away_score"].item()) == (home["score"], away["score"])

    totals = {
        row["event_team"]: row
        for row in play_by_play.group_by("event_team").agg(pl.col("goal", "shot").sum()).to_dicts()
    }
    assert (totals["TBL"]["goal"], totals["TBL"]["shot"]) == (home["score"], home["sog"])
    assert (totals["NSH"]["goal"], totals["NSH"]["shot"]) == (away["score"], away["sog"])


def test_goal_scorers_and_assists_match_api(fixture_game, fixture_api_response):
    """Every goal row carries the scorer and assists the API response lists, in order."""
    goals = (
        fixture_game.play_by_play.filter(pl.col("event") == "GOAL")
        .select("player_1_api_id", "player_2_api_id", "player_3_api_id")
        .rows()
    )
    expected = [
        (
            play["details"].get("scoringPlayerId"),
            play["details"].get("assist1PlayerId"),
            play["details"].get("assist2PlayerId"),
        )
        for play in fixture_api_response["plays"]
        if play["typeDescKey"] == "goal"
    ]

    assert goals == expected
    assert goals[0] == (8476453, 8475167, 8478010)  # Kucherov from Hedman and Point
    assert goals[4] == (8479542, None, None)  # Hagel, penalty shot


def test_event_counts(fixture_game):
    """The play-by-play has the number of each event the reports list."""
    counts = dict(fixture_game.play_by_play["event"].value_counts().rows())
    expected = {
        "GOAL": 8,
        "SHOT": 57,
        "MISS": 31,
        "BLOCK": 30,
        "FAC": 60,
        "HIT": 45,
        "GIVE": 15,
        "TAKE": 18,
        "PENL": 10,
        "CHANGE": 450,
        "STOP": 44,
    }

    assert {event: counts[event] for event in expected} == expected


def test_event_lengths_cover_the_game(fixture_game):
    """Event lengths add up to 60 minutes, split across the strength states."""
    play_by_play = fixture_game.play_by_play
    seconds = dict(play_by_play.group_by("strength_state").agg(pl.col("event_length").sum()).rows())

    assert play_by_play["event_length"].sum() == 3600
    assert {state: seconds[state] for state in ("5v5", "5v4", "4v5", "Ev5", "5vE")} == {
        "5v5": 2478,
        "5v4": 529,
        "4v5": 429,
        "Ev5": 105,
        "5vE": 59,
    }


# -----------------------------------------------------------------------------
# Aggregations without a strength split
# -----------------------------------------------------------------------------


def test_player_toi_matches_shift_report(fixture_game):
    """A player's time on ice equals the sum of their shifts in the HTML shift reports."""
    stats = prep_stats(
        fixture_game.play_by_play, fixture_game.play_by_play_ext, level="game", strength_state=False
    ).select("api_id", "toi")
    shift_toi = fixture_game.shifts.group_by("api_id").agg((pl.col("duration_seconds").sum() / 60).alias("shift_toi"))
    joined = stats.join(shift_toi, on="api_id", how="full")

    assert joined.height == 38
    assert joined.null_count().sum_horizontal().item() == 0
    assert joined.select(((pl.col("toi") - pl.col("shift_toi")).abs() < 1e-9).all()).item()
    assert dict(stats.rows())[8475167] == pytest.approx(26 + 55 / 60)  # Hedman, 26:55


def test_individual_totals(fixture_game):
    """Individual stats add up to the event counts in the play-by-play."""
    ind_stats = prep_ind(fixture_game.play_by_play, level="game", strength_state=False)
    columns = ["g", "a1", "a2", "isf", "iff", "icf", "ihf", "iht", "igive", "itake", "ifow", "ifol"]

    assert ind_stats.select(pl.col(columns).sum()).row(0) == (8, 7, 6, 65, 96, 126, 45, 45, 15, 18, 60, 60)

    nick_paul = ind_stats.filter(pl.col("api_id") == 8477426).select("g", "a1", "a2", "isf", "iff", "icf").row(0)
    assert nick_paul == (2, 1, 0, 3, 5, 6)


def test_team_stats_game_rows(fixture_game):
    """Each team's game row matches the box score, and the two rows mirror each other."""
    team_stats = prep_team_stats(
        fixture_game.play_by_play, fixture_game.play_by_play_ext, level="game", strength_state=False
    )
    tbl = team_stats.filter(pl.col("team") == "TBL").to_dicts()[0]
    nsh = team_stats.filter(pl.col("team") == "NSH").to_dicts()[0]

    assert (tbl["toi"], nsh["toi"]) == (60.0, 60.0)

    for_against = ["gf", "ga", "sf", "sa", "ff", "fa", "cf", "ca"]
    assert [tbl[column] for column in for_against] == [5, 3, 34, 31, 50, 46, 61, 65]
    assert [nsh[column] for column in for_against] == [3, 5, 31, 34, 46, 50, 65, 61]

    assert [tbl[column] for column in ("bsf", "bsa", "hf", "ht", "fow", "fol")] == [10, 17, 22, 23, 34, 26]
    assert tbl["gf_percent"] == pytest.approx(5 / 8)
    assert tbl["cf_percent"] == pytest.approx(61 / 126)


def test_stints_cover_the_game(fixture_game):
    """Stints add up to 60 minutes and to the box score."""
    stints = prep_stints(fixture_game.play_by_play)
    columns = ["home_gf", "away_gf", "home_sf", "away_sf", "home_cf", "away_cf"]

    assert stints.height == 385
    assert stints["toi"].sum() == 3600
    assert stints.select(pl.col(columns).sum()).row(0) == (5, 3, 34, 31, 61, 65)


# -----------------------------------------------------------------------------
# Aggregations with the default strength split
# -----------------------------------------------------------------------------


def test_team_stats_by_strength_match_play_by_play(fixture_game):
    """Each team's goals and shots in a strength state equal the play-by-play rows for that state."""
    team_stats = prep_team_stats(fixture_game.play_by_play, fixture_game.play_by_play_ext)
    direct = {
        (row["event_team"], row["strength_state"]): (row["goal"], row["shot"])
        for row in fixture_game.play_by_play.group_by("event_team", "strength_state")
        .agg(pl.col("goal", "shot").sum())
        .to_dicts()
    }

    assert team_stats.height == 8
    for row in team_stats.to_dicts():
        assert (row["gf"], row["sf"]) == direct[(row["team"], row["strength_state"])]

    five_on_five = team_stats.filter(pl.col("team") == "TBL", pl.col("strength_state") == "5v5").to_dicts()[0]
    assert five_on_five["toi"] == pytest.approx(2478 / 60)
    assert [five_on_five[column] for column in ("gf", "ga", "sf", "sa", "cf", "ca")] == [1, 2, 22, 18, 42, 38]


def test_team_toi_by_strength_mirrors(fixture_game):
    """A team's ice time in a strength state equals the other team's in the mirrored state."""
    team_stats = prep_team_stats(fixture_game.play_by_play, fixture_game.play_by_play_ext)
    toi = {(row["team"], row["strength_state"]): row["toi"] for row in team_stats.to_dicts()}
    other_team = {"TBL": "NSH", "NSH": "TBL"}

    for (team, state), minutes in toi.items():
        assert minutes == pytest.approx(toi[(other_team[team], MIRRORED_STATE[state])])


def test_player_stats_by_strength(fixture_game):
    """A player's default rows split their ice time and events by strength state."""
    stats = prep_stats(fixture_game.play_by_play, fixture_game.play_by_play_ext)
    nick_paul = {
        row["strength_state"]: row
        for row in stats.filter(pl.col("api_id") == 8477426)
        .select("strength_state", "toi", "g", "isf", "gf")
        .to_dicts()
    }

    assert sorted(nick_paul) == ["4v5", "5v4", "5v5", "5vE"]
    assert nick_paul["5v4"]["toi"] == pytest.approx(6.5)
    assert (nick_paul["5v4"]["g"], nick_paul["5v4"]["isf"], nick_paul["5v4"]["gf"]) == (2, 3, 2)
    assert nick_paul["5v5"]["toi"] == pytest.approx(497 / 60)
    assert (nick_paul["5vE"]["g"], nick_paul["5vE"]["gf"]) == (0, 1)


# -----------------------------------------------------------------------------
# Row order
# -----------------------------------------------------------------------------

ORDER_CASES = [
    ("prep_ind", {}),
    ("prep_ind", {"level": "period", "score": True}),
    ("prep_ind", {"teammates": True, "opposition": True}),
    ("prep_oi", {}),
    ("prep_oi", {"level": "period", "score": True}),
    ("prep_oi", {"teammates": True, "opposition": True}),
    ("prep_stats", {}),
    ("prep_stats", {"level": "period", "score": True}),
    ("prep_stats", {"teammates": True, "opposition": True}),
    ("prep_lines", {}),
    ("prep_lines", {"level": "period", "score": True}),
    ("prep_lines", {"teammates": True, "opposition": True}),
    ("prep_team_stats", {}),
    ("prep_team_stats", {"level": "period", "score": True, "opposition": True}),
]


@pytest.mark.parametrize("function, arguments", ORDER_CASES)
def test_rows_come_back_in_the_same_order(fixture_game, function, arguments):
    """Calling an aggregation twice on the same play-by-play gives the same rows in the same order."""
    if function == "prep_ind":
        frames = [prep_ind(fixture_game.play_by_play, **arguments) for _ in range(3)]
    else:
        aggregate = {
            "prep_oi": prep_oi,
            "prep_stats": prep_stats,
            "prep_lines": prep_lines,
            "prep_team_stats": prep_team_stats,
        }[function]
        frames = [aggregate(fixture_game.play_by_play, fixture_game.play_by_play_ext, **arguments) for _ in range(3)]

    assert frames[1].equals(frames[0])
    assert frames[2].equals(frames[0])


# -----------------------------------------------------------------------------
# Game in progress
# -----------------------------------------------------------------------------


def test_live_snapshot_values():
    """A snapshot taken in the third period matches the score, shots and clock in its API response."""
    snapshot = "2026020052_LIVE_P3"
    with open(os.path.join(LIVE_DATA_DIR, snapshot, "play_by_play.json")) as f:
        api_response = json.load(f)

    with patch("requests.Session.get", snapshot_session_get(snapshot)):
        scraper = Scraper([2026020052], disable_progress_bar=True)
        play_by_play = scraper.play_by_play

    assert scraper.failed_games == []
    assert play_by_play.height == 638

    last_event = play_by_play.tail(1)
    assert api_response["clock"]["secondsRemaining"] == 803
    assert (last_event["period"].item(), last_event["period_seconds"].item()) == (3, 1200 - 803)
    assert (last_event["home_score"].item(), last_event["away_score"].item()) == (1, 1)
    assert (api_response["homeTeam"]["score"], api_response["awayTeam"]["score"]) == (1, 1)

    shots = dict(play_by_play.group_by("event_team").agg(pl.col("shot").sum()).drop_nulls().rows())
    assert (shots["LAK"], shots["FLA"]) == (api_response["homeTeam"]["sog"], api_response["awayTeam"]["sog"])
    assert (shots["LAK"], shots["FLA"]) == (18, 24)
    assert play_by_play["event_length"].sum() == 2797
