from pathlib import Path
from typing import Any, cast

import numpy as np
import pytest

try:
    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    pd = None  # type: ignore[assignment]  # ty: ignore[invalid-assignment]
    HAS_PANDAS = False

import polars as pl

from chickenstats.chicken_nhl._game_utils import (
    _return_name_html as return_name_html,
    add_pred_goal_adj,
    calculate_score_adjustment,
    handle_penalty_details,
    load_score_adjustments,
)
from chickenstats.utilities.utilities import convert_to_list
from chickenstats.exceptions import InvalidInputError
from chickenstats.utilities.utilities import charts_directory, data_directory, norm_coords


@pytest.fixture(scope="package")
def test_pbp():
    filepath = Path("./tests/tests_chicken_nhl/data/test_pbp.csv")

    if not HAS_PANDAS:
        pytest.skip("pandas not installed")

    test_pbp = pd.read_csv(filepath, low_memory=False)

    return test_pbp


def test_norm_coords(test_pbp, norm_column="event_team", norm_value="NSH"):
    data = norm_coords(data=test_pbp, normalization_column=norm_column, normalization_value=norm_value)

    assert {"norm_coords_x", "norm_coords_y"} <= set(data.columns)
    assert data["norm_coords_x"].notna().any()
    assert data["norm_coords_y"].notna().any()


@pytest.mark.parametrize(
    "test_list",
    [
        2023020001,
        2023020001.0,
        "2023020001",
        (2023020001, 2023020002),
        np.array([2023020001]),
        *([pd.Series([2023020001])] if HAS_PANDAS else []),
    ],
)
def test_convert_to_list(test_list):
    test_list = convert_to_list(test_list, "GAME ID")

    assert isinstance(test_list, list) is True


def test_convert_to_list_fail():
    with pytest.raises(InvalidInputError):
        convert_to_list(cast(Any, {"test_key": "test_value"}), "TEST")


def test_return_name_html():
    name = return_name_html("GOALIE - PEKKA RINNE")

    assert name == "PEKKA RINNE"


def test_return_name_html_no_hyphen_no_crash():
    """Malformed HTML title text with no hyphen must not raise ValueError.

    Regression test: `.index("-")` used to raise uncaught ValueError for such input,
    which propagated past the caller's `except KeyError` in hs_strip_html and aborted
    parsing of the entire game's roster instead of degrading one player's name.
    """
    name = return_name_html("PEKKA RINNE")
    assert name == "PEKKA RINNE"


# -----------------------------------------------------------------------------
# calculate_score_adjustment
# -----------------------------------------------------------------------------


@pytest.fixture(scope="module")
def score_adjustments():
    return load_score_adjustments()


def _base_play(**overrides) -> dict:
    """Minimal play dict with all keys required by calculate_score_adjustment."""
    play = {
        "event": "SHOT",
        "event_team": "NSH",
        "opp_team": "CHI",
        "home_team": "NSH",
        "home_score_diff": 0,
        "strength_state": "5v5",
        "opp_strength_state": "5v5",
        "teammate_block": 0,
        "goal": 0,
        "pred_goal": 0.1,
        "shot": 1,
        "miss": 0,
        "block": 0,
        "fenwick": 1,
        "corsi": 1,
    }
    play.update(overrides)
    return play


def test_calculate_score_adjustment_shot_home(score_adjustments):
    """Home team SHOT produces adjusted columns."""
    play = _base_play()
    result = calculate_score_adjustment(play, score_adjustments)
    assert "shot_adj" in result
    assert result["shot_adj"] > 0


def test_calculate_score_adjustment_block_opponent(score_adjustments):
    """BLOCK with teammate_block=0 uses opp_team as the event_team (line 49 path)."""
    play = _base_play(event="BLOCK", event_team="NSH", opp_team="CHI", teammate_block=0, block=1, corsi=1)
    result = calculate_score_adjustment(play, score_adjustments)
    assert "block_adj" in result


def test_calculate_score_adjustment_blocked_power_play_shot(score_adjustments):
    """A blocked power-play shot gets the shooting team's power-play weight, like the same shot unblocked."""
    shot = _base_play(event="SHOT", event_team="NSH", opp_team="CHI", strength_state="5v4", opp_strength_state="4v5")
    block = _base_play(
        event="BLOCK",
        event_team="CHI",
        opp_team="NSH",
        strength_state="4v5",
        opp_strength_state="5v4",
        shot=0,
        fenwick=0,
        block=1,
    )

    shot_adj = calculate_score_adjustment(shot, score_adjustments)["corsi_adj"]
    block_adj = calculate_score_adjustment(block, score_adjustments)["corsi_adj"]

    assert block_adj == pytest.approx(shot_adj)


def _xg_rows(*rows: dict) -> pl.DataFrame:
    """Shot rows for add_pred_goal_adj, NSH at home against CHI, overriding the columns each case needs."""
    base = {
        "event": "SHOT",
        "event_team": "NSH",
        "opp_team": "CHI",
        "home_team": "NSH",
        "home_score_diff": 0,
        "strength_state": "5v5",
        "opp_strength_state": "5v5",
        "teammate_block": 0,
        "shootout_attempt": 0,
        "pred_goal": 0.2,
    }
    return pl.DataFrame([{**base, **row} for row in rows])


def test_add_pred_goal_adj_uses_the_score_adjustment_rules(score_adjustments):
    """The pred_goal weight is the shooting team's, mirrored on the penalty kill, and 1.0 outside the table."""
    df = _xg_rows(
        {},
        {"event_team": "CHI", "opp_team": "NSH", "home_score_diff": 5},
        {"strength_state": "4v5", "opp_strength_state": "5v4", "home_score_diff": -1},
        {
            "event": "BLOCK",
            "event_team": "CHI",
            "opp_team": "NSH",
            "strength_state": "4v5",
            "opp_strength_state": "5v4",
        },
        {"event": "GOAL", "strength_state": "1v0", "opp_strength_state": "0v1"},
    )
    expected = [
        score_adjustments["5v5"][0]["home_pred_goal_weight"],
        score_adjustments["5v5"][3]["away_pred_goal_weight"],
        score_adjustments["5v4"][-1]["away_pred_goal_weight"],
        score_adjustments["5v4"][0]["home_pred_goal_weight"],
        1.0,
    ]

    result = add_pred_goal_adj(df)

    assert result["pred_goal_adj"].to_list() == pytest.approx([0.2 * weight for weight in expected])


def test_add_pred_goal_adj_gives_shootout_attempts_no_xg():
    result = add_pred_goal_adj(_xg_rows({"shootout_attempt": 1, "strength_state": "1v0"}))

    assert result.select("pred_goal", "pred_goal_adj").row(0) == (0.0, 0.0)


def test_add_pred_goal_adj_keeps_a_supplied_adjustment():
    result = add_pred_goal_adj(_xg_rows({}).with_columns(pred_goal_adj=pl.lit(0.5)))

    assert result["pred_goal_adj"].to_list() == [0.5]


def test_add_pred_goal_adj_does_nothing_without_pred_goal():
    df = _xg_rows({}).drop("pred_goal")

    assert add_pred_goal_adj(df).columns == df.columns


def test_calculate_score_adjustment_block_teammate(score_adjustments):
    """BLOCK with teammate_block=1 uses event_team directly."""
    play = _base_play(event="BLOCK", event_team="NSH", opp_team="CHI", teammate_block=1, block=1, corsi=1)
    result = calculate_score_adjustment(play, score_adjustments)
    assert "block_adj" in result


def test_calculate_score_adjustment_miss(score_adjustments):
    """MISS event still uses fenwick weight column."""
    play = _base_play(event="MISS", shot=0, miss=1, fenwick=1)
    result = calculate_score_adjustment(play, score_adjustments)
    assert "miss_adj" in result


def test_calculate_score_adjustment_score_diff_clamped_low(score_adjustments):
    """home_score_diff below -3 is clamped to -3."""
    play = _base_play(home_score_diff=-5)
    result = calculate_score_adjustment(play, score_adjustments)
    assert "shot_adj" in result


def test_calculate_score_adjustment_score_diff_clamped_high(score_adjustments):
    """home_score_diff above +3 is clamped to +3."""
    play = _base_play(home_score_diff=5)
    result = calculate_score_adjustment(play, score_adjustments)
    assert "shot_adj" in result


def test_calculate_score_adjustment_empty_net_no_division(score_adjustments):
    """Strength state with 'E' uses identity weight (multiplied by 1)."""
    play = _base_play(strength_state="5v4E")
    result = calculate_score_adjustment(play, score_adjustments)
    assert "shot_adj" in result
    assert result["shot_adj"] == play["shot"]


def test_calculate_score_adjustment_disadvantage_flips_home(score_adjustments):
    """Strength state 4v5 swaps is_home before lookup, consistently across every adjusted column.

    Regression test: the flip used to be re-toggled once per column inside the adjusted-columns
    loop instead of decided once per play, so only every other column (goal/miss/teammate_block/
    corsi) got the flipped weight while the rest (shot/block/fenwick) silently kept the unflipped
    one. All seven columns must use the same (flipped) perspective.
    """
    play = _base_play(
        event_team="NSH",
        home_team="NSH",
        strength_state="4v5",
        home_score_diff=0,
        goal=1,
        shot=1,
        miss=1,
        block=1,
        teammate_block=1,
        fenwick=1,
        corsi=1,
    )
    result = calculate_score_adjustment(play, score_adjustments)

    # Home team ("NSH") is shorthanded (4v5), so is_home flips to the away perspective and
    # the strength state reverses to "5v4" for the weight-table lookup.
    away_weights = score_adjustments["5v4"][0]

    expected = {
        "goal_adj": away_weights["away_goal_weight"] * play["goal"],
        "shot_adj": away_weights["away_shot_weight"] * play["shot"],
        "miss_adj": away_weights["away_fenwick_weight"] * play["miss"],
        "block_adj": away_weights["away_corsi_weight"] * play["block"],
        "teammate_block_adj": away_weights["away_corsi_weight"] * play["teammate_block"],
        "fenwick_adj": away_weights["away_fenwick_weight"] * play["fenwick"],
        "corsi_adj": away_weights["away_corsi_weight"] * play["corsi"],
    }

    for key, value in expected.items():
        assert result[key] == pytest.approx(value), f"{key} did not use the flipped (away) weight"


def test_calculate_score_adjustment_non_shot_event_unchanged(score_adjustments):
    """Non-shot events (e.g. FACEOFF) pass through without adding _adj keys."""
    play = _base_play(event="FACEOFF")
    result = calculate_score_adjustment(play, score_adjustments)
    assert "shot_adj" not in result


# -----------------------------------------------------------------------------
# charts_directory / data_directory
# -----------------------------------------------------------------------------


def test_charts_directory_creates_folder(tmp_path):
    charts_directory(target_path=tmp_path)
    assert (tmp_path / "charts").is_dir()


def test_charts_directory_idempotent(tmp_path):
    charts_directory(target_path=tmp_path)
    charts_directory(target_path=tmp_path)  # second call should not raise
    assert (tmp_path / "charts").is_dir()


def test_data_directory_creates_folder(tmp_path):
    data_directory(target_path=tmp_path)
    assert (tmp_path / "data").is_dir()


def test_data_directory_idempotent(tmp_path):
    data_directory(target_path=tmp_path)
    data_directory(target_path=tmp_path)  # second call should not raise
    assert (tmp_path / "data").is_dir()


def test_charts_directory_default_path(tmp_path, monkeypatch):
    """charts_directory() with no argument defaults to cwd."""
    monkeypatch.chdir(tmp_path)
    charts_directory()
    assert (tmp_path / "charts").is_dir()


def test_data_directory_default_path(tmp_path, monkeypatch):
    """data_directory() with no argument defaults to cwd."""
    monkeypatch.chdir(tmp_path)
    data_directory()
    assert (tmp_path / "data").is_dir()


# -----------------------------------------------------------------------------
# handle_penalty_details
# -----------------------------------------------------------------------------


def _penalty(**details) -> dict:
    return {"typeCode": "BEN", "descKey": "too-many-men-on-the-ice", "duration": 2, **details}


def test_bench_minor_with_a_drawer_puts_the_drawer_in_player_2():
    """As for any penalty, player_2 drew it and player_3 serves it."""
    info = handle_penalty_details(_penalty(drawnByPlayerId=1, servedByPlayerId=2))

    assert (info["player_1"], info["player_2_api_id"], info["player_3_api_id"]) == ("BENCH", 1, 2)
    assert (info["player_2_type"], info["player_3_type"]) == ("DRAWN BY", "SERVED BY")


def test_bench_minor_with_no_drawer_puts_the_server_in_player_2():
    info = handle_penalty_details(_penalty(servedByPlayerId=2))

    assert (info["player_1"], info["player_2_api_id"], info["player_2_type"]) == ("BENCH", 2, "SERVED BY")
    assert info.get("player_3_api_id") is None
