"""Checks that every scraped game must pass, shared by the offline tests and the network-marked regression run.

Each check takes the frames for one game, as ``scrape_saved_game`` returns them, and asserts one invariant:
totals that agree however the output is split, for and against that mirror, Corsi that is Fenwick plus blocked
attempts, ice time equal to the shift reports, and identity columns that are a key.
"""

import polars as pl
import pytest

from chickenstats.chicken_nhl import prep_ind, prep_lines, prep_stats, prep_team_stats
from chickenstats.chicken_nhl.identity import identity_columns

# Games with a known data problem, scraped from the NHL under the regression marker
REGRESSION_GAME_IDS = [
    2022020194,
    2022020673,
    2016020001,
    2018021187,
    2010021176,
    2011020069,
    2012020095,
    2012020341,
    2012020627,
    2012020660,
    2012020671,
    2012030224,
    2013020305,
    2013030142,
    2013030155,
    2013020445,
    2014020120,
    2014020356,
    2014020417,
    2014020506,
    2014020939,
    2014020945,
    2014021127,
    2014021128,
    2014021203,
    2014030311,
    2014030315,
    2015020193,
    2015020401,
    2015020839,
    2015020917,
    2015021092,
    2016020049,
    2016020177,
    2016020256,
    2016020326,
    2016020433,
    2016020519,
    2016020625,
    2016020883,
    2016020963,
    2016021111,
    2016021165,
    2016030216,
    2017020033,
    2017020096,
    2017020209,
    2017020233,
    2017020548,
    2017020601,
    2017020615,
    2017020796,
    2017020835,
    2017020836,
    2017021136,
    2017021161,
    2018020006,
    2018020009,
    2018020049,
    2018020115,
    2018020122,
    2018020153,
    2018020211,
    2018020309,
    2018020363,
    2018020519,
    2018020561,
    2018020752,
    2018020794,
    2018020795,
    2018020841,
    2018020969,
    2018021087,
    2018021124,
    2018021171,
    2019020006,
    2019020136,
    2019020147,
    2019020179,
    2019020239,
    2019020316,
    2019020682,
    2020020456,
    2020020846,
    2020020860,
    2021020482,
    2023020838,
    2023021279,
]

TEAM_COUNTS = ["gf", "ga", "sf", "sa", "ff", "fa", "cf", "ca", "bsf", "bsa", "pent2", "pend2", "fow", "fol"]


def _mirrored(state: str) -> str:
    return "v".join(reversed(state.split("v"))) if "v" in state else state


def totals_agree_however_split(game) -> None:
    """Team, line and individual totals are the same with and without the strength and score splits."""
    pbp, ext = game.play_by_play, game.play_by_play_ext
    whole = prep_team_stats(pbp, ext, level="game", strength_state=False, score=False)
    expected = whole.group_by("team").agg(pl.col(TEAM_COUNTS + ["toi"]).sum()).sort("team")

    for strength_state, score in ((True, False), (False, True), (True, True)):
        split = prep_team_stats(pbp, ext, level="game", strength_state=strength_state, score=score)
        totals = split.group_by("team").agg(pl.col(TEAM_COUNTS + ["toi"]).sum()).sort("team")
        assert totals.select(TEAM_COUNTS).equals(expected.select(TEAM_COUNTS)), (strength_state, score)
        assert totals["toi"].to_list() == pytest.approx(expected["toi"].to_list()), (strength_state, score)

    ind_whole = prep_ind(pbp, level="game", strength_state=False)
    ind_split = prep_ind(pbp, level="game", strength_state=True, score=True)
    counts = [c for c, dtype in ind_whole.schema.items() if dtype.is_integer() and c not in ("season", "game_id")]
    counts = [c for c in counts if not c.endswith("api_id")]
    assert ind_split.select(pl.col(counts).sum()).row(0) == ind_whole.select(pl.col(counts).sum()).row(0)

    lines = prep_lines(pbp, ext, level="game", strength_state=True)
    assert dict(lines.group_by("team").agg(pl.col("gf").sum()).rows()) == dict(whole.select("team", "gf").rows())


def for_and_against_mirror(game) -> None:
    """What one team has for in a strength state, the other has against in the mirrored state."""
    team_stats = prep_team_stats(game.play_by_play, game.play_by_play_ext, level="game", strength_state=True)
    rows = {(row["team"], row["strength_state"]): row for row in team_stats.to_dicts()}
    teams = sorted({team for team, _ in rows})
    other = {teams[0]: teams[1], teams[1]: teams[0]}

    for (team, state), row in rows.items():
        mirror = rows.get((other[team], _mirrored(state)))
        assert mirror is not None, (team, state)
        for stat in ("g", "s", "f", "c", "bs"):
            assert row[f"{stat}f"] == mirror[f"{stat}a"], (team, state, stat)
        assert row["toi"] == pytest.approx(mirror["toi"]), (team, state)


def corsi_is_fenwick_plus_blocked(game) -> None:
    """Corsi is Fenwick plus blocked attempts, for and against, in every output."""
    pbp, ext = game.play_by_play, game.play_by_play_ext
    for df in (
        prep_stats(pbp, ext, level="game", strength_state=True),
        prep_lines(pbp, ext, level="game", strength_state=True),
        prep_team_stats(pbp, ext, level="game", strength_state=True),
    ):
        assert (df["cf"] == df["ff"] + df["bsf"]).all()
        assert (df["ca"] == df["fa"] + df["bsa"]).all()


def ice_time_matches_the_shift_reports(game) -> None:
    """Each player's ice time equals the sum of their shifts."""
    stats = prep_stats(game.play_by_play, game.play_by_play_ext, level="game", strength_state=False)
    shift_toi = game.shifts.group_by("api_id").agg(shift_toi=pl.col("duration_seconds").sum() / 60)
    joined = stats.select("api_id", "toi").join(shift_toi, on="api_id", how="full", coalesce=True)

    assert joined.null_count().sum_horizontal().item() == 0
    assert joined.select(((pl.col("toi") - pl.col("shift_toi")).abs() < 1e-9).all()).item()


def identity_columns_are_a_key(game) -> None:
    """identity_columns names a key of stats, both kinds of lines and team stats, with every split on."""
    pbp, ext = game.play_by_play, game.play_by_play_ext
    stats = prep_stats(pbp, ext, level="game", strength_state=True, score=True, teammates=True, opposition=True)
    key = identity_columns("stats", level="game", strength_state=True, score=True, teammates=True, opposition=True)
    assert stats.select(key).n_unique() == stats.height

    for position in ("f", "d"):
        lines = prep_lines(
            pbp, ext, position=position, level="game", strength_state=True, score=True, teammates=True, opposition=True
        )
        key = identity_columns(
            "lines", position=position, level="game", strength_state=True, score=True, teammates=True, opposition=True
        )
        assert lines.select(key).n_unique() == lines.height, position

    team_stats = prep_team_stats(pbp, ext, level="game", strength_state=True, score=True, opposition=True)
    key = identity_columns("team_stats", level="game", strength_state=True, score=True, opposition=True)
    assert team_stats.select(key).n_unique() == team_stats.height


def season_totals_equal_the_sum_of_games(game) -> None:
    """Season and session totals equal the sum of the game rows."""
    pbp, ext = game.play_by_play, game.play_by_play_ext
    by_game = prep_team_stats(pbp, ext, level="game", strength_state=False)
    expected = by_game.group_by("team").agg(pl.col(TEAM_COUNTS).sum()).sort("team")
    for level in ("season", "session"):
        rolled = prep_team_stats(pbp, ext, level=level, strength_state=False)
        assert rolled.group_by("team").agg(pl.col(TEAM_COUNTS).sum()).sort("team").equals(expected), level


INVARIANTS = [
    totals_agree_however_split,
    for_and_against_mirror,
    corsi_is_fenwick_plus_blocked,
    ice_time_matches_the_shift_reports,
    identity_columns_are_a_key,
    season_totals_equal_the_sum_of_games,
]
