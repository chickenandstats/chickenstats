import pandas as pd

import pytest

from pathlib import Path

from chickenstats.evolving_hockey.stats import (
    prep_pbp,
    prep_stats,
    prep_lines,
    prep_team,
)


@pytest.fixture(scope="package")
def raw_pbp():
    filepath = Path("./tests/tests_evolving_hockey/data/raw/raw_pbp.csv")

    raw_pbp = pd.read_csv(filepath, low_memory=False)

    return raw_pbp


@pytest.fixture(scope="package")
def raw_shifts():
    filepath = Path("./tests/tests_evolving_hockey/data/raw/raw_shifts.csv")

    raw_shifts = pd.read_csv(filepath, low_memory=False)

    return raw_shifts


@pytest.fixture(scope="package")
def test_pbp(raw_pbp, raw_shifts):
    pbp = prep_pbp(raw_pbp, raw_shifts)
    return pbp


@pytest.mark.parametrize("columns", ["full", "all", "light"])
def test_prep_pbp(columns, raw_pbp, raw_shifts):
    pbp = prep_pbp(raw_pbp, raw_shifts, columns)

    assert isinstance(pbp, pd.DataFrame) is True


@pytest.mark.parametrize("level", ["game", "period", "season"])
@pytest.mark.parametrize("score", [True, False])
@pytest.mark.parametrize("teammates", [True, False])
@pytest.mark.parametrize("opposition", [True, False])
def test_prep_stats(test_pbp, level, score, teammates, opposition):
    stats = prep_stats(
        test_pbp, level=level, score=score, teammates=teammates, opposition=opposition
    )

    assert isinstance(stats, pd.DataFrame) is True


@pytest.mark.parametrize("position", ["f", "d"])
@pytest.mark.parametrize("level", ["game", "period", "season"])
@pytest.mark.parametrize("score", [True, False])
@pytest.mark.parametrize("teammates", [True, False])
@pytest.mark.parametrize("opposition", [True, False])
def test_prep_lines(test_pbp, position, level, score, teammates, opposition):
    lines = prep_lines(
        test_pbp,
        position=position,
        level=level,
        score=score,
        teammates=teammates,
        opposition=opposition,
    )

    assert isinstance(lines, pd.DataFrame) is True


@pytest.mark.parametrize("level", ["game", "period", "season"])
@pytest.mark.parametrize("strengths", [True, False])
@pytest.mark.parametrize("score", [True, False])
def test_prep_team(test_pbp, level, strengths, score):
    team = prep_team(test_pbp, level, strengths, score)

    assert isinstance(team, pd.DataFrame) is True
