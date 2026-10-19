from __future__ import annotations

# Maximum batch sizes per endpoint — mirror the SDK's Field(le=N) constraints.
# When chickenstats-api bumps a limit, update the constant here and re-run
# tests/tests_api/test_api_limits.py to confirm alignment.
PBP_MAX_LIMIT: int = 50_000
STATS_MAX_LIMIT: int = 50_000
PRED_GOAL_MAX_LIMIT: int = 50_000
RAW_MAX_LIMIT: int = 50_000
"""Shared limit for the raw chicken_nhl endpoints (rosters, shifts, changes, games, players)."""

# Connect and read timeouts, in seconds, for every chickenstats API request, so a stalled connection fails
# instead of blocking for good (N43); the read timeout allows for a large page of results
REQUEST_TIMEOUT: tuple[float, float] = (3.05, 60)
