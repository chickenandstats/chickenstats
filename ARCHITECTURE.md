# chickenstats Architecture

## Submodules

| Submodule | Role |
|-----------|------|
| `chicken_nhl` | NHL scraping and aggregation (`Scraper`, `Season`, `Game`, `Player`, `Team`) |
| `evolving_hockey` | Aggregation over EvolvingHockey.com CSV exports |
| `utilities` | Progress bars, HTTP session, enums, type aliases, coordinate helpers |
| `api` | Client for the chickenstats.com REST API (`ChickenUser`, `ChickenStats`) |

Top-level `chickenstats` re-exports `Scraper`, `Season`, `Game`, `Player`, `Team`, so
`from chickenstats import Scraper` works directly. Everything else goes through its submodule
path.

---

## chicken_nhl module map

Public classes live in thin wrapper files; the logic behind each one is split across mixins.

**`scraper.py`** — `Scraper(_ScraperCore, _ScraperRawMixin, _ScraperStatsMixin)`
- `_scraper_core.py` — constructor, `game_ids`, `add_games()`, `failed_games`, the `_scrape()` dispatch loop
- `_scraper_persist.py` — `save()`/`load()`, the `cache=`/`overwrite=` constructor options
- `_scraper_raw.py` — cached properties: `play_by_play`, `play_by_play_ext`, `api_events`, `html_events`, `rosters`, `shifts`, `changes`
- `_scraper_stats.py` — `prep_stats()`, `prep_lines()`, `prep_team_stats()` and their cached result properties

**`game.py`** — `Game(_GameCore, _GameAPIMixin, _GameHTMLMixin, _GameRostersMixin, _GamePBPMixin)`
- `_game_core.py` — constructor, metadata attributes (game_id, season, home_team, etc.), `prefetch()`
- `_game_api.py` — `api_events`, `api_rosters` cached properties
- `_game_html.py` — thin composition of four HTML-report mixins into `_GameHTMLMixin`:
  - `_game_html_events.py` — `html_events`
  - `_game_html_rosters.py` — `html_rosters`
  - `_game_html_shifts.py` — `shifts`
  - `_game_html_changes.py` — `changes` (derived from `shifts`)
- `_game_rosters.py` — `rosters`, the combined API+HTML roster
- `_game_pbp.py` — `play_by_play`, `play_by_play_ext`

**`season.py`** — `Season` (self-contained): `schedule()`, `standings`

**`player.py`** — `Player` (self-contained): NHL API landing page and game log endpoints

**`team.py`** — `Team` (self-contained): color lookup, logo download

**`viz/`** — plotting functions built on the aggregation output (`plot_shot_chart`,
`plot_density_heatmap`, `plot_rolling_stats`, `plot_stat_comparison`, `plot_line_network`);
`_helpers.py` holds shared figure/axes and color-fallback utilities.

### Internal files

| File | Contents |
|------|----------|
| `_corrections.py` | Hand-curated NHL API/HTML data corrections keyed by game ID. Each function patches one known data-quality issue; unfixable ones are documented inline. |
| `_docstrings.py` | Centralized docstring registry — column descriptions are defined once and applied via `@shared_doc(...)`, so `Scraper` and `Game` properties that share output stay in sync. |
| `_aggregation.py` | Core aggregation logic: `prep_ind`, `prep_oi`, `prep_stats`, `prep_lines`, `prep_team_stats`, `build_play_by_play_ext`. |
| `_agg_constants.py` | Column groupings, stat lists, and other constants consumed by `_aggregation.py`. |
| `_game_utils.py` | Shared game-mixin utilities: `load_score_adjustments()`, `prefetch_concurrent()`, event-processing helpers. |
| `_player_names.py` | Name-normalization dictionaries: known misspellings, alternate spellings, NHL API overrides. |
| `_validation_schema.py` | Field definitions and schema-building functions for output DataFrames. |
| `_validation_utils.py` | Pydantic-to-Pandera/Polars schema conversion helpers. |
| `validation_polars.py` | Native Polars schema objects, one per output DataFrame. |
| `validation_pydantic.py` | Pydantic models for raw API/HTML event and roster data. |

---

## Key patterns

**Mixin + type-checking stub.** `Scraper` and `Game` use multiple inheritance to split logic
across files without circular imports. Each mixin inherits a base stub (`_ScraperBase`,
`_GameBase`) populated only under `TYPE_CHECKING`, so the type checker resolves cross-mixin
attributes with no runtime cost:

```python
class _ScraperBase:
    if TYPE_CHECKING:
        game_ids: list
        _backend: str
        ...
```

**`@shared_doc` decorator.** Docstrings for wide-DataFrame properties live as constants in
`_docstrings.py` and get stamped onto the callable:

```python
@shared_doc(_SCRAPER_API_EVENTS_DOC)
def api_events(self):
    ...
```

`help()` and mkdocstrings both see the real docstring this way, and the same field description
is reused across every `Scraper`/`Game` property that returns the same shape.

**Multi-backend support.** DataFrame-returning properties and methods accept `backend`
(`"polars"`, `"pandas"`, `"pyarrow"`, `"narwhals"`). Computation is always Polars internally;
`_to_backend()` in `utilities/utilities.py` converts at the return boundary.

**`_corrections.py` pattern.** Each function is `f(game_id, event_or_player_dict) -> dict`:
checks `game_id` against known-bad games, patches the dict in place, returns it. No-op
otherwise. Called inline from the `_game_*.py` mixins during scraping.

---

## Public API entry points

```python
# Primary entry points (also accessible as `from chickenstats import ...`)
from chickenstats.chicken_nhl import Scraper, Season, Game, Player, Team

# Standalone aggregation functions (for advanced use / non-Scraper workflows)
from chickenstats.chicken_nhl import prep_stats, prep_ind, prep_oi, prep_lines, prep_team_stats, build_play_by_play_ext

# EvolvingHockey.com data
from chickenstats.evolving_hockey import prep_pbp, prep_stats, prep_gar, prep_xgar

# Utilities (progress bars, enums, type alias, session)
from chickenstats.utilities import Scraper, AggLevel, Backend, DataFrameT, ChickenSession

# chickenstats.com API client
from chickenstats.api import ChickenUser, ChickenStats
```

---

## Testing

`tests/` mirrors the package structure:

```
tests/
  tests_chicken_nhl/   — Scraper, Season, Game, Player, Team, aggregation, corrections, validation
  tests_evolving_hockey/
  tests_api/
  tests_utilities/
```

Markers: `regression` (specific historical game IDs, run with `-m regression`), `live`
(hits a live API endpoint, excluded by default).

Coverage minimum: 80%, enforced via `pyproject.toml`'s `[tool.coverage.report]`.

Run with `pytest` — settings come from `pyproject.toml`, parallel by default via xdist.

---

## Naming conventions

- Public modules: no prefix (`scraper.py`, `season.py`, `team.py`)
- Internal implementation: `_` prefix (`_game_html.py`, `_aggregation.py`)
- Mixin classes: `_<Module>Mixin` suffix (`_ScraperRawMixin`)
- Base stubs: `_<Module>Base` suffix (`_ScraperBase`, `_GameBase`)
- Test files mirror the module they test: `test_scraper.py`, `test_corrections.py`
