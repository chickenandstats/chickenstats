# Changelog

## [Unreleased]

### Bug Fixes

- Updating CI tests
Updating CI tests to pull the correct tox environments from the pyproject.toml file
- Updating for tox changes
Updating pyproject.toml to account for changes to tox
- API keyword arguments
Fixing an issue where API keyword arguments had drifted from the underlying chickenstats-api library
- Update utilities.py
Updating matplotlib imports due to deprecation
- Error with type checking and pre-commit
Fixing a bug where type checking did not run on pre-commit, failing on CI
- Update zensical url
- **styles**: Prevent crash during matplotlib style registration on modern v3.9+ versions
- Apply score-adjustment is_home flip once per play, not per column
calculate_score_adjustment re-toggled is_home inside the per-column loop
for shorthanded strength states (4v5/3v5/3v4), so every other adjusted
column (shot/block/fenwick) kept the unflipped weight while the rest
(goal/miss/teammate_block/corsi) got the correct flipped one. Move the
flip decision outside the loop so it's made once per play.

Strengthens the existing disadvantage-state test to assert the actual
flipped weight is applied consistently across all seven adjusted
columns, verified to fail against the prior implementation.
- Guard 0-for-0 division in _prep_oi_percent to avoid NaN
stat_for / (stat_for + stat_against) produced NaN whenever both were
zero, which is the common case for most shifts (no goals/shots either
way). NaN silently passed schema validation since NaN != null. Fill
with 0.0 to match the existing missing-numerator convention.
- Handle period >= 5 shift-clock repair to prevent UnboundLocalError
The broken-clock fix in _munge_shifts only assigned fix values for
period < 4 (or playoffs) and for period == 4 in regular season/preseason,
so a shift with an unresolved "0:00 / 0:00" end time in period >= 5 (e.g.
a regular-season shootout) fell through both branches and raised
UnboundLocalError. Treat any non-playoff period >= 4 as a 5-minute
period, mirroring the same period < 4 or session == "P" split already
used a few lines later for expected_total_seconds.
- Handle float year input in Season.__init__
str(2023.0) == "2023.0" (len 6) matched neither the 8-digit nor
4-digit branch, so self.season was never assigned and the next line
raised AttributeError. Normalize float input to int first, and raise
InvalidSeasonError for any other unrecognized format instead of
silently falling through.
- Raise for any unsupported season year, not just non-adjacent ones
The guard only raised InvalidSeasonError when first_year != max(_TEAMS_BY_YEAR) + 1,
so the very next NHL season after the table was last updated passed
silently with self.teams = None. schedule() would then return an empty
DataFrame with no error, since `schedule_teams or []` treats None as
empty. Raise whenever the year isn't in the table, with no exception
for the "next" year.
- Raise on unrecognized player slot column in prep_oi
stats_list, col_names, and group_list were each assigned via separate
plain if-blocks keyed on substrings of the player slot name
(event_on/opp_on/change_on), with no final else. If a future edit to
the hardcoded players list ever introduced a value matching none of
these, the loop would silently reuse the previous iteration's
col_names/group_list instead of failing. Convert to if/elif/else
chains that raise ValueError on an unrecognized slot.
- Handle missing hyphen in _return_name_html without crashing
.index("-") raised uncaught ValueError for HTML title text with no
hyphen, which the caller's except KeyError in hs_strip_html didn't
catch, aborting parsing of the entire game's roster instead of just
that one player's name. Return the input unchanged when no hyphen is
present.
- Distinguish expected failures from bugs in _scrape_single_game
The bare except Exception treated known/expected per-game failures
(network errors, malformed data) identically to real programming bugs
(AttributeError, KeyError, TypeError), both logged at WARNING and
silently added to failed_games. Split into two handlers: known
ChickenstatsError/RequestException/ValidationError classes stay at
WARNING, anything else logs at ERROR so it's distinguishable when
scanning logs across a large batch scrape. Still returns None either
way rather than crashing the batch.
- Log prefetch task failures at WARNING instead of DEBUG
prefetch_concurrent and Player.prefetch swallowed all task exceptions
at DEBUG level, invisible unless a caller explicitly enables debug
logging. Both are best-effort cache-warming helpers (the synchronous
property access that follows will retry and surface a real error if
the fetch genuinely fails), so keep swallowing rather than raising,
but bump to WARNING so a persistently failing prefetch is visible by
default.
- Restore give/take columns to the on-ice stats schema
give/take were summed in prep_oi's aggregation and referenced in
_agg_constants.py's stat lists, but commented out of the actual
oi_stats_columns schema dict, so validate_dataframe's column
restriction silently dropped them from every output that includes
on-ice stats (oi_stats, stats, lines, team_stats).
- Add schema validation to evolving_hockey prep_gar/prep_xgar
These were the only two public functions in the module with no schema
validation at all, unlike prep_ind/prep_oi/prep_stats/prep_lines/
prep_team_stats which all validate their output. Add gar_fields/
xgar_fields schemas (dtypes verified against the real GAR/skater/
goalie/xGAR CSV fixtures, since draft_rd/draft_ov are strings and
fa_ev/fa_sh are ints in the actual EH export format) and wire
validate_dataframe into both functions.
- Raise on malformed EH season format in prep_gar/prep_xgar
The season-string reconstruction ("20" + split[0] + "20" + split[1])
had no format guard. A season string with a different shape than EH's
usual two-digit-dash-two-digit format (e.g. a 4-digit-dash-4-digit
variant) would silently produce a garbled-but-non-null season value
rather than erroring. Check the format up front and raise
DataMismatchError with the offending value(s) instead.
- Make _right join-suffix reliance explicit in _aggregation.py
Four joins (prep_ind, prep_oi, prep_lines, prep_team_stats) produced
column-name collisions consumed a few lines later via the "_right"
suffix (isb/icf, toi/bsf/cf_adj, lines toi, team_stats toi), relying
on Polars' default suffix rather than an explicit one. A refactor or
version change could silently break these without erroring. Pass
suffix="_right" explicitly at each call site — same behavior, but the
contract is now stated rather than implicit.
- Stop closing shared ChickenSession between scrape phases
_scrape() wrapped each call in `with self._requests_session:`, but
it's invoked separately per scrape_type by the cached properties in
_scraper_raw.py (api_events, html_events, shifts, etc. each trigger
their own _scrape() call). Session.__exit__ calls close(), so
sequential property access on the same Scraper tore down and rebuilt
the connection pool between every phase instead of once for the
Scraper's lifetime, defeating ChickenSession's pooling.
- Stop using session as a per-call context manager in Player/Season
`with self._requests_session as s:` calls Session.__exit__ -> close()
after every single request. In Player this is actively dangerous:
prefetch() runs _get_landing/_get_logs concurrently via
ThreadPoolExecutor, so one thread finishing and closing the shared
session could pull the connection pool out from under the other
thread's in-flight request. Season didn't have the concurrency risk
but had the same pool-churn issue. Call session.get() directly instead.
- Reuse a session and cache Team.logo instead of refetching
Team.logo constructed a brand-new throwaway ChickenSession() on every
property access, with no connection reuse across accesses or across
different Team instances' typical usage pattern, and re-downloaded
the image every time despite it never changing. Store one session on
the instance (same pattern as Player/Season) and make logo a
cached_property.
- Add raise_for_status() to HTTP calls in player/team/season
None of these three classes checked the HTTP response status before
parsing it as JSON/image content, so a 404 or other error response
surfaced as an opaque KeyError/JSONDecodeError deep inside a _munge_*
function or PIL's image decoder instead of a clear error at the call
site.
- Wire up _api_constants.py, fixing download_pbp's oversized default limit
_api_constants.py existed only as a target for test_api_limits.py's
drift check; api.py's 10 paginated download_* methods each hardcoded
their own default limit inline instead of referencing it. One of those
hardcoded values was wrong: download_pbp() defaulted to 100_000, but
the SDK's read_pbp caps limit at 50_000 (PBP_MAX_LIMIT) — calling it
with no explicit limit would fail SDK-side validation. Replaced all
ten literals with PBP_MAX_LIMIT/STATS_MAX_LIMIT/PRED_GOAL_MAX_LIMIT.
- Route ChickenStats._finalize_dataframe through shared _to_backend
_finalize_dataframe hand-rolled a separate pandas-only conversion path
instead of using the utilities._to_backend helper the rest of the
package relies on. Widened ChickenStats(backend=...) to accept
pyarrow/narwhals like Scraper/Game already do, and swapped the bare
ValueError for UnsupportedBackendError to match _validation_utils.py's
existing convention for the same failure mode.

### Build

- Update uv.lock
Updating dependencies
- Update dependencies

### CI/CD

- **docs**: Explicitly install docs dependency group for mkdocs/zensical builds
- Run a fast test check on push/PR, not just schedule/release
tests.yml previously only triggered on a monthly schedule, manual
dispatch, or as new_release.yaml's pre-publish gate — never on push
or pull_request. A broken PR could merge to main without the test
suite running against it at all. Added push/pull_request triggers
that run just the ubuntu-latest + 3.13 combo for fast PR signal; the
full 12-way OS x Python matrix still runs on schedule/dispatch/release.

### Documentation

- Update guides, tutorials, and roadmap for v1.8.0 release
- Clarify ChickenSession thread-safety invariant
Audited the shared-session-across-ThreadPoolExecutor pattern used by
Scraper/Game prefetch. requests.Session isn't documented thread-safe
in general, but the underlying urllib3 connection pool is, and
grepping the codebase confirms update_headers() is never called mid-
scrape (only in its own docstring example and test) — so the current
usage is safe in practice. Document the actual invariant (don't mutate
session-level state while requests are in flight) rather than adding a
lock that would serialize concurrent requests for no real benefit.

### Features

- **core**: Align scraper, api, and evolving_hockey modules for v1.8.0
- **core**: Add automated data lineage tracking and gzip score adjustments

### Miscellaneous

- Bumping version
- Configure test workflow and bump version to 1.8.0
- **docs**: Completely remove mkdocs packages from docs group, retaining zensical for documentation
- **docs**: Add mkdocstrings-python for zensical python reference API parsing
- **deps**: Normalize chickenstats_api dependency name and map to workspace sources
- Remove uv workspace, fix matplotlib style, and add offline tests

### Performance

- Cache Game instances per game_id on Scraper
_scrape_single_game constructed a fresh Game(...) on every call, so
sequential partial-property access (e.g. scraper.api_events then
scraper.rosters for the same games) redundantly re-fetched data
already cached on a previous Game instance's cached_properties.
Cache by game_id and reuse across scrape_type calls.
- Vectorize null-column check in _finalize_dataframe
df.select(col for col in df if col.is_not_null().any()) issued one
is_not_null().any() reduction per column. Compute all columns' not-null
status in a single vectorized pass instead.
- Make evolving_hockey score-adjustment weights load lazy
adj_weights_lf was built at module import time, unpickling and
gzip-decompressing chicken_nhl's score-adjustment file before any
data was actually processed. Defer via lru_cache so importing
chickenstats.evolving_hockey doesn't pay this cost upfront.
- Defer per-slot aggregation in prep_oi to a single group_by per category
prep_oi ran a separate df.group_by(group_list).agg(agg_stats) for each
of the 21 lineup-slot columns (event_on_1-7, opp_on_1-7, change_on_1-7),
even though the per-category concat immediately afterward
(event_stats/opp_stats/zones_stats) already re-aggregates across all
slots in one group_by. Replace the per-slot group_by with a plain
select+rename, so aggregation happens once per category (~3 group_by
calls) instead of once per slot (~21) plus category-level (~3).

Verified via a standalone diff harness comparing old vs. new output
across 4 real games spanning different NHL eras (modern, playoff,
pre-lockout, OT) and all 64 level/strength_state/score/teammates/
opposition parameter combinations per game (256 total) - zero
mismatches beyond float64 summation-order noise (~1e-15, well under
the 1e-6 tolerance used). Also ran the full test_aggregation.py,
test_scraper.py, test_game.py suites (368 tests) and all
regression-marked tests (190 tests) with zero failures.

### Refactor

- **deps**: Remove docs from dev dependency group to avoid build issues
- Extract season.py's hardcoded data tables to _season_constants.py
regular_season_end_dates and _TEAMS_BY_YEAR made up ~75% of season.py's
line count as pure data with no logic, mirroring the pattern already
used for _agg_constants.py elsewhere in the package. Extract them to a
dedicated module and import into season.py; no behavior change.
- Centralize opponent-swap rename dict in _agg_constants.py
prep_ind, prep_oi, prep_lines, and prep_team_stats each duplicated the
same team/lineup/goalie/score-state swap mapping inline. Extracted to
OPPONENT_SWAP_COLS so the four call sites stay in sync.
- Centralize evolving_hockey team-abbreviation map
pbp.py's replacement_teams and _aggregation.py's _TEAM_REPLACE were
identical dicts maintained separately. Moved to _agg_constants.py as
TEAM_REPLACE and updated the stale "no rename dicts are needed" comment.
- Delete unused validation_pandas.py
pbp_pandera_pandas and stats_pandera_pandas had no callers anywhere in
src/ or tests/ — api.py's pandas backend path doesn't validate through
pandera. Removed the module and its CLAUDE.md reference; the underlying
pandas_dtype_map/pandas_pandera_options in _validation_schema.py stay,
since they're still exercised by _validation_utils tests and back
polars_pandera_options.

## [1.7.9.29] - 2026-06-02

### Bug Fixes

- Update new_release
Updating to latest git-cliff action, threw an error last time
- Bug with API ID fallback
Fixing bug with API ID fallback
- Re-ordering based on API ID
reordering player aggregations based on API IDs, to eliminate duplicate rows for different lines
- Updating season dtype
Updating season dtype to int
- Aggregation error with team stats
Fixed a group_by error with the team stats where rows were being dropped
- Updating examples env.example
Updating examples for proper API env variables
- Updating filepaths for data workflows
Updating file paths for data workflows
- Updating dotenv
updating mlflow experiments dotenv
- Updating season column dtype
Updating season column dtype to always be an integer
- Adding proper env variables
- Adding errors and type validation
Improving user ergonomics with warnings and type validation
- Adding parquet to gitignore
Adding parquet files to gitignore
- Adding old schema so they don't have to be recreated
Adding the old schema to the file so they don't have to be recreated from scratch
- Importing from local directory
Importing the function to prep the data from the local xg file vs. the live version in the module.
- De-duplicate change players
Addresses a bug where player is repeated if shift ends at the same time as other shifts. De-duplicates the change on / off players
- Moving xG validation
Moving xG validation to the xg file to keep the code closer to prep_data functions. Should be easier version control as we iterate on the model
- Moving xg.py closer to xg scripts
Moving xg.py where closer to xg scripts for maintenance
- Updating RAPM target metrics and player ID columns
Updating the target metrics to be the env_xg and updating the player ID columns to be API ID
- Updating experiments.py
Updating experiments.py for latest
- Updating rolling stats for env_xg
updating rolling stats to compute off of environment xG
- Updating experiments.py
- Updating naming conventions
- Adding session as categorical column
- Addressing play speed nulls
Adding play speed so they're null and not infinity
- Addressing bugs with monotone constraints
Addressing bugs with monotone constraints where column may not be in the data
- Sorting data before saving
Sorting data before saving to preserver order
- Updating leakage of uv environments
- Removing xG features
Removes ixg from aggregations / stats unless column presenting
- Updating pre-commit
Updating pre-commit for repo changes
- Cleaning up dependencies
Cleaning up dependencies
- Updating logo url
Updating url for logos so the Team class can properly pull them in
- Addressing type checking errors
Addressing type checking errors

### Build

- Update uv.lock
Updating for this dependency
- Updating chickenstats_api dependency
Updating chickenstats_api dependency
- Updting dependencies
updating dependencies
- Update dependencies
Updating dependencies
- Updating dependencies
Upating dependencies
- Updating dependencies
- Updating dependencies
- Adding dependencies and updating ruff sources
Adding dependencies and updating ruff sources
- Updating dependencies
updating dependencies
- Updating chickenstats-api dependency
Updating chickenstats-api dependency now that it has been released

### CI/CD

- Update cliff.toml to add commit bodies
Updating cliff.toml file to add the body message for git commits vs. just the summary messages
- Adding xg_model to clean-dotenv
Adding xg_model as directory to generate a clean dotenv file
- Adding editorconfig
Adding editorconfig to enfroce the new line issues

### Documentation

- Updating evolving_hockey docs
Updating evolving_hockey docs to fix zensical error
- Update readme
Updating readme for polars and for the correct links to the documentation site
- Updating index
Updating documentation homepage for latest updates
- Updating chicken_nhl guide
Updating the chicken_nhl guide for the latest library improvements, include polars

### Features

- Adding team_stats upload
Adding team_stats upload to ChickenStats API class
- Adding experiments.py
Adding file to run xG experiments
- Adding env file for experiments
Adding env file for experiments
- Adding limit functionality
Adding functionality to account for API limits, with wrappers to fetch all data
- Adding API wrapper
Adding wrapper for API access via chickenstats package
- Adding ID generator utilities
Adding ID generator columns to be re-used in development (I.e., loading data)
- Adding module-level track function
Module-level track function for lightweight tracking when iterating through lists
- Custom ChickenTimeRemaining column in progress bar
Adding custom ChickenTimeRemaining column in the progress bar that adds fallback functionality for long-running processes
- Updating scrape_data to write parquet files
Updating the scrape script to write data as parquet (vs. csv) files
- Moving old versions of process and experiments
Moving old versions of processing data and experiments for xG model build
- Building cascade xG model
Initial steps in building a cascade xG model
- Adding process data scripts for env and inf xg
Adding separate process data scripts for environment and informed xG
- Creating a script to finalize inf_xg
Finalizing the informed xG models
- Last commit of xG model before moving behind paywall
Final commit of the xG model before moving behind the paywall
- Adding base_xg and pred_goal capabilities
Initial commit to prep for the cascade xG model
- Adding chickenstats-xg
Adjusting aggregation to account for base_xg, context_xg, and pred_goal, which are part of chickenstats-xg
- Adding chickenstats xG fields
Adding fields for scraper to natively populate fields used in xG model
- New chickenstats API sdk
Updating for the latest features in the chickenstats API sdk

### Refactor

- Consolidating api_utils
Consolidating the prep stats functions for api upload to single function in api_utils for maintainability
- Updating experiments
Updating the experiments file
- Moving xG model
Moving xG model to a new repo
- Moving xG to other repo
Moving xG experiments to another repo and prep scraper for public release without xG
- Moving xG files to other repo
Moving xG files to another repo
- Removing xG
Removing xG features and functionality from the public scraper
- Moving logos to assets
Moving logos from project root to assets folder for cleaner file structure
- Updating project root
Updating project root to consolidate files and improve developer experience. Added files like changelog

### Testing

- Streamlining tests to account for library changes
Streamlining tests and increasing speed of test suite in CI/CD builds

## [1.7.9.28] - 2026-04-17

### Bug Fixes

- Comprehensive dev dependencies
Ensures all dependencies included in uv dev group
- Updating _validation_utils
Updating _validation_utils to add a build_pandera_schema function and the _get_base_type_type_and_nullable function to pull in the first non-list basetype
- Updating team_stats
Updating team stats levels to pull in strength_state vs. strengths
- Updating dtypes and group columns
Updating dtypes and grouping columns for aggregation functions
- Updating tests suite
Updating test suite to use proper errors and to ignore certain ty results
- Updating polars aggregation
Updating polars aggregation for correct individual corsi for statistics
- Updating adjusted corsi calculations
Updating adjusted corsi calculations to properly calculate block_adj and teammate_block_adj
- Updating change events in pbp
Updating pbp and aggregations to propertly account for zone starts
- Updating testing exceptions
Updating test exceptions to filter warnings that impact polars performance, due to an issue with pandera and checking lazyframes. Will be removed once bug is addressed by pandera
- Adding missing shifts for several games
Add missing OT shifts for several games, there may be others to add later
- Aligning xg prep functions
Aligning xg prep functions and making use of polars validation
- Removing pandera polars performance warning
Removing pandera polars performance warning after latest pandera update
- Removing print debug statement
Accidentally left a print debug statement in there and needed to be removed
- Adding _to_backend utility
Adding _to_backend utility for consistency across modules
- Moving score adjustments calculations
Move score adjustments calculations to properly calculate xg-based adjusted values
- Adding dummy columns
Ensuring the proper dummy columns are constructed

### Build

- Updating dependencies
Updating to latest dependencies
- Update uv.lock
Updating uv.lock and dependencies
- Update dependencies
Updating dependencies to latest versions

### CI/CD

- Update pre-commit-config for git-cliff
Updating pre-commit settings to enforce git-cliff conventions on the commit message
- Adding git-cliff to new releases
Adding git-cliff functionality to the new release workflow to generate release notes automatically
- Updating tests to run monthly
Updating tests workflow to run monthly to ensure consistent build status vs. manual updates
- Updating git checkout in update-docs
Updating the git checkout action in update-docs.yml for an actual supported version (v6 isn't real)
- Updating uv in update-docs-mkdocs
Updating uv to the latest version in update-docs-mkdocs.yml
- Adding json config for renovatebot
Adding json configuration file for renovatebot
- Pre-commit aligned with repo
Ensuring that pre-commit uses the same versions of dependencies as the repository
- Updating pre-commit for ty
Updating pre-commit to run ty only staged files
- Adding codecov configuration
Adding codecove configuration
- Updating docs workflow
Updating docs workflow to reduce duplication by adding workflow dispatch call
- Delete update-docs workflow
Combing docs with update-docs workflows through dispatch call to remove duplication
- Adding lint workflow
Adding lint workflow to catch errors

### Documentation

- Creating _docstrings.py
Create _docstrings.py to house common docstrings to be re-used across modules / classes
- Update _fixes.py
Updating _fixes.py docstrings and comments
- Updating game.py for shared docstring conventions
Update game.py (and underlying Mixin classes) for the shared docstrings recently drafted
- Updating docstrings and code comments
updating docstrings and code comments for non-public functions
- Adding shared docstrings
Adding shared docstring conventions for scraper.py
- Updating docstrings and comments
Updating docstrings and comments for aggregations
- Updating validation docstrings and comments
Updating docstrings and code commenting for validation framework
- Updating utilities docstrings and comments
Updating docstrings and code commenting for utilities module
- Updating scraper docstrings and comments
Updating docstrings and code commenting for scraper classes
- Updating team and player docstrings and comments
Updating team and player docstrings and comments
- Updating progress bar documentation
Improving progress bar documentation to better match rich

### Features

- Dumping failed games to json
Dumping failed games to json, in the event that they exist, for debugging purposes

### Miscellaneous

- Adding git-cliff toml file
Adding toml file for git-cliff configuration
- Adding templates
Adding templates for bug requests, feature requests, and pull requests
- Adding py.typed
Adding py.typed for file type hints

### Refactor

- Creating custom exceptions
Creating custom exception classes for chicken_nhl module
- Updating src/chickenstats/__init__.py
Updating top-level __init__.py to show version number
- Chicken_nhl __init__.py
Updating chicken_nhl module's __init__.py file to show publicly available classes
- Custom exceptions in _validation_utils
Adding custom exception classes to _validation_utils
- Updating evolving_hockey __init__.py
Updating evolving_hockey module to show the publicly available functions
- Update utilities __init__.py
Updating utilities module to show the publicly available functions
- Adding ty type hints and custom exceptions
Adding type hints to comply with ty type checking and adding custom exception functionality
- Adding utility functions to __init__
Adding data_directory and charts_directory functions to chickenstats/utilities/__init__.py
- Converting evolving_hockey pbp functions to polars
Refactoring the evolving_hockey module for polars vs. pandas functionality. Impacts multiple files due to reorgnization
- Adding polars pandera schema
Adding functions to create the polars pandera schemas which are used later
- Significant refactor
Significantly changing the chicken_nhl module to reduce polars / pandas duplication, improve scraper performance, and improve validation framework. Too many changes to list here
- Updating evolving_hockey module
Updating evolving_hockey module to leverage similar polars / narwhal functionality, reduce polars / pandas duplication, and improve validation framework. Shares as much as possible with chicken_nhl module
- Adding enums to aggregation functions
Adds enums to aggregation functions
- Splitting game.py to multiple files
Splitting game.py into multiple files and making use of Mixins to improve simplicity of public API
- Splitting scraper.py
Splitting scraper.py into multiple files to simplify public facing API
- Updating scraper.py
Updating scraper.py to make use of multiple files to simplify public API
- Updating utilities
Updating utilities to be more easily shared across modules, adding enums, and adding type checks
- Adding enums and logging
Updating season and player with enums and logging functionality
- Adding type hints and enums
Adding type hints and enums
- Performance improvements in aggregations
Improving performance with aggregations
- Adding _to_backend and failed_games
Adding _to_backend function for consistency across modules and a failed_games public property
- Adding _to_backend and type hints
Adding _to_backend for consistency across modules and proper dataframe type hints
- Chaining capabilities and performance improvements
Adding ability to chain prep_* methods (I.e., scraper.prep_stats(level="season").stats) for ergonomics and performance improvements for aggregations
- Updating api module
Updating api functions for polars functionality and removing pandas de-duplication.

### Testing

- Updating tests suite
Updating tests suite for refactors and to improve coverage
- Removing polars exception filter
Pandera made the update, no need for polars performance warning exception filter
- Updating testing suite
Updating testing suite for latest changes
- Updating test suite
Updating testing suite for changes
- Updating testing suite for name changes
Updating testing suite for name changes

## [1.7.4] - 2023-12-28
