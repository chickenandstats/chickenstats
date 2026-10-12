# Contributing to chickenstats

Thank you for your interest in contributing! This guide covers the essentials for getting started.

---

## Quick start

```sh
# Fork and clone the repo, then install all dev dependencies
uv sync --group dev

# Install pre-commit hooks (ruff, ty, conventional commits, nb-clean), including the commit message checks
uv run pre-commit install --hook-type pre-commit --hook-type commit-msg
```

---

## Running tests

Run the local coverage suite (clean → test → report):

```sh
uv run tox run -m local
```

Run the full CI matrix (Python 3.10–3.13, Linux/macOS/Windows):

```sh
uv run tox run -m ci
```

Run a targeted subset during development:

```sh
uv run pytest tests/tests_chicken_nhl/ -x
```

Coverage threshold is 80% — the CI gate will fail if it drops below that.

Regression tests (specific historical game IDs) and live API tests are excluded by default. Run them explicitly when needed:

```sh
uv run pytest -m regression
uv run pytest -m live   # requires a live API endpoint and credentials
```

---

## Code style

```sh
uv run ruff format .          # format
uv run ruff check .           # lint
uv run ty check src/          # type checking
```

Docstrings follow [Google style](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings). Ruff and ty run automatically on commit via pre-commit.

---

## Commit format

[Conventional commits](https://www.conventionalcommits.org/) are required and enforced by a pre-commit hook. The
changelog and the GitHub release notes are generated from commit subjects, so the subject is what users read.

```
type(scope)!: subject
```

| Type | Use for | In the changelog |
|------|---------|------------------|
| `feat` | A new capability a user can call | Added |
| `fix` | A wrong result, crash or misleading message | Fixed |
| `perf` | Same output, less time or memory | Performance |
| `docs` | Documentation only | Documentation |
| `refactor` | No behavior change | No |
| `test` | Tests only | No |
| `build` | Build system or dependencies | No |
| `ci` | CI/CD | No |
| `style` | Formatting only | No |
| `chore` | Maintenance | No |

Every commit has a one-line summary, a blank line, then a body. A second pre-commit hook rejects a commit without
a body.

```
fix: count a shootout as one goal for the winner in play_by_play

Each shootout goal was added to the running score, so a 3-2 shootout
win could read 5-4. The score now changes once, when the shootout ends.
```

The summary:

- Write it for someone reading the changelog: say what changed for the user and name the public function, class or
  column. `fix: update _game_pbp.py` says nothing.
- Imperative mood, lower-case first word, no trailing period, 72 characters or fewer.
- One change per commit. If the summary needs "and", split the commit.

The body:

- Say what was wrong or missing, and what the code does now. Give the reason when it is not obvious.
- Plain, specific sentences in your own words: name the function, the column, the game or the number. No filler, no
  praise for the change, no restating the diff line by line.
- The body is for reviewers and is not copied into the changelog.

Append `!` whenever existing code could get a different result: a returned value, a column name, type or nullability,
a signature or a default. These are listed first, under "Breaking changes":

```
fix!: return null instead of 0.0 for percentages with a zero denominator
```

The scope is optional. When used, it is one of `chicken_nhl`, `evolving_hockey`, `api`, `utilities`, `cli`, `viz`
or `docs`.

`CHANGELOG.md` is generated, not edited by hand. Regenerate it on `main` once a release is tagged:

```bash
git-cliff --config pyproject.toml -o CHANGELOG.md
```

---

## Pull requests

- Branch from `main`; target `main`
- Keep PRs focused — one concern per PR
- Tests must pass and coverage must not drop below 80%
- Reference the relevant GitHub issue in the PR description if applicable

---

## Bug reports & feature requests

Please open an [issue](https://github.com/chickenstats/chickenstats/issues). Before filing a bug, check the [known issues](https://chickenstats.com/contribute/known_issues/) page — it may already be documented.

---

## Architecture

See [CLAUDE.md](CLAUDE.md) for a map of the codebase: submodule roles, class inheritance chains, internal file descriptions, and key patterns.
