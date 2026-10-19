from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import polars as pl

from chickenstats.chicken_nhl._scraper_core import _SCRAPE_REGISTRY, _ScraperBase, normalize_game_ids
from chickenstats.utilities.enums import Backend
from chickenstats.utilities.utilities import data_directory

if TYPE_CHECKING:
    from typing_extensions import Self

# scrape-data key -> Scraper attribute holding its frames keyed by game ID.
_RAW_DATA_ATTRS: dict[str, str] = {key: spec.list_attr for key, spec in _SCRAPE_REGISTRY.items()}

# The keys _meta.json lists games under; play_by_play_ext and xg_fields are stored with play_by_play.
_SCRAPED_KEYS: dict[str, str] = {
    key: key if spec.pbar_label is not None else "play_by_play" for key, spec in _SCRAPE_REGISTRY.items()
}


def _split_by_game(frame: pl.DataFrame) -> dict[int, pl.DataFrame]:
    """One type's saved rows, split by game.

    play_by_play_ext has no game_id column; its id is the game ID followed by a four-digit event index.
    """
    game_id = pl.col("game_id") if "game_id" in frame.columns else pl.col("id") // 10_000
    keyed = frame.with_columns(_game=game_id)
    return {key: rows.drop("_game") for (key,), rows in keyed.partition_by("_game", as_dict=True).items()}


class _ScraperPersistMixin(_ScraperBase):
    def save(self, path: str | Path | None = None) -> Path:
        """Save all currently-scraped raw data to disk as parquet files.

        Writes one parquet file per populated raw data type, plus a ``_meta.json``
        recording game IDs, failed games, and which games have which data type.

        Parameters:
            path (str | Path | None):
                Directory to save into. Created if it doesn't exist. Defaults to
                ``data_directory()`` (``./data`` in the current working directory).

        Returns:
            Path: The directory the data was saved to.

        Examples:
            >>> from chickenstats.chicken_nhl import Scraper
            >>> scraper = Scraper([2023020001, 2023020002])
            >>> pbp = scraper.play_by_play
            >>> save_path = scraper.save("my_scrape")
        """
        target = Path(path) if path is not None else data_directory()
        target.mkdir(parents=True, exist_ok=True)

        for key, attr in _RAW_DATA_ATTRS.items():
            frames: dict[int, pl.DataFrame] = getattr(self, attr)
            if frames:
                pl.concat(frames.values()).write_parquet(target / f"{key}.parquet")

        meta = {
            "game_ids": list(self.game_ids),
            "bad_games": list(self._bad_games),
            "backend": self._backend,
            "game_states": {str(game_id): state for game_id, state in self._game_states.items()},
            "scraped": {
                key: sorted(getattr(self, attr)) for key, attr in _RAW_DATA_ATTRS.items() if _SCRAPED_KEYS[key] == key
            },
        }
        (target / "_meta.json").write_text(json.dumps(meta, indent=2))

        return target

    def _apply_cache(self, path: Path, meta: dict | None = None) -> None:
        """Populate ``self`` with cached data previously written by :meth:`save`.

        Extends ``self.game_ids`` with any cached IDs not already present (cached first),
        loads the raw parquet data, and marks those games as already-scraped.

        Parameters:
            path (Path): Directory previously written by :meth:`save`.
            meta (dict | None): Already-parsed ``_meta.json`` contents, if the caller has
                them; read from ``path`` otherwise.
        """
        if meta is None:
            meta = json.loads((path / "_meta.json").read_text())

        cached_ids = normalize_game_ids(meta["game_ids"]) if meta["game_ids"] else []
        self.game_ids = list(dict.fromkeys([*cached_ids, *self.game_ids]))
        # A game that failed when the cache was saved is failed for every type until it is scraped again
        for game_id in normalize_game_ids(meta.get("bad_games") or []):
            if game_id in self.game_ids:
                self._failed_types.setdefault(game_id, set()).update(_SCRAPE_REGISTRY)
                if game_id not in self._bad_games:
                    self._bad_games.append(game_id)

        # A game with no recorded state is provisional, so it is fetched again on its next scrape
        self._game_states.update({int(game_id): state for game_id, state in meta.get("game_states", {}).items()})

        scraped = meta.get("scraped", {})
        for key, attr in _RAW_DATA_ATTRS.items():
            file = path / f"{key}.parquet"
            schema = _SCRAPE_REGISTRY[key].schema
            frame = pl.read_parquet(file) if file.exists() else pl.DataFrame(schema=schema)
            by_game = _split_by_game(frame)
            # A game listed as scraped with no rows is stored as an empty frame, so it is not fetched again
            stored: dict[int, pl.DataFrame] = getattr(self, attr)
            for game_id in scraped.get(_SCRAPED_KEYS[key], []):
                stored[game_id] = by_game.get(game_id, pl.DataFrame(schema=schema))

    @classmethod
    def load(
        cls,
        path: str | Path,
        game_ids: list | None = None,
        disable_progress_bar: bool = False,
        transient_progress_bar: bool = False,
        backend: Backend | Literal["pandas", "polars", "pyarrow", "narwhals"] | None = None,
    ) -> Self:
        """Load a Scraper from data previously written by :meth:`save`.

        Already-cached games are marked as scraped. ``game_ids`` not present in the
        cache are added and scraped on next access.

        Parameters:
            path (str | Path):
                Directory previously written by :meth:`save`.
            game_ids (list | None):
                Additional game IDs to scrape beyond what's cached. Defaults to ``None``
                (only the cached games).
            disable_progress_bar (bool):
                Suppress the Rich progress bar globally for this instance. Default ``False``.
            transient_progress_bar (bool):
                Clear the progress bar from the terminal after it completes. Default ``False``.
            backend (str | None):
                DataFrame backend for all returned data. Defaults to whatever backend the
                saved Scraper used.

        Returns:
            Scraper: A new instance with the cached data pre-loaded.

        Examples:
            >>> from chickenstats.chicken_nhl import Scraper
            >>> scraper = Scraper.load("my_scrape")
            >>> pbp = scraper.play_by_play  # no network calls for already-cached games

            Extend with new games — only the new ones get scraped
            >>> scraper = Scraper.load("my_scrape", game_ids=[2023020003])
            >>> pbp = scraper.play_by_play
        """
        source = Path(path)
        meta = json.loads((source / "_meta.json").read_text())

        extra_ids: list[str | float | int] = list(normalize_game_ids(game_ids)) if game_ids else []

        scraper = cls(
            extra_ids,
            disable_progress_bar=disable_progress_bar,
            transient_progress_bar=transient_progress_bar,
            backend=backend or meta.get("backend", "polars"),
        )
        scraper._apply_cache(source, meta=meta)

        return scraper
