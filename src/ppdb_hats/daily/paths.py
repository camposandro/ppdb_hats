"""Path utilities for daily pipeline operations.

Provides functions for discovering new input files and tracking
file provenance through the pipeline. Integrates with the
configuration system for flexible path management.
"""

import logging
from datetime import date
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def get_paths(
    dataset_type: str,
    input_lsst_dir: Path,
    collection_path: Optional[Path] = None,
    from_date: Optional[date] = None,
    until_date: Optional[date] = None,
) -> list:
    """Return parquet files for a dataset type within a date range (inclusive).

    Discovers new input files by comparing against previously imported files
    and optionally filters them by a date range.

    Parameters
    ----------
    dataset_type : str
        Type of dataset (``'dia_object'``, ``'dia_source'``, ``'dia_forced_source'``).
    input_lsst_dir : pathlib.Path
        Root directory containing LSST PPDB data.
    collection_path : pathlib.Path or None, optional
        Path to the existing HATS collection for provenance tracking.
    from_date, until_date : datetime.date or None, optional
        Inclusive date range to filter files. ``None`` means unbounded.

    Returns
    -------
    list
        Sorted list of new parquet file paths not previously imported.
    """
    used_paths = _load_used_paths(dataset_type, collection_path) if collection_path else []
    dataset_name = "".join(word.capitalize() for word in dataset_type.split("_"))
    all_paths = input_lsst_dir.rglob(f"{dataset_name}.parquet")
    new_paths = sorted(set(all_paths) - set(used_paths))
    return [path for path in new_paths if _in_range(_file_date(path, input_lsst_dir), from_date, until_date)]


def _file_date(path: Path, input_lsst_dir: Path) -> date:
    """Return the file's generation date inferred from its directory path.

    LSST PPDB organizes files by date using ``YYYY/MM/DD`` directory layout.

    Parameters
    ----------
    path : pathlib.Path
        Full path to the file.
    input_lsst_dir : pathlib.Path
        Root directory used to compute the relative path components.

    Returns
    -------
    datetime.date
        Date object representing the file generation date.
    """
    return date(*map(int, path.relative_to(input_lsst_dir).parts[:3]))


def _in_range(d: date, start: Optional[date], end: Optional[date]) -> bool:
    """Return whether ``d`` lies within the inclusive range ``[start, end]``.

    Parameters
    ----------
    d : datetime.date
        Date to test.
    start, end : datetime.date or None
        Inclusive range bounds. ``None`` means unbounded on that side.

    Returns
    -------
    bool
        ``True`` if ``d`` is within the range, else ``False``.
    """
    return (start is None or d >= start) and (end is None or d <= end)


def _load_used_paths(dataset_type: str, collection_path: Path) -> list:
    """Load previously used file paths for deduplication.

    Paths of previously ingested files are stored under
    ``<collection>/input_paths/{dataset_type}.txt``. This helper returns
    the list of those paths so callers can skip already-processed files.

    Parameters
    ----------
    dataset_type : str
        Dataset type to query (e.g. ``'dia_object'``).
    collection_path : pathlib.Path
        Path to the HATS collection that stores the provenance files.

    Returns
    -------
    list[pathlib.Path]
        Previously imported file paths.

    Raises
    ------
    FileNotFoundError
        If the provenance file does not exist.
    """
    path = collection_path / "input_paths" / f"{dataset_type}.txt"
    if not path.exists():
        raise FileNotFoundError(f"Could not open previous input paths at {path}")
    return [Path(line.strip()) for line in path.read_text().splitlines()]


def append_input_paths(
    dataset_type: str,
    input_file_list: list,
    collection_path: Path,
) -> None:
    """Append ingested file paths to the provenance tracking file.

    Parameters
    ----------
    dataset_type : str
        Dataset name for the provenance file.
    input_file_list : list
        Iterable of file paths (or strings) appended to the provenance file.
    collection_path : pathlib.Path
        Path to the HATS collection.
    """
    logger.info("Saving input paths for %s...", dataset_type)
    input_paths_dir = collection_path / "input_paths"
    input_paths_dir.mkdir(exist_ok=True)
    with (input_paths_dir / f"{dataset_type}.txt").open("a") as f:
        f.writelines(str(p) + "\n" for p in input_file_list)
