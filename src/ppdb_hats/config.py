"""Configuration settings for the pipelines.

This module declares dataclasses that hold configuration used by the
PPDB pipelines (Dask, filesystem paths and import settings).
"""

from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass
class DaskConfig:
    """Configuration for Dask distributed client.

    Attributes
    ----------
    n_workers : int
        Number of worker processes.
    memory_limit : str
        Memory limit per worker (e.g. ``"8GB"``).
    threads_per_worker : int
        Threads per worker process.
    """

    n_workers: int = 16
    memory_limit: str = "8GB"
    threads_per_worker: int = 1


@dataclass
class PathConfig:
    """Filesystem path configuration for PPDB.

    Attributes
    ----------
    ppdb_lsst_dir : pathlib.Path
        Root directory containing raw LSST PPDB files.
    commissioning_dir : pathlib.Path
        Base directory for LSDB commissioning / HATS collections.
    """

    ppdb_lsst_dir: Path = Path("/sdf/scratch/rubin/ppdb/data/ppdb_lsstcam")
    commissioning_dir: Path = Path("/sdf/data/rubin/shared/lsdb_commissioning")

    @property
    def ppdb_hats_dir(self) -> Path:
        """Base HATS directory for PPDB collections"""
        return self.commissioning_dir / "ppdb"

    @property
    def dia_object_collection_dir(self) -> Path:
        """Directory containing the dia_object_collection"""
        return self.ppdb_hats_dir / "dia_object_collection"

    def __post_init__(self):
        """Ensure paths are Path objects"""
        if isinstance(self.ppdb_lsst_dir, str):
            self.ppdb_lsst_dir = Path(self.ppdb_lsst_dir)
        if isinstance(self.commissioning_dir, str):
            self.commissioning_dir = Path(self.commissioning_dir)


@dataclass
class ImportConfig:
    """Configuration for import pipelines.

    Attributes
    ----------
    file_reader : str
        Backend used to read input files (e.g. ``"parquet"``).
    ra_column : str
        Column name for right ascension.
    dec_column : str
        Column name for declination.
    byte_pixel_threshold : int
        Threshold (bytes) for splitting pixels.
    resume : bool
        Whether to resume an interrupted import.
    simple_progress_bar : bool
        Use a simplified progress display for non-interactive runs.
    """

    file_reader: str = "parquet"
    ra_column: str = "ra"
    dec_column: str = "dec"
    byte_pixel_threshold: int = 1 << 30  # 1 GiB
    resume: bool = False
    simple_progress_bar: bool = True


@dataclass
class PostProcessConfig:
    """Configuration for post-processing pipelines.

    Attributes
    ----------
    position_time_cols : tuple[str, ...]
        Columns to exclude from float32 casting (positional/time fields).
        Default includes standard RA/Dec, position errors, and MJD fields.
    """

    position_time_cols: tuple = (
        "ra",
        "dec",
        "raErr",
        "decErr",
        "x",
        "y",
        "xErr",
        "yErr",
        "midpointMjdTai",
        "radecMjdTai",
        "validityStartMjdTai",
    )


@dataclass
class PipelineConfig:
    """Top-level pipeline configuration.

    Attributes
    ----------
    dask : DaskConfig
        Dask configuration.
    paths : PathConfig
        Filesystem path configuration.
    import_config : ImportConfig
        Import-specific configuration.
    postprocess_config : PostProcessConfig
        Post-processing configuration.
    until_date : datetime.date
        Inclusive upper bound date for files.
    margin_threshold : float
        Margin threshold in arcseconds used by default.
    """

    dask: DaskConfig
    paths: PathConfig
    import_config: ImportConfig
    postprocess_config: PostProcessConfig
    until_date: date = date.today()
    margin_threshold: float = 5


def get_default_config(**kwargs) -> PipelineConfig:
    """Return a :class:`PipelineConfig` filled with sensible defaults.

    Returns
    -------
    PipelineConfig
        Default configuration instance with Dask, path, import, and
        post-processing settings initialized.
    """
    return PipelineConfig(
        dask=DaskConfig(),
        paths=PathConfig(),
        import_config=ImportConfig(),
        postprocess_config=PostProcessConfig(),
        **kwargs,
    )
