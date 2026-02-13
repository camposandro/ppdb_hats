"""Utilities to nest sources in objects"""

import logging
from pathlib import Path

import lsdb
from hats_import import pipeline_with_client
from hats_import.margin_cache.margin_cache_arguments import MarginCacheArguments

logger = logging.getLogger(__name__)


def load_sources_with_margin(
    client,
    input_dir: Path,
    dataset_type: str,
    margin_arcsec: float,
):
    """Create source margins required for nesting operations.

    Parameters
    ----------
    client : dask.distributed.Client
        Dask client used to run the margin-cache pipeline.
    input_dir : pathlib.Path
        Directory containing the HATS catalog artifacts.
    dataset_type : str
        Dataset type (``'dia_source'`` or ``'dia_forced_source'``).
    margin_arcsec : float
        Margin in arcseconds to compute around each partition.

    Returns
    -------
    lsdb.Catalog
        LSDB catalog opened with the computed margin cache.
    """
    logger.info("Generating margins for %s...", dataset_type)
    source_path = input_dir / dataset_type
    margin_name = f"{dataset_type}_{margin_arcsec}arcs"

    args = MarginCacheArguments(
        input_catalog_path=source_path,
        output_path=input_dir,
        margin_threshold=margin_arcsec,
        output_artifact_name=margin_name,
        progress_bar=False,
        resume=False,
    )
    pipeline_with_client(args, client)
    return lsdb.open_catalog(source_path, margin_cache=input_dir / margin_name)


def nest_sources(dia_object, dia_source=None, dia_forced_source=None):
    """Nest source catalogs into the dia_object catalog.

    Parameters
    ----------
    dia_object
        Parent LSDB ``dia_object`` catalog.
    dia_source, dia_forced_source
        Optional LSDB source catalogs to nest under objects.

    Returns
    -------
    lsdb.Catalog
        Catalog with ``diaSource`` and/or ``diaForcedSource`` nested columns.
    """
    logger.info("Nesting all sources...")

    nested_cat = dia_object
    source_cols = []

    if dia_source is not None:
        nested_cat = nested_cat.join_nested(
            dia_source,
            left_on="diaObjectId",
            right_on="diaObjectId",
            nested_column_name="diaSource",
            how="left",
        )
        source_cols.append("diaSource")

    if dia_forced_source is not None:
        nested_cat = nested_cat.join_nested(
            dia_forced_source,
            left_on="diaObjectId",
            right_on="diaObjectId",
            nested_column_name="diaForcedSource",
            how="left",
        )
        source_cols.append("diaForcedSource")

    return nested_cat.map_partitions(sort_nested_sources, source_cols=source_cols)


def sort_nested_sources(df, *, source_cols, mjd_col="midpointMjdTai"):
    """Sort nested source lists by observation time.

    Parameters
    ----------
    df : pandas.DataFrame
        Partition dataframe containing nested source columns.
    source_cols : list[str]
        Names of nested source columns to sort.
    mjd_col : str, optional
        Column name representing observation time (default ``"midpointMjdTai"``).

    Returns
    -------
    pandas.DataFrame
        DataFrame with nested source lists sorted by ``mjd_col``.
    """
    for source_col in source_cols:
        flat_sources = df[source_col].nest.to_flat()
        df = df.drop(columns=[source_col]).join_nested(
            flat_sources.sort_values([flat_sources.index.name, mjd_col]), source_col
        )
    return df
