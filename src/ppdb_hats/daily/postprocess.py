"""Methods for post-processing PPDB data."""

import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Optional

import astropy.units as u
import hats
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dask.distributed import Client, as_completed
from hats.io.parquet_metadata import write_parquet_metadata
from tqdm import tqdm

logger = logging.getLogger(__name__)


def postprocess_catalog(
    client: Client,
    hats_dir: Path,
    catalog_name: str,
    position_time_cols: tuple,
    validity_col: Optional[str] = None,
    flux_colnames: Optional[list[str]] = None,
):
    """Post-process a HATS catalog by applying several optional steps.

    The function will schedule partition-level tasks to optionally:
    filter by validity, compute magnitudes from flux columns, and cast
    numeric columns to more efficient dtypes.

    Parameters
    ----------
    client : dask.distributed.Client
        Dask client used to submit partition processing tasks.
    hats_dir : pathlib.Path
        Path to the HATS directory containing the catalog.
    catalog_name : str
        Name of the catalog to post-process (e.g. ``"dia_object"``).
    position_time_cols : tuple
        Columns to exclude from float32 casting (positional/time fields).
    validity_col : str or None, optional
        Column name containing validity start times. If ``None``, validity
        filtering is skipped.
    flux_colnames : list[str] or None, optional
        List of flux column names for which magnitude columns should be
        computed. If ``None``, magnitude calculation is skipped.
    """
    logger.info("Post-processing %s...", catalog_name)

    catalog_dir = hats_dir / catalog_name
    catalog = hats.read_hats(catalog_dir)
    futures = []
    for target_pixel in catalog.get_healpix_pixels():
        futures.append(
            client.submit(
                process_partition,
                catalog_dir=catalog_dir,
                target_pixel=target_pixel,
                position_time_cols=position_time_cols,
                validity_col=validity_col,
                flux_colnames=flux_colnames,
            )
        )
    for future in tqdm(as_completed(futures), desc=catalog_name, total=len(futures)):
        if future.status == "error":
            raise future.exception()
    rewrite_catalog_metadata(catalog, hats_dir)


def process_partition(
    catalog_dir: Path,
    target_pixel,
    position_time_cols: tuple,
    validity_col: Optional[str] = None,
    flux_colnames: Optional[list[str]] = None,
):
    """Apply post-processing steps to a single partition file.

    Parameters
    ----------
    catalog_dir : pathlib.Path
        Path to catalog directory.
    target_pixel
        HEALPix pixel identifier used to locate the partition file.
    position_time_cols : tuple
        Columns excluded from float32 conversion.
    validity_col : str or None, optional
        Column name for validity filtering. If ``None``, filtering is skipped.
    flux_colnames : list[str] or None, optional
        Flux columns used to compute magnitudes. If ``None``, magnitude
        computation is skipped.
    """
    # Read partition
    file_path = hats.io.pixel_catalog_file(catalog_dir, target_pixel)
    table = pd.read_parquet(file_path, dtype_backend="pyarrow")

    # Apply validity filtering if configured
    if validity_col and validity_col in table.columns:
        table = select_by_latest_validity(table, validity_col)

    # Apply magnitude calculation if configured
    if flux_colnames:
        table = append_mag_and_magerr(table, flux_colnames)

    # Apply type casting for optimization
    table = cast_columns_float32(table, position_time_cols)

    # Overwrite partition
    final_table = pa.Table.from_pandas(table, preserve_index=False)
    pq.write_table(final_table.replace_schema_metadata(), file_path.path)


def rewrite_catalog_metadata(catalog, hats_dir: Path):
    """Update catalog metadata after processing leaf parquet files.

    Parameters
    ----------
    catalog
        HATS catalog object whose metadata should be updated.
    hats_dir : pathlib.Path
        Path to the HATS directory where properties and metadata live.
    """
    destination_path = hats_dir / catalog.catalog_name
    # Update _common_metadata and _metadata
    parquet_rows = write_parquet_metadata(destination_path)
    # Update hats.properties
    now = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M%Z")
    updated_info = catalog.catalog_info.copy_and_update(total_rows=parquet_rows, hats_creation_date=now)
    updated_info.to_properties_file(destination_path)


def select_by_latest_validity(table: pd.DataFrame, validity_col: str) -> pd.DataFrame:
    """Keep the row with the most recent validity for each object.

    Parameters
    ----------
    table : pandas.DataFrame
        DataFrame containing object records with a validity column.
    validity_col : str
        Column name containing the validity start times.

    Returns
    -------
    pandas.DataFrame
        DataFrame with duplicate ``diaObjectId`` rows removed, keeping the
        entry with the latest validity timestamp.
    """
    return table.sort_values(validity_col).drop_duplicates("diaObjectId", keep="last")


def append_mag_and_magerr(table: pd.DataFrame, flux_cols: list[str]) -> pd.DataFrame:
    """Compute AB magnitudes and magnitude errors from flux columns.

    For each flux column in ``flux_cols`` this function creates two new
    columns: the magnitude (``*Mag``) and its error (``*MagErr``).

    Parameters
    ----------
    table : pandas.DataFrame
        Table containing flux and flux error columns.
    flux_cols : list[str]
        Names of flux columns to convert (the function expects an
        accompanying ``<flux>Err`` column for errors).

    Returns
    -------
    pandas.DataFrame
        The original table with appended magnitude and magnitude-error
        columns (dtype float32).
    """
    mag_cols = {}
    for flux_col in flux_cols:
        flux_col_err = f"{flux_col}Err"
        mag_col = flux_col.replace("Flux", "Mag")
        mag_col_err = f"{mag_col}Err"

        # Convert flux to magnitude (nanojansky to AB magnitudes)
        flux = table[flux_col]
        mag = u.nJy.to(u.ABmag, flux)
        mag_cols[mag_col] = mag

        # Calculate magnitude error from flux errors
        flux_err = table[flux_col_err]
        upper_mag = u.nJy.to(u.ABmag, flux + flux_err)
        lower_mag = u.nJy.to(u.ABmag, flux - flux_err)
        mag_err = -(upper_mag - lower_mag) / 2
        mag_cols[mag_col_err] = mag_err

    mag_table = pd.DataFrame(mag_cols, dtype=pd.ArrowDtype(pa.float32()), index=table.index)
    return pd.concat([table, mag_table], axis=1)


def cast_columns_float32(table: pd.DataFrame, position_time_cols: tuple) -> pd.DataFrame:
    """Cast numeric columns (except positional/time) to float32.

    Parameters
    ----------
    table : pandas.DataFrame
        DataFrame to process.
    position_time_cols : tuple
        Columns to exclude from casting. Defaults to standard position/time
        columns used by the pipeline.

    Returns
    -------
    pandas.DataFrame
        A DataFrame with selected float64 columns cast to float32.
    """
    columns_to_cast = [
        field
        for (field, type) in table.dtypes.items()
        if field not in position_time_cols and type == pd.ArrowDtype(pa.float64())
    ]
    dtype_map = {col: pd.ArrowDtype(pa.float32()) for col in columns_to_cast}
    return table.astype(dtype_map)
