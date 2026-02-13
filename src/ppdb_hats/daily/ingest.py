"""Provides utilities for importing PPDB DIA base catalogs into HATS format."""

import logging
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from hats_import import pipeline_with_client
from hats_import.catalog.arguments import ImportArguments
from lsst.resources import ResourcePath
from ppdb.config import ImportConfig

logger = logging.getLogger(__name__)


def import_catalog(
    client,
    output_dir: Path,
    dataset_type: str,
    input_file_list: list,
    import_config: ImportConfig,
    existing_pixels: Optional[list] = None,
    highest_healpix_order: Optional[int] = None,
):
    """Import DIA catalogs into HATS format.

    Parameters
    ----------
    client
        Dask distributed client used to run the import pipeline.
    output_dir : pathlib.Path
        Output directory where the HATS catalog will be written.
    dataset_type : str
        Dataset identifier (``'dia_object'``, ``'dia_source'``, or ``'dia_forced_source'``).
    input_file_list : list
        List of input parquet file paths to import.
    import_config : ImportConfig
        Import-specific settings.
    existing_pixels : list or None, optional
        Existing HEALPix pixels in the target catalog (order, pixel tuples).
    highest_healpix_order : int or None, optional
        Highest HEALPix order present in the existing catalog.
    """
    logger.info("Importing %s...", dataset_type)

    schema_filepath = _download_schema(dataset_type, input_file_list[0], output_dir)

    args = ImportArguments(
        output_path=output_dir,
        output_artifact_name=dataset_type,
        input_file_list=input_file_list,
        file_reader=import_config.file_reader,
        ra_column=import_config.ra_column,
        dec_column=import_config.dec_column,
        catalog_type=dataset_type.rsplit("_", 1)[-1],  # "object" or "source"
        byte_pixel_threshold=import_config.byte_pixel_threshold,
        use_schema_file=schema_filepath,
        simple_progress_bar=import_config.simple_progress_bar,
        resume=import_config.resume,
    )

    # Add optional HEALPix parameters if provided
    if existing_pixels is not None:
        args.existing_pixels = existing_pixels
    if highest_healpix_order is not None:
        args.highest_healpix_order = highest_healpix_order

    pipeline_with_client(args, client)


def _download_schema(dataset_type: str, single_parquet_path, output_dir: Path) -> Path:
    """Extract a schema from a sample Parquet file and write it out.

    The schema is written as an empty Parquet file containing the same
    column metadata; the resulting file can be supplied to the import
    pipeline as a schema hint.

    Parameters
    ----------
    dataset_type : str
        Dataset being imported (used to name the schema file).
    single_parquet_path
        Path to a representative parquet file to read the schema from.
    output_dir : pathlib.Path
        Directory where the schema file will be written.

    Returns
    -------
    pathlib.Path
        Path to the written schema parquet file.
    """
    logger.info("Downloading schema for %s...", dataset_type)
    with ResourcePath(single_parquet_path).open("rb") as file:
        schema = pq.read_schema(file).remove_metadata()
    schema_table = pa.table({field.name: pa.array([], type=field.type) for field in schema})
    schema_filepath = output_dir / f"{dataset_type}_schema.parquet"
    pq.write_table(schema_table, schema_filepath)
    return schema_filepath
