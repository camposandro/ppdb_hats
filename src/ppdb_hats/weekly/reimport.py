"""Weekly reimport and collection generation for PPDB.

Handles aggregation of daily data and reimport of the entire catalog
on a weekly cycle to maintain optimal data organization.
"""

import logging
from pathlib import Path

import hats_import.collection.run_import as collection_runner
from dask.distributed import Client
from hats_import import CollectionArguments, ImportArguments, pipeline_with_client

logger = logging.getLogger(__name__)


def reimport_catalog(
    client: Client,
    tmp_dir: Path,
    ppdb_hats_dir: Path,
) -> None:
    """Reimport an aggregated catalog into a balanced collection.

    Parameters
    ----------
    client : dask.distributed.Client
        Dask client for the reimport pipeline.
    tmp_dir : pathlib.Path
        Directory containing the aggregated catalog to reimport.
    ppdb_hats_dir : pathlib.Path
        Base PPDB HATS directory where the reimported collection will be written.
    """
    logger.info("Reimporting catalog...")
    args = ImportArguments.reimport_from_hats(
        path=tmp_dir / "dia_object_lc",
        output_dir=ppdb_hats_dir / "dia_object_collection_reimport",
        output_artifact_name="dia_object_lc",
        byte_pixel_threshold=1 << 30,
        skymap_alt_orders=[2, 4, 6],
        npix_suffix="/",
    )
    pipeline_with_client(args, client)


def generate_collection(
    client: Client,
    ppdb_hats_dir: Path,
) -> None:
    """Generate a final HATS collection with indexes and margins.

    Parameters
    ----------
    client : dask.distributed.Client
        Dask client used to run collection generation.
    ppdb_hats_dir : pathlib.Path
        Base PPDB HATS directory where the collection will be created.
    """
    logger.info("Generating collection...")
    collection_args = (
        CollectionArguments(
            output_path=ppdb_hats_dir,
            output_artifact_name="dia_object_collection_reimport",
        )
        .catalog(catalog_path=ppdb_hats_dir / "dia_object_collection_reimport" / "dia_object_lc")
        .add_margin(margin_threshold=10)
        .add_index(indexing_column="diaObjectId")
    )
    collection_runner.run(collection_args, client)
