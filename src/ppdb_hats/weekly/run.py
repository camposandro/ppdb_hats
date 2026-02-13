"""Weekly PPDB catalog reprocessing pipeline.

Aggregates daily increments and reimports the entire catalog
for optimal data organization and query performance.
"""

import logging
from pathlib import Path

import lsdb
from dask.distributed import Client
from hats_import import pipeline_with_client
from hats_import.margin_cache.margin_cache_arguments import MarginCacheArguments

from ppdb_hats.pipeline import Pipeline
from ppdb_hats.weekly.aggregate import aggregate_object_data
from ppdb_hats.weekly.reimport import generate_collection, reimport_catalog

logger = logging.getLogger(__name__)


class WeeklyPipeline(Pipeline):
    """Reprocesses the entire PPDB catalog on a weekly cadence.

    Workflow:
    1. Load existing catalog with computed margins
    2. Aggregate all daily increments
    3. Deduplicate objects keeping latest validity records
    4. Re-nest sources in temporal order
    5. Reimport aggregated catalog with balanced partitioning
    6. Generate updated collection with indexes and margins
    """

    def run(self, client: Client, tmp_dir: Path) -> None:
        """Execute weekly reprocessing pipeline.

        Parameters
        ----------
        client : dask.distributed.Client
            Dask distributed client.
        tmp_dir : pathlib.Path
            Temporary directory for intermediate files.
        """
        ppdb_hats_dir = self.config.paths.ppdb_hats_dir

        # Step 1: Load collection with margins
        dia_object_lc = self._load_collection(client, tmp_dir)

        # Step 2: Aggregate daily increments
        aggregated_catalog = aggregate_object_data(dia_object_lc)

        # Step 3: Write aggregated catalog to disk for reimport
        self._write_catalog(tmp_dir, aggregated_catalog)

        # Step 4: Reimport aggregated catalog with hats-import,
        # to optimize partitioning and parquet specifications.
        reimport_catalog(client, tmp_dir, ppdb_hats_dir)

        # Step 5: Generate final collection
        generate_collection(client, ppdb_hats_dir)

        logger.info("Collection reprocessing complete.")

    def _load_collection(self, client: Client, tmp_dir: Path) -> lsdb.Catalog:
        """Create margin cache for a collection and open it.

        Parameters
        ----------
        client : dask.distributed.Client
            Dask client used to run the margin-cache pipeline.
        tmp_dir : pathlib.Path
            Directory where the margin-cache artifact will be written.

        Returns
        -------
        lsdb.Catalog
            LSDB catalog opened with the margin cache applied.
        """
        logger.info("Generating margin for catalog...")

        collection_dir = self.config.paths.dia_object_collection_dir
        margin_threshold = self.config.margin_threshold

        input_path = collection_dir / "dia_object_lc"

        margin_name = f"dia_object_lc_{margin_threshold}arcs"
        args = MarginCacheArguments(
            input_catalog_path=input_path,
            output_path=tmp_dir,
            margin_threshold=margin_threshold,
            output_artifact_name=margin_name,
            simple_progress_bar=True,
            resume=False,
        )
        pipeline_with_client(args, client)

        return lsdb.open_catalog(input_path, margin_cache=tmp_dir / margin_name)

    def _write_catalog(self, tmp_dir: Path, aggregated_catalog: lsdb.Catalog):
        """Write an aggregated LSDB catalog to disk.

        Parameters
        ----------
        tmp_dir : pathlib.Path
            Directory in which to write the aggregated catalog.
        aggregated_catalog : lsdb.Catalog
            Aggregated LSDB catalog to persist.
        """
        logger.info("Writing aggregated catalog to disk...")

        # Temporary fix: We currently cannot use `reimport_from_hats``
        # on a catalog with npix_suffix="/".
        aggregated_catalog.hc_structure.catalog_info.npix_suffix = ".parquet"

        aggregated_catalog.write_catalog(tmp_dir / "dia_object_lc", as_collection=False, overwrite=True)


def main():
    """Main entry point for weekly PPDB pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(module)s:%(funcName)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    WeeklyPipeline().execute()
