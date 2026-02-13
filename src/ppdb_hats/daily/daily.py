"""Pipeline for daily PPDB data ingestion."""

import logging
from pathlib import Path

import hats
import lsdb
from dask.distributed import Client

from ppdb_hats.daily.increment import update_metadata, update_skymaps, write_partitions
from ppdb_hats.daily.ingest import import_catalog
from ppdb_hats.daily.nest import load_sources_with_margin, nest_sources
from ppdb_hats.daily.paths import append_input_paths, get_paths
from ppdb_hats.daily.postprocess import postprocess_catalog
from ppdb_hats.pipeline import Pipeline

logger = logging.getLogger(__name__)


class DailyPipeline(Pipeline):
    """Daily increment pipeline.

    The pipeline imports new files from LSST PPDB, applies post-processing
    (validity filtering, magnitude computation, dtype optimization), nests
    sources into objects and writes updated partitions and metadata.
    """

    def run(self, client: Client, tmp_dir: Path):
        """Execute the daily increment workflow.

        Parameters
        ----------
        client : dask.distributed.Client
            Dask client to use for distributed operations.
        tmp_dir : pathlib.Path
            Temporary directory used for intermediate HATS artifacts.
        """
        cfg = self.config

        # Step 1: Open existing catalog
        catalog, existing_pixels, mapping_order = self._open_catalog()

        # Step 2: Get new input files
        object_files, source_files, fsource_files = self._get_paths()

        # Step 3: Import catalogs
        self._import_base_catalogs(
            client,
            tmp_dir,
            object_files,
            source_files,
            fsource_files,
            existing_pixels,
            mapping_order,
        )

        # Step 4: Post-process catalogs
        self._postprocess_base_catalogs(client, tmp_dir)

        # Step 5: Load sources with margin for nesting
        new_dia_object_lc = self._nest_sources(client, tmp_dir)

        # Step 7: Write partitions and update metadata
        results = write_partitions(new_dia_object_lc, catalog, mapping_order)
        new_pixels, new_counts, new_histograms = results
        update_skymaps(catalog, new_histograms, mapping_order)
        update_metadata(catalog, new_pixels, new_counts)

        # Step 8: Save provenance
        dia_object_collection_dir = cfg.paths.dia_object_collection_dir
        append_input_paths("dia_object", object_files, dia_object_collection_dir)
        append_input_paths("dia_source", source_files, dia_object_collection_dir)
        append_input_paths("dia_forced_source", fsource_files, dia_object_collection_dir)

        logger.info("Daily increment complete.")

    def _open_catalog(self) -> tuple:
        """Open a HATS collection and extract metadata used by the pipeline.

        Returns
        -------
        tuple
            ``(catalog, existing_pixels, mapping_order)`` where ``existing_pixels``
            is a list of ``(order, pixel)`` tuples and ``mapping_order`` is the
            skymap order from the catalog info.

        Raises
        ------
        ValueError
            If the catalog does not use leaf pixel directories (``npix_suffix"/"``).
        """
        catalog_path = self.config.paths.dia_object_collection_dir
        logger.info("Opening catalog at %s...", catalog_path)
        catalog = hats.read_hats(catalog_path)
        if catalog.catalog_info.npix_suffix != "/":
            raise ValueError("Catalog must have leaf pixel directories (npix_suffix='/')")
        existing_pixels = [(p.order, p.pixel) for p in catalog.get_healpix_pixels()]
        mapping_order = catalog.catalog_info.skymap_order
        return catalog, existing_pixels, mapping_order

    def _get_paths(self) -> tuple:
        """Obtain new input parquet paths for each dataset type.

        Returns
        -------
        tuple
            Tuple of lists: ``(object_files, source_files, fsource_files)`` containing
            new file paths for each dataset type.
        """
        ppdb_lsst_dir = self.config.paths.ppdb_lsst_dir
        dia_object_collection_dir = self.config.paths.dia_object_collection_dir

        object_files = get_paths("dia_object", ppdb_lsst_dir, dia_object_collection_dir)
        source_files = get_paths("dia_source", ppdb_lsst_dir, dia_object_collection_dir)
        fsource_files = get_paths("dia_forced_source", ppdb_lsst_dir, dia_object_collection_dir)

        logger.info("Found %d new object files", len(object_files))
        logger.info("Found %d new source files", len(source_files))
        logger.info("Found %d new forced source files", len(fsource_files))

        return object_files, source_files, fsource_files

    def _import_base_catalogs(
        self,
        client: Client,
        tmp_dir: Path,
        object_files: list,
        source_files: list,
        fsource_files: list,
        existing_pixels: list,
        mapping_order: int,
    ):
        """Import dia_object, dia_source and dia_forced_source catalogs.

        Parameters
        ----------
        client : dask.distributed.Client
            Dask client for import pipelines.
        tmp_dir : pathlib.Path
            Temporary directory for outputs.
        object_files, source_files, fsource_files : list
            Lists of input parquet file paths for each dataset.
        existing_pixels : list
            Existing HEALPix pixels present in the catalog.
        mapping_order : int
            Highest HEALPix order used by the existing catalog.
        """
        catalogs = [
            ("dia_object", object_files),
            ("dia_source", source_files),
            ("dia_forced_source", fsource_files),
        ]

        for dataset_type, files in catalogs:
            if not files:
                continue
            import_catalog(
                client,
                tmp_dir,
                dataset_type,
                files,
                self.config.import_config,
                existing_pixels=existing_pixels,
                highest_healpix_order=mapping_order,
            )

    def _postprocess_base_catalogs(self, client: Client, tmp_dir: Path):
        """Run post-processing for imported catalogs.

        Parameters
        ----------
        client : dask.distributed.Client
            Dask client used to schedule partition processing.
        tmp_dir : pathlib.Path
            Directory containing the imported HATS catalogs to process.
        """
        position_time_cols = self.config.postprocess_config.position_time_cols

        # dia_object: filter validity, calculate magnitudes, optimize types
        postprocess_catalog(
            client,
            tmp_dir,
            "dia_object",
            position_time_cols=position_time_cols,
            validity_col="validityStartMjdTai",
            flux_colnames=[f"{band}_scienceFluxMean" for band in "ugrizy"],
        )

        # dia_source: calculate magnitudes, optimize types
        postprocess_catalog(
            client,
            tmp_dir,
            "dia_source",
            position_time_cols=position_time_cols,
            flux_colnames=["scienceFlux"],
        )

        # dia_forced_source: calculate magnitudes, optimize types
        postprocess_catalog(
            client,
            tmp_dir,
            "dia_forced_source",
            position_time_cols=position_time_cols,
            flux_colnames=["scienceFlux"],
        )

    def _nest_sources(self, client, tmp_dir) -> lsdb.Catalog:
        """Nest dia_source and dia_forced_source catalogs into dia_object.

        Parameters
        ----------
        client : dask.distributed.Client
            Dask client used to schedule nesting.
        tmp_dir : pathlib.Path
            Directory containing the imported HATS catalogs to nest.
        """
        margin_threshold = self.config.nest_config.margin_threshold

        dia_object = lsdb.open_catalog(tmp_dir / "dia_object")
        dia_source = load_sources_with_margin(client, tmp_dir, "dia_source", margin_threshold)
        dia_forced_source = load_sources_with_margin(client, tmp_dir, "dia_forced_source", margin_threshold)

        # Temporary fix: Filter invalid sources (sources without object associations)
        dia_source = dia_source[~dia_source["diaObjectId"].isna()]
        dia_forced_source = dia_forced_source[~dia_forced_source["diaObjectId"].isna()]

        return nest_sources(dia_object, dia_source, dia_forced_source)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(module)s:%(funcName)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    DailyPipeline().execute()
