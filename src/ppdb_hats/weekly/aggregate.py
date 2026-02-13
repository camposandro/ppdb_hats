"""Utilities to aggregate daily pixel data for weekly processing.

Merges all daily increments into a single aggregated catalog with
deduplicated objects and merged sources while maintaining provenance.
"""

import logging

import hats as hc
import numpy as np
import pandas as pd
from lsdb import Catalog
from lsdb.dask.merge_catalog_functions import (
    align_and_apply,
    align_catalogs,
    construct_catalog_args,
    filter_by_spatial_index_to_pixel,
    get_healpix_pixels_from_alignment,
)

logger = logging.getLogger(__name__)


def aggregate_object_data(dia_object_lc: Catalog) -> Catalog:
    """Aggregate daily object and source data into a single catalog.

    The aggregation pipeline performs the following steps:
    1. Align the catalog with itself to determine pixel organization.
    2. Join object records with their margin rows.
    3. Sort objects by validity, keeping the latest records per object.
    4. Re-nest sources in temporal order and filter margin rows.

    Parameters
    ----------
    dia_object_lc : lsdb.Catalog
        Input LSDB catalog containing daily increments.

    Returns
    -------
    lsdb.Catalog
        Aggregated catalog with deduplicated objects and sorted sources.
    """
    logger.info("Aggregating object data...")
    alignment = align_catalogs(dia_object_lc, dia_object_lc)
    _, pixels = get_healpix_pixels_from_alignment(alignment)
    joined_partitions = align_and_apply(
        [(dia_object_lc, pixels), (dia_object_lc.margin, pixels)],
        perform_join_on,
    )
    ddf, ddf_map, alignment = construct_catalog_args(
        joined_partitions,
        dia_object_lc._ddf._meta,
        alignment,
    )
    hc_catalog = hc.catalog.Catalog(
        dia_object_lc.hc_structure.catalog_info,
        alignment.pixel_tree,
        schema=dia_object_lc.original_schema,  # the schema is the same
        moc=alignment.moc,
    )
    return Catalog(ddf, ddf_map, hc_catalog)


def perform_join_on(df: pd.DataFrame, margin: pd.DataFrame, df_pixel, *args) -> pd.DataFrame:
    """Per-partition transformation to join and re-nest sources.

    This function is applied to each partition and performs:
    - Concatenation of the main partition and its margin.
    - Deduplication of objects by keeping the latest validity record.
    - Re-nesting of `diaSource` and `diaForcedSource` in temporal order.
    - Spatial filtering to remove rows that fall outside the pixel bounds.

    Parameters
    ----------
    df : pandas.DataFrame
        Main partition dataframe.
    margin : pandas.DataFrame
        Margin partition dataframe associated with `df`.
    df_pixel : object
        HEALPix pixel information used for spatial filtering. Must provide
        `order` and `pixel` attributes.
    *args
        Additional unused arguments.

    Returns
    -------
    pandas.DataFrame
        Processed partition with aggregated objects and nested sources.
    """
    original_cols = list(df.columns)

    # 1. Join df with margin (combine daily data)
    final_df = pd.concat([df, margin])

    # 2. Order each object by validityStart
    final_df = final_df.sort_values(["diaObjectId", "validityStartMjdTai"], ascending=[True, False])

    # 3. Get the sources for all the objects
    final_df["diaSource.diaObjectId"] = final_df["diaObjectId"]
    final_df["diaForcedSource.diaObjectId"] = final_df["diaObjectId"]
    sources = final_df["diaSource"].explode().sort_values(["midpointMjdTai"])
    fsources = final_df["diaForcedSource"].explode().sort_values(["midpointMjdTai"])

    # 4. Grab the latest row per object (deduplicate by keeping newest validity)
    _, latest_indices = np.unique(final_df["diaObjectId"], return_index=True)
    final_df = final_df.iloc[latest_indices]

    # 5. Drop the sources and join them again (sorted)
    final_df = final_df.drop(columns=["diaSource", "diaForcedSource"])
    final_df = final_df.join_nested(sources, "diaSource", on="diaObjectId")
    final_df = final_df.join_nested(fsources, "diaForcedSource", on="diaObjectId")

    # 6. Filter out points outside of the pixel (that are therefore in margin)
    final_df = filter_by_spatial_index_to_pixel(final_df, df_pixel.order, df_pixel.pixel)

    # 7. Make sure columns keep the same order
    return final_df[original_cols]
