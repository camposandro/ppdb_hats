# PPDB_HATS

PPDB_HATS is a set of Python pipeline tools for "hatsifying" difference imaging catalog data from Rubin Prompt Processing (PPDB).

```text
src/ppdb_hats/
├─ daily/
│  ├─ run.py     # Main entry point for daily pipeline
│  └─ ...
├─ weekly/
│  ├─ run.py     # Main entry point for weekly pipeline
│  └─ ...
├─ pipeline.py   # Pipeline orchestration
└─ config.py     # Configuration (paths, import arguments, ...)
```

### Daily pipeline

Each night, we increment the existing catalog with new data:

- Import the new catalog data (`diaObject`, `diaSource`, `diaForcedSource`) into HATS.

- Apply post-processing (e.g. filter objects by latest validity start, add magnitudes from fluxes).

- Nest new sources and forced sources in each object, sorting them by timestamp.

- Write new daily parquet files to disk, update relevant HATS properties and skymaps.

This stage avoids rewriting pre-existing files, minimizing file I/O and partitioning changes.

### Weekly pipeline

Once a week, we reprocess the catalog, aggregating the pixel data:

- Deduplicate object rows (keeping the latest `diaObject`-level data for each object).

- Merge each object's source and forced source.

- Reimport according to a more balanced threshold argument.

- Generate collection with margin cache and index catalog.

## Installing and running

To install the package, execute:

```bash
pip install .
```

The pipelines can be run programmatically: 

```python
from ppdb_hats.daily.run import DailyPipeline
DailyPipeline().execute()
```

Or from the terminal via console scripts (`ppdb_hats_daily` and `ppdb_hats_weekly`), which are provided by the package entry points:

```bash
ppdb_hats_daily   
```

**Note:** The package is Rubin-specific and requires access to the PPDB data which is only available at USDF.

## Future improvements

When deduplicating object rows, if some of the most recent object-level data is missing and it existed previously, fill those missing (NaN values) with the latest available information.

## Acknowledgements

This project is supported by Schmidt Sciences.

This project is based upon work supported by the National Science Foundation under Grant No. AST-2003196.

This project acknowledges support from the DIRAC Institute in the Department of Astronomy at the University of Washington. The DIRAC Institute is supported through generous gifts from the Charles and Lisa Simonyi Fund for Arts and Sciences, and the Washington Research Foundation.