"""Base pipeline orchestration for PPDB data processing.

This module provides an abstract ``Pipeline`` base class that handles
common orchestration tasks such as creating a Dask client, managing a
temporary directory, and invoking the pipeline-specific ``run`` method.
"""

import logging
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path

from dask.distributed import Client

from ppdb_hats.config import PipelineConfig, get_default_config

logger = logging.getLogger(__name__)


class Pipeline(ABC):
    """Abstract base class for PPDB pipelines.

    The :class:`Pipeline` class centralizes orchestration concerns so
    concrete pipelines implement only their domain logic in ``run``.

    Attributes
    ----------
    config : PipelineConfig
        Top-level pipeline configuration instance used by the pipeline.
    """

    def __init__(self, config: PipelineConfig = None):
        """Initialize pipeline with configuration.

        Parameters
        ----------
        config : PipelineConfig, optional
            Pipeline configuration. If ``None``, :func:`get_default_config`
            is used to provide sane defaults.
        """
        self.config = config or get_default_config()

    def execute(self, *args, **kwargs):
        """Execute the pipeline with a Dask client and temporary directory.

        The method creates a temporary directory and a Dask client and
        then delegates to :meth:`run`.

        Parameters
        ----------
        *args, **kwargs
            Passed through to :meth:`run` implemented by subclasses.
        """
        logger.info("Starting pipeline...")
        with tempfile.TemporaryDirectory() as tmp_dir:
            self._run_with_client(Path(tmp_dir), *args, **kwargs)

    def _run_with_client(self, tmp_dir: Path, *args, **kwargs):
        """Create a Dask client and execute the pipeline.

        Parameters
        ----------
        tmp_dir : pathlib.Path
            Temporary directory used as the Dask local directory.
        *args, **kwargs
            Passed through to :meth:`run`.
        """
        dask_cfg = self.config.dask
        with Client(
            n_workers=dask_cfg.n_workers,
            memory_limit=dask_cfg.memory_limit,
            threads_per_worker=dask_cfg.threads_per_worker,
            local_directory=str(tmp_dir),
        ) as client:
            logger.info("Tmp dir: %s", tmp_dir)
            logger.info("Dask client: %s", client)
            self.run(client, tmp_dir, *args, **kwargs)

    @abstractmethod
    def run(self, client: Client, tmp_dir: Path, *args, **kwargs):
        """Execute pipeline-specific logic.

        Subclasses must implement this method.

        Parameters
        ----------
        client : dask.distributed.Client
            The Dask client to use for distributed operations.
        tmp_dir : pathlib.Path
            Temporary directory for intermediate files.
        *args, **kwargs
            Extra parameters specific to concrete pipelines.
        """
        raise NotImplementedError("Subclasses must implement the run method.")


def execute_pipeline(pipeline: Pipeline, *args, **kwargs):
    """Convenience wrapper to execute a :class:`Pipeline` instance.

    Parameters
    ----------
    pipeline : Pipeline
        Pipeline instance to execute.
    *args, **kwargs
        Passed to :meth:`Pipeline.execute`.
    """
    pipeline.execute(*args, **kwargs)
