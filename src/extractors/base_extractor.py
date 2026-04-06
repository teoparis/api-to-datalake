"""
Abstract base class for all extractors.

Every concrete extractor must implement two methods:

* ``validate_config`` — called at startup to catch configuration errors early.
* ``extract`` — performs the actual data pull and returns a ``pd.DataFrame``.
"""

from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd


class BaseExtractor(ABC):
    """Abstract base class that every extractor must subclass.

    Concrete subclasses live in the same package:

    * :class:`~src.extractors.rest_extractor.RESTExtractor`
    * :class:`~src.extractors.jdbc_extractor.JDBCExtractor`
    """

    @abstractmethod
    def validate_config(self, config: dict) -> None:
        """Validate the source configuration dict.

        Raise ``ValueError`` with a descriptive message if any required key is
        missing or holds an illegal value.  Called once during pipeline
        initialisation so that misconfigured sources are caught before the
        first extraction attempt.

        Args:
            config: Raw source configuration dict as parsed from
                    ``config/sources.yaml``.

        Raises:
            ValueError: If a required field is absent or invalid.
        """

    @abstractmethod
    def extract(
        self,
        config: dict,
        watermark: Optional[str] = None,
    ) -> pd.DataFrame:
        """Extract data from the source and return it as a DataFrame.

        Args:
            config: Source configuration dict (same structure validated by
                    :meth:`validate_config`).
            watermark: Last-seen watermark value from
                       :class:`~src.utils.watermark.WatermarkStore`, or
                       ``None`` for a full initial load.

        Returns:
            A :class:`pandas.DataFrame` containing the extracted records.
            The column ``_ingested_at`` (UTC datetime) must be present in
            every returned DataFrame.

        Raises:
            RuntimeError: On unrecoverable extraction errors.
        """
