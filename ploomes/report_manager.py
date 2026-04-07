"""Report file path management for Ploomes scripts.

Organizes CSV audit trails by operation type and date in a consistent structure:
    reports/{operation_type}/YYYY/MM/{operation_type}_{identifier}_{timestamp}.csv

Usage:
    manager = ReportManager("move_duplicate_deals", pipeline_id=110067326)
    path = manager.get_path()  # reports/move_duplicate_deals/2026/04/...csv
    manager.ensure_dir()       # Create directories if needed
"""

import os
from datetime import datetime
from pathlib import Path


class ReportManager:
    """Manages report file paths with automatic directory creation."""

    REPORTS_DIR = "reports"

    def __init__(
        self,
        operation_type: str,
        identifier: str = "",
        timestamp: str = "",
    ) -> None:
        """Initialize report manager.

        Args:
            operation_type: Operation type (e.g., 'move_duplicate_deals', 'delete_orphan_deals')
            identifier: Optional identifier (e.g., pipeline_id, contact_id, timestamp component)
            timestamp: Optional timestamp. Defaults to current datetime in YYYYMMDD_HHMMSS format.
        """
        self.operation_type = operation_type
        self.identifier = identifier
        self.timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")

    def get_path(self) -> str:
        """Returns the full path to the report CSV file.

        Structure: reports/{operation_type}/YYYY/MM/{operation_type}_{identifier}_{timestamp}.csv
        Or: reports/{operation_type}/YYYY/MM/{operation_type}_{timestamp}.csv (if no identifier)

        Returns:
            str: Absolute or relative path to report file.
        """
        now = datetime.now()
        year = now.strftime("%Y")
        month = now.strftime("%m")

        filename_parts = [self.operation_type]
        if self.identifier:
            filename_parts.append(str(self.identifier))
        filename_parts.append(self.timestamp)
        filename = "_".join(filename_parts) + ".csv"

        path = os.path.join(
            self.REPORTS_DIR,
            self.operation_type,
            year,
            month,
            filename,
        )
        return path

    def ensure_dir(self) -> None:
        """Create directory structure if it doesn't exist."""
        parent_dir = os.path.dirname(self.get_path())
        Path(parent_dir).mkdir(parents=True, exist_ok=True)

    def get_full_path(self) -> str:
        """Returns absolute path to the report file, creating directories if needed."""
        self.ensure_dir()
        return os.path.abspath(self.get_path())
