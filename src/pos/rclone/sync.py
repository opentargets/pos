import subprocess
from pathlib import Path
from subprocess import CompletedProcess

from loguru import logger


class Sync:
    """A class to handle rclone sync operations."""

    def __init__(self, source: Path, destination: Path, config: Path) -> None:
        self.source = source
        self.destination = destination
        self.config = config

    def sync(self, dry_run: bool = False) -> CompletedProcess:
        """Syncs the source directory to the destination using rclone."""
        logger.info(f'Syncing {self.source} to {self.destination}')
        return subprocess.run(
            ['rclone', 'sync', '--config', str(self.config), str(self.source), str(self.destination)],
            capture_output=True,
            text=True,
        )
