import subprocess
from pathlib import Path
from subprocess import CompletedProcess

from loguru import logger


class Sync:
    """A class to handle rclone sync operations."""

    def __init__(self, source: Path, destination: Path, config: Path, include_file: Path) -> None:
        self.source = source
        self.destination = destination
        self.config = config
        self.include_paths = include_file

    def sync(self, sync_flags: list[str]) -> CompletedProcess:
        """Syncs the source directory to the destination using rclone."""
        logger.info(f'Syncing {self.source} to {self.destination}')
        command = [
            'rclone',
            '--config',
            str(self.config),
            'sync',
            str(self.source),
            str(self.destination),
            *sync_flags,
            '--include-from',
            str(self.include_paths),
        ]
        logger.debug(f'Running command: {" ".join(command)}')
        rclone = subprocess.run(command, capture_output=True, text=True)
        if rclone.stdout:
            logger.debug(rclone.stdout)
        if rclone.stderr:
            logger.error(rclone.stderr) if rclone.returncode != 0 else logger.debug(rclone.stderr)
        rclone.check_returncode()
        return rclone
