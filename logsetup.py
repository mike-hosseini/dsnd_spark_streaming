import logging
from pathlib import Path

PARENT_DIR: Path = Path(__file__).parents[0]
logging.config.fileConfig(PARENT_DIR / "logging.ini")
