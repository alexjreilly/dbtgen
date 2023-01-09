from os import getcwd, remove
from pathlib import Path

from . import params
from .libs.logger import CustomLogger

logger = CustomLogger()


def main(args):

    logger.info("Cleaning up files")

    for clean_target in params.CLEAN_PATHS:
        for f in Path(getcwd()).rglob(clean_target):

            log_file_path = f"models{str(f).replace(params.TARGET_MODELS_DIR, '')}"
            logger.info(f"  Removing {log_file_path}")
            remove(f)
