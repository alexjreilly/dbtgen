import os
from ctypes import ArgumentError
from typing import Tuple

from .logger import CustomLogger
from ..params import (MODEL_DB_SELECTION_MAPPING, SOURCE_DB_SELECTION_MAPPING,
                      TARGET_MODELS_DIR)

logger = CustomLogger()


def path(node_selector: str) -> str:
    
    return  os.path.join(
        TARGET_MODELS_DIR, 
        node_selector.replace('.', '/')
    )


def namespace(model_path: str) -> str:
    """
    Generates a model identifier by separating folder paths with a '.'

    :param model_path: The file path location for the model
    :return: String used to identify the model
    """

    namespace = model_path \
            .replace(TARGET_MODELS_DIR, '') \
            .replace('/', '.') \
            .replace('\\', '.')

    if namespace.startswith('.'):
        namespace = namespace[1:]
        
    if namespace.startswith('models.'):
        namespace = namespace.replace('models.', '')

    return namespace


def database_and_schema(node_selector: str) -> Tuple[str, str]:
    """
    Return the database and schema from the node selection.
    If just database selected (e.g. ods), then return None.
    """

    if not node_selector or len(node_selector) == 0:
        return None, None

    selection = node_selector.split('.')

    if len(selection) == 1:
        schema = None

    elif len(selection) == 2:
        schema = selection[1].lower()       # Note, lower case applied

    elif len(selection) > 2:
        raise ArgumentError(
            '-s (--select): Value must be of the format [database].[schema]'
        )

    database = selection[0].lower()     # Note, lower case applied
    valid_dbs = list(SOURCE_DB_SELECTION_MAPPING.keys()) \
        + list(MODEL_DB_SELECTION_MAPPING.keys())

    if database not in valid_dbs:
        raise ArgumentError(
            f'Selected database {database} was not found in list of valid '
            f'databases for the project: {valid_dbs}'
        )

    return database, schema
