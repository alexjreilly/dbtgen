from os import path

MODULE_DIR = path.dirname(path.realpath(__file__))
PROJECT_ROOT = path.abspath(f'{MODULE_DIR}/../../')

INPUT_MODELS_DIR = path.abspath(f'{MODULE_DIR}/../models')
INPUT_DOCS_DIR = path.abspath(f'{PROJECT_ROOT}/docs/doc_blocks')

TARGET_MODELS_DIR = path.abspath(f'{PROJECT_ROOT}/models')
TARGET_SOURCES_DIR = path.abspath(f'{PROJECT_ROOT}/sources')
TARGET_PACKAGE_SOURCES_DIR = path.abspath(f'{PROJECT_ROOT}/.export/sources/')
TARGET_PACKAGE_DOCS_DIR = path.abspath(f'{PROJECT_ROOT}/.export/docs/')

DBT_PROJECT_PATH = path.abspath(f'{PROJECT_ROOT}/dbt_project.yml')
DBT_PROJECT_NAME = 'bia'
DBT_PROFILE_DEFAULT = 'default'

SOURCE_DB_SELECTION_MAPPING = {
    'raw': {
        'database': '{env}_raw',
        'dbt_database_value': "{{ env_var('DBT_RAW_DB',  target.name ~ '_RAW') }}"
    },
    'fivetran': {
        'database': 'qa_fivetran',
        'dbt_database_value': "{{ env_var('DBT_FIVETRAN_DB',  target.name ~ '_FIVETRAN_ODS') }}"
    }
}
MODEL_DB_SELECTION_MAPPING = {
    'ods': '{env}_ods',
    'datamart': '{env}_datamart'
}

CLEAN_PATHS = [
    '*.dbtgen__*.yml'
]
