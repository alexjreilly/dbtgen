from os import path, getcwd

MODULE_DIR = path.dirname(path.realpath(__file__))
PROJECT_ROOT = path.abspath(getcwd())

INPUT_MODELS_DIR = f'{getcwd()}/.dbtgen/'
TARGET_MODELS_DIR = path.abspath(f'{PROJECT_ROOT}/models')
TARGET_SOURCES_DIR = path.abspath(f'{PROJECT_ROOT}/sources')
TARGET_PACKAGE_SOURCES_DIR = path.abspath(f'{PROJECT_ROOT}/.export/sources/')

DBT_PROJECT_PATH = path.abspath(f'{PROJECT_ROOT}/dbt_project.yml')

SOURCE_DB_SELECTION_MAPPING = {
    'raw': {
        'database': '{env}_raw',
        'dbt_database_value': "{{ env_var('DBT_RAW_DB',  target.name ~ '_RAW') }}"
    },
    'fivetran': {
        'database': 'qa_fivetran',
        'dbt_database_value': "{{ env_var('DBT_FIVETRAN_DB',  target.name ~ '_FIVETRAN_ODS') }}"
    },
    'snowflake': {
        'database': 'snowflake',
        'dbt_database_value': 'snowflake'
    }
}
MODEL_DB_SELECTION_MAPPING = {
    'ods': '{env}_ods',
    'datamart': '{env}_datamart'
}

CLEAN_PATHS = [
    '*.dbtgen__*.yml'
]
