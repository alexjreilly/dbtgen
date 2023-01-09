import os
import sys
from typing import Tuple
import snowflake.connector
import yaml
from dbt.utils import deep_merge

from .libs import node, profile
from .libs.file_handler import list_files_in_dir
from .libs.logger import CustomLogger
from .libs.yaml_handler import BaseDumper

logger = CustomLogger()


def calculate_vars(args) -> Tuple[str, str]:

    """
    Derives the model properties file path and the Snowflake target schema 
    
    :returns: File path for model properties file, Snowflake schema to use
    """

    if len(args.select.split('.')) != 2:
        raise RuntimeError(
            'Must provide `-s` (`--select`) in the format [database].[schema]'
        )

    db_suffix, schema = node.database_and_schema(args.select)
    db = f"{args.target}_{db_suffix}"

    properties_file = os.path.join(
        node.path(args.select),
        f"{args.select.split('.')[-1]}.yml"
    )

    return properties_file, f"{db}.{schema}"


def get_recency(
        con,
        target_schema: str,
        updated_at_field: str,
        use_tables: bool = False,
        use_views: bool = False
) -> dict:

    use_objects = []

    if not(use_tables and use_views):
        logger.info(
            'Finding all models, `--use-views` or `--use-tables` not provided'
        )
        use_objects.extend(['table', 'view'])

    else:
        if use_tables:
            use_objects.append('table')
        if use_views:
            use_objects.append('view')

    object_filter = ', '.join(
        [ f"'{o.upper()}'" for o in use_objects ]
    )

    def query__show_objects():
        return f"SHOW OBJECTS IN SCHEMA {target_schema}"

    def query__filter_objects():
        return \
            f'SELECT "database_name", "schema_name", "name" ' \
            f'FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) ' \
            f'WHERE "kind" IN({object_filter})'

    def query__get_recency(objects):
        return ' UNION '.join(
            [
                f"SELECT LOWER(CONCAT_WS('_', '{obj[1]}', '{obj[2]}')) AS src, "
                f"DATEDIFF("
                f"  day, MAX({updated_at_field}), CURRENT_TIMESTAMP()"
                f") AS recency_in_days "
                f"FROM {obj[0]}.{obj[1]}.{obj[2]}" 
                    for obj in objects
            ]
        )

    with con.cursor() as cur:

        try:
            logger.info(f'Finding models in: {target_schema.upper()}')

            cur.execute(query__show_objects())
            models = cur.execute(query__filter_objects()).fetchall()
            if not models:
                logger.warn('[WARNING] No models found')
                sys.exit(1)

            logger.status('Calculating data recency', 'RUN')
            recency = cur.execute(query__get_recency(models)).fetchall()
            logger.status('Calculating data recency', 'DONE')

        except snowflake.connector.errors.ProgrammingError as err:
            logger.error(err.msg)
            sys.exit(1)

    return recency


def calculate_warn_error_thresholds(
        model_recency_in_days: list,
        model_properties_path: str
) -> list[dict]:

    days_interval = [0, 1, 2, 7, 30, 60, 90, 180]

    model_files = list_files_in_dir(
        os.path.dirname(model_properties_path),
        filter_extension='.sql',
        include_extension=False
    )

    thresholds = []

    for model, recency_days in model_recency_in_days:
        if model in model_files:
            
            for i in range(0, len(days_interval) - 1, 1):
                warn_days = None
                error_days = None

                if recency_days or recency_days == 0:
                    if days_interval[i] <= recency_days < days_interval[i + 1]:
                        warn_days = days_interval[i + 1]
                        if i + 2 < len(days_interval):
                            error_days = days_interval[i + 2] 
                        break

            thresholds.append(
                {
                    'name': model,
                    'warn_days': warn_days,
                    'error_days': error_days
                }
            )

    return thresholds


def generate_schema_tests(
        thresholds: list[dict],
        updated_at_field: str,
        warn_only: bool = False
) -> list[dict]:

    model_properties = []
    severities = ['warn']
    if not warn_only:
        severities.append('error')

    for threshold in thresholds:
        m_property = {
            'name': threshold['name'],
        }

        if threshold['warn_days'] or threshold['error_days']:
            m_property['tests'] = []

        for severity in severities:

            if threshold[f'{severity}_days']:
                m_property['tests'].append(
                    {
                        'dbt_utils.recency': {
                            'datepart': 'day',
                            'field': updated_at_field,
                            'interval': threshold[f'{severity}_days'],
                            'tags': ['recency'],
                            'config': {'severity': severity}
                        }
                    }
                )

        m_property['columns'] = [ 
            { 
                'name': 'sys_hash_key',
                'description': '{{ doc("sys_hash_key") }}',
                'tests': ['not_null', 'unique']
            }
        ]
        m_property['columns'].extend(
            [
                { 
                    'name': col,
                    'description': f'{{ doc("{col}") }}',
                } for col in ['sys_created', 'sys_modified', 'sys_job_runid']
            ]
        )

        model_properties.append(m_property)

    return {
        'version': 2,
        'models': model_properties
    }


def generate_model_properties(
    model_properties: dict, 
    model_properties_file_path: str
):

    # TODO: Read existing yaml file and merge contents

    # try:
    #     existing_model_properties = read_yaml_file(MODEL_PROPERTIES_FILE_PATH)
    # except FileNotFoundError:
    #     existing_model_properties = {}

    # merged_model_properites = deep_merge(existing_model_properties, model_properties)

    output_path = os.path.join(
        os.path.dirname(model_properties_file_path),
        f'.dbtgen__{os.path.basename(model_properties_file_path)}'
    )

    with open(output_path, 'w') as models_file:
        yaml.dump(
            model_properties,
            models_file,
            Dumper=BaseDumper,
            default_flow_style=False, 
            sort_keys=False
        )


def main(args):

    model_properties_file_path, target_schema = calculate_vars(args)

    sf_connection = profile.snowflake_connect(args.profile)

    model_recency = get_recency(
        sf_connection,
        target_schema,
        args.updated_at_field,
        args.use_tables,
        args.use_views
    )

    logger.info("Generating model properties file")
    logger.status(node.namespace(model_properties_file_path), 'RUN')

    model_recency_thresholds = calculate_warn_error_thresholds(
        model_recency,
        model_properties_file_path
    )
    model_properties = generate_schema_tests(
        model_recency_thresholds,
        args.updated_at_field,
        args.warn_only
    )
    generate_model_properties(
        model_properties,
        model_properties_file_path
    )

    logger.status(node.namespace(model_properties_file_path), 'DONE')
