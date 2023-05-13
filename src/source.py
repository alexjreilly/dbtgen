import sys
from typing import Union
import snowflake.connector

from src.libs import node, profile, source
from src.libs.logger import CustomLogger
from src.params import SOURCE_DB_SELECTION_MAPPING, TARGET_SOURCES_DIR

"""
    Generated dbt source files
"""

logger = CustomLogger()


def get_snowflake_tables(
    con,
    database,
    schemas: Union[str, list] = '*',
    loaded_at_field: str = None,
    get_freshness: bool = False,
    use_tables: bool = True,
    use_views: bool = False
) -> list:

    use_objects = []

    if use_tables:
        use_objects.append('table')
    if use_views:
        use_objects.append('view')

    object_filter = ', '.join(
        [ f"'{o.upper()}'" for o in use_objects ]
    )

    if isinstance(schemas, list):
        schemas_cs = ["'{s}'" for s in schemas]
        schema_filter = f'LOWER("schema_name") IN({", ".join(schemas_cs)})'
    else:
        if schemas == '*':
            schema_filter = '1=1'   # dummy filter
        else:
            schema_filter = f'LOWER("schema_name") = \'{schemas}\''

    def query__show_objects():
        return f"SHOW OBJECTS IN DATABASE {database}"

    def query__filter_objects():
        return \
            f'SELECT "database_name", "schema_name", "name" ' \
            f'FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) ' \
            f'WHERE "kind" IN({object_filter}) AND {schema_filter}'

    def query__get_recency(objects, filter_last_n_days: int = 180):
        """
        Get time in days between 'load_at_field', with results filtered to 
        include only the last n days
        """
        return ' UNION '.join(
            [
                f"SELECT '{obj['database_name']}' AS \"database_name\", " 
                f"'{obj['schema_name']}' AS \"schema_name\", "
                f"'{obj['name']}' AS \"name\", "
                f"AVG(diff_days)::float AS \"avg_freshness_in_days\" FROM ("
                f"SELECT DATEDIFF(day, LAG(ts) OVER (ORDER BY ts), ts) "
                f"AS diff_days FROM "
                f"(SELECT DISTINCT {loaded_at_field}::date AS ts "
                f"FROM {obj['database_name']}.{obj['schema_name']}.{obj['name']} "
                f"WHERE ts >= DATEADD(day, -{filter_last_n_days}, "
                f"CURRENT_TIMESTAMP())"
                f"))" for obj in objects
            ]
        )

    with con.cursor(snowflake.connector.DictCursor) as cur:

        try:
            logger.info(f'Finding sources in: {database.upper()}')

            cur.execute(query__show_objects())
            src_objects = cur.execute(query__filter_objects()).fetchall()

            if not src_objects:
                logger.warn('[WARNING] No sources found')
                sys.exit(1)

            if get_freshness:
                logger.status('Calculating source freshness', 'RUN')
                src_objects_with_freshness = \
                    cur.execute(query__get_recency(src_objects)).fetchall()
                logger.status('Calculating source freshness', 'DONE')

        except snowflake.connector.errors.ProgrammingError as err:
            logger.error(err.msg)
            sys.exit(1)

    return src_objects_with_freshness if get_freshness else src_objects


def main(args):

    selected_db, selected_schema = node.database_and_schema(args.select)

    src_db_mapping = { 
        selected_db: SOURCE_DB_SELECTION_MAPPING[selected_db]
    } if selected_db else SOURCE_DB_SELECTION_MAPPING

    all_src_dbs = {
        k: {
            i: j.format(env=args.target) \
                if i == 'database' else j for i, j in v.items()
        } for k, v in src_db_mapping.items()
    }

    sf_connection = profile.snowflake_connect(args.profile)

    for db__key, db__config in all_src_dbs.items():

        source_tables = get_snowflake_tables(
            sf_connection,
            database=db__config['database'], 
            schemas=selected_schema if selected_schema else '*',
            loaded_at_field=args.loaded_at_field,
            get_freshness=args.get_freshness
        )

        sources_grouped_by_schema = {}

        for source_table in source_tables:

            table = {
                'name': source_table['name'].lower()
            }
            if args.get_freshness:
                table['freshness'] = source_table['avg_freshness_in_days']

            sources_grouped_by_schema.setdefault(
                f"{source_table['schema_name']}".lower(), []
            ).append(table)

        for schema in list(sources_grouped_by_schema.keys()):

            src = source.Source(
                name=schema,
                database=db__config['dbt_database_value'],
                schema=schema,
                tables=sources_grouped_by_schema[schema]
            )

            src.write(
                f"{TARGET_SOURCES_DIR}/{db__key}/", overwrite=args.overwrite)
