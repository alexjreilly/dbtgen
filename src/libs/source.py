from os import makedirs, path
from typing import Tuple
import yaml

from .logger import CustomLogger
from .yaml_handler import read_yaml_file, BaseDumper, QuotedString

logger = CustomLogger()


class Source:
    """
    Creates an object for a dbt source
    """
    def __init__(
        self,
        name: str,
        database: str,
        tables: list,
        schema: str = None,     # same as name, by default
        **kwargs
    ):
        BaseDumper.add_representer(QuotedString, BaseDumper.quoted_scalar)

        self.name = name

        self.contents = {
            'version': 2,
            'sources': [
                {
                    'name': self.name,
                    'database': QuotedString(database),
                    'schema': schema if schema else name,
                    **kwargs,
                    'tables': tables
                }
            ]
        }

    def write(self, target_dir: str, overwrite: bool = False):
        """
        Writes the source object contents to a yaml file

        :param target_dir: Local path to folder to write the file        
        """

        file_name = f'.dbtgen__{self.name}' if not overwrite else self.name
        file_path = path.join(target_dir, f'{file_name}.yml')

        if not path.exists(target_dir):
            makedirs(target_dir)

        with open(file_path, 'w') as outfile:
            yaml.dump(
                self.contents, 
                outfile, 
                Dumper=BaseDumper,
                default_flow_style=False, 
                sort_keys=False
            )
        logger.status(file_name, 'CREATED')


class SourceFactory:

    """
    A class used for the creation of dbt source objects
    """

    def __init__(
        self,
        name: str,
        database: str,
        tables: list = []
    ):
        self.database = database
        self.source_name = name
        self.tables = tables

        self.source = Source(self.source_name, self.database, self.tables)

    @staticmethod
    def _strip_prefix_from_model_name(model_name: str, prefix: str) -> str:
        """
        Removes prefix substring from the model name
        """
        if model_name.startswith(prefix):
            return model_name.replace(prefix, '', 1)
        return model_name

    @staticmethod
    def _get_most_common_recency_test(models: list) -> Tuple[dict, dict]:
        """
        Returns a tuple of two dictionaries, each representing the most common 
        dbt recency tests with severity warn and error.

        :param models: List of dbt model properties
        :returns: Two dictionaries of dbt recency tests for warn and error
        """

        warn = []
        error = []

        for model in models:
            if isinstance(model.get('tests'), list):
                for test in model['tests']:
                    if 'dbt_utils.recency' in test:
                        recency_test = test['dbt_utils.recency']
                        if recency_test['config']['severity'] == 'warn':
                            warn.append(test)
                        elif recency_test['config']['severity'] == 'error':
                            error.append(test)

        def most_frequent_dict(list_in: list[dict]) -> dict:

            if len(list_in) > 0:
                counter = 0
                most_freq = list_in[0]
                
                for i in list_in:
                    count_current = list_in.count(i)
                    if count_current > counter:
                        counter = count_current
                        most_freq = i
            
                return most_freq

            return {}

        most_freq_warn = most_frequent_dict(warn)
        most_freq_error = most_frequent_dict(error)

        return most_freq_warn, most_freq_error

    @staticmethod
    def _recency_test_to_freshness(recency_test: dict) -> dict:
        """
        Takes a dictionary with a dbt recency test format and converts into a 
        dictionary for a dbt freshness

        :param recency_test: Dictionary with the following structure (example)

            {
                'dbt_utils.recency':
                    'datepart': 'day'
                    'field': 'sys_modified'
                    'interval': 30
                    'config': {
                        'severity': 'warn'
                    }
            }

        :returns: Dictionary with the following structure (example)

            {
                'loaded_at_field': 'sys_modified'
                'freshness':
                    'warn_after': {
                        'count': 30
                        'period': 'day'
                    }
            }
        """

        out_dict = {}

        if recency_test.get('dbt_utils.recency'):
            recency = recency_test['dbt_utils.recency']

            if recency['config']['severity'] in ['warn', 'error']:

                severity = recency['config']['severity']

                out_dict['freshness'] = {}
                out_dict['freshness'][f'{severity}_after'] = {
                    'count': recency['interval'], 
                    'period': recency['datepart']
                }
                out_dict['loaded_at_field'] = recency['field']

        return out_dict

    @staticmethod
    def _get_global_freshness(models: dict) -> Tuple[str, dict]:
        """
        Returns both the loaded_at_timestamp and source freshness as a 
        dictionary

        :param models: Dictionary with contents of a model properties file
        :returns: loaded_at_timestamp, freshness
        """
        
        recency_warn, recency_error = \
            SourceFactory._get_most_common_recency_test(
                models['models']
            )

        warn = SourceFactory._recency_test_to_freshness(recency_warn)
        error = SourceFactory._recency_test_to_freshness(recency_error)

        # Loaded at fields the same or not both empty
        if warn.get('loaded_at_field') == error.get('loaded_at_field') \
            or not(warn.get('loaded_at_field') and error.get('loaded_at_field')):

            freshness = {}

            if warn.get('freshness'):
                freshness.update(warn.get('freshness'))
            if error.get('freshness'):
                freshness.update(error.get('freshness'))
                    
            return warn.get('loaded_at_field', error.get('loaded_at')), \
                {'freshness': freshness}

        return None, {'freshness': {}}

    @staticmethod
    def _get_source_model_freshness(
        model: dict, 
        loaded_at_field: str = None,
        ignore: dict = {}
    ) -> Tuple[str, dict]:
        """
        Calculates the individual freshness for a source model (table/view).

        If data freshness matches the 'ignore' (most common) settings used for
        the entire source, then the 'freshness' config is omitted so that it 
        can inherit these from global settings.

        :param model: The model settings containing recency tests to be 
            converted into source freshness
        :param loaded_at_field: The loaded_at_field used for source freshness
        :param ignore: Global freshness settings to be ignored if matched
        """

        default_vals = {
            'loaded_at_field': None, 
            'freshness': {'warn_after': None, 'error_after': None}
        }
        derived = default_vals.copy()

        if isinstance(model.get('tests'), list):
            for test in model['tests']:
                if 'dbt_utils.recency' in test:
                    f = SourceFactory._recency_test_to_freshness(test)
                    derived['freshness'].update(f.get('freshness'))
                    derived['loaded_at_field'] = f.get('loaded_at_field')
                    # TODO: Check for different loaded_at_fields for warn/error

        source = default_vals | derived if derived else default_vals
        source_loaded_at = source.get('loaded_at_field')
        source_f = source.get('freshness')
        ignore_f = ignore.get('freshness', {})

        # Remove ignored freshness configs
        for severity in ['warn', 'error']:
            if f'{severity}_after' in source_f:

                if ignore_f.get(f'{severity}_after') == \
                    source_f[f'{severity}_after'] \
                    and loaded_at_field == source_loaded_at:

                    del source_f[f'{severity}_after']

        return source_loaded_at, {'freshness': source_f}

    def from_model_properties(self, file_path: str):
        """
        Method used to generate a dbt source (.yml) file contents using a model
        properties file. Creates/updates the source property.

        :param file_path: Full path for the model properties file 
        """

        model_config = read_yaml_file(file_path)

        g_loaded_at, g_freshness = self._get_global_freshness(
            models=model_config
        )
        self.tables = []

        for model in model_config['models']:

            name = self._strip_prefix_from_model_name(
                model['name'], f'{self.source_name}_'
            )

            table = {'name': name}

            if model.get('description'):
                table['description'] = model['description']

            if model.get('columns'):
                all_columns = []

                for column in model['columns']:
                    if column.get('name'):
                        all_columns.append({'name': column['name']})
                    if column.get('description'):
                        all_columns.append({'description': column['description']})

                table['columns'] = all_columns

            s_loaded_at, s_freshness = self._get_source_model_freshness(
                model,
                loaded_at_field=g_loaded_at,
                ignore=g_freshness
            )

            if s_loaded_at and s_loaded_at != g_loaded_at:
                table['loaded_at_field'] = s_loaded_at
            if s_freshness.get('freshness'):
                table.update(s_freshness)

            self.tables.append(table)

        self.source = Source(
            self.source_name,
            database=self.database,
            tables=self.tables,
            loaded_at_field=g_loaded_at,
            freshness=g_freshness.get('freshness')
        )
