"""
    Script to generate models in the dbt project using a template SQL script 
    with variables and a yaml file defining those model scoped variables.
    
    Iterates through every sub-directory and checks for two files:
        - *.sql
        - models.yml 
    
    models.yml structure:
    
        models:
          <model>:
            <key>: <value>
    
    Generates a corresponding model file (e.g. ./models/ods/ifs/my_model.sql) 
    in the dbt project, (for each model defined in models.yml) by using the 
    key-value pairs defined in YAML and performing a find and replace on the 
    template *.sql file.
"""

import os
from dataclasses import dataclass
import string

from src import params
from src.libs import node
from src.libs.file_handler import read_file
from src.libs.logger import CustomLogger
from src.libs.yaml_handler import read_yaml_file

logger = CustomLogger()
counts = {'created': 0, 'skipped': 0}


@dataclass
class Model:
    """
    Class to format and write the contents of a (.sql) model file to a dbt 
    project.

    :param name: The name of the model
    :param yaml_contents: Dictionary with model variables
    :param sql: Template SQL file to be formatted
    :param file_name_pattern: Naming pattern for the target model file
    """

    name: str
    target_dir: str
    file_name_pattern: str
    yaml_contents: dict
    sql: str

    @property
    def file_name(self) -> str:
        return self.file_name_pattern.format(
            name=self.name, **self.yaml_contents
        )

    @property
    def full_name(self) -> str:
        return node.namespace(
            os.path.abspath(f'{self.target_dir}/{self.file_name}')
            .replace(params.TARGET_MODELS_DIR, '')
            .replace('.sql', '')
        )

    @property
    def contents(self) -> str:
        return string.Template(self.sql).substitute(name=self.name, **self.yaml_contents)

    @property
    def contents_print_format(self) -> str:
        return self.contents_print_format

    @contents_print_format.getter
    def contents_print_format(self) -> str:
        """
        Get the compiled contents of a dbt model to be printed to the terminal.
        """

        body = f'Contents:' \
               '\n    ------------------------------' \
               '\n'

        count = 0
        for line in self.contents.splitlines():
            count += 1
            line_no = '{: >3d}'.format(count)
            body += f'\n    {line_no} | {line}'

        body += '\n' \
                '\n    ------------------------------'

        return body

    def write_file(
            self,
            overwrite: bool = False
    ) -> None:
        """
        Writes the contents of the model file to a target directory
        """

        if not os.path.exists(self.target_dir):
            os.makedirs(self.target_dir)

        file_path = f'{self.target_dir}/{self.file_name}'

        if not os.path.exists(file_path) or overwrite:
            with open(file_path, 'w') as model_file:
                model_file.write(self.contents)
                model_file.close()
            logger.status(self.full_name, 'CREATED')
            counts['created'] += 1

        else:
            logger.status(self.full_name, 'SKIPPED')
            counts['skipped'] += 1


def generate_models(
        models: dict,
        ignored: dict,
        template: str,
        target_dir: str,
        file_name_pattern: str,
        execute_mode: bool,
        overwrite_mode: bool
) -> None:
    """
    Generates objects of the Model class. For each definition and set of model 
    variables, instantiates an object and calls the class method to write the 
    contents to a .sql file.
    """

    for model_name in models['models']:
        if not ignored or model_name not in ignored.get('models'):

            model = Model(
                model_name,
                target_dir,
                file_name_pattern,
                models['models'][model_name],
                template
            )

            logger.status(model.full_name, 'RUN')
            if execute_mode:
                model.write_file(overwrite_mode)
            else:
                logger.info(model.contents_print_format)


def get_models_yml(
        dir_path: str,
        models_file: str = 'models.yml'
) -> dict:

    try:
        yml = read_yaml_file(f'{dir_path}/{models_file}')

    except FileNotFoundError:

        # If no models file found, traverse through parent directories to find one
        yml = {}
        parent_dir = dir_path

        while os.path.abspath(parent_dir) != params.PROJECT_ROOT:
            parent_dir += '../'
            try:
                yml = read_yaml_file(f'{parent_dir}/{models_file}')
                break
            except FileNotFoundError:
                pass

    return yml


def main(args):
    
    for root, sub_dirs, files in os.walk(params.INPUT_MODELS_DIR):
        for sub_dir in sub_dirs:

            sub_dir_path = os.path.join(root, sub_dir)
            sub_dir_namespace = node.namespace(
                sub_dir_path.replace(params.INPUT_MODELS_DIR, 'models')
            )
            
            if not args.select \
                or sub_dir_namespace.startswith(args.select + '.') \
                or sub_dir_namespace == args.select:

                models_yml = get_models_yml(sub_dir_path)
                ignore_yml = get_models_yml(sub_dir_path, 'ignore.yml')
                print(ignore_yml)

                try:
                    model_dir = os.path.abspath(
                        sub_dir_path.replace(
                            params.INPUT_MODELS_DIR,
                            params.TARGET_MODELS_DIR
                        )
                    )

                    for file in os.listdir(sub_dir_path):
                        if file.endswith('.sql'):

                            filename = os.path.join(sub_dir_path, file)
                            template_sql = read_file(
                                filename,
                                allow_empty=False
                            )
                            params_in_filename = [
                                tup[1] for tup in string.Formatter().parse(filename) \
                                if tup[1] is not None
                            ]

                            if params_in_filename:
                                generate_models(
                                    models_yml,
                                    ignore_yml,
                                    template_sql,
                                    model_dir,
                                    file,
                                    args.run,
                                    args.overwrite
                                )

                            else:
                                logger.status(file, 'RUN')
                                model = Model(
                                    file, 
                                    model_dir, 
                                    file, 
                                    yaml_contents={}, 
                                    sql=template_sql
                                )
                                if args.run:
                                    model.write_file(args.overwrite)
                                else:
                                    logger.info(model.contents_print_format)

                except FileNotFoundError:
                    pass

    if args.run:
        logger.info("")
        logger.info(f"Models created: {counts['created']}")
        logger.info(f"Models skipped: {counts['skipped']}")
    else:
        logger.info("Compile mode only - no model files created. "
                    "To execute, pass the CLI flag '--run'")
