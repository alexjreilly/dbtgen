import shutil
from os import listdir, path, walk

from . import params
from .libs import node
from .libs.logger import CustomLogger
from .libs.source import SourceFactory

logger = CustomLogger()


def generate_sources(
    input_models_dir: str, 
    output_sources_dir: str
):

    for root, sub_dirs, files in walk(input_models_dir):
        for sub_dir in sub_dirs:

            sub_dir_path = path.join(root, sub_dir)
            sources_dir = path.abspath(
                f'{sub_dir_path.replace(input_models_dir, output_sources_dir)}'
                f'/../'
            )

            for file in listdir(sub_dir_path):

                # TODO: Add ignore for .dbtgen__*.yml generated files
                if file.endswith('.yml'):

                    source_db_suffix = path.basename(root)
                    model_properties_path = path.join(sub_dir_path, file)
                    log_source_target = f"{node.namespace(sub_dir_path)}.yml"
                    
                    logger.status(log_source_target, "RUN")

                    source_name = path.splitext(
                        path.basename(model_properties_path)
                    )[0]

                    src = SourceFactory(
                        name=source_name,
                        database=f"{{ var('SOURCES_ENV', 'PROD') }}_"
                                 f"{source_db_suffix.upper()}"
                    )
                    src.from_model_properties(model_properties_path)
                    src.source.write(sources_dir, overwrite=True)

                    logger.status(log_source_target, "CREATED")


def main(args):

    logger.info("Creating dbt source files in .export/sources/")

    # cleanup target directory
    shutil.rmtree(params.TARGET_PACKAGE_SOURCES_DIR, ignore_errors=True)   

    generate_sources(
        params.TARGET_MODELS_DIR,
        params.TARGET_PACKAGE_SOURCES_DIR
    )
