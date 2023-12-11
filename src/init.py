from pathlib import Path


def init_models_yml():
    print(1)


def init_template_sql():
    print(1)


def main(args):
    dbtgen_path = Path('./.dbtgen/models/')
    dbtgen_path.mkdir(parents=True)
