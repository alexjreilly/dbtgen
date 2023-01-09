import yaml


class QuotedString(str):
    """Sub-class str used for quoting YAML contents"""
    pass


class BaseDumper(yaml.Dumper):

    def increase_indent(
        self, 
        flow=False, 
        indentless=False
    ):
        return super(
            BaseDumper,
            self
        ).increase_indent(flow, False)

    def quoted_scalar(self, data):  # a representer to force quotations on scalars
        return super(
            BaseDumper, 
            self
        ).represent_scalar('tag:yaml.org,2002:str', data, style='"')


def read_yaml_file(file_path: str) -> dict:
    """
    Reads the contents of a yaml file and returns as a dictionary

    :param file_path: Path to the .yml file which needs to be returned as a 
        dictionary
    :returns: Dictionary object with the YAML contents
    """

    with open(file_path, 'r') as f:
        file = f.read()

    return yaml.safe_load(file)
