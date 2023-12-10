import snowflake.connector
from dbt.cli.resolvers import default_profiles_dir
from dbt.utils import get_profile_from_project

from .yaml_handler import read_yaml_file

PROFILES = read_yaml_file(f"{default_profiles_dir()}/profiles.yml")


def get_profile_name_from_current_project():
    return read_yaml_file('dbt_project.yml')["profile"]


def get_credentials(profile: str):
    return get_profile_from_project(
        PROFILES[profile]
    )


def snowflake_connect(profile_name):
    """
    Reads the dbt profiles.yml stored locally, selects credentials for a given 
    profile and creates a Snowflake connection object

    :param profile_name: Name of the profile to use
    :returns: Snowflake connection object
    """

    sf_creds = get_credentials(profile_name)
    
    return snowflake.connector.connect(**sf_creds)
