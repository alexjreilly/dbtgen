import snowflake.connector
from dbt.flags import PROFILES_DIR

from ..params import DBT_PROFILE_DEFAULT
from .yaml_handler import read_yaml_file

PROFILES = read_yaml_file(f"{PROFILES_DIR}/profiles.yml")


def default_target(profile_name: str = DBT_PROFILE_DEFAULT):
    return PROFILES.get(profile_name)['target']


def snowflake_connect(profile_name: str = DBT_PROFILE_DEFAULT):
    """
    Reads the dbt profiles.yml stored locally, selects credentials for a given 
    profile and creates a Snowflake connection object

    :param profile_name: Name of the profile to use
    :returns: Snowflake connection object
    """

    sf_creds = PROFILES.get(profile_name)['outputs'][default_target()]
    
    return snowflake.connector.connect(**sf_creds)
