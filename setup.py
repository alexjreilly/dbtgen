#!/usr/bin/env python
import os
from setuptools import setup, find_packages
import sys


try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print("Error: dbtgen requires setuptools v40.1.0 or higher.")
    print('Please upgrade setuptools with "pip install --upgrade setuptools" ' "and try again")
    sys.exit(1)


this_directory = os.path.abspath(os.path.dirname(__file__))
packages_dir = os.path.join(this_directory, 'src')
with open(os.path.join(this_directory, "README.md")) as f:
    long_description = f.read()


setup(
    name="dbt-gen",
    version="1.0.0",
    description="Generate dbt models and properties files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='alexjreilly',
    packages=find_namespace_packages(include=["src", "src.*"]),
    include_package_data=True,
    entry_points={
        "console_scripts": ["dbtgen = src.main:cli"],
    },
    install_requires=[
        "dbt-core>=1.1.0",
        "dbt-snowflake>1.1.0",
        "snowflake-connector-python>=2.9.0",
        "requests<3.0.0",
        "idna>=2.5,<4",
        "cffi>=1.9,<2.0.0",
        "pyyaml>=6.0",
    ],
    python_requires=">=3.9.0"
)
