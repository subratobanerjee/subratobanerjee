from setuptools import setup, find_packages

PACKAGE_NAME = "utils"
AUTHOR = "2k_DE"
DESCRIPTION = "Package to encapsulate storage cost module"

def read_requirements(file_name="requirements.txt"):
    with open(file_name) as fd:
        return fd.read().splitlines()

setup(
    name=PACKAGE_NAME,
    version="0.1",
    packages=find_packages(include=['src.*']),
    include_package_data=True,
    entry_points={
    "utils": [
      "compute_storage_cost=src.storage_costs.compute_storage_cost:compute_storage_cost",
      "run_table_optimization=src.table_optimization.table_optimization:run_optimization",
      "snowflake_uniform_refresh=src.snowflake.snowflake_iceberg_refresh:main"
    ]
    },
    url="",
    author=AUTHOR,
    description=AUTHOR,
    install_requires=['utils','wheel','setuptools','snowflake']
)