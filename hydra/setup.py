from setuptools import setup, find_packages

PACKAGE_NAME = "streaming"
AUTHOR = "2k_DE"
DESCRIPTION = "Package to encapsulate src folder"

def read_requirements(file_name="requirements.txt"):
    with open(file_name) as fd:
        return fd.read().splitlines()

setup(
    name=PACKAGE_NAME,
    version="0.6",
    packages=find_packages(include=['streaming','streaming.*']),
    entry_points={
    "streaming": [
      "stream_player_match_summary=streaming.horizon.managed.fact_player_match_summary:stream_player_match_summary"
    ]
    },
    #package_dir={'': 'src'},
    url="",
    author=AUTHOR,
    description=AUTHOR,
    install_requires=['streaming','wheel','setuptools']
)