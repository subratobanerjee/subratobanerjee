from setuptools import setup, find_packages

PACKAGE_NAME = "common"
AUTHOR = "2k_DE"
DESCRIPTION = "Package to encapsulate src folder"

def read_requirements(file_name="requirements.txt"):
    with open(file_name) as fd:
        return fd.read().splitlines()

setup(
    name=PACKAGE_NAME,
    version="0.6",
    packages=find_packages(include=['src.*']),
    include_package_data=True,
    entry_points={
    "common": [
      "stream_raw_ingest=src.streaming.raw.common_raw_ingest:stream_raw_ingest",
      "s3_stream_raw_ingest=src.streaming.raw.s3_stream_raw_ingest:s3_stream_raw_ingest",
      "s3_stream_raw_ingest_t2gp=src.streaming.raw.s3_stream_raw_ingest_t2gp:s3_stream_raw_ingest_t2gp",
      "process_playtest_stats=src.batch.gameshaper.playtests_stats_api:process_playtest_stats",
      "process_playtest=src.batch.gameshaper.playtests_api:process_playtests",
       "fact_player_code_redemption=src.batch.ecommers.fact_player_code_redemption:run_batch"
    ]
    },
    url="",
    author=AUTHOR,
    description=AUTHOR,
    install_requires=['common','wheel','setuptools']
)