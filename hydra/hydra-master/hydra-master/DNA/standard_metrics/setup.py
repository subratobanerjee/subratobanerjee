from setuptools import setup, find_packages

PACKAGE_NAME = "standard_metrics"
AUTHOR = "2k_DE"
DESCRIPTION = "Package to encapsulate src folder"

def read_requirements(file_name="requirements.txt"):
    with open(file_name) as fd:
        return fd.read().splitlines()

setup(
    name=PACKAGE_NAME,
    version="0.6",
    packages=find_packages(include=['src.*']),
    entry_points={
    "standard_metrics": [
      "stream_players=src.streaming.managed.fact_players:stream_players",
      "stream_installs=src.streaming.managed.fact_installs:stream_installs",
      "stream_account_link=src.streaming.managed.agg_account_link_10min:start_stream",
      "stream_web_events=src.streaming.managed.agg_web_activity_10min:start_stream",
      "batch_fact_player_ltd=src.batch.managed.fact_player_ltd:batch_fact_player_ltd",
      "batch_fact_retention=src.batch.managed.fact_retention:batch_fact_retention",
      "batch_fact_login=src.batch.managed.fact_login:run_batch",
      "batch_fact_active_user=src.batch.managed.fact_active_user:run_batch",
      "batch_cdp_eth_summary=src.batch.managed.cdp_eth_summary:run_batch",
      "batch_cdp_eth_daily_events=src.batch.managed.cdp_eth_daily_events:run_batch",
      "batch_cdp_eth_franchise=src.batch.managed.cdp_eth_franchise:run_batch",
      "batch_cdp_eth_seasons=src.batch.managed.cdp_eth_seasons:run_batch"
    ]
    },
    url="",
    author=AUTHOR,
    description=AUTHOR,
    install_requires=['standard_metrics','wheel','setuptools']
)