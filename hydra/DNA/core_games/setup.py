from setuptools import setup, find_packages

PACKAGE_NAME = "core_games"
AUTHOR = "2k_DE"
DESCRIPTION = "Package to encapsulate src folder"

setup(
    name=PACKAGE_NAME,
    version="0.6",
    packages=find_packages(where='inverness/src'),
    package_dir={'': 'inverness/src'},
    entry_points={
    "core_games": [
        "process_dim_player=batch.managed.dim_player:process_dim_player",
        "stream_player_activity=streaming.intermediate.fact_player_activity:stream_player_activity",
        "stream_player_campaign_activity=streaming.intermediate.fact_player_campaign_activity:stream_player_campaign_activity",
        "stream_player_session=streaming.intermediate.fact_player_session:stream_player_session",
        "stream_player_entitlement=batch.intermediate.fact_player_entitlement:stream_player_entitlement"
    ]
    },
    url="",
    author=AUTHOR,
    description=AUTHOR,
    install_requires=['core_games','wheel','setuptools']
)