from setuptools import setup, find_packages

PACKAGE_NAME = "horizon"
AUTHOR = "2k_DE"
DESCRIPTION = "Package to encapsulate src folder"

setup(
    name=PACKAGE_NAME,
    version="0.6",
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    entry_points={
    "horizon": [
      "process_fact_ability_usage=batch.managed.fact_ability_usage:process_fact_ability_usage",
      "process_fact_weapon_match_summary=batch.managed.fact_weapon_match_summary:process_fact_weapon_match_summary",
      "process_fact_match_lvl_ability_usage=batch.managed.fact_match_level_ability_usage:process_fact_match_lvl_ability_usage",
      "process_dim_player=batch.managed.dim_player:process_dim_player",
      "stream_player_match_summary=streaming.managed.fact_player_match_summary:stream_player_match_summary",
      "stream_level_up=streaming.managed.fact_level_up:stream_level_up",
      "stream_gauntlet_fight_summary=streaming.managed.fact_player_gauntlet_fight_summary:stream_gauntlet_fight_summary",
      "stream_player_activity=streaming.intermediate.fact_player_activity:stream_player_activity",
      "process_fact_player_entitlement=batch.intermediate.fact_player_entitlement:process_fact_player_entitlement",
      "stream_agg_login=streaming.managed.agg_login_10min:stream_agg_login",
      "stream_agg_twitch_entitlement=streaming.managed.agg_twitch_entitlement_10min:start_stream",
      "stream_agg_subscription=streaming.managed.agg_subscription_10min:start_stream",
      "stream_agg_emails_sent=streaming.managed.agg_emails_sent_10min:stream_agg_emails_sent",
      "agg_match_summary_10min=streaming.managed.agg_match_summary_10min:agg_match_summary_10min",
      "stream_party_invite_agg=streaming.managed.agg_party_invite_accept_1hr:stream_party_invite_agg",
      "stream_agg_account_create=streaming.managed.agg_account_create_10min:stream_agg_account_create",
      "stream_agg_legal_docs=streaming.managed.agg_legal_docs_accepted_10min:stream_agg_legal_docs",
      "stream_agg_account_ban=streaming.managed.agg_account_ban_10min:start_stream",
      "stream_installs=streaming.managed.agg_install_10_min:stream_installs",
      "stream_down_death=streaming.managed.fact_down_death:start_stream",
      "stream_player_transactions=streaming.managed.fact_player_transactions:stream_player_transactions"
    ]
    },
    url="",
    author=AUTHOR,
    description=AUTHOR,
    install_requires=['horizon','wheel','setuptools']
)