"""Etat du schema apres application des migrations dans l'ordre lexicographique."""

import json

from ckpool_share_exporter.settings import settings


def config_of(row):
    """asyncpg rend une colonne jsonb sous forme de chaine."""
    config = row["config"]
    return json.loads(config) if isinstance(config, str) else config


async def test_the_migrations_apply_in_lexicographic_order(pg):
    tables = {
        row["table_name"]
        for row in await pg.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    }

    assert {"file", "share_weights"} <= tables


async def test_share_weights_is_a_hypertable_partitioned_on_bucket_at(pg):
    row = await pg.fetchrow(
        """SELECT hypertable_name FROM timescaledb_information.hypertables
           WHERE hypertable_name = 'share_weights'""")

    assert row is not None
    column = await pg.fetchval(
        """SELECT column_name FROM timescaledb_information.dimensions
           WHERE hypertable_name = 'share_weights' AND dimension_number = 1""")
    assert column == "bucket_at"


async def test_share_weights_primary_key_is_the_aggregation_key(pg):
    columns = await pg.fetch(
        """SELECT a.attname AS name
           FROM pg_index i
           JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
           WHERE i.indrelid = 'share_weights'::regclass AND i.indisprimary""")

    assert {row["name"] for row in columns} == {
        "bucket_at", "pool_instance", "workinfoid", "workername",
    }


async def test_shares_is_a_generated_column(pg):
    generated = await pg.fetchval(
        """SELECT is_generated FROM information_schema.columns
           WHERE table_name = 'share_weights' AND column_name = 'shares'""")

    assert generated == "ALWAYS"


async def test_both_continuous_aggregates_are_real_time(pg):
    """materialized_only = false: une share inseree est visible sans attendre le
    rafraichissement, et l'agregat journalier voit la partie temps reel de
    l'horaire sur lequel il est empile."""
    rows = await pg.fetch(
        """SELECT view_name, materialized_only
           FROM timescaledb_information.continuous_aggregates""")
    real_time = {row["view_name"]: row["materialized_only"] for row in rows}

    assert real_time == {"share_weights_hourly": False, "share_weights_daily": False}


async def test_the_monitored_files_index_exists(pg):
    indexes = {
        row["indexname"]
        for row in await pg.fetch("SELECT indexname FROM pg_indexes WHERE tablename = 'file'")
    }

    assert "file_monitored_idx" in indexes


async def test_the_block_column_is_mandatory_and_indexed(pg):
    """La fenetre chaude de get_monitored repose entierement sur cette colonne."""
    nullable = await pg.fetchval(
        """SELECT is_nullable FROM information_schema.columns
           WHERE table_name = 'file' AND column_name = 'block'""")
    indexes = {
        row["indexname"]
        for row in await pg.fetch("SELECT indexname FROM pg_indexes WHERE tablename = 'file'")
    }

    assert nullable == "NO"
    assert "file_block_idx" in indexes


async def test_window_chain_is_ordered_from_settings_to_retention(pg):
    """distribution <= ingestion < start_offset horaire < start_offset journalier
    < retention du brut < retention journaliere.

    Aucun de ces ordres n'est verifie a l'execution en dehors du premier (le
    validateur de Settings): un reglage deplace d'un cote sans l'autre fait
    disparaitre des donnees de l'agregat, sans erreur.
    """
    jobs = {
        (row["proc_name"], row["hypertable_name"]): config_of(row)
        for row in await pg.fetch(
            """SELECT proc_name, hypertable_name, config
               FROM timescaledb_information.jobs
               WHERE hypertable_name IS NOT NULL""")
    }

    assert settings.distribution_window_days <= settings.monitor_window_days
    assert settings.monitor_window_days < 17
    assert jobs[("policy_refresh_continuous_aggregate", "share_weights_hourly")]["start_offset"] == "17 days"
    assert jobs[("policy_refresh_continuous_aggregate", "share_weights_daily")]["start_offset"] == "18 days"
    assert jobs[("policy_retention", "share_weights")]["drop_after"] == "30 days"
    assert jobs[("policy_retention", "share_weights_hourly")]["drop_after"] == "30 days"
    assert jobs[("policy_retention", "share_weights_daily")]["drop_after"] == "3 mons"
    assert jobs[("policy_compression", "share_weights")]["compress_after"] == "2 days"


async def test_the_hourly_aggregate_lags_by_one_hour_at_most(pg):
    """end_offset = 1 hour: c'est ce decalage que materialized_only = false
    compense cote lecture."""
    config = config_of(await pg.fetchrow(
        """SELECT config FROM timescaledb_information.jobs
           WHERE proc_name = 'policy_refresh_continuous_aggregate'
             AND hypertable_name = 'share_weights_hourly'"""))

    assert config["end_offset"] == "01:00:00"
