-- Archive journaliere, empilee sur l'agregat horaire plutot que sur le brut:
-- elle continue donc a se remplir apres la purge du brut a 30 jours.
--
-- materialized_only = false ici aussi: un CAgg empile ne voit que la partie
-- MATERIALISEE de son parent. Sans ce reglage, le total du jour en cours est
-- systematiquement inferieur a celui de l'horaire.
CREATE MATERIALIZED VIEW share_weights_daily
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket('1 day', hour) AS day,
       pool_instance, username, workername,
       sum(diff_sum)  AS diff_sum,
       sum(shares_ok) AS shares_ok,
       sum(shares_ko) AS shares_ko,
       sum(shares)    AS shares
FROM share_weights_hourly
GROUP BY day, pool_instance, username, workername
WITH NO DATA;

-- start_offset (18 jours) depasse celui de l'horaire (17 jours) pour que
-- l'archive reprenne toute correction que l'horaire a pu integrer, et reste
-- inferieur a la retention de l'horaire (30 jours).
SELECT add_continuous_aggregate_policy('share_weights_daily',
  start_offset      => INTERVAL '18 days',
  end_offset        => INTERVAL '1 day',
  schedule_interval => INTERVAL '1 hour');

SELECT add_retention_policy('share_weights_daily', INTERVAL '3 months');
