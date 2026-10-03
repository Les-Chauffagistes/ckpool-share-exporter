CREATE EXTENSION IF NOT EXISTS timescaledb;

SELECT create_hypertable('share_weights', by_range('bucket_at', INTERVAL '1 day'));
CREATE INDEX ON share_weights (username, bucket_at DESC);

-- username est ajoute au segmentby: comme workername le determine
-- (workername = '<username>.<rig>'), le nombre de segments est identique, mais
-- un WHERE username = ... peut elaguer les segments sur les chunks compresses.
-- workinfoid doit etre dans orderby: il fait partie de la PK, sans quoi
-- TimescaleDB avertit "column workinfoid should be used for segmenting or
-- ordering". Le mettre en segmentby serait pire: un segment par job.
ALTER TABLE share_weights SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'pool_instance, username, workername',
  timescaledb.compress_orderby   = 'bucket_at DESC, workinfoid'
);

SELECT add_compression_policy('share_weights', INTERVAL '2 days');
SELECT add_retention_policy('share_weights', INTERVAL '30 days');

-- shares_ok ET shares_ko doivent etre agreges des maintenant: une colonne
-- ajoutee plus tard ne peut se remplir qu'a partir du brut encore present, donc
-- les periodes deja purgees par la retention (30 jours) resteraient vides pour
-- toujours. shares est la colonne generee (shares_ok + shares_ko) du brut.
-- materialized_only = false (agregat temps reel): interroger la vue renvoie
-- l'union du materialise et d'un calcul en direct sur le brut plus recent que
-- le dernier rafraichissement. Sans ca, end_offset = 1 hour rend l'heure en
-- cours invisible et la repartition affichee aux mineurs accuse jusqu'a 1h30
-- de retard. Avec, la fraicheur ne depend plus que de l'ingestion.
CREATE MATERIALIZED VIEW share_weights_hourly
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT time_bucket('1 hour', bucket_at) AS hour,
       pool_instance, username, workername,
       sum(diff_sum) AS diff_sum,
       sum(shares_ok) AS shares_ok,
       sum(shares_ko) AS shares_ko,
       sum(shares)    AS shares
FROM share_weights
GROUP BY hour, pool_instance, username, workername
WITH NO DATA;

-- WITH NO DATA: la politique ci-dessous remplit l'agregat au fil de l'eau. Si
-- cette migration est un jour rejouee sur une base contenant deja des donnees
-- anterieures a start_offset, il faut un
-- CALL refresh_continuous_aggregate('share_weights_hourly', NULL, NULL); unique.
-- start_offset doit couvrir la fenetre d'INGESTION (monitor_window_days = 16),
-- pas seulement celle de calcul (distribution_window_days = 14): toute donnee
-- arrivant dans un bucket plus ancien que start_offset voit son invalidation
-- ignoree definitivement et reste invisible dans l'agregat. Doit aussi rester
-- strictement inferieur a la retention du brut (30 jours), sinon un
-- rafraichissement recalculerait une periode deja purgee.
--
-- Chaine a preserver si un de ces reglages bouge (aucun de ces ordres n'est
-- verifie a l'execution, sauf le premier <= cote Python):
--   distribution 14 <= ingestion 16        (settings.py)
--    < start_offset horaire 17             (ici)
--    < start_offset journalier 18          (003)
--    < retention brut 30 = retention horaire 30   (ici)
--    < retention journaliere 3 mois        (003)
SELECT add_continuous_aggregate_policy('share_weights_hourly',
  start_offset      => INTERVAL '17 days',
  end_offset        => INTERVAL '1 hour',
  schedule_interval => INTERVAL '30 minutes');

-- L'horaire ne sert que la fenetre chaude consultee par les mineurs; au-dela,
-- c'est l'archive journaliere de 003 qui prend le relais. Doit rester
-- superieure au start_offset du CAgg journalier (18 jours, dans 003).
SELECT add_retention_policy('share_weights_hourly', INTERVAL '30 days');
