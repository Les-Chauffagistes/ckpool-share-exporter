-- `file` reste une table PostgreSQL ordinaire : sa cle primaire globale
-- (path, pool_instance) garantit qu'un sharelog ne peut etre enregistre qu'une
-- fois. Une hypertable partitionnee par discovered_at ne pourrait pas
-- conserver cette contrainte, car TimescaleDB exige la dimension temporelle
-- dans chaque cle unique.
--
-- La table n'est utile au suivi que pendant monitor_window_days (16 jours).
-- On conserve 30 jours pour laisser une marge aux traitements et retries,
-- puis une tache TimescaleDB purge les lignes anciennes quotidiennement.
CREATE INDEX IF NOT EXISTS file_discovered_at_idx ON file (discovered_at);

CREATE OR REPLACE PROCEDURE delete_expired_file_rows(job_id integer, config jsonb)
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM file
    WHERE discovered_at < now() - INTERVAL '30 days';
END;
$$;

SELECT add_job('delete_expired_file_rows', INTERVAL '1 day');
