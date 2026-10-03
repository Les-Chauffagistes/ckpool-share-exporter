-- Sert get_monitored: worker_B rejoue cette requete a chaque tick, sur une table
-- qui accumule un fichier par sharelog et n'est jamais purgee.
--
-- Le predicat porte sur un litteral (pas un parametre), donc le planificateur
-- peut l'exploiter meme en plan generique -- ce que asyncpg produit apres
-- quelques executions d'une requete preparee. Un predicat du type
-- `status = $1` serait au contraire ignore et degenererait en Seq Scan.
--
-- coalesce(ingested_mtime, discovered_at) est indexe comme expression, a
-- l'identique de la clause WHERE: sans ca l'index ne filtre que sur
-- pool_instance et parcourt toute l'historique pour ne garder que la fenetre.
CREATE INDEX file_monitored_idx
    ON file (pool_instance, (coalesce(ingested_mtime, discovered_at)), discovered_at)
    WHERE status <> 'QUARANTINED'::file_status;
