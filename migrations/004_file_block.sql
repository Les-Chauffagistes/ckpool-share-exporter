-- Le bloc d'un sharelog : le repertoire qui le contient
-- (logs/<block>/<...>.sharelog).
--
-- Sert la fenetre chaude de get_monitored. ckpool ne cree de jobs que pour le
-- bloc en cours, et il n'y a qu'un repertoire actif a la fois : des qu'un
-- nouveau bloc commence, aucun sharelog de l'ancien ne grossira plus. Un share
-- en retard sur un bloc revolu serait de toute facon rejete par la pool, donc
-- ne plus relire ces fichiers ne peut rien faire perdre.
--
-- C'est un critere structurel, pas une duree : il ne depend ni de la cadence de
-- rotation des sharelogs ni d'une horloge.
ALTER TABLE file
    ADD COLUMN IF NOT EXISTS block text;

-- Lignes anterieures a cette migration : le bloc est le repertoire parent, donc
-- derivable du chemin sans connaitre base_log_dir.
UPDATE file
SET block = regexp_replace(path, '^.*/([^/]+)/[^/]+$', '\1')
WHERE block IS NULL;

ALTER TABLE file
    ALTER COLUMN block SET NOT NULL;

comment on column file.block is 'ckpool block directory containing the sharelog; only the current one still grows';

-- Sert les deux etages de get_monitored : la sous-requete qui classe les blocs
-- par max(discovered_at), puis le predicat block IN (...) qui en decoule.
CREATE INDEX IF NOT EXISTS file_block_idx
    ON file (pool_instance, block, discovered_at DESC);
