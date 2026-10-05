-- Dernier instantane de logs/pool/pool.status, une ligne par instance.
--
-- Pas d'historique: chaque lecture REMPLACE la precedente (upsert sur
-- pool_instance), donc ni hypertable ni retention. Un seul cluster est actif a la
-- fois; pool_instance distingue les fichiers des instances ckpool successives,
-- et une lecture doit toujours filtrer dessus.
--
-- updated_at est le `lastupdate` ecrit par ckpool, pas l'heure de lecture.
--
-- Les hashrates sont stockes en hashes/s (le fichier les ecrit sous la forme
-- "1.48T").
create table if not exists pool_stat
(
    pool_instance text                     not null,
    updated_at    timestamp with time zone not null,
    runtime_s     bigint                   not null,
    users         integer                  not null,
    workers       integer                  not null,
    idle          integer                  not null,
    disconnected  integer                  not null,
    hashrate_1m   double precision         not null,
    hashrate_5m   double precision         not null,
    hashrate_15m  double precision         not null,
    hashrate_1h   double precision         not null,
    hashrate_6h   double precision         not null,
    hashrate_1d   double precision         not null,
    hashrate_7d   double precision         not null,
    diff          double precision         not null,
    accepted      bigint                   not null,
    rejected      bigint                   not null,
    bestshare     double precision         not null,
    sps_1m        double precision         not null,
    sps_5m        double precision         not null,
    sps_15m       double precision         not null,
    sps_1h        double precision         not null,
    constraint pool_stat_pk
        primary key (pool_instance)
);

alter table pool_stat
    owner to ckpool;

comment on column pool_stat.updated_at is 'ckpool "lastupdate" of the snapshot, not the read time';

comment on column pool_stat.runtime_s is 'ckpool uptime in seconds';

comment on column pool_stat.hashrate_1m is 'hashes per second (file suffix K/M/G/T/P/E/Z expanded)';

comment on column pool_stat.accepted is 'cumulative since ckpool start';

comment on column pool_stat.rejected is 'cumulative since ckpool start';
