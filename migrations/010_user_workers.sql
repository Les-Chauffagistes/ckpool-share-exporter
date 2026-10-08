-- Stats par worker (entrees de la cle "worker" des fichiers users/<adresse>).
-- address est l'adresse de paiement, le nom du fichier: workername la contient
-- deja (<adresse>.<rig>), mais la colonne evite un LIKE pour lister les workers
-- d'une adresse.
--
-- Rien ne supprime un worker disparu du fichier: purge a faire cote base, sur
-- lastshare.
create table if not exists public.user_workers
(
    workername    text                     not null,
    pool_instance text                     not null,
    address       text                     not null,
    hashrate1m    bigint default 0         not null,
    hashrate5m    bigint default 0         not null,
    hashrate1hr   bigint default 0         not null,
    hashrate1d    bigint default 0         not null,
    hashrate7d    bigint default 0         not null,
    lastshare     timestamp with time zone not null,
    shares        bigint default 0         not null,
    bestshare     bigint default 0         not null,
    constraint user_workers_pk primary key (workername, pool_instance)
);

create index if not exists user_workers_address_idx
    on public.user_workers (address, pool_instance);

create index if not exists user_workers_lastshare_idx
    on public.user_workers (lastshare);

comment on table public.user_workers is 'workers statistics';
