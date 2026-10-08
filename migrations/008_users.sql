create table if not exists public.users
(
    address     text                     not null
        constraint users_pk
            primary key,
    hashrate1m  bigint  default 0        not null,
    hashrate5m  bigint  default 0        not null,
    hashrate1hr bigint  default 0        not null,
    hashrate1d  bigint  default 0        not null,
    hashrate7d  bigint  default 0        not null,
    lastshare   timestamp with time zone not null,
    workers     integer default 0        not null,
    shares      bigint  default 0        not null,
    bestshare   bigint  default 0        not null,
    authorized  timestamp with time zone not null
);

comment on table public.users is 'users statistics';
