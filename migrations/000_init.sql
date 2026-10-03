do
$$
    begin
        create type file_status as enum (
            'PENDING', 'PROCESSING', 'DONE', 'QUARANTINED'
            );
    exception
        when duplicate_object then null;
    end
$$;

create table if not exists file
(
    path          text                                                    not null,
    discovered_at timestamp with time zone default now()                  not null,
    updated_at    timestamp with time zone,
    pool_instance text                                                    not null,
    retry_count   integer                  default 0                      not null,
    status        file_status              default 'PENDING'::file_status not null,
    ingested_mtime timestamp with time zone,
    ingested_size  bigint,
    constraint file_pk
        primary key (path, pool_instance)
);

alter table file
    owner to ckpool;

create table if not exists share_weights
(
    bucket_at     timestamp with time zone not null,
    pool_instance text                     not null,
    workinfoid    bigint                   not null,
    username      text                     not null,
    workername    text                     not null,
    diff_sum      double precision         not null,
    shares_ok     integer                  not null,
    shares_ko     integer                  not null,
    shares        integer generated always as (shares_ok + shares_ko) stored,
    constraint share_weights_pk
        primary key (bucket_at, pool_instance, workinfoid, workername)
);

comment on column share_weights.bucket_at is 'job start: createdate of the first line seen for this workinfoid (file order, not a minimum)';

comment on column share_weights.workinfoid is 'ckpool job id; the bucket is the job itself, not a fixed-width window';

comment on column share_weights.username is 'payout address: sharelog field "username", not "address" (client IP)';

comment on column file.ingested_mtime is 'mtime observed at last successful ingest';

comment on column file.ingested_size is 'size observed at last successful ingest; with mtime, detects appends';

comment on column share_weights.diff_sum is 'vardiff sum (field diff), accepted shares only';

comment on column share_weights.shares_ok is 'accepted shares count';

comment on column share_weights.shares_ko is 'rejected shares';

comment on column share_weights.shares is 'derived: shares_ok + shares_ko';

alter table share_weights
    owner to ckpool;

