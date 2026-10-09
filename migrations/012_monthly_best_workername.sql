alter table public.monthly_bests
    add if not exists workername text default null;

update public.monthly_bests
set workername = "user"
where workername is null or workername = '';

alter table public.monthly_bests
    alter column workername drop default,
    alter column workername set not null;

