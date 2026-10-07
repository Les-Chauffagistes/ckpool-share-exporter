create table if not exists public.monthly_bests
(
    month     date             not null,
    "user"    text             not null,
    best_diff double precision not null,
    constraint monthly_bests_pk
        primary key (month, "user"),
    constraint monthly_bests_month_first_day
        check (month = date_trunc('month', month)::date)
);

comment on column public.monthly_bests.month is 'premier jour du mois (UTC)';
comment on column public.monthly_bests."user" is 'user address';
