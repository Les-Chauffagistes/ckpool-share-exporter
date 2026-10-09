alter table public.monthly_bests
    add updated_at timestamp with time zone default NOW() not null;

