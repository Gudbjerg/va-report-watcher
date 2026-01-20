-- Supabase DDLs for per-index constituents, quarterly previews, and issuer snapshots
-- Run these in your Supabase SQL editor. Adjust schemas if you already have tables.

-- ============ Constituents (Daily snapshots, newest-only used by app) ============
create table if not exists public.index_constituents_kaxcap (
  id bigserial primary key,
  as_of timestamptz not null,
  ticker text not null,
  isin text,
  name text,
  price numeric,
  shares numeric,
  mcap numeric,
  weight numeric,
  capped_weight numeric,
  delta_pct numeric,
  avg_daily_volume numeric,
  source text default 'factset',
  created_at timestamptz default now()
);

create table if not exists public.index_constituents_helxcap (like public.index_constituents_kaxcap including defaults including indexes including constraints);
create table if not exists public.index_constituents_omxsalls (like public.index_constituents_kaxcap including defaults including indexes including constraints);

-- Unique snapshot constraint and helper indexes
create unique index if not exists idx_ic_kaxcap_asof_ticker on public.index_constituents_kaxcap (as_of, ticker);
create index if not exists idx_ic_kaxcap_asof on public.index_constituents_kaxcap (as_of desc);

create unique index if not exists idx_ic_helxcap_asof_ticker on public.index_constituents_helxcap (as_of, ticker);
create index if not exists idx_ic_helxcap_asof on public.index_constituents_helxcap (as_of desc);

create unique index if not exists idx_ic_omxsalls_asof_ticker on public.index_constituents_omxsalls (as_of, ticker);
create index if not exists idx_ic_omxsalls_asof on public.index_constituents_omxsalls (as_of desc);


-- ============ Quarterly preview/proforma (uncapped ranking + exception caps) ============
create table if not exists public.index_quarterly_kaxcap (
  id bigserial primary key,
  as_of timestamptz not null,
  ticker text not null,
  name text,
  price numeric,
  shares numeric,
  shares_capped numeric,
  mcap_uncapped numeric,
  mcap_capped numeric,
  curr_weight_uncapped numeric,
  curr_weight_capped numeric,
  weight numeric,             -- target issuer weight distributed to classes
  capped_weight numeric,      -- target (capped) for class
  delta_pct numeric,          -- target minus current capped
  delta_ccy numeric,
  delta_vol numeric,          -- in millions of shares
  days_to_cover numeric,      -- uses delta_vol (millions) / ADV (millions)
  avg_vol_30d numeric,
  avg_vol_30d_millions numeric,
  created_at timestamptz default now()
);

create table if not exists public.index_quarterly_helxcap (like public.index_quarterly_kaxcap including defaults including indexes including constraints);
create table if not exists public.index_quarterly_omxsalls (like public.index_quarterly_kaxcap including defaults including indexes including constraints);

create unique index if not exists idx_iq_kaxcap_asof_ticker on public.index_quarterly_kaxcap (as_of, ticker);
create unique index if not exists idx_iq_helxcap_asof_ticker on public.index_quarterly_helxcap (as_of, ticker);
create unique index if not exists idx_iq_omxsalls_asof_ticker on public.index_quarterly_omxsalls (as_of, ticker);

create index if not exists idx_iq_kaxcap_mcap on public.index_quarterly_kaxcap (mcap_uncapped desc);
create index if not exists idx_iq_helxcap_mcap on public.index_quarterly_helxcap (mcap_uncapped desc);
create index if not exists idx_iq_omxsalls_mcap on public.index_quarterly_omxsalls (mcap_uncapped desc);


-- ============ Issuer-level snapshots (company aggregation) ============
create table if not exists public.index_issuers_kaxcap (
  id bigserial primary key,
  as_of timestamptz not null,
  issuer text not null,
  name text,
  mcap_uncapped numeric,
  mcap_capped numeric,
  curr_weight_uncapped numeric,
  curr_weight_capped numeric,
  weight numeric,
  capped_weight numeric,
  delta_pct numeric,
  delta_ccy numeric,
  delta_vol numeric,
  days_to_cover numeric,
  created_at timestamptz default now()
);

create table if not exists public.index_issuers_helxcap (like public.index_issuers_kaxcap including defaults including indexes including constraints);
create table if not exists public.index_issuers_omxsalls (like public.index_issuers_kaxcap including defaults including indexes including constraints);

create unique index if not exists idx_ii_kaxcap_asof_issuer on public.index_issuers_kaxcap (as_of, issuer);
create unique index if not exists idx_ii_helxcap_asof_issuer on public.index_issuers_helxcap (as_of, issuer);
create unique index if not exists idx_ii_omxsalls_asof_issuer on public.index_issuers_omxsalls (as_of, issuer);

create index if not exists idx_ii_kaxcap_asof on public.index_issuers_kaxcap (as_of desc);
create index if not exists idx_ii_helxcap_asof on public.index_issuers_helxcap (as_of desc);
create index if not exists idx_ii_omxsalls_asof on public.index_issuers_omxsalls (as_of desc);

-- Optional: enable RLS (service role bypasses RLS); add anonymous read if you need direct client access
-- alter table public.index_constituents_kaxcap enable row level security;
-- create policy if not exists anon_read_ic_kaxcap on public.index_constituents_kaxcap for select using (true);
-- Repeat as desired for the other tables.
