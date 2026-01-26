-- Conflict-safe constraints to prevent duplicate rows per snapshot
-- Run this in Supabase SQL editor or via psql

-- Constituents: unique by (as_of, ticker)
CREATE UNIQUE INDEX IF NOT EXISTS uq_idx_const_kaxcap_asof_ticker ON public.index_constituents_kaxcap (as_of, ticker);
CREATE UNIQUE INDEX IF NOT EXISTS uq_idx_const_helxcap_asof_ticker ON public.index_constituents_helxcap (as_of, ticker);
CREATE UNIQUE INDEX IF NOT EXISTS uq_idx_const_omxsalls_asof_ticker ON public.index_constituents_omxsalls (as_of, ticker);

-- Quarterly: unique by (as_of, ticker)
CREATE UNIQUE INDEX IF NOT EXISTS uq_idx_quarterly_kaxcap_asof_ticker ON public.index_quarterly_kaxcap (as_of, ticker);
CREATE UNIQUE INDEX IF NOT EXISTS uq_idx_quarterly_helxcap_asof_ticker ON public.index_quarterly_helxcap (as_of, ticker);
CREATE UNIQUE INDEX IF NOT EXISTS uq_idx_quarterly_omxsalls_asof_ticker ON public.index_quarterly_omxsalls (as_of, ticker);

-- Issuers: unique by (as_of, issuer)
CREATE UNIQUE INDEX IF NOT EXISTS uq_idx_issuers_kaxcap_asof_issuer ON public.index_issuers_kaxcap (as_of, issuer);
CREATE UNIQUE INDEX IF NOT EXISTS uq_idx_issuers_helxcap_asof_issuer ON public.index_issuers_helxcap (as_of, issuer);
CREATE UNIQUE INDEX IF NOT EXISTS uq_idx_issuers_omxsalls_asof_issuer ON public.index_issuers_omxsalls (as_of, issuer);

-- Optional: ensure updated_at auto-updates (if not already in table definitions)
-- Uncomment and adapt if needed; Supabase often sets these up in table DDL.
-- CREATE OR REPLACE FUNCTION public.set_updated_at()
-- RETURNS trigger AS $$
-- BEGIN
--   NEW.updated_at := NOW();
--   RETURN NEW;
-- END;
-- $$ LANGUAGE plpgsql;
--
-- DO $$ BEGIN
--   IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_const_kaxcap_updated_at') THEN
--     CREATE TRIGGER trg_const_kaxcap_updated_at BEFORE UPDATE ON public.index_constituents_kaxcap
--     FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
--   END IF;
-- END $$;
