-- Migration: add issuer/old_weight/new_weight columns and stored procedure for atomic inserts
BEGIN;

ALTER TABLE IF EXISTS public.proposal_constituents
  ADD COLUMN IF NOT EXISTS issuer text;

ALTER TABLE IF EXISTS public.proposal_constituents
  ADD COLUMN IF NOT EXISTS old_weight numeric(18,12);

ALTER TABLE IF EXISTS public.proposal_constituents
  ADD COLUMN IF NOT EXISTS new_weight numeric(18,12);

-- Stored procedure to insert a proposal and its constituents atomically.
-- Accepts a single jsonb payload with keys: indexId, name, status, proposed (array of constituents).
CREATE OR REPLACE FUNCTION public.insert_proposal_with_constituents(payload jsonb)
RETURNS jsonb LANGUAGE plpgsql AS $$
DECLARE
  p ALIAS FOR payload;
  created_id bigint;
BEGIN
  -- Insert proposal row
  INSERT INTO public.index_proposals (index_id, name, status, payload)
  VALUES (p->> 'indexId', p->> 'name', COALESCE(p->> 'status', 'proposed'), p)
  RETURNING id INTO created_id;

  -- If there are constituents, insert them
  IF p ? 'proposed' THEN
    INSERT INTO public.proposal_constituents
      (proposal_id, index_id, issuer, ticker, name, price, shares, mcap, old_weight, new_weight, created_at)
    SELECT
      created_id,
      p->> 'indexId',
      COALESCE(pc->> 'issuer', pc->> 'ticker'),
      pc->> 'ticker',
      COALESCE(pc->> 'name', pc->> 'ticker'),
      NULLIF(pc->> 'price','')::numeric,
      NULLIF(pc->> 'shares','')::bigint,
      NULLIF(pc->> 'mcap','')::numeric,
      NULLIF(pc->> 'oldWeight','')::numeric,
      NULLIF(pc->> 'newWeight','')::numeric,
      now()
    FROM jsonb_array_elements(p->'proposed') AS pc;
  END IF;

  RETURN (SELECT row_to_json(r) FROM (SELECT * FROM public.index_proposals WHERE id = created_id) r);
EXCEPTION WHEN OTHERS THEN
  -- Bubble up the error to the caller; transaction will be rolled back by Supabase/PG
  RAISE;
END;
$$;

COMMIT;
