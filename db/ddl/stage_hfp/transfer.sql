DROP FUNCTION IF EXISTS stage_hfp.transfer_journeys;
CREATE FUNCTION stage_hfp.transfer_journeys(
  journey_table     regclass
)
RETURNS BIGINT
LANGUAGE PLPGSQL
AS $$
DECLARE
  cnt_jrn   bigint;
  cnt_ins   bigint;
BEGIN
  EXECUTE format(
    $s$
    SELECT count(*) FROM %1$s
    $s$,
    journey_table
  ) INTO cnt_jrn;

  EXECUTE format(
    $s$
    WITH
      inserted AS (
        INSERT INTO obs.journeys (
          jrnid, start_ts, ttid, oper, veh,
          n_obs, n_dropen, tst_span, odo_span, raw_distance
        )
        SELECT
          jrnid,
          start_ts,
          ttid,
          oper,
          veh,
          n_obs,
          n_dropen,
          tst_span,
          odo_span,
          raw_distance
        FROM %1$s
        ON CONFLICT DO NOTHING
        RETURNING 1
      )
    SELECT count(*) FROM inserted;
    $s$,
    journey_table
  ) INTO cnt_ins;

  IF cnt_ins <> cnt_jrn THEN
    RAISE WARNING '% of % journeys transferred', cnt_ins, cnt_jrn;
  END IF;

  RETURN cnt_ins;
END;
$$;
COMMENT ON FUNCTION stage_hfp.transfer_journeys IS
'Transfer final journeys from `journey_table` into `obs.journeys` table.
Rows breaking any constraint (pkey, fkey, not null) in `obs.journeys`
are ignored, and a warning is issued if the number of successfully inserted rows
differs from the number of rows in `journey_table`.';
