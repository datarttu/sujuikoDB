/*
 * Tables for reporting and auditing HFP data dump import processes
 * & discarded journey entries.
 */

DROP TABLE IF EXISTS stage_hfp.log_sessions CASCADE;
CREATE TABLE stage_hfp.log_sessions (
  session_id    text            PRIMARY KEY,
  session_start timestamptz     NOT NULL DEFAULT clock_timestamp(),
  route         text            NOT NULL,
  oday          date            NOT NULL,
  raw_file      text
);
COMMENT ON TABLE stage_hfp.log_sessions IS
'A log session represents a process of importing and transforming a single
HFP data file through `stage_hfp` to `obs` schema.
It does not need to be complete all the way until `obs`,
failed sessions count as well.';
CREATE INDEX ON stage_hfp.log_sessions USING BTREE (session_start);

DROP TABLE IF EXISTS stage_hfp.log_steps;
CREATE TABLE stage_hfp.log_steps (
  session_id    text            NOT NULL REFERENCES stage_hfp.log_sessions(session_id),
  ts            timestamptz     NOT NULL DEFAULT clock_timestamp(),
  step          text            NOT NULL,
  PRIMARY KEY (session_id, ts)
);
COMMENT ON TABLE stage_hfp.log_steps IS
'Describes the steps, or "state snapshots", of a HFP import session `session_id`.
Note that the session base information must already exist in `log_sessions`!';

DROP VIEW IF EXISTS stage_hfp.view_log_steps;
CREATE VIEW stage_hfp.view_log_steps AS (
  SELECT
    se.*,
    st.ts,
    st.step,
    coalesce(ts - lag(ts) OVER (
      PARTITION BY se.session_id ORDER BY st.ts
    ), '0 seconds'::interval)     AS step_duration,
    coalesce(ts - min(ts) OVER (
      PARTITION BY se.session_id
    ), '0 seconds'::interval)     AS total_duration
  FROM stage_hfp.log_sessions     AS se
  INNER JOIN stage_hfp.log_steps  AS st
    ON se.session_id = st.session_id
);

DROP PROCEDURE IF EXISTS stage_hfp.log_step;
CREATE PROCEDURE stage_hfp.log_step(
  session_id    text,
  step          text,
  route         text      DEFAULT NULL,
  oday          date      DEFAULT NULL,
  raw_file      text      DEFAULT NULL,
  notice        boolean   DEFAULT true
)
LANGUAGE PLPGSQL
AS $$
BEGIN
  IF route IS NOT NULL AND oday IS NOT NULL THEN
    INSERT INTO stage_hfp.log_sessions (session_id, route, oday, raw_file)
    VALUES (session_id, route, oday, raw_file);
  END IF;

  INSERT INTO stage_hfp.log_steps (session_id, step)
  VALUES (session_id, step);

  IF notice THEN
    RAISE NOTICE '%: %', session_id, step;
  END IF;

END;
$$;
COMMENT ON PROCEDURE stage_hfp.log_step IS
'Record a session state to log. `step` is an arbitrary description,
`session_id` should be a consistent unique text identifier for the session.
Usage:
- First time you call the function in the session, give all the parameters
  so the session is saved to `log_sessions`.
- Then just call with `session_id` and `step`.
Timestamps are given automatically.';


DROP TABLE IF EXISTS stage_hfp.discarded_journeys;
CREATE TABLE stage_hfp.discarded_journeys (
  LIKE stage_hfp.journeys
);
ALTER TABLE stage_hfp.discarded_journeys
  ADD COLUMN  added_ts  timestamptz   DEFAULT now(),
  ADD PRIMARY KEY (jrnid, added_ts);
CREATE INDEX ON stage_hfp.discarded_journeys USING BTREE(route, dir);
COMMENT ON TABLE stage_hfp.discarded_journeys IS
'Invalid rows that were discarded from stage_hfp.journeys or corresponding
temporary table, plus metadata fields, for auditing.';
