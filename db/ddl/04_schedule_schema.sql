/*
 * Create tables for the scheduled (planned) transit service schema.
 *
 * Depends on the network schema "nw".
 *
 * Arttu K 2020-02
 */
\set ON_ERROR_STOP on
\c sujuiko;

BEGIN;
CREATE SCHEMA IF NOT EXISTS sched;

/*
 * "timing" refers to the quality of a scheduled timestamp at a node or on a link:
 *
 * - `strict`:   strictly scheduled dep / arr timepoint at a stop in schedule
 * - `approx`:   estimated dep / arr time at a non-timepoint stop in schedule
 * - `interp`:   timestamp has been linearly interpolated (i.e., no schedule at hand for that location)
 */
CREATE TYPE sched.timing_type AS ENUM('strict', 'approx', 'interp');

CREATE TABLE sched.routes (
  route      text              PRIMARY KEY,
  mode       public.mode_type  NOT NULL
);

CREATE FUNCTION sched.populate_routes_from_gtfs()
RETURNS TABLE (
  mode            public.mode_type,
  rows_inserted   bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
BEGIN
  RETURN QUERY
  WITH
    inserted AS (
      INSERT INTO sched.routes
      SELECT
        route_id AS route,
        (CASE
         WHEN route_type = 0 THEN 'tram'
         WHEN route_type IN (700, 701, 702, 704) THEN 'bus'
         END
        )::public.mode_type AS mode
      FROM stage_gtfs.routes
      ON CONFLICT DO NOTHING
      RETURNING *
    )
  SELECT i.mode, count(i.route)::bigint AS rows_inserted
  FROM inserted AS i
  GROUP BY i.mode
  ORDER BY i.mode;
END;
$$;

CREATE TABLE sched.trip_templates (
  tripid      text              PRIMARY KEY,
  route       text              NOT NULL REFERENCES sched.routes(route),
  dir         smallint          NOT NULL,
  start_hms   interval          NOT NULL,
  dates       date[]            NOT NULL
);
CREATE INDEX trips_route_dir_idx
  ON sched.trip_templates (route, dir);

CREATE TABLE sched.segments (
  tripid         text           NOT NULL REFERENCES sched.trip_templates(tripid),
  inode          integer        NOT NULL,
  jnode          integer        NOT NULL,
  enter_hms      interval       NOT NULL,
  exit_hms       interval,
  enter_timing   sched.timing_type,
  exit_timing    sched.timing_type,
  PRIMARY KEY (tripid, inode, jnode, enter_hms),
  FOREIGN KEY (inode, jnode) REFERENCES nw.links(inode, jnode)
);

COMMIT;
