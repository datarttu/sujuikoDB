/*
 * Create tables for the GTFS staging schema.
 *
 * Arttu K 2020-02
 */
\set ON_ERROR_STOP on
\c sujuiko;

BEGIN;
\echo Creating stage_gtfs schema ...

CREATE SCHEMA IF NOT EXISTS stage_gtfs;

CREATE TABLE stage_gtfs.routes (
  route_id          text        PRIMARY KEY,
  agency_id         text,
  route_short_name  text,
  route_long_name   text,
  route_desc        text,
  route_type        smallint,
  route_url         text
);

CREATE TABLE stage_gtfs.calendar (
  service_id        text        PRIMARY KEY,
  monday            boolean,
  tuesday           boolean,
  wednesday         boolean,
  thursday          boolean,
  friday            boolean,
  saturday          boolean,
  sunday            boolean,
  start_date        date,
  end_date          date
);

CREATE TABLE stage_gtfs.calendar_dates (
  service_id        text,
  date              date,
  exception_type    smallint,
  PRIMARY KEY (service_id, date)
);

CREATE TABLE stage_gtfs.shapes (
  shape_id            text,
  shape_pt_lat        double precision,
  shape_pt_lon        double precision,
  shape_pt_sequence   integer,
  shape_dist_traveled double precision,
  PRIMARY KEY (shape_id, shape_pt_sequence)
);

CREATE TABLE stage_gtfs.trips (
  route_id              text,
  service_id            text,
  trip_id               text        PRIMARY KEY,
  trip_headsign         text,
  direction_id          smallint,
  shape_id              text,
  wheelchair_accessible smallint,
  bikes_allowed         smallint,
  max_delay             smallint
);

CREATE TABLE stage_gtfs.stop_times (
  trip_id               text,
  arrival_time          interval,
  departure_time        interval,
  stop_id               integer,
  stop_sequence         smallint,
  stop_headsign         text,
  pickup_type           smallint,
  drop_off_type         smallint,
  shape_dist_traveled   double precision,
  timepoint             boolean,
  PRIMARY KEY (trip_id, stop_sequence)
);

CREATE INDEX stop_times_stop_id_idx
  ON stage_gtfs.stop_times (stop_id);

CREATE TABLE stage_gtfs.stops (
  stop_id               integer     PRIMARY KEY,
  stop_code             text,
  stop_name             text,
  stop_desc             text,
  stop_lat              double precision,
  stop_lon              double precision,
  zone_id               text,
  stop_url              text,
  location_type         smallint,
  parent_station        integer,
  wheelchair_boarding   smallint,
  platform_code         text,
  vehicle_type          smallint
);

/*
 * # Derived GTFS tables
 */

CREATE TABLE stage_gtfs.service_dates (
 service_id    text      PRIMARY KEY,
 dates         date[]
);
COMMENT ON TABLE stage_gtfs.service_dates IS
'Operation days of service ids, based on gtfs calendar and calendar_dates.';

CREATE OR REPLACE FUNCTION stage_gtfs.populate_service_dates()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
 cnt       integer;
 mindate   date;
 maxdate   date;
BEGIN
 DELETE FROM stage_gtfs.service_dates;
 GET DIAGNOSTICS cnt = ROW_COUNT;
 RAISE NOTICE '% rows deleted from stage_gtfs.service_dates', cnt;

 SELECT INTO mindate min(start_date)
 FROM stage_gtfs.calendar;
 SELECT INTO maxdate max(end_date)
 FROM stage_gtfs.calendar;
 RAISE NOTICE 'Date range in stage_gtfs.calendar: % ... %', mindate, maxdate;

 WITH
   /*
    * Pivot "wide" table with days of week in cols
    * into "long" table with rows for valid dows only.
    */
   dows AS (
   	SELECT service_id, 0 AS dow, start_date, end_date
   	FROM stage_gtfs.calendar WHERE sunday IS TRUE
   	UNION
   	SELECT service_id, 1 AS dow, start_date, end_date
   	FROM stage_gtfs.calendar WHERE monday IS TRUE
   	UNION
   	SELECT service_id, 2 AS dow, start_date, end_date
   	FROM stage_gtfs.calendar WHERE tuesday IS TRUE
   	UNION
   	SELECT service_id, 3 AS dow, start_date, end_date
   	FROM stage_gtfs.calendar WHERE wednesday IS TRUE
   	UNION
   	SELECT service_id, 4 AS dow, start_date, end_date
   	FROM stage_gtfs.calendar WHERE thursday IS TRUE
   	UNION
   	SELECT service_id, 5 AS dow, start_date, end_date
   	FROM stage_gtfs.calendar WHERE friday IS TRUE
   	UNION
   	SELECT service_id, 6 AS dow, start_date, end_date
   	FROM stage_gtfs.calendar WHERE saturday IS TRUE
   ),
   /*
    * Construct a full set of individual dates so we can make weekday validities
    * into actual dates.
    */
   alldates AS (
   	SELECT
       generate_series::date AS date,
       extract(DOW FROM generate_series) AS dow
   	FROM generate_series(mindate::timestamp, maxdate::timestamp, '1 day'::interval)
   ),
   validdates AS (
     /*
   	 * Now the service_id is repeated for every single date on which it is valid.
      */
     SELECT dows.service_id, alldates.date, alldates.dow
   	FROM alldates
   		INNER JOIN dows
   		ON alldates.date <@ daterange(dows.start_date, dows.end_date + 1)
   		AND alldates.dow = dows.dow
   	/*
      * gtfs calendar_dates table format suits our needs without wrangling
      * between wide and long data.
      * Add calendar_dates with ADDED service.
      */
   	UNION
   	SELECT service_id, date, extract(DOW FROM date) AS dow
   	FROM stage_gtfs.calendar_dates
   	WHERE exception_type = 1
     /*
   	 * Remove calendar_dates with REMOVED service
      */
   	EXCEPT
   	SELECT service_id, date, extract(DOW FROM date) AS dow
   	FROM stage_gtfs.calendar_dates
   	WHERE exception_type = 2
   	ORDER BY date
   )
   INSERT INTO stage_gtfs.service_dates (service_id, dates)
   SELECT service_id, array_agg(date) AS dates
   FROM validdates
   GROUP BY service_id
   ORDER BY service_id;
   GET DIAGNOSTICS cnt = ROW_COUNT;
   RAISE NOTICE '% rows inserted into stage_gtfs.service_dates', cnt;

   RETURN 'OK';
END;
$$;

CREATE TABLE stage_gtfs.shape_lines (
 shape_id              text        PRIMARY KEY,
 gtfs_dist_total       double precision,
 geom                  geometry(LINESTRING, 3067),
 wgs_geom              geometry(LINESTRING, 4326)
);
CREATE INDEX ON stage_gtfs.shape_lines
USING GIST(geom);
CREATE INDEX ON stage_gtfs.shape_lines
USING GIST(wgs_geom);
COMMENT ON TABLE stage_gtfs.shape_lines IS
'GTFS trip geometries as in original "shapes" table,
but each record is a linestring instead of a point.
Includes both TM35 and WGS84 geometries.';

CREATE FUNCTION stage_gtfs.populate_shape_lines()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
 cnt   integer;
BEGIN

 DELETE FROM stage_gtfs.shape_lines;
 GET DIAGNOSTICS cnt = ROW_COUNT;
 RAISE NOTICE '% rows deleted from stage_gtfs.shape_lines', cnt;

 WITH
   shape_pts AS (
     SELECT
       shape_id,
       shape_pt_sequence,
       shape_dist_traveled,
       ST_SetSRID(ST_MakePoint(shape_pt_lon, shape_pt_lat), 4326) AS pt_geom
     FROM stage_gtfs.shapes
   )
 INSERT INTO stage_gtfs.shape_lines (
   shape_id, gtfs_dist_total, wgs_geom
 )
 SELECT
   shape_id,
   max(shape_dist_traveled) AS gtfs_dist_total,
   ST_MakeLine(pt_geom ORDER BY shape_pt_sequence) AS geom
 FROM shape_pts
 GROUP BY shape_id;
 GET DIAGNOSTICS cnt = ROW_COUNT;
 RAISE NOTICE '% rows inserted into stage_gtfs.shape_lines', cnt;

 UPDATE stage_gtfs.shape_lines
 SET geom = ST_Transform(wgs_geom, 3067);
 GET DIAGNOSTICS cnt = ROW_COUNT;
 RAISE NOTICE 'geom set for % rows in stage_gtfs.shape_lines', cnt;

 RETURN 'OK';
END;
$$;

CREATE FUNCTION stage_gtfs.populate_routes_from_gtfs()
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
COMMENT ON FUNCTION stage_gtfs.populate_routes_from_gtfs IS
'Insert tram and bus routes into sched schema,
with mode indicated as mode_type instead of an integer.
Note that bus mode is NOT based on standard GTFS integer id
but on HSL-specific ids!';

CREATE TABLE stage_gtfs.stops_with_mode (
  stopid        integer           NOT NULL,
  mode          public.mode_type  NOT NULL,
  code          text,
  name          text,
  descr         text,
  parent        integer,
  geom          geometry(POINT, 3067) NOT NULL,
  PRIMARY KEY (stopid, mode)
);
COMMENT ON TABLE stage_gtfs.stops_with_mode IS
'Stops with travel mode; same stop for multiple modes
is indicated by multiple records.';
CREATE INDEX stops_with_mode_geom_idx
  ON stage_gtfs.stops_with_mode
  USING GIST(geom);

CREATE OR REPLACE FUNCTION stage_gtfs.populate_stops_with_mode()
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
    mode_routes AS (
      SELECT
        route_id,
        (CASE
         WHEN route_type = 0 THEN 'tram'
         WHEN route_type IN (700, 701, 702, 704) THEN 'bus'
         END
        )::public.mode_type AS mode
      FROM stage_gtfs.routes
    ),
    mode_trips AS (
      SELECT t.trip_id, r.mode
      FROM stage_gtfs.trips     AS t
        INNER JOIN mode_routes  AS r
        ON t.route_id = r.route_id
    ),
    mode_stoptimes AS (
      SELECT DISTINCT st.stop_id, t.mode
      FROM stage_gtfs.stop_times  AS st
      INNER JOIN mode_trips       AS t
      ON st.trip_id = t.trip_id
    ),
    inserted AS (
      INSERT INTO stage_gtfs.stops_with_mode
      SELECT
        s.stop_id     AS stopid,
        m.mode        AS mode,
        s.stop_code   AS code,
        s.stop_name   AS name,
        s.stop_desc   AS desc,
        s.parent_station	AS parent,
        ST_Transform(
          ST_SetSRID(
            ST_MakePoint(s.stop_lon, s.stop_lat), 4326),
          3067) AS geom
      FROM stage_gtfs.stops     AS s
      INNER JOIN mode_stoptimes AS m
      ON s.stop_id = m.stop_id
      ORDER BY s.stop_id, m.mode
      ON CONFLICT DO NOTHING
      RETURNING *
    )
  SELECT i.mode, count(i.stopid)::bigint AS rows_inserted
  FROM inserted AS i
  GROUP BY i.mode
  ORDER BY i.mode;
END;
$$;

CREATE TABLE stage_gtfs.successive_stops (
  i_stop        integer           NOT NULL,
  j_stop        integer           NOT NULL,
  PRIMARY KEY (i_stop, j_stop)
);
COMMENT ON TABLE stage_gtfs.successive_stops IS
'Stop pairs that occur in schedules,
for finding network routes between stops.
Based on ALL bus and tram stops found in the GTFS data.';

CREATE OR REPLACE FUNCTION stage_gtfs.populate_successive_stops()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt   integer;
BEGIN
  DELETE FROM stage_gtfs.successive_stops;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows deleted from stage_gtfs.successive_stops', cnt;

  INSERT INTO stage_gtfs.successive_stops
  SELECT DISTINCT
    a.stop_id   AS i_stop,
    c.stop_id   AS j_stop
  FROM stage_gtfs.stop_times            AS a
  INNER JOIN stage_gtfs.stops_with_mode AS b
    ON  a.stop_id       = b.stopid
  INNER JOIN stage_gtfs.stop_times      AS c
    ON  a.trip_id       = c.trip_id
    AND a.stop_sequence = (c.stop_sequence - 1);
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% stop pairs inserted into stage_gtfs.successive_stops', cnt;

  RETURN 'OK';
END;
$$;
COMMENT ON FUNCTION stage_gtfs.populate_successive_stops IS
'Find unique successive stop pairs
from tram & bus stop times
and insert them into stage_gtfs.successive_stops table.
stage_gtfs.successive_stops is emptied first.';

CREATE TABLE stage_gtfs.trips_with_dates (
  trip_id               text              PRIMARY KEY,
  service_id            text,
  route_id              text,
  direction_id          smallint,
  trip_start_hms        interval,
  shape_id              text,
  dates                 date[]
);
COMMENT ON TABLE stage_gtfs.trips_with_dates IS
'Trips with validity dates included,
without need to join gtfs service date tables.';

CREATE OR REPLACE FUNCTION stage_gtfs.populate_trips_with_dates()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt       integer;
BEGIN
  DELETE FROM stage_gtfs.trips_with_dates;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows deleted from stage_gtfs.trips_with_dates', cnt;

  INSERT INTO stage_gtfs.trips_with_dates (
    trip_id, service_id, route_id, direction_id, shape_id
  )
  SELECT trip_id, service_id, route_id, direction_id, shape_id
  FROM stage_gtfs.trips;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows inserted into stage_gtfs.trips_with_dates', cnt;

  UPDATE stage_gtfs.trips_with_dates AS tr
  SET trip_start_hms = st.departure_time
  FROM (
    SELECT trip_id, departure_time
    FROM stage_gtfs.stop_times
    WHERE stop_sequence = 1
  ) AS st
  WHERE tr.trip_id = st.trip_id;
  RAISE NOTICE 'trip_start_hms field updated for % rows', cnt;

  UPDATE stage_gtfs.trips_with_dates AS tr
  SET
    service_id = sd.service_id,
    dates      = sd.dates
  FROM (
    SELECT service_id, dates
    FROM stage_gtfs.service_dates
  ) AS sd
  WHERE tr.service_id = sd.service_id;
  RAISE NOTICE 'service_id and dates fields updated for % rows', cnt;

  RETURN 'OK';
END;
$$;
COMMENT ON FUNCTION stage_gtfs.populate_trips_with_dates_trips IS
'Initializes and fills trips_with_dates table with trip ids,
service ids, trip start times and validity dates.';

CREATE TABLE stage_gtfs.normalized_stop_times (
  trip_id               text,
  arr_time_diff         interval,
  dep_time_diff         interval,
  stop_id               integer,
  stop_sequence         smallint,
  rel_dist_traveled     double precision,
  timepoint             boolean,
  PRIMARY KEY (trip_id, stop_sequence)
);
CREATE INDEX ON stage_gtfs.normalized_stop_times(stop_id);
COMMENT ON TABLE stage_gtfs.normalized_stop_times IS
'Stop times of bus and tram trips, where each trip id is assigned
the initial departure time of the trip, and each stop event
is assigned an arrival and a departure time difference based on that start time.
Also shape_dist_traveled (kilometers) is made into "relative" distance traveled,
meaning at each stop the proportion of the total trip shape length.
This is an intermediate step before the trips are grouped into
records with date & initial departure time arrays
based on route, direction, stop sequence and time difference information.';

CREATE OR REPLACE FUNCTION stage_gtfs.populate_normalized_stop_times()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt       bigint;
  cnt_fail  bigint;
BEGIN
  DELETE FROM stage_gtfs.normalized_stop_times;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows deleted from stage_gtfs.normalized_stop_times', cnt;

  INSERT INTO stage_gtfs.normalized_stop_times (
    trip_id,
    arr_time_diff,
    dep_time_diff,
    stop_id,
    stop_sequence,
    rel_dist_traveled,
    timepoint
  )
  SELECT
    st.trip_id,
    st.arrival_time - twd.trip_start_hms        AS arr_time_diff,
    st.departure_time - twd.trip_start_hms      AS dep_time_diff,
    st.stop_id,
    st.stop_sequence,
    st.shape_dist_traveled / sl.gtfs_dist_total AS rel_dist_traveled,
    st.timepoint
  FROM stage_gtfs.stop_times AS st
  /*
   * Should there be any non-matching records,
   * left join will leave time differences NULL for us to find later.
   */
  LEFT JOIN stage_gtfs.trips_with_dates AS twd
  ON st.trip_id = twd.trip_id
  LEFT JOIN stage_gtfs.shape_lines      AS sl
  ON twd.shape_id = sl.shape_id;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% records inserted into stage_gtfs.normalized_stop_times', cnt;

  SELECT INTO cnt_fail count(*)
  FROM stage_gtfs.normalized_stop_times
  WHERE arr_time_diff < interval '0 seconds' OR arr_time_diff IS NULL
    OR  dep_time_diff < interval '0 seconds' OR dep_time_diff IS NULL;
  IF cnt_fail > 0 THEN
    RAISE WARNING '% records with negative or NULL dep / arr time diffs in stage_gtfs.normalized_stop_times',
     cnt_fail;
  ELSE
    RAISE NOTICE 'All dep / arr differences in stage_gtfs.normalized_stop_times are valid';
  END IF;

  RETURN 'OK';
END;
$$;

CREATE TABLE stage_gtfs.trip_template_arrays (
  /*
   * This will be just a surrogate pkey with a running number.
   */
  ttid            text          PRIMARY KEY,
  /*
   * These define a unique record:
   */
  route_id        text,
  direction_id    smallint,
  shape_id        text,
  stop_ids        integer[],
  stop_sequences  smallint[],
  rel_distances   double precision[],
  arr_time_diffs  interval[],
  dep_time_diffs  interval[],
  timepoints      boolean[],
  /*
   * These describe to which individual trips the above attributes apply:
   */
  trip_ids        text[],
  service_ids     text[],
  start_times     interval[],
  dates           date[],
  route_found     boolean,
  UNIQUE (route_id, direction_id, shape_id, stop_ids, stop_sequences,
          rel_distances, arr_time_diffs, dep_time_diffs, timepoints)
);
COMMENT ON TABLE stage_gtfs.trip_template_arrays IS
'"Compressed" trip templates from GTFS trips and stop times.
GTFS trips that share identical
- route and direction ids,
- trip shape geometry,
- stop ids and their order,
- relative trip distances at stops,
- stop times calculated as differences from the first stop, and
- timepoint flags
are grouped into one record.
Stop time attributes, as well as the dates and
initial departure times to which the trip template applies,
are stored as arrays that can be later decomposed into rows.
From this table, the records that are successfully routable
on the network can be transferred to the production schedule tables.
Note that this table should already use the 1-2 direction id system
instead of GTFS 0-1 standard.
route_found is populated in a later stage, indicating whether the trip template
has a complete route on the network and can be transferred to sched schema.';

CREATE OR REPLACE FUNCTION stage_gtfs.populate_trip_template_arrays()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt       bigint;
BEGIN
  DELETE FROM stage_gtfs.trip_template_arrays;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows deleted from stage_gtfs.trip_template_arrays', cnt;

  WITH
    stoptime_arrays AS (
      SELECT
        trip_id,
        array_agg(stop_id ORDER BY stop_sequence)           AS stop_ids,
        array_agg(stop_sequence ORDER BY stop_sequence)     AS stop_sequences,
        array_agg(rel_dist_traveled ORDER BY stop_sequence) AS rel_distances,
        array_agg(arr_time_diff ORDER BY stop_sequence)     AS arr_time_diffs,
        array_agg(dep_time_diff ORDER BY stop_sequence)     AS dep_time_diffs,
        array_agg(timepoint ORDER BY stop_sequence)         AS timepoints
      FROM stage_gtfs.normalized_stop_times
      GROUP BY trip_id
    ),
    compressed_arrays AS (
      SELECT
        twd.route_id,
        twd.direction_id,
        twd.shape_id,
        sa.stop_ids,
        sa.stop_sequences,
        sa.rel_distances,
        sa.arr_time_diffs,
        sa.dep_time_diffs,
        sa.timepoints,
        array_agg(twd.trip_id ORDER BY twd.trip_id)         AS trip_ids,
        array_agg(twd.service_id ORDER BY twd.trip_id)      AS service_ids,
        array_agg(twd.trip_start_hms ORDER BY twd.trip_id)  AS start_times
      FROM stoptime_arrays                    AS sa
      INNER JOIN stage_gtfs.trips_with_dates  AS twd
      ON sa.trip_id = twd.trip_id
      GROUP BY
        twd.route_id,
        twd.direction_id,
        twd.shape_id,
        sa.stop_ids,
        sa.stop_sequences,
        sa.rel_distances,
        sa.arr_time_diffs,
        sa.dep_time_diffs,
        sa.timepoints
    )
  INSERT INTO stage_gtfs.trip_template_arrays (
    ttid, route_id, direction_id, shape_id, stop_ids, stop_sequences,
    rel_distances, arr_time_diffs, dep_time_diffs, timepoints,
    trip_ids, service_ids, start_times
  )
  SELECT
    concat_ws(
      '_',
      route_id,
      /*
       * NOTE: We do an early conversion of direction id
       *       from GTFS 0-1 to HFP 1-2 system here.
       */
      direction_id + 1,
      row_number() OVER (PARTITION BY route_id, direction_id)
    ) AS ttid,
    *
  FROM compressed_arrays;

  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows inserted into stage_gtfs.trip_template_arrays', cnt;

  /*
   * The start_times arrays created above still contain duplicated and
   * unsorted values, fix it here.
   */
  WITH
    unnested_times AS (
      SELECT
        ttid,
        unnest(start_times) AS start_time
      FROM stage_gtfs.trip_template_arrays
    ),
    unique_times AS (
      SELECT DISTINCT ttid, start_time
      FROM unnested_times
    ),
    new_time_arrays AS (
      SELECT
        ttid,
        array_agg(start_time ORDER BY start_time) AS start_times
      FROM unique_times
      GROUP BY ttid
    )
  UPDATE stage_gtfs.trip_template_arrays  AS tta
  SET start_times = nta.start_times
  FROM new_time_arrays                    AS nta
  WHERE tta.ttid = nta.ttid;

  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE 'Start time arrays with unique values updated for % rows in stage_gtfs.trip_template_arrays', cnt;

  WITH
    ttids_tripids AS (
      SELECT
        ttid,
        unnest(trip_ids) AS trip_id
      FROM stage_gtfs.trip_template_arrays
    ),
    ttid_all_dates AS (
      SELECT
        tt.ttid,
        unnest(twd.dates) AS valid_date
      FROM ttids_tripids                      AS tt
      INNER JOIN stage_gtfs.trips_with_dates  AS twd
      ON tt.trip_id = twd.trip_id
    ),
    ttid_uniq_dates AS (
      SELECT DISTINCT ttid, valid_date
      FROM ttid_all_dates
      ORDER BY ttid, valid_date
    )
  UPDATE stage_gtfs.trip_template_arrays AS tta
  SET dates = da.dates
  FROM (
    SELECT
      ttid,
      array_agg(valid_date) AS dates
    FROM ttid_uniq_dates
    GROUP BY ttid
  ) AS da
  WHERE tta.ttid = da.ttid;

  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows updated with dates array', cnt;

  RETURN 'OK';
END;
$$;

COMMIT;
