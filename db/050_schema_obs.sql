CREATE SCHEMA obs;

COMMENT ON SCHEMA obs IS
'Observed transit vehicle journeys and their events on the network.';

-- JOURNEYS (jrn)
CREATE TABLE obs.journey (
  jrnid           uuid          PRIMARY KEY,
  route           text          NOT NULL,
  dir             smallint      NOT NULL CHECK (dir IN (1, 2)),
  start_tst       timestamptz   NOT NULL,
  route_ver_id    text          NOT NULL REFERENCES nw.route_version(route_ver_id),
  oper            integer       NOT NULL,
  veh             integer       NOT NULL
);

COMMENT ON TABLE obs.journey IS
'Realized transit vehicle journeys from HFP data.';
COMMENT ON COLUMN obs.journey.jrnid IS
'Unique journey identifier. MD5 hash from <route_dir_oday_start_oper_veh>.';
COMMENT ON COLUMN obs.journey.route IS
'Scheduled route according to HFP.';
COMMENT ON COLUMN obs.journey.dir IS
'Scheduled direction according to HFP.';
COMMENT ON COLUMN obs.journey.start_tst IS
'Scheduled (oday+start) timestamp in UTC according to HFP.';
COMMENT ON COLUMN obs.journey.route_ver_id IS
'Route version id matching nw.route_version.route_ver_id, generated using route, dir and start_tst.';
COMMENT ON COLUMN obs.journey.oper IS
'Unique transit operator id according to HFP.';
COMMENT ON COLUMN obs.journey.veh IS
'Vehicle id, unique within operator, according to HFP.';

CREATE FUNCTION obs.tg_insert_journey_handler()
RETURNS trigger
AS $$
DECLARE
  correct_jrnid   uuid;
  rtver_id_found  text;
BEGIN
  -- Check that jrnid is calculated correctly
  correct_jrnid := md5(
    concat_ws('_',
      NEW.route,
      NEW.dir,
      (NEW.start_tst AT TIME ZONE 'Europe/Helsinki')::date,
      (NEW.start_tst AT TIME ZONE 'Europe/Helsinki')::time,
      NEW.oper,
      NEW.veh
    )
  )::uuid;
  IF NEW.jrnid <> correct_jrnid THEN
    RAISE NOTICE 'Skipping jrnid %: jrnid should be %', NEW.jrnid, correct_jrnid;
    RETURN NULL;
  END IF;

  -- Check and add route version id
  SELECT INTO rtver_id_found rv.route_ver_id
  FROM nw.route_version AS rv
  WHERE NEW.route = rv.route
    AND NEW.dir = rv.dir
    AND (NEW.start_tst AT TIME ZONE 'Europe/Helsinki')::date <@ rv.valid_during;

  IF NOT FOUND THEN
    RAISE NOTICE 'Skipping jrnid %: route version not found', NEW.jrnid;
    RETURN NULL;
  END IF;

  NEW.route_ver_id := rtver_id_found;

  RETURN NEW;

END;
$$ LANGUAGE PLPGSQL;

COMMENT ON FUNCTION obs.tg_insert_journey_handler IS
'Checks that the MD5 jrnid is correct and finds the correct route_ver_id when inserting a journey.';

CREATE TRIGGER tg_insert_journey
BEFORE INSERT OR UPDATE ON obs.journey
FOR EACH ROW EXECUTE FUNCTION obs.tg_insert_journey_handler();

-- HFP POINTS (obs)
CREATE TABLE obs.hfp_point (
  jrnid                 uuid          NOT NULL REFERENCES obs.journey(jrnid) ON DELETE CASCADE,
  tst                   timestamptz   NOT NULL,
  odo                   integer       NOT NULL,
  drst                  boolean,
  represents_n_points   integer       NOT NULL,
  represents_time_s     float8,
  geom                  geometry(POINT, 3067) NOT NULL,

  PRIMARY KEY (jrnid, tst)
);

CREATE INDEX ON obs.hfp_point USING GIST(geom);

COMMENT ON TABLE obs.hfp_point IS
'GPS positions of journeys from HFP data.';
COMMENT ON COLUMN obs.hfp_point.jrnid IS
'Journey identifier.';
COMMENT ON COLUMN obs.hfp_point.tst IS
'UTC timestamp.';
COMMENT ON COLUMN obs.hfp_point.odo IS
'Vehicle odometer reading in meters.';
COMMENT ON COLUMN obs.hfp_point.drst IS
'true = at least one vehicle door open, false = all doors closed, NULL = door status unknown.';
COMMENT ON COLUMN obs.hfp_point.represents_n_points IS
'Number of removed raw data points that this point represents reliably (when redundant halted points are removed).';
COMMENT ON COLUMN obs.hfp_point.represents_time_s IS
'Duration (with removed raw data points) that this point represents reliably.';
COMMENT ON COLUMN obs.hfp_point.geom IS
'Journey POINT position in ETRS-TM35 coordinates.';

SELECT create_hypertable(
  relation            => 'obs.hfp_point',
  time_column_name    => 'tst',
  partitioning_column => 'jrnid',
  number_partitions   => 4,
  chunk_time_interval => '1 day'::interval
);

CREATE VIEW obs.view_hfp_point_xy AS (
  SELECT jrnid, tst, odo, drst, represents_n_points, represents_time_s,
    ST_X(geom) AS X, ST_Y(geom) AS Y
  FROM obs.hfp_point
);

COMMENT ON VIEW obs.view_hfp_point_xy IS
'HFP point geometries with coordinates in X and Y columns, enabling imports from CSV files.';

CREATE FUNCTION obs.tg_insert_xy_hfp_point()
RETURNS trigger
AS $$
BEGIN
  INSERT INTO obs.hfp_point (
    jrnid, tst, odo, drst, represents_n_points, represents_time_s, geom
  )
  VALUES (
    NEW.jrnid, NEW.tst, NEW.odo, NEW.drst, NEW.represents_n_points, NEW.represents_time_s,
    ST_SetSRID(ST_MakePoint(NEW.X, NEW.Y), 3067)
  );
  RETURN NEW;
END;
$$ LANGUAGE PLPGSQL;

COMMENT ON FUNCTION obs.tg_insert_xy_hfp_point IS
'Converts X and Y coordinates into geom when inserting HFP points through obs.view_hfp_point_xy.';

CREATE TRIGGER tg_insert_xy_hfp_point
INSTEAD OF INSERT ON obs.view_hfp_point_xy
FOR EACH ROW EXECUTE FUNCTION obs.tg_insert_xy_hfp_point();

-- PROJECTED POINTS ON LINKS
CREATE TABLE obs.point_on_link (
  jrnid                 uuid          NOT NULL REFERENCES obs.journey(jrnid) ON DELETE CASCADE,
  tst                   timestamptz   NOT NULL,
  link_seq              integer,
  link_id               integer       NOT NULL REFERENCES nw.link(link_id) ON DELETE CASCADE ON UPDATE CASCADE,
  link_reversed         boolean,
  location_on_link      float8,
  distance_from_link    float8,

  PRIMARY KEY (jrnid, tst)
);

CREATE INDEX ON obs.point_on_link USING btree (tst DESC);
CREATE INDEX ON obs.point_on_link USING btree (link_id, link_reversed);

COMMENT ON TABLE obs.point_on_link IS
'HFP points projected to best matching links on route. Link references should be generated by obs.get_point_on_link_candidates().';
COMMENT ON COLUMN obs.point_on_link.jrnid IS
'Journey identifier.';
COMMENT ON COLUMN obs.point_on_link.tst IS
'UTC timestamp.';
COMMENT ON COLUMN obs.point_on_link.link_seq IS
'Link sequence number.';
COMMENT ON COLUMN obs.point_on_link.link_id IS
'Link identifier.';
COMMENT ON COLUMN obs.point_on_link.link_reversed IS
'true = link_id refers to the reversed version of a two-way link.';
COMMENT ON COLUMN obs.point_on_link.location_on_link IS
'Projected relative location along the link, between 0.0 and 1.0. Use link length with this to get the absolute location.';
COMMENT ON COLUMN obs.point_on_link.distance_from_link IS
'Distance between original and link-projected position. Negative, if point is on the left side of directed link.';

SELECT create_hypertable(
  relation            => 'obs.point_on_link',
  time_column_name    => 'tst',
  partitioning_column => 'jrnid',
  number_partitions   => 4,
  chunk_time_interval => '1 day'::interval
);

CREATE VIEW obs.view_point_on_link_geom AS (
  SELECT
    pol.jrnid,
    pol.tst,
    pol.link_seq,
    pol.link_id,
    pol.link_reversed,
    pol.location_on_link,
    pol.distance_from_link,
    ST_LineInterpolatePoint(vld.geom, pol.location_on_link) AS geom
  FROM 
    obs.point_on_link AS pol
  INNER JOIN 
    nw.view_link_directed AS vld
    ON (pol.link_id = vld.link_id
      AND pol.link_reversed = vld.link_reversed)
);

COMMENT ON VIEW obs.view_point_on_link_geom IS
'Point geometries of projected HFP points, 
based on link geometry and linear location value.';

CREATE VIEW obs.view_point_to_hfp_line_geom AS (
  SELECT
    pol.jrnid,
    pol.tst,
    pol.link_seq,
    pol.link_id,
    pol.link_reversed,
    pol.location_on_link,
    pol.distance_from_link,
    ST_MakeLine(
      hp.geom,
      ST_LineInterpolatePoint(vld.geom, pol.location_on_link)
    ) AS geom
  FROM 
    obs.point_on_link AS pol
  INNER JOIN 
    nw.view_link_directed AS vld
    ON (pol.link_id = vld.link_id
      AND pol.link_reversed = vld.link_reversed)
  INNER JOIN 
    obs.hfp_point AS hp
    ON (pol.jrnid = hp.jrnid
      AND pol.tst = hp.tst)
);

COMMENT ON VIEW obs.view_point_to_hfp_line_geom IS
'Linestring geometries between HFP points
and corresponding projected points on link.';

-- HALT (non-movement) events calculated from points_on_link
CREATE TABLE obs.halt_on_journey (
  jrnid                 uuid          NOT NULL REFERENCES obs.journey(jrnid) ON DELETE CASCADE,
  tst                   timestamptz   NOT NULL,
  total_s               float4,
  door_open_s           float4,
  door_closed_s         float4,
  represents_time_s     float4,

  PRIMARY KEY (jrnid, tst)
);

COMMENT ON TABLE obs.halt_on_journey IS
'Halt (non-movement) events derived from points on link. (jrnid, tst) connects these back to obs.point_on_link.';
COMMENT ON COLUMN obs.halt_on_journey.jrnid IS
'Journey identifier.';
COMMENT ON COLUMN obs.halt_on_journey.tst IS
'UTC timestamp.';
COMMENT ON COLUMN obs.halt_on_journey.total_s IS
'Total duration in seconds, before moving again.';
COMMENT ON COLUMN obs.halt_on_journey.door_open_s IS
'Total duration with doors open.';
COMMENT ON COLUMN obs.halt_on_journey.door_closed_s IS
'Total duration with doors closed.';
COMMENT ON COLUMN obs.halt_on_journey.represents_time_s IS
'Reliably represented total duration: sum(represents_time_s) from corresponding points on link.';

CREATE VIEW obs.view_halt_on_journey_extended AS (
  SELECT
    hoj.jrnid,
    hoj.tst,
    hoj.tst + hoj.total_s * interval '1 second'             AS end_tst,
    st.stop_id,
    hoj.total_s,
    hoj.total_s - hoj.represents_time_s                     AS unknown_s,
    hoj.door_open_s,
    hoj.door_closed_s,
    hoj.total_s - hoj.door_open_s - hoj.door_closed_s       AS door_unknown_s,
    hoj.represents_time_s,
    pol.link_id,
    pol.link_reversed,
    pol.link_seq,
    pol.location_on_link * vld.length_m                     AS halt_location_m,
    pol.distance_from_link,
    ST_LineInterpolatePoint(vld.geom, pol.location_on_link) AS geom
  FROM obs.halt_on_journey          AS hoj
  INNER JOIN obs.point_on_link      AS pol
    ON (hoj.jrnid = pol.jrnid AND hoj.tst = pol.tst)
  INNER JOIN nw.view_link_directed  AS vld
    ON (pol.link_id = vld.link_id AND pol.link_reversed = vld.link_reversed)
  LEFT JOIN nw.stop                 AS st
    ON (
      vld.link_id = st.link_id AND vld.link_reversed = st.link_reversed
      AND (pol.location_on_link * vld.length_m) >= (st.location_on_link * vld.length_m - st.stop_radius_m)
      AND (pol.location_on_link * vld.length_m) <= (st.location_on_link * vld.length_m + st.stop_radius_m)
    )
);

COMMENT ON VIEW obs.view_halt_on_journey_extended IS
'Halt events with represented, total and door times, possible stop references as well as linear locations along links and derived point geometries.';

CREATE VIEW obs.view_point_obs_combined AS (
  SELECT
    hp.jrnid,
    hp.tst,
    hp.odo,
    hp.drst,
    hp.represents_n_points,
    hp.represents_time_s,
    pol.link_seq,
    pol.link_id,
    pol.link_reversed,
    pol.location_on_link,
    pol.distance_from_link,
    hp.geom                         AS hfp_point_geom,
    ST_LineInterpolatePoint(
      vld.geom, pol.location_on_link
      )                             AS point_on_link_geom
  FROM
    obs.hfp_point                   AS hp
    LEFT JOIN obs.point_on_link     AS pol
      ON ((hp.jrnid, hp.tst) = (pol.jrnid, pol.tst))
    LEFT JOIN nw.view_link_directed AS vld
      ON ((pol.link_id, pol.link_reversed) = (vld.link_id, vld.link_reversed))
);

COMMENT ON VIEW obs.view_point_obs_combined IS
'Combines hfp_point and point_on_link observations.';

/*
 * LINK ON JOURNEY (traversed links) data model and routines.
 */

CREATE TABLE obs.link_on_journey (
  jrnid         uuid REFERENCES obs.journey(jrnid) ON DELETE CASCADE,
  enter_tst     timestamptz,
  exit_tst      timestamptz,
  link_seq      integer,
  link_id       integer REFERENCES nw.link(link_id) ON DELETE CASCADE ON UPDATE CASCADE,
  link_reversed boolean NOT NULL,

  PRIMARY KEY (jrnid, enter_tst)
);

CREATE INDEX ON obs.link_on_journey USING btree(link_id, link_reversed);

COMMENT ON TABLE obs.link_on_journey IS
'Links on route traversed completely by observed journeys, interpolated from obs.point_on_link.';
COMMENT ON COLUMN obs.link_on_journey.jrnid IS
'Journey identifier.';
COMMENT ON COLUMN obs.link_on_journey.enter_tst IS
'Interpolated timestamp at the link start.';
COMMENT ON COLUMN obs.link_on_journey.exit_tst IS
'Interpolated timestamp at the link end, equals enter_tst of the next link.';
COMMENT ON COLUMN obs.link_on_journey.link_seq IS
'Link sequence number, same as in the corresponding nw.link_on_route.';
COMMENT ON COLUMN obs.link_on_journey.link_id IS
'Link identifier.';
COMMENT ON COLUMN obs.link_on_journey.link_reversed IS
'true = link_id refers to the reversed version of a two-way link.';

CREATE VIEW obs.view_link_on_journey_stats AS (
  SELECT
    jrn.jrnid,
    jrn.route,
    jrn.dir,
    (start_tst AT TIME ZONE 'Europe/Helsinki')::date    AS oday,
    (start_tst AT TIME ZONE 'Europe/Helsinki')::time    AS start,
    jrn.route_ver_id,
    jrn.oper,
    jrn.veh,
    loj.link_seq,
    loj.link_id,
    loj.link_reversed,
    loj.enter_tst,
    loj.exit_tst,
    extract('epoch' FROM loj.exit_tst - loj.enter_tst)  AS duration_s,
    vld.length_m                                        AS link_length_m,
    3.6 * (vld.length_m / extract('epoch' FROM loj.exit_tst - loj.enter_tst)) AS speed_kmh
  FROM obs.journey                  AS jrn
  INNER JOIN obs.link_on_journey    AS loj
    ON (jrn.jrnid = loj.jrnid)
  INNER JOIN nw.view_link_directed  AS vld
    ON (loj.link_id = vld.link_id AND loj.link_reversed = vld.link_reversed)
);

CREATE VIEW obs.view_journey_stats AS (
  SELECT
    jrn.jrnid,
    jrn.route,
    jrn.dir,
    jrn.start_tst,
    (jrn.start_tst AT TIME ZONE 'Europe/Helsinki')::date            AS oday,
    (jrn.start_tst AT TIME ZONE 'Europe/Helsinki')::time::interval  AS "start",
    jrn.oper,
    jrn.veh,
    jrn.route_ver_id,
    lor.n_link_on_route,
    vpoc.n_hfp_point,
    vpoc.n_point_on_link,
    vpoc.first_hp_tst,
    vpoc.last_hp_tst,
    vpoc.total_duration_s,
    vpoc.total_hp_represents_time_s,
    vpoc.total_pol_represents_time_s,
    loj.n_link_on_journey,
    hoj.n_halt_on_journey
  FROM 
    obs.journey AS jrn
    LEFT JOIN (
      SELECT route_ver_id, count(*) AS n_link_on_route
      FROM nw.link_on_route
      GROUP BY route_ver_id
    ) AS lor
      ON (jrn.route_ver_id = lor.route_ver_id)
    LEFT JOIN (
      SELECT 
        jrnid, 
        count(tst)              AS n_hfp_point,
        count(link_seq)         AS n_point_on_link,
        min(tst)                AS first_hp_tst,
        max(tst)                AS last_hp_tst,
        extract(
          epoch FROM max(tst)-min(tst)
        )                       AS total_duration_s,
        sum(represents_time_s)  AS total_hp_represents_time_s,
        sum(
          CASE WHEN link_seq IS NULL THEN 0 
          ELSE represents_time_s END
        )                       AS total_pol_represents_time_s
      FROM obs.view_point_obs_combined
      GROUP BY jrnid
    ) AS vpoc
      ON (jrn.jrnid = vpoc.jrnid)
    LEFT JOIN (
      SELECT jrnid, count(*) AS n_link_on_journey
      FROM obs.link_on_journey
      GROUP BY jrnid
    ) AS loj
      ON (jrn.jrnid = loj.jrnid)
    LEFT JOIN (
      SELECT jrnid, count(*) AS n_halt_on_journey
      FROM obs.halt_on_journey
      GROUP BY jrnid
    ) AS hoj
      ON (jrn.jrnid = hoj.jrnid)
);

COMMENT ON VIEW obs.view_journey_stats IS
'Journeys with oday and start, and statistics from child objects.';

/*
 * Section results.
 */

CREATE VIEW obs.view_full_section_traversal AS (
  WITH
    last_per_section_marked AS (
      SELECT
        section_id,
        link_id,
        link_reversed,
        link_seq = max(link_seq) OVER (PARTITION BY section_id) AS is_last
      FROM
        nw.link_on_section
    ),
    section_bound_links AS (
      SELECT
        i.section_id,
        i.link_id       AS i_link_id,
        i.link_reversed AS i_link_reversed,
        j.link_id       AS j_link_id,
        j.link_reversed AS j_link_reversed
      FROM
        nw.link_on_section AS i
        INNER JOIN last_per_section_marked AS j ON (
          i.section_id = j.section_id
          AND i.link_seq = 1
          AND j.is_last
        )
    )
  SELECT
    sbl.section_id      AS section_id,
    i_loj.jrnid         AS jrnid,
    i_loj.enter_tst     AS section_enter_tst,
    j_loj.exit_tst      AS section_exit_tst,
    extract(
      'epoch' FROM j_loj.exit_tst - i_loj.enter_tst
      )                 AS section_duration_s
  FROM
    section_bound_links AS sbl
    INNER JOIN obs.link_on_journey AS i_loj ON (
      sbl.i_link_id = i_loj.link_id
      AND sbl.i_link_reversed = i_loj.link_reversed
    )
    INNER JOIN obs.link_on_journey AS j_loj ON (
      sbl.j_link_id = j_loj.link_id
      AND sbl.j_link_reversed = j_loj.link_reversed
      AND i_loj.jrnid = j_loj.jrnid
    )
);

COMMENT ON VIEW obs.view_full_section_traversal IS
'Traversal events of journeys through full sections.
Journeys that diverted halfway are not included.
NOTE: Events are unique by jrnid ONLY if the journey
traversed both the first and last link of the section
exactly once!';

CREATE VIEW obs.total_journey_halt_times_on_section AS (
  SELECT
    los.section_id,
    hoj.jrnid,
    coalesce(sum(hoj.door_open_s), 0.0)   AS tot_door_open_s,
    coalesce(sum(hoj.door_closed_s), 0.0) AS tot_door_closed_s,
    coalesce(
      sum(hoj.total_s) - sum(hoj.represents_time_s), 0.0
    )                                     AS tot_door_unknown_s
  FROM
    obs.halt_on_journey           AS hoj
    INNER JOIN obs.point_on_link  AS pol
      ON ((hoj.jrnid, hoj.tst) = (pol.jrnid, pol.tst))
    INNER JOIN nw.link_on_section AS los
      ON ((pol.link_id, pol.link_reversed) = (los.link_id, los.link_reversed))
  GROUP BY
    los.section_id,
    hoj.jrnid
);