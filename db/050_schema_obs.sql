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
  jrnid                 uuid          NOT NULL REFERENCES obs.journey(jrnid),
  tst                   timestamptz   NOT NULL,
  odo                   integer       NOT NULL,
  drst                  boolean,
  represents_n_points   integer       NOT NULL,
  represents_time_s     float8,
  geom                  geometry(POINT, 3067) NOT NULL,

  PRIMARY KEY (jrnid, tst)
);

CREATE INDEX ON obs.hfp_point USING GIST(geom);

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

CREATE TRIGGER tg_insert_xy_hfp_point
INSTEAD OF INSERT ON obs.view_hfp_point_xy
FOR EACH ROW EXECUTE FUNCTION obs.tg_insert_xy_hfp_point();

-- PROJECTED POINTS ON LINKS
CREATE TABLE obs.point_on_link (
  jrnid                 uuid          NOT NULL REFERENCES obs.journey(jrnid),
  tst                   timestamptz   NOT NULL,
  link_seq              integer,
  link_id               integer       NOT NULL REFERENCES nw.link(link_id),
  link_reversed         boolean,
  location_on_link      float8,
  distance_from_link    float8,

  PRIMARY KEY (jrnid, tst)
);

CREATE INDEX ON obs.point_on_link USING btree (tst DESC);
CREATE INDEX ON obs.point_on_link USING btree (link_id, link_reversed);

SELECT create_hypertable(
  relation            => 'obs.point_on_link',
  time_column_name    => 'tst',
  partitioning_column => 'jrnid',
  number_partitions   => 4,
  chunk_time_interval => '1 day'::interval
);

-- HALT (non-movement) events calculated from points_on_link
CREATE TABLE obs.halt_on_journey (
  jrnid                 uuid          NOT NULL REFERENCES obs.journey(jrnid),
  tst                   timestamptz   NOT NULL,
  total_s               float4,
  door_open_s           float4,
  door_closed_s         float4,
  represents_time_s     float4,

  PRIMARY KEY (jrnid, tst)
);

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
