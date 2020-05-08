/*
 * Construct linestring geometries for trip templates.
 */

\timing on
BEGIN;

DROP MATERIALIZED VIEW IF EXISTS sched.trip_template_geoms;

CREATE MATERIALIZED VIEW sched.trip_template_geoms AS (
  WITH
    segment_geoms AS (
      SELECT
        s.ttid,
        s.linkid,
        row_number() OVER (PARTITION BY s.ttid ORDER BY s.i_time) AS link_seq,
        l.geom
      FROM sched.segments       AS s
      INNER JOIN nw.links       AS l
      ON s.linkid = l.linkid
      ORDER BY s.ttid, link_seq
    ),
    tt_geoms AS (
      SELECT
        ttid,
        ST_LineMerge(
          ST_Union(
            geom ORDER BY link_seq
          )
        ) AS geom
      FROM segment_geoms
      GROUP BY ttid
      ORDER BY ttid
    ),
    add_route_dir AS (
      SELECT
        t.ttid,
        t.route,
        t.dir,
        g.geom
      FROM tt_geoms                   AS g
      INNER JOIN sched.trip_templates AS t
      ON g.ttid = t.ttid
    ),
    route_dir_geoms AS (
      SELECT
        route,
        dir,
        array_agg(ttid ORDER BY ttid)               AS ttids,
        geom
      FROM add_route_dir
      GROUP BY route, dir, geom
      ORDER BY route, dir, ttids
    )
  SELECT *
  FROM route_dir_geoms
);

CREATE INDEX ON sched.trip_template_geoms
USING GIST(geom);

COMMIT;
\timing off
