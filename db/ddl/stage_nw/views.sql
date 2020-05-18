CREATE OR REPLACE VIEW stage_nw.view_trip_template_segments AS (
  WITH
    unnested AS (
      SELECT
        ttid,
        route_id                AS route,
        direction_id + 1        AS dir,
        unnest(stop_ids)        AS stop_id,
        unnest(stop_sequences)  AS stop_seq,
        route_found
      FROM stage_gtfs.trip_template_arrays
    )
  SELECT
    un.*,
    tr.path_seq,
    li.linkid,
    li.geom
  FROM unnested                           AS un
  LEFT JOIN stage_nw.trip_template_routes AS tr
    ON un.ttid = tr.ttid AND un.stop_seq = tr.stop_seq
  LEFT JOIN nw.links                      AS li
    ON tr.edge = li.linkid
);
