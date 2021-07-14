-- WIP
-- TODO: Allow interpolating over one or several successive links on route without points on link
--       (tst is NULL etc.).

WITH
  edge_points AS (
    SELECT
      jrnid,
      link_seq,
      min(tst)                      AS first_tst,
      first(location_on_link, tst)  AS first_rel_loc,
      max(tst)                      AS last_tst,
      last(location_on_link, tst)   AS last_rel_loc
    FROM obs.point_on_link
    GROUP BY jrnid, link_seq
  ),
  complete_route_links AS (
    SELECT
      jrn.jrnid,
      lor.link_seq,
      vld.length_m,
      sum(vld.length_m) OVER w_link - vld.length_m  AS cumul_length_m,
      ep.first_tst,
      ep.first_rel_loc * vld.length_m AS first_loc_m,
      ep.last_tst,
      ep.last_rel_loc * vld.length_m  AS last_loc_m
    FROM nw.link_on_route             AS lor
    INNER JOIN nw.view_link_directed  AS vld
      ON (lor.link_id = vld.link_id AND lor.link_reversed = vld.link_reversed)
    INNER JOIN obs.journey            AS jrn
      ON (lor.route_ver_id = jrn.route_ver_id)
    LEFT JOIN edge_points             AS ep
      ON (jrn.jrnid = ep.jrnid AND lor.link_seq = ep.link_seq)
    WHERE jrn.jrnid = 'cd0cbca5-faf6-80d8-909e-06b720552f9b' -- FIXME: REMOVE !!!
    WINDOW w_link AS (PARTITION BY jrn.jrnid ORDER BY lor.link_seq)
  ),
  interpolation_parameters AS (
    SELECT
      jrnid,
      link_seq,
      lag(last_tst) OVER w_link                     AS t0,
      first_tst                                     AS t1,
      lag(cumul_length_m + last_loc_m) OVER w_link  AS x0,
      cumul_length_m                                AS x,
      cumul_length_m + first_loc_m                  AS x1,
      last_tst
    FROM complete_route_links
    WINDOW w_link AS (PARTITION BY jrnid ORDER BY link_seq)
  )
SELECT
  jrnid,
  link_seq,
  t0 AS prev_last_tst,
  t0 + ( (x - x0) * (extract(epoch FROM t1 - t0) / (x1 - x0)) * interval '1 second') AS enter_tst,
  t1 AS first_tst
FROM interpolation_parameters
ORDER BY jrnid, link_seq;
