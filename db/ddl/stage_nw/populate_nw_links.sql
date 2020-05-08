CREATE OR REPLACE FUNCTION stage_nw.populate_nw_links()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE
  cnt   integer;
BEGIN
  DELETE FROM nw.links;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows deleted from nw.links', cnt;

  WITH
    /*
     * The stops table we use has already a value
     * that tells us how far away the stop point is located from the edge start
     * (along the edge). We use this to split the edges at stop locations.
     */
    distances_ordered AS (
      SELECT DISTINCT ON (edges.id, stops.edge_start_dist)
        edges.id              AS edge,
        edges.geom            AS geom,
        ST_Length(edges.geom) AS len,
        edges.mode            AS mode,
        edges.oneway          AS oneway,
        stops.edge_start_dist AS dist
      FROM stage_nw.contracted_nw       AS edges
      INNER JOIN stage_nw.snapped_stops AS stops
        ON edges.id = stops.edgeid
      WHERE stops.edge_start_dist > 0
        AND stops.edge_start_dist < ST_Length(edges.geom)
      ORDER BY stops.edge_start_dist ASC
    ),
    /*
     * An UNION is required since the first data set here will not include
     * the last part of the split edge;
     * we construct the last parts separately.
     * GROUP BY is required because we have snapped stops close to each other
     * to the same location, so without grouping, this would result in
     * same split edge occurring multiple times.
     */
    splits AS (
      SELECT
        edge,
        mode,
        oneway,
        geom,
        coalesce(
          lag(dist) OVER (PARTITION BY edge ORDER BY dist),
          0) / len  AS start_frac,
        dist / len  AS end_frac
      FROM distances_ordered
      UNION
      SELECT
        edge,
        min(mode)   AS mode,
        min(oneway) AS oneway,
        geom,
        max(dist) / max(len) AS start_frac,
        1 AS end_frac
      FROM distances_ordered
      GROUP BY edge, geom
    ),
    /*
     * Note that the above only included those edges that are somehow related
     * to one or more stops. We want to include the rest of the edges too,
     * i.e. the not split ones.
     */
    combined AS (
    SELECT
      edge,
      mode,
      oneway,
      /*
       * Store these distance fraction values later
       * if needed for diagnostics or debugging.
       * For now, we do not save them for production.
      start_frac,
      end_frac,
       */
      ST_LineSubstring(geom, start_frac, end_frac) AS geom
    FROM splits
    UNION
    SELECT
      edges.id      AS edge,
      edges.mode    AS mode,
      edges.oneway  AS oneway,
      /*
      0::real       AS start_frac,
      1::real       AS end_frac,
      */
      edges.geom    AS geom
    FROM stage_nw.contracted_nw   AS edges
    WHERE edges.id NOT IN
      (
        SELECT DISTINCT edge
        FROM splits
      )
    )
  /*
   * At this point, we lose the previous edge id information (for now at least)
   * and use a running link id instead.
   * However, split links with the same original edge id should have
   * consecutive ids.
   * Moreover, we move from the 'B-FT' oneway marking system, used by the
   * contraction algorithm, to cost-rcost system used by routing algorithms.
   */
  INSERT INTO nw.links (linkid, mode, cost, rcost, geom, wgs_geom)
  (
    SELECT
      row_number() OVER (ORDER BY edge) AS linkid,
      mode,
      ST_Length(geom)                   AS cost,
      CASE
        WHEN oneway = 'B' THEN ST_Length(geom)
        ELSE -1
      END                               AS rcost,
      geom,
      ST_Transform(geom, 4326)          AS wgs_geom
    FROM combined
  );
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '% rows inserted into nw.links', cnt;
  RETURN 'OK';
END;
$$;
COMMENT ON FUNCTION stage_nw.populate_nw_links IS
'Split contracted network edges by stop locations,
and insert the resulting network edges to nw.links table.
nw.links will be emptied first.
Requires populated stage_nw.contracted_nw
and stage_nw.snapped_stops tables.';
