CREATE OR REPLACE FUNCTION stage_nw.build_contracted_network()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE forbidden integer[];
BEGIN
  RAISE NOTICE 'Checking restricted vertices ...';
  EXECUTE '
  SELECT array_agg(id)
  FROM stage_nw.raw_nw_vertices_pgr
  WHERE NOT (
    (owein = 1 and oweout = 1 and twein = 0 and tweout = 0)
    OR
    (owein = 0 and oweout = 0 and twein = 1 and tweout = 1)
    OR
    (owein = 0 and oweout = 0 and twein = 2 and tweout = 0)
    OR
    (owein = 0 and oweout = 0 and twein = 0 and tweout = 2)
  );'
  INTO forbidden;

  RAISE NOTICE 'Building contracted network from stage_nw.raw_nw ...';

  RAISE NOTICE '  Creating table for contracted vertices arrays ...';
  DROP TABLE IF EXISTS stage_nw.contracted_arr;
  CREATE TABLE stage_nw.contracted_arr AS (
    SELECT id, contracted_vertices, source, target, cost
    FROM pgr_contraction(
      'SELECT id, source, target, cost, reverse_cost
       FROM stage_nw.raw_nw',
       ARRAY[2], -- _linear_ contraction (2), as opposed to dead-end (1)
       max_cycles := 1,
       forbidden_vertices := forbidden,
       directed := true
    )
  );

  RAISE NOTICE '  Creating table for contracted edges to merge ...';
  DROP TABLE IF EXISTS stage_nw.contracted_edges_to_merge;
  CREATE TABLE stage_nw.contracted_edges_to_merge AS (
    WITH
      /*
       * Two-way links are represented twice in contraction arrays,
       * therefore we only pick distinct ones.
       */
      distinct_contraction_arrays AS (
        SELECT DISTINCT ON (contracted_vertices) *
        FROM stage_nw.contracted_arr
      ),
      /*
       * Open up intermediate vertices ("via points") to rows.
       */
      unnested AS (
        SELECT
          id AS grp,
          source::bigint,
          target::bigint,
          unnest(contracted_vertices)::bigint AS vertex
        FROM distinct_contraction_arrays
      ),
      /*
       * From possible combos of source-intermediate, intermediate-intermediate
       * and intermediate-end vertice ids, we find the ones that actually
       * have a matching link on the network, and then we can assign a
       * contraction group id to them.
       * We do this because the contraction algorithm did not output the
       * vertex array in the same order they appear on the links to merge.
       */
      vertex_pair_candidates AS (
        SELECT grp, source AS source, vertex AS target
        FROM unnested
        UNION
        SELECT grp, vertex AS source, target AS target
        FROM unnested
        UNION
        SELECT grp, target AS source, vertex AS target
        FROM unnested
        UNION
        SELECT grp, vertex AS source, source AS target
        FROM unnested
        UNION
        SELECT u1.grp AS grp, u1.vertex AS source, u2.vertex AS target
        FROM unnested       AS u1
        INNER JOIN unnested AS u2
          ON (u1.grp = u2.grp AND u1.vertex <> u2.vertex)
      )
    SELECT n.id, vpc.grp, n.source, n.target
    FROM stage_nw.raw_nw AS n
    INNER JOIN vertex_pair_candidates AS vpc
    ON n.source = vpc.source AND n.target = vpc.target
    ORDER BY vpc.grp, n.id
  );

  RAISE NOTICE '  Creating contracted network edge table ...';
  DROP TABLE IF EXISTS stage_nw.contracted_nw;
  CREATE TABLE stage_nw.contracted_nw AS (
    WITH
      all_edges_before_merging AS (
        SELECT
          raw.id,
          raw.source,
          raw.target,
          raw.oneway,
          raw.mode,
          coalesce(ctr.grp, raw.id) AS merge_group,
          raw.geom
        FROM stage_nw.raw_nw AS raw
        LEFT JOIN stage_nw.contracted_edges_to_merge AS ctr
        ON raw.source = ctr.source AND raw.target = ctr.target
      )
    SELECT
      merge_group::bigint             AS id,
      NULL::bigint                    AS source,
      NULL::bigint                    AS target,
      min(oneway)                     AS oneway,
      min(mode)                       AS mode,
      ST_LineMerge(ST_Collect(geom))  AS geom,
      false                           AS is_contracted
    FROM all_edges_before_merging
    GROUP BY merge_group
  );

  RAISE NOTICE '  Updating contracted network edge ids ...';
  /*
   * Replace negative ids produced by the contraction routine.
   * We rely on the fact that the least id used by OSM-based edges
   * is 1,000,000, and there are just thousands of contracted edges,
   * so we just flip the negative id sign.
   */
  UPDATE stage_nw.contracted_nw
  SET
    id = abs(id),
    is_contracted = true
  WHERE id < 0;

  RAISE NOTICE '  Adding primary key on contracted network ...';
  ALTER TABLE stage_nw.contracted_nw
  ADD PRIMARY KEY (id);

  RAISE NOTICE '  Creating pgr topology on contracted network ...';
  PERFORM pgr_createTopology(
    'stage_nw.contracted_nw',
    0.01,
    the_geom := 'geom',
    id := 'id',
    source := 'source',
    target := 'target',
    rows_where := 'true',
    clean := true
  );

  RETURN 'OK';

END;
$$;
COMMENT ON FUNCTION stage_nw.build_contracted_network IS
'Creates a new network from stage_nw.raw_nw where linear edges
(i.e., continuous one- or two-way edge groups between intersections)
are merged, resulting in fewer edges in total.
Detecting vertices restricted from contraction
requires that stage_nw.analyze_inout_edges() is run first.
Creates new network tables stage_nw.contracted_nw
and stage_nw.contracted_nw_vertices_pgr.';
