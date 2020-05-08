CREATE TABLE stage_nw.raw_nw (
  id            bigint                      PRIMARY KEY,
  source        bigint                          NULL,
  target        bigint                          NULL,
  cost          double precision            DEFAULT -1,
  reverse_cost  double precision            DEFAULT -1,
  oneway        text                        NOT NULL,
  mode          public.mode_type            NOT NULL,
  geom          geometry(LINESTRING, 3067)  NOT NULL
);

CREATE INDEX raw_nw_geom_idx
  ON stage_nw.raw_nw
  USING GIST (geom);
CREATE INDEX raw_nw_source_idx
  ON stage_nw.raw_nw (source);
CREATE INDEX raw_nw_target_idx
  ON stage_nw.raw_nw (target);

CREATE OR REPLACE FUNCTION stage_nw.populate_raw_nw()
RETURNS TABLE (
  by_mode         public.mode_type,
  rows_inserted   bigint
)
LANGUAGE PLPGSQL
VOLATILE
AS $$
BEGIN
  IF EXISTS (SELECT * FROM stage_nw.raw_nw LIMIT 1)
  THEN
    RAISE EXCEPTION 'Table stage_nw.raw_nw is not empty!'
    USING HINT = 'Truncate the table first.';
  END IF;

  RAISE NOTICE 'Populating stage_nw.raw_nw ...';
  INSERT INTO stage_nw.raw_nw (
    id, cost, reverse_cost, oneway, mode, geom
  )
  SELECT
    c.osm_id AS id,
    ST_Length(c.geom) AS cost,
    CASE
      WHEN c.oneway LIKE 'FT' THEN -1
      ELSE ST_Length(c.geom)
      END AS reverse_cost,
    c.oneway,
    c.mode,
    c.geom
  FROM stage_osm.combined_lines AS c;

  RAISE NOTICE 'Creating pgr topology on stage_nw.raw_nw ...';
  -- NOTE: tolerance in meters! (EPSG 3067)
  PERFORM pgr_createTopology(
    'stage_nw.raw_nw',
    1.0,
    the_geom := 'geom',
    id := 'id',
    source := 'source',
    target := 'target',
    rows_where := 'true',
    clean := true
  );

  RETURN QUERY
  SELECT
    mode AS by_mode,
    count(mode) AS rows_inserted
  FROM stage_nw.raw_nw
  GROUP BY mode;
END;
$$;
COMMENT ON FUNCTION stage_nw.populate_raw_nw IS
'Read line geometries from stage_osm.combined_lines
into stage_nw.raw_nw and create pgr routing topology.';

CREATE OR REPLACE FUNCTION stage_nw.analyze_inout_edges()
RETURNS TEXT
LANGUAGE PLPGSQL
VOLATILE
AS $$
DECLARE cnt integer;
BEGIN
  RAISE NOTICE 'Counting incoming and outgoing oneway edges ...';

  ALTER TABLE stage_nw.raw_nw_vertices_pgr
    ADD COLUMN IF NOT EXISTS owein integer DEFAULT 0;
  UPDATE stage_nw.raw_nw_vertices_pgr AS upd
    SET owein = results.cnt
    FROM (
      SELECT target AS id, count(target) AS cnt
      FROM stage_nw.raw_nw
      WHERE oneway = 'FT'
      GROUP BY target
    ) AS results
    WHERE upd.id = results.id;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '"owein" set for % rows', cnt;

  ALTER TABLE stage_nw.raw_nw_vertices_pgr
    ADD COLUMN IF NOT EXISTS oweout integer DEFAULT 0;
  UPDATE stage_nw.raw_nw_vertices_pgr AS upd
    SET oweout = results.cnt
    FROM (
      SELECT source AS id, count(source) AS cnt
      FROM stage_nw.raw_nw
      WHERE oneway = 'FT'
      GROUP BY source
    ) AS results
    WHERE upd.id = results.id;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '"oweout" set for % rows', cnt;

  RAISE NOTICE 'Counting incoming and outgoing two-way edges ...';

  ALTER TABLE stage_nw.raw_nw_vertices_pgr
    ADD COLUMN IF NOT EXISTS twein integer DEFAULT 0;
  UPDATE stage_nw.raw_nw_vertices_pgr AS upd
    SET twein = results.cnt
    FROM (
      SELECT target AS id, count(target) AS cnt
      FROM stage_nw.raw_nw
      WHERE oneway = 'B'
      GROUP BY target
    ) AS results
    WHERE upd.id = results.id;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '"twein" set for % rows', cnt;

  ALTER TABLE stage_nw.raw_nw_vertices_pgr
    ADD COLUMN IF NOT EXISTS tweout integer DEFAULT 0;
  UPDATE stage_nw.raw_nw_vertices_pgr AS upd
    SET tweout = results.cnt
    FROM (
      SELECT source AS id, count(source) AS cnt
      FROM stage_nw.raw_nw
      WHERE oneway = 'B'
      GROUP BY source
    ) AS results
    WHERE upd.id = results.id;
  GET DIAGNOSTICS cnt = ROW_COUNT;
  RAISE NOTICE '"tweout" set for % rows', cnt;

  RETURN 'OK';
END;
$$;
COMMENT ON FUNCTION stage_nw.analyze_inout_edges IS
'For each vertex in stage_nw.raw_nw_vertices_pgr,
calculate number of incoming and outgoing oneway
and two-way edges.
Adds integer columns "owein", "oweout", "twein" and "tweout".';
