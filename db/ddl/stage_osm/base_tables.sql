CREATE SCHEMA IF NOT EXISTS stage_osm;

CREATE TABLE stage_osm.raw_bus_lines (
  fid                          serial    PRIMARY KEY,
  osm_id                       varchar,  -- Unique, will cast to int
  oneway                       varchar,  -- Only values 'yes', 'no', null
  highway                      varchar,  -- Text values
  lanes                        varchar,  -- Will cast to int
  junction                     varchar,  -- Text values
  geom                         geometry(LINESTRING, 4326)
);
CREATE INDEX ON stage_osm.raw_bus_lines USING GIST (geom);

CREATE TABLE stage_osm.raw_tram_lines (
  fid                          serial    PRIMARY KEY,
  osm_id                       varchar,  -- Unique, will cast to int
  tram_segregation_physical    varchar,  -- Text values
  geom                         geometry(LINESTRING, 4326)
);
CREATE INDEX ON stage_osm.raw_tram_lines USING GIST (geom);
