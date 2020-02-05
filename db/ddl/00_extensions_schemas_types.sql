/*
Create extensions, schemas and custom types for sujuiko database.

Arttu K 2020-02
*/
\c sujuiko;

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS postgis CASCADE;
CREATE EXTENSION IF NOT EXISTS pgrouting CASCADE;
CREATE EXTENSION IF NOT EXISTS hstore CASCADE;

/*
# Postgres schemas

- `stage_*` schemas are meant for intermediate
  source data handling and transformation, i.e., staging.
  They should not hold any data that is used in services,
  and you should be able to empty the tables any time.
  Currently it can take raw data from OpenStreetMap, GTFS and HFP.
- `obs` holds data from *observed journeys*.
  This data is primarily based on HFP observations,
  but it is made compatible with the network and GTFS data
  in the staging phase.
- `sched` holds data from *planned operations*.
  The data is basically GTFS data transformed to a more usable
  format for us and referenced to the network segments.
- `nw` holds the routable transit network model.

We assume that the following schema names are NOT added to the search path,
i.e., tables or other objects under a schema must always be
prefixed with the schema name.
This is for clarity and to enable using colliding object names
within different schemas.
*/
CREATE SCHEMA IF NOT EXISTS stage_osm;
CREATE SCHEMA IF NOT EXISTS stage_gtfs;
CREATE SCHEMA IF NOT EXISTS stage_hfp;
CREATE SCHEMA IF NOT EXISTS obs;
CREATE SCHEMA IF NOT EXISTS sched;
CREATE SCHEMA IF NOT EXISTS nw;

/*
# Custom types

For fields with a limited set of known string values,
we use ENUM types instead text,
to save some storage space and perhaps make queries faster.
These types are defined in default schema `public`
and thus do not need explicit schema name prefix when applied.
*/ /* Self-explanatory. */
CREATE TYPE public.mode_type AS ENUM('bus', 'tram');

/*
"timing" refers to the quality of a timestamp at a node or on a link:

- `strict`:   strictly scheduled dep / arr timepoint at a stop in schedule
- `approx`:   estimated dep / arr time at a non-timepoint stop in schedule
- `observ`:   segment's enter and exit times are based on real observations on that segment
- `interp`:   segment's enter and exit times are interpolated from adjacent segments,
              no observations available on that segment.

**TODO:** Should we split this to two different types instead?
*/
CREATE TYPE public.timing_type AS ENUM('strict', 'approx', 'observ', 'interp');

/*
HFP event types, in priority order such that the most important one remains
if we select distinct records with the same timestamp.
See: https://digitransit.fi/en/developers/apis/4-realtime-api/vehicle-positions/#event-types
*/
CREATE TYPE public.event_type AS ENUM(
  'DOO', 'DOC', 'TLR', 'TLA', 'ARS', 'DEP', 'PAS', 'VP',
  'DUE', 'ARR', 'WAIT', 'PDE',
  'DA', 'DOUT', 'BA', 'BOUT', 'VJA', 'VJOUT');
