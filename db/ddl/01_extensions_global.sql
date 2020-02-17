/*
 * Create extensions and global objects for sujuiko database.
 *
 * Arttu K 2020-02
 */
\c sujuiko;

--CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS postgis CASCADE;
CREATE EXTENSION IF NOT EXISTS pgrouting CASCADE;
CREATE EXTENSION IF NOT EXISTS hstore CASCADE;

/*
 * # Custom types
 *
 * For fields with a limited set of known string values,
 * we use ENUM types instead text,
 * to save some storage space and perhaps make queries faster.
 * These types are defined in default schema `public`
 * and thus do not need explicit schema name prefix when applied.
 */

/*
 * Self-explanatory.
 */
CREATE TYPE public.mode_type AS ENUM('bus', 'tram');

/*
 * HFP event types, in priority order such that the most important one remains
 * if we select distinct records with the same timestamp.
 * See: https://digitransit.fi/en/developers/apis/4-realtime-api/vehicle-positions/#event-types
 */
CREATE TYPE public.event_type AS ENUM(
  'DOO', 'DOC', 'TLR', 'TLA', 'ARS', 'DEP', 'PAS', 'VP',
  'DUE', 'ARR', 'WAIT', 'PDE',
  'DA', 'DOUT', 'BA', 'BOUT', 'VJA', 'VJOUT');
