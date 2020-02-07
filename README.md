# Sujuiko database

WIP

## Postgres schemata

- `stage_*` schemata are meant for intermediate source data handling and transformation, i.e., staging.
  They should not hold any data that is used in services, and you should be able to empty the tables any time.
  Currently it can take raw data from OpenStreetMap, GTFS and HFP.
- `obs` holds data from *observed journeys*.
  This data is primarily based on HFP observations, but it is made compatible with the network and GTFS data in the staging phase.
- `sched` holds data from *planned operations*.
  The data is basically GTFS data transformed to a more usable
  format for us and referenced to the network segments.
- `nw` holds the routable transit network model.

We assume that these schema names are NOT added to the `search_path`, i.e., tables or other objects under a schema must always be prefixed with the schema name.
This is for clarity and to enable using colliding object names within different schemas.
