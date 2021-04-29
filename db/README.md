# Database model

The sujuiko database uses two schemas:

- `nw`, as in "network", models the transit network and transit routes on it, as well as parts of the network selected for analyses.
- `obs` _(not yet implemented)_, as in "observations", models actual transit vehicle movements on the network as well as various aggregates and analysis results based on the movements.

Extensions and objects not specific to an individual schema, such as generic helper functions, are created in the default `public` schema.

## General notes

The database uses some [extensions](./010_global.sql):

- `btree_gist`, provided by PostgreSQL itself, allows including `=` equality operator to `EXCLUDE` constraints that would normally only work with range operators.
- [`postgis`](https://postgis.net/docs/manual-3.0/) provides geometry types and various geoprocessing functions, we use it especially for "project point to line"-type calculations.
- [`pgrouting`](https://docs.pgrouting.org/latest/en/) helps us find shortest paths to model routes and analysis sections on the network.
- [`timescaledb`](https://docs.timescale.com/latest/main) automates partitioning of large HFP observation tables by timestamp columns and provides some nice time-series functions.
TimescaleDB affects the database instance at a bit deeper level, and it is recommended by the provider to [optimize](./001_timescaledb_tune.sh) the instance performance.

All the geometry columns use **SRID 3067 (ETRS-TM35FIN)**.
This is a metric coordinate system, meaning it is more straightforward with geospatial functions such as `ST_Distance()`, and it works pretty accurately in the HSL / Greater Helsinki area.
This SRID is hard-coded to the DDL and should be changed if the model is applied to a different area, but this is not likely to happen at the moment.

There are some logical rules and assumptions in the data model that are not strictly modeled and enforced by foreign keys, for example, but are still required for consistent results.
For example, links belonging to a route version should form a continuous path without gaps, but saving data that is against this rule is still allowed to provide flexibility in modifying the data in the database.
Most relations have an array field `errors` to indicate such cases where rules or assumptions are not fulfilled.
_TODO: The user must run validation procedures to validate the data and to update these fields._

## `nw` network model

![ER diagram of the network tables](../docs/img/db_relations_er.png)

### Relations and views

#### `nw.node`

Points where links start or end.

`nw.view_node_wkt` works as a data insert API that allows copying CSV files with WKT geometries (through the `INSTEAD OF INSERT` trigger).
See [node example data](../example_data/node.csv).

#### `nw.link`

Connected parts of street or tram network where buses and/or trams can drive.
Links must have a start (i) and end (j) node located in the start and end points of the link geometry.

Links can be two-way or oneway (traversing allowed only from i to j).

A link can allow multiple modes, if in reality buses and trams use the same lane and street space, for example.
This is why `link_modes` is an array rather than a single value.

`nw.view_link_directed` view returns oneway versions of all links.
Two-way links are duplicated into original and reversed versions of the geometries and i/j node orders.
Oneway links are included as such.
A (non-persistent) `uniq_link_id` is given to distinct between the oneway versions of the links.
This view helps creating route and section paths, where the traversal direction of a link must be known exactly (by `link_dir` attribute value).

`nw.view_link_wkt` works as a data insert API that allows copying CSV files with WKT geometries (through the `INSTEAD OF INSERT` trigger).
See [link example data](../example_data/link.csv).

## `obs` observations model

_TODO_
