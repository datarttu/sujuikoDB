# Known issues

## Issues due to source data

These issues are particularly related to HSL-GTFS, OSM in HSL area or HFP.
They represent the data as of November 2019.

### Missing OSM bus relations

Unlike tram network that is based on all `railway=tram` links in the area, bus network is currently based on OSM ways that are part of at least one bus route relation.
This makes the number of ways to handle significantly smaller than if we extracted all the ways that buses could *possibly* use.
However, by doing this we basically assume that OSM users have modeled all the bus routes that we have in GTFS as well, but this is not true.
Some routes are outdated, and some routes such as minor local service routes are not found in OSM at all.

See `scripts/overpass_get_network_used_by_busroutes.sh` for more details on bus network extraction from OSM.

Below is an example from Munkkiniemi.

![Example of an area with clear GTFS stops but no bus links.](img/missing_osm_bus_relations_example.png)

### Inaccurate GTFS stop locations

Some GTFS stop points are modeled inaccurately, which may cause stops to be snapped to a wrong link.
This particularly applies to tram network where opposite directions are modeled as separate oneway links.

Below is an example from Huutokonttori, Jätkäsaari.

![Example of stop points that will snap to wrong links.](img/inaccurate_gtfs_stop_locations_example.png)

For tram stops, this issue can be examined with `db/patches/find_missnapped_tram_stops.sql` and fixed manually with `db/patches/move_misplaced_stops.sql`.
Bus stops have not yet been reviewed but fixes on them can be appended to the same patch file.

### Invalid OSM topology

Some OSM ways do not connect to each other as they should do and would seem to.
Instead of an end-to-end connection, they may touch each other, but this is not enough to form a working routing topology.

Example from Arabia.

![Example of OSM ways without proper end-to-end connection.](img/invalid_osm_topology_example.png)

This could be fixed by manually splitting the most critical spots from `stage_osm.combined_lines` or by automating the splitting process e.g. by using `ST_Touches()`, which would leave out any direct intersections of lines (e.g. bridges).
However, the latter would probably involve some nontrivial problems such as bus links following partly the same geometries on top of tram links, still without meaning that the links should be split.

### Topology tolerance value too high

There are OSM nodes that are closer to each other than the tolerance used in `pgr_createTopology`.
As of writing this, we have used a tolerance of `1.0 meters`.
This is apparently too high (example from Mannerheimintie-Nordenskiöldinkatu):

![Example of tolerance value too high.](img/raw_vertices_below_tolerance.png)

In this case, the node above `5950` is not included in the network topology, and pgRouting thinks that the merging tram tracks go both via `5950` and `6503`.
A smaller tolerance value should be used, though it should not be too low.

*2020-05-15: topology tolerance has now been set to 0.01, i.e. 1 cm, for `stage_nw.raw_nw` and `stage_nw.contracted_nw`.
nw.links already had that tolerance.*

### Incomplete GTFS route identifiers

See `scripts/import_gtfs.sql` notes:

*HSL has some bus and tram route variants indicated by a trailing whitespace and a digit, e.g. "1001" -> variant "1001 3" but these seem not to have propagated correctly to the GTFS dataset:
instead, the trailing whitespace+digit are left out from the route_id, but they are still visible in the beginning of the trip_id (all HSL trip_ids start with the route identifier).*

This issue is fixed in the aforementioned import script.

### Service dates and > 24 hrs trip start times

*NOTE: This may be either due to the data model or errors in HSL GTFS generation.*

Some night trips seem not to get any correct dates when made into `stage_gtfs.trips_with_dates`.
They seem to have initial departure times > 24 hrs (with GTFS 30-hour clock).
The problem is that `start_date` and `end_date` do not conform with the weekday definition in GTFS:
this is at least the case at the start of the GTFS dataset validity time.

For example, see trips with `service_id = '1020N_20191031_20191031_Ke'`.
