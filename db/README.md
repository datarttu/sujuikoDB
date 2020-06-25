# `db`

## Creating the data model and functions

First, run the [`ddl`](ddl) scripts to create the database schemas, extensions, functions etc.
Then you should be ready to populate the database with raw GTFS and OSM data, and transform it by running the scripts in this directory.

## Raw data importing

Start from [`scripts`](`../scripts`) in the project root directory.
After this, you should have raw data in `stage_gtfs` and `stage_osm` tables.

## Transforming schedule and network data

We need to take several steps to clean up and transform GTFS and OSM data so they fit the "production" schemas `sched` and `nw`.
These steps are described in the scripts in this directory.
In short:

- *TODO: brief summary of the main steps*

## Importing and transforming HFP data

You should have the raw HFP data in csv files, preferably one file per operating date (`oday`) and route.
A raw data file should look like this - note that a real file is expected NOT to have headers but they are here for clarity:

```
is_ongoing,event_type,dir,oper,veh,tst,lat,long,odo,drst,oday,start,loc,stop,route
t,VJA,2,12,1319,2019-11-01 02:16:13+00,60.168634,24.80453,,t,2019-11-01,04:25:00,GPS,1453214,2550
t,VP,2,12,1319,2019-11-01 02:16:14+00,60.168634,24.80453,0,t,2019-11-01,04:25:00,GPS,1453214,2550
t,DUE,2,12,1319,2019-11-01 02:16:14+00,60.168634,24.80453,0,t,2019-11-01,04:25:00,GPS,2231221,2550
t,VP,2,12,1319,2019-11-01 02:16:15+00,60.168634,24.80453,0,t,2019-11-01,04:25:00,GPS,,2550
t,VP,2,12,1319,2019-11-01 02:16:16+00,60.168634,24.80453,0,t,2019-11-01,04:25:00,GPS,,2550
t,VP,2,12,1319,2019-11-01 02:16:17+00,60.168634,24.80453,0,t,2019-11-01,04:25:00,GPS,,2550
t,VP,2,12,1319,2019-11-01 02:16:18+00,60.168634,24.80453,0,t,2019-11-01,04:25:00,GPS,,2550
t,VP,2,12,1319,2019-11-01 02:16:19+00,60.168634,24.80453,0,t,2019-11-01,04:25:00,GPS,,2550
...
```

*TODO: Each file is imported separately: an import process creates temporary tables and indexes based on `stage_hfp` schema, uses them to transform the raw data, and finally inserts the transformed data to the "production" tables in the `obs` schema.*

In fact, we are only interested in *ongoing `VP` events* that have *valid coordinates*, and we do not use the `stop` attribute for anything, since our own network model should be able to tell if the vehicle is close to a specific stop.

## About the data model

Some functions and types are used globally so they are created in the `public` schema.
Other than that, tables, views and functions for transforming data between them are placed in different schemas.

Why separate "production" and `stage_*` schemas?
The idea is that the production schemas model the data as it is needed for queries and analyses, no matter where the data originally comes from.
The `stage_*` schemas model a particular (current) way to import the data from GTFS and OSM.
Later we might want to import this data from elsewhere, such as JORE, so we can write a new schema for that or do it completely outside the database and just dump the results into the corresponding production schema.

### Notes on the terminology

- `link` describes a road section in the network. pgRouting refers to links as "edges".
- `node` is a point in the network separating links. pgRouting refers to nodes as "vertices"; we prefer "nodes" as link start and end points, and should we use "vertices", they would mean the points that constitute the *geometry* of a link.
- `segment` is an entity that *uses* a link: e.g. scheduled patterns, templates and finally individual trips traverse through links.
Also a set of HFP observations belonging to a `journey` and projected to a single link constitute an "observed segment" that can be used as a source for various analyses.
- `path` is a sequence of segments along the network.
- `trip` refers to a *planned* or *scheduled* individual transit operation departing at a datetime `start_ts` and traversing through transit stops along a network `path`.
- `journey` refers to a *realized* and *observed* version of a trip.
HFP observations with common vehicle, route, direction and start timestamp, ordered by observation timestamps, constitute a `journey`.
- `observation` is a single HFP data point.

### `nw`, `stage_nw`, `stage_osm`

*Network* schema and the staging / import schema for it.
The most important thing is the `nw.links` table that models the bus and tram network in the HSL area.
Links provide the common structure by which we can describe transit service spatially.
The HFP data is projected onto the links so it can be cleaned up, compared and aggregated.

Network geometries are stored in EPSG:3067 (TM35) coordinate system, enabling straightforward metric calculations;
raw data sources use EPSG:4326 (WGS84) coordinates.

A link has a unique `linkid`, a traversal cost (currently just the link length) and a mode (`bus` or `tram` as enum type).
Note that it would be useful for some links to have an array of modes, if buses and trams use the same road section or lane in the real world, but this is not possible right now.
Links can be one- or two-way: two-way links have `cost = rcost`, one-way links have `rcost = -1`.
And of course, links have `LINESTRING` geometries.
Example:

```
 linkid │ inode │ jnode │ mode │       cost       │      rcost       │           geom           
════════╪═══════╪═══════╪══════╪══════════════════╪══════════════════╪══════════════════════════
  10883 │   170 │   171 │ bus  │ 10.7890741492153 │ 10.7890741492153 │ LINESTRING(383618.99 ...
  11113 │   172 │   173 │ bus  │ 9.69395780888292 │               -1 │ LINESTRING(381330.57 ...
  11199 │   174 │   175 │ bus  │ 29.8206753021273 │ 29.8206753021273 │ LINESTRING(393525.86 ...
  11268 │   176 │   177 │ bus  │ 13.6829459229728 │               -1 │ LINESTRING(393728.84 ...
  11344 │   178 │   179 │ bus  │ 7.71657177807143 │               -1 │ LINESTRING(382503.49 ...
```

When referring to links, `i` means the start of the link and `j` the end.
Note that two-way links can be referred to as `reversed`: then `i` and `j` are kind of flipped.

The network is established with the help of `pgRouting`, and whatever is done with the network, it should remain routable, i.e. have correct geometry topology and node references.

`nodes` are points that used as link starts and ends.
There should be nodes only at intersections and stops; in future, if we add more attributes to links, such as number of lanes or speed limit, a node should cut the link at a location where these attributes change.
Conversely, every stop and intersection *MUST* have a node that cuts the links; otherwise routing does not work at that location.
In general, we should try not to create too short links: if the link length is just a couple of meters, it does not provide a reliable basis for projecting HFP observations and making analyses.
Example:

```
 nodeid │           geom           
════════╪══════════════════════════
      1 │ POINT(403700.0668992 ...
      2 │ POINT(403501.9657832 ...
      3 │ POINT(376775.0728413 ...
```

`stops` refers to part of the `nodes` that work as transit stops.
These nodes are assigned a unique `stopid` and some descriptive attributes such as stop short code and name as in GTFS.
Note that a node can be assigned multiple stops.
This is the case when real stop points are close enough to each other: we want to combine stops closer than e.g. 20 m to each other in order to avoid creating too short links.
Example:

```
 stopid  │ nodeid │ mode │ code  │       name        │       descr        │ parent  
═════════╪════════╪══════╪═══════╪═══════════════════╪════════════════════╪═════════
 2214251 │      3 │ bus  │ E2143 │ Urheilupuistontie │ Koivu-Mankkaan tie │    NULL
 1465148 │      5 │ bus  │ 1604  │ Valimo            │ Valimotie 29       │    NULL
 4150261 │    111 │ bus  │ V1561 │ Myyrmäen asema    │ Myyrmäen asema     │ 4000006
 1020123 │    223 │ bus  │ 2051  │ Rautatientori     │ Rautatientori      │ 1000003
```

`stage_osm` stores the raw OSM data import and does some OSM-specific transformations to it, such as marking one- and two-way links in a `pgRouting` compatible way.
It also fixes topology problems, such as links that are not cut at intersections as they should, and roundabouts and end turn loops that are modeled as ring geometries which would be problematic for routing.
(This could be merged to `stage_nw` as well...)

`stage_nw` makes a "contracted" network from the `stage_osm` links (see [pgr_contraction](http://docs.pgrouting.org/latest/en/pgr_contraction.html)), i.e. it eliminates useless nodes and merges successive links between intersections.
It then takes raw stop point locations from `stage_gtfs` and projects them onto the nearest links; at this point, stops too far away (> 20 m) are simply discarded, which later means that transit trips using those stops cannot be used in production (currently this applies to some rarely operated routes so not a big deal).
Stops close to each other are merged into one point location, and links are split at stop locations.
Finally, links, nodes and stops are imported into the `nw` schema.

*NOTE:* currently, if you detect incorrectly located stops, you should fix them in `stage_gtfs.stops_with_mode`, and if you detect network errors such as missing links, you should fix them in `stage_osm.combined_lines` - and then re-run `stage_nw` routines from scratch.

### `sched`, `stage_gtfs`

*Schedule* schema and staging schema for the schedule model.

We do not want to model every possible transit trip individually as it would be quite repetitive.
Instead, we use a hierarchy like this.
It is similar to the Digitransit model.

`route` describes a transit service of a single transit mode `bus` or `tram` known under a single headsign (e.g. `550`).
In addition to the visible headsign, there are HSL-specific prefixes categorizing the routes.
Example:

```
 route │ mode
═══════╪══════
 1001  │ tram
 1001H │ tram
 1002  │ tram
 1014  │ bus
 1015  │ bus
 1016  │ bus
```

Routes can be operated in two directions `1` or `2`, e.g. `2550_1` and `2550_2`.

`pattern` is a realization of a `route` to either of the two directions, consisting of a unique sequence of stops and, as a result, a unique path on the network.
A route + direction can have multiple variants such as morning / evening divertion to / from the depot, or a temporary variant that goes around a building site: that's why we need `patterns`.
Example:

```
    ptid    │ route  │ dir │ total_dist │   gtfs_shape_id   
════════════╪════════╪═════╪════════════╪═══════════════════
 1001_1_1   │ 1001   │   1 │    9914.73 │ 1001_20190930_1
 1002 3_1_1 │ 1002 3 │   1 │    2791.02 │ 1002 3_20191021_1
 1087N_2_1  │ 1087N  │   2 │      10836 │ 1087N_20190617_2
 2134N_2_1  │ 2134N  │   2 │    23000.3 │ 2134N_20180103_2
 1093 3_2_1 │ 1093 3 │   2 │    1511.13 │ 1093 3_20120813_2
```

`segment` belongs to a `pattern` and refers to a single link that the pattern traverses through.
I.e., a pattern consists of an ordered set of segments.
If a segment is `reversed`, the link is traversed opposite to the link geometry direction.
`ij_stops` array tells if the start and / or the end of the segment is used as a stop by the pattern (the pattern may traverse through nodes that are used as stops in general but not necessarily by that pattern).
`ij_dist_span` describes the cumulative distance values that the segment covers from its pattern.
Example:

```
   ptid    │ segno │ linkid │ reversed │     ij_stops      │            ij_dist_span             │ stop_seq
═══════════╪═══════╪════════╪══════════╪═══════════════════╪═════════════════════════════════════╪══════════
 1001H_1_1 │     1 │   2188 │ f        │ {1050417,1050416} │ [0,253.025810206265)                │        1
 1001H_1_1 │     2 │   2185 │ f        │ {1050416,NULL}    │ [253.025810206265,505.110008993299) │        2
 1001H_1_1 │     3 │   8539 │ f        │ {NULL,1060404}    │ [505.110008993299,558.663084326526) │        2
 1001H_1_1 │     4 │   1084 │ f        │ {1060404,1050408} │ [558.663084326526,908.421927705316) │        3
 1001H_1_1 │     5 │   7166 │ f        │ {1050408,1050413} │ [908.421927705316,1126.90957855036) │        4
```

`template` is a variant of `pattern` that has a unique total operating time and / or set of stop times at segments.
Example:

```
    ttid     │   ptid    
═════════════╪═══════════
 1001H_1_1_1 │ 1001H_1_1
 1001H_1_1_2 │ 1001H_1_1
 1001H_1_1_3 │ 1001H_1_1
 1001H_1_1_4 │ 1001H_1_1
 1001H_2_1_1 │ 1001H_2_1
```

`segment_times` describe the operating time through `segments` specific to a `template`.
These are NOT absolute times but time differences relative to the trip start time, and they are modeled as `interval` type.
This way you can just take the start timestamp, add the interval field to it and have absolute traversal times.
`ij_times` are "reliable" only if the segment start / end time is a *timepoint* time, i.e. time equalization point in the original schedule;
other time values are interpolated along the network path, based on the segment link length.
Example:

```
    ttid     │ segno │             ij_times              │ ij_timepoints
═════════════╪═══════╪═══════════════════════════════════╪═══════════════
 1001H_1_1_1 │     1 │ {00:00:00,00:00:59.978235}        │ {t,f}
 1001H_1_1_1 │     2 │ {00:00:59.978235,00:01:59.733266} │ {f,f}
 1001H_1_1_1 │     3 │ {00:01:59.733266,00:02:12.427698} │ {f,f}
 1001H_1_1_1 │     4 │ {00:02:12.427698,00:03:35.335912} │ {f,f}
 1001H_1_1_1 │     5 │ {00:03:35.335912,00:04:27.127085} │ {f,f}
```

`template_timestamps` describe the absolute start datetime values of individual trips belonging to `templates`.
By using this together with the `sched` model hierarchy, you can construct individual scheduled trips on the network.
Example:

```
    ttid     │        start_ts        
═════════════╪════════════════════════
 1001H_1_1_1 │ 2019-10-31 19:54:00+00
 1001H_1_1_1 │ 2019-11-01 19:54:00+00
 1001H_1_1_1 │ 2019-11-03 19:10:00+00
 1001H_1_1_1 │ 2019-11-03 19:22:00+00
 1001H_1_1_1 │ 2019-11-03 19:34:00+00
```
