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

A link has a unique `linkid`, a traversal cost (currently just the link length) and a mode (`bus` or `tram` as enum type).
Note that it would be useful for some links to have an array of modes, if buses and trams use the same road section or lane in the real world, but this is not possible right now.
Links can be one- or two-way: two-way links have `cost = rcost`, one-way links have `rcost = -1`.
And of course, links have `LINESTRING` geometries.

When referring to links, `i` means the start of the link and `j` the end.
Note that two-way links can be referred to as `reversed`: then `i` and `j` are kind of flipped.

The network is established with the help of `pgRouting`, and whatever is done with the network, it should remain routable, i.e. have correct geometry topology and node references.

`nodes` are points that used as link starts and ends.
There should be nodes only at intersections and stops; in future, if we add more attributes to links, such as number of lanes or speed limit, a node should cut the link at a location where these attributes change.
Conversely, every stop and intersection *MUST* have a node that cuts the links; otherwise routing does not work at that location.
In general, we should try not to create too short links: if the link length is just a couple of meters, it does not provide a reliable basis for projecting HFP observations and making analyses.

`stops` refers to part of the `nodes` that work as transit stops.
These nodes are assigned a unique `stopid` and some descriptive attributes such as stop short code and name as in GTFS.
Note that a node can be assigned multiple stops.
This is the case when real stop points are close enough to each other: we want to combine stops closer than e.g. 20 m to each other in order to avoid creating too short links.

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

We do not want to model every possible transit trip as
