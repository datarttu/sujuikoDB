# Importing and handling raw HFP data

HFP data is imported to the database *one operating day* at a time:
there should only appear one distinct `oday` value in the dataset
that is taken from the filesystem, mangled through `stage_hfp` schema
and finally imported into `obs` schema.
All the tables in `stage_hfp` should be emptied thereafter,
before the next `oday`.

## Requirements for data files

Data must be available in `.csv` files separated by comma,
preferably in the database server readable by `postgres` user,
to avoid latency from client to the server.

*Note: in the server we're currently using, the data is available in*
`/data0/hfpdumps/november/`.

Example of the required csv file structure - note that the real files should NOT have a header but it is here for clarity:

```
is_ongoing,event_type,dir,oper,veh,tst,lat,long,odo,drst,oday,start,loc,stop,route
t,VJA,2,55,1252,2019-11-02 07:14:22+00,60.230585,25.043217,,t,2019-11-02,09:20:00,GPS,1362151,1078
t,VP,2,55,1252,2019-11-02 07:14:23+00,60.230585,25.043217,,t,2019-11-02,09:20:00,GPS,1362151,1078
t,DUE,2,55,1252,2019-11-02 07:14:23+00,60.230585,25.043217,,t,2019-11-02,09:20:00,GPS,1362151,1078
t,ARR,2,55,1252,2019-11-02 07:14:23+00,60.230585,25.043217,0,t,2019-11-02,09:20:00,GPS,1362151,1078
t,ARS,2,55,1252,2019-11-02 07:14:23+00,60.230585,25.043217,0,t,2019-11-02,09:20:00,GPS,1362151,1078
t,VP,2,55,1252,2019-11-02 07:14:24+00,60.230585,25.043217,0,t,2019-11-02,09:20:00,GPS,1362151,1078
t,VP,2,55,1252,2019-11-02 07:14:25+00,60.230585,25.043217,0,t,2019-11-02,09:20:00,GPS,1362151,1078
t,VP,2,55,1252,2019-11-02 07:14:26+00,60.230585,25.043216,0,t,2019-11-02,09:20:00,GPS,1362151,1078
t,VP,2,55,1252,2019-11-02 07:14:27+00,60.230585,25.043216,0,t,2019-11-02,09:20:00,GPS,1362151,1078
t,VP,2,55,1252,2019-11-02 07:14:28+00,60.230585,25.043216,0,t,2019-11-02,09:20:00,GPS,1362151,1078
```

There must not be empty `tst` values in the data files.
`stage_hfp.raw` has a `NOT NULL` constraint on `tst`, because this table is
partitioned into Timescale hypertable along `tst`.
We don't even have any use for observations without a timestamp.
You should get rid of such rows already outside the database.

## Data flow

### Importing from csv files

`stage_hfp.raw` receives the data by `COPY FROM` command(s).
Right away, a `BEFORE INSERT` trigger fills some additional fields:

- `geom` is the point geometry in metric TM35 coordinates, making various spatial processes much simpler than raw lon/lat or WGS84 points.
- `start_ts` is the same as `oday + start` but it's also timezone-aware: we assume here that the original values are based on `Europe/Helsinki`.
This way we get to treat the point timestamps and initial departure timestamps the same way.
- `jrnid` describes a unique *journey* taken by a *vehicle*, and it will be used as a surrogate key for identifying journeys and observations belonging to them.
This way we do not always have to do joins using `(start_ts, route, dir, oper, veh)`.
Note that regarding the raw data, finding unique journeys without the vehicle information would not be enough: multiple vehicle drivers may have signed in on the same trip in the schedule system, and later we have to choose which of these we consider valid.

### Splitting into journey- and point-specific tables

`stage_hfp.journeys` gets a row for each `jrnid` in the raw data,
along with the following journey-specific values:
operating day, journey start time, route, direction, vehicle operator and vehicle id.
None of these may be `NULL`.

At the same time, values that vary within each `jrnid` are transferred into `stage_hfp.points`:
ongoing status, event type, timestamp, coordinates, door status.
*NOTE: we do not save odometer value, location source or stop id for now.*

Note that these have to be done inside the same transaction / function,
such that every record in `stage_hfp.journeys` has corresponding records
in `stage_hfp.points` and vice versa.

Data processing from stage_hfp.raw:

1) Get common journey attributes (`jrnid...veh`) and aggregate statistics, and insert them into `.journeys`.
Any journeys with a NULL `jrnid` are ignored.
2) `.journeys`: find a corresponding trip template in `sched` schema, and set `ttid`.
If `ttid` is not found, add an invalid_reason for that.
(A journey is "valid" is it has zero `invalid_reasons` and "invalid" otherwise.)
3) `.journey_points`: import all the ongoing points whose journey `jrnid` is valid.
For each point, look for the nearest matching trip segment:
use not only distance but also relative point and segment ranks, since the same link can be traversed multiple times, and we don't want the "randomly first one" of those segments to pick all the observations.
Calculate the projected point geometry on the segment, distance between original and projected point, and the relative location of the projected point on the segment (between 0.0 and 1.0).
Points with `NULL` coordinates do not of course get a segment reference at this point.

Choose the best one from route-dir-start_ts duplicates

Interpolate points where coordinates are missing but odometer can be used

Delete duplicated timestamps per journey: prioritize minimal distance to segment & information completeness

Detect actual initial start timestamp (when first moving along the itinerary): `actual_start_ts`

#### journeys

Various per-journey statistics are calculated so they can be used later for validation:

- `n_total` total number of observations,
- `n_ongoing`: number of obs where `is_ongoing IS true`.
  In the later steps, we are only interested in `ongoing` observations where
  the vehicle has been in customer service.
- `n_dooropen`: number of obs where `drst IS true`.
  If this is zero, no door events have been detected, the door sensor might have been defect,
  and the journey is useless in that sense.
- `tst_span`: range of timestamp `tst` values **in ongoing observations**.
  Some hints of a possibly invalid journey:
  - `oday+start` is too far away from the start or end of `tst_span`
  - Duration of `tst_span` in seconds is considerably larger value than
    the number of ongoing observations. Usually there should be one observation per second.
    Note, though, that some observations are duplicated over `tst`, distinct by `event_type`,
    so this is not a completely accurate metric.

Based on `oday`, `start`, `route` and `dir`, for each journey
we look up for a corresponding trip template from `sched` schema.
Matching journeys get a `ttid` value.
Non-matching journeys should be discarded as invalid ones.
Then we can fill some geometry values regarding the journey as a whole:

- `line_raw_length`: length of the linestring composed by raw observation points, in meters
- `line_tt_length`: length of the linestring given by `ttid` from the `sched` schema
- `line_ref_length`: length of the linestring composed by raw points that are projected on the trip template linestring, without "reverse" parts


- `invalid_reasons`: reasons why a journey is considered invalid and therefore discarded
  will be listed in this text array.
  If the arrays is empty, the journey is valid.


#### points_ongoing

*TODO:* Calculate total nr of observations, but only save ongoing ones!
