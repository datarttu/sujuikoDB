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

- Data files on the server can be read directly by `COPY FROM`.
- Data files on the client side must be read by psql `\copy` meta-command.

The csv files must have a header and the fields must be the same as in
`stage_hfp.raw` up until `route`.
(Other fields in the table will be calculated by triggers or otherwise.)

A data file must not contain more than one distinct `oday` value.
However, it is recommended to distribute data into even smaller files,
e.g., `hfp_[oday]_[route].csv`, to better monitor which of the raw data
can be successfully imported and which not.

There must not be empty `tst` values in the data files.
`stage_hfp.raw` has a `NOT NULL` constraint on `tst`, because this table is
partitioned into Timescale hypertable along `tst`.
We don't have any use for observations without a timestamp, anyway.
You should get rid of such rows already outside the database.

## Data flow

### Importing from csv files

`stage_hfp.raw` receives the data by `COPY FROM` command(s).

`jrnid` is calculated on the fly by a trigger.
This field describes a unique *trip* taken by a *vehicle*.
Note that regarding the raw data, finding unique trips only would not be
enough: multiple vehicle drivers may have signed in on the same trip
in the schedule system, and later we have to choose which of these
we consider valid.

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



- `invalid_reasons`: reasons why a journey is considered invalid and therefore discarded
  will be listed in this text array.
  If the arrays is empty, the journey is valid.


#### points_ongoing

*TODO:* Calculate total nr of observations, but only save ongoing ones!
