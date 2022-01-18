# Sujuiko database

*NOTE: The thesis behind this software project was completed, and this repository is no longer actively maintained.*
*The tool is incomplete and you'll find a bunch of TODOs here.*
*However, feel free to use parts of it in your work, just remember to include a reference to here.*

![Title picture: general idea.](docs/img/title_example_picture.png)

The purpose of this tool is to enable analysis of public transport service in the past at a more detailed level than transit stops, and, on the other hand, at the general transit network level.
This is done by aggregating historical [high-frequency positioning data](https://digitransit.fi/en/developers/apis/4-realtime-api/vehicle-positions/) (HFP) points projected onto a transit network consisting of links and nodes.
Currently, this very experimental tool is developed using the [HSL](hsl.fi/) bus and tram data only, as well as the related OpenStreetMap network and GTFS data.
Development is based on an HFP raw data set from November 2019, but in future the tool should support analyzing much longer periods of time.

By using the tool, one should be able to answer the following questions, for example:

- A given transit line seems to get always delayed between stops A and B.
What is happening along the network route between A and B?
Where do the vehicles tend to stop, and for how long at each location?
- What is the average speed and its standard deviation of transit vehicles that went through a given link, or from point A to point B using the same path on the network?
- How many seconds do transit vehicles in average remain stopped at a given intersection?
- How do these measures vary in time, e.g., between two different weeks, working days vs. weekends, or peak hours vs. off-peak?
- What are the "worst" links on the network causing delays, in relation to a weight measure such as number of scheduled trips per link?

The general idea is that we do not store every single HFP observation as a point geometry but instead, we store a reference to a network link that is used by a set of successive observations, and an array of relative time and location values for those observations along the link.
As a result, we can inspect time-space profiles along sequences of links like these ...

![Example of a driving time profile](docs/img/1088_optime_example.png)

![Example of a speed profile](docs/img/1088_speed_example.png)

... and, finally, aggregate that data by link, route or other common attributes.

To reliably project the HFP data onto the network links, we also need a reasonable model for planned transit routes and their paths on the network.
This enables mapping each HFP journey (effectively, GPS track) to a path of network links, and further, map-matching HFP points to those links.

# Requirements

The tool is being developed on an Ubuntu 18.04 LTS server with 2 TB of disk space, 8 GB RAM and 2 CPU cores.
I have not tested anything on Windows.

You will need the following, either installed on the machine or by using Docker (deployment instructed below):

- [PostgreSQL](https://www.postgresql.org/) 13.
This is the core of the tool.
Also majority of the data transformation logic is written in PLPGSQL.
- [PostGIS](https://postgis.net/) 3.
Core of the geometries and spatial operations.
- [pgRouting](http://docs.pgrouting.org/latest/en/index.html) 3.
Core of the routable network model.
- [TimescaleDB](https://docs.timescale.com/latest/main) 2.
Supports partitioning and managing large amounts of the HFP data.

# Development

This is how you should get the database up and running for development purposes.
For production, you may want to create and fill the database using a PostgreSQL cluster installed directly on your server.

```
git clone https://github.com/datarttu/sujuikoDB
cd sujuikoDB
cp .env_test .env
```

Now configure the values in `.env` according to your local environment.
For instance, you may want to use a custom Postgres port instead of `5432` if a Postgres cluster is already running on your system.
Also check that the `docker-compose` files suit your needs.
Then build the Docker image:

```
docker build -t datarttu/sujuikodb:latest .
```

## Testing the DDL and data imports with example data

1. Run `./test.sh`.
1. Check `db` and `dataimporter` log entries in your terminal. They should end successfully after the `COPY` commands (like `sujuikodb_dataimporter_1 exited with code 0`).

The database data is saved on a temporary volume that is removed after removing the services.

## Testing with a persistent database

1. In `.env`, set `IMPORT_DATA_DIR` to a directory containing the import files you want into the database, as well as the `import.sql` script, structured the same way as in [`example_data/`](./example_data/).
Or just use the example data directory.
1. Start the database: `docker-compose -f docker-compose.db.yml up -d`.
1. Now you can connect to the database e.g. with psql or QGIS, using `localhost` and the connection parameters you set in `.env`.
1. If you started the database from scratch, run the data importer: `./db_importer.sh`.
(If the data has already been imported, the importer will crash to the first primary key conflict.)

A Docker volume `db_volume` is created and the database data is kept there between `docker-compose` runs unless you explicitly remove the volume.
Should you run any incremental / ad-hoc changes to the database, they are saved on `db_volume` and the database state will be the same if you restart the services.

# Data model & data import and transformation

Read more about the data model in the [db readme](./db/README.md).
Example datasets can be found [here](.example_data/).
Note that the `section` dataset was created in a running sujuiko instance, as it's not meant to be imported from any external data source but should be created by the user.

To produce data dumps for the database, see [sujuiko-network-prepare](https://github.com/datarttu/sujuiko-network-prepare) and .
Unfortunately, the data import and transformation procedure has not been streamlined very neatly.
It requires a lot of manual intervention.

## Data sources

- [HFP from Digitransit](https://digitransit.fi/en/developers/apis/4-realtime-api/vehicle-positions/) - this is real-time data but you can collect it yourself e.g. with this [hfplogger](https://github.com/datarttu/hfplogger) tool (a bit messy).
I am using a data dump from HSL.
See [sujuiko-hfp-manager](https://github.com/datarttu/sujuiko-hfp-manager) for preparing the data for sujuiko.
- Transit stops and route versions based on HSL Jore (Transit data registry) export dumps.
Unfortunately, this data is not open at the moment.
- [Digiroad](https://vayla.fi/vaylista/aineistot/digiroad) road links.
Links for sujuiko must be extracted manually at the moment, e.g. in QGIS, because there is no automated way to get links used by HSL bus routes.
See [sujuiko-network-prepare](https://github.com/datarttu/sujuiko-network-prepare) for further steps.

Note that sujuiko supports temporal versioning for routes but not for stops or links.
For instance, if a stop of interest was moved during the analysis period, you must still choose a "representative" location for that stop.
Or, possibly, manipulate the route versions and their stops to point to two different stop ids that represent the two versions of the same stop;
however, you will lose backward compatibility to Jore that way.

# Author

Arttu Kosonen, [@datarttu](https://github.com/datarttu), [HSL](https://hsl.fi) / [Aalto University](https://aalto.fi), 2019-2021.
Developing this tool was part of my master's thesis in Spatial Planning and Transportation Engineering.

See also: [Thoughts about Transit Data](https://datarttu.github.io/thoughts-about-transit-data/)
