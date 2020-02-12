# Sujuiko database

WIP

## Installation

On Ubuntu 18.04 LTS.

```
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
RELEASE=$(lsb_release -cs)
echo "deb http://apt.postgresql.org/pub/repos/apt/ ${RELEASE}"-pgdg main | sudo tee  /etc/apt/sources.list.d/pgdg.list
# Verify:
cat /etc/apt/sources.list.d/pgdg.list
sudo apt update
# Install postgres, postgis and pgrouting
sudo apt -y install postgresql-11
sudo apt -y install postgis postgresql-11-postgis-2.5
sudo apt -y install postgresql-11-pgrouting
# Install timescaledb
sudo add-apt-repository ppa:timescale/timescaledb-ppa
sudo apt-get update
sudo apt -y install timescaledb-postgresql-11
```

Run `timescaledb-tune` to configure memory and other parameters according to your system.

```
sudo timescaledb-tune
```

Tweak `/etc/postgresql/11/main/postgresql.conf` if needed.
For example, ensure that you are using `data_directory` on a correct disk and with permissions for `postgres` user.

Finally, restart PostgreSQL server:

```
sudo service postgresql restart
```

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
