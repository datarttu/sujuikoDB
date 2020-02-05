FROM timescale/timescaledb-postgis
FROM pgrouting/pgrouting:v2.6.3-postgresql_11
COPY ./db/ddl/* /docker-entrypoint-initdb.d/
