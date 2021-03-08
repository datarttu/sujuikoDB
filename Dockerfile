FROM pgrouting/pgrouting:13-3.1-3.1.3
RUN set -ex \
  && apt-get update \
  && apt-get -y install wget \
  && sh -c "echo 'deb https://packagecloud.io/timescale/timescaledb/debian/ buster main' > /etc/apt/sources.list.d/timescaledb.list" \
  && wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | apt-key add - \
  && apt-get update \
  && apt-get -y install timescaledb-2-postgresql-13
COPY config/001_timescaledb_tune.sh /docker-entrypoint-initdb.d/
