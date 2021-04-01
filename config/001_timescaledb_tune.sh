#!/bin/bash
timescaledb-tune --quiet --yes
sed -r -i "s/[#]*\s*(shared_preload_libraries)\s*=\s*'(.*)'/\1 = 'timescaledb,\2'/;s/,'/'/" /var/lib/postgresql/data/postgresql.conf
service postgresql restart
