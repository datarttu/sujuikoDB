#!/bin/bash
timescaledb-tune --quiet --yes
pg_ctl restart -D /var/lib/postgresql/data
