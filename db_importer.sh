#!/bin/sh

set -u

[ ! -f ".env" ] && \
  echo ".env file missing" && \
  exit 1

export $(cat .env | xargs)

datadir="$(realpath $IMPORT_DATA_DIR)"

docker run --rm -v "$datadir"":/data" \
  --network sujuikodb_sujuikodb \
  --env-file .env datarttu/sujuikodb:latest \
sh -c "psql -1 -v ON_ERROR_STOP=on -f /data/import.sql"
