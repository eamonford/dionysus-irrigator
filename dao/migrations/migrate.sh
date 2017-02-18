#!/bin/bash
export PGPASSWORD=password
FILES=./sql/*

for f in $FILES
do
  echo "Migrating $f..."
  psql --host=localhost --user=admin --db=database --file=$f
done
echo "Migration complete"
