#!/bin/sh

docker compose up -d rethinkdb
for i in 3.11 3.12 3.13; do
	echo "Testing Python version ${i}"
	PYTHON_VERSION="$i" docker compose run --rm --build test || exit 1
done
