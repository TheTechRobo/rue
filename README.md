# rue

RethinkDB-backed queue

## Requirements

Python 3.11 or newer is required.

## WARNING

rue is still under heavy development. There are missing features, and the schema and/or API may change in incompatible ways. While the database has a schema version field, it is currently unused as the schema is too unstable for it to be practical.

Proper documentation will come when rue is feature-complete.

## Tests

A test suite is provided in the `test` directory. Use the `tests.sh` file to run tests. It will test against multiple Python versions. Currently, the testing code is a little ugly, but it does work.
