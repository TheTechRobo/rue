#!/usr/bin/env python3

import asyncio, sys

from rue import Queue, DefaultOptions

if len(sys.argv) != 3:
    sys.exit(f"Usage: {sys.argv[0]} <DB_NAME> <MAX_TRIES>")
_, database_name, max_tries = sys.argv
max_tries = int(max_tries)

queue = Queue(database_name)
res = asyncio.run(queue.create(DefaultOptions(max_tries = max_tries)))
print("Successfully created or updated database:")
print(res)
