import asyncio

from rue import Queue, DefaultOptions

database_name = input("Enter database name: ")
print("Using default table names.")
max_tries = int(input("Enter maxtries: "))

queue = Queue(database_name)
res = asyncio.run(queue.create(DefaultOptions(max_tries = max_tries)))
print("Successfully created or updated database:")
print(res)
