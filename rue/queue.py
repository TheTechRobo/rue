import collections.abc
import dataclasses
import typing
import time

from .rethinkdb import *
from .exceptions import *
from .schema import *
from .logging import *

__all__ = [
    "QueueStatus",
    "JobResult",
    "Queue",
    "DefaultOptions"
]

DbCreate = collections.namedtuple("DbCreate", ("name",))
TableCreate = collections.namedtuple("TableCreate", ("database", "name"))
IndexCreate = collections.namedtuple("IndexCreate", ("database", "table", "name", "args"))
DbInsert = collections.namedtuple("DbInsert", ("database", "table", "document"))

class ActionPlan(list):
    async def _run(self, conn):
        # TODO: If conflict, assume it was by another process
        for action in self:
            if isinstance(action, DbCreate):
                await r.db_create(action.name).run(conn)
            elif isinstance(action, TableCreate):
                await r.db(action.database).table_create(action.name).run(conn)
            elif isinstance(action, IndexCreate):
                await (
                    r
                    .db(action.database)
                    .table(action.table)
                    .index_create(action.name, *action.args)
                    .run(conn)
                )
            elif isinstance(action, DbInsert):
                res = await (
                    r
                    .db(action.database)
                    .table(action.table)
                    .insert(action.document)
                    .run(conn)
                )
                if res['inserted'] != 1 or res['errors']:
                    raise WriteError(res)
            else:
                raise TypeError("Unexpected action in action plan")

    def append_db(self, database):
        self.append(DbCreate(database))

    def append_table(self, database, table):
        self.append(TableCreate(database, table))

    def append_index(self, database, table, index, *args):
        self.append(IndexCreate(database, table, index, args))

    def append_insert(self, database, table, document):
        self.append(DbInsert(database, table, document))

class RueOptions:
    def __init__(self, queue: "Queue"):
        self.table = queue.metadata_table_name
        self.db = queue.database_name

    async def max_tries(self, new_value: typing.Optional[int] = None) -> int:
        async with connect() as conn:
            if new_value is not None:
                res = await (
                    r
                    .db(self.db)
                    .table(self.table)
                    .insert({
                        "id": "max_tries",
                        "value": new_value
                    }, conflict="replace")
                    .run(conn)
                )
                if res['errors']:
                    raise WriteError(res)
                return new_value
            res = await r.db(self.db).table(self.table).get("max_tries").run(conn)
            if res is None:
                # go with default; items fail immediately
                return DefaultOptions().max_tries
            return res['value']

@dataclasses.dataclass
class DefaultOptions:
    max_tries: int = 0

QueueStatus = collections.namedtuple("QueueStatus", ["counts", "limbo"])
JobResult = collections.namedtuple("JobResult", ["id", "item", "type", "data", "attempt"])
DripfeedBehaviour = collections.namedtuple("DripfeedBehaviour", ["concurrency"])
HeartbeatData = collections.namedtuple("HeartbeatData", ["last_seen"])

class Queue:
    """
    A RethinkDB-backed queue.

    To ensure consistency, you must call check() before using the queue object.
    check() ensures the database is usable.

    Concurrency note: Using the same database with two Queue objects is safe.
    Of course, the tables must be used for the same function.
    """

    current_db_version: int = 1

    options: RueOptions

    def __init__(self, database_name: str, queue_table_name: str = "queue", results_table_name: str = "results", metadata_table_name: str = "control", heartbeat_table_name: str = "heartbeat"):
        """
        Creates a new Queue object.

        database_name: The database to use, e.g. 'chromebot' or 'myreallycoolproject123'.
        queue_table_name: Table to store the queue in.
        results_table_name: Table to store job results from store_result() in.
        metadata_table_name: Table to store control information, like rate-limits, DB version, etc.
        """
        self.database_name = database_name
        self.table_name = queue_table_name
        self.results_table_name = results_table_name
        self.metadata_table_name = metadata_table_name
        self.heartbeat_table_name = heartbeat_table_name
        self.setup_finished = False
        self.options = RueOptions(self)

    def _r(self, table_name: str):
        return r.db(self.database_name).table(table_name)

    def _queue(self):
        return self._r(self.table_name)

    def _results(self):
        return self._r(self.results_table_name)

    def _control(self):
        return self._r(self.metadata_table_name)

    def _heartbeat(self):
        return self._r(self.heartbeat_table_name)

    def _ensure_setup(self):
        if not self.setup_finished:
            raise RuntimeError("Must call check() successfully before using queue")

    def _create_index(self, plan: ActionPlan, existing: list, db: str, table: str, name: str, *args):
        if name not in existing:
            plan.append_index(
                db,
                table,
                name,
                *args
            )

    async def _check_version(self, conn):
        if version := await self._control().get("version").run(conn):
            version = version['value']
            if version > self.current_db_version:
                raise DatabaseVersionTooHigh
            if version < self.current_db_version:
                raise DatabaseVersionTooLow
        else:
            raise DatabaseNotReady

    async def check(self):
        """
        Checks that the database is usable. Must be called before the queue can be used.
        """
        async with connect() as conn:
            await self._check_version(conn)
        self.setup_finished = True

    async def _create_table(self, conn, action_plan: ActionPlan, tables: list[str], table: str, new_indexes):
        if table in tables:
            existing_indexes = await (
                self._r(table)
                .index_list()
                .run(conn)
            )
        else:
            existing_indexes = []
            action_plan.append_table(self.database_name, table)
            tables.append(table)

        for index in new_indexes:
            self._create_index(
                action_plan,
                existing_indexes,
                self.database_name,
                table,
                *index
            )

    async def create(self, default_options: DefaultOptions, dry_run = False):
        """
        Ensures the database is ready for use. This may take awhile.

        rue will compute any necessary changes. This may include:
        - creating the database
        - creating tables
        - creating indexes
        - migrating existing records
        It also may simply do nothing, if the database is already ready.
        If dry_run = False, the changes will then be applied to the database.

        create() should not be run with dry_run = False from two threads/processes at once,
        or when another object/process is currently using the queue. In the former case, one
        or both calls will fail; in the latter case, rue may behave unpredictably.

        default_options contains the options that should be used if they have not been set yet.

        create() will set the setup_finished field to True, meaning you can use it without
        then calling check().

        Returns the action plan.
        """
        async with connect() as conn:
            action_plan = ActionPlan()

            dbs = await r.db_list().run(conn)
            if self.database_name in dbs:
                tables = await r.db(self.database_name).table_list().run(conn)
            else:
                tables = []
                action_plan.append_db(self.database_name)
                dbs.append(self.database_name)

            # Metadata table + version check

            version_ok = False
            max_tries = None
            if self.metadata_table_name in tables:
                try:
                    await self._check_version(conn)
                except DatabaseNotReady:
                    version_ok = False
                else:
                    version_ok = True
                max_tries = await self._control().get("max_tries").run(conn)

            indexes = [
                # --- Simple indexes
                # Allow filtering by mtype (used when there are multiple with the same type)
                ("mtype",)
            ]
            await self._create_table(
                conn,
                action_plan,
                tables,
                self.metadata_table_name,
                indexes
            )
            if not version_ok:
                action_plan.append_insert(
                    self.database_name,
                    self.metadata_table_name,
                    {"id": "version", "value": self.current_db_version}
                )
            if not max_tries:
                action_plan.append_insert(
                    self.database_name,
                    self.metadata_table_name,
                    {"id": "max_tries", "value": default_options.max_tries}
                )

            # Main queue table

            tries = r.row['attempts'].count()
            nf_queued = lambda row : row['status'].ne(Status.DONE).and_(row['status'].ne(Status.ERROR))
            # Order by effective priority followed by FIFO.
            nf_order = [r.row['priority'] + tries, r.row['queued_at']]

            # Allows us to save on index space usage by pruning finished items from the index
            # done is going to eventually get large, so keeping it out of the index is a good idea.
            # (RethinkDB doesn't index nulls.)
            no_done = lambda q : r.branch(r.row['status'].eq(Status.DONE), None, q)

            indexes = [
                # --- Simple indexes
                # Allow filtering by item name
                ("item",),

                # --- Compound indexes
                # True if the item is NOT in done or error.
                ("nf_queued", nf_queued),
                # Chonker of an index to filter by status and pipeline type, while still being able to order.
                ("nf_order", no_done([r.row['status'], r.row['pipeline_type'], r.row['run_on']] + nf_order)),
                # TODO: Figure out a way to get rid of this.
                # It's necessary because if you don't use pipeline_type or run_on in the
                # nf_order index, the regular order will be ignored because they come first.
                # Because between uses lexographical sorting, we can't put the real order first
                # as it will short-circuit the actual filters away.
                # With no_done, it's probably not a huge issue, but it's still ugly.
                ("nf_order_only", no_done([r.row['status']] + nf_order)),
                # Item status and time last claimed.
                ("nf_status", [r.row['status'], r.row['claimed_at'], r.row['claimed_by']]),
                # None if it is not in todo to save space.
                # This over [r.row['status'], r.row['stash']] so done isn't a part of it.
                ("nf_was_stashed", r.branch(r.row['status'] == Status.TODO, r.row['stash'], None)),
                # Item parent and nf_queued
                ("nf_parent", lambda row : [row['parent_item'], nf_queued(row)]),
            ]
            await self._create_table(
                conn,
                action_plan,
                tables,
                self.table_name,
                indexes
            )

            # Results table

            indexes = [
                # --- Regular indexes
                ("item",),
            ]
            await self._create_table(
                conn,
                action_plan,
                tables,
                self.results_table_name,
                indexes
            )

            await self._create_table(
                conn,
                action_plan,
                tables,
                self.heartbeat_table_name,
                []
            )

            if action_plan and not dry_run:
                await action_plan._run(conn)
                for table in tables:
                    await self._r(table).index_wait().run(conn)

            self.setup_finished = True

            return action_plan

    def _fifo(self, status: Status, for_who: str, pipeline_type: str):
        return (
            self._queue()
            # You can only use one secondary index at a time, so we use a chonker of an
            # index that includes status, type, priority, and timestamp.
            # This allows us to filter the output and add FIFO, all with the same index.
            .between(
                [status, pipeline_type, for_who, r.minval, r.minval],
                [status, pipeline_type, for_who, r.maxval, r.maxval],
                index="nf_order"
            )
            .order_by(index="nf_order")
        )

    def _fifo_all(self, status: Status):
        return (
            self._queue()
            .between(
                [status, r.minval, r.minval],
                [status, r.maxval, r.maxval],
                index = "nf_order_only"
            )
            .order_by(index = "nf_order_only")
        )

    async def _claim(self, conn, by_who: str, for_who: str, pipeline_type: str) -> Entry | None:
        new_attempt = Attempt(pipeline = by_who).as_dict()
        rv = await (
            self._fifo(Status.TODO, for_who, pipeline_type)
            .limit(1)
            .update(
                # https://rethinkdb.com/docs/consistency/
                # The filter isn't atomic, so we have to add this branch to the update too.
                r.branch(
                    r.row['status'] == str(Status.TODO),
                    {
                        "status": str(Status.CLAIMED),
                        "claimed_at": r.now(),
                        "attempts": r.row['attempts'].append(new_attempt)
                    },
                    {}
                ), return_changes = True
            )
            .run(conn)
        )
        if rv['replaced'] == 0:
            # Either there was nothing to replace, someone else claimed it at the same time,
            # or a write error occured.
            # Either way, no item found.
            return None
        assert rv['replaced'] == 1 and rv['errors'] == 0
        return Entry._from_dict(rv['changes'][0]['new_val'])

    async def claim(self, by_who: str, pipeline_type: str) -> Entry | None:
        """
        Tries to claim a job from the database.

        Atempts to dequeue from the pipeline-specific queue first (where run_on = by_who),
        and only try the general queue when no item is found.

        Returns the item, or None if none can be found. Future circumstances, such as ratelimiting,
        may be handled by raising subclasses of NoItemServes.
        """
        self._ensure_setup()
        async with connect() as conn:
            # Try to dequeue from pipeline-specific queue first, then try the general queue
            return await self._claim(conn, by_who, by_who, pipeline_type) \
                or await self._claim(conn, by_who, "", pipeline_type)

    async def pending(self) -> list[Entry]:
        """
        Gets list of pending items, in order. WARNING: This list may be huge!
        """
        # Pipeline-specific is always dequeued first
        pipeline_specific = []
        general = []
        async with connect() as conn:
            gen = await (
                self._fifo_all(Status.TODO)
                .run(conn)
            )
            async for entry in gen:
                entry = Entry._from_dict(entry)
                if entry.run_on:
                    pipeline_specific.append(entry)
                else:
                    general.append(entry)
        return pipeline_specific + general

    async def claimed(self):
        """
        Yields claimed items, in order of first claimed to last claimed.
        """
        async with connect() as conn:
            gen = await (
                self._queue()
                .between(
                    [Status.CLAIMED, r.minval, r.minval],
                    [Status.CLAIMED, r.maxval, r.maxval],
                    index = "nf_status"
                )
                .order_by(index = "nf_status")
                .run(conn)
            )
            async for entry in gen:
                entry = Entry._from_dict(entry)
                yield entry

    async def get(self, item: str) -> typing.Optional[Entry]:
        """
        Retrieves (but does not claim) a specific job from the database.
        """
        self._ensure_setup()
        async with connect() as conn:
            e = await (
                self._queue()
                .get(item)
                .run(conn)
            )
            if e:
                return Entry._from_dict(e)
            return None

    async def new(self, item: str, pipeline_type: str, queued_by: str, expires: typing.Optional[Seconds] = None, priority: int = 0, metadata: typing.Optional[dict] = None, explanation: typing.Optional[str] = None, stash: typing.Optional[str] = None) -> Entry:
        """
        Creates a new job and adds it to the database.
        If stash is non-null, the item will be stashed; otherwise, it will be added to todo.
        """
        self._ensure_setup()
        return await Entry._new(self.database_name, self.table_name, item, pipeline_type, queued_by, expires, priority, metadata, explanation, stash)

    def _limbo(self, grace = 86400):
        self._ensure_setup()
        return (
            self._queue()
            .between(
                [Status.CLAIMED, r.minval, r.minval],
                [Status.CLAIMED, r.now() - grace, r.maxval],
                index="nf_status"
            )
        )

    async def get_limbo(self, grace = 86400):
        """
        Gets all jobs in limbo.

        A job is considered 'in limbo' if it has been claimed for more than `grace` seconds.
        `grace` defaults to one day (86400 seconds).
        """
        self._ensure_setup()
        async with connect() as conn:
            l = []
            async for id in await self._limbo(grace)['id'].run(conn):
                l.append(id)
            return l

    async def counts(self) -> QueueStatus:
        """
        Gets the amount of jobs in each status for each pipeline type, and the number of jobs in limbo.
        Pipeline types that have no jobs are not returned. So if there aren't any jobs that aren't in DONE or ERROR, this will return {}.
        """
        self._ensure_setup()
        baseline = {v: 0 for _, v in Status._member_map_.items()}
        del baseline[Status.DONE], baseline[Status.ERROR]
        async with connect() as conn:
            weird_counts = await (
                self._queue()
                .get_all(True, index="nf_queued")
                .group(lambda item : item.pluck("status", "pipeline_type"))
                .count()
                .run(conn)
            )
            in_limbo = await self._limbo().count().run(conn)
        # Format is { frozenset({("status", "todo"), ("pipeline_type", "brozzler")}): 0 }
        reorganized = {}
        for weird_key, count in weird_counts.items():
            # set up dictionary with status and pipeline
            ci = {}
            for rk, rv in weird_key: # real key and real value
                ci[rk] = rv
            status = Status(ci['status'])
            # put it into reorganized dict
            d = reorganized.get(ci['pipeline_type'], {})
            d[status] = count
            reorganized[ci['pipeline_type']] = baseline | d
        return QueueStatus(reorganized, in_limbo)

    async def store_result(self, item: Entry, current_attempt: int, data: dict[str, typing.Any], result_type: str) -> str:
        """
        Stores the result of an item in the results table.

        item must have an associated ID. (It must have been saved in the database at some point.)
        data must be JSON-serializable.

        Why must current_attempt be provided, you ask? Well, because if you are using e.g. reclaiming,
        the Entry's current values may be inaccurate. If you know you aren't, you may take
        the values from `item`.

        Returns the primary key of the stored result.
        """
        self._ensure_setup()
        if item.id is None:
            raise TypeError("Item does not have an identifier")
        async with connect() as conn:
            res = await (
                self._results()
                .insert({
                    "item": item.id,
                    "data": data,
                    "type": result_type,
                    "try": current_attempt
                })
                .run(conn)
            )
        if res['inserted'] != 1 or res['errors']:
            raise WriteError(res)
        return res['generated_keys'][0]

    async def get_results(self, item: Entry) -> collections.abc.AsyncGenerator[JobResult]:
        """
        Gets the job results for a given item.

        item must have an associated ID. (It must have been saved to the database at one point.)
        """
        self._ensure_setup()
        if item.id is None:
            raise TypeError("Item does not have an identifier")
        async with connect() as conn:
            res = await (
                self._results()
                .get_all(item.id, index="item")
                .run(conn)
            )
            async for entry in res:
                yield JobResult(
                    id = entry['id'],
                    item = entry['item'],
                    type = entry['type'],
                    data = entry['data'],
                    attempt = entry['try']
                )

    async def fail(self, item: Entry, reason: str, current_attempt: int, is_poke: bool = False) -> Entry:
        """
        Fails an item.

        If len(item.attempts) >= max_tries, its status will be set to ERROR. Otherwise, it will
        be moved back to TODO.

        See store_result for why current_attempt must be provided.

        If is_poke is True, the failure message will be stored under poke_reason rather than error.
        This is so that error messages aren't lost by admin wizardry.

        Returns a new Entry object.
        """
        self._ensure_setup()
        if item.id is None:
            raise TypeError("Item does not have an identifier")
        if current_attempt < 0:
            raise ValueError("current_attempt must be positive")
        reason_key = "poke_reason" if is_poke else "error"
        new_attempts = r.row['attempts'].change_at(
            current_attempt,
            r.row['attempts'][current_attempt].merge({reason_key: reason})
        )
        async with connect() as conn:
            max_tries = await self.options.max_tries()
            res = await (
                self._queue()
                .get(item.id)
                .update(
                    # set error reason and maybe update status
                    r.branch(
                        # if tries >= max_tries...
                        r.row['attempts'].count() >= max_tries,
                        r.branch(
                            # Allows everything except done in case items get reclaimed or
                            # something while the pipeline still has it
                            r.row['status'] != Status.DONE,
                            {"status": Status.ERROR, "attempts": new_attempts},
                            {"attempts": new_attempts}
                        ),
                        # else...
                        r.branch(
                            r.row['status'] != Status.DONE,
                            {"status": Status.TODO, "attempts": new_attempts},
                            {"attempts": new_attempts}
                        )
                    ), return_changes = True
                )
                .run(conn)
            )
            if res['errors']:
                raise WriteError(res)
            new_entry = Entry._from_dict(res['changes'][0]['new_val'])
            if new_entry.status not in (Status.TODO, Status.ERROR):
                raise LostTheRace(item, new_entry)
            # TODO: Parent item finishing
            return new_entry

    async def finish(self, item: Entry) -> Entry:
        """
        Finishes an item.
        The item will be marked finished even if it is not currently claimed. Be careful!
        """
        self._ensure_setup()
        if item.id is None:
            raise TypeError("Item does not have an identifier")
        async with connect() as conn:
            res = await (
                self._queue()
                .get(item.id)
                .update({"status": Status.DONE}, return_changes = True)
                .run(conn)
            )
            if res['errors']:
                raise WriteError(res)
            new_entry = Entry._from_dict(res['changes'][0]['new_val'])
            # TODO: Parent item finishing
            return new_entry

    async def set_dripfeed_behaviour(self, stash: str, concurrency: int):
        """
        Sets the dripfeed behaviour for a stash.
        """
        async with connect() as conn:
            res = await (
                self._control()
                .insert({
                    "id": f"stash-{stash}-dripfeed",
                    "mtype": "dripfeed",
                    "concurrency": concurrency,
                    "name": stash
                }, conflict = "replace")
                .run(conn)
            )
            if res['errors']:
                raise WriteError(res)

    async def get_dripfeed_behaviour(self, stash: str) -> DripfeedBehaviour | None:
        """
        Gets the dripfeed behaviour for a stash.
        """
        async with connect() as conn:
            res = await (
                self._control()
                .get(f"stash-{stash}-dripfeed")
                .run(conn)
            )
            if res:
                return DripfeedBehaviour(res['concurrency'])
            return None

    async def dripfeed(self) -> list[Entry]:
        """
        Tries to dripfeed enough items from each stash to meet the requested concurrency.
        THIS IS NOT ATOMIC. If you run this twice at the same time, even in another process,
        rue will dripfeed too many items. Either use a lock, or ensure it is only being called
        once at a time.
        It's recommended to do this in a separate task, as it may take some time with a lot
        of stashed items.
        """
        fin = []
        async with connect() as conn:
            eligible_stashes = await (
                self._control()
                .get_all("dripfeed", index = "mtype")
                .filter(r.row['concurrency'] > 0)
                .run(conn)
            )
            async for stash in eligible_stashes:
                stash, concurrency = stash['name'], stash['concurrency']
                num_items = await (
                    self._queue()
                    .get_all(stash, index = "nf_was_stashed")
                    .count()
                    .run(conn)
                )
                difference = concurrency - num_items
                if difference > 0:
                    res = await (
                        # Save the last items for last
                        self._fifo_all(Status.STASHED)
                        .filter({"stash": stash})
                        .limit(difference)
                        .update(r.branch(
                            r.row['status'].eq(Status.STASHED).and_(r.row['stash'] == stash),
                            {"status": Status.TODO},
                            {}
                        ), return_changes = True)
                        .run(conn)
                    )
                    if res['errors']:
                        # Carry on, but log the issue so it's known
                        logger.warning(f"failed to dripfeed one or more items from stash {stash}: {res}")
                    for change in res['changes']:
                        new_val = change['new_val']
                        ent = Entry._from_dict(new_val)
                        if ent.status == Status.TODO:
                            fin.append(ent)
                        else:
                            # We lost the race, but don't bother raising an exception
                            pass
        return fin

    async def heartbeat(self, pipeline: str):
        async with connect() as conn:
            await (
                self._heartbeat()
                .insert({
                    "id": pipeline,
                    "last_seen": time.time()
                }, conflict = "replace")
                .run(conn)
            )

    async def get_last_heartbeat(self, pipeline: str) -> HeartbeatData:
        async with connect() as conn:
            res = await (
                self._heartbeat()
                .get(pipeline)
                .run(conn)
            )
            return HeartbeatData(last_seen = res['last_seen'])

    async def change_explanation(self, item: Entry, new_explanation: str):
        if item.id is None:
            raise TypeError("Item does not have an identifier")
        async with connect() as conn:
            res = await (
                self._queue()
                .get(item.id)
                .update({"explanation": new_explanation}, return_changes = True)
                .run(conn)
            )
            if res['errors']:
                raise WriteError
            e = Entry._from_dict(res['changes'][0]['new_val'])
            return e
