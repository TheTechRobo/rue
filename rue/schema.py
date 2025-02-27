import copy
import dataclasses
import datetime
import enum
import typing

from .rethinkdb import *
from .exceptions import *
from .logging import *

__all__ = [
    "Id",
    "Status",
    "Seconds",
    "Expiry",
    "Entry",
    "Attempt"
]

Id = typing.NewType("Id", str)

class Status(enum.StrEnum):
    TODO = enum.auto()
    CLAIMED = enum.auto()
    DONE = enum.auto()
    ERROR = enum.auto()
    STASHED = enum.auto()

_Seconds = int
Seconds = typing.NewType("Seconds", _Seconds)
# If datetime, the time it expires. If 'never', never expires. If None, no limitation.
Expiry = datetime.datetime | typing.Literal['never'] | None

@dataclasses.dataclass(frozen = True)
class Attempt:
    pipeline: str
    """The pipeline that ran this attempt."""

    error: typing.Optional[str] = None
    """The error encountered on this attempt, if any."""

    poke_reason: typing.Optional[str] = None
    """Reason for admin poke (abort or manual reclaim), if any."""

    @staticmethod
    def _from_dict(d):
        return Attempt(**d)

    def as_dict(self):
        rv = dataclasses.asdict(self)
        # Don't include error field if there was no error, to save space
        if not rv['error']:
            del rv['error']
        if not rv['poke_reason']:
            del rv['poke_reason']
        return rv

@dataclasses.dataclass(frozen = True)
class Entry:
    id: typing.Optional[Id]
    """The primary key of the entry. If it is None, it has not been added to the database yet."""

    item: str
    """The task payload, e.g. a URL, username, etc."""

    queued_by: str
    """The person or program that queued the job."""

    pipeline_type: str
    """The type of job, e.g. brozzler, zeno, etc."""

    status: Status
    """Current status of the job."""

    attempts: list[Attempt]
    """
    Record of all claims of an item. If an item exceeds X attempts, it is marked failed.
    The number of attempts is added to the priority when determining job order. So a job
    with priority 20 that has been tried twice will have an effective priority of 22.
    """

    queued_at: datetime.datetime
    """The time the job was created."""

    claimed_at: typing.Optional[datetime.datetime] = None
    """The time the job was last claimed, if any."""

    finished_at: typing.Optional[datetime.datetime] = None
    """The time the job was marked finished, if any."""

    # We don't use indexes when retrieving this so that a) we can provide a good error message,
    # and b) because indexes don't include null.
    expires: Expiry = None
    """When the job expires and can be queued again."""

    explanation: typing.Optional[str] = None
    """A string with more details about the job."""

    parent_item: typing.Optional[Id] = None
    """The parent job, if any."""

    # Lower priority = runs first
    # Blame RethinkDB: https://github.com/rethinkdb/rethinkdb/issues/2306
    priority: int = 0
    """The priority. Lower priority runs first. Jobs with the same priority are FIFO."""

    # Empty string means no limitation.
    # Null is not indexed into secondary indexes, so we can't use that.
    run_on: str = ""
    """Force the job to run on a specific pipeline. An empty string means no limitation."""

    stash: typing.Optional[str] = None
    """If the item is in a stash, which stash? (A dripfed item is still considered in the stash.)"""

    metadata: dict = dataclasses.field(default_factory = dict)
    """Arbitrary metadata for the job."""

    def attempt_number(self) -> int:
        return len(self.attempts) - 1

    @staticmethod
    def _from_dict(d):
        d = copy.copy(d)
        d['status'] = Status(d['status'])
        d['attempts'] = [Attempt._from_dict(attempt) for attempt in d['attempts']]
        return Entry(**d)

    @staticmethod
    async def _new(database_name: str, table_name: str, item: str, pipeline_type: str, queued_by: str, expires: typing.Optional[Seconds] = None, priority: int = 0, metadata: typing.Optional[dict] = None, explanation: str | None = None, stash: str | None = None):
        if metadata is None:
            metadata = {}
        d = Entry(
            id = None,
            item = item,
            queued_by = queued_by,
            expires = r.now() + expires if isinstance(expires, _Seconds) else expires, # What the type system doesn't know won't hurt it.
            status = Status.STASHED if stash else Status.TODO,
            queued_at = r.now(),
            priority = priority,
            pipeline_type = pipeline_type,
            metadata = metadata,
            explanation = explanation,
            attempts = [],
            stash = stash
        )
        async with connect() as conn:
            res = await (
                r
                .db(database_name)
                .table(table_name)
                .insert(d.as_dict(), return_changes=True)
                .run(conn)
            )
            assert res['inserted'] == 1 and not res['errors']
        return Entry._from_dict(res['changes'][0]['new_val'])

    def as_dict(self):
        d = dataclasses.asdict(self)
        if self.id is None:
            del d['id'] # So that RethinkDB knows to generate its own.
        return d

    def as_json_friendly_dict(self):
        """
        Like as_dict, but converts unserializable objects (e.g. datetimes) to JSON equivalents.
        You cannot directly deserialize a dict in this form.
        """
        d = self.as_dict()
        d['queued_at'] = self.queued_at.timestamp()
        if self.claimed_at is not None: d['claimed_at'] = self.claimed_at.timestamp()
        if self.finished_at is not None: d['finished_at'] = self.finished_at.timestamp()
        d['attempts'] = [i.as_dict() for i in self.attempts]
        return d
