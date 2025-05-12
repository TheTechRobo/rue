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
    "Attempt",
    "AttemptId",
    "Poke"
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
AttemptId = typing.NewType("AttemptId", int)

@dataclasses.dataclass(frozen = True)
class Poke:
    time: datetime.datetime
    """The time of the action."""

    action: tuple[str, typing.Optional[AttemptId]]
    """The action being logged."""

    reason: typing.Optional[str] = None
    """The reason for the action."""

    @classmethod
    def _from_dict(cls, d):
        d = copy.copy(d)
        d['action'] = tuple(d['action'])
        return cls(**d)

    def as_dict(self):
        d = dataclasses.asdict(self)
        if not self.reason:
            del d['reason']
        return d

@dataclasses.dataclass(frozen = True)
class Attempt:
    pipeline: str
    """The pipeline that ran this attempt."""

    error: typing.Optional[str] = None
    """The error encountered on this attempt, if any."""

    pipeline_version: typing.Optional[str] = None
    """The version of the pipeline. Can be any format of string; rue does not care about the value."""

    time: typing.Optional[datetime.datetime] = None
    """The time the attempt was started."""

    @classmethod
    def _from_dict(cls, d):
        d = copy.copy(d)
        if ver := d.get("ver"):
            d['pipeline_version'] = ver
            del d['ver']
        return cls(**d)

    def as_dict(self):
        rv = dataclasses.asdict(self)
        # Don't include error field if there was no error, to save space
        if not rv['error']:
            del rv['error']
        if pv := rv['pipeline_version']:
            rv['ver'] = pv
        del rv['pipeline_version']
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

    admin_log: list[Poke] = dataclasses.field(default_factory = list)
    """Record of changes made to the item. Some operations can optionally add to this."""

    metadata: dict = dataclasses.field(default_factory = dict)
    """Arbitrary metadata for the job."""

    def current_attempt(self) -> AttemptId:
        """
        Returns the index of the latest attempt. -1 if the item has not been tried yet.
        """
        return AttemptId(len(self.attempts) - 1)

    def attempt_count(self) -> AttemptId:
        """
        Returns the number of attempts.
        """
        return AttemptId(len(self.attempts))

    @staticmethod
    def _from_dict(d):
        d = copy.copy(d)
        d['status'] = Status(d['status'])
        d['attempts'] = [Attempt._from_dict(attempt) for attempt in d['attempts']]
        d['admin_log'] = [Poke._from_dict(poke) for poke in d.get("admin_log", [])]
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
        d['admin_log'] = [i.as_dict() for i in self.admin_log]
        return d
