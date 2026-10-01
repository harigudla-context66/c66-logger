from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

try:
    import pymongo
    from pymongo.errors import BulkWriteError
except ImportError as exc:  # pragma: no cover - exercised only without the extra
    from ..exceptions import MissingDependencyError

    raise MissingDependencyError("mongodb", "mongodb") from exc

from ..exceptions import ConfigurationError
from ..record import LogEntry, LogRun, utcnow
from .base import BaseSink

_DUPLICATE_KEY = 11000


class MongoSink(BaseSink):
    """
    Mirrors the two Postgres tables as two MongoDB collections with the same
    field names: ``log_entry`` (``_id`` = log_id) and ``log_run`` (``_id`` = run_id).
    UUIDs are stored as strings; ``created_at`` is set when a document is first written.

    Connection — give exactly one of:
      * ``uri``     e.g. "mongodb://user:pass@host:27017"
      * ``client``  an existing pymongo.MongoClient owned by the host package
                    (the sink will never close it)
    plus ``database``.

    MongoDB has no foreign keys, so nothing is rejected the way Postgres
    rejects an unknown tenant_id or run_id.
    """

    target_type = "mongodb"

    def __init__(
        self,
        *,
        uri: Optional[str] = None,
        client: Optional["pymongo.MongoClient"] = None,
        database: Optional[str] = None,
        entry_collection: str = "log_entry",
        run_collection: str = "log_run",
        create_indexes: bool = True,
        client_options: Optional[Dict[str, Any]] = None,
    ):
        if bool(uri) == bool(client):
            raise ConfigurationError("MongoSink needs exactly one of uri or client")
        if not database:
            raise ConfigurationError("MongoSink: 'database' is required")

        self._owns_client = client is None
        self._client = client or pymongo.MongoClient(uri, **(client_options or {}))
        db = self._client[database]
        self.entry_collection = db[entry_collection]
        self.run_collection = db[run_collection]
        self._create_indexes = create_indexes

    def open(self) -> None:
        if not self._create_indexes:
            self._client.admin.command("ping")  # fail fast on bad connection details
            return
        # Same access paths as the Postgres indexes.
        self.entry_collection.create_index([("tenant_id", 1), ("environment_id", 1), ("logged_at", -1)])
        self.entry_collection.create_index([("tenant_id", 1), ("level_no", 1), ("logged_at", -1)])
        self.entry_collection.create_index([("run_id", 1)])
        self.entry_collection.create_index([("correlation_id", 1)], sparse=True)
        self.run_collection.create_index([("tenant_id", 1), ("environment_id", 1), ("started_at", -1)])
        self.run_collection.create_index([("status", 1)])

    def write_batch(self, entries: Sequence[LogEntry]) -> None:
        if not entries:
            return
        now = utcnow()
        docs = [{"_id": e.log_id, **e.as_row(), "created_at": now} for e in entries]
        try:
            self.entry_collection.insert_many(docs, ordered=False)
        except BulkWriteError as exc:
            errors = exc.details.get("writeErrors", [])
            if errors and all(e.get("code") == _DUPLICATE_KEY for e in errors):
                return  # retry of a batch that partly landed: the rest were written
            raise

    def write_run(self, run: LogRun) -> None:
        self.run_collection.update_one(
            {"_id": run.run_id},
            {"$set": run.as_row(), "$setOnInsert": {"created_at": utcnow()}},
            upsert=True,
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()
