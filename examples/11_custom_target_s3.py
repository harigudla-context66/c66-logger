"""
Case 11 — a future target: S3.

S3 isn't built in yet; this is what a sink for it looks like. Entries: one
JSON Lines object per batch. Runs: one JSON object per run, overwritten when
the run ends. Field names match logs.log_entry / logs.log_run, so Athena or
Spark can query them with the same column names.

Runs anywhere: without C66_EXAMPLE_S3_BUCKET it writes to a local folder through
a stand-in client; with it set (plus boto3 and AWS credentials) it writes to S3.

    python examples/11_custom_target_s3.py
"""

from __future__ import annotations

import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Any, Optional, Sequence

from _settings import TENANT

from c66_logger import AuditLogger, BaseSink, LogEntry, LogRun, register_sink


class S3Sink(BaseSink):
    """
    s3://<bucket>/<prefix>/log_entry/tenant_id=<t>/dt=YYYY-MM-DD/<uuid>.jsonl
    s3://<bucket>/<prefix>/log_run/tenant_id=<t>/<run_id>.json
    """

    target_type = "s3"

    def __init__(self, *, bucket: str, prefix: str = "logs", client: Optional[Any] = None,
                 region: Optional[str] = None):
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        if client is None:
            import boto3  # only needed when the caller doesn't inject a client

            client = boto3.client("s3", region_name=region)
        self._client = client

    def open(self) -> None:
        self._client.head_bucket(Bucket=self.bucket)  # fail fast on bucket / credentials

    def write_batch(self, entries: Sequence[LogEntry]) -> None:
        if not entries:
            return
        first = entries[0]
        key = (f"{self.prefix}/log_entry/tenant_id={first.tenant_id}/"
               f"dt={first.logged_at:%Y-%m-%d}/{uuid.uuid4()}.jsonl")
        body = "\n".join(e.to_json() for e in entries).encode("utf-8")
        self._client.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/x-ndjson")
        # A retried batch lands as a new object; readers de-duplicate on log_id.

    def write_run(self, run: LogRun) -> None:
        key = f"{self.prefix}/log_run/tenant_id={run.tenant_id}/{run.run_id}.json"
        self._client.put_object(Bucket=self.bucket, Key=key, Body=run.to_json().encode("utf-8"),
                                ContentType="application/json")


class LocalFolderS3Client:
    """Stand-in for boto3's client so the example runs without AWS."""

    def __init__(self, root: Path):
        self.root = root

    def head_bucket(self, Bucket: str) -> None:
        (self.root / Bucket).mkdir(parents=True, exist_ok=True)

    def put_object(self, Bucket: str, Key: str, Body: bytes, **_: Any) -> None:
        path = self.root / Bucket / Key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(Body)


register_sink("s3", S3Sink)


def main() -> None:
    bucket = os.environ.get("C66_EXAMPLE_S3_BUCKET")
    if bucket:
        connection = {"bucket": bucket}
    else:
        root = Path(tempfile.gettempdir()) / "c66_fake_s3"
        shutil.rmtree(root, ignore_errors=True)  # fresh folder for the demo
        connection = {"bucket": "log-bucket", "client": LocalFolderS3Client(root)}

    with AuditLogger(**TENANT, target_type="s3", connection=connection, logger_name="clickstream") as audit:
        with audit.run("clickstream_export"):
            audit.info(*[{"message": "page view", "page": f"/p/{i}"} for i in range(5)])
            audit.info("message,order_id\norder created,6001\norder paid,6001")

    if not bucket:
        print(f"objects under {root}:")
        for obj in sorted(root.rglob("*.json*")):
            print(f"  {obj.relative_to(root)}  ({len(obj.read_text().splitlines())} line(s))")


if __name__ == "__main__":
    main()
