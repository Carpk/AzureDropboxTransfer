#!/usr/bin/env python3
"""Transfer Dropbox files directly to S3 Glacier/Deep Archive storage classes.

Designed for large, long-running migrations (e.g. 30TB+):
- Streams file bytes from Dropbox to S3 (no full-file local staging)
- Tracks progress in a local SQLite checkpoint DB for resumability
- Retries transient errors with exponential backoff
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional

import boto3
import botocore
import dropbox
from boto3.s3.transfer import TransferConfig
from dropbox.exceptions import ApiError, AuthError, HttpError
from dropbox.files import FileMetadata, FolderMetadata


LOGGER = logging.getLogger("dropbox_to_glacier")


@dataclass
class TransferRecord:
    path_lower: str
    path_display: str
    size: int
    rev: str


class CheckpointDB:
    def __init__(self, db_path: str) -> None:
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                path_lower TEXT PRIMARY KEY,
                path_display TEXT NOT NULL,
                size INTEGER NOT NULL,
                rev TEXT NOT NULL,
                status TEXT NOT NULL,
                etag TEXT,
                last_error TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def upsert_discovered(self, rec: TransferRecord) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            INSERT INTO files(path_lower, path_display, size, rev, status, updated_at)
            VALUES (?, ?, ?, ?, 'discovered', ?)
            ON CONFLICT(path_lower) DO UPDATE SET
              path_display=excluded.path_display,
              size=excluded.size,
              rev=excluded.rev,
              updated_at=excluded.updated_at
            """,
            (rec.path_lower, rec.path_display, rec.size, rec.rev, now),
        )

    def mark_done(self, rec: TransferRecord, etag: Optional[str]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            UPDATE files
            SET status='done', etag=?, last_error=NULL, updated_at=?
            WHERE path_lower=?
            """,
            (etag, now, rec.path_lower),
        )

    def mark_failed(self, rec: TransferRecord, err: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            UPDATE files
            SET status='failed', last_error=?, updated_at=?
            WHERE path_lower=?
            """,
            (err[:2000], now, rec.path_lower),
        )

    def already_done(self, rec: TransferRecord) -> bool:
        row = self.conn.execute(
            "SELECT status, rev FROM files WHERE path_lower=?", (rec.path_lower,)
        ).fetchone()
        if not row:
            return False
        status, rev = row
        return status == "done" and rev == rec.rev

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


class DropboxResponseStream:
    """File-like wrapper around Dropbox SDK response for boto3 upload_fileobj."""

    def __init__(self, response, chunk_size: int = 8 * 1024 * 1024):
        self.response = response
        self.raw = response.raw
        self.chunk_size = chunk_size

    def read(self, amt: Optional[int] = None) -> bytes:
        if amt is None:
            amt = self.chunk_size
        return self.raw.read(amt)

    def close(self) -> None:
        try:
            self.response.close()
        except Exception:
            pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transfer Dropbox data to S3 Glacier/Deep Archive with resume support"
    )
    parser.add_argument("--dropbox-token", required=True, help="Dropbox API token")
    parser.add_argument("--dropbox-root", default="", help="Dropbox root path (default: entire account)")
    parser.add_argument("--bucket", required=True, help="Destination S3 bucket")
    parser.add_argument("--prefix", default="", help="Destination S3 key prefix")
    parser.add_argument(
        "--storage-class",
        default="DEEP_ARCHIVE",
        choices=["GLACIER", "DEEP_ARCHIVE", "GLACIER_IR", "STANDARD_IA", "STANDARD"],
        help="S3 storage class",
    )
    parser.add_argument("--region", default=None, help="AWS region for S3 client")
    parser.add_argument("--checkpoint-db", default="checkpoint.db", help="SQLite checkpoint path")
    parser.add_argument("--multipart-chunk-mb", type=int, default=64, help="Multipart chunk size in MiB")
    parser.add_argument("--max-concurrency", type=int, default=4, help="S3 multipart upload concurrency")
    parser.add_argument("--retries", type=int, default=8, help="Retries per file")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def iter_dropbox_files(dbx: dropbox.Dropbox, root: str) -> Iterable[TransferRecord]:
    result = dbx.files_list_folder(root or "", recursive=True)
    while True:
        for entry in result.entries:
            if isinstance(entry, FileMetadata):
                yield TransferRecord(
                    path_lower=entry.path_lower,
                    path_display=entry.path_display,
                    size=entry.size,
                    rev=entry.rev,
                )
            elif isinstance(entry, FolderMetadata):
                continue
        if not result.has_more:
            break
        result = dbx.files_list_folder_continue(result.cursor)


def s3_key_for(prefix: str, dropbox_path_display: str) -> str:
    normalized = dropbox_path_display.lstrip("/")
    if prefix:
        return f"{prefix.rstrip('/')}/{normalized}"
    return normalized


def retry_sleep(attempt: int) -> None:
    delay = min(60, (2**attempt) + (0.1 * attempt))
    time.sleep(delay)


def transfer_file(
    dbx: dropbox.Dropbox,
    s3_client,
    transfer_config: TransferConfig,
    rec: TransferRecord,
    bucket: str,
    key: str,
    storage_class: str,
    retries: int,
) -> str:
    for attempt in range(retries + 1):
        stream = None
        try:
            _, response = dbx.files_download(rec.path_display)
            stream = DropboxResponseStream(response)
            extra_args = {
                "StorageClass": storage_class,
                "Metadata": {
                    "dropbox-path": rec.path_display,
                    "dropbox-rev": rec.rev,
                    "dropbox-size": str(rec.size),
                },
            }
            s3_client.upload_fileobj(
                Fileobj=stream,
                Bucket=bucket,
                Key=key,
                ExtraArgs=extra_args,
                Config=transfer_config,
            )
            head = s3_client.head_object(Bucket=bucket, Key=key)
            return head.get("ETag", "")
        except (ApiError, HttpError, botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError) as exc:
            if attempt >= retries:
                raise
            LOGGER.warning("Retrying %s (attempt %s/%s): %s", rec.path_display, attempt + 1, retries, exc)
            retry_sleep(attempt)
        finally:
            if stream:
                stream.close()
    raise RuntimeError("unreachable")


def validate_clients(dbx: dropbox.Dropbox, s3_client, bucket: str) -> None:
    dbx.users_get_current_account()
    s3_client.head_bucket(Bucket=bucket)


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    dbx = dropbox.Dropbox(args.dropbox_token, timeout=900)
    s3_client = boto3.client("s3", region_name=args.region)

    transfer_config = TransferConfig(
        multipart_threshold=args.multipart_chunk_mb * 1024 * 1024,
        multipart_chunksize=args.multipart_chunk_mb * 1024 * 1024,
        max_concurrency=args.max_concurrency,
        use_threads=True,
    )

    db = CheckpointDB(args.checkpoint_db)
    try:
        validate_clients(dbx, s3_client, args.bucket)

        discovered = 0
        transferred = 0
        skipped = 0

        for rec in iter_dropbox_files(dbx, args.dropbox_root):
            discovered += 1
            db.upsert_discovered(rec)

            if db.already_done(rec):
                skipped += 1
                if skipped % 500 == 0:
                    LOGGER.info("Skipped already-transferred files: %s", skipped)
                continue

            key = s3_key_for(args.prefix, rec.path_display)
            LOGGER.info("Transferring %s (%s bytes) -> s3://%s/%s", rec.path_display, rec.size, args.bucket, key)
            try:
                etag = transfer_file(
                    dbx=dbx,
                    s3_client=s3_client,
                    transfer_config=transfer_config,
                    rec=rec,
                    bucket=args.bucket,
                    key=key,
                    storage_class=args.storage_class,
                    retries=args.retries,
                )
                db.mark_done(rec, etag)
                transferred += 1
            except Exception as exc:
                db.mark_failed(rec, str(exc))
                LOGGER.exception("Failed transferring %s: %s", rec.path_display, exc)

            if (transferred + skipped) % 50 == 0:
                db.commit()
                LOGGER.info(
                    "Progress: discovered=%s transferred=%s skipped=%s",
                    discovered,
                    transferred,
                    skipped,
                )

        db.commit()
        LOGGER.info(
            "Finished. discovered=%s transferred=%s skipped=%s checkpoint=%s",
            discovered,
            transferred,
            skipped,
            os.path.abspath(args.checkpoint_db),
        )
    except AuthError as exc:
        LOGGER.error("Authentication failed: %s", exc)
        return 2
    finally:
        db.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
