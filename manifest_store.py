"""The bucket-root `manifest.json` index: read it honestly, merge it safely.

WHY this module: `manifest.json` has two owners with different fields. This
pipeline owns `dates` (the trading-day index the site's Historical tab selects
from); the live collector owns `weekly_expirations` (which hourly weekly
archives exist). Both used to read the whole object, change their own field and
write it back unconditionally, so whichever wrote second discarded the other's
field.

The worse half was here: `load_manifest` caught *every* exception and returned
`{"dates": []}`, so one failed GET on a timeout or a 5xx was written back over
the real document, deleting the whole date index, the `note` and every weekly
reference in a single request. No concurrency was required.

The rule now is compare-and-swap. Read the object and its ETag, merge only your
own field while carrying every other key through verbatim, then write with
`IfMatch` (or `IfNoneMatch: "*"` when creating). A lost race is a conflict to
re-read, not a document to overwrite.
"""

import json
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

MANIFEST_KEY = "manifest.json"

# Five attempts with sub-second backoff: the contention window is a single
# read-modify-write in another process, not a queue.
MANIFEST_ATTEMPTS = 5
MANIFEST_BACKOFF_BASE_SECS = 0.1
MANIFEST_BACKOFF_CAP_SECS = 1.1

_MISSING_CODES = {"NoSuchKey", "NotFound", "404"}
_CONFLICT_CODES = {"PreconditionFailed", "ConditionalRequestConflict", "412", "409"}


class ManifestError(RuntimeError):
    """Base class for manifest failures that must not be written around."""


class ManifestUnreadable(ManifestError):
    """The object could not be read faithfully. Never treated as empty."""


class ManifestDamaged(ManifestError):
    """The object is missing a key its published contract requires."""


class ManifestConflict(ManifestError):
    """Another writer changed the object between our read and our write."""


class ConditionalWriteUnsupported(ManifestError):
    """This client cannot send a precondition, so no safe write is possible."""


def _error_code(exc: BaseException) -> Optional[str]:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        error = response.get("Error")
        if isinstance(error, dict) and error.get("Code") is not None:
            return str(error["Code"])
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status is not None:
            return str(status)
    for name in ("code", "status"):
        value = getattr(exc, name, None)
        if value is not None:
            return str(value)
    return None


def _is_missing(exc: BaseException) -> bool:
    if _error_code(exc) in _MISSING_CODES:
        return True
    return "NoSuchKey" in f"{type(exc).__name__} {exc}"


def _is_conflict(exc: BaseException) -> bool:
    return _error_code(exc) in _CONFLICT_CODES


def assert_conditional_writes_supported(client) -> None:
    """Fail before a write if this client cannot send a precondition.

    `PutObject` gained `IfMatch`/`IfNoneMatch` after the `boto3>=1.34` floor
    this repository pins, so the capability is checked rather than assumed. A
    client with no service model (a test double) is trusted to model the
    semantics it implements; the concurrency suite is what proves it does.
    """
    service_model = getattr(getattr(client, "meta", None), "service_model", None)
    if service_model is None:
        return
    try:
        members = service_model.operation_model("PutObject").input_shape.members
    except Exception as exc:
        raise ConditionalWriteUnsupported(f"cannot inspect PutObject shape: {exc}") from exc
    if "IfMatch" not in members or "IfNoneMatch" not in members:
        raise ConditionalWriteUnsupported(
            "this botocore cannot send PutObject preconditions (IfMatch/IfNoneMatch); "
            "refusing to write manifest.json unconditionally"
        )


def read_manifest(client, bucket: str) -> tuple[Optional[dict], Optional[str]]:
    """Return `(document, etag)`, or `(None, None)` when the object is absent.

    An absent index is a legitimate first run. An unreadable one is not: a
    timeout or a 5xx raises `ManifestUnreadable` so the caller cannot mistake it
    for an empty index and publish that.
    """
    try:
        response = client.get_object(Bucket=bucket, Key=MANIFEST_KEY)
    except Exception as exc:
        if _is_missing(exc):
            return None, None
        raise ManifestUnreadable(f"manifest read failed: {exc}") from exc
    body = response["Body"].read()
    etag = response.get("ETag")
    if not etag:
        raise ManifestUnreadable("manifest read returned no ETag; a write could not be conditional")
    try:
        document = json.loads(body.decode() if isinstance(body, (bytes, bytearray)) else body)
    except Exception as exc:
        raise ManifestUnreadable(f"manifest is not valid JSON: {exc}") from exc
    if not isinstance(document, dict):
        raise ManifestUnreadable("manifest is not a JSON object")
    return document, etag


def write_manifest(client, document: dict, etag: Optional[str],
                   bucket: str) -> Optional[str]:
    """Write `document` only if the object is still the version we read.

    An `etag` of None means "we read absence", which becomes a create-only
    write so two simultaneous creators cannot both claim the index.
    """
    assert_conditional_writes_supported(client)
    options: dict[str, Any] = {
        "Bucket": bucket,
        "Key": MANIFEST_KEY,
        "Body": json.dumps(document, indent=2, sort_keys=True).encode(),
        "ContentType": "application/json",
    }
    if etag:
        options["IfMatch"] = etag
    else:
        options["IfNoneMatch"] = "*"
    try:
        response = client.put_object(**options)
    except Exception as exc:
        if _is_conflict(exc):
            raise ManifestConflict(f"manifest changed since it was read: {exc}") from exc
        raise
    return (response or {}).get("ETag") if isinstance(response, dict) else None


def _backoff(attempt: int) -> float:
    return min(MANIFEST_BACKOFF_BASE_SECS * (2 ** attempt), MANIFEST_BACKOFF_CAP_SECS)


def update_manifest(
    client,
    mutate: Callable[[dict], dict],
    bucket: str,
    *,
    seed: Optional[dict] = None,
    attempts: int = MANIFEST_ATTEMPTS,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict:
    """Read-merge-CAS with bounded retry against a concurrent writer.

    `mutate` receives the freshly read document and returns the document to
    write. It must change only the caller's own field: everything it does not
    touch -- `weekly_expirations` for the collector, `note`, and any field added
    later -- travels through untouched.

    `seed` is the document to start from when the object is absent. It is never
    used when the object exists, however damaged it looks: a present document is
    mutated, or refused, never replaced.
    """
    last_error: Optional[BaseException] = None
    for attempt in range(attempts):
        document, etag = read_manifest(client, bucket)
        base = dict(document) if document is not None else dict(seed or {})
        next_document = mutate(base)
        if not isinstance(next_document, dict):
            raise ManifestError("mutate() must return the document to write")
        try:
            write_manifest(client, next_document, etag, bucket)
            return next_document
        except ManifestConflict as exc:
            last_error = exc
            if attempt < attempts - 1:
                sleeper(_backoff(attempt))
    raise ManifestConflict(f"manifest update conflicted after {attempts} attempts: {last_error}")


def empty_manifest_seed() -> dict:
    """A first-run document: an explicit empty date index."""
    return {"dates": []}


def add_date(document: dict, date_str: str) -> dict:
    """Union one trading day into `dates`.

    `dates` is required, not defaulted. A document that exists without it is a
    damaged index: writing `[date_str]` over it would publish a one-date history
    that looks valid and has lost every earlier day. Refuse, and let the index
    be rebuilt from the `raw/qqq_chain_*.csv` listing.
    """
    if "dates" not in document:
        raise ManifestDamaged(
            "manifest has no 'dates' key; refusing to replace the date index with one date"
        )
    dates = set(document.get("dates") or [])
    dates.add(date_str)
    document["dates"] = sorted(dates)
    # Preserved contract, not freshness: `updated_at` records which writer
    # touched the object last. The architecture note already says it is not the
    # date index's freshness, and keeping it stamped avoids a semantic drift.
    document["updated_at"] = datetime.now(timezone.utc).isoformat()
    return document


__all__ = [
    "MANIFEST_KEY",
    "MANIFEST_ATTEMPTS",
    "ManifestError",
    "ManifestUnreadable",
    "ManifestDamaged",
    "ManifestConflict",
    "ConditionalWriteUnsupported",
    "assert_conditional_writes_supported",
    "read_manifest",
    "write_manifest",
    "update_manifest",
    "empty_manifest_seed",
    "add_date",
]
