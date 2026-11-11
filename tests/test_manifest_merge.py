"""The daily writer's half of the `manifest.json` merge protocol.

WHY this matters here: this repository's `load_manifest` used to catch every
exception and return `{"dates": []}`, and the caller wrote that back over the
real object. One failed GET on a timeout or a 5xx therefore deleted the whole
trading-day index, the published `note` and every weekly reference the collector
had added -- no concurrency required. The other half is the race: the collector
owns `weekly_expirations` in the same document, and an unconditional
read-modify-write discards whichever field lost.

These tests drive `manifest_store` directly (no network, no R2) against a stub
that keeps a version per object and rejects a stale precondition.
"""

import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import manifest_store as ms  # noqa: E402

BUCKET = "bucket"
TODAY = "2026-11-11"
EXPIRY = "2026-11-13"
WEEKLY_KEY = f"raw/weekly/20261113/qqq_chain_20261111_093000000000.csv"


class FakeBody:
    def __init__(self, payload):
        self.payload = payload

    def read(self):
        return self.payload


class MissingKey(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class PreconditionFailed(Exception):
    response = {"Error": {"Code": "PreconditionFailed"}}


class Unavailable(Exception):
    response = {"Error": {"Code": "InternalError"}}


class R2Stub:
    """One object, versioned ETags, real preconditions, injectable race window."""

    def __init__(self, document=None):
        self.document = None
        self.etag = None
        self.version = 0
        self.writes = []
        self.reads = 0
        self.conflicts = 0
        self.fail_reads_with = None
        self.before_write = None
        if document is not None:
            self._store(document)

    def _store(self, document):
        self.version += 1
        self.document = json.loads(json.dumps(document))
        self.etag = f'"{self.version}"'
        return {"ETag": self.etag}

    def get_object(self, Bucket, Key):  # noqa: N803
        self.reads += 1
        if self.fail_reads_with is not None:
            raise self.fail_reads_with
        if self.document is None:
            raise MissingKey(Key)
        return {"Body": FakeBody(json.dumps(self.document).encode()), "ETag": self.etag}

    def put_object(self, Bucket, Key, Body, **kwargs):  # noqa: N803
        if self.before_write is not None:
            self.before_write()
        payload = json.loads(Body.decode() if isinstance(Body, bytes) else Body)
        expected = kwargs.get("IfMatch")
        create_only = kwargs.get("IfNoneMatch")
        if expected is not None and expected != self.etag:
            self.conflicts += 1
            raise PreconditionFailed(f"IfMatch {expected} != {self.etag}")
        if create_only == "*" and self.etag is not None:
            self.conflicts += 1
            raise PreconditionFailed("object already exists")
        self.writes.append(payload)
        return self._store(payload)


def daily_write(stub, date_str, **kwargs):
    return ms.update_manifest(
        stub, lambda document: ms.add_date(document, date_str), BUCKET,
        seed=ms.empty_manifest_seed(), **kwargs,
    )


def collector_write(stub, expiry=EXPIRY, key=WEEKLY_KEY, **kwargs):
    """The collector's merge, as the other repository implements it."""

    def merge(document):
        weekly = document.setdefault("weekly_expirations", {})
        keys = weekly.setdefault(expiry, [])
        if key not in keys:
            keys.append(key)
            keys.sort()
        return document

    return ms.update_manifest(stub, merge, BUCKET, seed={"dates": [], "weekly_expirations": {}},
                              **kwargs)


class ManifestMergeTests(unittest.TestCase):
    def test_absent_index_is_created_with_an_explicit_date_list(self):
        stub = R2Stub()
        self.assertEqual(ms.read_manifest(stub, BUCKET), (None, None))
        written = daily_write(stub, TODAY)
        self.assertEqual(written["dates"], [TODAY])
        self.assertIn("updated_at", written)
        self.assertEqual(len(stub.writes), 1)

    def test_failed_read_aborts_instead_of_publishing_an_empty_index(self):
        original = {"dates": ["2026-11-10", TODAY], "note": "keep me",
                    "weekly_expirations": {EXPIRY: [WEEKLY_KEY]}}
        stub = R2Stub(original)
        stub.fail_reads_with = Unavailable("storage said no")

        with self.assertRaises(ms.ManifestUnreadable):
            daily_write(stub, TODAY)
        self.assertEqual(stub.document, original, "the real index is untouched")
        self.assertEqual(stub.writes, [])

        # Failing-before witness: the old reader returned {"dates": []} for any
        # exception and the caller PUT it over the real object.
        self.assertNotEqual({"dates": []}, original)

    def test_damaged_index_is_refused_rather_than_replaced_with_one_date(self):
        broken = {"weekly_expirations": {EXPIRY: [WEEKLY_KEY]}, "note": "no dates"}
        stub = R2Stub(broken)
        with self.assertRaises(ms.ManifestDamaged):
            daily_write(stub, TODAY)
        self.assertEqual(stub.document, broken)
        self.assertEqual(stub.writes, [])

    def test_corrupt_json_is_unreadable_not_empty(self):
        class Corrupt(R2Stub):
            def get_object(self, Bucket, Key):  # noqa: N803
                return {"Body": FakeBody(b'{"dates": ["2026-11-10"'), "ETag": '"1"'}

        stub = Corrupt()
        with self.assertRaises(ms.ManifestUnreadable):
            daily_write(stub, TODAY)
        self.assertEqual(stub.writes, [], "no write follows an unreadable document")

    def test_conflict_shapes_are_all_recognised_and_retried(self):
        """A lost precondition must be recognised however the client reports it.

        R2's SDK path surfaces `Error.Code`, but boto3 also synthesises failures
        from the HTTP status alone, and a client reset can leave only a
        `.status` attribute. The classifier has three fallbacks; if any is
        dropped the exception propagates as itself, the caller stops retrying
        and the index write fails instead of merging.
        """

        class CodeOnly(Exception):
            response = {"Error": {"Code": "PreconditionFailed"}}

        class HttpStatusOnly(Exception):
            response = {"ResponseMetadata": {"HTTPStatusCode": 412}}

        class AttrStatusOnly(Exception):
            status = 412

        class AttrCodeOnly(Exception):
            code = 409

        class PlainConflict(Exception):
            response = {"Error": {"Code": "ConditionalRequestConflict"}}

        shapes = [
            ("Error.Code PreconditionFailed", CodeOnly),
            ("Error.Code ConditionalRequestConflict", PlainConflict),
            ("ResponseMetadata 412", HttpStatusOnly),
            ("status attribute 412", AttrStatusOnly),
            ("code attribute 409", AttrCodeOnly),
        ]
        for label, factory in shapes:
            stub = R2Stub({"dates": ["2026-11-10"], "weekly_expirations": {}})

            def _raise(*_args, _factory=factory, **_kwargs):
                raise _factory(f"injected {label}")

            stub.put_object = _raise
            with self.assertRaises(ms.ManifestConflict):
                daily_write(stub, TODAY, sleeper=lambda _: None)
            self.assertEqual(stub.reads, ms.MANIFEST_ATTEMPTS, label)

        class ServerError(Exception):
            response = {"Error": {"Code": "InternalError"}}

        stub = R2Stub({"dates": ["2026-11-10"], "weekly_expirations": {}})

        def _boom(*_args, **_kwargs):
            raise ServerError("boom")

        stub.put_object = _boom
        with self.assertRaises(ServerError):
            daily_write(stub, TODAY, sleeper=lambda _: None)
        self.assertEqual(stub.reads, 1, "a 5xx gets exactly one attempt")

    def test_protocol_constants_are_pinned(self):
        """The two repositories keep this protocol as a copy, not an import.

        A one-sided edit to the retry budget or the conflict set would be
        invisible to the other repository's suite, so the values are asserted
        here and in the collector's copy. Changing one without the other is then
        a deliberate act with two failing tests, not a silent divergence.
        """
        self.assertEqual(ms.MANIFEST_ATTEMPTS, 5)
        self.assertEqual(ms.MANIFEST_BACKOFF_BASE_SECS, 0.1)
        self.assertEqual(ms.MANIFEST_BACKOFF_CAP_SECS, 1.1)

    def test_daily_then_weekly_preserves_the_new_date(self):
        stub = R2Stub({"dates": ["2026-11-10"], "weekly_expirations": {}})
        landed = {"done": False}

        def collector_lands_mid_flight():
            if not landed["done"]:
                landed["done"] = True
                stub._store({"dates": ["2026-11-10"],
                             "weekly_expirations": {EXPIRY: [WEEKLY_KEY]}})

        stub.before_write = collector_lands_mid_flight
        try:
            daily_write(stub, TODAY)
        except ms.ManifestConflict:
            self.fail("the daily writer must retry a lost race, not give up")
        self.assertEqual(stub.conflicts, 1)
        self.assertEqual(stub.document["dates"], ["2026-11-10", TODAY], "dates kept")
        self.assertEqual(stub.document["weekly_expirations"][EXPIRY], [WEEKLY_KEY])

    def test_weekly_then_daily_preserves_the_weekly_reference(self):
        stub = R2Stub({"dates": ["2026-11-10"], "weekly_expirations": {}})
        landed = {"done": False}

        def daily_lands_mid_flight():
            if not landed["done"]:
                landed["done"] = True
                stub._store({"dates": ["2026-11-10", TODAY], "weekly_expirations": {}})

        stub.before_write = daily_lands_mid_flight
        final = collector_write(stub)
        self.assertEqual(stub.conflicts, 1)
        self.assertEqual(final["dates"], ["2026-11-10", TODAY], "the daily date survives")
        self.assertEqual(final["weekly_expirations"][EXPIRY], [WEEKLY_KEY])

    def test_retry_exhaustion_raises_without_writing(self):
        stub = R2Stub({"dates": ["2026-11-10"]})
        sleeps = []
        stub.before_write = lambda: stub._store(dict(stub.document))

        with self.assertRaises(ms.ManifestConflict):
            daily_write(stub, TODAY, sleeper=sleeps.append)
        self.assertEqual(stub.conflicts, ms.MANIFEST_ATTEMPTS)
        self.assertEqual(len(sleeps), ms.MANIFEST_ATTEMPTS - 1)
        self.assertEqual(stub.document, {"dates": ["2026-11-10"]})

    def test_unknown_top_level_keys_survive(self):
        original = {"dates": ["2026-11-10"], "note": "D-date files",
                    "metadata": {"keep": True}, "weekly_expirations": {"x": ["k.csv"]}}
        stub = R2Stub(original)
        final = daily_write(stub, TODAY)
        self.assertEqual(final["note"], original["note"])
        self.assertEqual(final["metadata"], {"keep": True})
        self.assertEqual(final["weekly_expirations"], original["weekly_expirations"])

    def test_client_without_conditional_put_refuses_to_write(self):
        class Shape:
            def __init__(self, members):
                self.members = members

        class ServiceModel:
            def __init__(self, members):
                self.members = members

            def operation_model(self, name):
                members = self.members

                class Op:
                    class input_shape:
                        pass

                Op.input_shape = Shape(members)
                return Op()

        class Meta:
            def __init__(self, members):
                self.service_model = ServiceModel(members)

        stub = R2Stub({"dates": [TODAY]})
        stub.meta = Meta({"Bucket", "Key", "Body"})
        with self.assertRaises(ms.ConditionalWriteUnsupported):
            daily_write(stub, TODAY)
        self.assertEqual(stub.writes, [])

        stub.meta = Meta({"Bucket", "Key", "Body", "IfMatch", "IfNoneMatch"})
        self.assertEqual(daily_write(stub, TODAY)["dates"], [TODAY])

    def test_interleaved_mixed_writers_converge_on_the_union(self):
        import random

        rng = random.Random(20261111)
        for _ in range(20):
            stub = R2Stub({"dates": [], "weekly_expirations": {"old": ["keep.csv"]}})
            expected_dates = set()
            expected_weekly = {}

            def other_writer():
                if rng.random() < 0.5:
                    stub._store(json.loads(json.dumps(stub.document)))

            for round_index in range(6):
                stub.before_write = other_writer
                if rng.random() < 0.5:
                    day = f"2026-11-{10 + round_index:02d}"
                    expected_dates.add(day)
                    try:
                        daily_write(stub, day, sleeper=lambda _: None)
                    except ms.ManifestConflict:
                        expected_dates.discard(day)
                else:
                    expiry = f"2026-11-{20 + round_index:02d}"
                    key = f"raw/weekly/{expiry.replace('-', '')}/k{round_index}.csv"
                    expected_weekly.setdefault(expiry, []).append(key)
                    try:
                        collector_write(stub, expiry, key, sleeper=lambda _: None)
                    except ms.ManifestConflict:
                        expected_weekly[expiry].remove(key)

            self.assertEqual(set(stub.document["dates"]), expected_dates)
            for expiry, keys in expected_weekly.items():
                self.assertEqual(sorted(keys), stub.document["weekly_expirations"].get(expiry))
            self.assertEqual(stub.document["weekly_expirations"]["old"], ["keep.csv"])


if __name__ == "__main__":
    unittest.main()
