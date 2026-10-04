import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sink.envelope import parse_envelope, to_bq_row


def make_envelope(op, before=None, after=None):
    return {"payload": {"before": before, "after": after, "op": op,
                        "source": {"db": "claimsdb", "table": "claims"},
                        "ts_ms": 1728000000000}}


ROW = {"claim_id": 42, "member_id": "M00123", "provider_id": "P0045",
       "diagnosis_code": "J06.9", "amount": 250.75, "status": "pending",
       "updated_at": 1728000000000}


def test_insert_uses_after():
    op, row = parse_envelope(make_envelope("c", before=None, after=ROW))
    assert op == "c" and row["claim_id"] == 42


def test_snapshot_read_uses_after():
    op, row = parse_envelope(make_envelope("r", before=None, after=ROW))
    assert op == "r" and row["member_id"] == "M00123"


def test_update_uses_after():
    updated = dict(ROW, status="approved")
    op, row = parse_envelope(make_envelope("u", before=ROW, after=updated))
    assert op == "u" and row["status"] == "approved"


def test_delete_uses_before():
    op, row = parse_envelope(make_envelope("d", before=ROW, after=None))
    assert op == "d" and row["claim_id"] == 42


def test_tombstone_returns_nones():
    assert parse_envelope(None) == (None, None)


def test_to_bq_row_converts_types():
    bq_row = to_bq_row(ROW)
    assert bq_row["claim_id"] == 42
    assert isinstance(bq_row["amount"], float)
    assert bq_row["updated_at"].startswith("2024-")


def test_to_bq_row_missing_keys_default_none():
    bq_row = to_bq_row({"claim_id": 1})
    assert bq_row["member_id"] is None and bq_row["updated_at"] is None
