from datetime import datetime, timezone

def parse_envelope(value):
    """Debezium envelope -> (op, row). Tombstones (null value) -> (None, None)."""
    if value is None:
        return None, None
    p = value["payload"]
    op = p["op"]
    return op, (p["before"] if op == "d" else p["after"])

def to_bq_row(row):
    """Debezium row dict -> BigQuery STRUCT dict (keys must match table fields)."""
    ts = row.get("updated_at")
    updated = datetime.fromtimestamp(ts / 1000, tz=timezone.utc) if isinstance(ts, (int, float)) else None
    return {
        "claim_id": row.get("claim_id"),
        "member_id": row.get("member_id"),
        "provider_id": row.get("provider_id"),
        "diagnosis_code": row.get("diagnosis_code"),
        "amount": float(row["amount"]) if row.get("amount") is not None else None,
        "status": row.get("status"),
        "updated_at": updated.isoformat() if updated is not None else None,
        "place_of_service": row.get("place_of_service"),
    }



STRUCT_TYPE = ("STRUCT<claim_id INT64, member_id STRING, provider_id STRING, "
               "diagnosis_code STRING, amount FLOAT64, status STRING, updated_at TIMESTAMP>")
