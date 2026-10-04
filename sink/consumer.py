import json
import time
from kafka import KafkaConsumer
from google.cloud import bigquery
from sink.envelope import parse_envelope, to_bq_row

PROJECT = "claims-cdc-pipeline"
TABLE_ID = f"{PROJECT}.claims.claims"
STAGING_TABLE_ID = f"{PROJECT}.claims.claims_staging"
TOPIC = "dbserver1.public.claims"

bq = bigquery.Client(project=PROJECT)
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",  # first run reads the snapshot (op=r)
    enable_auto_commit=False,      # commit only after BigQuery confirms
    group_id="bq-sink-v1",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")) if b is not None else None,
)


def flush(batch):
    latest = {}
    for op, row in batch:
        if op is None:
            continue  # tombstone: compaction marker, skip
        latest[row["claim_id"]] = (op, row)  # last write wins per key
    upserts = [to_bq_row(r) for op, r in latest.values() if op != "d"]
    deletes = [r["claim_id"] for op, r in latest.values() if op == "d"]

    if upserts:
        # Stage the batch (bulk load), then merge — the classic CDC sink pattern.
        bq.load_table_from_json(
            upserts,
            STAGING_TABLE_ID,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                schema=[
                    bigquery.SchemaField("claim_id", "INT64"),
                    bigquery.SchemaField("member_id", "STRING"),
                    bigquery.SchemaField("provider_id", "STRING"),
                    bigquery.SchemaField("diagnosis_code", "STRING"),
                    bigquery.SchemaField("amount", "FLOAT64"),
                    bigquery.SchemaField("status", "STRING"),
                    bigquery.SchemaField("updated_at", "TIMESTAMP"),
                    bigquery.SchemaField("place_of_service", "STRING"),
                ],
            ),
        ).result()
        bq.query(
            f"""MERGE `{TABLE_ID}` T
                USING `{STAGING_TABLE_ID}` S
                ON T.claim_id = S.claim_id
                WHEN MATCHED THEN UPDATE SET
                  member_id = S.member_id, provider_id = S.provider_id,
                  diagnosis_code = S.diagnosis_code, amount = S.amount,
                  status = S.status, updated_at = S.updated_at,
                  place_of_service = S.place_of_service
                WHEN NOT MATCHED THEN INSERT
                  (claim_id, member_id, provider_id, diagnosis_code, amount, status, updated_at,
                   place_of_service)
                VALUES
                  (S.claim_id, S.member_id, S.provider_id, S.diagnosis_code,
                   S.amount, S.status, S.updated_at, S.place_of_service)"""
        ).result()
        print(f"merged {len(upserts)}", flush=True)

    if deletes:
        bq.query(
            f"DELETE FROM `{TABLE_ID}` WHERE claim_id IN UNNEST(@ids)",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ArrayQueryParameter("ids", "INT64", deletes)]),
        ).result()
        print(f"deleted {len(deletes)}", flush=True)


batch, last = [], time.time()
for msg in consumer:
    batch.append(parse_envelope(msg.value))
    if len(batch) >= 500 or time.time() - last > 10:
        if batch:
            flush(batch)       # BigQuery confirms first...
            consumer.commit()  # ...then the bookmark moves
        batch, last = [], time.time()
