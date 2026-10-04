\# Claims CDC Pipeline



Stream every change from a Postgres `claims` table into BigQuery within seconds — no batch jobs, no polling the source database.



\## Architecture

Postgres ──WAL──▶ Debezium ──▶ Kafka ──▶ Python consumer ──▶ BigQuery

(claims (CDC (topic: (MERGE/DELETE, (reporting

table) engine) dbserver1. idempotent sink) warehouse)

public.claims)









\## Why CDC instead of batch ETL?



Batch extraction hammers the source database with heavy `SELECT \*` scans and delivers data hours late. Change Data Capture reads Postgres's write-ahead log — the journal it already keeps for crash recovery — and streams each committed change as an event. The source database barely notices, and BigQuery lags by seconds, not hours.



\## How it works



1\. \*\*Postgres\*\* holds the `claims` table (10k seeded rows). Every write hits the WAL first.

2\. \*\*Debezium\*\* (running in Kafka Connect) tails the WAL through a replication slot. On startup it snapshots the existing rows (`op: r` events), then streams live changes.

3\. \*\*Kafka\*\* shelves the change events in the `dbserver1.public.claims` topic, so the consumer can read at its own pace and replay after failures.

4\. \*\*The Python consumer\*\* batches events, collapses them to last-write-wins per `claim\_id`, bulk-loads each batch into a staging table, and applies it with MERGE (upserts) and DELETE. Kafka offsets commit only after BigQuery confirms.



\## The change envelope



Every Debezium event looks like this:



```json

{"payload": {"before": {...}, "after": {...}, "op": "c",

&#x20; "source": {"db": "claimsdb", "table": "claims"}, "ts\_ms": 1728000000000}}

op codes: r = snapshot read, c = insert, u = update, d = delete. Deletes carry the row in before; everything else uses after. After a delete, Debezium also emits a tombstone (null value) — a log-compaction marker for Kafka, not data; the sink skips it.



Exactly-once delivery

Debezium delivers at-least-once (it may redeliver after a restart). The sink makes that safe:



Idempotent writes — MERGE-by-PK and DELETE-by-PK applied twice give the same result as once.

Commit-after-write — Kafka offsets advance only after BigQuery confirms the flush. A crash mid-batch replays harmlessly.

Tests

Schema evolution — ran ALTER TABLE claims ADD COLUMN place\_of\_service mid-stream. Debezium picked up the DDL automatically; the sink reads new fields with .get() defaults so old rows don't break. Verified the new column flowing into BigQuery.



Tombstone deletes — deleted a row in Postgres; the op: d envelope drove a BigQuery DELETE and the trailing tombstone was correctly ignored.



Unit tests — python -m pytest tests/ -v covers envelope parsing for all op codes, tombstone handling, type conversion, and missing-key defaults.



Project structure





├── docker-compose.yml   # Postgres (wal\_level=logical) + Kafka + Kafka Connect

├── init.sql             # claims table + REPLICA IDENTITY FULL

├── connector.json       # Debezium Postgres connector config

├── seed.py              # generates 10k fake claims

├── sink/

│   ├── envelope.py      # parsing + BigQuery row conversion (pure functions)

│   └── consumer.py      # Kafka → BigQuery sink (micro-batch, idempotent)

└── tests/

&#x20;   └── test\_envelope.py

Run it yourself

Prereqs: Docker, Python 3.11+, a GCP project with billing enabled (DML isn't allowed on the no-card tier).



bash





pip install psycopg2-binary google-cloud-bigquery kafka-python pytest

docker compose up -d

python seed.py

curl -X POST -H "Content-Type: application/json" --data @connector.json http://localhost:8083/connectors

\# GCP: create dataset `claims`, the tables, a service account with

\# BigQuery Data Editor + Job User, save the key as gcp-key.json (gitignored)

setx GOOGLE\_APPLICATION\_CREDENTIALS "<path>\\gcp-key.json"   # fresh terminal after

python -m sink.consumer

Tech stack

Python, PostgreSQL, Debezium, Kafka, Kafka Connect, BigQuery, Docker





