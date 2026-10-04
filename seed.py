import random
import psycopg2
from datetime import datetime, timedelta, timezone

conn = psycopg2.connect(host="localhost", port=5433, dbname="claimsdb",
                        user="postgres", password="postgres")
cur = conn.cursor()
DIAG = ["J06.9", "I10", "E11.9", "M54.5", "F32.9", "R51", "K21.9"]
now = datetime.now(timezone.utc)
rows = [(
    f"M{random.randint(1, 2000):05d}",
    f"P{random.randint(1, 300):04d}",
    random.choice(DIAG),
    round(random.uniform(25, 9000), 2),
    random.choices(["pending", "approved", "denied", "paid"], weights=[3, 4, 1, 2])[0],
    now - timedelta(days=random.randint(0, 90)),
) for _ in range(10000)]
cur.executemany(
    "INSERT INTO claims (member_id, provider_id, diagnosis_code, amount, status, updated_at)"
    " VALUES (%s,%s,%s,%s,%s,%s)", rows)
conn.commit()
print("seeded", cur.rowcount)
cur.close(); conn.close()
