import psycopg2
conn = psycopg2.connect(host="localhost", port=5433, dbname="claimsdb",
                        user="postgres", password="postgres")
cur = conn.cursor()
cur.execute("UPDATE claims SET status='approved' WHERE claim_id=42")
conn.commit()
print("updated", cur.rowcount)
cur.close(); conn.close()
