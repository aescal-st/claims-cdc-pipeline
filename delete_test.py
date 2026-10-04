import psycopg2
conn = psycopg2.connect(host="localhost", port=5433, dbname="claimsdb",
                        user="postgres", password="postgres")
cur = conn.cursor()
cur.execute("DELETE FROM claims WHERE claim_id = 7")
conn.commit()
print("deleted", cur.rowcount)
cur.close(); conn.close()
