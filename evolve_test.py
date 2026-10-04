import psycopg2
conn = psycopg2.connect(host="localhost", port=5433, dbname="claimsdb",
                        user="postgres", password="postgres")
cur = conn.cursor()
cur.execute("ALTER TABLE claims ADD COLUMN place_of_service VARCHAR(10)")
conn.commit()
print("column added")
cur.close(); conn.close()
