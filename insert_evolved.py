import psycopg2
conn = psycopg2.connect(host="localhost", port=5433, dbname="claimsdb",
                        user="postgres", password="postgres")
cur = conn.cursor()
cur.execute("INSERT INTO claims (member_id, provider_id, diagnosis_code, amount, status, updated_at, place_of_service) VALUES ('M99999', 'P0999', 'J06.9', 150.00, 'pending', now(), 'office')")
conn.commit()
print("inserted")
cur.close(); conn.close()
