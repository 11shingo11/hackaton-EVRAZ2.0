import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="hackaton",
    user="newuser",
    password="1234",
    port="5432"
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM real_time")
rows = cursor.fetchall()

for row in rows:
    print(row)

cursor.close()
conn.close()
