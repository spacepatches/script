import os
import psycopg2

try:
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        sslmode="require",
        connect_timeout=10
    )

    cur = conn.cursor()
    cur.execute("SELECT NOW();")  # semplice query di test
    now = cur.fetchone()[0]
    print(f"✅ Connessione OK! Database server time: {now}")

    cur.close()
    conn.close()

except Exception as e:
    print(f"❌ Connessione FALLITA: {e}")
