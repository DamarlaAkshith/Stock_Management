import psycopg2


def set_connection():
    try:
        conn = psycopg2.connect(
            host="172.16.1.236",
            port="5432",
            database="bctst",
            user="akshith",
            password="akshith"
        )
        cur = conn.cursor()
        print("database connected")
        return cur, conn
    except (Exception, psycopg2.Error) as error:
        print("Failed connected due to: ", error)
        return None, None
