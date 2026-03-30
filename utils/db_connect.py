import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """
    Returns a new PostgreSQL connection using credentials from .env.
    """
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", 5432),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def get_dict_connection():
    """
    Returns a connection whose cursors return rows as dicts
    (column name → value) instead of plain tuples.

    Useful in scoring and reporting scripts where named access
    is cleaner than positional indexing.
    """
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", 5432),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        cursor_factory=RealDictCursor,
    )
    return conn


def test_connection():
    """
    Prints server version if connection succeeds.
    """
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
            print(f"[db_connect] Connection successful.")
            print(f"[db_connect] PostgreSQL version: {version}")
        conn.close()
    except Exception as e:
        print(f"[db_connect] Connection failed: {e}")
        raise


if __name__ == "__main__":
    test_connection()
