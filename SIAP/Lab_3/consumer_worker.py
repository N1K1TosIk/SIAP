import json
import re
import psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import execute_batch
from psycopg2 import sql

from .config import KAFKA_SERVER, KAFKA_TOPIC, POSTGRES_URL


def sanitize_identifier(name: str) -> str:
    """Оставляем только буквы/цифры/подчёркивание, остальное -> '_'."""
    safe = re.sub(r"[^0-9a-zA-Z_]", "_", name)
    if not safe:
        safe = "t"
    if safe[0].isdigit():
        safe = f"t_{safe}"
    return safe


def ensure_table(cur, table_name: str, columns: list[str]) -> None:
    safe_table = sanitize_identifier(table_name)
    safe_cols = [sanitize_identifier(c) for c in columns]

    # создаём таблицу, если нет
    col_defs = [sql.SQL("{} TEXT").format(sql.Identifier(c)) for c in safe_cols]
    create_stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({});").format(
        sql.Identifier(safe_table),
        sql.SQL(", ").join(col_defs)
    )
    cur.execute(create_stmt)


def insert_rows(cur, table_name: str, columns: list[str], rows: list[list[str]]) -> None:
    safe_table = sanitize_identifier(table_name)
    safe_cols = [sanitize_identifier(c) for c in columns]

    insert_stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(safe_table),
        sql.SQL(", ").join(map(sql.Identifier, safe_cols)),
        sql.SQL(", ").join(sql.Placeholder() * len(safe_cols))
    )
    execute_batch(cur, insert_stmt, rows, page_size=100)


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=True,
        auto_offset_reset="earliest"
    )

    conn = psycopg2.connect(POSTGRES_URL)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            for msg in consumer:
                try:
                    payload = msg.value
                    table_name = payload.get("table_name")
                    columns = payload.get("columns") or []
                    data = payload.get("data") or []
                    if not table_name or not columns or not data:
                        continue

                    ensure_table(cur, table_name, columns)
                    insert_rows(cur, table_name, columns, data)
                    conn.commit()
                except Exception:
                    conn.rollback()
    finally:
        try:
            consumer.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()


