import csv
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
SCHEMA_PATH = BASE_DIR / "sql" / "schema.sql"

TABLE_TO_FILE = {
    "responses": OUTPUT_DIR / "responses_clean.csv",
    "question_metadata": OUTPUT_DIR / "question_metadata.csv",
    "aggregates": OUTPUT_DIR / "aggregates.csv",
    "correlations": OUTPUT_DIR / "correlations.csv",
    "open_topics": OUTPUT_DIR / "open_topics.csv",
}


def normalize_database_url(database_url: str) -> str:
    if database_url.startswith("postgresql+asyncpg://"):
        return "postgresql://" + database_url.split("://", 1)[1]
    if database_url.startswith("postgres+asyncpg://"):
        return "postgresql://" + database_url.split("://", 1)[1]
    return database_url


def truncate_tables(connection: psycopg.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            "TRUNCATE TABLE open_topics, correlations, aggregates, question_metadata, responses RESTART IDENTITY"
        )


def import_csv_file(connection: psycopg.Connection, table_name: str, path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Brakuje pliku do importu: {path}")

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = reader.fieldnames or []
        if not columns:
            return

        placeholders = ", ".join(["%s"] * len(columns))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        with connection.cursor() as cursor:
            for row in reader:
                values = [None if row[column] == "" else row[column] for column in columns]
                cursor.execute(query, values)


def main() -> None:
    load_dotenv(BASE_DIR / ".env")
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("Ustaw DATABASE_URL w systemie albo w pliku .env.")

    schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
    connection_url = normalize_database_url(database_url)

    with psycopg.connect(connection_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(schema_sql)
        truncate_tables(connection)

        for table_name, path in TABLE_TO_FILE.items():
            import_csv_file(connection, table_name, path)

        connection.commit()
        print("Import zakonczony powodzeniem.")


if __name__ == "__main__":
    main()
