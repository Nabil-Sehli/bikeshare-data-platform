"""Create the warehouse source tables (idempotent).

Usage:
    python -m bikeshare.warehouse.bootstrap
"""

from __future__ import annotations

import time
from pathlib import Path

import psycopg2

from bikeshare.config import pg_dsn

SQL = (Path(__file__).parent / "bootstrap.sql").read_text()


def bootstrap(retries: int = 20) -> None:
    for attempt in range(1, retries + 1):
        try:
            with psycopg2.connect(pg_dsn()) as conn, conn.cursor() as cur:
                cur.execute(SQL)
            return
        except psycopg2.OperationalError as exc:
            if attempt == retries:
                raise
            print(f"waiting for Postgres ({attempt}/{retries}): {exc}")
            time.sleep(3)


def main() -> None:
    bootstrap()
    print("warehouse tables ready")


if __name__ == "__main__":
    main()
