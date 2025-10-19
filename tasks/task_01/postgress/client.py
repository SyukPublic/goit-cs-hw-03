# -*- coding: utf-8 -*-

"""
PostgreSQL Client (psycopg 3)
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator, Iterable, Mapping, Optional, Sequence

import psycopg
import psycopg.rows
import psycopg.errors
import psycopg_pool


class DatabaseError(psycopg.errors.DatabaseError):
    pass


class DuplicateDatabase(psycopg.errors.DuplicateDatabase):
    pass


@dataclass(slots=True)
class PgConfig:
    """
    Configuration for connecting to PostgreSQL.
    """

    dsn: str  # like "postgresql://user:pass@localhost:5432/dbname"
    min_size: int = 1
    max_size: int = 10
    timeout: float = 30.0  # seconds
    # max_waiting: int = 5
    num_workers: int = 3
    autocommit: bool = False

    def setup_session(self, conn: psycopg.Connection):
        """
        Set session parameters.
        Executed once when each new connection is created.

        :param conn: connection from the pool
        """
        conn.execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED;")
        conn.execute("SET client_encoding TO 'UTF8';")
        if not self.autocommit:
            conn.commit()


class PostgresClient:
    """
    PostgreSQL client with a connection pool (psycopg 3).

    Support:
      - parameterized queries
      - transactions using a context manager
      - fetch/execute methods
    """

    def __init__(self, config: PgConfig):
        self._config = config
        # row_factory=dict_row -> returns dict rows: {"col": value}
        self._pool = psycopg_pool.ConnectionPool(
            conninfo=config.dsn,
            configure=config.setup_session,
            min_size=config.min_size,
            max_size=config.max_size,
            timeout=config.timeout,
            kwargs={"row_factory": psycopg.rows.dict_row, "autocommit": config.autocommit},
        )

    def close(self) -> None:
        """
        Close the pool (when shutting down the service).
        """
        self._pool.close()

    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection]:
        """
        Provide a raw connection from the pool (if low-level access is needed).

        :return: Raw connection.
        """
        with self._pool.connection() as conn:
            yield conn

    @contextmanager
    def transaction(self) -> Iterator[Optional[psycopg.Cursor[Any]]]:
        """Transaction with commit/rollback."""
        with self._pool.connection() as conn:
            try:
                with conn.cursor() as cur:
                    yield cur
                conn.commit()
            except DatabaseError:
                conn.rollback()
                raise

    # ----------------------------------------------------- helpers ----------------------------------------------------

    def execute(self, sql: str, params: Optional[Sequence[Any] | Mapping[str, Any]] = None) -> int:
        """
        Execute DML SQL statement (INSERT/UPDATE/DELETE).

        :param sql: SQL with placeholders (%s or %(name)s)
        :param params: Parameter values
        :return: Number of rows affected.
        """
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount or 0

    def executemany(self, sql: str, seq_of_params: Iterable[Sequence[Any] | Mapping[str, Any]]) -> int:
        """
        Execute the same DML SQL statement multiple times with different parameters.

        :param sql: SQL with placeholders (%s or %(name)s)
        :param seq_of_params: Set of parameter values
        :return: Number of rows affected.
        """
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.executemany(sql, seq_of_params)
            return cur.rowcount or 0

    def fetch_one(
            self,
            sql: str,
            params: Optional[Sequence[Any] | Mapping[str, Any]] = None,
    ) -> Optional[dict[str, Any]]:
        """
        Fetch a single row (as a dictionary) or None.

        :param sql: SQL with placeholders (%s or %(name)s)
        :param params: Parameter values
        :return: Row data or None
        """
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row is not None else None

    def fetch_all(self, sql: str, params: Optional[Sequence[Any] | Mapping[str, Any]] = None) -> list[dict[str, Any]]:
        """
        Fetch all rows (as a list of dictionary).

        :param sql: SQL with placeholders (%s or %(name)s)
        :param params: Parameter values
        :return: List of all rows data
        """
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]

    # ----------------------------------------------------- helpers ----------------------------------------------------
