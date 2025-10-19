# -*- coding: utf-8 -*-

"""
Database and tables create script
"""

import logging
from typing import Optional

from .postgress import PgConfig, PostgresClient, DuplicateDatabase, DatabaseError


def create_database(host: str, port: int, user: str, password: str, dbname: str, logger: logging.Logger) -> bool:
    """
    Creates the database if it does not exist.
    """
    pg_client = PostgresClient(
        PgConfig(dsn=f"postgresql://{user}:{password}@{host}:{port}/postgres", autocommit=True, min_size=1, max_size=5)
    )

    try:
        exists = pg_client.fetch_one("select EXISTS( SELECT 1 FROM pg_database WHERE datname = %s);", (dbname,))
        if not  exists.get('exists', False):
            logger.warning(f"The database \"{dbname}\" does not exist. Creating it.")
            try:
                pg_client.execute(f"CREATE DATABASE \"{dbname}\" ENCODING \"UTF8\"")
                logger.info(f"The database \"{dbname}\" created.")
            except DuplicateDatabase:
                logger.info(f"The database \"{dbname}\" already exists.")
        else:
            logger.info(f"The database \"{dbname}\" already exists.")

        return True
    except Exception as e:
        logger.error(e)
        return False
    finally:
        pg_client.close()


def create_table(pg_client: PostgresClient, sql: str, logger: logging.Logger) -> bool:
    """
    Creates a table.

    :param pg_client: Database connection class
    :param sql: SQL with create table
    :param logger: Logger class
    :return: Success status
    """
    try:
        with pg_client.transaction() as cur:
            cur.execute(sql)
            return True
    except DatabaseError as e:
        logger.error(e)
        return False


def create_tables(host: str, port: int, user: str, password: str, dbname: str, logger: logging.Logger) -> bool:
    """
    Creates tables.
    """
    pg_client = PostgresClient(
        PgConfig(dsn=f"postgresql://{user}:{password}@{host}:{port}/{dbname}", min_size=1, max_size=5)
    )

    try:
        # Create  table: users
        if create_table(
                pg_client,
                """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    fullname VARCHAR(100) NOT NULL,
                    email VARCHAR(100) NOT NULL,
                    CONSTRAINT uq_users_email UNIQUE (email)
                );
                """,
                logger
        ):
            logger.info("The table \"users\" created.")

        # Create  table: status
        if create_table(
                pg_client,
                """
                CREATE TABLE IF NOT EXISTS status (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50) NOT NULL CHECK (name IN ('new', 'in progress', 'completed')),
                    CONSTRAINT uq_status_name UNIQUE (name)
                );
                """,
                logger
        ):
            logger.info("The table \"status\" created.")

        # Create  table: tasks
        if create_table(
                pg_client,
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id          SERIAL PRIMARY KEY,
                    title       VARCHAR(100) NOT NULL,
                    description TEXT,
                    status_id   INTEGER NOT NULL,
                    user_id     INTEGER NOT NULL,
                
                    CONSTRAINT fk_tasks_status
                        FOREIGN KEY (status_id)
                        REFERENCES status (id)
                        ON UPDATE RESTRICT
                        ON DELETE RESTRICT,
                
                    CONSTRAINT fk_tasks_user
                        FOREIGN KEY (user_id)
                        REFERENCES users (id)
                        ON UPDATE RESTRICT
                        ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_status_id ON tasks (status_id);
                CREATE INDEX IF NOT EXISTS idx_tasks_user_id   ON tasks (user_id);
                """,
                logger
        ):
            logger.info("The table \"tasks\" created.")

        return True
    except Exception as e:
        logger.error(e)
        return False
    finally:
        pg_client.close()


def create(
        host: str,
        port: str,
        user: str,
        password: str,
        dbname: str,
        logger: Optional[logging.Logger] = None,
) -> None:
    if logger is None:
        logger = logging.getLogger(__name__)
    if create_database(host, int(port), user, password, dbname, logger):
        create_tables(host, int(port), user, password, dbname, logger)
