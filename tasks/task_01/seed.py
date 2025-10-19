# -*- coding: utf-8 -*-

"""
Populate data with fake values script
"""

import logging
import random
from typing import Optional, Any

import faker


from .postgress import PgConfig, PostgresClient, DatabaseError


def generate_fake_data(
        users_number: int = 5,
        tasks_number: int = 25,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Generating fake data.

    :param users_number: Number of users
    :param tasks_number: Number of tasks
    :return: Lists of fake users and tasks
    """
    fake_users = []
    fake_tasks = []

    fake = faker.Faker("uk_UA")
    fake.seed_instance(42)

    for _ in range(users_number):
        fake_users.append(dict(fullname=fake.name(), email=fake.email()))

    for _ in range(tasks_number):
        fake_tasks.append(
            dict(title=fake.sentence(nb_words=5), description=fake.paragraph(nb_sentences=3))
        )

    return fake_users, fake_tasks


def fill_users_data(pg_client: PostgresClient, users: list[dict[str, Any]], logger: logging.Logger) -> bool:
    """
    Insert users data into the database.

    :param pg_client: Database connection class
    :param users: List of user data
    :param logger: Logger class
    :return: Success status
    """
    try:
        affected_rows = pg_client.executemany(
            "INSERT INTO users (fullname, email) VALUES(%(fullname)s, %(email)s) ON CONFLICT (email) DO NOTHING;",
            users
        )
        logger.info(f"Data for {affected_rows} of {len(users)} users has been added to the database.")
        return True
    except DatabaseError as e:
        logger.error(e)
        return False


def get_users_ids(pg_client: PostgresClient) -> list[int]:
    """
    Return list of user IDs.

    :param pg_client: Database connection class
    :return: List of user IDs
    """
    users = pg_client.fetch_all("SELECT id FROM users")
    return [user.get('id') for user in users if user.get('id') is not None]


def get_status_ids(pg_client: PostgresClient) -> list[int]:
    """
    Return list of status IDs.

    :param pg_client: Database connection class
    :return: List of status IDs
    """
    users = pg_client.fetch_all("SELECT id FROM status")
    return [user.get('id') for user in users if user.get('id') is not None]


def fill_tasks_data(pg_client: PostgresClient, tasks: list[dict[str, Any]], logger: logging.Logger) -> bool:
    """
    Insert users data into the database.

    :param pg_client: Database connection class
    :param tasks: List of tasks data
    :param logger: Logger class
    :return: Success status
    """
    try:
        users_ids = get_users_ids(pg_client)
        status_ids = get_status_ids(pg_client)
        for task in tasks:
            task['status_id'] = random.choice(status_ids)
            task['user_id'] = random.choice(users_ids)

        affected_rows = pg_client.executemany(
            "INSERT INTO tasks (title, description, status_id, user_id) VALUES(%(title)s, %(description)s, %(status_id)s, %(user_id)s);",
            tasks
        )
        logger.info(f"Data for {affected_rows} of {len(tasks)} tasks has been added to the database.")
        return True
    except DatabaseError as e:
        logger.error(e)
        return False


def seed(
        host: str,
        port: str,
        user: str,
        password: str,
        dbname: str,
        logger: Optional[logging.Logger] = None,
) -> None:
    if logger is None:
        logger = logging.getLogger(__name__)

    users, tasks = generate_fake_data(users_number=10, tasks_number=100)

    pg_client = PostgresClient(
        PgConfig(dsn=f"postgresql://{user}:{password}@{host}:{port}/{dbname}", autocommit=True, min_size=1, max_size=5)
    )

    if fill_users_data(pg_client, users, logger):
        fill_tasks_data(pg_client, tasks, logger)