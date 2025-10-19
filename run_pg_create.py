# -*- coding: utf-8 -*-

"""
Tests for Task 1 Create DB script
"""

import dotenv

from tasks import console_logger, create


config = dotenv.dotenv_values(".env")


if __name__ == "__main__":
    create(
        config["PG_HOST"],
        config["PG_PORT"],
        config["PG_USER"],
        config["PG_PASSWORD"],
        config["PG_DBNAME"],
        logger=console_logger(__name__),
    )
