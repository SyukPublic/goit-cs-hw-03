# -*- coding: utf-8 -*-

"""
Tests for Task 1 Create DB script
"""

import dotenv

from tasks import console_logger, seed


config = dotenv.dotenv_values(".env")


if __name__ == "__main__":
    seed(
        config["PG_HOST"],
        config["PG_PORT"],
        config["PG_USER"],
        config["PG_PASSWORD"],
        config["PG_DBNAME"],
        logger=console_logger(__name__),
    )
