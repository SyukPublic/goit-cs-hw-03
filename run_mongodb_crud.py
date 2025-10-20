# -*- coding: utf-8 -*-

"""
Tests for Task 2
"""

import dotenv

from tasks import mongodb_crud_cli


config = dotenv.dotenv_values(".env")


if __name__ == "__main__":
    mongodb_crud_cli(config['MONGODB_URI'], config['MONGODB_DBNAME'])
