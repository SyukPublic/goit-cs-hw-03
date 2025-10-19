# -*- coding: utf-8 -*-

"""
Logger initialization
"""

import logging
from typing import Optional


def console_logger(name: Optional[str]) -> logging.Logger:
    logger = logging.getLogger(name or __name__)
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s\t%(levelname)s\t%(message)s")
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger
