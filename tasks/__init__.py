# -*- coding: utf-8 -*-

__title__ = 'Home Work Tasks'
__author__ = 'Roman'

from .logger import console_logger
from .task_01 import create, seed
from .task_02 import mongodb_crud_cli


__all__ = ['console_logger', 'create', 'seed', 'mongodb_crud_cli']
