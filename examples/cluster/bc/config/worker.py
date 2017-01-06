# -*- coding: utf-8 -*-
from __future__ import absolute_import
from .common import *

BACKEND = 'frontera.contrib.backends.hbase.HBaseBackend'
HBASE_DROP_ALL_TABLES = False
HBASE_TIMEOUT = 30000
HBASE_THRIFT_HOST = "10.2.0.17"
MAX_NEXT_REQUESTS = 2048
NEW_BATCH_DELAY = 3.0
