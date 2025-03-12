
import datetime
import logging
import random
import uuid

CREATE_KEYSPACE = """

        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""
