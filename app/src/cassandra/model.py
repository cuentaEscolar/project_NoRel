
import datetime
import logging
import random
import uuid

CREATE_KEYSPACE = """

        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

"""
    ALL TABLES SHOULD CONTAIN
        account TEXT, 
        log_date TIMEUUID,
        device uuid,
        unit TEXT,
        value DECIMAL,
        comment TEXT
"""

TABLE_TEMPLATE = """
    CREATE TABLE IF NOT EXISTS {} (

        account TEXT, 
        log_date TIMEUUID,
        device uuid,
        unit TEXT,
        value DECIMAL,
        comment TEXT,
        PRIMARY KEY ((account), log_date {} )
    ) WITH CLUSTERING ORDER BY ( {} log_date DESC)
"""

SHORTNAME_VALUES = {
    "a": "account" ,
    "d":  "log_date ",
    "de": "device" ,
    "u": "unit" ,
    "v": "value"
    }

FULLNAMER = (lambda x: SHORTNAME_VALUES[x])

TABLE_NAMES = [
    "log_by_a_d",
    "log_by_a_d_de",
    "log_by_a_d_u",
    "log_by_a_d_u_v",
    "log_by_a_d_de_u",
    "log_by_a_d_de_u_v"
]


def gen_tables( ):

    tables_ = []
    for table in TABLE_NAMES:
        parameters = list(map( FULLNAMER, (table.split("_"))[4:]))
        primary_key_keys = ",".join(parameters)
        clustering_order_keys = " DESC ,".join( parameters[::-1])

        if primary_key_keys != "":
            primary_key_keys = "," +  primary_key_keys

        if clustering_order_keys != "":
            clustering_order_keys += " DESC ,"

        tables_.append(
            TABLE_TEMPLATE.format(table, primary_key_keys, clustering_order_keys)

        )
    return tables_



TABLES = gen_tables() 

if __name__ == "__main__":
    print(TABLES)


