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
        device_type TEXT,
        log_date TIMEUUID,
        device uuid,
        unit TEXT,
        value DECIMAL,
        comment TEXT,
        PRIMARY KEY ((account), log_date {} )
    ) WITH CLUSTERING ORDER BY ( {} log_date DESC)
"""

SELECT_TEMPLATE = """
    SELECT {}
    from {}
    where account = ?
    and 
    {};
"""

INSERT_TEMPLATE = """

        INSERT INTO {} 
        (account , device_type , log_date , device , unit , value , comment),
        VALUES( ?,  ?,  ?,  ?,  ?,  ?,  ?  )
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
TABLE_PARAMETERS = { table : list(map( FULLNAMER, (table.split("_"))[4:])) for table in TABLE_NAMES}
FULL_PARAMETERS = { table : list(map( FULLNAMER, (table.split("_"))[2:])) for table in TABLE_NAMES}


def gen_tables( ):

    return  [
        TABLE_TEMPLATE.format(table, 
            (lambda x:  "," + x if x else x)( ",".join( TABLE_PARAMETERS[table] ))
            , 
            (lambda x: x + " DESC ," if x else x )( " DESC ,".join(  TABLE_PARAMETERS[table][::-1]))
        )
        for table in TABLE_NAMES
    ]

TABLES = gen_tables() 

def gen_selects( ):

    select_queries = {}
    for table in TABLE_NAMES:
        parameters = TABLE_PARAMETERS[table]
        print(TABLE_PARAMETERS[table])
        select_queries[table] = SELECT_TEMPLATE.format(
                ",".join(FULL_PARAMETERS[table]),
                table, 
                "{}" 
            )

    return select_queries
        



SELECT_QUERIES = gen_selects()

def insert_by_uuid(uuid):

    session.prepare(f"INSERT INTO {table} (account, trade_id, type, symbol, shares, price, amount) VALUES(?, ?, ?, ?, ?, ?, ?)")

if __name__ == "__main__":
    pass
    #print(TABLES)
    print(SELECT_QUERIES)


