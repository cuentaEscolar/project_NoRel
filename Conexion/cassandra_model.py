import datetime
import os
import sys
import logging
import random
import uuid
import inspect
from cassandra.cluster import Cluster

log = logging.getLogger(__name__)
logging.basicConfig(filename=f'{__file__}.log', level=logging.INFO)
#print("log created")

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
    ) WITH CLUSTERING ORDER BY (  log_date DESC {})
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
SHORTENED_TABLE_PARAMETERS = { table : list(map( lambda x: x, (table.split("_"))[4:])) for table in TABLE_NAMES}
TABLE_PARAMETERS = { table : list(map( FULLNAMER, (table.split("_"))[4:])) for table in TABLE_NAMES}
FULL_PARAMETERS = { table : list(map( FULLNAMER, (table.split("_"))[2:])) for table in TABLE_NAMES}

def execute_batch(session, stmt, data):
    batch_size = 10
    for i in range(0, len(data), batch_size):
        batch = BatchStatement()
        for item in data[i : i+batch_size]:
            batch.add(stmt, item)
        session.execute(batch)
    session.execute(batch)

def gen_tables( ):

    return  {
        table: 
        TABLE_TEMPLATE.format(table, 
            (lambda x:  "," + x if x else x)( ",".join( TABLE_PARAMETERS[table] ))
            , 
            (lambda x: "," + x + " DESC " if x else x )( " DESC ,".join(  TABLE_PARAMETERS[table]))
        )
        for table in TABLE_NAMES
    }

TABLES = gen_tables() 
def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):

    log.info("Creating model schema")
    for table in TABLES:
        log.info(table)
        #print(table)
        #print(TABLES[table])
        session.execute(TABLES[table])

def gen_selects( ):

    select_queries = {}
    for table in TABLE_NAMES:
        parameters = TABLE_PARAMETERS[table]
        #print(TABLE_PARAMETERS[table])
        select_queries[table] = SELECT_TEMPLATE.format(
                ",".join(FULL_PARAMETERS[table]),
                table, 
                "{}" 
            )

    return select_queries
        



SELECT_QUERIES = gen_selects()


def insert_into_all(params):
    for table in TABLE_NAMES: 
        stmt = session.prepare(INSERT_TEMPLATE.format(table))
        session.execute(
            stmt, params
        )
def print_my_functions():
    current_module = sys.modules[__name__]
    functions = inspect.getmembers(current_module, inspect.isfunction)
    
    print("All functions in this module:")
    for name, func in functions:
        print(name)

def call_select(session, select_stmt, data ):

    stmt = session.prepare(select_stmt)
    rows = session.execute(stmt, data )
    return rows

def create_gets(session):
    call_template = """get_{}({})"""
    get_template = """
def get_{}( acc, d, {}):
    '''
    acc is expected for every table
    one really does not want someone else to be able to see their devices 
    iot is plenty insecure as is 
    d can be a range date so it should be passed as a string
    '''
    print(session)
    select_stmt = (SELECT_QUERIES['{}'])
    return 0
    return call_select( session,  [{}] )
            """
    SELECT_TEMPLATE
    for table_name in TABLE_NAMES:
        get_x = get_template.format(table_name,
                                ",".join(SHORTENED_TABLE_PARAMETERS[table_name]),
                                table_name
                                    ,table_name
                                    )
        print(exec(get_x))
        print(get_x)
        #print(help(curry_session))
        exec( call_template.format(table_name, ",".join( ["0"]* (2+len(SHORTENED_TABLE_PARAMETERS[table_name])) ) )  )
        exec(f"print( help(get_{table_name}))")
        
    print_my_functions()
        
    

def get_session():
    CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
    KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'investments')
    REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)
    create_schema(session)
    return session


if __name__ == "__main__":
    #print_table_descriptions()
    print_mermaid()
    #print_tables()
    #print_requirements()
    #session = get_session()
    #create_gets(session)
    #print("done")
    #print_my_functions()

