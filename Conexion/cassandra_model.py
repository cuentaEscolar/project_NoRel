import datetime
import os
import sys
import logging
import random
import uuid
import inspect
from cassandra.cluster import Cluster
import Conexion.printing_utils as pc
import cassandra

"""
This module provides some helper functions to sync cassandra
"""

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
        value TEXT,
        comment TEXT,
        PRIMARY KEY ((account), log_date {} )
    ) WITH CLUSTERING ORDER BY (  log_date DESC {})
"""

SELECT_TEMPLATE = """
    SELECT {}
    from {}
    where log_date >= ? and log_date <= ? 
    {};
"""

INSERT_TEMPLATE = """
        INSERT INTO {} 
        (account , device_type , log_date , device , unit , value , comment)
        VALUES( ?,  ?,  ?,  ?,  ?,  ?,  ?  )
"""


SHORTNAME_VALUES = {
    "a": "account" ,
    "d":  "log_date",
    "de": "device" ,
    "u": "unit" ,
    "v": "value"
    }

SHORTNAME_VALUES_ES = {
    "a": "cuenta" ,
    "d":  "fecha_del_log",
    "de": "dispositivo" ,
    "u": "unidad" ,
    "v": "valor"
    }

FULLNAMER = (lambda x: SHORTNAME_VALUES[x])
FULLNAMER_ES = (lambda x: SHORTNAME_VALUES_ES[x])

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
FULL_PARAMETERS_ES = { table : list(map( FULLNAMER_ES, (table.split("_"))[2:])) for table in TABLE_NAMES}

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
        session.execute(TABLES[table])

def gen_selects( ):

    select_queries = {}
    for table in TABLE_NAMES:
        select_queries[table] = SELECT_TEMPLATE.format(
                ",".join(FULL_PARAMETERS[table]),
                table, 
                "{}" ,
            "?"
            )

    return select_queries
        



SELECT_QUERIES = gen_selects()



def call_select(session, select_stmt, data ):

    stmt = session.prepare(select_stmt)
    rows = session.execute(stmt, data )
    return rows

def insert_data(session):
    def insert_into_all(params):
        for table in TABLE_NAMES: 
            stmt = session.prepare(INSERT_TEMPLATE.format(table))
            session.execute(
                stmt, params
            )
    return insert_into_all


def create_gets(session):
    globals()['session'] = session
    """
    Notice that only the log_date may be in a range.
    Notice that if we only need a single date instead of a date_range
    then we can simply set both sides of the interval to the same date 
    """
    print(session)
    call_template = """get_{}({})"""
    get_template = """
def get_{}( account, d_s, d_e, {}):
    '''
    one really does not want someone else to be able to see their devices 
    iot is plenty insecure as is 
    d can be a range date so it should be passed as a string
    '''
    #print(d_s)
    #print(d_e)
    #d_s = uuid.UUID(d_s)
    #d_e = uuid.UUID(d_e)
    select_stmt = "SELECT * from {} where log_date >= maxTimeuuid('"+d_s+"') and log_date <= minTimeuuid('"+d_e+"') {};"
    #print(select_stmt)
    acc = account 
    a = account
    stmt = session.prepare(select_stmt)
    return session.execute(stmt, [ {}])
"""
    get_method_names = []
    for table_name in TABLE_NAMES:
        get_x = get_template.format(table_name,
            ",".join([x for x in SHORTENED_TABLE_PARAMETERS[table_name] if x != "d" ]) ,  
            #",".join(FULL_PARAMETERS[table_name]) ,
            f"{table_name}",
            " and " + " and ".join(
            list( map( lambda x : f"{x} = ?" , [ x for x in FULL_PARAMETERS[table_name] if x !="log_date"] ) )
            ) ,
            ",".join([ x for x in SHORTENED_TABLE_PARAMETERS[table_name] if x !="log_date"]) + "a"
                                    )
        #print(help(curry_session))
        #exec( call_template.format(table_name, ",".join( ["0"]* (2+len(SHORTENED_TABLE_PARAMETERS[table_name])) ) )  )
        exec(get_x, globals())
        get_method_names.append(f"get_{table_name}")
        
    return get_method_names
    #print_my_functions()
        
    
def get_all_logs(account):
    start_dt = datetime.datetime(1970, 1, 1, 0, 0, 0)
    end_dt = datetime.datetime(2999, 9, 9, 23, 59, 59)

    start_dt_str = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    end_dt_str = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    start_timestamp = int(start_dt.timestamp())  # Timestamp in seconds
    end_timestamp = int(end_dt.timestamp())  # Timestamp in seconds
    return get_log_by_a_d(account, start_dt_str, end_dt_str)

def get_session():
    """
    this creates the connection to the cassandra database 
    and returns an active session
    """
    CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
    KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'iot')
    REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()
    assert type(session) == cassandra.cluster.Session

    create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)
    create_schema(session)
    return session

def test_session(session):
    
    assert type(session) == cassandra.cluster.Session
    session.execute(session.prepare("use iot;"))
    stmt = session.prepare("describe tables;")
    result = (session.execute(stmt))
    return result


if __name__ == "__main__":
    session = (get_session())
    create_gets(session)
