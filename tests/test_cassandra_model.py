from Conexion import cassandra_model
import inspect
import cassandra

def test_session_creation():
    session = cassandra_model.get_session()
    assert type(session) == cassandra.cluster.Session
    return session

def test_schema_creation():
    expected_tables = ( "log_by_a_d", "log_by_a_d_de",
    "log_by_a_d_u", "log_by_a_d_u_v",
    "log_by_a_d_de_u", "log_by_a_d_de_u_v")
    session = cassandra_model.get_session()
    created_tables = cassandra_model.test_session(session) 
    assert created_tables is not None

    for table in created_tables:
        assert table.name in expected_tables

def test_get_methods():

    session = cassandra_model.get_session()
    get_methods = cassandra_model.create_gets(session)
    print(get_methods)
    assert get_methods is not None
    
    res= cassandra_model.get_log_by_a_d("6", '8810f754-0f06-11f0-809a-8f84824a116e', '8810f754-0f06-11f0-809a-8f84824a116e')
    assert res is not None

