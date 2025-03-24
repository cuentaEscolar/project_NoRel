from crud_interface import CRUD
import collections 
from collections.abc import Iterable
from cassandra.cluster import Cluster
from cassandra.cluster import Session

import os

class CassandraCrud(CRUD):

    #these all return stuff to prepare

    def __init__(self, session, KEYSPACE):

        assert type(session) == Session
        session.set_keyspace(KEYSPACE)
        self.session = session
    
    def set_table(self, table_name):

        assert type(table_name) == str 
        self.table_name = table_name
        self.select_template = (lambda : f"""SELECT {{}} from {self.table_name} {{}};""")

    def advancedQueryFactory(self, names, vals):

        #in cassandra, this would obv be the where clause
        #any value in vals may be {} in case 
        #we want or need to fill it with cassandra 
        #instead
        assert len(names)==len(vals)
        cassandra_where_clause = "where "
        string_arr = []
        l = len(names)

        for i in range(l):

            string_arr.append(
                str(names[i]) + str(vals[i]) 
            )

        cassandra_where_clause += "and".join(string_arr)
        return cassandra_where_clause
        print(type(names_val_map))
        print(names_val_map)

    def getXbyY(self, X, Y):
        #we do not want them to not be iterable. 
        #that would really not make any sense
        assert isinstance(X, Iterable)
        assert isinstance(Y, Iterable)
        fields_to_select = ",".join(X)
        fields_to_select_by = ""
        
        return self.select_template().format(fields_to_select, fields_to_select_by)
        

def main():

    CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
    KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'investments')
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    x = CassandraCrud(session, KEYSPACE)
    x.set_table("awa")
    print(1)
    print(
        x.advancedQueryFactory( ["1"],  ["=2"])
          )
    print(
        x.getXbyY([], [])
    )
if __name__ == "__main__":
    main()
