from crud_interface import CRUD
import collections 
from collections.abc import Iterable
from cassandra.cluster import Cluster
from cassandra.cluster import Session

import os

class Range:
    def __init__(self, lower, b):
        pass

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

    # getXbyY takes 
    #
    # getXbyY :: Iterable -> Iterable -> Iterable
    # Vals is expected to contain the operator
    # as so ["op", [the thing they r operatin on ]]
    # thus , opVals is of type
    # [Str, Iterable] 👍
    def getXbyY(self, X):

        assert isinstance(X, Iterable)
        def getY(Y):

            assert isinstance(Y, Iterable)
            def getVals(opVals):
                assert(isinstance(Vals, Iterable))
                assert len(Y) == len(Vals)

                #we do not want them to not be iterable. 
                #that would really not make any sense
                # we also want to process strings differently
                string_tagger = lambda x: f"'{x}'"
                def process_vals(x):
                    #so. what happens if we have more than one?


                    x[1] = ","
                    return " ".join()


                                                
                 
                y_and_vals =  ",".join(list(map(  (lambda x : " ".join(x)) , zip(Y, opVals) )))
                
                full_query = ( 
                        self.select_template().format(
                                ",".join(X), 
                                                       )
                            )
                
                print(full_query)
                return self.session.execute(full_query)
            
            return getVals
        return getY
        

def main():

    CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
    KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'investments')
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    x = CassandraCrud(session, KEYSPACE)
    x.set_table("positions_by_account") 
    print(1)
    print(
        x.advancedQueryFactory( ["account"],  ["=aecc4fdf-da48-4c5c-b715-6136f625c368"])
          )
    print(
        x.getXbyY(["symbol", "quantity"])(["account"])([["=", ["fc487b9e-1c05-40e1-b03d-7d0d7d71b410"] ] )
    )
if __name__ == "__main__":
    main()
