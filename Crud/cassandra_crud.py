from crud_interface import CRUD
import collections 
class CassandraCrud(CRUD):
    def advancedQueryFactory(names_val_map):

        #in cassandra, this would obv be the where clause
        #
        assert type(names_val_map) in (dict, collections.defaultdict)
        cassandra_where_clause = "where "
        string_arr = []
        for key in names_val_map:
            string_arr.append(
                str(key) + 
            )
            print(key)

        cassandra_where_clause += "and".join(string_arr)
        return cassandra_where_clause
        print(type(names_val_map))
        print(names_val_map)
        #assert typeof(names_val_map) in 


def main():
    x = CassandraCrud
    print(1)
    print(x.advancedQueryFactory({"1": ["2"] }))
    print(x.advancedQueryFactory(collections.defaultdict(str)))
if __name__ == "__main__":
    main()
