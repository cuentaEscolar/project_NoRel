from functools import reduce

def coerce_to_string(x):
    if not x : 
        x = ""
    if type(x) != str:
        if type(x).__dict__.get('__str__') :
            x = str(x)
        elif  type(x).__str__ is not None: ##has a str method
                x = str(x)
        else:
            x = ""
    return x

def html_tagger(s):

    if not s: 
        s = "error"
    else:
        s = coerce_to_string(s)
    def tummy(x):
        x = coerce_to_string(x)
        return f"<{s}>{x}</{s}>"

    return tummy 

def tr(x): return html_tagger("tr")(x)
def th(x): return html_tagger("th")(x)
def td(x): return html_tagger("td")(x)
def tabler(x): return html_tagger("table")(x)

def nice_print(arr):
    if not arr: return ""
    if len(arr) == 0:
        return ""
    if len(arr) == 1:
        return coerce_to_string(arr[0]) 
    
    res = reduce( lambda x, y: f"{x}, {y}", map( coerce_to_string, arr[:-1]))
    res += " and " + coerce_to_string(arr[-1])
    return res



def printable_tables(TABLES):
    return tabler( reduce( 
        lambda x,y : x + y, 
        ( tr(th(table_name)) + tr(td(TABLES[table_name]))  for table_name in TABLES.keys() )
    ))


def printable_table_descriptions(TABLE_NAMES, FULL_PARAMETERS):
    return tabler(

        reduce(
            lambda x,y : x + y
            ,

            ( ( tr( td(table_name) + td( "Logs by " + nice_print(FULL_PARAMETERS[table_name] )) ) ) 
                for table_name in TABLE_NAMES)
            ,
            ( tr( th("Table name") + th("Description") ) )
        )
    )


def print_requirements():


    print("<table>")
    print(tr( th("Requirement") + th("Expected Output") ))
    c = 0
    for table_name, table_create in TABLES.items():

        if "unit" not in ' '.join(FULL_PARAMETERS[table_name]):
            c+=1 
            print(tr(
                td( f"Users should be able to filter from their account by {nice_print(FULL_PARAMETERS[table_name][1:]) }")
                + 
                td ( f"A set of rows matching the {nice_print(FULL_PARAMETERS[table_name]) }")
            ))
            continue
        
        newparameters = [ x for x in FULL_PARAMETERS[table_name]  if x!= "unit"]
        for unit in "voltage,light level,humidity,temperature".split(","):
            newparameters.append(unit + " as the captured data")
            print(tr(
                td( f"Users should be able to filter the logs by {nice_print(newparameters) }")
                + 
                td ( f"A set of rows matching the {nice_print(newparameters) }")
            ))
            c+=1
            newparameters.pop() 
    print("</table>")
    print(c)

def tabulator(s , tabs=1):
    print("\t"*tabs + s)

def space_swapper(s):
    return " ".join(s.split()[::-1])

def key_er(table_name, s):
    arr = s.split(" ")
    if len(arr) < 2:
        return s
    name = arr[1]
    if "account" in arr :
        arr.append(f'"PK"')
        return  " ".join(arr)

    if name in FULL_PARAMETERS[table_name] or name=="log_date":

        arr.append(f'"CK"')
        return  " ".join(arr)

    return s
    

def print_mermaid():
    newLine = "\n"
    print("erDiagram")
    for table_name in TABLE_NAMES:
        tabulator( f"""{table_name} {"{"}
        {newLine.join(
        ( (  key_er(table_name, space_swapper(table.replace(",", ""))) for table in TABLES[table_name].split(newLine)[2:-3]) )
        ) }
{"}"}"""

        )

def print_my_functions():
    current_module = sys.modules[__name__]
    functions = inspect.getmembers(current_module, inspect.isfunction)
    
    print("All functions in this module:")
    for name, func in functions:
        print(name)
