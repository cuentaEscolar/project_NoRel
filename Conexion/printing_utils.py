from functools import reduce
from cassandra.util import unix_time_from_uuid1
from datetime import datetime



#Row(account='68222538324d9acdbb4c5f7b', log_date=UUID('77c4ae02-2cdb-11f0-b8f7-f3e38481b747'))
def logs_unpacker(res):

    """Row(account='68222538324d9acdbb4c5f7b'
    , log_date=UUID('9621ae02-2cc9-11f0-b8cd-8cd0895c73c3')✔️
    , comment=''
    , device=UUID('88550b2c-1c96-5bf9-9879-e23d65f30ae2')✔️
    , device_type='bombilla'✔️
    , unit='kWh'✔️
    , value='0.007')✔️
    """

    for log in res:
        tp = ""
        date,time = datetime.utcfromtimestamp(unix_time_from_uuid1(log.log_date)).strftime('%Y-%m-%d %H:%M:%S').split()
        type_ = log.device_type 
        unit = log.unit
        value = log.value
        device_id = str(log.device)
        tp += "|".join([f"{date=}",
                        f"{time=}",
                        f"type={type_}",
                        f"type={unit}",
                        f"{value=}",
                        f"{device_id=}",
                        ]
                       )

        print(tp)

def user_printer(x):
    print(f"""
ID: {x['_id']}
Usuario: {x['username']}
Correo: {x['correo']}
Casas: {nice_house_format(x['casas'])}
    """
          )

def nice_house_format(houses):

    #house_to_id = house_reader(houses)
    res = "\n\t".join( list( map(  lambda h: f"Casa #{h['num_casa']} : ID: {h['id_casa']}", houses) ) )
    return "\n\t" + res

def house_reader(houses):
    #x is a dict
    house_to_id = { x['num_casa']: x['id_casa']  for x in houses}

    return house_to_id


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

def print_bonito(arr):
    if not arr: return ""
    if len(arr) == 0:
        return ""
    if len(arr) == 1:
        return coerce_to_string(arr[0]) 
    
    res = reduce( lambda x, y: f"{x}, {y}", map( coerce_to_string, arr[:-1]))
    res += " y " + coerce_to_string(arr[-1])
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
                td( f"Los usuarios deberian de poder filtrar sus logs por {nice_print(FULL_PARAMETERS[table_name][1:]) }")
                + 
                td ( f"Un grupo de filas con los {nice_print(FULL_PARAMETERS[table_name]) } correspondientes.")
            ))
        
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

def imprimir_reqs_cas(TABLES, FULL_PARAMETERS, arr_printer): 

    header = tr( th("Requerimiento") + th("Resultado Esperado") )+ '\n'
    res = header
    for table_name, table_create in TABLES.items():

        if "unit" not in ' '.join(FULL_PARAMETERS[table_name]):
            res += tr(
                td( f"Los usuarios deberian de poder filtar sus logs por {arr_printer(FULL_PARAMETERS[table_name][1:]) }")
                + 
                td ( f"Un grupo de filas con los {arr_printer(FULL_PARAMETERS[table_name]) } correspondientes")
            ) + '\n'
        
        newparameters = [ x for x in FULL_PARAMETERS[table_name]  if x!= "unit"]
        for unit in "voltaje,nivel de luz,humedad,temperatura".split(","):
            newparameters.append(unit + " como el tipo de dato.")
            res += (tr(
                td( f"Users should be able to filter the logs by {arr_printer(newparameters) }")
                + 
                td ( f"A set of rows matching the {arr_printer(newparameters) }")
            )) + '\n'
            newparameters.pop() 

    print(tabler(res))

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
