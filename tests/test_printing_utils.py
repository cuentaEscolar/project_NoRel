import pytest 
import Conexion.printing_cassandra_utils as pc
coerce_to_string = pc.coerce_to_string
def test_coerce_to_string():
    assert coerce_to_string("a") == "a"
    assert coerce_to_string("") == ""
    assert coerce_to_string(None) == ""
    assert coerce_to_string(1) == "1"
    class No_Str():
        def __init__(self, x):
            self.x = x 
    assert coerce_to_string(No_Str(1)) is not None

def test_taggers():
    assert pc.tr("") == "<tr></tr>"
    assert pc.tr("a") == "<tr>a</tr>"
    assert pc.td("") == "<td></td>"
    assert pc.td("a") == "<td>a</td>"
    assert pc.tabler("") == "<table></table>"
    assert pc.tabler("a") == "<table>a</table>"

def test_nice_print():
    assert pc.nice_print(None) == ""
    assert pc.nice_print([]) == ""
    assert pc.nice_print(["a"]) == "a"
    assert pc.nice_print(["1"]) == "1"


