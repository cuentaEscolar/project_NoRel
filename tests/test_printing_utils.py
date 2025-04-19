import pytest 
from Conexion.printing_cassandra_utils import *

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
    assert tr("") == "<tr></tr>"
    assert tr("a") == "<tr>a</tr>"
    assert td("") == "<td></td>"
    assert td("a") == "<td>a</td>"
    assert tabler("") == "<table></table>"
    assert tabler("a") == "<table>a</table>"

def test_nice_print():
    assert nice_print(None) == ""
    assert nice_print([]) == ""
    assert nice_print(["a"]) == "a"
    assert nice_print(["1"]) == "1"


