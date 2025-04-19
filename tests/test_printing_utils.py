import pytest 
from Conexion.printing_cassandra_utils import *


def test_taggers():
    assert tr("") == "<tr></tr>"
    assert tr("a") == "<tr>a</tr>"
    assert td("") == "<td></td>"
    assert td("a") == "<td>a</td>"

