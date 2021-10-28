from contextlib import contextmanager
from suanpan.imports import imports
import json
import decimal
from mysql import connector

def registerArguments():
    node = imports("suanpan.node.node")
    argumentsToSet = {
        "json": imports("args.Json"),
        "dataframe": imports("args.Csv"),
        "csv": imports("args.Csv"),
        "hdf5": imports("args.Hdf5"),
        "sqlite": imports("args.Sqlite"),
        "parquet": imports("args.Parquet"),
        "excel": imports("args.Excel"),
    }
    node.setDataSubtypeArgs(**argumentsToSet)

def formatJson(data):
    jsonData = json.dumps(data, cls=SPEncoder)
    jsonData = jsonData.replace("NaN", "null")
    jsonData = jsonData.replace("-Infinity", '"-Inf"')
    jsonData = jsonData.replace("Infinity", '"Inf"')
    return json.loads(jsonData)

class SPEncoder(json.JSONEncoder):
    def default(self, obj):  
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

@contextmanager
def getConnection():
    try:
        connection = connector.connect(
                host = '47.102.131.179',
                port = 3306,
                user = "root",
                password = '1234@qwer',
                connection_timeout = 60,
                database = "excavator"
                )
        yield connection
    except Exception as e:
        return e
    finally:
        connection.close()