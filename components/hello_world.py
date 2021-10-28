import suanpan
from suanpan.app import app
from suanpan.app.arguments import String
from modules.data import module as data_module
from suanpan.node import node
from utils import common

dataModule = app.modules.register("data", data_module)

common.registerArguments()
inputNode = list(node.ins)[0]
outputNode = list(node.outs)[0]


@app.input(String(key="inputData1"))
@app.output(String(key="outputData1", alias="result"))
def hello_world(context):
    args = context.args
    print("done")
    


if __name__ == "__main__":
    suanpan.run(app)
