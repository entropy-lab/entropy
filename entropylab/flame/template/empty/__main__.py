import os
import shutil

print("\n = = = = = = entropylab empty project = = = = =")
print("Writting simple example project in the current directory...")

origin = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "empty"
)
destination = os.getcwd()

shutil.copy(os.path.join(origin, "node.py"), destination)
shutil.copy(os.path.join(origin, "workflow.py"), destination)

print("\t1. First write and execute\tpython node.py\t\tto test single node")
print("\t2. Then execute\t\t\tpython workflow.py\tto generate list of parameters")
print("\t3. Write any unresolved input in parameters.json  ")
print("\t4. Execute workflow with\tpython -m entropylab.flame.execute ")
print("\t  (to interrupt and terminate execution press Ctrl+C)")
print("\n")
