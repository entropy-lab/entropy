import os
from shutil import copytree, copy

print("\n = = = = = = entropylab example project = = = = =")
print("Writting simple example project in the current directory...")

origin = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "simple"
)
destination = os.getcwd()

file_list = [
    "cheerful_node.py",
    "grumpy_administrator.py",
    "workflow.py",
    "parameters.json",
]

copytree(
    os.path.join(origin, "entropynodes"),
    os.path.join(destination, "entropynodes"),
    dirs_exist_ok=True,
)

for f in file_list:
    copy(os.path.join(origin, f), destination)

print("\t1. First execute\tpython cheerful_node.py\t\tto test single node")
print("\t2. First execute\tpython grumpy_administrator.py\tto test single node")
print("\t3. Then execute\t\tpython workflow.py\t\tto generate list of parameters")
print(
    "\t4. Open and change parameters.json (Null is not acceptable value for "
    "final resolved parameters) "
)
print("\t5. Execute workflow with\tpython -m entropylab.flame.execute ")
print("\t  (to interrupt and terminate execution press Ctrl+C)")
print("\n")
