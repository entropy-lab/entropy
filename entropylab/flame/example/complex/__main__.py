import os
from shutil import copytree, copy

print("\n = = = = = = entropylab example project = = = = =")
print("Writting simple example project in the current directory...")

origin = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "complex"
)
destination = os.getcwd()

file_list = [
    "calibrate.py",
    "circuitExec.py",
    "errorCorrection.py",
    "externalScan.py",
    "plot.py",
    "sequence.py",
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


print("\t Execute workflow with\tpython -m entropylab.flame.execute ")
print("\t (to interrupt and terminate execution press Ctrl+C)")
print("\n")
