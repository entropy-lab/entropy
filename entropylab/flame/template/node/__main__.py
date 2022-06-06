import os
import shutil

print("\n = = = = = = entropylab Python NodeIO = = = = = =")
print("Writting empty node.py example in the current directory...")

origin = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "empty"
)
destination = os.getcwd()

shutil.copy(os.path.join(origin, "node.py"), destination)

print("\tdone!")
print("\n")
