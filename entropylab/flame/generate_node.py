"""
Generates python class for the workflow module based on module schema .json
"""

import sys
import os
import json
from jinja2 import Template
from .enum_types import InputType

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise ValueError("Enter module names")

    template_filename = os.path.dirname(os.path.realpath(__file__))
    template_filename = os.path.join(template_filename, "node_template.py.jinja")
    with open(template_filename, "r") as file:
        node_template = Template(file.read())

    for node_name in sys.argv[1:]:
        full_path_in = os.path.join(os.path.join(os.getcwd(), "entropynodes"), "schema")
        full_path_in = os.path.join(full_path_in, f"{node_name}.json")

        with open(full_path_in, encoding="utf-8", mode="r") as f:
            schema = json.loads(f.read())

        full_path_out = os.path.join(
            os.path.join(os.getcwd(), "entropynodes"), "library"
        )
        if not os.path.exists(full_path_out):
            os.makedirs(full_path_out)

        # TODO: handle properly Inputs instances in Inputs lists, and multiple
        # Inputs instances (currently takes only first instance)

        input_type = {}
        for key, value in schema["inputs"][0]["type"].items():
            input_type[key] = InputType(value).name

        with open(
            os.path.join(full_path_out, f"{node_name}.py"), encoding="utf-8", mode="w"
        ) as f:
            f.write(
                node_template.render(
                    name=schema["name"],
                    description=schema["description"],
                    command=schema["command"],
                    bin=schema["bin"],
                    inputunits=schema["inputs"][0]["units"],
                    inputdescription=schema["inputs"][0]["description"],
                    inputtype=input_type,
                    outputdescription=schema["outputs"][0]["description"],
                    outputunits=schema["outputs"][0]["units"],
                    node_icon=schema["icon"],
                )
            )

        # load all the classes defined in library directory

        init_file = ""
        for py in [
            f[:-3]
            for f in os.listdir(full_path_out)
            if f.endswith(".py") and f != "__init__.py"
        ]:
            init_file += f"from .{py} import {py}\n"

        with open(os.path.join(full_path_out, "__init__.py"), "w") as f:
            f.write(init_file)

        with open(
            os.path.join(os.path.join(os.getcwd(), "entropynodes"), "__init__.py"), "w"
        ) as f:
            f.write("")
