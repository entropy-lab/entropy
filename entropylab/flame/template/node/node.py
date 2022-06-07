# ==================== DEFINE NODE ====================
import entropylab.flame.nodeio as nodeio

nodeio.context(
    name="NameForThisNodeClass",
    description="what it does",
    icon="bootstrap/gear-fill.svg",  # any bootstrap icon, see https://icons.getbootstrap.com/
)

inputs = nodeio.Inputs()
# define inputs
# inputs.stream( "input name", units="units or data type", description="some detailed explanation" )
# inputs.state(  "state input name", units="units or data type", description="some detailed explanation" )

outputs = nodeio.Outputs()
# define outputs
# outputs.define( "output name", units="units or data type", description="some detailed explanation" )

nodeio.register()

# ==================== DRY RUN DATA ====================

# set inputs data for dry-run of the node
inputs.reset_all()  # needed just for repeated for development with repeated runs in IPython
# inputs.set( <input_name> = <input_value>)

# =============== RUN NODE STATE MACHINE ===============

while nodeio.status.active:

    # here write code that solves the problem this node
    # deals with, controlling execution flow using when needed
    # inputs.get( ... )   inputs.updated( ... ) and outputs.set ( ... )
    # you can programmatically terminate whole workflow by calling
    # nodeio.terminate_workflow()
    # or you can exit this individual node by calling usual exit()

    pass  # delete this line when writting your code
