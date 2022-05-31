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
# inputs.set( <input_name> = <input_value>)

# =============== RUN NODE STATE MACHINE ===============

while nodeio.status.active:

    # here write logic that solves the problem this node
    # deals with, contriling execution flow using when needed
    # inputs.get( ... )   inputs.updated( ... ) and outputs.set ( ... )
    # you can programmatically terminate node by calling
    # nodeio.terminate_workflow()

    pass  # delete this line when writting your code
