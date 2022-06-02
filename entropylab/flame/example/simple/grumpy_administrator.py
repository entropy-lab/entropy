# ==================== DEFINE NODE ====================
import entropylab.flame.nodeio as nodeio
import time

nodeio.context(
    name="GrumpyAdministrator",
    description="divides task lisk and sends them to others",
    icon="bootstrap/person-lines-fill.svg",
)

inputs = nodeio.Inputs()
inputs.state(
    "customers", units="list of strings", description="all customers we have today"
)
inputs.state("clerk_salary", units="k$", description="workforce demands")

outputs = nodeio.Outputs()
outputs.define(
    "clerk_request",
    units="string",
    description="notifies connected clerk to do work",
    retention=0,
)

nodeio.register()

# ==================== DRY RUN DATA ====================

inputs.set(customers=["Alice", "Bob"])
inputs.set(clerk_salary=23.2)

# =============== RUN NODE STATE MACHINE ===============

while nodeio.status.active:
    today_work = inputs.get("customers")
    for i, customer in enumerate(today_work):
        outputs.set(clerk_request=customer)
        print(f"Here comes {customer} (number = {i})")
        # budget_requests = inputs.get("clerk_salary")
        # print(f"Clerk demands {budget_requests} k$")

    time.sleep(1)  # coffee break
    budget_requests = inputs.get("clerk_salary")
    print(f"Clerk demands {budget_requests} k$")

    nodeio.terminate_workflow()
