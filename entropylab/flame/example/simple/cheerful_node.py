# ==================== DEFINE NODE ====================
import entropylab.flame.nodeio as nodeio

nodeio.context(
    name="CheerfulNode",
    description="greets customers",
    icon="bootstrap/person-circle.svg",
)

inputs = nodeio.Inputs()
inputs.stream("customer", units="human", description="one person at a time")
inputs.state("weather", units="best guess", description="How is weather today")

outputs = nodeio.Outputs()
outputs.define(
    "requested_salary",
    units="k$",
    description="requested fees from administrator",
    retention=2,
)

nodeio.register()

# ==================== DRY RUN DATA ====================

inputs.set(weather="sunny")
inputs.set(customer="Alice")
inputs.set(customer="Bob")
inputs.set(customer="Mars")
inputs.set(customer="Venus")

# =============== RUN NODE STATE MACHINE ===============

part_of_day = 0
salary_demand = 0.0

while nodeio.status.active:
    person = inputs.get("customer")
    weather = inputs.get("weather")
    if part_of_day % 2 == 0:
        day_time = "morning"
    else:
        day_time = "afternoon"
    print(f"Hi {person}, it is a {weather} {day_time}")
    part_of_day += 1

    salary_demand += 1.2

    outputs.set(requested_salary=salary_demand)
