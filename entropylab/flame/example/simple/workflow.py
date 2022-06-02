import entropynodes.library as expNodes
from entropylab.flame.workflow import Workflow

wf = Workflow("A day in office", description="What happens in local office")

boss = expNodes.GrumpyAdministrator("boss")
clerk = expNodes.CheerfulNode("clerk", customer=boss.o.clerk_request)
boss.i.clerk_salary = clerk.o.requested_salary

wf.register()
