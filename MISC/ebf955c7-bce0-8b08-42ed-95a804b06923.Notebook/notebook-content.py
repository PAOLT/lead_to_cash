# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "867de34a-41e2-44a4-83ed-789a8e3feb01",
# META       "default_lakehouse_name": "ops_data",
# META       "default_lakehouse_workspace_id": "beeadc18-d85e-4c30-89e9-fa6b3fc07736",
# META       "known_lakehouses": [
# META         {
# META           "id": "867de34a-41e2-44a4-83ed-789a8e3feb01"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Fabric Data Agent Setup (ONTO)
# 
# This notebook creates and configures a **Microsoft Fabric Data Agent** over an Ontology. It configures **global instructions** so the agent understands its objectives. Finally, the agent is **published** so it can be used from other agents via MCP.

# MARKDOWN ********************

# ### Imports and settings

# CELL ********************

%pip install fabric-data-agent-sdk

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

from datetime import date
import json

from fabric.dataagent.client import (
    FabricDataAgentManagement,
    create_data_agent,
    delete_data_agent,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

AGENT_DISPLAY_NAME = 'sales_agent_onto'
ONTO_NAME = 'sales_onto'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ---- Agent instructions ----
GLOBAL_INSTRUCTIONS = f'''


# You are a Sales & Support Operations analyst for a financial software vendor serving banks. Your role is to answer business questions related to:
- Sales pipeline health
- Opportunity progression and risk
- Customer renewals
- Support performance and customer satisfaction
Work exclusively with the business data provided in the approved data source.

# Business reasoning guidelines:
- Prioritize precise, repeatable answers grounded in data.
- When applicable, explain the business logic behind conclusions (e.g., why an opportunity is considered at risk).
- Provide SQL logic or summaries only when explicitly requested.

# Standard business definitions:
## Slip risk (default policy):
  - No recent sales activity for 14+ days
  - stage stagnant: opportunity stage unchanged for more than 21 days
  - Seller notes expressing risks or concerns in the last 60 days
## Renewal risk (default policy):
  - Limited expansion pipeline: opportunities for expansion or project aggregated value is lower than 100000
  - High volume of support incidents (3 open incidents with high severity)
  - SLA breaches
  - Low customer satisfaction in the most recent month
If a question requires refining these definitions, adapt them explicitly and state the new assumptions.

# Business modeling 
## Sales opportunities:
- type: renewal, project, expansion
- status: open, lost or win (when close or win, the opportunity is closed).
- stage: Discovery, Qualification, Procurement, Negotiation, Proposal
- forecast flag: true or false
- forecast category: pipeline, commit, best_case 

## Opportunity notes:
- note type: provide sale judgements (neutral, progress, risk)

## Support tickets:
- status: closed, open
- severity: Medium, Low, Critical, High
- priority: P1 (high-priority), P2, P3, P4 (low-priority)
'''


MCP_INSTRUCTIONS = '''
Use this agent to answer to sales questions
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Create and configure the data agent

# CELL ********************

data_agent = create_data_agent(AGENT_DISPLAY_NAME)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

data_agent.update_configuration(instructions=GLOBAL_INSTRUCTIONS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

datasource = data_agent.add_datasource(ONTO_NAME, type="ontology")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

data_agent.get_configuration()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Publish the data agent

# CELL ********************

data_agent.publish(description=MCP_INSTRUCTIONS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
