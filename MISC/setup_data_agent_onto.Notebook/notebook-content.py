# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# MARKDOWN ********************

# # Fabric Data Agent Setup Using an Ontology
# 
# This notebook sets up and configures a Microsoft Fabric Data Agent on top of an IQ ontology. It defines global instructions to communicate business objectives and guide agent behavior, without requiring data-source-specific query instructions. The agent is then published for reuse by other agents via MCP.

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

ENTITY_NAMES = ["customer", "sales_opportunity", "product", "sales_note", "sales_activity", "support_ticket", "support_activity"]
RELS = ["customer --> sales_opportunity", "customer --> support_ticket"]
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Prompting

# CELL ********************

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

MCP_INSTRUCTIONS = '''
An agent providing a Sales & Support Operations Analyst for a financial software vendor serving banks. Agent's role is to answer business questions related to:
- Sales pipeline health
- Opportunity progression and risk
- Customer renewals
- Support performance and customer satisfaction
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ### Create and configure the Data Agent

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

# MARKDOWN ********************

# ### Publish the data agent (optional)

# CELL ********************

# data_agent.publish(description=MCP_INSTRUCTIONS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
