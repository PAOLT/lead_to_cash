# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   }
# META }

# MARKDOWN ********************

# # Fabric Data Agent Setup
# 
# This notebook creates and configures a **Microsoft Fabric Data Agent** over the Lakehouse tables generated in this workspace (sales + support operations demo).
# 
# Key points**
# - The agent is created programmatically (Preview SDK).
# - It adds your **Lakehouse** as a data source and selects the demo tables.
# - It configures **global instructions** and **data‑source instructions** so the agent understands how to query your schema (e.g., how to derive last activity from activities).
# - Finally, it **publishes** the agent so you can use it from other agents via MCP.


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

ENTITY_NAMES = ["customer", "sales_opportunity", "product", "sales_note", "sales_activity", "support_ticket", "support_activity"]
RELS = ["customer --> sales_opportunity", "customer --> support_ticket"]
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Name your agent and the Lakehouse to bind
AGENT_DISPLAY_NAME = 'FinServ Sales Agent Onto'
ONTO_NAME = 'sales_onto' 

# ---- Agent instructions ----
GLOBAL_INSTRUCTIONS = f'''
You are a Sales & Support Operations analyst for a financial software vendor serving banks.
Work strictly from the ontology entities and relationships.

Important modelling constraints:
- There is NO last_activity property on opportunities or tickets. When asked about "last activity" use the date properties from the respective *activities* entity.
- Opportunity status is one of Open, Won, Lost.
- Ticket severity can be Low, Medium, High, Critical; sla_breach_flag indicates risk.

Reasoning guidelines:
- Prefer precise, reproducible answers.
- For "slip risk" this month, consider: no recent activity (14+ days), stage stagnant (>21 days), delivery risk notes.
- For "renewal risk": low expansion pipeline, high incident volume, SLA breaches, or low CSAT in latest month.
'''

MCP_INSTRUCTIONS = '''
use this agent to answer to sales questions
'''


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

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

# datasource = data_agent.get_datasources()[0]

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

# CELL ********************

data_agent.publish(description=MCP_INSTRUCTIONS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
