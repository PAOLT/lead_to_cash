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
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Name your agent and the Lakehouse to bind
AGENT_DISPLAY_NAME = 'FinServ Sales Agent LH'
LAKEHOUSE_NAME = 'ops_data' 
SCHEMA = 'dbo'

# Expected demo tables (created by the generator v2)
TABLE_NAMES = [
    'customers','products','csat_by_month',
    'support_tickets','support_activities',
    'sales_opportunities','sales_activities','opportunity_notes'
 ]

# ---- Agent instructions ----
GLOBAL_INSTRUCTIONS = f'''
You are a Sales & Support Operations analyst for a financial software vendor serving banks.
Work strictly from the Lakehouse tables we selected.

Business rules and joins:
- Join sales_activities to sales_opportunities on opp_id.
- Join support_activities to support_tickets on ticket_id.
- customers.customer_id joins to sales_opportunities.customer_id and support_tickets.customer_id.
- products.product_id joins to sales_opportunities.product_id and support_tickets.product_id.
- csat_by_month has ONLY (customer_id, month INT 1..12, csat INT 1..5).

Important modelling constraints:
- There is NO last_activity column on opportunities or tickets.
  When asked about "last activity" use MAX(activity_at) from the respective *activities* table.
- Opportunity status is one of Open, Won, Lost.
- Ticket severity can be Low, Medium, High, Critical; sla_breach_flag indicates risk.

Reasoning guidelines:
- Prefer precise, reproducible answers and show the SQL (or a summary) on request.
- For "slip risk" this month, consider: no recent activity (14+ days), stage stagnant (>21 days), delivery risk notes.
- For "renewal risk": low expansion pipeline, high incident volume, SLA breaches, or low CSAT in latest month.

Tables available: {TABLE_NAMES}
Generated on {date.today().isoformat()}.
'''

DATA_SOURCE_INSTRUCTIONS = '''
Per-source guidance for the Lakehouse:

- Use sales_opportunities(expected_close_date, status, stage, stage_last_changed_at, amount, probability, type)
  with sales_activities to derive last activity: SELECT opp_id, MAX(activity_at) AS last_activity_at FROM sales_activities GROUP BY opp_id.
- Use support_tickets(severity, sla_breach_flag, status, opened_at, closed_at) with support_activities to derive last activity per ticket.
- For "delivery risk", check opportunity_notes where tags LIKE '%delivery risk%'.
- CSAT by month uses 1–12 as month index (no year). When asked for "last month", map from current month-1 (wrap 1->12).
- Use products.product_line to segment results by product line.
- Avoid selecting *; project meaningful columns and include filters for the asked time window.
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

datasource = data_agent.add_datasource(LAKEHOUSE_NAME, type="lakehouse")

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

for t in TABLE_NAMES:
    datasource.select(SCHEMA, t)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

datasource.pretty_print()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

datasource.update_configuration(instructions=DATA_SOURCE_INSTRUCTIONS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# example_dict = {
#     "How many customers does the company has?": "SELECT COUNT(*) AS NumberOfCustomers FROM dbo.customers"
# }
# datasource.add_fewshots(example_dict)

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
