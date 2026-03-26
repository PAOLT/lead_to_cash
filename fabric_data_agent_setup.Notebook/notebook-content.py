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

# # Fabric Data Agent Setup on Lakehouse Tables
# 
# This notebook creates and configures a Microsoft Fabric Data Agent over Lakehouse tables. It defines both global instructions and data-source-level instructions to guide how the agent queries the Lakehouse. The agent is then published for reuse by other agents via MCP.

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

AGENT_DISPLAY_NAME = 'sales_agent_lh'
LAKEHOUSE_NAME = 'ops_data' 
SCHEMA = 'dbo'

TABLE_NAMES = [
    'customers','products','csat_by_month',
    'support_tickets','support_activities',
    'sales_opportunities','sales_activities','opportunity_notes'
 ]

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

DATA_SOURCE_DESCRIPTION = """
This datasource provides data for the following entities:
- customers master data and csat metrics
- products master data
- opportunities master data, activities and notes
- support tickets master data and activities
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

DATA_SOURCE_INSTRUCTIONS = """
# Tables and most relevant columns vailable in the LakeHouse.

## CUSTOMERS 
- provides customers master data
- Columns: customer_id, customer_name, country

## CSAT_BY_MONTH 
- provides customer satisfaction (csat) metrics by month
- Columns: customer_id, month, csat

## PRODUCTS 
- provides products master data
- Columns: product_id, product_name, product_line

## SALES_OPPORTUNITIES 
- provides sales opportunities by product and customer
- Columns: opp_id, customer_id, product_id, type, status, stage, opened_at, expected_close_date, closed_at, is_forecast, forecast_category, stage_last_changed_at, amount, currency, probability, renewal_term_months

## SALES_ACTIVITIES 
- provides the sales activities for a certain sales opportunity
- Columns: activity_id, opp_id, activity_at, description, type, contact_name

## OPPORTUNITY_NOTES
- provides sale judjements and annotations
- Columns: note_id, opp_id, note_at, note_type, note_text, tags

## SUPPORT_TICKETS
- provides master data about support tickets by customer and product
- Columns: ticket_id, customer_id, product_id, status, opened_at, closed_at, severity, priority, channel, title

## SUPPORT ACTIVITIES:
- provides support operator activities for a certain support ticket
- Columns: activity_id, ticket_id, activity_at, description, author, activity_type, minutes_spent 

# How to join tables:
- sales_opportunities o JOIN customers c on o.customer_id=c.customer_id
- sales_opportunities o JOIN products p on o.product_id=c.product_id
- sales_activities a JOIN sales_opportunities o on a.opp_id=o.opp_id
- opportunity_notes n JOIN sales_opportunities o on n.opp_id=o.opp_id
- support_tickets t JOIN customers c on t.customer_id=c.customer_id
- support_tickets t JOIN products p on t.product_id=c.product_id
- support_activities a JOIN support_tickets t on a.opp_id=t.opp_id
- csat_by_month m JOIN customers c on m.customer_id=c.customer_id

Modeling constraints:
- Use support_tickets(severity, sla_breach_flag, status, opened_at, closed_at) with support_activities to derive last activity per ticket.
- Use products.product_line to segment results by product line.
- There is NO last_activity column on opportunities or tickets. When asked about "last activity" use MAX(activity_at) from the respective *activities* table.
"""

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

# ### Create and configure the data agent

# CELL ********************

data_agent = create_data_agent(AGENT_DISPLAY_NAME)
print(f"Created agent {data_agent} with name {AGENT_DISPLAY_NAME}")

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
datasource

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

datasource.update_configuration(instructions=DATA_SOURCE_INSTRUCTIONS, user_description=DATA_SOURCE_DESCRIPTION)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# selecting tables in the lakehouse
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

# MARKDOWN ********************

# ### Publish the data agent (optional)

# CELL ********************

#data_agent.publish(description=MCP_INSTRUCTIONS)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
