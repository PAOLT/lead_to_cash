# Fabric notebook source


# MARKDOWN ********************

# # Fabric Data Agent Setup (Python Notebook)
# 
# This notebook creates and configures a **Microsoft Fabric Data Agent** over the Lakehouse tables generated in this workspace (sales + support operations demo).
# 
**Key points**
# - The agent is created programmatically (Preview SDK).
# - It adds your **Lakehouse** as a data source and selects the demo tables.
# - It configures **global instructions** and **data‑source instructions** so the agent understands how to query your schema (e.g., how to derive last activity from activities).
# - Finally, it **publishes** the agent so you can use it from Copilot in Fabric or connect it from Microsoft Copilot Studio.


# CELL ********************

%pip install -U fabric-data-agent-sdk

# CELL ********************

# ----- Configuration -----
from datetime import date
import json

# Name your agent and the Lakehouse to bind
AGENT_DISPLAY_NAME = 'FinServ Sales & Support Agent'
LAKEHOUSE_NAME = None  # If None, will try to use the attached Lakehouse in this notebook

# Discover tables from the attached Lakehouse via Spark catalog
try:
    tables = [t.name for t in spark.catalog.listTables() if t.tableType in ('MANAGED','EXTERNAL')]
except Exception as e:
    tables = []

# Expected demo tables (created by the generator v2)
EXPECTED = [
    'customers','products','csat_by_month',
    'support_tickets','support_activities',
    'sales_opportunities','sales_activities','opportunity_notes'
 ]
FOUND = [t for t in EXPECTED if t in tables]
MISSING = [t for t in EXPECTED if t not in tables]
print('Found tables:', FOUND)
print('Missing  :', MISSING)

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

Tables available: {FOUND}
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


# CELL ********************

# ----- Create & configure the Data Agent (Preview SDK) -----
from typing import Optional
agent = None

def get_client():
    # Try common import patterns seen in SDK previews
    try:
        from fabric_data_agent_sdk import DataAgentClient
        return ('client', DataAgentClient())
    except Exception:
        pass
    try:
        from fabric.dataagent.client import create_data_agent  # alt preview name in blog posts
        return ('factory', create_data_agent)
    except Exception:
        pass
    return (None, None)

kind, obj = get_client()
if kind is None:
    raise RuntimeError('Fabric Data Agent SDK is not available in this kernel. Make sure %pip install -U fabric-data-agent-sdk ran successfully and you are in a Fabric notebook runtime.')

# Create or get agent
if kind == 'client':
    client = obj
    try:
        agent = client.create_agent(name=AGENT_DISPLAY_NAME, description='NL analytics over sales & support demo tables')
    except Exception as e:
        print('create_agent failed, trying get_or_create...')
        try:
            agent = client.get_or_create_agent(name=AGENT_DISPLAY_NAME)
        except Exception as e2:
            raise
elif kind == 'factory':
    agent = obj(AGENT_DISPLAY_NAME)

print('Agent handle:', type(agent))

# Add Lakehouse data source and select the demo tables
def add_lakehouse(agent, lakehouse_name: Optional[str], tables_to_select):
    ds = None
    # Try a few expected SDK patterns defensively
    for attempt in ('add_datasource','add_data_source','add_lakehouse'):
        try:
            fn = getattr(agent, attempt)
            # Some SDKs infer attached Lakehouse if name is None
            if lakehouse_name:
                ds = fn(lakehouse_name, type='lakehouse')
            else:
                ds = fn(type='lakehouse')
            break
        except Exception as _e:
            ds = None
            continue
    if ds is None:
        print('Could not add lakehouse via SDK; you may need to add it in the UI and rerun selection.')
        return None

    # Try selecting tables (schema name can be 'dbo' in the Lakehouse SQL endpoint; Spark reports 'default')
    for schema in ('dbo','default','public',''):
        for t in tables_to_select:
            try:
                # common patterns: ds.select(schema, table) or ds.select_table(table, schema=...)
                if hasattr(ds, 'select'):
                    ds.select(schema, t)
                elif hasattr(ds, 'select_table'):
                    ds.select_table(t, schema=schema)
            except Exception:
                pass
    return ds

datasource = add_lakehouse(agent, LAKEHOUSE_NAME, FOUND)

# Set instructions
def set_instructions(agent, global_text: str, ds_text: str):
    # global / meta instructions
    for attempt in ('set_instructions','set_global_instructions','update_instructions'):
        try:
            getattr(agent, attempt)(global_text)
            break
        except Exception:
            continue
    # data source scope instructions
    try:
        if datasource is not None:
            for attempt in ('set_instructions','set_datasource_instructions','update_instructions'):
                try:
                    getattr(datasource, attempt)(ds_text)
                    break
                except Exception:
                    continue
    except Exception as e:
        print('Could not set per‑source instructions:', e)

set_instructions(agent, GLOBAL_INSTRUCTIONS, DATA_SOURCE_INSTRUCTIONS)

# Add a few example questions to reduce ambiguity (few‑shot)
EXAMPLES = [
    ("Which 10 opportunities expected to close this month are most likely to slip and why?", 
     "Compute last activity from sales_activities; flag no activity>14d, stage stagnant>21d, or delivery risk notes."),
    ("List renewals due in the next 90 days with low expansion pipeline or high incident volume.", 
     "Sum open Expansion/Project amount next 90d; count support_tickets last 90d; join on customer_id."),
    ("Which customers show rising incident counts over the last 3 months and declining expansion pipeline?", 
     "Aggregate support_tickets by month and compare with sum(amount) of open Expansion/Project opps by month.")
 ]
for q, hint in EXAMPLES:
    for attempt in ('add_example','add_example_query','add_fewshot'):
        try:
            getattr(agent, attempt)(question=q, guidance=hint)
            break
        except Exception:
            continue

# Publish the agent
published_info = None
for attempt in ('publish','publish_agent'):
    try:
        published_info = getattr(agent, attempt)(description='Sales & Support demo agent over Lakehouse tables')
        break
    except Exception:
        continue

print('✅ Data Agent configured. If publish() succeeded, locate it in your workspace Data Agent items UI.')


# CELL ********************

# (Optional) REST API fallback template — requires workspaceId and Fabric OAuth token
# See: Items - Create Data Agent (REST)
# from azure.identity import InteractiveBrowserCredential
# import requests
# WORKSPACE_ID = '00000000-0000-0000-0000-000000000000'
# cred = InteractiveBrowserCredential()
# token = cred.get_token('https://api.fabric.microsoft.com/.default').token
# headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
# body = { 'displayName': AGENT_DISPLAY_NAME, 'description': 'Sales & Support demo agent' }
# r = requests.post(f'https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/DataAgents',
#                   headers=headers, json=body)
# print(r.status_code, r.text)

