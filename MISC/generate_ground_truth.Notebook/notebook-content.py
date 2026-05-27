# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
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

# # Ground Truth Dataset Generation
# 
# This notebook generates an evaluation dataset by defining a small set of core business questions and manually authoring their corresponding SQL queries. Question variants are created by introducing business entity attributes, while SQL logic is adjusted accordingly. An LLM is then used to rephrase the business questions into natural language without changing the SQL. The resulting query outputs are stored as ground truth, using absolute dates to ensure dataset stability.

# MARKDOWN ********************

# ### Libraries and helpers

# CELL ********************

!pip install tabulate

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install -q openai 2>/dev/null

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import os
import re
from typing import Union, List
import synapse.ml.spark.aifunc as aifunc

ground_truth = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_text(t: str)->str:
    '''
        Clean a string
    '''
    t = t.replace('\t', ' ') # removes \t, will be used as line separators in txt files
    t = t.replace('"', r'\"') # escape " with \"
    t = re.sub(r' +', ' ', t) # normalize spaces
    return t.strip()

# clean_text(' this \t \n is "ciccio" a §test \n ')


def clean_md(df: pd.DataFrame, txt: str = None)->str: 
    '''
        Transform a Pandas DataFrame and an otional title in a clean MarkDown fragment
    '''
    t = df.to_markdown(index=False)
    t = t.replace('|-', "|:-")
    t = t.replace(':|', "|")
    t = t.replace("| |", "|\n|")
    
    if txt:
        txt = txt if txt[-1]==":" else txt + ':'
        t = txt + '\n\n' + t

    return clean_text(t)

# clean_md(gt1, 'this is a md')

def df_to_md(dfs: Union[pd.DataFrame, List[pd.DataFrame]], 
            texts: Union[str, List[str]] = None)->str:
    '''
        Transform a (list of) Pandas dataframe(s) and a (list of) title(s) to a MarkDown fragment
    '''
    text = None
    dfs = dfs if type(dfs)==list else [dfs]
    if texts:
        texts = texts if type(texts)==list else [texts]
        assert len(dfs)==len(texts)
    else:
        texts = [None]*len(dfs)
    
    for df, txt in zip(dfs, texts): 
        t = clean_md(df, txt)
        text = text + '\n\n' + t if text else t
    
    return text

# df_to_md([gt1, gt1], ['this is a md', 'this is another md'])

def sqls_to_str(sqls: Union[List[str], str])->str:
    if type(sqls)==list:
        sqls = ";\n".join(sqls)
    return clean_text(sqls)

# sqls_to_str(['select * from T', 'select a,b,c from K where a=1'])

def make_item(question: str, 
                sqls: str, 
                dfs: Union[pd.DataFrame, List[pd.DataFrame]],
                texts: Union[str, List[str]] = None) -> dict:
    '''
        Transform a test case into a dictionary where:
        - question provides the test's question
        - sql provides the test's SQL statement
        - dfs provides a list of Pandas dataframes (or a single Pandas dataframe) being the result of running SQL over data
        - texts (optional) provides a list of strings (or a single string) as a title for Pandas dataframe(s)
    '''

    item = {}
    item["question"] = clean_text(question)
    item["sql"] = sqls_to_str(sqls)
    item["result"] = df_to_md(dfs, texts)
    return item

# make_item(q1, [sql1,sql1], [gt1, gt1], ['this is a md', 'this is another md'])

def run_case(q: str, sql: str):
    _gt = spark.sql(sql).toPandas()
    return _gt, make_item(q, sql, _gt)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Seed business questions with SQL statements

# MARKDOWN ********************

# #### Q1: top 10 opportunities most likely to slip in February 2026 and why


# CELL ********************

q1a = '''
top 10 opportunities most likely to slip in February 2026 and why. 

Opportunities can slip due to either of the following:
- stage stagnation: opportunity stage didn't change in the last 21 days 
- no activity: no sales activities in last 14 days,
- delivery at risk: opportunity notes indicating risk in the last 60 days

Return the following fields for each opportunity being at risk of slipping:
- opportunity ID
- customer name
- product name
- opportunity expected close date
- opportunity stage
- forecast category
- opportunity size
- reason for being at risk of slipping

Answer only with a table and keep the reason field as much concise as possible.
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql1a = """
WITH last_sales_activity AS (
  SELECT opp_id, MAX(activity_at) AS last_activity_at
  FROM sales_activities
  GROUP BY opp_id
), risk_notes AS (
  SELECT DISTINCT n.opp_id
  FROM opportunity_notes n
  WHERE n.note_at >= DATEADD(day, -60, CAST('2026-02-01' AS date))
    AND (n.tags LIKE '%risk%' OR n.note_type = 'risk')
), recent_high_sev_tickets AS (
  SELECT DISTINCT st.customer_id, st.product_id
  FROM support_tickets st
  WHERE st.opened_at >= DATEADD(day, -60, CAST('2026-02-01' AS date))
    AND st.status = 'Open'
    AND st.severity IN ('High','Critical')
) 
SELECT o.opp_id, o.type, c.customer_name, p.product_name, o.stage, o.forecast_category, o.amount, o.expected_close_date,
         CASE WHEN lsa.last_activity_at IS NULL OR DATEDIFF(day, lsa.last_activity_at, '2026-02-15') > 14 THEN 1 ELSE 0 END AS no_activity_flag,
         CASE WHEN DATEDIFF(day, o.stage_last_changed_at, '2026-02-15') > 21 THEN 1 ELSE 0 end AS stage_stagnant_flag,
         CASE WHEN rn.opp_id IS NOT NULL OR rht.customer_id IS NOT NULL THEN 1 ELSE 0 END AS delivery_risk_flag
  FROM sales_opportunities o
  LEFT JOIN last_sales_activity lsa ON lsa.opp_id = o.opp_id
  LEFT JOIN risk_notes rn ON rn.opp_id = o.opp_id
  LEFT JOIN recent_high_sev_tickets rht ON rht.customer_id = o.customer_id AND rht.product_id = o.product_id
  LEFT JOIN customers c on c.customer_id = o.customer_id
  LEFT JOIN products p on p.product_id = o.product_id
  WHERE o.status = 'Open'
    AND YEAR(o.expected_close_date)=YEAR('2026-02-01') AND MONTH(o.expected_close_date)=MONTH('2026-02-01')
LIMIT 10;
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_df, item = run_case(q1a, sql1a)
ground_truth.append(item)
_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

q1b = '''
top 10 opportunities for product "Loan Origination" most likely to slip in February 2026 and why. 

Opportunities can slip due to either of the following:
- stage stagnation: opportunity stage didn't change in the last 21 days 
- no activity: no sales activities in last 14 days,
- delivery at risk: opportunity notes indicating risk in the last 60 days

Return the following fields for each opportunity being at risk of slipping:
- opportunity ID
- customer name
- product name
- opportunity expected close date
- opportunity stage
- forecast category
- opportunity size
- reason for being at risk of slipping

Answer only with a table and keep the reason field as much concise as possible.
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql1b = """
WITH last_sales_activity AS (
  SELECT opp_id, MAX(activity_at) AS last_activity_at
  FROM sales_activities
  GROUP BY opp_id
), risk_notes AS (
  SELECT DISTINCT n.opp_id
  FROM opportunity_notes n
  WHERE n.note_at >= DATEADD(day, -60, CAST('2026-02-01' AS date))
    AND (n.tags LIKE '%risk%' OR n.note_type = 'risk')
), recent_high_sev_tickets AS (
  SELECT DISTINCT st.customer_id, st.product_id
  FROM support_tickets st
  WHERE st.opened_at >= DATEADD(day, -60, CAST('2026-02-01' AS date))
    AND st.status = 'Open'
    AND st.severity IN ('High','Critical')
) 
SELECT o.opp_id, o.type, c.customer_name, p.product_name, o.stage, o.forecast_category, o.amount, o.expected_close_date,
         CASE WHEN lsa.last_activity_at IS NULL OR DATEDIFF(day, lsa.last_activity_at, '2026-02-15') > 14 THEN 1 ELSE 0 END AS no_activity_flag,
         CASE WHEN DATEDIFF(day, o.stage_last_changed_at, '2026-02-15') > 21 THEN 1 ELSE 0 end AS stage_stagnant_flag,
         CASE WHEN rn.opp_id IS NOT NULL OR rht.customer_id IS NOT NULL THEN 1 ELSE 0 END AS delivery_risk_flag
  FROM sales_opportunities o
  LEFT JOIN last_sales_activity lsa ON lsa.opp_id = o.opp_id
  LEFT JOIN risk_notes rn ON rn.opp_id = o.opp_id
  LEFT JOIN recent_high_sev_tickets rht ON rht.customer_id = o.customer_id AND rht.product_id = o.product_id
  LEFT JOIN customers c on c.customer_id = o.customer_id
  LEFT JOIN products p on p.product_id = o.product_id
  WHERE o.status = 'Open'
    AND YEAR(o.expected_close_date)=YEAR('2026-02-01') AND MONTH(o.expected_close_date)=MONTH('2026-02-01')
    AND p.product_name = "Loan Origination"
LIMIT 10;
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_df, item = run_case(q1b, sql1b)
ground_truth.append(item)
_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

q1c = '''
top 10 opportunities for product "Account Ledger" most likely to slip in November 2025 and why. 

Return the following fields for each opportunity being at risk of slipping:
- opportunity ID
- customer name
- product name
- opportunity expected close date
- opportunity stage
- forecast category
- opportunity size
- reason for being at risk of slipping

Answer only with a table and keep the reason field as much concise as possible.
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql1c = """
WITH last_sales_activity AS (
  SELECT opp_id, MAX(activity_at) AS last_activity_at
  FROM sales_activities
  GROUP BY opp_id
), risk_notes AS (
  SELECT DISTINCT n.opp_id
  FROM opportunity_notes n
  WHERE n.note_at >= DATEADD(day, -60, CAST('2026-02-01' AS date))
    AND (n.tags LIKE '%risk%' OR n.note_type = 'risk')
), recent_high_sev_tickets AS (
  SELECT DISTINCT st.customer_id, st.product_id
  FROM support_tickets st
  WHERE st.opened_at >= DATEADD(day, -60, CAST('2026-02-01' AS date))
    AND st.status = 'Open'
    AND st.severity IN ('High','Critical')
) 
SELECT o.opp_id, o.type, c.customer_name, p.product_name, o.stage, o.forecast_category, o.amount, o.expected_close_date,
         CASE WHEN lsa.last_activity_at IS NULL OR DATEDIFF(day, lsa.last_activity_at, '2026-02-15') > 14 THEN 1 ELSE 0 END AS no_activity_flag,
         CASE WHEN DATEDIFF(day, o.stage_last_changed_at, '2026-02-15') > 21 THEN 1 ELSE 0 end AS stage_stagnant_flag,
         CASE WHEN rn.opp_id IS NOT NULL OR rht.customer_id IS NOT NULL THEN 1 ELSE 0 END AS delivery_risk_flag
  FROM sales_opportunities o
  LEFT JOIN last_sales_activity lsa ON lsa.opp_id = o.opp_id
  LEFT JOIN risk_notes rn ON rn.opp_id = o.opp_id
  LEFT JOIN recent_high_sev_tickets rht ON rht.customer_id = o.customer_id AND rht.product_id = o.product_id
  LEFT JOIN customers c on c.customer_id = o.customer_id
  LEFT JOIN products p on p.product_id = o.product_id
  WHERE o.status = 'Open'
    AND YEAR(o.expected_close_date)=YEAR('2025-11-01') AND MONTH(o.expected_close_date)=MONTH('2025-11-01')
    AND p.product_name = "Account Ledger"
LIMIT 10;
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_df, item = run_case(q1c, sql1c)
ground_truth.append(item)
_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Q2: Renewals at risk in February, March, April 2026, due to low expansion or high incidents


# CELL ********************

q2a = '''
Renewals at risk in February, March, April 2026, due to low expansion or high incidents

Opportunities can slip due to either of the following:
- Consider low expansion when open opportunities for expansion or project aggregated value is lower than 100000
- Consider high incidents volume when there are at least 3 open incidents of any severity

Return the following fields for each renewal opportunity at risk:
- opportunity ID
- customer name
- product name
- opportunity expected close date
- opportunity amount
- opportunity type
- expansion amount
- num incidents
- opportunity size
- reason for being at risk

Answer only with a table and keep the reason field as much concise as possible.
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql2a = """
WITH renewals AS (
  SELECT * FROM sales_opportunities
  WHERE type = 'Renewal' AND MONTH(expected_close_date) BETWEEN 9 AND 12 AND YEAR(expected_close_date)=2025
), expansion_pipeline AS (
  SELECT customer_id, SUM(amount) AS expansion_amount
  FROM sales_opportunities
  WHERE status='Open' AND type IN ('Expansion','Project')
  GROUP BY customer_id
), incidents AS (
  SELECT customer_id, COUNT(*) AS num_incidents
  FROM support_tickets
  WHERE status='Open' AND severity IN ('High', 'Critical')
  GROUP BY customer_id
)
SELECT r.opp_id, c.customer_name, p.product_name, r.expected_close_date, r.amount, r.type,
       COALESCE(ep.expansion_amount,0) AS expansion_amount,
       COALESCE(i.num_incidents,0) AS num_incidents,
       CASE WHEN COALESCE(ep.expansion_amount,0) < 100000 THEN 1 ELSE 0 END AS low_expansion_flag,
       CASE WHEN COALESCE(i.num_incidents,0) >= 3 THEN 1 ELSE 0 END AS high_incident_flag
FROM renewals r
LEFT JOIN expansion_pipeline ep ON ep.customer_id = r.customer_id
LEFT JOIN incidents i ON i.customer_id = r.customer_id
LEFT JOIN customers c on c.customer_id = r.customer_id
  LEFT JOIN products p on p.product_id = r.product_id
WHERE COALESCE(ep.expansion_amount,0) < 100000 OR COALESCE(i.num_incidents,0) >= 3
"""


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_df, item = run_case(q2a, sql2a)
ground_truth.append(item)
_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

q2b = '''
Renewals for product "Treasury Manager" at risk in February, March, April 2026, due to low expansion or high incidents

Opportunities can slip due to the default policy.

Return the following fields for each renewal opportunity at risk:
- opportunity ID
- customer name
- product name
- opportunity expected close date
- opportunity amount
- opportunity type
- expansion amount
- num incidents
- opportunity size
- reason for being at risk

Answer only with a table and keep the reason field as much concise as possible.
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql2b = """
WITH renewals AS (
  SELECT * FROM sales_opportunities
  WHERE type = 'Renewal' AND MONTH(expected_close_date) BETWEEN 9 AND 12 AND YEAR(expected_close_date)=2025
), expansion_pipeline AS (
  SELECT customer_id, SUM(amount) AS expansion_amount
  FROM sales_opportunities
  WHERE status='Open' AND type IN ('Expansion','Project')
  GROUP BY customer_id
), incidents AS (
  SELECT customer_id, COUNT(*) AS num_incidents
  FROM support_tickets
  WHERE status='Open' AND severity IN ('High', 'Critical')
  GROUP BY customer_id
)
SELECT r.opp_id, c.customer_name, p.product_name, r.expected_close_date, r.amount, r.type,
       COALESCE(ep.expansion_amount,0) AS expansion_amount,
       COALESCE(i.num_incidents,0) AS num_incidents,
       CASE WHEN COALESCE(ep.expansion_amount,0) < 100000 THEN 1 ELSE 0 END AS low_expansion_flag,
       CASE WHEN COALESCE(i.num_incidents,0) >= 3 THEN 1 ELSE 0 END AS high_incident_flag
FROM renewals r
LEFT JOIN expansion_pipeline ep ON ep.customer_id = r.customer_id
LEFT JOIN incidents i ON i.customer_id = r.customer_id
LEFT JOIN customers c on c.customer_id = r.customer_id
  LEFT JOIN products p on p.product_id = r.product_id
WHERE (COALESCE(ep.expansion_amount,0) < 100000 OR COALESCE(i.num_incidents,0) >= 3)
AND p.product_name = "Treasury Manager"
"""


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_df, item = run_case(q2b, sql2b)
ground_truth.append(item)
_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

q2c = '''
Renewals at risk in February, March, April 2026, due to low expansion or high incidents

Opportunities can slip due to either of the following:
- Consider low expansion when open opportunities for expansion or project aggregated value is lower than 50000
- Consider high incidents volume when there are at least 4 open incidents of any severity

Return the following fields for each renewal opportunity at risk:
- opportunity ID
- customer name
- product name
- opportunity expected close date
- opportunity amount
- opportunity type
- expansion amount
- num incidents
- opportunity size
- reason for being at risk

Answer only with a table and keep the reason field as much concise as possible.
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql2c = """
WITH renewals AS (
  SELECT * FROM sales_opportunities
  WHERE type = 'Renewal' AND MONTH(expected_close_date) BETWEEN 9 AND 12 AND YEAR(expected_close_date)=2025
), expansion_pipeline AS (
  SELECT customer_id, SUM(amount) AS expansion_amount
  FROM sales_opportunities
  WHERE status='Open' AND type IN ('Expansion','Project')
  GROUP BY customer_id
), incidents AS (
  SELECT customer_id, COUNT(*) AS num_incidents
  FROM support_tickets
  WHERE status='Open' AND severity IN ('High', 'Critical')
  GROUP BY customer_id
)
SELECT r.opp_id, c.customer_name, p.product_name, r.expected_close_date, r.amount, r.type,
       COALESCE(ep.expansion_amount,0) AS expansion_amount,
       COALESCE(i.num_incidents,0) AS num_incidents,
       CASE WHEN COALESCE(ep.expansion_amount,0) < 100000 THEN 1 ELSE 0 END AS low_expansion_flag,
       CASE WHEN COALESCE(i.num_incidents,0) >= 3 THEN 1 ELSE 0 END AS high_incident_flag
FROM renewals r
LEFT JOIN expansion_pipeline ep ON ep.customer_id = r.customer_id
LEFT JOIN incidents i ON i.customer_id = r.customer_id
LEFT JOIN customers c on c.customer_id = r.customer_id
  LEFT JOIN products p on p.product_id = r.product_id
WHERE COALESCE(ep.expansion_amount,0) < 50000 OR COALESCE(i.num_incidents,0) >= 4
"""


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_df, item = run_case(q2c, sql2c)
ground_truth.append(item)
_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Q3 - Accelerate opportunities closing H1CY266

# CELL ********************

q3a = '''
I need a list of sales opportunities to accelerate. Consider the following opportunities:
- open opportunities with expected close date in H2 2025
- must be in *Proposal* state
- must have at least one delivered sales activity 
- the last sales note type must not be "Risk"
- forecast must be set to True

For each of them, return the following:
- opportunity ID
- customer name
- expected close date
- forecast flag
- product name
- a summary of sales notes
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql3a = """
WITH notes AS (
SELECT
  opp_id, 
  max_by(note_type, note_at) AS last_signal,
  concat_ws(
    ';\n\n',
    transform(
      array_sort(
        collect_list(named_struct('d', note_at, 's',
          concat(date_format(note_at, 'M/d/yyyy'), ': ', note_text)
        ))
      ),
      x -> x.s
    )
  ) AS opp_notes
FROM opportunity_notes
GROUP BY opp_id
), activities AS (
  SELECT opp_id, count(*) AS num_activities
  FROM sales_activities
  GROUP BY opp_id
) 

SELECT o.opp_id, c.customer_name, o.expected_close_date, o.is_forecast, p.product_name, n.last_signal, n.opp_notes
FROM customers c
LEFT JOIN sales_opportunities o ON o.customer_id = c.customer_id
LEFT JOIN notes n ON n.opp_id = o.opp_id
LEFT JOIN activities a on a.opp_id = o.opp_id
LEFT JOIN products p on o.product_id = p.product_id
WHERE o.status = 'Open' AND Stage = 'Proposal' AND (n.last_signal IS NULL OR n.last_signal != 'risk') AND a.num_activities>=1 AND MONTH(o.expected_close_date)>6 AND YEAR(o.expected_close_date)=2025
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_df, item = run_case(q3a, sql3a)
ground_truth.append(item)
_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

q3b = '''
I need a list of sales opportunities for product line "Core Banking Suite" to accelerate. Consider the following opportunities:
- open opportunities with expected close date in 2025
- must be in *Proposal* state
- must have at least one delivered sales activity 
- the last sales note type must not be "Risk"
- forecast must be set to True

For each of them, return the following:
- opportunity ID
- customer name
- expected close date
- forecast flag
- product_line
- product name
- a summary of sales notes
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql3b = """
WITH notes AS (
SELECT
  opp_id, 
  max_by(note_type, note_at) AS last_signal,
  concat_ws(
    ';\n\n',
    transform(
      array_sort(
        collect_list(named_struct('d', note_at, 's',
          concat(date_format(note_at, 'M/d/yyyy'), ': ', note_text)
        ))
      ),
      x -> x.s
    )
  ) AS opp_notes
FROM opportunity_notes
GROUP BY opp_id
), activities AS (
  SELECT opp_id, count(*) AS num_activities
  FROM sales_activities
  GROUP BY opp_id
) 

SELECT o.opp_id, c.customer_name, o.expected_close_date, o.is_forecast, p.product_line, p.product_name, n.last_signal, n.opp_notes
FROM customers c
LEFT JOIN sales_opportunities o ON o.customer_id = c.customer_id
LEFT JOIN notes n ON n.opp_id = o.opp_id
LEFT JOIN activities a on a.opp_id = o.opp_id
LEFT JOIN products p on o.product_id = p.product_id
WHERE o.status = 'Open' AND Stage = 'Proposal' AND (n.last_signal IS NULL OR n.last_signal != 'risk') AND a.num_activities>=1 
AND YEAR(o.expected_close_date)=2025 AND p.product_line="Core Banking Suite"
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_df, item = run_case(q3b, sql3b)
ground_truth.append(item)
_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

q3c = '''
I need a list of sales opportunities for product line "Core Banking Suite" to accelerate. Consider the following opportunities:
- open opportunities with expected close date in 2025
- must be in *Proposal* state
- must have at least one delivered sales activity of type demo
- forecast must be set to True

For each of them, return the following:
- opportunity ID
- customer name
- expected close date
- forecast flag
- product_line
- product name
- a summary of sales notes
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql3c = """
WITH activities AS (
  SELECT opp_id, count(*) AS num_activities
  FROM sales_activities
  WHERE type IN ('demo')
  GROUP BY opp_id
) 
SELECT o.opp_id, c.customer_name, o.expected_close_date, o.is_forecast, p.product_name
FROM customers c
LEFT JOIN sales_opportunities o ON o.customer_id = c.customer_id
LEFT JOIN activities a on a.opp_id = o.opp_id
LEFT JOIN products p on o.product_id = p.product_id
WHERE o.status = 'Open' AND Stage = 'Proposal' AND a.num_activities>=1 AND YEAR(o.expected_close_date)=2025
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_df, item = run_case(q3c, sql3c)
ground_truth.append(item)
_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Add more cases with equivalent queries

# CELL ********************

df = spark.createDataFrame(ground_truth)
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prompt = """
Rephrase the following user prompt. Do not change anything specific, keep the same semantic and data.
Just provide a rephrased text, using different words. Do not add any specific JSON structure.

User prompt:
{question}
"""
df = df.ai.generate_response(prompt=prompt, is_prompt_template=True, output_col="question2", response_format="text")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prompt = """
Generate a user prompt equivalent to the ones provided below. Do not change anything specific, keep the same semantic and data. 
Just provide a rephrased text, using different words. Do not add any specific JSON structure.

User prompt 1:
{question}

User prompt 2:
{question2}
"""

df = df[["question", "result", "sql", "question2"]].ai.generate_response(prompt=prompt, is_prompt_template=True, output_col="question3", response_format="text")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

ground_truth_df = df.select(
    F.expr("""
        stack(3,
              'question1', question,
              'question2', question2,
              'question3', question3
        ) as (question_col, question)
    """),
    F.col("sql"),
    F.col("result")
)

ground_truth_df = ground_truth_df.drop("question_col")
display(ground_truth_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Save ground truth to a file

# CELL ********************

output_path = "/lakehouse/default/Files/ground_truth/ground_truth.csv"
os.makedirs(os.path.dirname(output_path), exist_ok=True)

ground_truth_df.toPandas().to_csv(output_path, index=False, quotechar='"', lineterminator='\t')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = pd.read_csv(output_path, quotechar='"', lineterminator='\t')
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

len(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
