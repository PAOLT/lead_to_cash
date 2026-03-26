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

# # Fabric Data Agent Evaluation Notebook


# MARKDOWN ********************

# ### Libraries and helpers

# CELL ********************

%pip install -U fabric-data-agent-sdk

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
from fabric.dataagent.evaluation import evaluate_data_agent
from fabric.dataagent.evaluation import get_evaluation_summary
from fabric.dataagent.evaluation import get_evaluation_details

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Name of your Data Agent
# data_agent_name = "FinServ Sales Agent Onto"
data_agent_name = "FinServ Sales & Support Agent"

# Name of the output table to store evaluation results (default: "evaluation_output")
table_name = f"eval_{data_agent_name}"

# Specify the Data Agent stage: "production" (default) or "sandbox"
data_agent_stage = "sandbox"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

round_truth_path = "/lakehouse/default/Files/ground_truth/ground_truth.csv"
df = pd.read_csv(round_truth_path, quotechar='"', lineterminator='\t')
df = df.rename(columns={'result': 'expected_answer'})
df = df[['question', 'expected_answer']]
display(df[:3])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Run evaluation

# CELL ********************

# Define a custom prompt for evaluating agent responses
# critic_prompt = """
#         Given the following query and ground truth, please determine if the most recent answer is equivalent or satifies 
#         the ground truth. If they are numerically and semantically equivalent or satify (even with reasonable rounding), 
#         respond with "Yes". If they clearly differ, respond with "No". If it is ambiguous or unclear, respond with "Unclear". 
#         Return only one word: Yes, No, or Unclear..

#         Query: {query}

#         Ground Truth: {expected_answer}
# """

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

critic_prompt = """
Given the following question and ground truth answer (expressed as a SQL table), evaluate the validity of the answer provided to the question. Using your understanding of semantics and data, assign a single score from 1 to 4 according to these guidelines:

1: The answer is not relevant to the question.
2: The answer is somewhat relevant, but substantially diverges from the ground truth data.
3: The answer is relevant and largely coherent with the ground truth, but exhibits minor differences.
4: The answer is fully relevant and entirely consistent with the ground truth data.
Question: {query}

Ground Truth: {expected_answer}

Return only the score (1, 2, 3, or 4).
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

evaluation_id = evaluate_data_agent(
    df,
    data_agent_name,
    table_name=table_name,
    data_agent_stage=data_agent_stage,
    critic_prompt=critic_prompt
)

print(f"Unique ID for the current evaluation run: {evaluation_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Check eval

# CELL ********************

eval_results_df = get_evaluation_summary(table_name)
display(eval_results_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Whether to return all evaluation rows (True) or only failures (False)
get_all_rows = False

# Whether to print a summary of the results
verbose = True

# Retrieve evaluation details for a specific run
eval_details = get_evaluation_details(
    evaluation_id,
    table_name,
    get_all_rows=get_all_rows,
    verbose=verbose
)
eval_details

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
