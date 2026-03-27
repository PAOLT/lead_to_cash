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

# # Evaluate sales_agent_lh data agent


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

data_agent_name = "sales_agent_lh"

# output table to store evaluation results
table_name = f"evl.eval_{data_agent_name}"

# "production" (default) or "sandbox"
data_agent_stage = "sandbox"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ground_truth_path = "/lakehouse/default/Files/ground_truth/ground_truth.csv"
df = pd.read_csv(ground_truth_path, quotechar='"', lineterminator='\t')
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

prompt = """
Given the following Query and ground truth, please determine if the most recent answer is equivalent or satifies the ground truth.
The ground truth is expressed as a SQL table, while the answer is expressed in natural language. Use the following rules and return only one score: 1,2,3,4, or 5:
    - 1: Actual Answer is not relevant to Query or it is not coherent to True Dataset (i.e., they cover completely different data points)
    - 2: Actual Answer relevant to Query, but it is not coherent to True Dataset (i.e., they cover quite different data points)
    - 3: Actual Answer relevant to Query, and it is almost coherent to True Dataset (i.e., they cover compatibe data points, but they are different)
    - 4: Actual Answer relevant to Query, and it is mostly coherent to True Dataset (i.e., they cover almost the same  data points, with a few exceptions)
    - 5: Actual Answer is relevant to Query and it is completely coherent to True Dataset (i.e., they cover exactly the same data points)

    Query: {query}

    Expected Answer:
    {expected_answer}
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
    critic_prompt=prompt
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
get_all_rows = True

# Whether to print a summary of the results
verbose = True

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
