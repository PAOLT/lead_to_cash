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
from fabric.dataagent.evaluation import (evaluate_data_agent, 
                                        get_evaluation_summary,
                                        get_evaluation_details
                                    )

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

# prompt = """
# A user question and an expected answer are provided below. The expected answer is provided as a structured table. An actual answer given by an agent is also provided. You need to assess at what extent the actual answer is equivalent to the expected answer.
# To do the assessment, use the following rules to provide a numeric score (i.e., 0, 1, 2, 3, 4, 5):

# If the actual answer is empty, or it states that it was impossible to obtain an answer from the agent for whatever reason, answer with "0".

# If the actual answer is a given answer, but it is not relevant to the user question, answer with "1".

# If the actual answer is a given and it is relevant to the user question, consider the following cases:
#     - if the actual answer and the expected answer cover completely different data points, answer with "2"
#     - if the actual answer and the expected answer cover compatibe data points but they are numerically different, answer with "3"
#     - if the actual answer and the expected answer cover almost the same data points with a few exceptions, answer with "4"
#     - if the actual answer and the expected answer cover exactly the same data points, answer with "5"

# Answer only with a single integer number (0 to 5)

#     User question: 
#     {query}

#     Expected Answer:
#     {expected_answer}
# """

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prompt = """
Given the following query and ground truth (provided as a structured table), please determine if the most recent answer is equivalent or satifies the ground truth. 
You are an evaluator. Your task is to compare the most recent answer with an expected answer , and assign a similarity score from 0 to 5.

Evaluation criteria:

Score = 0  
The most recent answer is empty, missing, or explicitly states that no answer could be generated.

Score = 1  
The most recent answer is provided but is not relevant to the user question.

Score = 2  
The most recent answer is relevant, but it covers completely different data points than the ground truth.

Score = 3  
The most recent answer covers comparable data points, but there are significant numerical or factual discrepancies.

Score = 4  
The most recent answer largely matches the ground truth, with only minor omissions or deviations.

Score = 5  
The most recent answer fully matches the ground truth, covering the same data points accurately and completely.

Instructions:
- Focus only on semantic equivalence between the expected and the most recent answer.
- Do not consider formatting differences unless they affect meaning.
- Base your judgment strictly on the content provided.

Output:
Return only a single integer (0, 1, 2, 3, 4, or 5). Do not include any explanation.

Inputs:

Query: {query}

Ground Truth: {expected_answer}
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Evaluating agent {data_agent_name}")
evaluation_id = evaluate_data_agent(
    df,
    data_agent_name,
    workspace_name=None,
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
verbose = False

eval_details = get_evaluation_details(
    evaluation_id,
    table_name,
    get_all_rows=get_all_rows,
    verbose=verbose
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

eval_details.iloc[0:][["question", "expected_answer", "actual_answer", "evaluation_message"]]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
