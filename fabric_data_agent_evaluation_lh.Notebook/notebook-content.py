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

%pip install fabric-data-agent-sdk==0.1.21a0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import ast
from fabric.dataagent.evaluation import (evaluate_data_agent, 
                                        get_evaluation_summary,
                                        get_evaluation_details,
                                        get_evaluation_summary_per_question
                                    )
from fabric.dataagent.evaluation._storage import _get_data

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

evaluation_id = None

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

# CELL ********************

print(len(df))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Run evaluation

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
Return a JSON object with the following key/value pairs:
- score: a single integer (0, 1, 2, 3, 4, or 5) representing your assigned score following the evaluation criteria
- reason: the explanation of your judgement.

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

#Return only a single integer (0, 1, 2, 3, 4, or 5). Do not include any explanation.

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

# eval_results_df = get_evaluation_summary(table_name)
# display(eval_results_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# set an evaluatio_id
if evaluation_id is None:
    evaluation_id = '59c7e4e9-96d8-4c50-8330-fcc6c86c1c10'
    print("Changing evaluation id --> ", end='')
else:
    print("Using evaluation id of the latest run --> ", end='')
print(f"{evaluation_id}")

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
eval_details[:3]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# parsed_evaluation_messages = [
#     ast.literal_eval(str(msg)) if isinstance(msg, str) else msg
#     for msg in eval_details['evaluation_message']
# ]

# # parsed_evaluation_messages is now a Python list of dictionaries
# for eval in parsed_evaluation_messages:
#     print(f"{eval['score']}\t{eval['reason']}\n\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def pprint(n:int=0):
    question = str(eval_details.iloc[n]["question"])
    expected_answer = str(eval_details.iloc[n]["expected_answer"])
    actual_answer = str(eval_details.iloc[n]["actual_answer"])
    eval_json = ast.literal_eval(str(eval_details.iloc[n]["evaluation_message"]))
    eval_score = eval_json['score']
    eval_reason = eval_json['reason']
    thread_id = str(eval_details.iloc[n]["thread_id"])
    print(f"User question #{n} with score {eval_score}\n\n***Judgement:\n{eval_reason}\n\n***Question:\n{question}\n\n***Expected answer\n{expected_answer}\n\n***Generated answer\n{actual_answer}\n")
    print(f"\n\n***Use this thread_id to explore steps: {thread_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pprint(1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def pprint_steps(thread_id, table_name):
    
    table_name = table_name if table_name[-6:]=='_steps' else table_name+'_steps'
    df = _get_data(table_name)
    df=df[df.thread_id == thread_id]
    # print(f"\n\nfunction_names:\n{str(df.iloc[0]['function_names'])}")
    # print(f"\n\nfunction_queries:\n{str(df.iloc[0]['function_queries'])}")
    # print(f"\n\nfunction_outputs:\n{str(df.iloc[0]['function_outputs'])}")
    sql_steps = str(df.iloc[0]['sql_steps'])
    sql_steps = sql_steps[1:-1].split(",")
    sql_steps = [s.strip() for s in sql_steps if s.strip()!="'None'"]
    
    print("\n\n***SQL_steps:\n")
    for s in sql_steps:
        print(s)
    # print(f"\n\ndax_steps:\n{str(df.iloc[0]['dax_steps'])}")
    # print(f"\n\nkql_steps:\n{str(df.iloc[0]['kql_steps'])}") 

pprint_steps('thread_FwQ4VUJbANBI7BCnwXl2gQIg', table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=_get_data(table_name+'_steps')
df[df.thread_id == 'thread_FwQ4VUJbANBI7BCnwXl2gQIg']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
