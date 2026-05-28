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

%pip install fabric-data-agent-sdk==0.1.21a0 -q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

!pip install -q openai

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
import synapse.ml.spark.aifunc as aifunc

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

print(f"Evaluating agent {data_agent_name}")
evaluation_id = evaluate_data_agent(
    df,
    data_agent_name,
    workspace_name=None,
    table_name=table_name,
    data_agent_stage=data_agent_stage 
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

# set an evaluatio_id
if evaluation_id is None:
    evaluation_id = '46c233a7-828c-45f6-ac18-8fe18e105d30'
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
eval_df = spark.createDataFrame(eval_details[["thread_id", "question", "expected_answer", "actual_answer"]])
eval_df = eval_df.withColumnRenamed('expected_answer','context')
display(eval_df.limit(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Evaluate returned data

# CELL ********************

prompt_execution_accuracy = """
Given the following query and ground truth (provided as a structured table), please determine if the generated answer is equivalent or satifies the ground truth. 
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

Inputs:

Query: {question}

Ground Truth: {context}

Generated Answer: {actual_answer}

Output:
Return this JSON shape: {{"score": "<0-5>", "reason": "short explanation of your judgement"}}
Return ONLY valid JSON, no markdown, no explanation.

"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

eval_df = eval_df[["thread_id", "question", "context", "actual_answer"]].ai.generate_response(prompt=prompt_execution_accuracy, is_prompt_template=True, output_col="execution_accuracy", response_format="text")
display(eval_df.limit(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Evaluate groundedness

# CELL ********************

# prompt_groundedness = """ 
# You are a groundedness judge for a data agent.

# You will receive:
# 1. A user question.
# 2. A context fragment produced from SQL query results and rendered as Markdown.
# 3. An answer generated from that context.

# Your job is to evaluate whether the answer is grounded in the context.

# Use only the provided context. Ignore outside knowledge. The SQL result context is authoritative, but only for what it explicitly shows or strictly entails.

# Important rules:
# - Do not judge whether the answer is useful, fluent, or complete.
# - Do not judge whether the SQL query retrieved the right data.
# - Do not infer beyond the visible Markdown context.
# - Do not treat absent rows as proof of nonexistence unless the context explicitly represents a complete result for the relevant question.
# - Do not treat NULL, empty string, zero, false, and missing data as interchangeable.
# - Be strict with numeric claims, totals, filters, date ranges, rankings, maximums, minimums, and comparisons.
# - Be strict with claims involving “all,” “none,” “only,” “always,” “never,” “highest,” “lowest,” “first,” “last,” or “most.”
# - If the answer appropriately states that the context is insufficient, that statement can be fully grounded.

# Context: {context}

# Question: {question}

# Answer: {actual_answer}

# Assess each meaningful claim in the answer and assign a groundedness score.

# Use this rubric for the evaluation criteria:
# 5 = Fully grounded. Every substantive claim is explicitly supported or strictly entailed.
# 4 = Mostly grounded. Main claims are supported; only minor unsupported details or wording.
# 3 = Partially grounded. Mix of supported and unsupported claims; unsupported content affects interpretation.
# 2 = Weakly grounded. Most important claims are unsupported or over-inferred.
# 1 = Ungrounded. Answer is largely unsupported, contradicted, or based on outside knowledge.

# Output:
# Return this JSON shape: {{"score": "<0-5>", "reason": "short explanation of your judgement"}}
# Return ONLY valid JSON, no markdown, no explanation.

# """

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# eval_df = eval_df[["thread_id", "question", "context", "actual_answer", "execution_accuracy"]].ai.generate_response(prompt=prompt_groundedness, is_prompt_template=True, output_col="groundedness", response_format="text")
# display(eval_df.limit(3))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Evaluate relevance of the answer

# CELL ********************

prompt_relevance = """
You are a strict relevance judge for a data agent.

Given a user question and a generated answer, assign a relevance score from 1 to 5 based only on how well the answer addresses the question.

Ignore factual accuracy, source grounding, writing quality, and citation correctness. Evaluate only semantic relevance to the question.

Scoring rubric:
5 = The answer directly answers the exact question asked.
4 = The answer answers the main question but has minor omissions or irrelevant additions.
3 = The answer is related but incomplete, generic, or only partially addresses the question.
2 = The answer is only loosely related and mostly misses the user’s intent.
1 = The answer is off-topic or does not address the question.

Return only valid JSON:

{{
  "score": 1-5,
  "reason": "short explanation"
}}

Question:
{question}

Answer:
{actual_answer}

"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

eval_df = eval_df[["thread_id", "question", "context", "actual_answer", "execution_accuracy"]].ai.generate_response(prompt=prompt_relevance, is_prompt_template=True, output_col="relevance_accuracy", response_format="text")
display(eval_df.limit(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Present results

# CELL ********************

def pprint(df, n:int=0):
    def parse_json(json_el: str):
        eval_json = ast.literal_eval(str(json_el))
        score = eval_json['score']
        reason = eval_json['reason']
        return score, reason

    question = str(df.iloc[n]["question"])
    context = str(df.iloc[n]["context"])
    actual_answer = str(df.iloc[n]["actual_answer"])

    exec_acc_score, exec_acc_reason = parse_json(str(df.iloc[n]["execution_accuracy"]))
    relevance_score, relevance_reason = parse_json(str(df.iloc[n]["relevance_accuracy"]))
    
    thread_id = str(df.iloc[n]["thread_id"])

    print(f"*** User question #{n}:\n")
    print(f"{question}\n\n")
    print(f"---- Execution accuracy: {exec_acc_score}\n{exec_acc_reason}\n\n")
    print(f"---- Relevance: {relevance_score}\n{relevance_reason}\n\n")
    
    print(f"---- Context\n{context}\n\n")
    print(f"---- Generated answer\n{actual_answer}\n\n")
    print(f"---- Use this thread_id to explore steps: {thread_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

eval_pdf = eval_df.toPandas()
pprint(eval_pdf, 1)

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

pprint_steps('thread_BOXlniwevrAkDknqxRx3YC8z', table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df=_get_data(table_name+'_steps')
df[df.thread_id == 'thread_BOXlniwevrAkDknqxRx3YC8z']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
