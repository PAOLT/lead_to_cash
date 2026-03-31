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

# # Delta - CSV helpers
# 
# Store CSV files as Tables and store tables as CSV

# MARKDOWN ********************

# ### Imports and settings

# CELL ********************

import pandas as pd
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_names=["csat_by_month", "customers", "opportunity_notes", "products", "sales_activities", "sales_opportunities", "support_activities", "support_tickets"]
# output_dir = "ops_data/Files/data_exp"
output_dir = "/lakehouse/default/Files/data_exp"
lakehouse_name = 'ops_data'
schema_name = 'dbo'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Store Delta Tables as CSV lakehouse files

# CELL ********************

os.makedirs(output_dir, exist_ok=True)

for table_name in table_names:
    print(f"{table_name}...", end=" ")
    file_name = f"{table_name}.csv"
    output_path = os.path.join(output_dir, file_name)

    try: 
        pdf=spark.table(f"{schema_name}.{table_name}").toPandas()
        pdf.to_csv(output_path, sep='\t', index=False)
        print("OK")
    except Exception as e:
        print("\n\n")
        print(e)
        raise Exception

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Doublecheck**

# CELL ********************

for table_name in table_names:
    print(f"{table_name}...", end=" ")
    file_name = f"{table_name}.csv"
    output_path = os.path.join(output_dir, file_name)

    pdf = pd.read_csv(output_path, sep='\t')
    print(f"{len(pdf)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Save CSV files as Delta tables

# CELL ********************

from pyspark.sql import Row

schemas = {}

schemas["customers"] = T.StructType([
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("customer_name", T.StringType(), False),
    T.StructField("country", T.StringType(), True)
])

schemas["products"] = T.StructType([
    T.StructField("product_id", T.StringType(), False),
    T.StructField("product_name", T.StringType(), False),
    T.StructField("product_line", T.StringType(), False)
])

schemas["csat_by_month"] = T.StructType([
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("month", T.DateType(), False),
    T.StructField("csat", T.IntegerType(), False)
])

schemas["support_tickets"] = T.StructType([
    T.StructField("ticket_id", T.StringType(), False),
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("product_id", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("opened_at", T.DateType(), False),
    T.StructField("closed_at", T.DateType(), True),
    T.StructField("severity", T.StringType(), False),
    T.StructField("priority", T.StringType(), False),
    T.StructField("channel", T.StringType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("sla_breach_flag", T.BooleanType(), True),
    T.StructField("assigned_group", T.StringType(), True)
])

schemas["support_activities"] = T.StructType([
    T.StructField("activity_id", T.StringType(), False),
    T.StructField("ticket_id", T.StringType(), False),
    T.StructField("activity_at", T.DateType(), False),
    T.StructField("description", T.StringType(), False),
    T.StructField("author", T.StringType(), True),
    T.StructField("activity_type", T.StringType(), True),
    T.StructField("minutes_spent", T.IntegerType(), True)
])

schema["sales_opportunities"] = T.StructType([
    T.StructField("opp_id", T.StringType(), False),
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("product_id", T.StringType(), False),
    T.StructField("type", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("stage", T.StringType(), False),
    T.StructField("opened_at", T.DateType(), False),
    T.StructField("expected_close_date", T.DateType(), False),
    T.StructField("closed_at", T.DateType(), True),
    T.StructField("is_forecast", T.BooleanType(), False),
    T.StructField("forecast_category", T.StringType(), True),
    T.StructField("stage_last_changed_at", T.DateType(), False),
    T.StructField("amount", T.DoubleType(), False),
    T.StructField("currency", T.StringType(), False),
    T.StructField("probability", T.IntegerType(), False),
    T.StructField("renewal_term_months", T.IntegerType(), True)
])

schema["sales_activities"] = T.StructType([
    T.StructField("activity_id", T.StringType(), False),
    T.StructField("opp_id", T.StringType(), False),
    T.StructField("activity_at", T.DateType(), False),
    T.StructField("description", T.StringType(), False),
    T.StructField("type", T.StringType(), True),
    T.StructField("contact_name", T.StringType(), True)
])

schema["opportunity_notes"] = T.StructType([
    T.StructField("note_id", T.StringType(), False),
    T.StructField("opp_id", T.StringType(), False),
    T.StructField("note_at", T.DateType(), False),
    T.StructField("note_type", T.StringType(), False),
    T.StructField("note_text", T.StringType(), False),
    T.StructField("tags", T.StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table_name in table_names:
    print(f"{table_name}...", end=" ")
    file_name = f"{table_name}.csv"
    output_path = os.path.join(output_dir, file_name)

    pdf = pd.read_csv(output_path, sep='\t')
    df_customers = spark.createDataFrame(pdf, schemas[table_name])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
