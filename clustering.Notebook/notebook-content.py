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

# CELL ********************

%pip install umap-learn hdbscan


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import numpy as np
import pandas as pd
from umap import UMAP
import hdbscan
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from synapse.ml.spark import aifunc as ai
import matplotlib.pyplot as plt

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ground_truth_path = "/lakehouse/default/Files/ground_truth/ground_truth.csv"

# Read the CSV into pandas
pdf = pd.read_csv(ground_truth_path, quotechar='"', lineterminator='\t')

# Ensure we have a string column named 'question'
pdf["question"] = pdf["question"].astype(str)

# Add an id column based on the pandas DataFrame index
pdf = pdf.reset_index(drop=True)
pdf["id"] = pdf.index.astype(int)

# Define explicit schema for Spark DataFrame
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("question", StringType(), True)
])

# Create Spark DataFrame from the pandas columns using the schema
df = spark.createDataFrame(pdf[["id", "question"]], schema=schema)

# Preview first 3 rows
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

embedded_df = df.ai.embed(input_col="question", output_col="embedding")
display(embedded_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# embedded_df.write.mode("overwrite").format("delta").saveAsTable("sentence_embeddings")
# embedded_df = spark.table("sentence_embeddings")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pdf = embedded_df.toPandas()
pdf["embedding"] = pdf["embedding"].apply(lambda v: list(v))

# numpy array of embeddings
X = np.stack(pdf["embedding"].to_numpy())
X.shape

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

umap_model = UMAP(
    n_neighbors=5,
    n_components=5,
    min_dist=0.0,
    metric="cosine",
    random_state=42
)

X_reduced = umap_model.fit_transform(X)

clusterer = hdbscan.HDBSCAN(
    min_cluster_size=2,
    min_samples=1,
    metric="euclidean"
)

labels = clusterer.fit_predict(X_reduced)

pdf["cluster"] = labels
pdf["cluster_probability"] = clusterer.probabilities_

display(
    pdf[["id", "question", "cluster", "cluster_probability"]]
    .sort_values(["cluster", "cluster_probability"], ascending=[True, False])
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

umap_2d = UMAP(
    n_neighbors=15,
    n_components=2,
    min_dist=0.1,
    metric="cosine",
    random_state=42
)

X_2d = umap_2d.fit_transform(X)

pdf["x"] = X_2d[:, 0]
pdf["y"] = X_2d[:, 1]

plt.figure(figsize=(10, 7))
plt.scatter(
    pdf["x"],
    pdf["y"],
    c=pdf["cluster"],
    cmap="tab20",
    s=60
)
plt.title("Sentence clusters from Azure OpenAI embeddings")
plt.xlabel("UMAP 1")
plt.ylabel("UMAP 2")
plt.colorbar(label="Cluster")
plt.show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
