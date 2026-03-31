# Experimenting with Data Agents and Ontologies in Fabric IQ

Accelerating Data Agent initiatives typically requires overcoming common challenges: sourcing or generating use‑case‑representative datasets; iterating on semantic modeling strategies—i.e., Delta Tables, Semantic Models, and Ontologies—and agent instructions; and defining a robust evaluation framework to verify alignment with business objectives. *Lead to Cash* (L2C) is an example of experimenting with Data Agents and Fabric IQ, providing reusable yet simple assets that can be adapted to your own scenarios.

## Lead‑to‑Cash

The *Lead‑to‑Cash* (L2C) example refers to managing sales opportunities in a software company. Specifically, L2C covers:
- Sales pipeline health
- Opportunity lifecycle progression and risk
- Customer renewals
- Support performance and customer satisfaction

The following schema represents the provided ontology:

![Lead to Cash Ontology definition](l2c_onto.png)

Typical business prompts include:
- Top 10 opportunities most likely to slip in February 2026 and why
- Renewals at risk in Q1 2026 due to low expansion or high incident rates
- Which opportunities closing in H1 2026 can be accelerated

## Provision L2C in your own Fabric

Do the following:
- Fork this repo
- In Fabric create a workspace and link it to the forked git repo
- Use the provided notebook `MISC/delta_csv.ipynb` to load data files in `sample_data/` to the `ops_data` LakeHouse

The following sample assets are covered in this repository:
- The L2C sample data in a LakeHouse (`ops_data`)
- L2C Ontology, as per the above picture (`sales_onto`)
- Two Fabric Data Agents, one relying on LakeHouse data (`sales_agent_lh`) and one relying on the Ontology (`sales_agent_onto`)
- A ground truth dataset (`ground_truth.csv`) and evaluation notebook (`fabric_data_agent_evaluation.ipynb`). 

## Reuse L2C with your own use case

The same methodology used in L2C can be reused and improved in many different scenarios. Specifically:

**Generation of Data, Ontology and Data Agents**
If you already have data, simply generate the ontology from the Fabric portal. Otherwise, data and ontology can be generated together with vibe coding - follow this [video](https://github.com/microsoft/Fabric-IQ-and-Real-Time-Intelligence-assets/blob/main/Repo%20assets/).

Data Agents can be generated easily in the portal or reusing the provided notebooks (`setup_data_agent_lh.ipynb` and `setup_data_agent_onto.ipynb`). It is worth considering to test Data Agents with Semantic Models: although not provided, it should be immediate to do.

**Evaluation**

Generating high‑quality ground truth is non‑trivial. We propose the following methodology, implemented in a reusable notebook (`generate_ground_truth.ipynb`):
- Seed a small set of manually curated test cases, each pairing a business prompt with a query used to retrieve the relevant data. In the L2C example, this consists of three business prompts and their associated Lakehouse SQL queries.
- Manually derive simple variations from the seed cases. Although such variations could be auto‑generated, doing so would require additional validation. The L2C example uses manual variations (e.g., adjusting time horizons, filtering by different products, or overriding policies defined in Data Agent instructions).
- Use an LLM to generate prompt rephrasings while keeping the underlying queries unchanged.

For example, the L2C example starts from three seed test cases, each expanded with three manual variations and three LLM‑generated rephrasings, resulting in a total of 27 test cases.

Next, evaluate your Data Agents with the generated ground truth dataset, by reusing `fabric_data_agent_evaluation.ipynb` (it uses the Fabric Data Agent SDK with a custom LLM-as-judge prompt).
