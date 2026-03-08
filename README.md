
# Sales & Support Operations Demo (Microsoft Fabric, v2)

## 1. Purpose of This Example

This example demonstrates how to use **Microsoft Fabric**—specifically the **Lakehouse**, **Notebooks**, and **SQL endpoint**—to create and analyze an integrated **sales + support operations** dataset for a financial software vendor. 

It provides:
- A **synthetic but realistic dataset** for sales, support, CSAT, and notes.
- A **data generator notebook** to populate a Lakehouse using Delta tables.
- An **analytics notebook** containing SQL queries that answer real business questions such as:
  - Which opportunities are most likely to slip?
  - Which renewals are at risk?
  - Which accounts show rising incidents and shrinking pipeline?

This structure mirrors real-world customer architectures where Fabric is used for **data engineering**, **semantic modeling**, **analytics**, and **Copilot-driven insights**.

---

## 2. Required Architecture

To successfully run this example, you need:

### **Microsoft Fabric Workspace**
A workspace with Fabric capacity (F SKU, P SKU, or Fabric Trial). You must be able to run **Lakehouse**, **Notebook**, and **SQL** workloads.

### **Fabric Lakehouse**
All generated data is stored as **Delta tables** in a Fabric Lakehouse. The Lakehouse provides:
- ACID transactions via Delta format
- OneLake unified storage
- A built-in SQL analytics endpoint
- Direct interoperability with Power BI, Copilot, and notebooks

### **Two Fabric Notebooks**
The project includes two notebooks with distinct roles:

#### **1. `fabric_sales_support_generator_v2.ipynb` — Data Generator**
**Intent:** Build and load a full synthetic dataset into the Lakehouse.

This notebook:
- Creates the complete data model (customers, products, opportunities, tickets, activities, CSAT, notes)
- Writes each dataset as a **Delta table** using `saveAsTable()`
- Ensures realistic scenario patterns (e.g., stale opportunities, renewal risk, incident spikes)
- Enforces updated schema (no `last_activity` on opps/tickets)

You should run this notebook **first**.

#### **2. `fabric_sales_support_sql_v2.ipynb` — Analytics & SQL Tests**
**Intent:** Execute business-level analytics using the generated data.

This notebook includes SQL for:
- Slip risk detection
- Renewal risk scoring
- Pipeline vs. incidents trend analysis
- Variants of each question

The notebook automatically computes **last activity** for opportunities and support tickets using:
```sql
MAX(activity_at)
```
from their respective activities tables.

You should run this notebook **after generating the data**.

---

## 3. How to Re-run Everything

### **Step 1 — Upload Both Notebooks**
Upload the following files into your Fabric workspace:
- `fabric_sales_support_generator_v2.ipynb`
- `fabric_sales_support_sql_v2.ipynb`

### **Step 2 — Attach a Lakehouse**
For both notebooks:
1. Open the notebook in Fabric.
2. In the left pane, click **+ Add** → **Lakehouse**.
3. Select or create the Lakehouse you want to use.

### **Step 3 — Run the Data Generator Notebook**
Open `fabric_sales_support_generator_v2.ipynb` and run all cells.

When successful, it prints:
```
✅ Tables created (v2, no last_activity on opp/ticket):
 - customers
 - products
 - csat_by_month
 - support_tickets
 - support_activities
 - sales_opportunities
 - sales_activities
 - opportunity_notes
```
This confirms that all tables are now available in the Lakehouse and SQL endpoint.

### **Step 4 — Run the SQL Analytics Notebook**
Open `fabric_sales_support_sql_v2.ipynb`.
Ensure the **same Lakehouse** is attached.

Run all cells to execute:
- Slip-risk analysis
- Renewal-risk analysis
- Incident-vs-pipeline analysis
- Their variations (monthly, quarterly, by product line, by severity, etc.)

Results appear directly in notebook outputs.

---

## 4. Notes

- Re-running the generator notebook **overwrites** the existing Delta tables.
- The SQL notebook never writes—only reads and analyzes.
- The entire solution is intentionally compact and reproducible, suitable for:
  - Workshops
  - Demos
  - Architecture reviews
  - Copilot + Fabric integration showcases
  - Power BI prototype modeling

---

Generated automatically on {datetime.date.today().isoformat()}.
