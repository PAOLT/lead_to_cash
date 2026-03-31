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

# # Fabric Data Generator v2 (Sales & Support)
# - No `last_activity` columns on opportunities or support tickets.
# - Last activity must be inferred from `sales_activities` and `support_activities`.
# 
# **Run steps**: Attach your Lakehouse, then run the code cell.


# CELL ********************

# Fabric Notebook - Data Generator (v2)
# Generates realistic Sales & Support data for a FinServ software vendor
# Writes Delta tables registered in the attached Lakehouse
# NOTE: 'last activity' is NOT stored on opportunities or tickets.
#       It must be inferred from sales_activities and support_activities.

from datetime import date, timedelta
import random
from pyspark.sql import types as T

random.seed(42)
TODAY = date.today()
START_DATE = date(TODAY.year-1, 7, 1)
END_DATE = TODAY

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------
# Dimension tables
# ----------------------------
customers = [
    {"customer_id": "C001", "customer_name": "Banco Aurora", "country": "IT"},
    {"customer_id": "C002", "customer_name": "Cassa Nova", "country": "IT"},
    {"customer_id": "C003", "customer_name": "EuroTrust Bank", "country": "DE"},
    {"customer_id": "C004", "customer_name": "Mediterraneo Credito", "country": "ES"},
    {"customer_id": "C005", "customer_name": "Alpine Bank", "country": "CH"},
]

products = [
    {"product_id": "P001", "product_name": "Account Ledger",     "product_line": "Core Banking Suite"},
    {"product_id": "P002", "product_name": "Loan Origination",   "product_line": "Core Banking Suite"},
    {"product_id": "P003", "product_name": "Customer 360",       "product_line": "Core Banking Suite"},
    {"product_id": "P004", "product_name": "AML Monitor",        "product_line": "Risk & Compliance"},
    {"product_id": "P005", "product_name": "KYC Orchestrator",   "product_line": "Risk & Compliance"},
    {"product_id": "P006", "product_name": "Stress Test Pro",    "product_line": "Risk & Compliance"},
    {"product_id": "P007", "product_name": "Real-Time Payments", "product_line": "Payments & Treasury"},
    {"product_id": "P008", "product_name": "Treasury Manager",   "product_line": "Payments & Treasury"},
]

# Helpers
from datetime import date

def rand_date(start: date, end: date):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def after(d: date, min_days: int, max_days: int, cap: date):
    nd = d + timedelta(days=random.randint(min_days, max_days))
    return nd if nd <= cap else cap

def pick(seq):
    return random.choice(seq)

def skew_recent(customer_id: str, base_start: date = START_DATE, base_end: date = END_DATE) -> date:
    if customer_id == "C003":
        # Inject rising trend for C003 (more recent dates)
        recent_start = TODAY - timedelta(days=120)
        return rand_date(recent_start, base_end) if random.random() < 0.75 else rand_date(base_start, base_end)
    return rand_date(base_start, base_end)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------
# csat_by_month (exactly 3 columns)
# ----------------------------
csat_rows = []
for c in customers:
    base = random.randint(3, 4)
    for m in range(1, 13):
        seasonal_adj = 1 if (m in (2,3) and c["customer_id"] == "C003") else 0
        val = max(1, min(5, base + random.choice([-1,0,0,0,1]) + seasonal_adj))
        month = TODAY - timedelta(days=m*30)
        csat_rows.append({"customer_id": c["customer_id"], "month": month, "csat": val})
csat_rows[:3]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------
# Support tickets & activities (NO last_activity on ticket)
# ----------------------------
severities = ["Low", "Medium", "High", "Critical"]
priorities = ["P4", "P3", "P2", "P1"]
channels = ["Portal", "Email", "Phone"]

support_tickets = []
support_activities = []

ticket_id_seq = 1000
support_act_id_seq = 20000

for c in customers:
    n_t = random.randint(35, 65)
    for _ in range(n_t):
        product = pick(products)
        opened_at = skew_recent(c["customer_id"])  # more recent for C003
        status = "Closed" if random.random() < 0.75 else "Open"
        closed_at = after(opened_at, 1, 30, TODAY) if status == "Closed" else None
        sev = random.choices(severities, weights=[0.4, 0.35, 0.2, 0.05])[0]
        pr = priorities[min(severities.index(sev), 3)]
        sla_breach = (sev in ["High","Critical"]) and (status == "Open") and (random.random() < 0.3)

        t = {
            "ticket_id": f"T{ticket_id_seq}",
            "customer_id": c["customer_id"],
            "product_id": product["product_id"],
            "status": status,
            "opened_at": opened_at,
            "closed_at": closed_at,
            # NO last_activity_at on ticket by design
            "severity": sev,
            "priority": pr,
            "channel": pick(channels),
            "title": f"Issue on {product['product_name']}",
            "sla_breach_flag": sla_breach,
            "assigned_group": pick(["L1 Support","L2 Support","AML Specialists"]) 
        }
        support_tickets.append(t)

        # Activities may be 0..5 to allow 'no activity' cases
        n_act = random.randint(0, 5)
        for _a in range(n_act):
            act_date = after(opened_at, 0, 30, TODAY)
            support_activities.append({
                "activity_id": f"SA{support_act_id_seq}",
                "ticket_id": t["ticket_id"],
                "activity_at": act_date,
                "description": pick(["Acknowledged issue","Requested logs","Provided workaround","Patch applied","Escalated to L2","Awaiting customer response"]),
                "author": pick(["agent.john","agent.ana","agent.samir","agent.luca"]),
                "activity_type": pick(["comment","worknote","escalation"]),
                "minutes_spent": random.randint(5,120)
            })
            support_act_id_seq += 1

        ticket_id_seq += 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------
# Opportunities, sales activities, notes (NO last_activity on opp)
# ----------------------------
opp_stages = ["Qualification","Discovery","Proposal","Negotiation","Procurement"]
opp_statuses = ["Open","Won","Lost"]
opp_types = ["Renewal","Expansion","Project"]
forecast_categories = ["Pipeline","BestCase","Commit"]

sales_opportunities = []
sales_activities = []
opportunity_notes = []

opp_id_seq = 5000
sales_act_id_seq = 60000
note_id_seq = 70000

from datetime import date

# Utility to annotate risk notes
def add_risk_note(opp_id: str, when: date):
    global note_id_seq
    opportunity_notes.append({
        "note_id": f"N{note_id_seq}",
        "opp_id": opp_id,
        "note_at": when,
        "note_type": "risk",
        "note_text": "Delivery risk flagged: scope creep and integration uncertainty.",
        "tags": "delivery risk;scope creep;integration"
    })
    note_id_seq += 1

cur_month_start = date(TODAY.year, TODAY.month, 1)
cur_month_end = date(TODAY.year, TODAY.month, 28)

for c in customers:
    n_opps = random.randint(10, 18)
    renewals_created = 0
    for _ in range(n_opps):
        product = pick(products)
        typ = pick(opp_types)
        if renewals_created < 2 and random.random() < 0.25:
            typ = "Renewal"; renewals_created += 1
        elif typ == "Renewal" and renewals_created >= 2:
            typ = pick(["Expansion","Project"])

        opened_at = rand_date(START_DATE, END_DATE)
        expected_close = after(opened_at, 14, 120, TODAY + timedelta(days=120))
        status = pick(opp_statuses)
        stage = pick(opp_stages)
        stage_last_changed_at = after(opened_at, 3, 90, TODAY)
        amount = float(pick([25000, 40000, 60000, 85000, 120000, 180000]))
        currency = "EUR"
        probability = 100 if status=="Won" else (0 if status=="Lost" else pick([30,40,50,60,70]))
        is_forecast = (status == "Open")
        forecast_cat = pick(forecast_categories) if is_forecast else None
        closed_at = after(opened_at,14,150,min(TODAY,expected_close)) if status in ("Won","Lost") else None

        opp = {
            "opp_id": f"O{opp_id_seq}",
            "customer_id": c["customer_id"],
            "product_id": product["product_id"],
            "type": typ,
            "status": status,
            "stage": stage,
            "opened_at": opened_at,
            "expected_close_date": expected_close,
            "closed_at": closed_at,
            # NO last_activity_at on opp by design
            "is_forecast": is_forecast,
            "forecast_category": forecast_cat,
            "stage_last_changed_at": stage_last_changed_at,
            "amount": amount,
            "currency": currency,
            "probability": probability,
            "renewal_term_months": 12 if typ=="Renewal" else None
        }
        sales_opportunities.append(opp)

        # Activities: shape to induce staleness for some closing-this-month opps
        is_this_month_close = (status == "Open" and cur_month_start <= expected_close <= cur_month_end)
        if is_this_month_close and random.random() < 0.5:
            # High slip risk: either zero activities or stale activities only
            n_act = pick([0,0,1])  # 66% zero, 33% one stale activity
            if n_act == 1:
                act_date = TODAY - timedelta(days=pick([15,21,28]))
                sales_activities.append({
                    "activity_id": f"A{sales_act_id_seq}",
                    "opp_id": opp["opp_id"],
                    "activity_at": act_date,
                    "description": pick(["Follow-up email","Internal review","Waiting on customer"]),
                    "type": pick(["email","meeting","call"]),
                    "contact_name": pick(["CIO","Head of Compliance","Procurement","IT Manager"])
                })
                sales_act_id_seq += 1
        else:
            # Normal 0..5 activities
            n_act = random.randint(0,5)
            for _a in range(n_act):
                act_date = after(opened_at, 1, 90, TODAY)
                sales_activities.append({
                    "activity_id": f"A{sales_act_id_seq}",
                    "opp_id": opp["opp_id"],
                    "activity_at": act_date,
                    "description": pick(["Intro call with bank stakeholders","Demo delivered to ops team","Proposal shared with legal","Negotiation on pricing","Security questionnaire sent","Management review scheduled"]),
                    "type": pick(["call","demo","email","meeting"]),
                    "contact_name": pick(["CIO","Head of Compliance","Procurement","IT Manager"])
                })
                sales_act_id_seq += 1

        # Risk notes to power 'why'
        if status == "Open" and random.random() < 0.25:
            add_risk_note(opp["opp_id"], after(opened_at, 10, 90, TODAY))

        opp_id_seq += 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ----------------------------
# Create Spark DataFrames (schemas without last_activity)
# ----------------------------
from pyspark.sql import Row

schema_customers = T.StructType([
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("customer_name", T.StringType(), False),
    T.StructField("country", T.StringType(), True)
])

schema_products = T.StructType([
    T.StructField("product_id", T.StringType(), False),
    T.StructField("product_name", T.StringType(), False),
    T.StructField("product_line", T.StringType(), False)
])

schema_csats = T.StructType([
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("month", T.DateType(), False),
    T.StructField("csat", T.IntegerType(), False)
])

schema_support_tickets = T.StructType([
    T.StructField("ticket_id", T.StringType(), False),
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("product_id", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("opened_at", T.DateType(), False),
    T.StructField("closed_at", T.DateType(), True),
    # NO last_activity_at
    T.StructField("severity", T.StringType(), False),
    T.StructField("priority", T.StringType(), False),
    T.StructField("channel", T.StringType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("sla_breach_flag", T.BooleanType(), True),
    T.StructField("assigned_group", T.StringType(), True)
])

schema_support_activities = T.StructType([
    T.StructField("activity_id", T.StringType(), False),
    T.StructField("ticket_id", T.StringType(), False),
    T.StructField("activity_at", T.DateType(), False),
    T.StructField("description", T.StringType(), False),
    T.StructField("author", T.StringType(), True),
    T.StructField("activity_type", T.StringType(), True),
    T.StructField("minutes_spent", T.IntegerType(), True)
])

schema_sales_opps = T.StructType([
    T.StructField("opp_id", T.StringType(), False),
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("product_id", T.StringType(), False),
    T.StructField("type", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("stage", T.StringType(), False),
    T.StructField("opened_at", T.DateType(), False),
    T.StructField("expected_close_date", T.DateType(), False),
    T.StructField("closed_at", T.DateType(), True),
    # NO last_activity_at
    T.StructField("is_forecast", T.BooleanType(), False),
    T.StructField("forecast_category", T.StringType(), True),
    T.StructField("stage_last_changed_at", T.DateType(), False),
    T.StructField("amount", T.DoubleType(), False),
    T.StructField("currency", T.StringType(), False),
    T.StructField("probability", T.IntegerType(), False),
    T.StructField("renewal_term_months", T.IntegerType(), True)
])

schema_sales_activities = T.StructType([
    T.StructField("activity_id", T.StringType(), False),
    T.StructField("opp_id", T.StringType(), False),
    T.StructField("activity_at", T.DateType(), False),
    T.StructField("description", T.StringType(), False),
    T.StructField("type", T.StringType(), True),
    T.StructField("contact_name", T.StringType(), True)
])

schema_opportunity_notes = T.StructType([
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

# Create DataFrames

df_customers = spark.createDataFrame(customers, schema_customers)
df_products = spark.createDataFrame(products, schema_products)
df_csats = spark.createDataFrame(csat_rows, schema_csats)
df_support_tickets = spark.createDataFrame(support_tickets, schema_support_tickets)
df_support_activities = spark.createDataFrame(support_activities, schema_support_activities)
df_sales_opps = spark.createDataFrame(sales_opportunities, schema_sales_opps)
df_sales_activities = spark.createDataFrame(sales_activities, schema_sales_activities)
df_opportunity_notes = spark.createDataFrame(opportunity_notes, schema_opportunity_notes)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Persist as Delta & register tables
(df_customers.write.format("delta").mode("overwrite").saveAsTable("customers"))
(df_products.write.format("delta").mode("overwrite").saveAsTable("products"))
(df_csats.write.format("delta").mode("overwrite").saveAsTable("csat_by_month"))
(df_support_tickets.write.format("delta").mode("overwrite").saveAsTable("support_tickets"))
(df_support_activities.write.format("delta").mode("overwrite").saveAsTable("support_activities"))
(df_sales_opps.write.format("delta").mode("overwrite").saveAsTable("sales_opportunities"))
(df_sales_activities.write.format("delta").mode("overwrite").saveAsTable("sales_activities"))
(df_opportunity_notes.write.format("delta").mode("overwrite").saveAsTable("opportunity_notes"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
