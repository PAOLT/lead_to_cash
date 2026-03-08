# Fabric notebook source


# MARKDOWN ********************

# # Sales & Support Test Questions — SQL Notebook (v2)
# This notebook assumes the **v2 generator** has been run and tables exist.
# 
> Note: Last activity is **derived** from activities; it's not stored on opportunities/tickets.


# MARKDOWN ********************

# ## Q1 — Top 10 likely-to-slip opportunities this month


# CELL ********************

# Execute SQL and display
_q = """
WITH this_month AS (
  SELECT DATEFROMPARTS(YEAR(CURRENT_TIMESTAMP), MONTH(CURRENT_TIMESTAMP), 1) AS first_day,
         EOMONTH(CURRENT_TIMESTAMP) AS last_day
), last_sales_activity AS (
  SELECT opp_id, MAX(activity_at) AS last_activity_at
  FROM sales_activities
  GROUP BY opp_id
), risk_notes AS (
  SELECT DISTINCT n.opp_id
  FROM opportunity_notes n
  WHERE n.note_at >= DATEADD(day, -60, CAST(CURRENT_TIMESTAMP AS date))
    AND (n.tags LIKE '%delivery risk%' OR n.note_type = 'risk')
), recent_high_sev_tickets AS (
  SELECT DISTINCT st.customer_id, st.product_id
  FROM support_tickets st
  WHERE st.opened_at >= DATEADD(day, -60, CAST(CURRENT_TIMESTAMP AS date))
    AND st.status = 'Open'
    AND st.severity IN ('High','Critical')
), candidates AS (
  SELECT o.opp_id, o.customer_id, o.product_id, o.stage, o.status, o.amount, o.expected_close_date,
         lsa.last_activity_at, o.stage_last_changed_at,
         CASE WHEN lsa.last_activity_at IS NULL OR DATEDIFF(day, lsa.last_activity_at, CURRENT_TIMESTAMP) > 14 THEN 1 ELSE 0 END AS no_activity_flag,
         CASE WHEN DATEDIFF(day, o.stage_last_changed_at, CURRENT_TIMESTAMP) > 21 THEN 1 ELSE 0 END AS stagnation_flag,
         CASE WHEN rn.opp_id IS NOT NULL OR rht.customer_id IS NOT NULL THEN 1 ELSE 0 END AS delivery_risk_flag
  FROM sales_opportunities o
  CROSS JOIN this_month tm
  LEFT JOIN last_sales_activity lsa ON lsa.opp_id = o.opp_id
  LEFT JOIN risk_notes rn ON rn.opp_id = o.opp_id
  LEFT JOIN recent_high_sev_tickets rht ON rht.customer_id = o.customer_id AND rht.product_id = o.product_id
  WHERE o.status = 'Open'
    AND o.expected_close_date BETWEEN tm.first_day AND tm.last_day
), scored AS (
  SELECT c.*, (c.no_activity_flag + c.stagnation_flag + c.delivery_risk_flag) AS risk_score,
         CONCAT(
           CASE WHEN c.no_activity_flag = 1 THEN 'No activity >14d; ' ELSE '' END,
           CASE WHEN c.stagnation_flag = 1 THEN 'Stage stagnant >21d; ' ELSE '' END,
           CASE WHEN c.delivery_risk_flag = 1 THEN 'Delivery risk signals; ' ELSE '' END
         ) AS reasons
  FROM candidates c
)
SELECT TOP 10 opp_id, customer_id, product_id, stage, amount, expected_close_date, risk_score, reasons
FROM scored
ORDER BY risk_score DESC, DATEDIFF(day, last_activity_at, CURRENT_TIMESTAMP) DESC;
"""
try:
    display(spark.sql(_q))
except NameError:
    print('Spark session not found.')


# MARKDOWN ********************

# ### Q1 — Variation 1 (Next 30 days)


# CELL ********************

# Execute SQL and display
_q = """
WITH windowed AS (
  SELECT CAST(CURRENT_TIMESTAMP AS date) AS today,
         DATEADD(day, 30, CAST(CURRENT_TIMESTAMP AS date)) AS horizon
), last_sales_activity AS (
  SELECT opp_id, MAX(activity_at) AS last_activity_at
  FROM sales_activities
  GROUP BY opp_id
), risk_notes AS (
  SELECT DISTINCT opp_id FROM opportunity_notes
  WHERE note_at >= DATEADD(day,-60,CAST(CURRENT_TIMESTAMP AS date))
    AND (tags LIKE '%delivery risk%' OR note_type='risk')
)
SELECT TOP 10 o.opp_id, o.customer_id, o.product_id, o.stage, o.amount, o.expected_close_date,
  CONCAT(
    CASE WHEN lsa.last_activity_at IS NULL OR DATEDIFF(day, lsa.last_activity_at, CURRENT_TIMESTAMP) > 14 THEN 'No activity >14d; ' ELSE '' END,
    CASE WHEN DATEDIFF(day, o.stage_last_changed_at, CURRENT_TIMESTAMP) > 21 THEN 'Stage stagnant >21d; ' ELSE '' END,
    CASE WHEN rn.opp_id IS NOT NULL THEN 'Delivery risk signals; ' ELSE '' END
  ) AS reasons
FROM sales_opportunities o
JOIN windowed w ON o.expected_close_date BETWEEN w.today AND w.horizon
LEFT JOIN last_sales_activity lsa ON lsa.opp_id = o.opp_id
LEFT JOIN risk_notes rn ON rn.opp_id = o.opp_id
WHERE o.status = 'Open'
ORDER BY (
  CASE WHEN lsa.last_activity_at IS NULL OR DATEDIFF(day, lsa.last_activity_at, CURRENT_TIMESTAMP) > 14 THEN 1 ELSE 0 END +
  CASE WHEN DATEDIFF(day, o.stage_last_changed_at, CURRENT_TIMESTAMP) > 21 THEN 1 ELSE 0 END +
  CASE WHEN rn.opp_id IS NOT NULL THEN 1 ELSE 0 END
) DESC, o.expected_close_date ASC;
"""
try:
    display(spark.sql(_q))
except NameError:
    print('Spark session not found.')


# MARKDOWN ********************

# ### Q1 — Variation 2 (This quarter)


# CELL ********************

# Execute SQL and display
_q = """
WITH q AS (
  SELECT DATEFROMPARTS(YEAR(CURRENT_TIMESTAMP), ((DATEPART(quarter,CURRENT_TIMESTAMP)-1)*3)+1, 1) AS q_start,
         EOMONTH(DATEADD(month,2,DATEFROMPARTS(YEAR(CURRENT_TIMESTAMP), ((DATEPART(quarter,CURRENT_TIMESTAMP)-1)*3)+1, 1))) AS q_end
), last_sales_activity AS (
  SELECT opp_id, MAX(activity_at) AS last_activity_at
  FROM sales_activities
  GROUP BY opp_id
), risk_notes AS (
  SELECT DISTINCT opp_id FROM opportunity_notes
  WHERE note_at >= DATEADD(day,-60,CAST(CURRENT_TIMESTAMP AS date))
    AND (tags LIKE '%delivery risk%' OR note_type='risk')
)
SELECT o.opp_id, o.customer_id, o.product_id, o.stage, o.amount, o.expected_close_date,
  CASE WHEN lsa.last_activity_at IS NULL OR DATEDIFF(day, lsa.last_activity_at, CURRENT_TIMESTAMP) > 14 THEN 1 ELSE 0 END AS no_activity,
  CASE WHEN DATEDIFF(day, o.stage_last_changed_at, CURRENT_TIMESTAMP) > 21 THEN 1 ELSE 0 END AS stagnation,
  CASE WHEN rn.opp_id IS NOT NULL THEN 1 ELSE 0 END AS delivery_risk
FROM sales_opportunities o
CROSS JOIN q
LEFT JOIN last_sales_activity lsa ON lsa.opp_id = o.opp_id
LEFT JOIN risk_notes rn ON rn.opp_id = o.opp_id
WHERE o.status = 'Open' AND o.expected_close_date BETWEEN q.q_start AND q.q_end
ORDER BY (no_activity + stagnation + delivery_risk) DESC, o.amount DESC;
"""
try:
    display(spark.sql(_q))
except NameError:
    print('Spark session not found.')


# MARKDOWN ********************

# ### Q1 — Variation 3 (By product line)


# CELL ********************

# Execute SQL and display
_q = """
WITH this_month AS (
  SELECT DATEFROMPARTS(YEAR(CURRENT_TIMESTAMP), MONTH(CURRENT_TIMESTAMP), 1) AS first_day,
         EOMONTH(CURRENT_TIMESTAMP) AS last_day
), last_sales_activity AS (
  SELECT opp_id, MAX(activity_at) AS last_activity_at
  FROM sales_activities GROUP BY opp_id
), risk_notes AS (
  SELECT DISTINCT opp_id FROM opportunity_notes
  WHERE note_at >= DATEADD(day,-60,CAST(CURRENT_TIMESTAMP AS date))
    AND (tags LIKE '%delivery risk%' OR note_type='risk')
), scored AS (
  SELECT p.product_line,
    (CASE WHEN lsa.last_activity_at IS NULL OR DATEDIFF(day, lsa.last_activity_at, CURRENT_TIMESTAMP) > 14 THEN 1 ELSE 0 END) +
    (CASE WHEN DATEDIFF(day, o.stage_last_changed_at, CURRENT_TIMESTAMP) > 21 THEN 1 ELSE 0 END) +
    (CASE WHEN rn.opp_id IS NOT NULL THEN 1 ELSE 0 END) AS risk_score
  FROM sales_opportunities o
  JOIN products p ON p.product_id = o.product_id
  CROSS JOIN this_month tm
  LEFT JOIN last_sales_activity lsa ON lsa.opp_id = o.opp_id
  LEFT JOIN risk_notes rn ON rn.opp_id = o.opp_id
  WHERE o.status='Open' AND o.expected_close_date BETWEEN tm.first_day AND tm.last_day
)
SELECT product_line, SUM(risk_score) AS total_risk_score, COUNT(*) AS opp_count
FROM scored
GROUP BY product_line
ORDER BY total_risk_score DESC;
"""
try:
    display(spark.sql(_q))
except NameError:
    print('Spark session not found.')


# MARKDOWN ********************

# ## Q2 — Renewal risk (next 90 days): low expansion or high incidents


# CELL ********************

# Execute SQL and display
_q = """
WITH horizon AS (
  SELECT CAST(CURRENT_TIMESTAMP AS date) AS today,
         DATEADD(day, 90, CAST(CURRENT_TIMESTAMP AS date)) AS horizon
), renewals AS (
  SELECT o.*
  FROM sales_opportunities o
  JOIN horizon h ON o.expected_close_date BETWEEN h.today AND h.horizon
  WHERE o.type = 'Renewal'
), expansion_pipeline AS (
  SELECT o.customer_id, SUM(o.amount) AS expansion_amount_next_90d
  FROM sales_opportunities o
  JOIN horizon h ON o.expected_close_date BETWEEN h.today AND h.horizon
  WHERE o.status='Open' AND o.type IN ('Expansion','Project')
  GROUP BY o.customer_id
), incidents_90d AS (
  SELECT st.customer_id, COUNT(*) AS incidents_last_90d
  FROM support_tickets st
  JOIN horizon h ON st.opened_at BETWEEN DATEADD(day,-90,h.horizon) AND h.horizon
  GROUP BY st.customer_id
)
SELECT r.opp_id, r.customer_id, r.product_id, r.expected_close_date,
       COALESCE(ep.expansion_amount_next_90d,0) AS expansion_amount_next_90d,
       COALESCE(i.incidents_last_90d,0) AS incidents_last_90d,
       CASE WHEN COALESCE(ep.expansion_amount_next_90d,0) < 50000 THEN 1 ELSE 0 END AS low_expansion_flag,
       CASE WHEN COALESCE(i.incidents_last_90d,0) >= 12 THEN 1 ELSE 0 END AS high_incident_flag
FROM renewals r
LEFT JOIN expansion_pipeline ep ON ep.customer_id = r.customer_id
LEFT JOIN incidents_90d i ON i.customer_id = r.customer_id
WHERE (COALESCE(ep.expansion_amount_next_90d,0) < 50000)
   OR (COALESCE(i.incidents_last_90d,0) >= 12)
ORDER BY r.expected_close_date ASC;
"""
try:
    display(spark.sql(_q))
except NameError:
    print('Spark session not found.')


# MARKDOWN ********************

# ### Q2 — Variation 1 (60 days, P1/P2 open)


# CELL ********************

# Execute SQL and display
_q = """
WITH horizon AS (
  SELECT CAST(CURRENT_TIMESTAMP AS date) AS today,
         DATEADD(day,60,CAST(CURRENT_TIMESTAMP AS date)) AS horizon
), renewals AS (
  SELECT * FROM sales_opportunities o
  WHERE o.type='Renewal' AND o.expected_close_date BETWEEN (SELECT today FROM horizon) AND (SELECT horizon FROM horizon)
), expansion_pipeline AS (
  SELECT customer_id, SUM(amount) AS expansion_amount_next_60d
  FROM sales_opportunities o
  WHERE o.status='Open' AND o.type IN ('Expansion','Project')
    AND o.expected_close_date BETWEEN (SELECT today FROM horizon) AND (SELECT horizon FROM horizon)
  GROUP BY customer_id
), p1p2_open_incidents AS (
  SELECT customer_id, COUNT(*) AS p1p2_open
  FROM support_tickets
  WHERE status='Open' AND severity IN ('Critical','High')
    AND opened_at >= DATEADD(day,-60,CAST(CURRENT_TIMESTAMP AS date))
  GROUP BY customer_id
)
SELECT r.opp_id, r.customer_id, r.expected_close_date,
       COALESCE(ep.expansion_amount_next_60d,0) AS expansion_amount_next_60d,
       COALESCE(p.p1p2_open,0) AS p1p2_open,
       CASE WHEN COALESCE(ep.expansion_amount_next_60d,0) < 30000 THEN 1 ELSE 0 END AS low_expansion_flag,
       CASE WHEN COALESCE(p.p1p2_open,0) >= 5 THEN 1 ELSE 0 END AS high_p1p2_flag
FROM renewals r
LEFT JOIN expansion_pipeline ep ON ep.customer_id = r.customer_id
LEFT JOIN p1p2_open_incidents p ON p.customer_id = r.customer_id
WHERE (COALESCE(ep.expansion_amount_next_60d,0) < 30000) OR (COALESCE(p.p1p2_open,0) >= 5);
"""
try:
    display(spark.sql(_q))
except NameError:
    print('Spark session not found.')


# MARKDOWN ********************

# ### Q2 — Variation 2 (Low CSAT or no engagement 21d)


# CELL ********************

# Execute SQL and display
_q = """
WITH horizon AS (
  SELECT CAST(CURRENT_TIMESTAMP AS date) AS today,
         DATEADD(day,90,CAST(CURRENT_TIMESTAMP AS date)) AS horizon,
         MONTH(CURRENT_TIMESTAMP) AS curr_month
), last_month AS (
  SELECT CASE WHEN (SELECT curr_month FROM horizon)=1 THEN 12 ELSE (SELECT curr_month FROM horizon)-1 END AS lm
), low_csat AS (
  SELECT customer_id FROM csat_by_month, last_month
  WHERE month = (SELECT lm FROM last_month) AND csat <= 3
), renewals AS (
  SELECT * FROM sales_opportunities o
  WHERE o.type='Renewal' AND o.expected_close_date BETWEEN (SELECT today FROM horizon) AND (SELECT horizon FROM horizon)
), last_sales_activity AS (
  SELECT opp_id, MAX(activity_at) AS last_activity_at
  FROM sales_activities GROUP BY opp_id
)
SELECT r.opp_id, r.customer_id, r.expected_close_date,
       CASE WHEN lc.customer_id IS NOT NULL THEN 1 ELSE 0 END AS low_csat_flag,
       CASE WHEN lsa.last_activity_at IS NULL OR DATEDIFF(day, lsa.last_activity_at, CURRENT_TIMESTAMP) > 21 THEN 1 ELSE 0 END AS no_engagement_flag
FROM renewals r
LEFT JOIN low_csat lc ON lc.customer_id = r.customer_id
LEFT JOIN last_sales_activity lsa ON lsa.opp_id = r.opp_id
WHERE (lc.customer_id IS NOT NULL) OR (lsa.last_activity_at IS NULL OR DATEDIFF(day, lsa.last_activity_at, CURRENT_TIMESTAMP) > 21);
"""
try:
    display(spark.sql(_q))
except NameError:
    print('Spark session not found.')


# MARKDOWN ********************

# ### Q2 — Variation 3 (SLA breaches & no expansion)


# CELL ********************

# Execute SQL and display
_q = """
WITH horizon AS (
  SELECT CAST(CURRENT_TIMESTAMP AS date) AS today,
         DATEADD(day,90,CAST(CURRENT_TIMESTAMP AS date)) AS horizon
), renewals AS (
  SELECT * FROM sales_opportunities o
  WHERE o.type='Renewal' AND o.expected_close_date BETWEEN (SELECT today FROM horizon) AND (SELECT horizon FROM horizon)
), sla_breaches AS (
  SELECT customer_id, COUNT(*) AS sla_breaches_90d
  FROM support_tickets
  WHERE sla_breach_flag = 1 AND opened_at >= DATEADD(day,-90,CAST(CURRENT_TIMESTAMP AS date))
  GROUP BY customer_id
), has_expansion AS (
  SELECT DISTINCT customer_id
  FROM sales_opportunities
  WHERE status='Open' AND type IN ('Expansion','Project')
    AND expected_close_date BETWEEN (SELECT today FROM horizon) AND (SELECT horizon FROM horizon)
)
SELECT r.opp_id, r.customer_id, r.expected_close_date,
       COALESCE(sb.sla_breaches_90d,0) AS sla_breaches_90d,
       CASE WHEN he.customer_id IS NULL THEN 1 ELSE 0 END AS no_expansion_flag
FROM renewals r
LEFT JOIN sla_breaches sb ON sb.customer_id = r.customer_id
LEFT JOIN has_expansion he ON he.customer_id = r.customer_id
WHERE COALESCE(sb.sla_breaches_90d,0) > 0 OR he.customer_id IS NULL;
"""
try:
    display(spark.sql(_q))
except NameError:
    print('Spark session not found.')


# MARKDOWN ********************

# ## Q3 — Rising incidents & declining expansion pipeline


# CELL ********************

# Execute SQL and display
_q = """
WITH months AS (
  SELECT 
    DATEFROMPARTS(YEAR(DATEADD(month,-1,CURRENT_TIMESTAMP)), MONTH(DATEADD(month,-1,CURRENT_TIMESTAMP)), 1) AS m0_start,
    EOMONTH(DATEADD(month,-1,CURRENT_TIMESTAMP)) AS m0_end,
    DATEFROMPARTS(YEAR(DATEADD(month,-2,CURRENT_TIMESTAMP)), MONTH(DATEADD(month,-2,CURRENT_TIMESTAMP)), 1) AS m1_start,
    EOMONTH(DATEADD(month,-2,CURRENT_TIMESTAMP)) AS m1_end,
    DATEFROMPARTS(YEAR(DATEADD(month,-3,CURRENT_TIMESTAMP)), MONTH(DATEADD(month,-3,CURRENT_TIMESTAMP)), 1) AS m2_start,
    EOMONTH(DATEADD(month,-3,CURRENT_TIMESTAMP)) AS m2_end
), incidents AS (
  SELECT st.customer_id,
    SUM(CASE WHEN st.opened_at BETWEEN (SELECT m2_start FROM months) AND (SELECT m2_end FROM months) THEN 1 ELSE 0 END) AS inc_m2,
    SUM(CASE WHEN st.opened_at BETWEEN (SELECT m1_start FROM months) AND (SELECT m1_end FROM months) THEN 1 ELSE 0 END) AS inc_m1,
    SUM(CASE WHEN st.opened_at BETWEEN (SELECT m0_start FROM months) AND (SELECT m0_end FROM months) THEN 1 ELSE 0 END) AS inc_m0
  FROM support_tickets st
  GROUP BY st.customer_id
), pipeline AS (
  SELECT o.customer_id,
    SUM(CASE WHEN o.type IN ('Expansion','Project') AND o.status='Open' AND o.expected_close_date BETWEEN (SELECT m2_start FROM months) AND (SELECT m2_end FROM months) THEN o.amount ELSE 0 END) AS pipe_m2,
    SUM(CASE WHEN o.type IN ('Expansion','Project') AND o.status='Open' AND o.expected_close_date BETWEEN (SELECT m1_start FROM months) AND (SELECT m1_end FROM months) THEN o.amount ELSE 0 END) AS pipe_m1,
    SUM(CASE WHEN o.type IN ('Expansion','Project') AND o.status='Open' AND o.expected_close_date BETWEEN (SELECT m0_start FROM months) AND (SELECT m0_end FROM months) THEN o.amount ELSE 0 END) AS pipe_m0
  FROM sales_opportunities o
  GROUP BY o.customer_id
)
SELECT c.customer_id, cu.customer_name,
       i.inc_m2, i.inc_m1, i.inc_m0,
       p.pipe_m2, p.pipe_m1, p.pipe_m0
FROM incidents i
JOIN pipeline p ON p.customer_id = i.customer_id
JOIN customers cu ON cu.customer_id = i.customer_id
CROSS JOIN months c
WHERE i.inc_m2 < i.inc_m1 AND i.inc_m1 < i.inc_m0
  AND p.pipe_m2 > p.pipe_m1 AND p.pipe_m1 > p.pipe_m0
ORDER BY i.inc_m0 DESC;
"""
try:
    display(spark.sql(_q))
except NameError:
    print('Spark session not found.')


# MARKDOWN ********************

# ### Q3 — Variation 1 (Consecutive 3 months)


# CELL ********************

# Execute SQL and display
_q = """
-- Same as original with explicit naming
WITH months AS (
  SELECT 
    DATEFROMPARTS(YEAR(DATEADD(month,-1,CURRENT_TIMESTAMP)), MONTH(DATEADD(month,-1,CURRENT_TIMESTAMP)), 1) AS m0_start,
    EOMONTH(DATEADD(month,-1,CURRENT_TIMESTAMP)) AS m0_end,
    DATEFROMPARTS(YEAR(DATEADD(month,-2,CURRENT_TIMESTAMP)), MONTH(DATEADD(month,-2,CURRENT_TIMESTAMP)), 1) AS m1_start,
    EOMONTH(DATEADD(month,-2,CURRENT_TIMESTAMP)) AS m1_end,
    DATEFROMPARTS(YEAR(DATEADD(month,-3,CURRENT_TIMESTAMP)), MONTH(DATEADD(month,-3,CURRENT_TIMESTAMP)), 1) AS m2_start,
    EOMONTH(DATEADD(month,-3,CURRENT_TIMESTAMP)) AS m2_end
), incidents AS (
  SELECT st.customer_id,
    SUM(CASE WHEN st.opened_at BETWEEN (SELECT m2_start FROM months) AND (SELECT m2_end FROM months) THEN 1 ELSE 0 END) AS inc_m2,
    SUM(CASE WHEN st.opened_at BETWEEN (SELECT m1_start FROM months) AND (SELECT m1_end FROM months) THEN 1 ELSE 0 END) AS inc_m1,
    SUM(CASE WHEN st.opened_at BETWEEN (SELECT m0_start FROM months) AND (SELECT m0_end FROM months) THEN 1 ELSE 0 END) AS inc_m0
  FROM support_tickets st
  GROUP BY st.customer_id
), expansion AS (
  SELECT o.customer_id,
    SUM(CASE WHEN o.type IN ('Expansion','Project') AND o.status='Open' AND o.expected_close_date BETWEEN (SELECT m2_start FROM months) AND (SELECT m2_end FROM months) THEN o.amount ELSE 0 END) AS ex_m2,
    SUM(CASE WHEN o.type IN ('Expansion','Project') AND o.status='Open' AND o.expected_close_date BETWEEN (SELECT m1_start FROM months) AND (SELECT m1_end FROM months) THEN o.amount ELSE 0 END) AS ex_m1,
    SUM(CASE WHEN o.type IN ('Expansion','Project') AND o.status='Open' AND o.expected_close_date BETWEEN (SELECT m0_start FROM months) AND (SELECT m0_end FROM months) THEN o.amount ELSE 0 END) AS ex_m0
  FROM sales_opportunities o
  GROUP BY o.customer_id
)
SELECT cu.customer_name, i.inc_m2, i.inc_m1, i.inc_m0, e.ex_m2, e.ex_m1, e.ex_m0
FROM incidents i
JOIN expansion e ON e.customer_id = i.customer_id
JOIN customers cu ON cu.customer_id = i.customer_id
WHERE i.inc_m2 < i.inc_m1 AND i.inc_m1 < i.inc_m0
  AND e.ex_m2 > e.ex_m1 AND e.ex_m1 > e.ex_m0
ORDER BY i.inc_m0 DESC;
"""
try:
    display(spark.sql(_q))
except NameError:
    print('Spark session not found.')


# MARKDOWN ********************

# ### Q3 — Variation 2 (High/Critical + next vs prev 90d)


# CELL ********************

# Execute SQL and display
_q = """
WITH months AS (
  SELECT 
    DATEFROMPARTS(YEAR(DATEADD(month,-1,CURRENT_TIMESTAMP)), MONTH(DATEADD(month,-1,CURRENT_TIMESTAMP)), 1) AS m0_start,
    EOMONTH(DATEADD(month,-1,CURRENT_TIMESTAMP)) AS m0_end,
    DATEFROMPARTS(YEAR(DATEADD(month,-2,CURRENT_TIMESTAMP)), MONTH(DATEADD(month,-2,CURRENT_TIMESTAMP)), 1) AS m1_start,
    EOMONTH(DATEADD(month,-2,CURRENT_TIMESTAMP)) AS m1_end
), crit_incidents AS (
  SELECT st.customer_id,
         SUM(CASE WHEN st.opened_at BETWEEN (SELECT m1_start FROM months) AND (SELECT m1_end FROM months) AND st.severity IN ('High','Critical') THEN 1 ELSE 0 END) AS crit_m1,
         SUM(CASE WHEN st.opened_at BETWEEN (SELECT m0_start FROM months) AND (SELECT m0_end FROM months) AND st.severity IN ('High','Critical') THEN 1 ELSE 0 END) AS crit_m0
  FROM support_tickets st GROUP BY st.customer_id
), expansion_next_90 AS (
  SELECT customer_id, SUM(CASE WHEN status='Open' AND type IN ('Expansion','Project') AND expected_close_date BETWEEN CAST(CURRENT_TIMESTAMP AS date) AND DATEADD(day,90,CAST(CURRENT_TIMESTAMP AS date)) THEN amount ELSE 0 END) AS pipe_90
  FROM sales_opportunities GROUP BY customer_id
), expansion_prev_90 AS (
  SELECT customer_id, SUM(CASE WHEN status='Open' AND type IN ('Expansion','Project') AND expected_close_date BETWEEN DATEADD(day,-90,CAST(CURRENT_TIMESTAMP AS date)) AND CAST(CURRENT_TIMESTAMP AS date) THEN amount ELSE 0 END) AS pipe_prev_90
  FROM sales_opportunities GROUP BY customer_id
)
SELECT cu.customer_name, ci.crit_m1, ci.crit_m0, ep.pipe_prev_90, en.pipe_90
FROM crit_incidents ci
JOIN expansion_prev_90 ep ON ep.customer_id = ci.customer_id
JOIN expansion_next_90 en ON en.customer_id = ci.customer_id
JOIN customers cu ON cu.customer_id = ci.customer_id
WHERE ci.crit_m0 > ci.crit_m1 AND en.pipe_90 < ep.pipe_prev_90
ORDER BY (ci.crit_m0 - ci.crit_m1) DESC;
"""
try:
    display(spark.sql(_q))
except NameError:
    print('Spark session not found.')


# MARKDOWN ********************

# ### Q3 — Variation 3 (Declining CSAT + shrinking pipeline)


# CELL ********************

# Execute SQL and display
_q = """
WITH months AS (
  SELECT MONTH(CURRENT_TIMESTAMP) AS m0,
         CASE WHEN MONTH(CURRENT_TIMESTAMP)=1 THEN 12 ELSE MONTH(CURRENT_TIMESTAMP)-1 END AS m1,
         CASE WHEN MONTH(CURRENT_TIMESTAMP)<=2 THEN MONTH(CURRENT_TIMESTAMP)+10 ELSE MONTH(CURRENT_TIMESTAMP)-2 END AS m2
), declining_csat AS (
  SELECT c1.customer_id
  FROM csat_by_month c1
  JOIN months m ON c1.month = m.m2
  JOIN csat_by_month c2 ON c2.customer_id = c1.customer_id
  JOIN csat_by_month c3 ON c3.customer_id = c1.customer_id
  WHERE c2.month = m.m1 AND c3.month = m.m0 AND c1.csat > c2.csat AND c2.csat > c3.csat
), pipe_prev AS (
  SELECT o.customer_id, SUM(o.amount) AS prev_90
  FROM sales_opportunities o
  WHERE o.status='Open' AND o.type IN ('Expansion','Project') AND o.expected_close_date BETWEEN DATEADD(day,-90,CAST(CURRENT_TIMESTAMP AS date)) AND CAST(CURRENT_TIMESTAMP AS date)
  GROUP BY o.customer_id
), pipe_next AS (
  SELECT o.customer_id, SUM(o.amount) AS next_90
  FROM sales_opportunities o
  WHERE o.status='Open' AND o.type IN ('Expansion','Project') AND o.expected_close_date BETWEEN CAST(CURRENT_TIMESTAMP AS date) AND DATEADD(day,90,CAST(CURRENT_TIMESTAMP AS date))
  GROUP BY o.customer_id
)
SELECT cu.customer_name, COALESCE(pp.prev_90,0) AS prev_90, COALESCE(pn.next_90,0) AS next_90
FROM declining_csat dc
JOIN customers cu ON cu.customer_id = dc.customer_id
LEFT JOIN pipe_prev pp ON pp.customer_id = dc.customer_id
LEFT JOIN pipe_next pn ON pn.customer_id = dc.customer_id
WHERE COALESCE(pn.next_90,0) < COALESCE(pp.prev_90,0)
ORDER BY (COALESCE(pp.prev_90,0) - COALESCE(pn.next_90,0)) DESC;
"""
try:
    display(spark.sql(_q))
except NameError:
    print('Spark session not found.')

