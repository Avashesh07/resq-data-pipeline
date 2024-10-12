WITH first_orders AS (
  SELECT
    user_id,
    MIN(DATE(order_created_at)) AS first_order_date
  FROM `lunar-listener-438315-b9.resq_data.presentation_table`
  GROUP BY user_id
),
cohort_data AS (
  SELECT
    fo.user_id,
    DATE_TRUNC(fo.first_order_date, MONTH) AS cohort_month,
    DATE_TRUNC(DATE(pt.order_created_at), MONTH) AS order_month
  FROM `lunar-listener-438315-b9.resq_data.presentation_table` pt
  JOIN first_orders fo ON pt.user_id = fo.user_id
)
SELECT
  cohort_month,
  COUNT(DISTINCT user_id) AS cohort_size,
  SUM(CASE WHEN order_month = DATE_ADD(cohort_month, INTERVAL 1 MONTH) THEN 1 ELSE 0 END) AS retained_users,
  SAFE_DIVIDE(SUM(CASE WHEN order_month = DATE_ADD(cohort_month, INTERVAL 1 MONTH) THEN 1 ELSE 0 END), COUNT(DISTINCT user_id)) AS m1_retention_rate
FROM cohort_data
GROUP BY cohort_month
ORDER BY cohort_month;
