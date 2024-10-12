SELECT
  defaultoffertype AS partner_segment,
  COUNT(DISTINCT user_id) AS customer_count
FROM `lunar-listener-438315-b9.resq_data.presentation_table` 
GROUP BY partner_segment
ORDER BY customer_count DESC;
