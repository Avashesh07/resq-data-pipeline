SELECT 
  provider_id,
  SUM(sales) AS total_sales
FROM `lunar-listener-438315-b9.resq_data.presentation_table` 
GROUP BY provider_id
ORDER BY total_sales DESC
LIMIT 10