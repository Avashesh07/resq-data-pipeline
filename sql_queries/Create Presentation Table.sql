CREATE TABLE `lunar-listener-438315-b9.resq_data.presentation_table` AS
SELECT
  o.id AS order_id,
  o.createdAt AS order_created_at,
  o.userid AS user_id,
  o.quantity,
  o.refunded,
  o.currency,
  o.sales,
  o.providerid AS provider_id,
  p.defaultoffertype,
  p.country AS provider_country,
  p.registereddate AS provider_registered_date,
  u.country AS user_country,
  u.registereddate AS user_registered_date
FROM `lunar-listener-438315-b9.resq_data.orders` o
LEFT JOIN `lunar-listener-438315-b9.resq_data.providers` p ON o.providerid = p.id
LEFT JOIN `lunar-listener-438315-b9.resq_data.users` u ON o.userid = u.id;
