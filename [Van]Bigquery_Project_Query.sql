-- Big project for SQL

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT SUBSTRING(date,1,6) AS month, 
       SUM(totals.visits) AS visits, 
       SUM(totals.pageviews) AS pageviews, 
       SUM(totals.transactions) AS transactions,
       SUM(totals.transactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
GROUP BY month
ORDER BY month;

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT trafficSource.source AS source,
       SUM(totals.visits) AS total_visits,
       SUM(totals.bounces) AS total_bounces,
       SUM(totals.bounces) / SUM(totals.visits) * 100 AS bounce_rate 
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.visits IS NOT NULL
AND totals.bounces IS NOT NULL
GROUP BY source
ORDER BY total_visits DESC;

-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL
SELECT 'Week' AS time_type,
       EXTRACT(WEEK FROM date) AS week, source, revenue 
FROM (SELECT PARSE_DATE('%Y%m%d', date) AS date,
       trafficSource.source AS source,
       SUM(totals.transactionRevenue) AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201606*`
GROUP BY source, date)

UNION ALL 

SELECT 'Month' AS time_type,
       EXTRACT(MONTH FROM date) AS month, source, revenue 
FROM (SELECT PARSE_DATE('%Y%m%d', date) AS date,
       trafficSource.source AS source,
       SUM(totals.transactionRevenue) AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201606*`
GROUP BY source, date)
GROUP BY source, month, revenue
ORDER BY revenue DESC;

-- Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
WITH june AS (
    WITH june_nonp AS (SELECT
       SUM(totals.pageviews) / count(fullVisitorId) AS avg_pageviews_nonpurchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201606*`
    WHERE totals.transactions is NULL 
    AND fullVisitorId IS NOT NULL),

    june_p AS (SELECT SUBSTRING(date,1,6) AS month,
       SUM(totals.pageviews) / count(g.fullVisitorId) AS avg_pageviews_purchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201606*` 
    WHERE totals.transactions >= 1
    AND fullVisitorId IS NOT NULL
    GROUP BY month)

SELECT june_p.month, avg_pageviews_purchase, avg_pageviews_nonpurchase
FROM june_p, june_nonp),

july AS (
    WITH july_nonp AS (SELECT
       SUM(totals.pageviews) / count(fullVisitorId) AS avg_pageviews_nonpurchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    WHERE totals.transactions is NULL
    AND fullVisitorId IS NOT NULL),

    july_p AS (SELECT SUBSTRING(date,1,6) AS month,
       SUM(totals.pageviews) / count(g.fullVisitorId) AS avg_pageviews_purchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    WHERE totals.transactions >= 1
   AND fullVisitorId IS NOT NULL
    GROUP BY month)

SELECT july_p.month, avg_pageviews_purchase, avg_pageviews_nonpurchase
FROM july_p, july_nonp)

SELECT * FROM june
UNION ALL 
SELECT * FROM july;

-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
SELECT substring(date,1,6) AS month,
       SUM(totals.transactions) / count(fullVisitorId) AS Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
WHERE totals.transactions is NOT NULL 
AND fullVisitorId IS NOT NULL
GROUP BY month;

-- Query 06: Average amount of money spent per session
#standardSQL
SELECT substring(date,1,6) AS month,
       SUM(totals.transactionRevenue) / SUM(totals.visits) AS Avg_total_revenue_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
WHERE totals.transactions is NOT NULL 
AND totals.visits > 0
GROUP BY month;

-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL
SELECT product.v2ProductName AS other_purchased_products, 
       COUNT(1) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`, 
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE fullVisitorId IN (
     SELECT fullVisitorId FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
     UNNEST (hits) hits,
     UNNEST (hits.product) product
     WHERE product.v2ProductName = "Youtube Men's Vintage Henley"
     AND totals.transactions>=1
     GROUP BY fullVisitorId
 )
AND product.v2ProductName != "Youtube Men's Vintage Henley"
AND product.v2ProductName IS NOT NULL
AND product.productRevenue IS NOT NULL
GROUP BY other_purchased_products
ORDER BY quantity DESC


-- Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
SELECT view01.month, num_product_view, num_addtocart, num_purchase, 
       round((num_addtocart / num_product_view) *100, 2) AS add_to_cart_rate, 
       round((num_purchase / num_product_view) *100, 2) AS purchase_rate 

FROM 

(SELECT substring(date,1,6) AS month,
       COUNT(*) AS num_product_view,
       hits.item.productSku	
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201701*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE hits.eCommerceAction.action_type = '2'
GROUP BY month, hits.item.productSku) view01

JOIN

(SELECT substring(date,1,6) AS month,
       COUNT(*) AS num_addtocart, 
       hits.item.productSku
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201701*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE hits.eCommerceAction.action_type = '3'
GROUP BY month, hits.item.productSku) cart01

ON cart01.month	= view01.month

JOIN

(SELECT substring(date,1,6) AS month,
       COUNT(*) AS num_purchase,
       hits.item.productSku	
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201701*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE hits.eCommerceAction.action_type = '6'
GROUP BY month, hits.item.productSku) purchase01

ON cart01.month = purchase01.month

UNION ALL 

SELECT view02.month, num_product_view, num_addtocart, num_purchase, 
       round((num_addtocart / num_product_view) *100, 2) AS add_to_cart_rate, 
       round((num_purchase / num_product_view) *100, 2) AS purchase_rate 

FROM 

(SELECT substring(date,1,6) AS month,
       COUNT(*) AS num_product_view,
       hits.item.productSku	
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201702*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE hits.eCommerceAction.action_type = '2'
GROUP BY month, hits.item.productSku) view02

JOIN

(SELECT substring(date,1,6) AS month,
       COUNT(*) AS num_addtocart, 
       hits.item.productSku
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201702*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE hits.eCommerceAction.action_type = '3'
GROUP BY month, hits.item.productSku) cart02

ON cart02.month	= view02.month

JOIN

(SELECT substring(date,1,6) AS month,
       COUNT(*) AS num_purchase,
       hits.item.productSku	
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201702*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE hits.eCommerceAction.action_type = '6'
GROUP BY month, hits.item.productSku) purchase02

ON cart02.month = purchase02.month

UNION ALL

SELECT view03.month, num_product_view, num_addtocart, num_purchase, 
       round((num_addtocart / num_product_view) *100, 2) AS add_to_cart_rate, 
       round((num_purchase / num_product_view) *100, 2) AS purchase_rate 

FROM 

(SELECT substring(date,1,6) AS month,
       COUNT(*) AS num_product_view,
       hits.item.productSku	
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201703*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE hits.eCommerceAction.action_type = '2'
GROUP BY month, hits.item.productSku) view03

JOIN

(SELECT substring(date,1,6) AS month,
       COUNT(*) AS num_addtocart, 
       hits.item.productSku
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201703*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE hits.eCommerceAction.action_type = '3'
GROUP BY month, hits.item.productSku) cart03

ON cart03.month	= view03.month

JOIN

(SELECT substring(date,1,6) AS month,
       COUNT(*) AS num_purchase,
       hits.item.productSku	
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201703*`,
UNNEST (hits) hits,
UNNEST (hits.product) product
WHERE hits.eCommerceAction.action_type = '6'
GROUP BY month, hits.item.productSku) purchase03

ON cart03.month = purchase03.month

ORDER BY month;