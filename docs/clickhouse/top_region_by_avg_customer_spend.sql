WITH sales AS (
  	SELECT 
  	toInt64(sales.salesterritorykey) AS salesterritorykey,
  	toInt64(sales.customerkey) AS customerkey,
  	toInt32(sales.order_quantity) AS order_quantity,
	  toFloat64(sales.sales_amount) AS sales_amount
    FROM franco.sales AS sales
	),
groupings AS (
    SELECT 
  	salesterritorykey,
  	COUNT(customerkey) AS customer_count,
  	SUM(sales_amount) AS total_sales_amount
  	
    FROM sales 
  	GROUP BY 1
),
 ranked AS (
    SELECT
    	-- salesterritorykey,
    	sales_territory.sales_territory_region,
		total_sales_amount,
		customer_count,
		ROUND((total_sales_amount / customer_count),2) AS avg_customer_spend
      
    FROM groupings
    INNER JOIN sales_territory ON toInt64(sales_territory.sales_territory_id) = groupings.salesterritorykey
), 
final AS (
    SELECT sales_territory_region, avg_customer_spend FROM ranked
)

SELECT * FROM final
