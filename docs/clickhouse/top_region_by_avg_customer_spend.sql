WITH sales AS (
  	SELECT 
  	toInt64(sales.salesterritorykey) AS salesterritorykey,
  	COUNT(toInt64(sales.customerkey)) AS customer_count,
	SUM(toFloat64(sales.sales_amount)) AS total_sales_amount

    FROM franco.sales AS sales
    GROUP BY 1
	),
 ranked AS (
    SELECT
    	sales_territory.sales_territory_region,
		total_sales_amount,
		customer_count,
		ROUND((total_sales_amount / customer_count),2) AS avg_customer_spend
      
    FROM sales
    INNER JOIN sales_territory ON toInt64(sales_territory.sales_territory_id) = sales.salesterritorykey
), 
final AS (
    SELECT sales_territory_region, avg_customer_spend FROM ranked
)

SELECT * FROM final
