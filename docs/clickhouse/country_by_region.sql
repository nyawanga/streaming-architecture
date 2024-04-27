WITH sales AS (
    SELECT 
  	toInt64(sales.salesterritorykey) AS salesterritorykey,
  	ROUND(AVG(order_quantity), 2) AS avg_amount,
  	sum(toFloat64(sales_amount)) AS sales_amount
  	
    FROM franco.sales AS sales
  	GROUP BY 1
),
 main AS (
    SELECT
    	-- salesterritorykey,
		sales_territory.sales_territory_country,
    	sales_territory.sales_territory_region,
    	avg_amount,
    	sales_amount

      FROM sales
      INNER JOIN sales_territory ON toInt64(sales_territory.sales_territory_id) = sales.salesterritorykey
)

SELECT * FROM main
