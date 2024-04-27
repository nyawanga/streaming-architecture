WITH sales AS (
  	SELECT 
  	toInt64(sales.salesterritorykey) AS salesterritorykey,
  	toInt64(sales.customerkey) AS customerkey,
  	toInt32(sales.order_quantity) AS order_quantity,
  	toDate(duedate) AS duedate,
  	COALESCE(toDate(duedate) - toDate(lagInFrame(duedate, 1) OVER(PARTITION BY toInt64(sales.customerkey) ORDER BY duedate ASC )), 0) AS dateslag
    
    FROM franco.sales AS sales
    WHERE CAST(sales.salesterritorykey AS INT) IN (53, 76)
	),
groupings AS (
    SELECT 
  	salesterritorykey,
  	customerkey,
  	ROUND(AVG(order_quantity), 2) AS avg_amount,
  	CEIL(AVG(dateslag)) date_lag
  	
    FROM sales 
  	GROUP BY 1,2
),
 ranked AS (
    SELECT
    	-- salesterritorykey,
    	sales_territory.sales_territory_region,
    	-- customerkey,
    	toString(customer.first_name) || ' ' || toString(customer.last_name) AS customer_name,
    	avg_amount,
    	date_lag,
    	RANK() OVER(PARTITION BY salesterritorykey ORDER BY avg_amount DESC, date_lag ASC) AS ranking
    	
      FROM groupings
      INNER JOIN customer ON toInt64(customer.customer_id) = groupings.customerkey
      INNER JOIN sales_territory ON toInt64(sales_territory.sales_territory_id) = groupings.salesterritorykey
), final AS (
    SELECT sales_territory_region, customer_name, ranking FROM ranked WHERE ranking <= 100
)

SELECT * FROM ranked
