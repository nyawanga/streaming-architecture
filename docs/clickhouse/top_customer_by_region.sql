WITH sales AS (
    SELECT 
  	toInt64(sales.salesterritorykey) AS salesterritorykey,
  	toInt64(sales.customerkey) AS customerkey,
  	ROUND(AVG(toInt32(sales.order_quantity)), 2) AS avg_amount,
  	CEIL(AVG(dateslag)) date_lag
  	
     FROM franco.sales AS sales
  	GROUP BY 1,2
),
ranked AS (
    SELECT
    	sales_territory.sales_territory_region,
    	toString(customer.first_name) || ' ' || toString(customer.last_name) AS customer_name,
    	avg_amount,
    	date_lag,
    	RANK() OVER(PARTITION BY salesterritorykey ORDER BY avg_amount DESC, date_lag ASC) AS ranking
    	
      FROM sales
      INNER JOIN customer ON toInt64(customer.customer_id) = sales.customerkey
      INNER JOIN sales_territory ON toInt64(sales_territory.sales_territory_id) = sales.salesterritorykey

), 
final AS (
    SELECT 
		sales_territory_region, 
		customer_name, 
		ranking 
	FROM ranked 
	WHERE ranking <= 100
)

SELECT * FROM ranked


I use clickhouse to run the query as it was my choise analytical databse for the project.
The query is supposed to get me the top 100 customers by average spend and time between each purchase.
I rank the customers first by their average spend followed by average number of days betwee each purchase
I then group them by the region and limit it to 100.
I use RANK and not DENSE RANK to get the first 100 customers.
Because the data was being streamed using kafka and I was not doing som much processing on it I have ton explicitly type cast some of the columns.
