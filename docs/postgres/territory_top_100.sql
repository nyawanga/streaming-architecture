WITH sales AS (
	SELECT 
	CAST(sales.salesterritorykey AS INT) salesterritorykey,
	CAST(sales.customerkey AS INT) customerkey,
	sales.order_quantity,
	DATE(duedate) duedate,
	COALESCE(DATE(duedate) - DATE(LAG(duedate, 1) OVER(PARTITION BY sales.customerkey ORDER BY duedate ASC )), 0) dateslag
    
    FROM franco.sales AS sales
    WHERE CAST(sales.salesterritorykey AS INT) IN (53, 76)
	),
groupings AS (
    SELECT 
	salesterritorykey,
	customerkey,
	ROUND(AVG(CAST(order_quantity AS INT)), 2) AS avg_amount,
	CEIL(AVG(dateslag)) date_lag
	
    FROM sales 
	GROUP BY 1,2
), ranked AS (
SELECT
	salesterritorykey,
	customerkey,
	avg_amount,
	date_lag,
	DENSE_RANK() OVER(PARTITION BY salesterritorykey ORDER BY avg_amount DESC, date_lag ASC) AS ranking
	
    FROM groupings
)

SELECT * FROM ranked WHERE ranking <= 100
