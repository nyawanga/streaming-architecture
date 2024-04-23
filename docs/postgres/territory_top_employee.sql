WITH sales AS (
	SELECT 
	CAST(sales.salesterritorykey AS INT) salesterritorykey,
	sales.employee_id,
	COUNT(CAST(sales.order_quantity AS INT)) AS total_order_qnty
FROM franco.sales AS sales
GROUP BY 1,2
	),
main AS (
	SELECT
	salesterritorykey,
	employee_id,
	total_order_qnty,
	DENSE_RANK() OVER(PARTITION BY salesterritorykey ORDER BY total_order_qnty DESC) AS ranking

	FROM sales
	)
SELECT * FROM main WHERE ranking = 1
