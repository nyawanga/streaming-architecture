WITH sales AS (
	SELECT 
	toInt64(sales.salesterritorykey) salesterritorykey,
	toInt64(sales.employee_id) AS employee_id,
	COUNT(toInt32(sales.order_quantity)) AS total_order_qnty

	FROM franco.sales AS sales
	GROUP BY 1,2
	),
main AS (
	SELECT
	salesterritorykey,
	employee_id,
	total_order_qnty,
	RANK() OVER(PARTITION BY salesterritorykey ORDER BY total_order_qnty DESC) AS ranking

	FROM sales
	)
SELECT * FROM main WHERE ranking = 1
