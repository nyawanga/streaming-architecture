WITH main AS (
	SELECT 
	date_part('year', CAST(duedate AS DATE)) year_,
	SUM(CAST(sales_amount AS INTEGER)) sales_amount

	FROM sales
	GROUP BY 1
	), year_on_year AS (
	SELECT 
		year_,
		sales_amount,
		COALESCE(CAST(sales_amount AS INT) - LAG(sales_amount, 1) OVER( ORDER BY year_ ASC), 0) yoy_difference
	FROM main
	)
SELECT * FROM year_on_year
	