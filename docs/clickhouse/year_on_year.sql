WITH main AS (
    SELECT 
    toYear(toDate(duedate)) AS year_,  
    sum(toFloat64(sales_amount)) AS sales_amount
    FROM sales
    GROUP BY year_
), 
year_on_year AS (
  SELECT
  year_,
  sales_amount,
  sales_amount - lagInFrame(sales_amount, 1) OVER(ORDER BY year_) AS yoy_difference
  
  FROM main
  )

SELECT * FROM year_on_year
