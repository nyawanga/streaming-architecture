WITH territory AS (
    SELECT
        a.sales_territory_id,
        a.territory_name
    FROM sales_territory AS a
    INNER JOIN employee ON AS b b.employee_territory_region = a.sales_territory_id
    
),
employee AS (
    SELECT
        employee_id,
        employee_name
    FROM employee
),
sales AS (
    SELECT
        employee_id,
        SUM(sale_amount) sale_amount
    FROM sale
    GROUP BY 1
),
main AS (
    SELECT
        territory.territory_name AS territory_td,
        employee.employee_name AS employee_td,
        SUM(sales.sale_amount) OVER(PARTITION BY territory.territory_name, AS sale_amount
    FROM territory
    JOIN employee
    ON territory.territory_id = employee.territory_id
    JOIN sales
    ON employee.employee_id = sales.employee_id
)

SELECT
    salesterritoryid,
    employee_id,
    SUM(sale_amount) OVER(PARTITION BY salesterritoryid, employee_id ORDER BY )TotalSales
FROM sales
WHERE sale_datetime >= today()
GROUP BY territory_id
ORDER BY totalsales DESC;
