-- queries.sql
-- Add SELECT/UPDATE/DELETE queries here.
-- Tables are auto-registered from CSV files in data/ by spark_sql.py.

-- Example: all employees
SELECT * FROM sample;

-- Example: headcount and average salary per department
SELECT department, COUNT(*) AS headcount, AVG(salary) AS avg_salary
FROM sample
GROUP BY department
ORDER BY avg_salary DESC;
