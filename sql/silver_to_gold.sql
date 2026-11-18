-- =====================================================
-- SILVER TO GOLD: EMPLOYEE ANALYTICS AGGREGATION
-- =====================================================

CREATE OR REPLACE TABLE
`madalleoin-flow-project.Gold_layer.employee_analytics_summary`
AS

SELECT

    Department_Region,

    Status,

    COUNT(Employee_ID) AS total_employees,

    ROUND(AVG(Salary), 2) AS average_salary,

    ROUND(AVG(Age), 1) AS average_age,

    ROUND(
        AVG(Performance_Score), 2
    ) AS average_performance_score,

    CURRENT_TIMESTAMP() AS execution_timestamp

FROM
`madalleoin-flow-project.Silver_layer.clean_`

GROUP BY
    Department_Region,
    Status;