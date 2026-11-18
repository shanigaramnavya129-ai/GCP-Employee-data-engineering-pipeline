-- =====================================================
-- BRONZE TO SILVER: EMPLOYEE DATA CLEANING
-- =====================================================

CREATE OR REPLACE TABLE
`madalleoin-flow-project.Silver_layer.clean_` AS

WITH ranked_data AS (

    SELECT

        TRIM(CAST(Employee_ID AS STRING))
            AS Employee_ID,

        INITCAP(TRIM(CAST(First_Name AS STRING)))
            AS First_Name,

        INITCAP(TRIM(CAST(Last_Name AS STRING)))
            AS Last_Name,

        SAFE_CAST(Age AS INT64)
            AS Age,

        UPPER(TRIM(CAST(Department_Region AS STRING)))
            AS Department_Region,

        INITCAP(TRIM(CAST(Status AS STRING)))
            AS Status,

        SAFE_CAST(Join_Date AS DATE)
            AS Join_Date,

        SAFE_CAST(Salary AS NUMERIC)
            AS Salary,

        LOWER(TRIM(CAST(Email AS STRING)))
            AS Email,

        REGEXP_REPLACE(
            TRIM(CAST(Phone AS STRING)),
            r'[-.() ]',
            ''
        ) AS Phone,

        SAFE_CAST(Performance_Score AS INT64)
            AS Performance_Score

    FROM
    `madalleoin-flow-project.Bronze_layer.Messy_Employee_dataset`

)

SELECT *
FROM ranked_data;