-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "bbd4eaab-2f02-4dee-bab9-5352dcd559b7",
-- META       "default_lakehouse_name": "Raw",
-- META       "default_lakehouse_workspace_id": "31d8944c-9e2f-4ae3-a4cb-9bca985e1a21",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "bbd4eaab-2f02-4dee-bab9-5352dcd559b7"
-- META         }
-- META       ]
-- META     },
-- META     "environment": {
-- META       "environmentId": "d901975f-d10c-4c05-88f8-2def084f407e",
-- META       "workspaceId": "00000000-0000-0000-0000-000000000000"
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- # Create Tables

-- CELL ********************

-- Dimension Table: Dim_Recipient
CREATE TABLE IF NOT EXISTS dim_recipient (
    recipient_id BIGINT,
    recipient_type STRING,
    recipient_city STRING,
    recipient_state STRING,
    recipient_country STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS dim_specialty (
    specialty_id BIGINT,
    primary_type STRING,
    specialty_main STRING,
    specialty_type STRING,
    specialty_subtype STRING
) USING DELTA;

-- Dimension Table: Dim_Product (Merged Drugs and Devices)
CREATE TABLE IF NOT EXISTS dim_product (
    product_id BIGINT,
    product_name STRING,
    type STRING -- 'drug' or 'device'
) USING DELTA;

-- Dimension Table: Dim_Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_id BIGINT,
    full_date DATE,
    year INT,
    month INT,
    day INT,
    quarter INT
) USING DELTA;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- # Insert From `stage_payment` to `dim_*` tables

-- CELL ********************

WITH unique_recipients AS (
    SELECT DISTINCT
        recipient_type,
        recipient_city,
        recipient_state,
        recipient_country
    FROM stage_payment
)
INSERT INTO dim_recipient
SELECT
    dense_rank() OVER (ORDER BY recipient_type, recipient_city, recipient_state, recipient_country) AS recipient_id,
    recipient_type,
    recipient_city,
    recipient_state,
    recipient_country
FROM unique_recipients;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

WITH exploded_products AS (
    SELECT DISTINCT 
        explode(drugs_or_biologicals) AS product_name
    FROM stage_payment
    WHERE size(drugs_or_biologicals) > 0
)
INSERT INTO dim_product
SELECT 
    dense_rank() OVER (ORDER BY product_name) AS product_id,
    TRIM(BOTH '"' FROM product_name) AS product_name,
    'drug_or_biological' AS type
FROM exploded_products;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

WITH exploded_products AS (
    SELECT DISTINCT 
        explode(devices_or_medical_supplies) AS product_name
    FROM stage_payment
    WHERE size(devices_or_medical_supplies) > 0
)
INSERT INTO dim_product
SELECT 
    dense_rank() OVER (ORDER BY product_name) AS product_id,
    TRIM(BOTH '"' FROM product_name) AS product_name,
    'device_or_medical_supply' AS type
FROM exploded_products;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

WITH unique_dates AS (
    SELECT DISTINCT 
        date_of_payment
    FROM stage_payment
)
INSERT INTO dim_date
SELECT
    dense_rank() OVER (ORDER BY date_of_payment) AS date_id,
    date_of_payment AS full_date,
    YEAR(date_of_payment) AS year,
    MONTH(date_of_payment) AS month,
    DAY(date_of_payment) AS day,
    CASE
        WHEN MONTH(date_of_payment) BETWEEN 1 AND 3 THEN 1
        WHEN MONTH(date_of_payment) BETWEEN 4 AND 6 THEN 2
        WHEN MONTH(date_of_payment) BETWEEN 7 AND 9 THEN 3
        WHEN MONTH(date_of_payment) BETWEEN 10 AND 12 THEN 4
    END AS quarter
FROM unique_dates;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

WITH non_empty_principal_investigators AS (
    SELECT 
        record_id, 
        total_amount, 
        payment_nature, 
        date_of_payment, 
        recipient_type, 
        primary_type, 
        specialty_main, 
        specialty_type, 
        specialty_subtype, 
        recipient_city, 
        recipient_state, 
        recipient_country, 
        drugs_or_biologicals,
        devices_or_medical_supplies,
        principal_investigators
    FROM stage_payment
    WHERE SIZE(principal_investigators) > 1
),
exploded_investigators AS (
    SELECT 
        record_id, 
        total_amount, 
        payment_nature, 
        date_of_payment, 
        recipient_type, 
        primary_type, 
        specialty_main, 
        specialty_type, 
        specialty_subtype, 
        recipient_city, 
        recipient_state, 
        recipient_country, 
        drugs_or_biologicals,
        devices_or_medical_supplies,
        exploded_investigator.Primary_Type AS exploded_primary_type,  -- Renamed to avoid ambiguity
        exploded_investigator.Specialty AS exploded_specialty,
        principal_investigators  -- Include the original array in the CTE
    FROM non_empty_principal_investigators
    LATERAL VIEW EXPLODE(principal_investigators) AS exploded_investigator
),
filtered_investigators AS (
    SELECT
        record_id, 
        total_amount, 
        payment_nature, 
        date_of_payment, 
        recipient_type, 
        primary_type, 
        specialty_main, 
        specialty_type, 
        specialty_subtype, 
        recipient_city, 
        recipient_state, 
        recipient_country, 
        drugs_or_biologicals,
        devices_or_medical_supplies,
        exploded_primary_type,
        exploded_specialty,
        SIZE(FILTER(principal_investigators, x -> x.Primary_Type IS NOT NULL)) AS num_investigators  -- Fix here: Use original field names
    FROM exploded_investigators
    WHERE exploded_primary_type IS NOT NULL AND exploded_specialty IS NOT NULL
),
payment_adjusted_investigators AS (
    SELECT
        record_id, 
        total_amount, 
        payment_nature, 
        date_of_payment, 
        recipient_type, 
        primary_type, 
        specialty_main, 
        specialty_type, 
        specialty_subtype, 
        recipient_city, 
        recipient_state, 
        recipient_country, 
        drugs_or_biologicals,
        devices_or_medical_supplies,
        exploded_primary_type,
        exploded_specialty,
        num_investigators,
        total_amount / num_investigators AS new_total_amount
    FROM filtered_investigators
),
specialty_split_data AS (
  SELECT 
    record_id,
    new_total_amount AS total_amount,
    payment_nature, 
    date_of_payment, 
    recipient_type, 
    exploded_primary_type AS primary_type,
    exploded_specialty AS specialty,
    recipient_city, 
    recipient_state, 
    recipient_country, 
    drugs_or_biologicals,
    devices_or_medical_supplies,
    SPLIT(REGEXP_REPLACE(specialty, '/ ', '|'), '\\|') AS specialty_split
  FROM payment_adjusted_investigators
),
specialty_split_count AS (
  SELECT *,
         SIZE(specialty_split) AS pipe_char_count
  FROM specialty_split_data
),
specialty_main_processed AS (
  SELECT *,
         CASE
           WHEN pipe_char_count = 1 THEN specialty_split[0]
           WHEN pipe_char_count = 2 THEN specialty_split[0]
           WHEN pipe_char_count >= 3 THEN specialty_split[0]
           ELSE 'No Specialty'
         END AS specialty_main
  FROM specialty_split_count
),
specialty_type_processed AS (
  SELECT *,
         CASE
           WHEN pipe_char_count = 1 THEN 'No Specialty Type'
           WHEN pipe_char_count = 2 THEN specialty_split[1]
           WHEN pipe_char_count >= 3 THEN specialty_split[1]
           ELSE 'No Specialty Type'
         END AS specialty_type
  FROM specialty_main_processed
),
specialty_subtype_processed AS (
  SELECT *,
         CASE
           WHEN pipe_char_count = 1 THEN 'No Specialty Subtype'
           WHEN pipe_char_count = 2 THEN 'No Specialty Subtype'
           WHEN pipe_char_count = 3 THEN specialty_split[2]
           WHEN pipe_char_count > 3 THEN CONCAT_WS(' | ', SLICE(specialty_split, 3, 100))
           ELSE 'No Specialty Subtype'
         END AS specialty_subtype
  FROM specialty_type_processed
),
unique_sepcialties AS (
  SELECT 
      dense_rank() OVER (ORDER BY ssp.primary_type, ssp.specialty_main, ssp.specialty_type, ssp.specialty_subtype) AS  specialty_id,
      ssp.primary_type, 
      ssp.specialty_main, 
      ssp.specialty_type, 
      ssp.specialty_subtype 
  FROM specialty_subtype_processed AS ssp
)
INSERT INTO dim_specialty
SELECT DISTINCT
  us.specialty_id,
  us.primary_type, 
  us.specialty_main, 
  us.specialty_type, 
  us.specialty_subtype 
FROM unique_sepcialties us;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
