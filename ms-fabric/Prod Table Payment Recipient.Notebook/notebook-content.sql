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
-- META       "default_lakehouse_workspace_id": "31d8944c-9e2f-4ae3-a4cb-9bca985e1a21"
-- META     },
-- META     "environment": {
-- META       "environmentId": "d901975f-d10c-4c05-88f8-2def084f407e",
-- META       "workspaceId": "00000000-0000-0000-0000-000000000000"
-- META     }
-- META   }
-- META }

-- CELL ********************

CREATE TABLE IF NOT EXISTS prod_payment_recipient (
    record_id INT,
    total_amount DECIMAL(12,2),
    payment_nature STRING,
    recipient_type STRING,
    specialty_main STRING,
    specialty_type STRING,
    specialty_subtype STRING,
    recipient_state STRING,
    date_of_payment DATE
) USING DELTA;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- WITH non_empty_principal_investigators AS (
--     SELECT 
--         record_id,
--         total_amount,
--         payment_form,
--         date_of_payment,
--         recipient_type,
--         recipient_city,
--         recipient_country,
--         recipient_state,
--         primary_type,
--         specialty_main,
--         specialty_subtype,
--         specialty_type,
--         principal_investigators
--     FROM stage_research_payment
--     WHERE SIZE(principal_investigators) > 1
-- ),
-- exploded_investigators AS (
--     SELECT 
--         npi.record_id, 
--         npi.total_amount, 
--         npi.date_of_payment, 
--         npi.recipient_type,
--         npi.specialty_main, 
--         npi.specialty_type, 
--         npi.specialty_subtype, 
--         npi.recipient_city, 
--         npi.recipient_state, 
--         npi.recipient_country,  
--         epi.Primary_Type AS exploded_primary_type,  -- Renamed to avoid ambiguity
--         epi.Specialty AS exploded_specialty,
--         npi.principal_investigators  -- Include the original array in the CTE
--     FROM non_empty_principal_investigators AS npi
--     LATERAL VIEW EXPLODE(principal_investigators) AS epi
-- ),
-- filtered_investigators AS (
--     SELECT
--         ei.record_id, 
--         ei.total_amount,
--         ei.date_of_payment, 
--         ei.recipient_type,
--         ei.specialty_main, 
--         ei.specialty_type, 
--         ei.specialty_subtype, 
--         ei.recipient_city, 
--         ei.recipient_state, 
--         ei.recipient_country, 
--         ei.exploded_primary_type,
--         ei.exploded_specialty,
--         SIZE(FILTER(ei.principal_investigators, x -> x.Primary_Type IS NOT NULL)) AS num_investigators  -- Fix here: Use original field names
--     FROM exploded_investigators AS ei
--     WHERE exploded_primary_type IS NOT NULL AND exploded_specialty IS NOT NULL
-- ),
-- payment_adjusted_investigators AS (
--     SELECT
--         fi.record_id, 
--         fi.total_amount, 
--         fi.date_of_payment, 
--         fi.recipient_type,
--         fi.specialty_main, 
--         fi.specialty_type, 
--         fi.specialty_subtype, 
--         fi.recipient_city, 
--         fi.recipient_state, 
--         fi.recipient_country, 
--         fi.exploded_primary_type,
--         fi.exploded_specialty,
--         fi.num_investigators,
--         fi.total_amount / fi.num_investigators AS new_total_amount
--     FROM filtered_investigators AS fi
-- ),
-- specialty_split_data AS (
--   SELECT 
--     pai.record_id,
--     pai.new_total_amount AS total_amount,
--     pai.date_of_payment, 
--     pai.recipient_type,
--     pai.specialty_main, 
--     pai.specialty_type, 
--     pai.specialty_subtype, 
--     pai.recipient_city, 
--     pai.recipient_state, 
--     pai.recipient_country, 
--     pai.exploded_primary_type AS primary_type,
--     pai.exploded_specialty AS specialty,
--     SPLIT(REGEXP_REPLACE(specialty, '/ ', '|'), '\\|') AS specialty_split
--   FROM payment_adjusted_investigators AS pai
-- ),
-- specialty_split_count AS (
--   SELECT 
--     ssd.record_id,
--     ssd.total_amount,
--     ssd.date_of_payment, 
--     ssd.recipient_type,
--     ssd.specialty_main, 
--     ssd.specialty_type, 
--     ssd.specialty_subtype, 
--     ssd.recipient_city, 
--     ssd.recipient_state, 
--     ssd.recipient_country, 
--     ssd.primary_type,
--     ssd.specialty,
--     ssd.specialty_split,
--     SIZE(ssd.specialty_split) AS pipe_char_count
--   FROM specialty_split_data AS ssd
-- ),
-- specialty_main_processed AS (
--   SELECT 
--     ssp.record_id,
--     ssp.total_amount,
--     ssp.date_of_payment, 
--     ssp.recipient_type, 
--     ssp.primary_type,
--     ssp.specialty_type, 
--     ssp.specialty_subtype, 
--     ssp.recipient_city, 
--     ssp.recipient_state, 
--     ssp.recipient_country,
--     ssp.pipe_char_count,
--     ssp.primary_type,
--     ssp.specialty,
--     ssp.specialty_split,
--     CASE
--         WHEN ssp.pipe_char_count = 1 THEN ssp.specialty_split[0]
--         WHEN ssp.pipe_char_count = 2 THEN ssp.specialty_split[0]
--         WHEN ssp.pipe_char_count >= 3 THEN ssp.specialty_split[0]
--         ELSE 'No Specialty'
--     END AS specialty_main
--   FROM specialty_split_count AS ssp
-- ),
-- specialty_type_processed AS (
--   SELECT
--     smp.record_id,
--     smp.total_amount,
--     smp.date_of_payment, 
--     smp.recipient_type, 
--     smp.primary_type,
--     smp.specialty_main, 
--     smp.specialty_subtype, 
--     smp.recipient_city, 
--     smp.recipient_state, 
--     smp.recipient_country,
--     smp.pipe_char_count,
--     smp.primary_type,
--     smp.specialty,
--     smp.specialty_split,
--     CASE
--       WHEN smp.pipe_char_count = 1 THEN 'No Specialty Type'
--       WHEN smp.pipe_char_count = 2 THEN smp.specialty_split[1]
--       WHEN smp.pipe_char_count >= 3 THEN smp.specialty_split[1]
--       ELSE 'No Specialty Type'
--     END AS specialty_type
--   FROM specialty_main_processed AS smp
-- ),
-- specialty_subtype_processed AS (
--   SELECT
--     stp.record_id,
--     stp.total_amount,
--     stp.date_of_payment, 
--     stp.recipient_type, 
--     stp.primary_type,
--     stp.specialty_main, 
--     stp.specialty_type, 
--     stp.recipient_city, 
--     stp.recipient_state, 
--     stp.recipient_country,
--     stp.pipe_char_count,
--     stp.primary_type,
--     stp.specialty,
--     stp.specialty_split,
--     CASE
--       WHEN stp.pipe_char_count = 1 THEN 'No Specialty Subtype'
--       WHEN stp.pipe_char_count = 2 THEN 'No Specialty Subtype'
--       WHEN stp.pipe_char_count = 3 THEN stp.specialty_split[2]
--       WHEN stp.pipe_char_count > 3 THEN CONCAT_WS(' | ', SLICE(stp.specialty_split, 3, 100))
--       ELSE 'No Specialty Subtype'
--     END AS specialty_subtype
--   FROM specialty_type_processed AS stp
-- )
-- INSERT INTO prod_payment_recipient
-- SELECT 
--   sstp.record_id,
--   sstp.total_amount,
--   "Research" AS payment_nature,
--   sstp.recipient_type,
--   sstp.specialty_main,
--   sstp.specialty_type,
--   sstp.specialty_subtype,
--   sstp.recipient_state,
--   sstp.date_of_payment
-- FROM specialty_subtype_processed AS sstp
-- WHERE sstp.recipient_country = "United States"
-- UNION ALL
-- SELECT
--   sgp.record_id,
--   sgp.total_amount,
--   sgp.payment_nature,
--   sgp.recipient_type,
--   sgp.specialty_main AS specialty_main,
--   sgp.specialty_type,
--   sgp.specialty_subtype,
--   sgp.recipient_state,
--   sgp.date_of_payment
-- FROM stage_general_payment AS sgp
-- WHERE sgp.recipient_country = "United States";

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

WITH non_empty_investigators AS (
  SELECT 
    record_id,
    total_amount,
    payment_form,
    date_of_payment,
    recipient_type,
    recipient_city,
    recipient_country,
    recipient_state,
    principal_investigators
  FROM stage_research_payment
  WHERE SIZE(principal_investigators) > 1
),
exploded_investigators AS (
  SELECT 
    npi.record_id, 
    npi.total_amount, 
    npi.date_of_payment, 
    npi.recipient_type,
    npi.recipient_city, 
    npi.recipient_state, 
    npi.recipient_country,  
    epi.Primary_Type AS primary_type,  
    epi.Specialty AS specialty,
    npi.principal_investigators
  FROM non_empty_investigators AS npi
  LATERAL VIEW EXPLODE(npi.principal_investigators) AS epi
),
filtered_investigators AS (
  SELECT
    ei.*, 
    SIZE(FILTER(ei.principal_investigators, x -> x.Primary_Type IS NOT NULL)) AS num_investigators
  FROM exploded_investigators AS ei
  WHERE ei.primary_type IS NOT NULL AND ei.specialty IS NOT NULL
),
adjusted_payments AS (
  SELECT
    fi.*,
    fi.total_amount / fi.num_investigators AS adjusted_total_amount
  FROM filtered_investigators AS fi
),
specialty_processed AS (
  SELECT
    ap.record_id,
    ap.adjusted_total_amount AS total_amount,
    ap.date_of_payment, 
    ap.recipient_type,
    ap.recipient_city, 
    ap.recipient_state, 
    ap.recipient_country,
    ap.primary_type,
    ap.specialty,
    SPLIT(REGEXP_REPLACE(ap.specialty, '/ ', '|'), '\\|') AS specialty_split,
    -- Process specialty components
    CASE
      WHEN SIZE(SPLIT(ap.specialty, '\\|')) >= 1 THEN SPLIT(ap.specialty, '\\|')[0]
      ELSE 'No Specialty'
    END AS specialty_main,
    CASE
      WHEN SIZE(SPLIT(ap.specialty, '\\|')) >= 2 THEN SPLIT(ap.specialty, '\\|')[1]
      ELSE 'No Specialty Type'
    END AS specialty_type,
    CASE
      WHEN SIZE(SPLIT(ap.specialty, '\\|')) >= 3 THEN SPLIT(ap.specialty, '\\|')[2]
      ELSE 'No Specialty Subtype'
    END AS specialty_subtype
  FROM adjusted_payments AS ap
)
INSERT INTO prod_payment_recipient
SELECT 
  sp.record_id,
  sp.total_amount,
  'Research' AS payment_nature,
  sp.recipient_type,
  sp.specialty_main,
  sp.specialty_type,
  sp.specialty_subtype,
  sp.recipient_state,
  sp.date_of_payment
FROM specialty_processed AS sp
WHERE sp.recipient_country = 'United States'

UNION ALL

SELECT
  sgp.record_id,
  sgp.total_amount,
  sgp.payment_nature,
  sgp.recipient_type,
  sgp.specialty_main,
  sgp.specialty_type,
  sgp.specialty_subtype,
  sgp.recipient_state,
  sgp.date_of_payment
FROM stage_general_payment AS sgp
WHERE sgp.recipient_country = 'United States';


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
