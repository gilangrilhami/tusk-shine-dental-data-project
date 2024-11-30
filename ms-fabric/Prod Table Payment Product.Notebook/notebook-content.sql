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
-- META     }
-- META   }
-- META }

-- CELL ********************

SELECT
    sgp.total_amount,
    sgp.drugs_or_biologicals,
    sgp.devices_or_medical_supplies
FROM stage_general_payment AS sgp
WHERE SIZE(sgp.devices_or_medical_supplies) != 0
AND SIZE(sgp.drugs_or_biologicals) != 0
;

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
