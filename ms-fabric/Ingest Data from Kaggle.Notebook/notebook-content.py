# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bbd4eaab-2f02-4dee-bab9-5352dcd559b7",
# META       "default_lakehouse_name": "Raw",
# META       "default_lakehouse_workspace_id": "31d8944c-9e2f-4ae3-a4cb-9bca985e1a21",
# META       "known_lakehouses": [
# META         {
# META           "id": "bbd4eaab-2f02-4dee-bab9-5352dcd559b7"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "d901975f-d10c-4c05-88f8-2def084f407e",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# PATHS

KAGGLE_CREDENTIALS_DIR = f"{mssparkutils.nbResPath}/builtin/configs/"
KAGGLE_DATASET_NAME = "cms/cms-open-payments-dataset-2013"
DATALAKE_DATASET_DIR = "/lakehouse/default/Files/dataset"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
os.environ['KAGGLE_CONFIG_DIR'] = KAGGLE_CREDENTIALS_DIR

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from kaggle.api.kaggle_api_extended import KaggleApi

# Initialize Kaggle API
api = KaggleApi()
api.authenticate()

# Download all files of a dataset
# Signature: dataset_download_files(dataset, path=None, force=False, quiet=True, unzip=False)
api.dataset_download_files(KAGGLE_DATASET_NAME, DATALAKE_DATASET_DIR, unzip=True, quiet=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
