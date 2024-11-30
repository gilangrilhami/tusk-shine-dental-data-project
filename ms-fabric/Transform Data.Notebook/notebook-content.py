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
# META       "default_lakehouse_workspace_id": "31d8944c-9e2f-4ae3-a4cb-9bca985e1a21"
# META     },
# META     "environment": {
# META       "environmentId": "d901975f-d10c-4c05-88f8-2def084f407e",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Load Datasets


# MARKDOWN ********************

# ## Define Dataset Schema

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType, DecimalType

general_payment_schema = StructType([
    StructField("Change_Type", StringType(), True),
    StructField("Covered_Recipient_Type", StringType(), True),
    StructField("Teaching_Hospital_CCN", StringType(), True),
    StructField("Teaching_Hospital_ID", IntegerType(), True),
    StructField("Teaching_Hospital_Name", StringType(), True),
    StructField("Physician_Profile_ID", LongType(), True),
    StructField("Physician_First_Name", StringType(), True),
    StructField("Physician_Middle_Name", StringType(), True),
    StructField("Physician_Last_Name", StringType(), True),
    StructField("Physician_Name_Suffix", StringType(), True),
    StructField("Recipient_Primary_Business_Street_Address_Line1", StringType(), True),
    StructField("Recipient_Primary_Business_Street_Address_Line2", StringType(), True),
    StructField("Recipient_City", StringType(), True),
    StructField("Recipient_State", StringType(), True),
    StructField("Recipient_Zip_Code", StringType(), True),
    StructField("Recipient_Country", StringType(), True),
    StructField("Recipient_Province", StringType(), True),
    StructField("Recipient_Postal_Code", StringType(), True),
    StructField("Physician_Primary_Type", StringType(), True),
    StructField("Physician_Specialty", StringType(), True),
    StructField("Physician_License_State_code1", StringType(), True),
    StructField("Physician_License_State_code2", StringType(), True),
    StructField("Physician_License_State_code3", StringType(), True),
    StructField("Physician_License_State_code4", StringType(), True),
    StructField("Physician_License_State_code5", StringType(), True),
    StructField("Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name", StringType(), True),
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID", StringType(), True),
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name", StringType(), True),
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State", StringType(), True),
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country", StringType(), True),
    StructField("Total_Amount_of_Payment_USDollars", DecimalType(12, 2), True),
    StructField("Date_of_Payment", StringType(), True),
    StructField("Number_of_Payments_Included_in_Total_Amount", IntegerType(), True),
    StructField("Form_of_Payment_or_Transfer_of_Value", StringType(), True),
    StructField("Nature_of_Payment_or_Transfer_of_Value", StringType(), True),
    StructField("City_of_Travel", StringType(), True),
    StructField("State_of_Travel", StringType(), True),
    StructField("Country_of_Travel", StringType(), True),
    StructField("Physician_Ownership_Indicator", StringType(), True),
    StructField("Third_Party_Payment_Recipient_Indicator", StringType(), True),
    StructField("Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value", StringType(), True),
    StructField("Charity_Indicator", StringType(), True),
    StructField("Third_Party_Equals_Covered_Recipient_Indicator", StringType(), True),
    StructField("Contextual_Information", StringType(), True),
    StructField("Delay_in_Publication_Indicator", StringType(), True),
    StructField("Record_ID", IntegerType()),
    StructField("Dispute_Status_for_Publication", StringType(), True),
    StructField("Product_Indicator", StringType(), True),
    StructField("Name_of_Associated_Covered_Drug_or_Biological1", StringType(), True),
    StructField("Name_of_Associated_Covered_Drug_or_Biological2", StringType(), True),
    StructField("Name_of_Associated_Covered_Drug_or_Biological3", StringType(), True),
    StructField("Name_of_Associated_Covered_Drug_or_Biological4", StringType(), True),
    StructField("Name_of_Associated_Covered_Drug_or_Biological5", StringType(), True),
    StructField("NDC_of_Associated_Covered_Drug_or_Biological1", StringType(), True),
    StructField("NDC_of_Associated_Covered_Drug_or_Biological2", StringType(), True),
    StructField("NDC_of_Associated_Covered_Drug_or_Biological3", StringType(), True),
    StructField("NDC_of_Associated_Covered_Drug_or_Biological4", StringType(), True),
    StructField("NDC_of_Associated_Covered_Drug_or_Biological5", StringType(), True),
    StructField("Name_of_Associated_Covered_Device_or_Medical_Supply1", StringType(), True),
    StructField("Name_of_Associated_Covered_Device_or_Medical_Supply2", StringType(), True),
    StructField("Name_of_Associated_Covered_Device_or_Medical_Supply3", StringType(), True),
    StructField("Name_of_Associated_Covered_Device_or_Medical_Supply4", StringType(), True),
    StructField("Name_of_Associated_Covered_Device_or_Medical_Supply5", StringType(), True),
    StructField("Program_Year", StringType(), True),
    StructField("Payment_Publication_Date", StringType(), True),
])

research_payment_schema = StructType([
    StructField("Change_Type", StringType(), True),
    StructField("Covered_Recipient_Type", StringType(), True),
    StructField("Noncovered_Recipient_Entity_Name", StringType(), True),
    StructField("Teaching_Hospital_CCN", StringType(), True),
    StructField("Teaching_Hospital_ID", LongType(), True),
    StructField("Teaching_Hospital_Name", StringType(), True),
    StructField("Physician_Profile_ID", LongType(), True),
    StructField("Physician_First_Name", StringType(), True),
    StructField("Physician_Middle_Name", StringType(), True),
    StructField("Physician_Last_Name", StringType(), True),
    StructField("Physician_Name_Suffix", StringType(), True),
    StructField("Recipient_Primary_Business_Street_Address_Line1", StringType(), True),
    StructField("Recipient_Primary_Business_Street_Address_Line2", StringType(), True),
    StructField("Recipient_City", StringType(), True),
    StructField("Recipient_State", StringType(), True),
    StructField("Recipient_Zip_Code", StringType(), True),
    StructField("Recipient_Country", StringType(), True),
    StructField("Recipient_Province", StringType(), True),
    StructField("Recipient_Postal_Code", StringType(), True),
    StructField("Physician_Primary_Type", StringType(), True),
    StructField("Physician_Specialty", StringType(), True),
    StructField("Physician_License_State_code1", StringType(), True),
    StructField("Physician_License_State_code2", StringType(), True),
    StructField("Physician_License_State_code3", StringType(), True),
    StructField("Physician_License_State_code4", StringType(), True),
    StructField("Physician_License_State_code5", StringType(), True),
    StructField("Principal_Investigator_1_Profile_ID", LongType(), True),
    StructField("Principal_Investigator_1_First_Name", StringType(), True),
    StructField("Principal_Investigator_1_Middle_Name", StringType(), True),
    StructField("Principal_Investigator_1_Last_Name", StringType(), True),
    StructField("Principal_Investigator_1_Name_Suffix", StringType(), True),
    StructField("Principal_Investigator_1_Business_Street_Address_Line1", StringType(), True),
    StructField("Principal_Investigator_1_Business_Street_Address_Line2", StringType(), True),
    StructField("Principal_Investigator_1_City", StringType(), True),
    StructField("Principal_Investigator_1_State", StringType(), True),
    StructField("Principal_Investigator_1_Zip_Code", StringType(), True),
    StructField("Principal_Investigator_1_Country", StringType(), True),
    StructField("Principal_Investigator_1_Province", StringType(), True),
    StructField("Principal_Investigator_1_Postal_Code", StringType(), True),
    StructField("Principal_Investigator_1_Primary_Type", StringType(), True),
    StructField("Principal_Investigator_1_Specialty", StringType(), True),
    StructField("Principal_Investigator_1_License_State_code1", StringType(), True),
    StructField("Principal_Investigator_1_License_State_code2", StringType(), True),
    StructField("Principal_Investigator_1_License_State_code3", StringType(), True),
    StructField("Principal_Investigator_1_License_State_code4", StringType(), True),
    StructField("Principal_Investigator_1_License_State_code5", StringType(), True),
    StructField("Principal_Investigator_2_Profile_ID", LongType(), True),
    StructField("Principal_Investigator_2_First_Name", StringType(), True),
    StructField("Principal_Investigator_2_Middle_Name", StringType(), True),
    StructField("Principal_Investigator_2_Last_Name", StringType(), True),
    StructField("Principal_Investigator_2_Name_Suffix", StringType(), True),
    StructField("Principal_Investigator_2_Business_Street_Address_Line1", StringType(), True),
    StructField("Principal_Investigator_2_Business_Street_Address_Line2", StringType(), True),
    StructField("Principal_Investigator_2_City", StringType(), True),
    StructField("Principal_Investigator_2_State", StringType(), True),
    StructField("Principal_Investigator_2_Zip_Code", StringType(), True),
    StructField("Principal_Investigator_2_Country", StringType(), True),
    StructField("Principal_Investigator_2_Province", StringType(), True),
    StructField("Principal_Investigator_2_Postal_Code", StringType(), True),
    StructField("Principal_Investigator_2_Primary_Type", StringType(), True),
    StructField("Principal_Investigator_2_Specialty", StringType(), True),
    StructField("Principal_Investigator_2_License_State_code1", StringType(), True),
    StructField("Principal_Investigator_2_License_State_code2", StringType(), True),
    StructField("Principal_Investigator_2_License_State_code3", StringType(), True),
    StructField("Principal_Investigator_2_License_State_code4", StringType(), True),
    StructField("Principal_Investigator_2_License_State_code5", StringType(), True),
    StructField("Principal_Investigator_3_Profile_ID", LongType(), True),
    StructField("Principal_Investigator_3_First_Name", StringType(), True),
    StructField("Principal_Investigator_3_Middle_Name", StringType(), True),
    StructField("Principal_Investigator_3_Last_Name", StringType(), True),
    StructField("Principal_Investigator_3_Name_Suffix", StringType(), True),
    StructField("Principal_Investigator_3_Business_Street_Address_Line1", StringType(), True),
    StructField("Principal_Investigator_3_Business_Street_Address_Line2", StringType(), True),
    StructField("Principal_Investigator_3_City", StringType(), True),
    StructField("Principal_Investigator_3_State", StringType(), True),
    StructField("Principal_Investigator_3_Zip_Code", StringType(), True),
    StructField("Principal_Investigator_3_Country", StringType(), True),
    StructField("Principal_Investigator_3_Province", StringType(), True),
    StructField("Principal_Investigator_3_Postal_Code", StringType(), True),
    StructField("Principal_Investigator_3_Primary_Type", StringType(), True),
    StructField("Principal_Investigator_3_Specialty", StringType(), True),
    StructField("Principal_Investigator_3_License_State_code1", StringType(), True),
    StructField("Principal_Investigator_3_License_State_code2", StringType(), True),
    StructField("Principal_Investigator_3_License_State_code3", StringType(), True),
    StructField("Principal_Investigator_3_License_State_code4", StringType(), True),
    StructField("Principal_Investigator_3_License_State_code5", StringType(), True),
    StructField("Principal_Investigator_4_Profile_ID", LongType(), True),
    StructField("Principal_Investigator_4_First_Name", StringType(), True),
    StructField("Principal_Investigator_4_Middle_Name", StringType(), True),
    StructField("Principal_Investigator_4_Last_Name", StringType(), True),
    StructField("Principal_Investigator_4_Name_Suffix", StringType(), True),
    StructField("Principal_Investigator_4_Business_Street_Address_Line1", StringType(), True),
    StructField("Principal_Investigator_4_Business_Street_Address_Line2", StringType(), True),
    StructField("Principal_Investigator_4_City", StringType(), True),
    StructField("Principal_Investigator_4_State", StringType(), True),
    StructField("Principal_Investigator_4_Zip_Code", StringType(), True),
    StructField("Principal_Investigator_4_Country", StringType(), True),
    StructField("Principal_Investigator_4_Province", StringType(), True),
    StructField("Principal_Investigator_4_Postal_Code", StringType(), True),
    StructField("Principal_Investigator_4_Primary_Type", StringType(), True),
    StructField("Principal_Investigator_4_Specialty", StringType(), True),
    StructField("Principal_Investigator_4_License_State_code1", StringType(), True),
    StructField("Principal_Investigator_4_License_State_code2", StringType(), True),
    StructField("Principal_Investigator_4_License_State_code3", StringType(), True),
    StructField("Principal_Investigator_4_License_State_code4", StringType(), True),
    StructField("Principal_Investigator_4_License_State_code5", StringType(), True),
    StructField("Principal_Investigator_5_Profile_ID", LongType(), True),
    StructField("Principal_Investigator_5_First_Name", StringType(), True),
    StructField("Principal_Investigator_5_Middle_Name", StringType(), True),
    StructField("Principal_Investigator_5_Last_Name", StringType(), True),
    StructField("Principal_Investigator_5_Name_Suffix", StringType(), True),
    StructField("Principal_Investigator_5_Business_Street_Address_Line1", StringType(), True),
    StructField("Principal_Investigator_5_Business_Street_Address_Line2", StringType(), True),
    StructField("Principal_Investigator_5_City", StringType(), True),
    StructField("Principal_Investigator_5_State", StringType(), True),
    StructField("Principal_Investigator_5_Zip_Code", StringType(), True),
    StructField("Principal_Investigator_5_Country", StringType(), True),
    StructField("Principal_Investigator_5_Province", StringType(), True),
    StructField("Principal_Investigator_5_Postal_Code", StringType(), True),
    StructField("Principal_Investigator_5_Primary_Type", StringType(), True),
    StructField("Principal_Investigator_5_Specialty", StringType(), True),
    StructField("Principal_Investigator_5_License_State_code1", StringType(), True),
    StructField("Principal_Investigator_5_License_State_code2", StringType(), True),
    StructField("Principal_Investigator_5_License_State_code3", StringType(), True),
    StructField("Principal_Investigator_5_License_State_code4", StringType(), True),
    StructField("Principal_Investigator_5_License_State_code5", StringType(), True),
    StructField("Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name", StringType(), True),
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID", LongType(), True),
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name", StringType(), True),
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State", StringType(), True),
    StructField("Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country", StringType(), True),
    StructField("Product_Indicator", StringType(), True),
    StructField("Name_of_Associated_Covered_Drug_or_Biological1", StringType(), True),
    StructField("Name_of_Associated_Covered_Drug_or_Biological2", StringType(), True),
    StructField("Name_of_Associated_Covered_Drug_or_Biological3", StringType(), True),
    StructField("Name_of_Associated_Covered_Drug_or_Biological4", StringType(), True),
    StructField("Name_of_Associated_Covered_Drug_or_Biological5", StringType(), True),
    StructField("NDC_of_Associated_Covered_Drug_or_Biological1", StringType(), True),
    StructField("NDC_of_Associated_Covered_Drug_or_Biological2", StringType(), True),
    StructField("NDC_of_Associated_Covered_Drug_or_Biological3", StringType(), True),
    StructField("NDC_of_Associated_Covered_Drug_or_Biological4", StringType(), True),
    StructField("NDC_of_Associated_Covered_Drug_or_Biological5", StringType(), True),
    StructField("Name_of_Associated_Covered_Device_or_Medical_Supply1", StringType(), True),
    StructField("Name_of_Associated_Covered_Device_or_Medical_Supply2", StringType(), True),
    StructField("Name_of_Associated_Covered_Device_or_Medical_Supply3", StringType(), True),
    StructField("Name_of_Associated_Covered_Device_or_Medical_Supply4", StringType(), True),
    StructField("Name_of_Associated_Covered_Device_or_Medical_Supply5", StringType(), True),
    StructField("Total_Amount_of_Payment_USDollars", DecimalType(12, 2), True),
    StructField("Date_of_Payment", StringType(), True),
    StructField("Form_of_Payment_or_Transfer_of_Value", StringType(), True),
    StructField("Expenditure_Category1", StringType(), True),
    StructField("Expenditure_Category2", StringType(), True),
    StructField("Expenditure_Category3", StringType(), True),
    StructField("Expenditure_Category4", StringType(), True),
    StructField("Expenditure_Category5", StringType(), True),
    StructField("Expenditure_Category6", StringType(), True),
    StructField("Preclinical_Research_Indicator", StringType(), True),
    StructField("Delay_in_Publication_Indicator", StringType(), True),
    StructField("Name_of_Study", StringType(), True),
    StructField("Dispute_Status_for_Publication", StringType(), True),
    StructField("Record_ID", LongType(), True),
    StructField("Program_Year", StringType(), True),
    StructField("Payment_Publication_Date", StringType(), True),
    StructField("ClinicalTrials_Gov_Identifier", StringType(), True),
    StructField("Research_Information_Link", StringType(), True),
    StructField("Context_of_Research", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load CSV Files based on Schema

# CELL ********************

cols_of_interest = [
    "Physician_Primary_Type",
    "Physician_Specialty",
    "Total_Amount_of_Payment_USDollars",
    "Form_of_Payment_or_Transfer_of_Value",
    "Date_of_Payment",
    "Recipient_City",
    "Recipient_State",
    "Recipient_Country",
    "Record_ID",
    "Covered_Recipient_Type",
    "Name_of_Associated_Covered_Drug_or_Biological1",
    "Name_of_Associated_Covered_Drug_or_Biological2",
    "Name_of_Associated_Covered_Drug_or_Biological3",
    "Name_of_Associated_Covered_Drug_or_Biological4",
    "Name_of_Associated_Covered_Drug_or_Biological5",
    "Name_of_Associated_Covered_Device_or_Medical_Supply1",
    "Name_of_Associated_Covered_Device_or_Medical_Supply2",
    "Name_of_Associated_Covered_Device_or_Medical_Supply3",
    "Name_of_Associated_Covered_Device_or_Medical_Supply4",
    "Name_of_Associated_Covered_Device_or_Medical_Supply5"
]

cols_of_interest_general_specific = [
  "Nature_of_Payment_or_Transfer_of_Value"
]

cols_of_interest_research_specific = [
  "Principal_Investigator_1_Primary_Type",
  "Principal_Investigator_1_Specialty",
  "Principal_Investigator_1_City",
  "Principal_Investigator_1_State",
  "Principal_Investigator_1_Country",
  "Principal_Investigator_2_Primary_Type",
  "Principal_Investigator_2_Specialty",
  "Principal_Investigator_2_City",
  "Principal_Investigator_2_State",
  "Principal_Investigator_2_Country",
  "Principal_Investigator_3_Primary_Type",
  "Principal_Investigator_3_Specialty",
  "Principal_Investigator_3_City",
  "Principal_Investigator_3_State",
  "Principal_Investigator_3_Country",
  "Principal_Investigator_4_Primary_Type",
  "Principal_Investigator_4_Specialty",
  "Principal_Investigator_4_City",
  "Principal_Investigator_4_State",
  "Principal_Investigator_4_Country",
  "Principal_Investigator_5_Primary_Type",
  "Principal_Investigator_5_Specialty",
  "Principal_Investigator_5_City",
  "Principal_Investigator_5_State",
  "Principal_Investigator_5_Country",
]

df_general = spark.read.format("csv").option("header","true").schema(general_payment_schema).load("Files/dataset/general_payments.csv").select(
    cols_of_interest + cols_of_interest_general_specific
)
df_research = spark.read.format("csv").option("header","true").schema(research_payment_schema).load("Files/dataset/research_payments.csv").select(
    cols_of_interest + cols_of_interest_research_specific
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from typing import List, Dict


class BaseTransformer:
    def __init__(self, df: DataFrame):
        """
        Initialize with the DataFrame that will be transformed.
        """
        self.df = df

    def compact_array(self, array_col: str) -> "BaseTransformer":
        """
        Compact the array column by removing nulls and empty strings.
        """
        self.df = self.df.withColumn(
            array_col, expr(f"filter({array_col}, x -> x IS NOT NULL AND x != '')")
        )
        return self

    def convert_date(
        self, date_col: str, new_col: str, format: str = "MM/dd/yyyy"
    ) -> "BaseTransformer":
        """
        Convert date column to a specific format.
        """
        self.df = self.df.withColumn(new_col, to_date(col(date_col), format))
        return self

    def rename_columns(self, rename_mapping: Dict[str, str]) -> "BaseTransformer":
        """
        Rename columns according to a provided mapping.
        """
        for old_col, new_col in rename_mapping.items():
            self.df = self.df.withColumnRenamed(old_col, new_col)
        return self

    def regexp_replace_column(
        self, column: str, pattern: str, replacement: str, new_col_name: str = None
    ) -> "BaseTransformer":
        """
        Perform a regex replacement in a column.

        :param column: The column on which to perform the replacement.
        :param pattern: The regex pattern to match.
        :param replacement: The string to replace the pattern with.
        :param new_col_name: If provided, will store the result in this column. Otherwise, overwrites the original column.

        :return: BaseTransformer object with the transformation applied.
        """
        if new_col_name is None:
            new_col_name = column  # Overwrite the original column if no new column name is provided

        self.df = self.df.withColumn(
            new_col_name, regexp_replace(col(column), pattern, replacement)
        )
        return self

    def add_homogeneous_column(
        self, new_col_name: str, value: any
    ) -> "BaseTransformer":
        """
        Create a new column with homogeneous values (same value for every row).

        :param new_col_name: Name of the new column to be added.
        :param value: The value to populate in the new column.

        :return: ResearchDataTransformer object with the new column added.
        """
        # Add the new column with the same value for every row
        self.df = self.df.withColumn(new_col_name, lit(value))
        return self

    def select_final_columns(self, columns: List[str]) -> DataFrame:
        """
        Select final columns for the DataFrame.
        """
        return self.df.select(*columns)
    
    def fill_nulls(self, null_replacements: Dict[str, any]) -> "BaseTransformer":
        """
        Fill null values for specified columns based on a provided dictionary of replacements.

        :param null_replacements: A dictionary where keys are column names and values are the replacements for null values.

        :return: BaseTransformer object with the transformation applied.
        """
        for col_name, replacement_value in null_replacements.items():
            self.df = self.df.withColumn(
                col_name, coalesce(col(col_name), lit(replacement_value))
            )
        return self



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Transform General Payment

# CELL ********************

class GeneralPaymentTransformer(BaseTransformer):
    def create_array_columns(
        self, cols_drug_bio: List[str], cols_device_medical: List[str]
    ) -> "GeneralPaymentTransformer":
        """
        Create array columns for drugs/biologicals and devices/medical supplies.
        """
        self.df = self.df.withColumn("drugs_or_biologicals", array(*cols_drug_bio))
        self.compact_array("drugs_or_biologicals")

        self.df = self.df.withColumn(
            "devices_or_medical_supplies", array(*cols_device_medical)
        )
        self.compact_array("devices_or_medical_supplies")
        return self

    def split_specialties(self, specialty_col: str) -> "GeneralPaymentTransformer":
        """
        Split specialties into main, type, and subtype.
        """
        self.df = self.df.withColumn(
            "specialty_split",
            split(regexp_replace(col(specialty_col), "/ ", "|"), "\\|"),
        )
        self.df = self.df.withColumn("pipe_char_count", size(col("specialty_split")))

        # Specialty main, type, and subtype columns
        self.df = (
            self.df.withColumn(
                "specialty_main",
                when(col("pipe_char_count") >= 1, col("specialty_split")[0]).otherwise(
                    lit("No Specialty")
                ),
            )
            .withColumn(
                "specialty_type",
                when(col("pipe_char_count") >= 2, col("specialty_split")[1]).otherwise(
                    lit("No Specialty Type")
                ),
            )
            .withColumn(
                "specialty_subtype",
                when(col("pipe_char_count") == 3, col("specialty_split")[2])
                .when(
                    col("pipe_char_count") > 3,
                    concat_ws(" | ", slice(col("specialty_split"), 3, 100)),
                )
                .otherwise(lit("No Specialty Subtype")),
            )
        )
        return self


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from typing import List


COLUMNS_DRUGS_OR_BIOLOFICALS = [
    'Name_of_Associated_Covered_Drug_or_Biological1',
    'Name_of_Associated_Covered_Drug_or_Biological2',
    'Name_of_Associated_Covered_Drug_or_Biological3',
    'Name_of_Associated_Covered_Drug_or_Biological4',
    'Name_of_Associated_Covered_Drug_or_Biological5'
]

COLUMNS_DEVICES_OR_MEDICAL_SUPPLIES = [
    'Name_of_Associated_Covered_Device_or_Medical_Supply1',
    'Name_of_Associated_Covered_Device_or_Medical_Supply2',
    'Name_of_Associated_Covered_Device_or_Medical_Supply3',
    'Name_of_Associated_Covered_Device_or_Medical_Supply4',
    'Name_of_Associated_Covered_Device_or_Medical_Supply5'
]

COLUMNS_TRANSFORMATION_OUTPUT = [
    "record_id", "total_amount", "payment_nature", "payment_form", "date_of_payment", "recipient_type", "primary_type",
    "specialty_main", "specialty_type", "specialty_subtype", "recipient_city",
    "recipient_state", "recipient_country", "drugs_or_biologicals", "devices_or_medical_supplies"
]

MAPPING_RENAMED_COLUMNS = {
    "Physician_Primary_Type": "primary_type",
    "Total_Amount_of_Payment_USDollars": "total_amount",
    "Recipient_City": "recipient_city",
    "Recipient_State": "recipient_state",
    "Recipient_Country": "recipient_country",
    "Record_ID": "record_id",
    "Nature_of_Payment_or_Transfer_of_Value": "payment_nature",
    "Form_of_Payment_or_Transfer_of_Value": "payment_form"
}

MAPPING_NULL_VAUES = {
    "primary_type": "No Primary Type",
    "recipient_state": "No Recipient State"
}

transformer = GeneralPaymentTransformer(df_general)
df_general_transformed = (
    transformer
    .create_array_columns(
        COLUMNS_DRUGS_OR_BIOLOFICALS,
        COLUMNS_DEVICES_OR_MEDICAL_SUPPLIES
    )
    .convert_date(
        'Date_of_Payment', 
        'date_of_payment'
    )
    .split_specialties("Physician_Specialty")
    .regexp_replace_column(
        "Covered_Recipient_Type",
        "Covered Recipient ",
        "",
        "recipient_type",
    )
    .rename_columns(MAPPING_RENAMED_COLUMNS)
    .fill_nulls(MAPPING_NULL_VAUES)
    .select_final_columns(COLUMNS_TRANSFORMATION_OUTPUT)
)

display(
    df_general_transformed
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Transform Research Payment

# CELL ********************

from typing import Dict, List
# from pyspark.sql import DataFrame
# from pyspark.sql.functions import array, struct, col

class ResearchPaymentTransformer(GeneralPaymentTransformer):
    def add_investigators_struct(
        self, columns_dict: Dict[str, List[str]]
    ) -> "ResearchPaymentTransformer":
        """
        Create an array of structs for the investigators with their respective fields.
        
        Args:
            columns_dict: A dictionary where the keys are the aliases (field names) 
                          and the values are lists of column names. All lists must have the same length.
                          
        Returns:
            The transformed DataFrame with an array of structs representing investigators.
        """
        # Ensure that all column lists have the same length
        col_lengths = [len(cols) for cols in columns_dict.values()]
        assert len(set(col_lengths)) == 1, "All column lists must have the same length"
        
        num_investigators = col_lengths[0]

        # Create an array of structs for each investigator
        self.df = self.df.withColumn(
            "principal_investigators",
            array(
                *[
                    struct(
                        *[
                            col(columns_dict[alias][i]).alias(alias)
                            for alias in columns_dict.keys()
                        ]
                    )
                    for i in range(num_investigators)
                ]
            ),
        )
        return self

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from typing import List


RESEARCH_COLUMNS_DRUGS_OR_BIOLOFICALS = [
    'Name_of_Associated_Covered_Drug_or_Biological1',
    'Name_of_Associated_Covered_Drug_or_Biological2',
    'Name_of_Associated_Covered_Drug_or_Biological3',
    'Name_of_Associated_Covered_Drug_or_Biological4',
    'Name_of_Associated_Covered_Drug_or_Biological5'
]

RESEARCH_COLUMNS_DEVICES_OR_MEDICAL_SUPPLIES = [
    'Name_of_Associated_Covered_Device_or_Medical_Supply1',
    'Name_of_Associated_Covered_Device_or_Medical_Supply2',
    'Name_of_Associated_Covered_Device_or_Medical_Supply3',
    'Name_of_Associated_Covered_Device_or_Medical_Supply4',
    'Name_of_Associated_Covered_Device_or_Medical_Supply5'
]

RESEARCH_COLUMNS_TRANSFORMATION_OUTPUT = [
    "record_id", "total_amount", "payment_form", "date_of_payment", "recipient_type", "primary_type",
    "specialty_main", "specialty_type", "specialty_subtype", "recipient_city",
    "recipient_state", "recipient_country", "drugs_or_biologicals", "devices_or_medical_supplies",
    "principal_investigators"
]

RESEARCH_MAPPING_PRINCIPAL_INVESTIGATOR_STRUCT = {
    "Primary_Type": [
        "Principal_Investigator_1_Primary_Type",
        "Principal_Investigator_2_Primary_Type",
        "Principal_Investigator_3_Primary_Type",
        "Principal_Investigator_4_Primary_Type",
        "Principal_Investigator_5_Primary_Type",
    ],
    "Specialty": [
        "Principal_Investigator_1_Specialty",
        "Principal_Investigator_2_Specialty",
        "Principal_Investigator_3_Specialty",
        "Principal_Investigator_4_Specialty",
        "Principal_Investigator_5_Specialty",
    ],
    "City": [
        "Principal_Investigator_1_City",
        "Principal_Investigator_2_City",
        "Principal_Investigator_3_City",
        "Principal_Investigator_4_City",
        "Principal_Investigator_5_City",
    ],
    "State": [
        "Principal_Investigator_1_State",
        "Principal_Investigator_2_State",
        "Principal_Investigator_3_State",
        "Principal_Investigator_4_State",
        "Principal_Investigator_5_State",
    ],
    "Country": [
        "Principal_Investigator_1_Country",
        "Principal_Investigator_2_Country",
        "Principal_Investigator_3_Country",
        "Principal_Investigator_4_Country",
        "Principal_Investigator_5_Country",
    ]
}

RESEARCH_MAPPING_RENAMED_COLUMNS = {
    "Physician_Primary_Type": "primary_type",
    "Total_Amount_of_Payment_USDollars": "total_amount",
    "Recipient_City": "recipient_city",
    "Recipient_State": "recipient_state",
    "Recipient_Country": "recipient_country",
    "Record_ID": "record_id",
    "Form_of_Payment_or_Transfer_of_Value": "payment_form"
}

RESEARCH_MAPPING_NULL_VALUES = {
    "primary_type": "No Primary Type",
    "recipient_state": "No Recipient State",
    "recipient_city": "No Recipient City",
    "recipient_country": "No Recipient Country"
}

transformer = ResearchPaymentTransformer(df_research)
df_research_transformed = (
    transformer
    .create_array_columns(
        RESEARCH_COLUMNS_DRUGS_OR_BIOLOFICALS,
        RESEARCH_COLUMNS_DEVICES_OR_MEDICAL_SUPPLIES
    )
    .convert_date(
        'Date_of_Payment', 
        'date_of_payment'
    )
    .split_specialties("Physician_Specialty")
    .regexp_replace_column(
        "Covered_Recipient_Type",
        "Covered Recipient ",
        "",
        "recipient_type",
    )
    .regexp_replace_column(
        "recipient_type",
        "Recipient ",
        ""
    )
    .rename_columns(RESEARCH_MAPPING_RENAMED_COLUMNS)
    .add_investigators_struct(
        RESEARCH_MAPPING_PRINCIPAL_INVESTIGATOR_STRUCT
    )
    .fill_nulls(RESEARCH_MAPPING_NULL_VALUES)
    .select_final_columns(RESEARCH_COLUMNS_TRANSFORMATION_OUTPUT)
)

display(df_research_transformed)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Save Transformed Data as Delta Table

# CELL ********************

df_general_transformed.write.format("delta").mode("overwrite").saveAsTable("stage_general_payment")
df_research_transformed.write.format("delta").mode("overwrite").saveAsTable("stage_research_payment")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
