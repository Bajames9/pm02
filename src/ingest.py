

"""PM02 Canopy Data Pipeline — Data Ingestion.

Read the Canopy source CSV into a Spark DataFrame using
an explicit schema.

Students implement the two functions in this module.

The Spark session is created or obtained by main.py and
passed to ingest_canopy_data(). This avoids relying on
a notebook's global spark variable.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, TimestampType, DateType
)


def create_canopy_schema() -> StructType:
    """Create the explicit schema for the Canopy source CSV.

    Define all 25 columns in the order specified by the
    Canopy Data Dictionary.

    Use appropriate PySpark data types for every column.
    Decimal-valued numeric fields, including
    customer_satisfaction, must use DoubleType.

    This function defines the schema only. It does not
    read the CSV or require a Spark session.

    Returns:
        StructType: The complete 25-column source schema,
            including column names, data types, and
            nullability.
    """

    return StructType([
        StructField("customer_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("order_date", TimestampType(), False),
        StructField("product_id", StringType(), False),
        StructField("product_category", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("discount", DoubleType(), False),
        StructField("order_status", StringType(), False),
        StructField("payment_method", StringType(), False),
        StructField("shipping_method", StringType(), False),
        StructField("promised_delivery_date", TimestampType(), False),
        StructField("delivery_date", TimestampType(), True),
        StructField("return_flag", BooleanType(), False),
        StructField("return_reason", StringType(), True),
        StructField("customer_join_date", DateType(), False),
        StructField("acquisition_source", StringType(), False),
        StructField("supplier_id", StringType(), False),
        StructField("support_channel", StringType(), True),
        StructField("support_category", StringType(), True),
        StructField("self_service_attempted", BooleanType(), False),
        StructField("self_service_success", BooleanType(), True),
        StructField("customer_satisfaction", DoubleType(), True),
        StructField("promised_receipt_date", TimestampType(), False),
        StructField("actual_receipt_date", TimestampType(), False),
    ])


def ingest_canopy_data(
    spark: SparkSession,
    input_path: str
) -> DataFrame:
    """Ingest the Canopy CSV using an explicit schema.

    Call create_canopy_schema() before reading the CSV.
    The schema establishes the required column names,
    types, and order before Spark reads the records.

    Requirements:
        - Obtain the schema from create_canopy_schema().
        - Read the CSV using the supplied Spark session.
        - Treat the first CSV row as column headers.
        - Use FAILFAST mode for malformed records.
        - Apply the explicit schema during ingestion.
        - Return the ingested DataFrame.

    Args:
        spark: Active Spark session supplied by main.py.
            Use this session to read the source CSV.
        input_path: Unity Catalog volume path to
            canopy_project_data.csv.

    Returns:
        DataFrame: Ingested Canopy records containing
            the 25 source columns with their required
            data types.

    Notes:
        This function does not clean, validate, transform,
        or write the data. Those responsibilities belong
        to subsequent pipeline stages.
    """
    
    schema = create_canopy_schema()
    ingested_df = (
            spark.read
            .format("csv")
            .option("header", True)
            .option("mode", "FAILFAST")
            .schema(schema)
            .load(input_path))
    return ingested_df
    raise NotImplementedError("Implement ingest_canopy_data")

