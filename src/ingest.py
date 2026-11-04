

"""PM02 Canopy Data Pipeline — Data Ingestion.

Read the Canopy source CSV into a Spark DataFrame using
an explicit schema.

Students implement the two functions in this module.

The Spark session is created or obtained by main.py and
passed to ingest_canopy_data(). This avoids relying on
a notebook's global spark variable.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


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
    # TODO: Define and return the 25-column Canopy schema.
    raise NotImplementedError("Implement create_canopy_schema")


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
    # TODO: Obtain the explicit Canopy schema.
    # TODO: Read the CSV using spark and the schema.
    # TODO: Return the ingested DataFrame.
    raise NotImplementedError("Implement ingest_canopy_data")

