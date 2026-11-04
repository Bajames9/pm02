

"""PM02 Canopy Data Pipeline — Main Application.

Execute the complete Canopy data engineering pipeline.

PIPELINE STAGES:
    1. Ingest the source CSV using an explicit schema.
    2. Clean the ingested records.
    3. Validate records and separate valid/rejected records.
    4. Transform valid records for downstream analytics.
    5. Write and verify five Parquet datasets.
    6. Generate pipeline_report.json.

THIS MODULE IS PROVIDED.
Students should not need to modify the pipeline logic.

STAGE DEPENDENCIES:
    Each processing stage receives the DataFrame produced
    by the preceding stage.

    Cleaning must follow ingestion because it operates on
    the ingested source records.

    Validation must follow cleaning because validation
    rules should operate on normalized values.

    Transformation must follow validation because rejected
    records must not enter the prepared analytics dataset.

    Output and reporting must follow processing because
    they require the completed pipeline DataFrames.

    The report is written last so a PASS status indicates
    that all five Parquet outputs were written and verified.

SOFTWARE ENGINEERING:
    main.py coordinates the pipeline. It does not contain
    the implementation of cleaning, validation, or
    transformation rules.

    Each module has one primary responsibility.
"""

from pyspark.sql import SparkSession

from ingest import ingest_canopy_data
from clean import clean_canopy_data
from validate import validate_canopy_data
from transform import transform_canopy_data
from output import write_pipeline_outputs
from report import generate_pipeline_report


# ------------------------------------------------------------------
# Pipeline configuration
# ------------------------------------------------------------------

# Unity Catalog volume containing the source CSV.
DATA_PATH = "/Volumes/workspace/default/de_course_data"

INPUT_PATH = f"{DATA_PATH}/canopy_project_data.csv"

OUTPUT_PATH = DATA_PATH


def run_pipeline(spark: SparkSession) -> dict:
    """Execute the complete Canopy pipeline.

    This function coordinates the pipeline stages and
    verifies that the correct DataFrames are passed
    between modules.

    Args:
        spark: Active Spark session used by ingestion
            and Parquet output verification.

    Returns:
        dict: Execution report containing pipeline
            counts, schemas, and validation statistics.

    Raises:
        AssertionError: If pipeline reconciliation
            or output verification fails.
    """

    # --------------------------------------------------------------
    # Stage 1: Ingestion
    # --------------------------------------------------------------
    # Read the CSV using the explicit schema defined in ingest.py.
    ingested_df = ingest_canopy_data(
        spark,
        INPUT_PATH,
    )

    # --------------------------------------------------------------
    # Stage 2: Cleaning
    # --------------------------------------------------------------
    # Cleaning receives the ingested DataFrame and returns a
    # cleaned DataFrame with the same 25 source columns.
    cleaned_df = clean_canopy_data(ingested_df)

    # --------------------------------------------------------------
    # Stage 3: Validation
    # --------------------------------------------------------------
    # Validation checks every cleaned record against all rules.
    #
    # Valid records retain the original 25 columns.
    # Rejected records contain 26 columns, including
    # rejection_reason.
    valid_df, rejected_df = validate_canopy_data(
        cleaned_df
    )

    # --------------------------------------------------------------
    # Stage 4: Transformation
    # --------------------------------------------------------------
    # Only valid records are transformed.
    # Five derived columns are added, producing 30 columns.
    prepared_df = transform_canopy_data(valid_df)

    # --------------------------------------------------------------
    # Stage 5: Record reconciliation
    # --------------------------------------------------------------
    # These checks ensure records are accounted for at each
    # stage. They do not hardcode expected record counts.
    #
    # A correct pipeline should satisfy:
    #
    #   ingested = cleaned + duplicates_removed
    #   cleaned = valid + rejected
    #   valid = prepared
    ingested_count = ingested_df.count()
    cleaned_count = cleaned_df.count()
    valid_count = valid_df.count()
    rejected_count = rejected_df.count()
    prepared_count = prepared_df.count()

    duplicates_removed = ingested_count - cleaned_count

    assert duplicates_removed >= 0, (
        "Cleaning produced more records than ingestion."
    )

    assert cleaned_count == valid_count + rejected_count, (
        "Validation record counts do not reconcile."
    )

    assert valid_count == prepared_count, (
        "Transformation changed the number of valid records."
    )

    # --------------------------------------------------------------
    # Stage 6: Write and verify Parquet datasets
    # --------------------------------------------------------------
    # The dictionary maps each required output name to the
    # DataFrame produced by its pipeline stage.
    #
    # output.py writes each dataset and reads it back to
    # verify record count and schema.
    datasets = {
        "canopy_ingested": ingested_df,
        "canopy_cleaned": cleaned_df,
        "canopy_valid": valid_df,
        "canopy_rejected": rejected_df,
        "canopy_prepared": prepared_df,
    }

    write_pipeline_outputs(
        spark,
        datasets,
        OUTPUT_PATH,
    )

    # --------------------------------------------------------------
    # Stage 7: Generate pipeline execution report
    # --------------------------------------------------------------
    # The provided report.py module calculates the report
    # metrics and writes pipeline_report.json.
    #
    # The report is written only after Parquet verification
    # completes successfully.
    report = generate_pipeline_report(
        ingested_df,
        cleaned_df,
        valid_df,
        rejected_df,
        prepared_df,
        INPUT_PATH,
        OUTPUT_PATH,
    )

    return report


def main() -> None:
    """Obtain the Spark session and run the pipeline.

    Databricks notebooks provide a Spark session
    automatically. This application obtains the active
    session explicitly so that Python modules do not
    depend on notebook-global variables.
    """
    spark = SparkSession.builder.getOrCreate()

    run_pipeline(spark)


if __name__ == "__main__":
    main()

