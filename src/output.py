

"""PM02 Canopy Data Pipeline — Output Utilities.

Write the five required pipeline datasets to Parquet files
and verify that the saved datasets can be read correctly.

IMPORTANT
Do not modify this file.

The pipeline's main.py module is responsible for:
    1. Running ingestion, cleaning, validation, and transformation.
    2. Building a dictionary containing the five DataFrames.
    3. Calling write_pipeline_outputs() with the Spark session,
       the DataFrame dictionary, and the output directory.

WHY PARQUET?
    Parquet is a column-oriented storage format commonly used
    in data engineering. It preserves column data types and
    supports efficient analytical processing.

    Unlike a CSV file, a Parquet dataset written by Spark
    is usually a directory containing multiple part files.

WHY VERIFY OUTPUT?
    A pipeline is not complete merely because a write
    operation finishes. Reading the output back provides
    evidence that the dataset was saved successfully and
    that its record count and schema were preserved.

    This verification checks record counts and schemas.
    It is not a complete record-by-record comparison.

DEPENDENCIES:
    This module requires an active SparkSession and the
    five DataFrames produced by earlier pipeline stages.

    It does not import or execute those earlier stages.
"""

from pyspark.sql import DataFrame, SparkSession


# Default Unity Catalog volume directory.
#
# IMPORTANT:
# Teams should use their own output subdirectory to avoid
# overwriting another team's files in a shared volume.
#
# main.py should pass the team's output directory explicitly
# to write_pipeline_outputs().
OUTPUT_BASE = "/Volumes/workspace/default/de_course_data"


# Required output dataset names.
#
# These names are part of the PM02 output contract.
# Do not rename them: downstream consumers and the
# instructor's grading tools expect these names.
OUTPUT_NAMES = [
    "canopy_ingested",
    "canopy_cleaned",
    "canopy_valid",
    "canopy_rejected",
    "canopy_prepared",
]


def write_parquet(
    df: DataFrame,
    output_path: str,
) -> None:
    """Write one Spark DataFrame as a Parquet dataset.

    Spark writes the dataset to a directory rather than
    necessarily creating one individual Parquet file.

    Overwrite mode allows the pipeline to be executed
    repeatedly without requiring manual deletion of
    previous output.

    Args:
        df: Spark DataFrame to save. It may come from
            ingestion, cleaning, validation, or
            transformation.
        output_path: Full Unity Catalog volume path
            for the destination Parquet directory.

    Returns:
        None. Writes the dataset to storage.

    Notes:
        This function does not change the input DataFrame.
        It does not collect records into Python memory.
    """
    df.write.mode("overwrite").parquet(output_path)


def verify_parquet(
    spark: SparkSession,
    original_df: DataFrame,
    output_path: str,
    dataset_name: str,
) -> None:
    """Verify the count and schema of a saved Parquet dataset.

    Read the dataset back from storage and compare it
    with the original Spark DataFrame.

    Verification checks:
        1. The saved record count matches the original.
        2. Column names and their order are preserved.
        3. Column data types are preserved.

    Args:
        spark: Active Spark session used to read Parquet.
        original_df: DataFrame that was written to storage.
        output_path: Directory containing the saved
            Parquet dataset.
        dataset_name: Descriptive dataset name used in
            verification messages.

    Returns:
        None. Prints a PASS message if both checks succeed.

    Raises:
        AssertionError: If the saved count or schema
            differs from the original DataFrame.
        Exception: If Spark cannot read the saved dataset.

    Notes:
        This is a basic output verification, not a
        record-by-record comparison.

        The count() calls are Spark actions. They trigger
        computation of the underlying DataFrames.
    """
    saved_df = spark.read.parquet(output_path)

    expected_count = original_df.count()
    saved_count = saved_df.count()

    assert saved_count == expected_count, (
        f"{dataset_name}: record count mismatch"
    )

    # Compare both column names and Spark data types.
    # Column order matters because the output schema is
    # part of the contract with downstream consumers.
    assert [
        (field.name, field.dataType)
        for field in saved_df.schema.fields
    ] == [
        (field.name, field.dataType)
        for field in original_df.schema.fields
    ], f"{dataset_name}: column name or type mismatch"

    print(
        f"PASS: {dataset_name}: "
        f"{saved_count} records, "
        f"{len(saved_df.columns)} columns."
    )


def write_pipeline_outputs(
    spark: SparkSession,
    datasets: dict[str, DataFrame],
    base_path: str = OUTPUT_BASE,
) -> None:
    """Write and verify all five required pipeline datasets.

    This is the public function that main.py should call.

    The datasets dictionary connects each required output
    name to the DataFrame produced by the corresponding
    pipeline stage.

    Expected dictionary keys:
        canopy_ingested
        canopy_cleaned
        canopy_valid
        canopy_rejected
        canopy_prepared

    Args:
        spark: Active Spark session supplied by main.py.
        datasets: Dictionary mapping each required output
            name to its Spark DataFrame.
        base_path: Unity Catalog volume directory where
            the five Parquet datasets will be saved.

    Returns:
        None. Writes and verifies five Parquet datasets.

    Raises:
        AssertionError: If the dictionary does not contain
            exactly the five required dataset names, or
            if output verification fails.

    Notes:
        All five DataFrames must already exist when this
        function is called.

        The output directory should be team-specific.
        Reusing another team's directory may overwrite
        that team's datasets.

        This function does not generate
        pipeline_report.json. That report is a separate
        pipeline requirement.
    """
    # Check the output contract before writing anything.
    #
    # Comparing sets detects both missing dataset names
    # and unexpected extra dataset names.
    assert set(datasets) == set(OUTPUT_NAMES), (
        "Expected exactly five pipeline datasets."
    )

    # Write each dataset, then immediately read it back
    # and verify its record count and schema.
    #
    # This loop avoids repeating nearly identical code
    # five times.
    for name in OUTPUT_NAMES:
        output_path = f"{base_path}/{name}"

        write_parquet(
            datasets[name],
            output_path,
        )

        verify_parquet(
            spark,
            datasets[name],
            output_path,
            name,
        )

