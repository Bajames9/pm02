

"""PM02 Canopy Data Pipeline — Execution Report.

Generate pipeline_report.json using the results of the
Canopy data engineering pipeline.

THIS MODULE IS PROVIDED.
Students do not need to modify its implementation.

The report contains:
    - Pipeline name and execution status
    - UTC execution timestamp
    - Input and output paths
    - Record counts for all five pipeline stages
    - Number of duplicate records removed
    - Column counts for all five datasets
    - Violation counts for each validation rule

The report structure matches the instructor's reference
pipeline_report.json.

IMPORTANT:
    The report describes actual execution results.
    Expected counts must never be hardcoded.

    A record may violate multiple validation rules.
    Each applicable violation must be counted.

    This module writes one small JSON file, not a
    distributed Spark JSON dataset.
"""

import json
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# These validation codes are part of the PM02 contract.
#
# Their spelling must match the rejection_reason values
# produced by validate.py.
VALIDATION_RULES = [
    "customer_id_required",
    "order_id_required",
    "product_id_required",
    "quantity_invalid",
    "unit_price_invalid",
    "discount_invalid",
    "order_date_required",
    "delivery_before_order",
    "customer_join_after_order",
    "return_flag_required",
    "return_reason_required",
    "self_service_success_required",
]


def count_validation_violations(
    rejected_df: DataFrame,
) -> dict[str, int]:
    """Count occurrences of each validation violation.

    A rejected record can contain several violation codes
    in its rejection_reason column.

    The PM02 rejection_reason format uses semicolons:

        customer_id_required; quantity_invalid

    Each code must be counted independently.

    Args:
        rejected_df: Rejected DataFrame returned by
            validate_canopy_data(). It contains the
            original 25 columns plus rejection_reason.

    Returns:
        dict[str, int]: Violation count for each of the
            12 validation rules.

    Notes:
        Use exact code matching rather than substring
        matching.

        Counting violations is different from counting
        rejected records. A single rejected record may
        contribute to several violation counts.
    """
    # Split the rejection reason string into individual
    # rule codes. The expression tolerates optional
    # whitespace around the semicolon delimiter.
    reasons = F.split(
        F.coalesce(
            F.col("rejection_reason"),
            F.lit(""),
        ),
        r"\s*;\s*",
    )

    # Calculate all violation counts in one aggregation.
    # This avoids launching a separate Spark count()
    # operation for every validation rule.
    count_expressions = [
        F.sum(
            F.when(
                F.array_contains(reasons, rule_name),
                1,
            ).otherwise(0)
        ).alias(rule_name)
        for rule_name in VALIDATION_RULES
    ]

    counts_row = rejected_df.agg(
        *count_expressions
    ).first()

    return {
        rule_name: int(counts_row[rule_name] or 0)
        for rule_name in VALIDATION_RULES
    }


def build_pipeline_report(
    ingested_df: DataFrame,
    cleaned_df: DataFrame,
    valid_df: DataFrame,
    rejected_df: DataFrame,
    prepared_df: DataFrame,
    input_path: str,
    output_path: str,
) -> dict:
    """Build the pipeline execution report.

    Calculate record counts, column counts, and
    validation violation counts using actual pipeline
    results.

    Verify that the record counts reconcile before
    declaring the pipeline successful.

    Args:
        ingested_df: DataFrame returned by ingestion.
        cleaned_df: DataFrame returned by cleaning.
        valid_df: Valid DataFrame returned by validation.
        rejected_df: Rejected DataFrame returned by validation.
        prepared_df: DataFrame returned by transformation.
        input_path: Full path to the source CSV.
        output_path: Team-specific output directory.

    Returns:
        dict: JSON-serializable report matching the
            instructor's required report structure.

    Raises:
        AssertionError: If record counts do not reconcile.
    """
    ingested_count = ingested_df.count()
    cleaned_count = cleaned_df.count()
    valid_count = valid_df.count()
    rejected_count = rejected_df.count()
    prepared_count = prepared_df.count()

    duplicates_removed = ingested_count - cleaned_count

    # Reconciliation checks are basic pipeline
    # quality controls. Records should not disappear
    # unexpectedly between processing stages.
    assert duplicates_removed >= 0, (
        "Cleaning produced more records than ingestion."
    )

    assert cleaned_count == valid_count + rejected_count, (
        "Validation record counts do not reconcile."
    )

    assert valid_count == prepared_count, (
        "Transformation changed the number of valid records."
    )

    violation_counts = count_validation_violations(
        rejected_df
    )

    return {
        "pipeline": "Canopy Data Pipeline",
        "status": "PASS",
        "executed_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),
        "input_path": input_path,
        "output_base": output_path,
        "record_counts": {
            "ingested": ingested_count,
            "duplicates_removed": duplicates_removed,
            "cleaned": cleaned_count,
            "valid": valid_count,
            "rejected": rejected_count,
            "prepared": prepared_count,
        },
        "column_counts": {
            "ingested": len(ingested_df.columns),
            "cleaned": len(cleaned_df.columns),
            "valid": len(valid_df.columns),
            "rejected": len(rejected_df.columns),
            "prepared": len(prepared_df.columns),
        },
        "validation_violations": violation_counts,
    }


def write_pipeline_report(
    report: dict,
    report_path: str,
) -> None:
    """Save the report to a Databricks Unity Catalog volume.

    Use Databricks filesystem utilities to write one
    JSON file at the specified path.

    Args:
        report: Dictionary returned by
            build_pipeline_report().
        report_path: Destination path ending with
            pipeline_report.json.

    Returns:
        None. Writes the JSON file to storage.

    Notes:
        This function runs in the Databricks environment
        where dbutils is available.

        overwrite=True allows the pipeline to be rerun
        without manually deleting the previous report.
    """
    # Obtain Databricks utilities explicitly rather than
    # relying on a notebook-global dbutils variable.
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    dbutils.fs.put(
        report_path,
        json.dumps(report, indent=2),
        overwrite=True,
    )


def generate_pipeline_report(
    ingested_df: DataFrame,
    cleaned_df: DataFrame,
    valid_df: DataFrame,
    rejected_df: DataFrame,
    prepared_df: DataFrame,
    input_path: str,
    output_path: str,
) -> dict:
    """Generate and save the complete pipeline report.

    This is the public function called by main.py.

    All five pipeline DataFrames must already exist.
    Parquet output verification should finish before
    this function is called.

    Args:
        ingested_df: Ingested 25-column DataFrame.
        cleaned_df: Cleaned 25-column DataFrame.
        valid_df: Valid 25-column DataFrame.
        rejected_df: Rejected 26-column DataFrame.
        prepared_df: Prepared 30-column DataFrame.
        input_path: Source CSV path used by ingestion.
        output_path: Team-specific output directory.

    Returns:
        dict: The generated report, also saved to
            pipeline_report.json.

    Notes:
        The report is written last so its PASS status
        indicates that the earlier output stage has
        completed successfully.
    """
    report = build_pipeline_report(
        ingested_df,
        cleaned_df,
        valid_df,
        rejected_df,
        prepared_df,
        input_path,
        output_path,
    )

    report_path = f"{output_path}/pipeline_report.json"

    write_pipeline_report(
        report,
        report_path,
    )

    print()
    print("CANOPY PIPELINE COMPLETE")
    print(json.dumps(report, indent=2))
    print(f"\nReport saved to: {report_path}")

    return report

