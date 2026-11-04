

"""PM02 Canopy Data Pipeline — Data Validation.

Validate cleaned Canopy records and separate valid records
from rejected records.

Students implement three helper functions:

1. validate_required_fields()
2. validate_numeric_fields()
3. validate_business_rules()

The public validate_canopy_data() function is provided.

Each helper receives a DataFrame and appends its validation
violations to the rejection_reason column. It must preserve
violations identified by earlier helpers.

ORDER AND DEPENDENCIES:
    Validation begins with the cleaned 25-column DataFrame.

    The first helper creates rejection_reason and checks
    required fields. Subsequent helpers append additional
    reasons without removing existing ones.

    Records are separated only after all validation rules
    have been evaluated. This ensures a record with multiple
    violations retains every applicable rejection reason.

    The validation helpers must not remove records.
"""

from pyspark.sql import DataFrame


def validate_required_fields(df: DataFrame) -> DataFrame:
    """Check required identifiers and other required fields.

    This is the first validation operation.

    Create the rejection_reason column and record violations
    of required-field rules defined in the PM02 specification.

    Args:
        df: Cleaned 25-column Canopy DataFrame returned by
            clean_canopy_data(). No rejection_reason column
            exists yet.

    Returns:
        DataFrame: All input records and original columns,
            plus rejection_reason containing any violations
            of required-field rules.

    Notes:
        Do not remove records.
        Do not discard existing column values.
        Use the rejection reason codes from the specification.
    """
    required_field_rules = [
        ("customer_id_required", F.col("customer_id").isNull()),
        ("order_id_required", F.col("order_id").isNull()),
        ("product_id_required", F.col("product_id").isNull()),
        ("order_date_required", F.col("order_date").isNull()),
        ("return_flag_required", F.col("return_flag").isNull()),
    ]

    reason = F.concat_ws(
        "; ",
        *[F.when(condition, F.lit(code)) for code, condition in required_field_rules],
    )

    return df.withColumn(
        "rejection_reason",
        F.when(reason == "", F.lit(None).cast("string")).otherwise(reason),
    )


def validate_numeric_fields(df: DataFrame) -> DataFrame:
    """Check numeric data-quality rules.

    This is the second validation operation.

    Apply the numeric validation rules from the PM02
    specification, including quantity, unit price,
    and discount constraints.

    Args:
        df: DataFrame returned by validate_required_fields().
            Contains all cleaned records, the original
            25 columns, and rejection_reason.

    Returns:
        DataFrame: The same records with numeric violations
            appended to rejection_reason.

    Notes:
        Preserve all previously recorded violations.
        Do not remove records.
    """
    invalid_quantity = (
        F.col("quantity").isNull()
        | (F.col("quantity") <= 0)
    )

    invalid_unit_price = (
        F.col("unit_price").isNull()
        | (F.col("unit_price") < 0)
    )

    invalid_discount = (
        F.col("discount").isNull()
        | (F.col("discount") < 0)
        | (F.col("discount") > 1)
    )
    numeric_reasons = F.concat_ws(
        "; ",
        F.when(invalid_quantity, F.lit("quantity_invalid")),
        F.when(invalid_unit_price, F.lit("unit_price_invalid")),
        F.when(invalid_discount, F.lit("discount_invalid"))
    )

    return df.withColumn(
        "rejection_reason",
        F.concat_ws(
            "; ",
            F.col("rejection_reason"),
            F.when(numeric_reasons != "", numeric_reasons)
        )
    )


def validate_business_rules(df: DataFrame) -> DataFrame:
    """Check date, return, and self-service business rules.

    This is the third validation operation.

    Apply the business rules from the PM02 specification,
    including date relationships, return conditions,
    and self-service conditions.

    Args:
        df: DataFrame returned by validate_numeric_fields().
            Contains all cleaned records and any previously
            recorded rejection reasons.

    Returns:
        DataFrame: The same records with all applicable
            business-rule violations appended to
            rejection_reason.

    Notes:
        Preserve earlier rejection reasons.
        Do not remove records.

        A null self_service_attempted value alone is not
        a rejection reason.

        self_service_success is required only when
        self_service_attempted is true.
    """

    business_rules = [
        (
            "delivery_before_order",
            F.col("delivery_date").isNotNull()
            & F.col("order_date").isNotNull()
            & (F.col("delivery_date") < F.col("order_date")),
        ),
        (
            "customer_join_after_order",
            F.col("customer_join_date").isNotNull()
            & F.col("order_date").isNotNull()
            & (F.col("customer_join_date") > F.to_date(F.col("order_date"))),
        ),
        (
            "return_reason_required",
            (F.col("return_flag") == True) & F.col("return_reason").isNull(),
        ),
        (
            "self_service_success_required",
            (F.col("self_service_attempted") == True)
            & F.col("self_service_success").isNull(),
        ),
    ]

    combined = F.concat_ws(
        "; ",
        F.col("rejection_reason"),
        *[F.when(condition, F.lit(code)) for code, condition in business_rules],
    )

    return df.withColumn(
        "rejection_reason",
        F.when(combined == "", F.lit(None).cast("string")).otherwise(combined),
    )



def validate_canopy_data(
    cleaned_df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Validate all records and separate valid and rejected data.

    This orchestration function is provided. Students
    should not need to modify it.

    Each helper evaluates a group of validation rules and
    preserves violations recorded by previous helpers.

    All records pass through every helper before separation.
    This is important because one record may violate several
    rules. Rejecting records early would prevent us from
    capturing all applicable violations.

    Args:
        cleaned_df: The 25-column DataFrame returned by
            clean_canopy_data() in clean.py.

    Returns:
        tuple[DataFrame, DataFrame]:
            valid_df:
                Records with no validation violations.
                Contains the original 25 columns.

            rejected_df:
                Records with at least one violation.
                Contains the original 25 columns plus
                rejection_reason.
    """
    required_df = validate_required_fields(cleaned_df)

    numeric_df = validate_numeric_fields(required_df)

    checked_df = validate_business_rules(numeric_df)

    valid_df = checked_df.filter(
        "rejection_reason IS NULL"
    ).drop("rejection_reason")

    rejected_df = checked_df.filter(
        "rejection_reason IS NOT NULL"
    )

    return valid_df, rejected_df

