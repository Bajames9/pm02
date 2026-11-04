
"""PM02 Canopy Data Pipeline — Data Transformation.

Prepare validated Canopy records for downstream analytics.

Add five derived columns:

Financial measures:
    gross_amount
    discount_amount
    net_amount

Delivery measures:
    delivery_delay_days
    supplier_receipt_delay_days

Students implement two helper functions:

1. add_financial_measures()
2. add_delivery_measures()

The public transform_canopy_data() function is provided.

STRUCTURE AND DEPENDENCIES:
    Transformation begins with the valid 25-column
    DataFrame returned by validate_canopy_data().

    Financial measures are calculated first because
    discount_amount depends on gross_amount, and
    net_amount depends on both gross_amount and
    discount_amount.

    Delivery measures are calculated separately because
    they depend on date columns, not financial measures.

    Both helper functions must preserve all input records.
    The completed DataFrame contains 30 columns.
"""

from pyspark.sql import DataFrame


def add_financial_measures(df: DataFrame) -> DataFrame:
    """Calculate the three financial measures.

    Add the following columns:

        gross_amount
        discount_amount
        net_amount

    The measures must follow the formulas in the PM02
    project specification.

    Calculate dependent values in the correct order:
    gross_amount first, then discount_amount, and
    finally net_amount.

    Args:
        df: Valid 25-column Canopy DataFrame returned
            by validate_canopy_data() in validate.py.

    Returns:
        DataFrame: All valid records and original columns,
            plus the three financial measure columns.

    Notes:
        Preserve all input records.
        Do not change existing source columns.
        Do not perform validation or remove records.
    """
    # TODO: Calculate gross_amount.
    # TODO: Calculate discount_amount using gross_amount.
    # TODO: Calculate net_amount using the financial measures.
    raise NotImplementedError("Implement add_financial_measures")


def add_delivery_measures(df: DataFrame) -> DataFrame:
    """Calculate the two delivery-related measures.

    Add the following columns:

        delivery_delay_days
        supplier_receipt_delay_days

    Use the date columns and definitions specified in
    the PM02 project instructions.

    Args:
        df: DataFrame returned by add_financial_measures().
            Contains all valid records, the original
            25 columns, and three financial measures.

    Returns:
        DataFrame: The prepared Canopy DataFrame containing
            all valid records and 30 columns.

    Notes:
        Preserve existing financial measures.
        Preserve all input records.
        Handle null source dates according to the
        project specification.
        Do not filter or validate records.
    """
    # TODO: Calculate delivery_delay_days.
    # TODO: Calculate supplier_receipt_delay_days.
    raise NotImplementedError("Implement add_delivery_measures")


def transform_canopy_data(valid_df: DataFrame) -> DataFrame:
    """Execute the complete Canopy transformation stage.

    This orchestration function is provided. Students
    should not need to modify it.

    First, calculate financial measures. Their formulas
    have internal dependencies and must be evaluated in
    the correct order.

    Next, calculate delivery measures. These calculations
    use source date columns and do not depend on the
    financial measures.

    Args:
        valid_df: The valid 25-column DataFrame returned
            as the first result of validate_canopy_data()
            in validate.py.

    Returns:
        DataFrame: Prepared Canopy DataFrame containing
            the same valid records and 30 columns:
            25 original columns plus five derived columns.
    """
    financial_df = add_financial_measures(valid_df)

    prepared_df = add_delivery_measures(financial_df)

    return prepared_df

