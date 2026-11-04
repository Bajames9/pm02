

"""PM02 Canopy Data Pipeline — Data Cleaning.

Clean the ingested Canopy dataset using three operations:

1. Trim leading and trailing whitespace from string values.
2. Convert blank strings to null values.
3. Remove exact duplicate records after normalization.

Students implement the three helper functions.

The public clean_canopy_data() function is provided and
demonstrates how to coordinate the cleaning operations.

ORDER MATTERS:
    Trimming must occur before blank-string conversion
    because whitespace-only values become empty strings
    after trimming.

    Duplicate removal must occur after normalization
    because records that originally differ only in
    whitespace or blank/null representation may become
    identical after cleaning.

Each helper returns a new Spark DataFrame. The original
DataFrame is not modified in place.
"""

from pyspark.sql import DataFrame


def trim_string_columns(df: DataFrame) -> DataFrame:
    """Remove leading and trailing whitespace from strings.

    This is the first cleaning operation.

    Args:
        df: The 25-column ingested Canopy DataFrame
            returned by ingest_canopy_data().

    Returns:
        DataFrame: A DataFrame containing the same records
            and columns, with leading and trailing
            whitespace removed from every string column.

    Notes:
        Preserve null values and non-string columns.
        Do not remove records or change column data types.
    """

    string_cols = {
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    }


    return df.select(
        *[
            F.trim(F.col(c)).alias(c) if c in string_cols else F.col(c)
            for c in df.columns
        ]
    )


def convert_blank_strings_to_null(df: DataFrame) -> DataFrame:
    """Replace empty strings with null values.

    This is the second cleaning operation.

    Trimming has already occurred, so values containing
    only whitespace are now empty strings.

    Args:
        df: The trimmed Canopy DataFrame returned by
            trim_string_columns().

    Returns:
        DataFrame: A DataFrame with empty string values
            replaced by null in every string column.

    Notes:
        Preserve existing null values, non-string columns,
        and all records. Do not perform validation.
    """
    # TODO: Convert empty strings to null in string columns.
    # TODO: Preserve existing null values and other columns.
    raise NotImplementedError(
        "Implement convert_blank_strings_to_null"
    )


def remove_exact_duplicates(df: DataFrame) -> DataFrame:
    """Remove duplicate records after value normalization.

    This is the third cleaning operation.

    Two records are duplicates when all 25 column values
    are identical after trimming and blank-string
    normalization.

    Args:
        df: The normalized Canopy DataFrame returned by
            convert_blank_strings_to_null().

    Returns:
        DataFrame: A DataFrame containing one copy of
            each distinct record.

    Notes:
        Compare all columns, not just order_id or
        customer_id. Preserve records that share an
        identifier but differ in other column values.
    """
    return df.distinct()


def clean_canopy_data(ingested_df: DataFrame) -> DataFrame:
    """Execute the complete Canopy cleaning stage.

    This function is provided. Students should not need
    to modify it.

    The order is important:

    1. Trim whitespace so whitespace-only values become
       empty strings.
    2. Convert empty strings to null so missing values
       have a consistent representation.
    3. Remove duplicates after normalization so records
       made identical by cleaning are recognized.

    Args:
        ingested_df: The 25-column Spark DataFrame returned
            by ingest_canopy_data() in ingest.py.

    Returns:
        DataFrame: The cleaned 25-column Canopy DataFrame,
            ready for validation.

    Notes:
        This function coordinates cleaning operations.
        It does not contain the implementation of any
        individual cleaning operation.
    """
    trimmed_df = trim_string_columns(ingested_df)

    normalized_df = convert_blank_strings_to_null(trimmed_df)

    cleaned_df = remove_exact_duplicates(normalized_df)

    return cleaned_df

