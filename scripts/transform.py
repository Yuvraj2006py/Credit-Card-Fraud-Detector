"""Module responsible for transforming the raw dataset.

This module handles data cleaning and feature engineering for the
credit card fraud detection pipeline. It cleans missing values and
duplicates, then derives additional features such as the hour of day
when each transaction occurred and a categorical bucket for the
transaction amount.

Functions
---------
transform(df: pandas.DataFrame) -> pandas.DataFrame
    Cleans and augments the provided DataFrame with engineered
    features.
"""

import logging
import pandas as pd


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich the raw transactions dataset.

    The transformation consists of three steps:

    1. Remove duplicate rows, which ensures no transaction is counted
       twice during training or loading.
    2. Replace any missing values with sensible defaults (zero) to
       prevent downstream algorithms from failing.
    3. Add two engineered features:

       - ``HourOfDay``: The hour (0–23) extracted from the ``Time``
         column, computed as the number of whole hours since the
         beginning of the dataset modulo 24. This helps capture
         diurnal spending patterns that may differ between fraud and
         legitimate transactions.
       - ``AmountCategory``: A categorical representation of the
         ``Amount`` column that bins transaction amounts into
         descriptive ranges (Small, Medium, Large, XL). Categorising
         the amounts often aids simple models by grouping similar
         magnitudes together.

    Parameters
    ----------
    df : pandas.DataFrame
        Raw credit card transactions. Must contain ``Time`` and
        ``Amount`` columns, otherwise a ``KeyError`` is raised.

    Returns
    -------
    pandas.DataFrame
        A new DataFrame with duplicates removed, missing values filled
        and the engineered features appended.
    """
    # Remove duplicate transactions
    original_count = len(df)
    df = df.drop_duplicates().copy()
    logging.info("Dropped %d duplicate rows", original_count - len(df))

    # Fill missing values; using zero to maintain numeric type
    if df.isnull().values.any():
        df = df.fillna(0)
        logging.info("Filled missing values with zeros")

    # Ensure required columns exist
    if 'Time' not in df.columns or 'Amount' not in df.columns:
        missing = [col for col in ('Time', 'Amount') if col not in df.columns]
        raise KeyError(f"Missing required columns: {missing}")

    # Derive the hour of day (0-23) from the 'Time' column. The original
    # dataset records time in seconds since the first transaction.
    df['HourOfDay'] = ((df['Time'] // 3600) % 24).astype(int)

    # Create a categorical feature for the 'Amount' column. The bins
    # intentionally span a wide range to capture differences between
    # micropayments and very large transactions. Adjust boundaries
    # according to domain knowledge if necessary.
    bins = [0, 50, 200, 1000, float('inf')]
    labels = ['Small', 'Medium', 'Large', 'XL']
    df['AmountCategory'] = pd.cut(df['Amount'], bins=bins, labels=labels, right=False)

    logging.info("Added HourOfDay and AmountCategory features")
    return df