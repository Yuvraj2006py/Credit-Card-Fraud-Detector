"""Module responsible for extracting the credit card transactions dataset.

This module defines a function to load the raw transactions from a CSV
file into a pandas DataFrame. By isolating the extraction logic here
we make it easy to adjust file locations and formats without impacting
the rest of the pipeline.

Functions
---------
extract(file_path: str) -> pandas.DataFrame
    Loads the specified CSV file into a DataFrame.

Example
-------
>>> from scripts.extract import extract
>>> df = extract()
>>> print(df.head())
"""

import logging
import os
from typing import Optional

import pandas as pd


def extract(file_path: Optional[str] = None) -> pd.DataFrame:
    """Load the credit card transactions dataset into a DataFrame.

    Parameters
    ----------
    file_path : Optional[str], default None
        An optional path to the CSV file. When omitted the function
        assumes the dataset lives at ``../data/creditcard.csv`` relative
        to this module.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the raw transactions.

    Raises
    ------
    FileNotFoundError
        If the CSV file cannot be located at the provided path.
    Exception
        Propagates any exception raised by ``pandas.read_csv``.
    """
    # Derive a default path relative to this file when none is provided
    if file_path is None:
        module_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(module_dir, os.pardir, 'data', 'creditcard.csv')
        file_path = os.path.normpath(file_path)

    logging.info("Attempting to read dataset from %s", file_path)
    if not os.path.exists(file_path):
        logging.error("Dataset not found at %s", file_path)
        raise FileNotFoundError(f"Dataset not found at {file_path}")

    try:
        df = pd.read_csv(file_path)
        logging.info("Loaded %d rows and %d columns", df.shape[0], df.shape[1])
        return df
    except Exception as exc:
        logging.exception("Failed to read CSV file: %s", exc)
        raise