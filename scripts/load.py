"""Module responsible for loading transformed data into a relational database.

This module provides a single function to insert a pandas DataFrame
into a PostgreSQL database using SQLAlchemy. It encapsulates the
connection details and table name so callers only need to pass in
their data.

Functions
---------
load(df: pandas.DataFrame, table_name: str, db_url: str) -> None
    Write the provided DataFrame to the specified PostgreSQL table.
"""

import logging
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine


def _get_engine(db_url: str) -> Engine:
    """Construct a SQLAlchemy engine for the given connection URL.

    Parameters
    ----------
    db_url : str
        A SQLAlchemy–compatible connection string.

    Returns
    -------
    sqlalchemy.engine.base.Engine
        A SQLAlchemy engine bound to the specified database.
    """
    return create_engine(db_url)


def load(
    df: pd.DataFrame,
    table_name: str = 'transactions',
    db_url: str = 'postgresql+psycopg2://airflow:airflow@postgres:5432/frauddb',
) -> None:
    """Persist the DataFrame to a PostgreSQL table.

    If a table with the given name already exists, its contents will
    be replaced. The function uses SQLAlchemy to handle the database
    connection and pandas' ``to_sql`` for bulk insertion.

    Parameters
    ----------
    df : pandas.DataFrame
        The DataFrame containing fraud predictions to persist.
    table_name : str, default 'transactions'
        The name of the destination table within the database.
    db_url : str, default 'postgresql+psycopg2://airflow:airflow@postgres:5432/frauddb'
        A SQLAlchemy connection URL pointing at the target PostgreSQL
        instance.

    Returns
    -------
    None
        This function writes data to the database but does not
        return anything.
    """
    logging.info(
        "Writing %d rows to table '%s' in database %s",
        len(df),
        table_name,
        db_url,
    )
    engine = _get_engine(db_url)
    with engine.begin() as connection:
        # Write the DataFrame to the specified table; replace if it exists
        df.to_sql(table_name, con=connection, if_exists='append', index=False)
    logging.info("Data successfully written to table '%s'", table_name)