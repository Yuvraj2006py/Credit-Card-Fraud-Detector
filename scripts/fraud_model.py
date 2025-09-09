"""Module providing a simple fraud detection model.

This module encapsulates the machine learning logic used to train a
logistic regression classifier on the transformed credit card
transactions. It exposes a helper function to fit the model and
predict fraud labels for the entire dataset. While extremely simple
compared to production fraud systems, it demonstrates a typical
workflow: preparing features, training a classifier and generating
predictions.

Functions
---------
train_and_predict(df: pandas.DataFrame) -> pandas.DataFrame
    Fits a logistic regression model and appends a ``FraudPrediction``
    column to the DataFrame indicating predicted class labels.
"""

import logging
from typing import Tuple

import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split


def train_and_predict(df: pd.DataFrame) -> pd.DataFrame:
    """Train a logistic regression model and predict fraud labels.

    The function splits the input DataFrame into features and target,
    applies one‑hot encoding to the categorical ``AmountCategory``
    column, fits a logistic regression classifier and then predicts
    fraud labels for all observations. The predicted labels are
    appended to the returned DataFrame under the ``FraudPrediction``
    column.

    Parameters
    ----------
    df : pandas.DataFrame
        The transformed DataFrame containing features and the target
        column named ``Class``.

    Returns
    -------
    pandas.DataFrame
        The input DataFrame with an additional ``FraudPrediction``
        column containing the predicted class (0 or 1) for each
        transaction.
    """
    # Ensure the target column exists
    if 'Class' not in df.columns:
        raise KeyError("DataFrame must contain a 'Class' column for training")

    # Separate features and target
    X = df.drop(columns=['Class']).copy()
    y = df['Class']

    # One‑hot encode the AmountCategory feature; drop first level to
    # avoid multicollinearity
    if 'AmountCategory' in X.columns:
        X = pd.get_dummies(X, columns=['AmountCategory'], drop_first=True)

    # Train/test split for model evaluation; we still fit on the full
    # training portion but predictions are generated on the entire
    # dataset. Splitting prevents data leakage during parameter
    # estimation when the dataset is updated over time.
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # Instantiate and train logistic regression. Increased max_iter
    # ensures convergence given the number of records. The saga solver
    # handles both L1 and L2 regularisation and supports multinomial
    # classification if extended in future. For binary classification
    # it's still appropriate.
    model = LogisticRegression(max_iter=1000, solver='saga', n_jobs=-1)

    logging.info("Training logistic regression model on %d samples", len(X_train))
    model.fit(X_train, y_train)

    # Predict on full feature set for consistency across training and
    # testing data. This yields a predicted class label for each
    # transaction. Predictions are deterministic given the model
    # parameters.
    predictions = model.predict(X)
    df = df.copy()
    df['FraudPrediction'] = predictions

    logging.info(
        "Model training completed; %d fraud cases predicted",
        int(df['FraudPrediction'].sum()),
    )
    return df