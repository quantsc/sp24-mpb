"""
Base class for cointegration approach in statistical arbitrage.
"""

from abc import ABC
import pandas as pd


class CointegratedPortfolio(ABC):
    """
    Class for portfolios formed using the cointegration method (Johansen test, Engle-Granger test).
    """

    def construct_mean_reverting_portfolio(self, price_data: pd.DataFrame,
                                           cointegration_vector: pd.Series = None) -> pd.Series:
        """
        When cointegration vector was formed, this function is used to multiply asset prices by cointegration vector
        to form mean-reverting portfolio which is analyzed for possible trade signals.

        :param price_data: (pd.DataFrame) Price data with columns containing asset prices.
        :param cointegration_vector: (pd.Series) Cointegration vector used to form a mean-reverting portfolio.
            If None, a cointegration vector with maximum eigenvalue from fit() method is used.
        :return: (pd.Series) Cointegrated portfolio dollar value.
        """

        if cointegration_vector is None:
            cointegration_vector = self.cointegration_vectors.iloc[0]  # Use eigenvector with biggest eigenvalue.

        return (cointegration_vector * price_data).sum(axis=1)

    def get_scaled_cointegration_vector(self, cointegration_vector: pd.Series = None) -> pd.Series:
        """
        This function returns the scaled values of the cointegration vector in terms of how many units of other
        cointegrated assets should be bought if we buy one unit of one asset.

        :param cointegration_vector: (pd.Series) Cointegration vector used to form a mean-reverting portfolio.
            If None, a cointegration vector with maximum eigenvalue from fit() method is used.
        :return: (pd.Series) The scaled cointegration vector values.
        """

        if cointegration_vector is None:
            cointegration_vector = self.cointegration_vectors.iloc[0]  # Use eigenvector with biggest eigenvalue

        scaling_coefficient = 1 / cointegration_vector.iloc[0]  # Calculating the scaling coefficient

        # Calculating the scaled cointegration vector
        scaled_cointegration_vector = cointegration_vector * scaling_coefficient

        return scaled_cointegration_vector
