"""
This module implements the PCA approach described by by Marco Avellaneda and Jeong-Hyun Lee in
`"Statistical Arbitrage in the U.S. Equities Market"
<https://math.nyu.edu/faculty/avellane/AvellanedaLeeStatArb20090616.pdf>`_.
"""


import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.linear_model import LinearRegression

# pylint: disable=invalid-name


class PCAStrategy:
    """
    This strategy creates mean reverting portfolios using Principal Components Analysis. The idea of the strategy
    is to estimate PCA factors affecting the dynamics of assets in a portfolio. Thereafter, for each asset in a
    portfolio, we define OLS residuals by regressing asset returns on PCA factors. These residuals are used to
    calculate S-scores to generate trading signals and the regression coefficients are used to construct
    eigen portfolios for each asset. If the eigen portfolio shows good mean-reverting properties and the S-score
    deviates enough from its mean value, that eigen portfolio is being traded. The output trading signals of
    this strategy are weights for each asset in a portfolio at each given time. These weights are are a composition
    of all eigen portfolios that satisfy the required properties.
    """

    def __init__(self, n_components: int = 15):
        """
        Initialize PCA StatArb Strategy.

        The original paper suggests that the number of components would be chosen to explain at least
        50% of the total variance in time. Authors also denote that for G8 economies, stock returns are explained
        by approximately 15 factors (or between 10 and 20 factors).

        :param n_components: (int) Number of PCA principal components to use in order to build factors.
        """

        self.n_components = n_components  # Number of PCA components
        self.pca_model = PCA(n_components)  # Model for PCA calculation

    @staticmethod
    def standardize_data(matrix: pd.DataFrame) -> (pd.DataFrame, pd.Series):
        """
        A function to standardize data (returns) that is being fed into the PCA.

        The standardized returns (R)are calculated as:

        R_standardized = (R - mean(R)) / st.d.(R)

        :param matrix: (pd.DataFrame) DataFrame with returns that need to be standardized.
        :return: (pd.DataFrame. pd.Series) a tuple with two elements: DataFrame with standardized returns and Series of
            standard deviations.
        """

        # Standardizing data
        standardized = (matrix - matrix.mean()) / matrix.std()

        return standardized, matrix.std()

    def get_factorweights(self, matrix: pd.DataFrame) -> pd.DataFrame:
        """
        A function to calculate weights (scaled eigen vectors) to use for factor return calculation.

        Weights are calculated from PCA components as:

        Weight = Eigen vector / st.d.(R)

        So the output is a dataframe containing the weight for each asset in a portfolio for each eigen vector.

        :param matrix: (pd.DataFrame) Dataframe with index and columns containing asset returns.
        :return: (pd.DataFrame) Weights (scaled PCA components) for each index from the matrix.
        """

        # Standardizing input
        standardized, std = self.standardize_data(matrix)

        # Fitting PCA
        pca_factors = self.pca_model.fit(standardized)

        # Output eigen vectors for weights calculation
        weights = pd.DataFrame(pca_factors.components_, columns=standardized.columns)

        # Scaling eigen vectors to get weights for eigen portfolio creation
        weights = weights / std

        return weights

    def get_residuals(self, matrix: pd.DataFrame, pca_factorret: pd.DataFrame) -> (pd.DataFrame, pd.Series):
        """
        A function to calculate residuals given matrix of returns and factor returns.

        First, for each asset in a portfolio, we fit its returns to PCA factor returns as:

        Returns = beta_0 + beta * PCA_factor_return + residual

        Residuals are used to generate trading signals and beta coefficients are used as
        weights to later construct eigenportfolios for each asset.

        :param matrix: (pd.DataFrame) Dataframe with index and columns containing asset returns.
        :param pca_factorret: (pd.DataFrame) Dataframe with PCA factor returns for assets.
        :return: (pd.DataFrame, pd.Series) Dataframe with residuals and series of beta coefficients.
        """

        # Creating a DataFrame to store residuals
        residual = pd.DataFrame(columns=matrix.columns, index=matrix.index)

        # And a DataFrame to store regression coefficients
        coefficient = pd.DataFrame(columns=matrix.columns, index=range(self.n_components))

        # A class for regression
        regression = LinearRegression()

        # Iterating through all tickers - to create residuals for every eigen portfolio
        for ticker in matrix.columns:
            # Fitting a regression
            regression.fit(pca_factorret, matrix[ticker])

            # Calculating residual for eigen portfolio
            residual[ticker] = matrix[ticker] - regression.intercept_ - np.dot(pca_factorret, regression.coef_)

            # Writing down the regression coefficient
            coefficient[ticker] = regression.coef_

        return residual, coefficient

    @staticmethod
    def get_sscores(residuals: pd.DataFrame, k: float) -> pd.Series:
        """
        A function to calculate S-scores for asset eigen portfolios given dataframes of residuals
        and a mean reversion speed threshold.

        From residuals, a discrete version of the OU process is created for each asset eigen portfolio.

        If the OU process of the asset shows a mean reversion speed above the given
        threshold k, it can be traded and the S-score is being calculated for it.

        The output of this function is a dataframe with S-scores that are directly used
        to determine if the eigen portfolio of a given asset should be traded at this period.

        In the original paper, it is advised to choose k being less than half of a
        window for residual estimation. If this window is 60 days, half of it is 30 days.
        So k > 252/30 = 8.4. (Assuming 252 trading days in a year)

        :param residuals: (pd.DataFrame) Dataframe with residuals after fitting returns to
            PCA factor returns.
        :param k: (float) Required speed of mean reversion to use the eigen portfolio in
            trading.
        :return: (pd.Series) Series of S-scores for each asset for a given residual dataframe.
        """

        # Creating the auxiliary process K_k - discrete version of X(t)
        X_k = residuals.cumsum()

        # Variable for mean - m
        m = pd.Series(index=X_k.columns, dtype=np.float64)

        # Variable sigma for S-score calculation
        sigma_eq = pd.Series(index=X_k.columns, dtype=np.float64)

        # Iterating over tickers
        for ticker in X_k.columns:

            # Calculate parameter b using auto-correlations
            b = X_k[ticker].autocorr()

            # If mean reversion times are good, enter trades
            if -np.log(b) * 252 > k:
                # Temporary variable for a + zeta_n
                a_zeta = (X_k[ticker] - X_k[ticker].shift(1) * b)[1:]

                # Deriving the a parameter
                a = a_zeta.mean()

                # Deriving zeta_n series
                zeta = a_zeta - a

                # Calculating the mean parameter for every ticker
                m[ticker] = a / (1 - b)

                # Calculating sigma for S-score of each ticker
                sigma_eq[ticker] = np.sqrt(zeta.var() / (1 - b * b))

        # Small filtering for parameter m and sigma
        m = m.dropna()
        sigma_eq = sigma_eq.dropna()

        # Original paper suggests that centered means show better results
        m = m - m.mean()

        # S-score calculation for each ticker
        s_score = -m / sigma_eq

        return s_score

    @staticmethod
    def _generate_signals(position_stock: pd.DataFrame, s_scores: pd.Series, coeff: pd.DataFrame,
                          sbo: float, sso: float, ssc: float, sbc: float, size: float) -> pd.DataFrame:
        """
        A helper function to generate trading signals based on S-scores.

        This function follows the logic:

        Enter a long position if s-score < −sbo
        Close a long position if s-score > −ssc
        Enter a short position if s-score > +sso
        Close a short position if s-score < +sbc

        :param position_stock: (pd.DataFrame) Dataframe with current positions for each asset in each
            eigen portfolio.
        :param s_scores: (pd.Series) Series with S-scores used to generate trading signals.
        :param coeff: (pd.DataFrame) Dataframe with regression coefficients used to create eigen portfolios.
        :param sbo: (float) Parameter for signal generation for the S-score.
        :param sso: (float) Parameter for signal generation for the S-score.
        :param ssc: (float) Parameter for signal generation for the S-score.
        :param sbc: (float) Parameter for signal generation for the S-score.
        :param size: (float) Number of units invested in assets when opening trades. So when opening
            a long position, buying (size) units of stock and selling (size) * betas units of other
            stocks.
        :return: (pd.DataFrame) Updated dataframe with positions for each asset in each eigen portfolio.
        """

        # Generating signals using obtained s-scores
        for ticker in position_stock.columns:

            # If no generated S-score then we exit the current position
            if ticker not in s_scores.index:
                if position_stock[ticker][-1] != 0:
                    position_stock[ticker] = 0

            # If we have an S-score generated
            else:
                if position_stock[ticker][-1] == 0:

                    # Entering a long position
                    if s_scores[ticker] < -sbo:
                        position_stock.loc[-1, ticker] = size
                        position_stock.loc[0:, ticker] = -size * coeff[ticker]

                    # Entering a short position
                    elif s_scores[ticker] > sso:
                        position_stock.loc[-1, ticker] = - size
                        position_stock.loc[0:, ticker] = size * coeff[ticker]

                # Exiting a long position
                elif position_stock[ticker][0] > 0 and s_scores[ticker] > -ssc:
                    position_stock[ticker] = 0

                # Exiting a short position
                elif position_stock[ticker][0] < 0 and s_scores[ticker] < sbc:
                    position_stock[ticker] = 0

        return position_stock

    def get_signals(self, matrix: pd.DataFrame, k: float = 8.4, corr_window: int = 252,
                    residual_window: int = 60, sbo: float = 1.25, sso: float = 1.25,
                    ssc: float = 0.5, sbc: float = 0.75, size: float = 1) -> pd.DataFrame:
        """
        A function to generate trading signals for given returns matrix with parameters.

        First, the correlation matrix to get PCA components is calculated using a
        corr_window parameter. From this, we get weights to calculate PCA factor returns.
        These weights are being recalculated each time we generate (residual_window) number
        of signals.

        It is expected that corr_window>residual_window. In the original paper, corr_window is
        set to 252 days and residual_window is set to 60 days. So with corr_window==252, the
        first 252 observation will be used for estimation and the first signal will be
        generated for the 253rd observation.

        Next, we pick the last (residual_window) observations to compute PCA factor returns and
        fit them to residual_window observations to get residuals and regression coefficients.

        Based on the residuals the S-scores are being calculated. These S-scores are calculated as:

        s_i = (X_i(t) - m_i) / sigma_i

        Where X_i(t) is the OU process generated from the residuals, m_i and sigma_i are the
        calculated properties of this process.

        The S-score is being calculated only for eigen portfolios that show mean reversion speed
        above the given threshold k.

        In the original paper, it is advised to choose k being less than half of a
        window for residual estimation. If this window is 60 days, half of it is 30 days.
        So k > 252/30 = 8.4. (Assuming 252 trading days in a year)

        So, we can have mean-reverting eigen portfolios for each asset in our portfolio. But this
        portfolio is worth investing in only if it shows good mean reversion speed and the S-score
        has deviated enough from its mean value. Based on this logic we pick promising eigen portfolios
        and invest in them. The trading signals we get are the target weights for each of the assets
        in our portfolio at any given time.

        Trading rules to enter a mean-reverting portfolio based on the S-score are:

        Enter a long position if s-score < −sbo
        Close a long position if s-score > −ssc
        Enter a short position if s-score > +sso
        Close a short position if s-score < +sbc

        The authors empirically chose the optimal values for the above parameters based on stock
        prices for years 2000-2004 as: sbo = sso = 1.25; sbc = 0.75; ssc = 0.5.

        Opening a long position on an eigne portfolio means buying one dollar of the corresponding asset
        and selling beta_i1 dollars of weights of other assets from component1, beta_i2 dollars of weights
        of other assets from component2 and so on. Opening a short position means selling the corresponding
        asset and buying betas of other assets.

        :param matrix: (pd.DataFrame) Dataframe with returns for assets.
        :param k: (float) Required speed of mean reversion to use the eigen portfolio in trading.
        :param corr_window: (int) Look-back window used for correlation matrix estimation.
        :param residual_window: (int) Look-back window used for residuals calculation.
        :param sbo: (float) Parameter for signal generation for the S-score.
        :param sso: (float) Parameter for signal generation for the S-score.
        :param ssc: (float) Parameter for signal generation for the S-score.
        :param sbc: (float) Parameter for signal generation for the S-score.
        :param size: (float) Number of units invested in assets when opening trades. So when opening
            a long position, buying (size) units of stock and selling (size) * betas units of other
            stocks.
        :return: (pd.DataFrame) DataFrame with target weights for each asset at every observation.
            It is being calculated as a combination of all eigen portfolios that are satisfying the
            mean reversion speed requirement and S-score values.
        """
        # pylint: disable=too-many-locals

        # Dataframe containing target quantities - trading signals
        target_quantities = pd.DataFrame()

        # Series of current positions for assets in our portfolio
        position_stock = pd.DataFrame(0, columns=matrix.columns, index=[-1] + list(range(self.n_components)))

        # Iterating through time windows
        for t in range(corr_window - 1, len(matrix.index) - 1):

            # Each time we generate (residual_window) number of signals we update our weights
            if (t - (corr_window - 1)) % residual_window == 0:
                # Getting a new set of observations for correlation matrix generation
                obs_corr = matrix[(t - corr_window + 1):(t + 1)]
                # Updating factor weights
                weights = self.get_factorweights(obs_corr)

            # Look-back window of observations used
            obs_residual = matrix[(t - residual_window + 1):(t + 1)]

            # PCA factor returns - a product of weights and observations
            factorret_resid = pd.DataFrame(np.dot(obs_residual, weights.transpose()), index=obs_residual.index)

            # Calculating residuals for this window
            resid, coeff = self.get_residuals(obs_residual, factorret_resid)

            # Finding the S-scores for eigen portfolios in this period
            s_scores = self.get_sscores(resid, k)

            # Generating signals using obtained S-scores
            position_stock = self._generate_signals(position_stock, s_scores, coeff,
                                                    sbo, sso, ssc, sbc, size)

            # Temporary series to store all weights
            position_stock_temp = pd.Series(0, index=matrix.columns, dtype=np.float64)

            # Sum over all components to combine betas (other assets) all eigen portfolios
            fac_sum = position_stock.sum(axis=1)[1:]

            # Iterating through tickers inside weights
            for ticker in weights.columns:
                # Multiplying our target quantities by weights
                position_stock_temp = sum(weights[ticker] * fac_sum)

            # Adding also first stocks from all eigen portfolios
            position_stock_temp = position_stock_temp + position_stock.iloc[0]

            # Adding final Series of weights to a general DataFrame with weights
            target_quantities[matrix.index[t]] = position_stock_temp

        # Transposing to make dates as an index of the resulting DataFrame
        target_quantities = target_quantities.T

        return target_quantities
