"""
This module is a realization of the methodology in the following paper:
`Liu, J. and Timmermann, A., 2013. Optimal convergence trade strategies. The Review of Financial Studies, 26(4), pp.1048-1086.
<https://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.905.236&rep=rep1&type=pdf>`__
"""
# pylint: disable=invalid-name, too-many-instance-attributes

import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression


class OptimalConvergence:
    """
    This module models the optimal convergence trades under both recurring and nonrecurring arbitrage opportunities
    represented by continuing and “stopped” co-integrated price processes.

    Along with delta neutral portfolios, this module also considers unconstrained optimal portfolios where the portfolio weights
    of both the stocks in the spread are calculated dynamically. Conventional long-short delta neutral strategies
    are generally suboptimal and it can be optimal to simultaneously go long (or short) in two mis-priced assets.
    Standard arbitrage strategies and/or delta neutral convergence trades are designed to explore long-term arbitrage
    opportunities but do typically not optimally exploit the short-run risk return trade-off. By placing arbitrage
    opportunities in the context of a portfolio maximization problem, this optimal convergence strategy accounts
    for both arbitrage opportunities and diversification benefits.
    """

    def __init__(self):
        """
        Initializes the parameters of the module.
        """

        # Estimated from error-correction model
        self.ticker_A = None
        self.ticker_B = None
        self.delta_t = 1 / 252
        self.lambda_1 = 0
        self.lambda_2 = None

        # Moment based estimates
        self.b_squared = None
        self.sigma_squared = 0
        self.beta = None

        # Parameters inputted by user
        self.gamma = None  # gamma should be positive
        self.r = None
        self.mu_m = None
        self.sigma_m = None

    def fit(self, prices: pd.DataFrame, mu_m: float, sigma_m: float, r: float, delta_t: float = 1 / 252):
        """
        This method estimates the error-correction terms (lambda) using the inputted pricing data.

        :param prices: (pd.DataFrame) Contains price series of both stocks in spread.
        :param mu_m: (float) Market Risk Premium.
        :param sigma_m: (float) Market Volatility.
        :param r: (float) Interest Rate.
        :param delta_t: (float) Time difference between each index of data, calculated in years.
        """

        # Setting instance attributes
        self.delta_t = delta_t
        self.ticker_A, self.ticker_B = prices.columns[0], prices.columns[1]

        self.mu_m = mu_m
        self.sigma_m = sigma_m
        self.r = r

        # Using Equations (1) and (2) to calculate lambda's and beta term

        x, _ = self._x_tau_calc(prices)
        prices = self._data_preprocessing(prices)

        returns_df = prices.pct_change()
        returns_df = returns_df.replace([np.inf, -np.inf], np.nan).ffill().dropna()

        lr = LinearRegression(fit_intercept=True)

        y_1 = returns_df.iloc[:, 0].to_numpy()
        y_2 = returns_df.iloc[:, 1].to_numpy()

        lr.fit(x[:-1].reshape(-1, 1), y_1)

        self.lambda_1 = -lr.coef_[0]
        beta_1 = (lr.intercept_ - self.r) / self.mu_m

        lr.fit(x[:-1].reshape(-1, 1), y_2)
        self.lambda_2 = lr.coef_[0]
        beta_2 = (lr.intercept_ - self.r) / self.mu_m

        self.beta = (beta_1 + beta_2) / 2
        # Equation (5) in the paper models x as a mean reverting OU process with 0 drift.
        # The parameter estimators are taken from Appendix in Jurek paper.

        mu = 0
        # Estimator for rate of mean reversion
        k = (-1 / self.delta_t) * np.log(np.multiply(x[1:] - mu, x[:-1] - mu).sum()
                                              / np.power(x[1:] - mu, 2).sum())

        # Part of sigma estimation formula
        sigma_calc_sum = np.power((x[1:] - mu - np.exp(-k * self.delta_t) * (x[:-1] - mu))
                                  / np.exp(-k * self.delta_t), 2).sum()

        # Estimator for standard deviation
        b_x = np.sqrt(2 * k * sigma_calc_sum / ((np.exp(2 * k * self.delta_t) - 1) * (len(x) - 2)))

        self.b_squared = (b_x ** 2) / 2

        sigma_squared_1 = (np.var(y_1, ddof=1) / self.delta_t) - self.b_squared - (self.beta ** 2) * (self.sigma_m ** 2)
        sigma_squared_2 = (np.var(y_2, ddof=1) / self.delta_t) - self.b_squared - (self.beta ** 2) * (self.sigma_m ** 2)

        if sigma_squared_1 < 0 or sigma_squared_2 < 0:
            sigma_squared_1 = max(0, sigma_squared_1)
            sigma_squared_2 = max(0, sigma_squared_2)

            warnings.warn("The value of sigma estimated from the inputted data is poor. "
                          "This pricing data might be not be well-suited for this particular model. "
                          "Possible solution would be to use a longer time period, and use pairs suited for exploiting arbitrage mis-pricings.")

        self.sigma_squared = (sigma_squared_1 + sigma_squared_2) / 2


    def describe(self) -> pd.Series:
        """
        Method returns values of instance attributes calculated from training data.

        :return: (pd.Series) Series describing parameter values.
        """

        if self.beta is None:
            raise Exception("Please run the fit method before calling describe.")

        # List defines the indexes of the final pandas object
        index = ['Ticker of first stock', 'Ticker of second stock',
                 'lambda_1', 'lambda_2', 'b_squared', 'sigma_squared',
                 'beta']

        # List defines the values of the final pandas object
        data = [self.ticker_A, self.ticker_B,
                self.lambda_1, self.lambda_2, self.b_squared, self.sigma_squared,
                self.beta]

        # Combine data and indexes into the pandas Series
        output = pd.Series(data=data, index=index)

        return output


    def unconstrained_portfolio_weights_continuous(self, prices: pd.DataFrame, gamma: float = 4) -> tuple:
        """
        Implementation of Proposition 1.

        This method calculates the portfolio weights for the market asset and for both the stocks in the spread
        when there are no constraints put on values of lambda.
        We also assume a continuing cointegrated price process (recurring arbitrage opportunities),
        which gives closed-form solutions for the optimal portfolio weights.

        If lambda_1 = lambda_2, from the portfolio weights outputted from this method phi_2 + phi_1 = 0,
        which implies delta neutrality. This follows Proposition 3 in the paper.

        :param prices: (pd.DataFrame) Contains price series of both stocks in spread.
        :param gamma: (float) Signifies investor's attitude towards risk (positive float value).
        :return: (tuple) Consists of three numpy arrays: weights for asset 1, asset 2, and market portfolio.
        """

        if self.beta is None:
            raise Exception("Please run fit before calling this method.")

        if gamma <= 0:
            raise Exception("The value of gamma should be positive.")

        self.gamma = gamma

        x, tau = self._x_tau_calc(prices)

        C_t = self._C_calc(tau)

        matrix = np.zeros((2, 2))
        matrix[0, 0] = self.sigma_squared + self.b_squared
        matrix[0, 1] = - self.sigma_squared
        matrix[1, 0] = matrix[0, 1]
        matrix[1, 1] = matrix[0, 0]

        C_matrix = np.zeros((2, len(tau)))
        C_matrix[0, :] = - self.lambda_1 + self.b_squared * C_t
        C_matrix[1, :] =   self.lambda_2 - self.b_squared * C_t

        phi = (1 / (self.gamma * (2 * self.sigma_squared + self.b_squared) * self.b_squared)) \
              * (matrix @ C_matrix) * x

        phi_1 = phi[0, :]
        phi_2 = phi[1, :]

        phi_m = (self.mu_m / (self.gamma * self.sigma_m ** 2)) - (phi_1 + phi_2) * self.beta

        return phi_1, phi_2, phi_m


    def delta_neutral_portfolio_weights_continuous(self, prices: pd.DataFrame, gamma: float = 4) -> tuple:
        """
        Implementation of Proposition 2.

        This method calculates the portfolio weights for the market asset and for both the stocks in the spread
        when the portfolio is constrained to be delta-neutral, where sum of portfolio weights of both the assets
        in the spread is zero. We also assume a continuing cointegrated price process (recurring arbitrage opportunities),
        which gives closed-form solutions for the optimal portfolio weights.

        :param prices: (pd.DataFrame) Contains price series of both stocks in spread.
        :param gamma: (float) Signifies investor's attitude towards risk (positive float value).
        :return: (tuple) Consists of three numpy arrays: weights for asset 1, asset 2, and market portfolio.
        """

        if self.beta is None:
            raise Exception("Please run fit before calling this method.")

        if gamma <= 0:
            raise Exception("The value of gamma should be positive.")

        self.gamma = gamma

        x, tau = self._x_tau_calc(prices)

        D_t = self._D_calc(tau)

        phi_1 = (-(self.lambda_1 + self.lambda_2) * x + 2 * self.b_squared * D_t * x) / (2 * self.gamma * self.b_squared)

        phi_2 = -phi_1

        phi_m = self.mu_m / (self.gamma * self.sigma_m ** 2) + np.zeros(phi_1.shape)

        return phi_1, phi_2, phi_m


    def wealth_gain_continuous(self, gamma: float = 4, sigma: float = 0.15, b: float = 0.30, mu_m: float = 0.05,
                               sigma_m: float = 0.35, r: float = 0.02, lambda_1: float = 0.52, lambda_2: float = -0.35 ) -> np.array:
        """
        Implementation of Proposition 4.

        This method calculates the expected wealth gain of the unconstrained optimal strategy relative to the
        delta neutral strategy assuming a mis-pricing of the spread.

        We take fixed values of spread between [0, 0.2]. The time to maturity is assumed to be 1 year for each value of spread.

        The default values of the input parameters are taken according to the paper.

        :param gamma: (float) Signifies investor's attitude towards risk (positive float value).
        :param b: (float) Model Parameter found in Equation (2-3) in the paper.
        :param mu_m: (float) Market Risk Premium.
        :param sigma_m: (float) Market Volatility.
        :param r: (float) Interest Rate.
        :param lambda_1: (float) Parameter signifies relative liquidity of asset 1.
        :param lambda_2: (float) Parameter signifies relative liquidity of asset 2.
        :param sigma: (float) Model Parameter found in Equation (2-3) in the paper.
        :return: (np.array) Wealth gain numpy array.
        """

        if gamma <= 0:
            raise Exception("The value of gamma should be positive.")

        self.gamma = gamma

        x = np.linspace(0, 0.2, 252)
        tau = np.ones(len(x))

        self.sigma_squared = sigma ** 2
        self.b_squared = b ** 2
        self.mu_m = mu_m
        self.sigma_m = sigma_m
        self.r = r
        self.lambda_1 = lambda_1
        self.lambda_2 = lambda_2

        u_x_t = self._u_func_continuous_calc(x, tau)
        v_x_t = self._v_func_continuous_calc(x, tau)

        R = np.exp((u_x_t - v_x_t) / (1 - self.gamma))
        return R


    @staticmethod
    def plot_wealth_process(prices: pd.DataFrame, phi_1: np.array, phi_2: np.array, r: float, delta_t: float = 1/252):
        """
        Function for plotting the wealth process.

        :param prices: (pd.DataFrame) Contains price series of both stocks in spread.
        :param phi_1: (np.array) Weights for asset 1.
        :param phi_2: (np.array) Weights for asset 2.
        :param r: (float) Interest Rate.
        :param delta_t: (float) Time difference between each index of data, calculated in years.
        """

        returns_df = prices.ffill().pct_change()
        returns_df = returns_df.replace([np.inf, -np.inf], np.nan).ffill().dropna()
        phi_1, phi_2 = phi_1[1:], phi_2[1:]

        phi_1_ = phi_1 / (abs(phi_1) + abs(phi_2))
        phi_2_ = phi_2 / (abs(phi_1) + abs(phi_2))
        phi_1, phi_2 = phi_1_, phi_2_

        V = np.ones(len(prices) - 1)

        for i in range(len(prices) - 2):
            # Calculating the wealth process from optimal weights.
            # Follows Section 2 in the paper.

            V[i + 1] = V[i] + V[i] * (r * delta_t + phi_1[i] * (returns_df.iloc[i, 0] - r * delta_t)
                                      + phi_2[i] * (returns_df.iloc[i, 1] - r * delta_t))

        # Plotting
        plt.figure(figsize=(10, 6))
        plt.plot(prices.index[1:], V, 'c-')
        plt.title("Wealth process with initial wealth normalized to 1")
        plt.ylabel("Wealth")
        plt.xlabel("Date")
        plt.show()


    def _x_tau_calc(self, prices: pd.DataFrame) -> tuple:
        """
        Calculates the error correction term x given in equation (4) and the time remaining in years.

        :param prices: (pd.DataFrame) Contains price series of both stocks in spread.
        :return: (tuple) Consists of two numpy arrays: error correction term x, time remaining in years.
        """

        prices = self._data_preprocessing(prices).to_numpy()

        t = np.arange(0, len(prices)) * self.delta_t
        tau = t[-1] - t  # Stores time remaining till closure (in years)

        x = np.log(prices[:, 0] / prices[:, 1])

        return x, tau


    def _lambda_x_calc(self) -> float:
        """
        Helper function calculates lambda_x.

        :return: (float) Final value of lambda_x.
        """

        lambda_x = self.lambda_1 + self.lambda_2  # This should be always positive

        return lambda_x


    def _xi_calc(self) -> tuple:
        """
        Helper function which calculates xi, present in Appendix A.1.
        Xi is used in the calculations of A and C functions.

        :return: (tuple) Consists of two floats, xi and lambda_x.
        """

        lambda_x = self._lambda_x_calc()

        inner_term = ((self.lambda_1 ** 2 + self.lambda_2 ** 2) * (self.sigma_squared + self.b_squared)
                      + 2 * self.lambda_1 * self.lambda_2 * self.sigma_squared) / (
                                 self.b_squared + 2 * self.sigma_squared)

        xi = np.sqrt(lambda_x ** 2 - 2 * inner_term * (1 - self.gamma))

        return xi, lambda_x


    def _C_calc(self, tau: np.array) -> np.array:
        """
        Implementation of function C given in Appendix A.1.

        :param tau: (np.array) Time remaining in years.
        :return: (np.array) Final C array.
        """

        xi, lambda_x = self._xi_calc()

        C_plus = (lambda_x + xi) / (2 * self.b_squared)
        C_minus = (lambda_x - xi) / (2 * self.b_squared)

        exp_term = np.exp((2 * self.b_squared / self.gamma) * (C_plus - C_minus) * tau)

        C = C_minus * (exp_term - 1) / (exp_term - (C_minus / C_plus))

        return C


    def _D_calc(self, tau: np.array) -> np.array:
        """
        Implementation of function D given in Appendix A.2.

        :param tau: (np.array) Time remaining in years.
        :return: (np.array) Final D array.
        """

        lambda_x = self._lambda_x_calc()
        sqrt_term = np.sqrt(self.gamma)

        D_plus = (lambda_x / (2 * self.b_squared)) * (1 + sqrt_term)
        D_minus = (lambda_x / (2 * self.b_squared)) * (1 - sqrt_term)

        exp_term = np.exp(2 * lambda_x * tau / sqrt_term)

        D = (1 - exp_term) / ((1 / D_plus) - (exp_term / D_minus))

        return D


    def _A_calc(self, tau: np.array) -> np.array:
        """
        Implementation of function A given in Appendix A.1.

        :param tau: (np.array) Time remaining in years.
        :return: (np.array) Final A array.
        """

        xi, lambda_x = self._xi_calc()

        A = self._A_B_helper(lambda_x, tau, xi)

        return A


    def _B_calc(self, tau: np.array) -> np.array:
        """
        Implementation of function B given in Appendix A.2.

        :param tau: (np.array) Time remaining in years.
        :return: (np.array) Final B array.
        """

        lambda_x = self._lambda_x_calc()
        eta = lambda_x * np.sqrt(self.gamma)

        B = self._A_B_helper(lambda_x, tau, eta)

        return B


    def _A_B_helper(self, lambda_x: float, tau: np.array, rep_term: float) -> np.array:
        """
        Helper function implements the common formulae present in A and B function calculations.
        Returns either the A or B array depending on whether xi or eta is inputted to the argument rep_term.

        :param lambda_x: (float) Sum of lambda's.
        :param tau: (np.array) Time remaining in years.
        :param rep_term: (float) Either the xi or eta value.
        :return: (np.array) Final result array.
        """

        inner_exp_term = (rep_term / self.gamma) * tau
        exp_term_1 = np.exp(inner_exp_term)
        exp_term_2 = np.exp(-inner_exp_term)

        first_term = self.r + (1 / (2 * self.gamma)) * (self.mu_m ** 2 / self.sigma_m ** 2)
        log_term = np.log((lambda_x / 2) * ((exp_term_1 - exp_term_2) / rep_term) + 0.5 * (exp_term_1 + exp_term_2))

        result_array = first_term * (1 - self.gamma) * tau + (lambda_x / 2) * tau - (self.gamma / 2) * log_term

        return result_array


    def _u_func_continuous_calc(self, x: np.array, tau: np.array) -> np.array:
        """
        Implementation of the u function given in Lemma 1.

        :param x: (np.array) Error correction term.
        :param tau: (np.array) Time remaining in years.
        :return: (np.array) Final output of u function.
        """

        C_t = self._C_calc(tau)
        A_t = self._A_calc(tau)

        u = A_t + 0.5 * C_t * np.power(x, 2)

        return u


    def _v_func_continuous_calc(self, x: np.array, tau: np.array) -> np.array:
        """
        Implementation of the v function given in Lemma 2.

        :param x: (np.array) Error correction term.
        :param tau: (np.array) Time remaining in years.
        :return: (np.array) Final output of u function.
        """

        D_t = self._D_calc(tau)
        B_t = self._B_calc(tau)

        v = B_t + 0.5 * D_t * np.power(x, 2)

        return v


    @staticmethod
    def _data_preprocessing(prices: pd.DataFrame) -> pd.DataFrame:
        """
        Helper function for input data preprocessing.

        :param prices: (pd.DataFrame) Pricing data of both stocks in spread.
        :return: (pd.DataFrame) Processed dataframe.
        """

        return prices.ffill()
