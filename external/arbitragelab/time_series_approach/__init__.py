"""
This module implements Time Series-based Statistical Arbitrage strategies.
"""

from arbitragelab.time_series_approach.arima_predict import AutoARIMAForecast, get_trend_order
from arbitragelab.time_series_approach.quantile_time_series import QuantileTimeSeriesTradingStrategy
from arbitragelab.time_series_approach.ou_optimal_threshold_bertram import OUModelOptimalThresholdBertram
from arbitragelab.time_series_approach.ou_optimal_threshold_zeng import OUModelOptimalThresholdZeng
