import alpaca.trading
import requests
import alpaca
from alpaca.trading.client import TradingClient

# TODO: add more general functionality here for fall 2024 quant projects
# TODO: add erro handling

class AlpacaFramework:
    # rly basic alpaca framework for market/limit orders and paper trading

    def __init__(self, api_key, api_secret, base_url="https://paper-api.alpaca.markets/v2", paper=True):
        self.base_url = base_url # default is paper trading

        self.api_key = api_key
        self.api_secret = api_secret

        client = TradingClient(api_key, api_secret, paper=paper)
        self.client = client
        

    def place_market_order(self, ticker, qty, side):
        """place normal market order, wouldnt reccomend depending on use case
        """
        order = alpaca.trading.requests.MarketOrderRequest(
            symbol=ticker,
            qty=qty,
            side=side,
            type='market',
            time_in_force='gtc'
            # time_in_force ==> for when it expires
        )
        result = self.client.submit_order(order)
        return result
    

    def place_limit_order(self, ticker, qty, side, limit_price):
        """place limit order, makes more sense
        """
        order = alpaca.trading.requests.LimitOrderRequest(
            symbol=ticker,
            qty=qty,
            side=side,
            type="limit",
            limit_price=limit_price,
            time_in_force='gtc'
            # time_in_force ==> for when it expires
        )
        result = self.client.submit_order(order)
        return result
    
    # TODO: stop order? idk if needed

    def cancel_all_orders(self):
        """cancel all orders
        """
        return self.client.cancel_orders()
    
    def cancel_order(self, order_id):
        """cancel a specific order
        """
        return self.client.cancel_order_by_id(order_id)
    
    def get_orders(self):
        """get all orders
        """
        return self.client.get_orders()

    def get_account(self):
        """get account info
        """
        return self.client.get_account()
    