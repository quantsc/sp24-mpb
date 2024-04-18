from framework import AlpacaFramework

import configparser

def main():
    config = configparser.ConfigParser()
    config.read('.config') # set the path of the config file appropriately

    # create alpaca framework
    print("Creating alpaca freamewor")
    f = AlpacaFramework(config['alpaca']['api_key'], config['alpaca']['api_secret'])

    # print orderbook
    print("Orderbook")
    print(f.get_orders())

    # place a market order
    f.place_market_order("AAPL", 1, "buy")

    # place a limit order
    f.place_limit_order("AAPL", 1, "buy", 100)

    # print orderbook
    print("\nOrderbook")
    print(f.get_orders())

    # cancel all orders
    print("Cancel orders")
    f.cancel_all_orders()

    # print orerbook
    print("\nOrderbook")
    print(f.get_orders())


if __name__ == '__main__':
    main()