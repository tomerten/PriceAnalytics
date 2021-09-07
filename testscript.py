from pymongo import MongoClient

from priceana import YahooPrices
from priceana.tradingstrategies.StatisticalTrading import strategy_mean_reverting
from priceana.utils.DataBroker import DataBrokerMongoDb


def main():
    client = MongoClient("mongodb://localhost:27017")
    dbm = DataBrokerMongoDb(client)

    yp = YahooPrices("APC.F", dbm, interval="1d")
    yp.download()

    print(yp.data)
    strategy_mean_reverting(yp.data[0][1], months=3)


if __name__ == "__main__":
    main()
