from pymongo import MongoClient

from priceana import YahooPrices
from priceana.utils.DataBroker import DataBrokerMongoDb


def main():
    client = MongoClient("mongodb://localhost:27017")
    dbm = DataBrokerMongoDb(client)

    yp = YahooPrices("APC.F", dbm, interval="1d")
    yp.download()

    print(yp.data)


if __name__ == "__main__":
    main()
