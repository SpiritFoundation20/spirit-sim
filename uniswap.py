import requests
import pandas as pd
import ccxt
binance_exchange = ccxt.binance
import datetime as dt
class Uniswap:
    """
    A powerful access class to the world of defi tokens.
    """
    def __init__(self):
        """
        Initialize Uniswap with our additional metadata.
        """
        self.sector = 'DeFi'
        self.broker = 'Uniswap'
        self.cc_key = "f4c265730044e74d20660c034279083ef9bac9fe9eaebedc9b866338095eb680"
        self.base_url = "https://min-api.cryptocompare.com/data/v2/pair/mapping/exchange"
        self.url = f"{self.base_url}?api_key={self.cc_key}"

    def get_pair_mapping(self, exchange: str = 'ALL'):
        """
        :param:
        :return:
        """
        url = self.url
        if exchange != 'ALL':
            url = self.url + f"&e={exchange}"
        data = requests.get(url)
        pair_mapping = pd.DataFrame(data.json()['Data']['current'])
        return pair_mapping

    def get_top_volume(self,
                       quote: str = 'ETH',
                       minimum_volume: int = 1e4) -> pd.DataFrame:
        """
        :param:
        :return:
        """
        pair_mapping = self.get_pair_mapping('Uniswap')
        pair_mapping[
            'Symbol'] = pair_mapping['fsym'] + '/' + pair_mapping['tsym']
        return pair_mapping[['Symbol']].set_index('Symbol')

    def get_ohlcv(self, symbol, frequency, periods, exchange='Uniswap'):
        """
        This is the crypto compare version of our get_ohlcv interface.
        This opens up a world of data access, similar to ccxt.
        :param:
        :return:
        """
        assert (frequency in ('1d', '1h'))
        fsym, tsym = symbol.split('/')
        limit = 2000

        def get_historical(fsym,
                           tsym,
                           exchange='ALL',
                           frequency='1d',
                           toTs=None,
                           limit=100):
            key = "f4c265730044e74d20660c034279083ef9bac9fe9eaebedc9b866338095eb680"
            if frequency == '1h':
                base_url = "https://min-api.cryptocompare.com/data/v2/histohour"
            elif frequency == '1d':
                base_url = "https://min-api.cryptocompare.com/data/v2/histoday"
            else:
                print(f"Unsupported frequency: {frequency}")
                raise Exception
            url = f"{base_url}?api_key={key}&fsym={fsym}&tsym={tsym}"
            if exchange != 'ALL':
                url += f"&e={exchange}"
            if toTs:
                url += f"&toTs={toTs}"
            if limit:
                url += f"&limit={limit}"
            data = requests.get(url)
            # print(url)
            historical = pd.DataFrame(
                data.json()['Data']['Data']).set_index('time')[[
                    'open', 'high', 'low', 'close', 'volumeto'
                ]]
            historical.index = pd.to_datetime(
                [dt.datetime.fromtimestamp(x) for x in historical.index])
            historical.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
            return historical

        # Gives the chunk size in ms
        chunk_size = limit * binance_exchange.parse_timeframe(frequency) * 999

        # Gives the current time in iso8601 format
        fetch_since = binance_exchange.milliseconds()

        # Fetch data by chunks
        ohlcv = []
        for _ in range(periods // limit):
            fetch_since = fetch_since - chunk_size
            data = get_historical(fsym,
                                  tsym,
                                  exchange,
                                  frequency=frequency,
                                  toTs=(fetch_since + chunk_size) // 1000,
                                  limit=min(limit, periods))
            ohlcv.insert(0, data)

        # Fetch the remainder
        remainder = periods % limit
        fetch_since = fetch_since - remainder * binance_exchange.parse_timeframe(
            frequency) * 1000
        data = get_historical(fsym,
                              tsym,
                              exchange,
                              frequency=frequency,
                              toTs=((fetch_since + chunk_size) // 1000),
                              limit=remainder)

        ohlcv.insert(0, data)
        ohlcv = pd.concat(ohlcv)
        ohlcv.index.name = 'date'

        return ohlcv



if __name__ == "__main__":
    u = Uniswap()
    print(u.get_pair_mapping(exchange='Uniswap'))
