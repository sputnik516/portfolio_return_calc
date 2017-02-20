import pandas_datareader.data as web
import pandas_datareader._utils as pdr_utils        # For error handling
import pandas as pd
import datetime as dt

class StockData(object):

    """
    Retrieves historical stock data for a list of stocks, between Start / End dates
    self.data is a Pandas Dataframe object containing historical data from Yahoo Finance, such as
    Open, High, Low, Close, Volume, Adj Close for each stock, in the format:
    ABC_Open, ABC_Close, etc.
    """

    def __init__(self, start, end, stock_file):

        # Check if start, end are datetitme objects
        if isinstance(start, dt.datetime):
            self.start = start
        else:
            raise Exception('start must be a datetime object. Received type {}'.format(type(start)))

        if isinstance(end, dt.datetime):
            self.end = end
        else:
            raise Exception('end must be a datetime object. Received type {}'.format(type(end)))

        # Note, the sample list contains a ticker 'FAKESTOCK' to demonstrate handling of error tickers
        try:
            self.stock_list = pd.read_csv(stock_file)
        except FileNotFoundError:
            print('File {} can\'t be read. Make sure it exists.'.format(stock_file))
            exit()

        # Tickers that for which we get a successful response
        self.good_tickers = []

        # Gets stock combined data, in DataFrame format
        self.historical_data = self.get_data()

    def get_data(self):
        """Get historical stock data between 'start' and 'end' dates"""

        # Blank dataframe to hold data for each stock
        data = pd.DataFrame()

        # Loop through stock_list, get historical data for each stock
        for index, stock in self.stock_list.iterrows():

            # Try getting historical data for each stock. If error, warn user, and continue on to next stock
            try:
                # Get Open, High, Low, Close, Volume, Adj Close for 'stock' from Yahoo Finance
                # Set 'index' to 'Date' to be consistent when joining with other dataframes
                ohlcv = web.DataReader(stock.Ticker, 'yahoo', self.start, self.end)
                ohlcv.index.names = ['Date']

                # Get dividend amount, date from Yahoo Finance
                # Set 'index' to 'Date' to be consistent when joining with other dataframes
                # Typical responses: DIVIDEND
                divs = web.DataReader(stock.Ticker, 'yahoo-actions', self.start, self.end)
                divs.index.names = ['Date']

                # Merge Open, High, Low, Close, Volume, Adj Close with dividend data
                ohlcv_divs = ohlcv.join(divs)

                # Change column names, add stock ticker to each value
                ohlcv_divs.rename(columns={'Open': stock.Ticker + '_Open',
                                           'High': stock.Ticker + '_High',
                                           'Low': stock.Ticker + '_Low',
                                           'Close': stock.Ticker + '_Close',
                                           'Volume': stock.Ticker + '_Volume',
                                           'Adj Close': stock.Ticker + '_Adj_Close',
                                           # Note slightly different format/spelling
                                           'action': stock.Ticker + '_action',
                                           # Note slightly different format/spelling
                                           'value': stock.Ticker + '_action_amount'
                                           # Note slightly different format/spelling
                                           }, inplace=True)

                # Add data for each stock to master dataframe
                data = pd.concat([data, ohlcv_divs], axis=1)

                # Add ticker to good_tickers
                self.good_tickers.append(stock.Ticker)

            except pdr_utils.RemoteDataError:
                print('Caught RemoteDataError on ticker {}. Make sure it\'s a real stock.'.format(stock.Ticker))

        print('Finished getting stock data for tickers: {}'.format(self.good_tickers))

        # Remove lines with duplicate index values
        data = data[~data.index.duplicated(keep='first')]

        # data.to_csv('get_data_test.csv')

        return data

