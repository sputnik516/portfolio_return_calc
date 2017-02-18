import pandas_datareader.data as web
import pandas_datareader._utils as pdr_utils        # For error handling
import pandas as pd

def get_stock_list(stock_file):
    """Open .csv file containing list of stocks"""

    # Note, the list contains a ticker 'FAKESTOCK' to demonstrate hnadling of error tickers
    stock_list = pd.read_csv(stock_file)

    return stock_list


def get_data(stock_file, start, end):
    """Get historical stock data between 'start' and 'end' dates"""

    # Get list of stocks
    stock_list = get_stock_list(stock_file)

    # Blank dataframe to hold data for each stock
    data = pd.DataFrame()

    # Loop through stock_list, get historical data for each stock
    for index, stock in stock_list.iterrows():

        # Try getting historical data for each stock. If error, warn user, and continue on to next stock
        try:
            # Get Open, High, Low, Close, Volume, Adj Close for 'stock' from Yahoo Finance
            # Set 'index' to 'Date' to be consistent when joining with other dataframes
            ohlcv = web.DataReader(stock.Ticker, 'yahoo', start, end)
            ohlcv.index.names = ['Date']

            # Get dividend amount, date from Yahoo Finance
            # Set 'index' to 'Date' to be consistent when joining with other dataframes
            divs = web.DataReader(stock.Ticker, 'yahoo-actions', start, end)
            divs.index.names = ['Date']

            # Merge Open, High, Low, Close, Volume, Adj Close with dividend data
            ohlcv_divs = ohlcv.join(divs)

            # Change column names, add stock ticker to each value
            ohlcv_divs.rename(columns={'Open': stock.Ticker + '_Open',
                                       'High': stock.Ticker + '_High',
                                       'Low': stock.Ticker + '_Low',
                                       'Close': stock.Ticker + '_Close',
                                       'Volume': stock.Ticker + '_Volume',
                                       'Adj Close': stock.Ticker + '_Adj_Close',    # Note slightly different format/spelling
                                       'action': stock.Ticker + '_action',          # Note slightly different format/spelling
                                       'value': stock.Ticker + '_action_amount'     # Note slightly different format/spelling
                                       }, inplace=True)



            # # Add data for each stock to master dataframe
            data = pd.concat([data, ohlcv_divs], axis=1)

        except pdr_utils.RemoteDataError:
            print('Caught RemoteDataError on ticker ' + stock.Ticker + '. Make sure it\'s a real stock.')

    return data


