from datetime import datetime
from get_stock_data import StockData


class EqualWeightPortfolio(object):

    def __init__(self, start, end, stock_file, invested_cap):
        self.data_object = StockData(start=start, end=end, stock_file=stock_file)

        # Pandas Dataframe of historical data:
        self.historical_data = self.data_object.historical_data

        # $'s invested in portfolio
        self.invested_cap = invested_cap

        # List of tickers which received a data response from Yahoo Finance
        self.good_tickers = self.data_object.good_tickers

        # Determine rebalance dates
        self.get_rebalance_dates()

        # Get total returns
        self.get_return(return_type='total')

        # Get price returns
        self.get_return(return_type='price')

    def get_return(self, return_type):
        """'return_type' should be either 'price' or 'total'"""

        # Initially, 'capital' = 'invested_cap', then at each rebalance, 'capital' = prior 'Portfolio_Value'
        capital = self.invested_cap

        # Create a copy of 'self.historical_data' so that original is not changed
        data = self.historical_data

        # If 'return_type' = 'price', deduct dividends
        if return_type == 'price':
            data = self.less_divs(data)

            # Loop through each 'rebalance_period'
        t = 0
        while t < len(self.rebalance_dates) - 1:

            # Get list of stocks that traded as of beginning of period 't'
            stocks_with_trades = self.stocks_with_trades(t)

            # Fill Number of Shares column ONLY for stocks that traded during rebalance period 't'
            for key, value in stocks_with_trades.items():
                num_shares = ((capital / len(stocks_with_trades)) / stocks_with_trades.get(key))

                data.loc[data['rebalance_period'] == t,
                                         '{}_Shares'.format(key)] = num_shares

                # Value of position in $
                pos_value = data['{}_Shares'.format(key)] * data['{}_Adj_Close'.format(key)]
                data.loc[data['rebalance_period'] == t,
                                         '{}_Position_Value'.format(key)] = pos_value

            # Add 'Portfolio_Value' column
            data.loc[data['rebalance_period'] == t,
                                     'Portfolio_Value'] = data.filter(regex='_Position_Value$').sum(axis=1)

            # Set 'capital' = prior 'Portfolio_Value' for rebalancing
            capital = data.loc[data['rebalance_period'] == t, 'Portfolio_Value'].iat[-1]

            # Increment rebalance period 't'
            t += 1

        # Save summary with 'Portfolio_Value' to .csv file
        data['Portfolio_Value'].to_csv('{}_summary_output.csv'.format(return_type))

        # Save all data to .csv file
        data.to_csv('{}_return_output.csv'.format(return_type))

    def less_divs(self, data):
        """for 'price' returns, we're not re-investing dividends"""
        for stock in self.good_tickers:
            try:
                data['{}_divs_total'.format(stock)] = data['{}_action_amount'.format(stock)][::-1].fillna(0).cumsum()[::-1]
                data['{}_Adj_Close'.format(stock)] = data['{}_Adj_Close'.format(stock)] + data['{}_divs_total'.format(stock)]

            except KeyError:
                # If 'action' column is not found for a stock, then it didn't have a dividend
                print('No dividends for {}'.format(stock))

        return data


    def stocks_with_trades(self, rebalance_period):
        """Get list of stocks that traded on a given day, and their closing price on day 1 of the period.
        Prevents allocating capital to stocks that did not trade yet"""

        traded_stocks = {}

        period = self.historical_data[self.historical_data['rebalance_period'] == rebalance_period]

        # Create a dict with tickers and their Close prices on 1st day of period 'rebalance_period'
        for stock in self.good_tickers:
            if period.iloc[0]['{}_Adj_Close'.format(stock)] > 0:
                traded_stocks.update({stock: period.iloc[0]['{}_Adj_Close'.format(stock)]})

        return traded_stocks

    def get_rebalance_dates(self):
        """Rebalance date are the last trading day of each year,
        12-31 or closest prior trading day is 12-31 is a holiday / weekend. Also, the period start date is at
        the beginning because the portfolio is balanced for the first time then"""

        self.rebalance_dates = [self.historical_data.index.values[0], ]

        # Make a list of dates on which to rebalance, dates must be within historical_data index
        for year in range(start.year, end.year + 1):
            date = self.historical_data.loc[self.historical_data.index <= datetime(year, 12, 31)].tail(1).index.values[0]

            # Add dates to list
            self.rebalance_dates.append(date)

        # Add a column 'rebalance_date', set value to True for each row where index = rebalance date
        for d in self.rebalance_dates:
            self.historical_data.set_value(d, 'rebalance_date', True)

        # Add column 'rebalance_period', to separate entire time frame into rebalance periods
        t = 0
        while t < len(self.rebalance_dates) - 1:
            self.historical_data.loc[(self.historical_data.index >= self.rebalance_dates[t]) &
                                  (self.historical_data.index <= self.rebalance_dates[t + 1]), 'rebalance_period'] = t
            t += 1


if __name__ == '__main__':

    # File with list of stocks
    stock_file = 'djt_components.csv'
    # stock_file = 'test_tickers.csv'

    # Set the Start and End date ranges, invested capital
    start = datetime(2007, 1, 1)
    end = datetime(2016, 12, 31)
    invested_cap = 10000

    # Create new portfolio
    p = EqualWeightPortfolio(start=start,
                  end=end,
                  stock_file=stock_file,
                  invested_cap=invested_cap)

