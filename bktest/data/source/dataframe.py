import numpy
import pandas

from .base import DataSource
from ... import constants


class DataFrameDataSource(DataSource):

    def __init__(
        self,
        dataframe: pandas.DataFrame,
        date_column=constants.DEFAULT_DATE_COLUMN,
        symbol_column=constants.DEFAULT_SYMBOL_COLUMN,
        price_column=constants.DEFAULT_PRICE_COLUMN,
        execution_price_column=None,
        closeable=True,
        order_dataframe=None,
    ) -> None:
        super().__init__()

        dataframe = dataframe.drop_duplicates(
            subset=[symbol_column, date_column],
            keep="first"
        )

        # TODO: Prefiltre avant
        if order_dataframe is not None:
            filter_assets = set(dataframe[symbol_column].unique())
            min_date = order_dataframe[date_column].min()
            dataframe = dataframe[(dataframe[date_column] >= min_date) & (dataframe[symbol_column].isin(filter_assets))].copy()

        self.dataframe = dataframe.pivot(
            index=date_column,
            columns=symbol_column,
            values=price_column
        )
        self.dataframe.index = pandas.to_datetime(self.dataframe.index)
        self.dataframe.index.name = constants.DEFAULT_DATE_COLUMN

        if execution_price_column is not None:
            self.execution_dataframe = dataframe.pivot(
                index=date_column,
                columns=symbol_column,
                values=execution_price_column
            )
            self.execution_dataframe.index = pandas.to_datetime(self.execution_dataframe.index)
            self.execution_dataframe.index.name = constants.DEFAULT_DATE_COLUMN

            self.has_execution_prices = True
        else:
            # Fallback to using the same prices for execution
            self.execution_dataframe = self.dataframe
            self.has_execution_prices = False

        self.closeable = closeable

    def fetch_prices(self, symbols, start, end):
        """Fetch prices for portfolio valuation and return calculation"""
        return self._fetch_from_dataframe(self.dataframe, symbols, start, end)

    def fetch_execution_prices(self, symbols, start, end):
        """Fetch prices for order execution (e.g., open prices)"""
        return self._fetch_from_dataframe(self.execution_dataframe, symbols, start, end)

    def _fetch_from_dataframe(self, dataframe, symbols, start, end):
        """Helper method to fetch prices from a specific dataframe"""
        symbols = set(symbols)

        missings = symbols - set(dataframe.columns)
        founds = symbols - missings

        prices = None
        if len(founds):
            start = pandas.to_datetime(start)
            end = pandas.to_datetime(end)

            prices = dataframe[
                (dataframe.index >= start) &
                (dataframe.index <= end)
            ][list(founds)].copy()
        else:
            prices = pandas.DataFrame(
                index=pandas.DatetimeIndex(
                    data=pandas.date_range(start=start, end=end),
                    name=constants.DEFAULT_DATE_COLUMN
                )
            )

        prices[list(missings)] = numpy.nan

        return prices

    def is_closeable(self):
        return self.closeable