import pandas as pd
import numpy as np
import warnings
import datetime as dt
import os.path

class IndexModel:

    def __init__(self) -> None:
        self.final_index = 0

        # Here I store all constants
        self.weight_1 = 0.5
        self.weight_2 = 0.25
        self.weight_3 = 0.25
        self.base_index = 100


    def calc_index_level(self, start_date: dt.date, end_date: dt.date) -> None:

        warnings.filterwarnings(action="ignore")
        #warning was generated that might be relevant for later resolving,
        # but it does not impact results.

        # import the stock data
        df = pd.read_csv(os.path.join("data_sources","stock_prices.csv"), sep=",")

        df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")

        # calculate the rebalancing dates

        rebalancing_dates = pd.DataFrame(df["Date"])

        rebalancing_dates.insert(1, "Month", rebalancing_dates["Date"].dt.month)
        rebalancing_dates.insert(1, "Rebalance", np.nan)

        lagged_months = rebalancing_dates["Month"].iloc[1:].reset_index(drop=True)

        rebalancing_dates["Rebalance"] = rebalancing_dates["Month"].iloc[:-
        1] != lagged_months

        rebalancing_EOM = rebalancing_dates["Date"].loc[rebalancing_dates["Rebalance"] == True]

        #based on rebalancing dates, rank the top 3 stocks in a table.

        ranking_frame = pd.DataFrame(columns=["Date", "#1", "#2", "#3"])

        for x in rebalancing_EOM.index.values:
            balancing_date = rebalancing_EOM.loc[x]

            stocks_oneday = df.iloc[x][1:]

            order = stocks_oneday.sort_values()[::-1]
            winners1 = order.head(3).index.values
            winners2 = np.append(balancing_date, winners1)
            ranking_frame.loc[len(ranking_frame)] = winners2
        # convert the ranking frame into a weights frame with the weights defined in __init__

        weighting_frame = pd.DataFrame(data=rebalancing_EOM, columns=df.columns)
        weighting_frame = weighting_frame.fillna(0)

        for x in rebalancing_EOM:

            for y in weighting_frame.iloc[:, 1:].columns:

                if y == ranking_frame["#1"].loc[ranking_frame["Date"] == x].values:

                    weighting_frame[y].loc[weighting_frame["Date"] == x] = self.weight_1

                elif y == ranking_frame["#2"].loc[ranking_frame["Date"] == x].values:

                    weighting_frame[y].loc[weighting_frame["Date"] == x] = self.weight_2

                elif y == ranking_frame["#3"].loc[ranking_frame["Date"] == x].values:

                    weighting_frame[y].loc[weighting_frame["Date"] == x] = self.weight_3

        weighting_frame["Date"] = df["Date"].iloc[weighting_frame.index.values + 1].values

        # convert the price indices to return indices for simplifying calculations

        returns = df.copy()
        returns.iloc[:, 1:] = returns.iloc[:, 1:].pct_change()

        #create an intermediary index with a column per stock. This can later be collapsed to the final index
        index = pd.DataFrame(df["Date"], columns=df.columns)

        placeholder = df["Date"].loc[df["Date"] == pd.to_datetime(start_date)].index[
            0]  # retrieve index position where the index should start

        #calculate index values for the first period
        day_weight = weighting_frame.loc[weighting_frame["Date"] == pd.to_datetime(start_date)].reset_index(
            drop=True)  # weight for first date
        day_weight_abs = day_weight.copy()
        day_weight_abs.iloc[:, 1:] = self.base_index * day_weight.iloc[:, 1:]

        first_index = (day_weight_abs.iloc[:, 1:])

        index.iloc[placeholder, 1:] = first_index.values[0]

        # using a loop, calculate the index values for all subsequent periods

        for x in range((placeholder + 1), len(index)):

            day_return = returns.loc[returns["Date"] == index.iloc[x, 0]].reset_index(drop=True)
            day_index = (1 + day_return.iloc[:, 1:]) * index.iloc[(x - 1), 1:]

            if (x > (placeholder + 1) and (x - 2) in weighting_frame.index.values):

                rebalancer = weighting_frame.loc[(x - 2), weighting_frame.columns != 'Date']
                multiplier = index.iloc[(x - 1), 1:].sum()

                day_index = ((1 + day_return.iloc[:, 1:]) * rebalancer * multiplier)
                index.iloc[x, 1:] = day_index.values[0]
            else:
                index.iloc[x, 1:] = day_index.values[0]

        #collapse the intermediary index to create the final index

        final_index = pd.DataFrame(df["Date"], columns=["Date", "final_index"])
        final_index["final_index"] = index.iloc[:, 1:].sum(axis=1)

        final_index = final_index.loc[final_index["Date"] >= pd.to_datetime(start_date), :]
        final_index = final_index.loc[final_index["Date"] <= pd.to_datetime(end_date), :]

        #store the index so the function below can export it
        self.final_index = final_index

    def export_values(self, file_name: str) -> None:

        self.final_index.to_csv(os.path.join(file_name), index=False)