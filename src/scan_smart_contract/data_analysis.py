import pandas as pd
import matplotlib.pyplot as plt
# Create the DataFrame data
data = {
    'Address': ['0x77877', '0x99966'],
    '3/2': [2, 3],
    '3/3': [4, 6],
    '3/4': [8, 9],
    '3/5': [1, 7],
    '3/6': [7, 7],
    '3/7': [9, 1]
}

# Convert dictionary to DataFrame


class DataAnalysis:
    def __init__(self):
        self.df = pd.DataFrame(data)

    def calculate_ema_avg(self):
        for index, row in self.df.iterrows():
            prices = row[1:]
            ema = prices.ewm(span=3).mean()
            avg = sum(prices) / len(prices)
            self.df.at[index, 'AVG'] = avg
            self.df.at[index, 'EMA'] = ema.iloc[-1]

        return self.df

if __name__ == '__main__':
    da = DataAnalysis()
    print(da.calculate_ema_avg())