from constants import w3
from web3 import Web3
import csv
import pandas as pd


def get_balance(address, block_num):
    balance = w3.eth.get_balance(Web3.to_checksum_address(address), block_num)
    df = pd.DataFrame({'': [address], block_num: [balance]})
    return df


if __name__ == '__main__':
    list_dataframes = []
    with open('address_wallet.csv', 'r') as csvfile:
        for i in range(6368170, 6368174):
            csv_reader = csv.reader(csvfile)
            for row in csv_reader:
                df = get_balance(address=row[0], block_num=i)
                list_dataframes.append(df)
    combined_df = pd.concat(list_dataframes, ignore_index=True)
    print(combined_df)