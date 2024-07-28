from constants import w3
import csv
import pandas as pd
from web3 import Web3
import sqlite3


def get_address(block_num):
    for j in range(6294000, block_num):
        print(j)
        try:
            trx = w3.eth.get_block(j, full_transactions=True)
            for i in trx.transactions:
                from_is_smart_contract = checker_smart_contract(i['from'])
                to_is_smart_contract = checker_smart_contract(i['to'])

                if not from_is_smart_contract:
                    address_wallet.add(i['from'])
                else:
                    smart_contracts.add(i['from'])

                if not to_is_smart_contract:
                    address_wallet.add(i['to'])
                else:
                    smart_contracts.add(i['to'])

        except Exception:
            with open('last_block.txt', 'w') as file:
                file.write(str(j))
            with open('address_wallet.csv', mode='w', newline='') as file:
                writer = csv.writer(file)
                for item in address_wallet:
                    writer.writerow([item])
            with open('smart_contract.csv', mode='w', newline='') as file:
                writer = csv.writer(file)
                for item in smart_contracts:
                    writer.writerow([item])
            break


def checker_smart_contract(address):
    check = w3.eth.get_code(w3.to_checksum_address(address))
    if check == b'':
        return False
    else:
        return True


def insert_address(addr: str):
    cursor.execute(create_table_query)
    sqlite_insert_query = """INSERT INTO test (Address) VALUES (?)"""
    cursor.execute(sqlite_insert_query, (addr,))
    conn.commit()


def get_balance(address, block_num):
    balance = w3.eth.get_balance(Web3.to_checksum_address(address), block_num)
    # df = pd.DataFrame({'': [address], block_num: [balance]})
    return balance, address


def insert_balance(address, balance, clmn):
    update_query = f'''
    UPDATE test
    SET "{clmn}" = ?
    WHERE address = ?
    '''
    cursor.execute(update_query, (balance, address))
    conn.commit()


if __name__ == '__main__':
    address_wallet = set()
    smart_contracts = set()

    conn = sqlite3.connect('Blockchain.db')
    cursor = conn.cursor()
    columns = ["Address TEXT"] + [f'"{i}" TEXT' for i in range(101)]
    create_table_query = f'''
        CREATE TABLE IF NOT EXISTS test (
            {", ".join(columns)}
        )
        '''

    # SAVE IN DATABASE
    # df = pd.read_csv('address_wallet.csv')
    # for index, row in df.iterrows():
    #     insert_address(row[0])
    # READ ADDRESS FROM DATABASE
    query = "SELECT Address FROM test"
    cursor.execute(query)

    results = cursor.fetchall()
    for row in results:
        balance, addr = get_balance(row[0], 0)
        insert_balance(balance=balance, address=addr, clmn=1)