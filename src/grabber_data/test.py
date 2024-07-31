import threading
import numpy as np
from constants import w3, client
import sqlite3
from web3 import HTTPProvider, Web3


class GrabberData:
    def __init__(self):
        self.max_threads = 10
        self.address_wallet = set()
        self.smart_contract = set()
        self.result_array = np.array([])
        self.list_th_get_balance = []
        self.conn = sqlite3.connect('Blockchain.db')
        self.cursor = self.conn.cursor()
        query = "SELECT Address FROM smart_contract" #OR Table: address_wallet
        self.cursor.execute(query)
        self.list_address = self.cursor.fetchall()

    def main_get_address(self):
        list_th_get_address = []
        for j in range(1998000, 1998040, self.max_threads):
            for i in range(self.max_threads):
                print(i + j)
                my_threads = threading.Thread(target=self.get_address, args=(i + j,))
                list_th_get_address.append(my_threads)
            for i in list_th_get_address:
                i.start()
            for i in list_th_get_address:
                i.join()
            list_th_get_address = []
            # self.save_address_to_database()
            self.save_address_to_database(self.smart_contract)
    def save_address_to_database(self,list_address):
        for j in list_address:
            self.insert_smart_contract(j)

    def insert_smart_contract(self, addr: str):
        try:
            sqlite_insert_query = """INSERT INTO smart_contract (Address) VALUES (?)"""
            self.cursor.execute(sqlite_insert_query, (addr,))
            self.conn.commit()
        except sqlite3.IntegrityError:
            pass

    # def save_address_to_database(self):
    #     for j in self.address_wallet:
    #         self.insert_address_wallet(j)

    def insert_address_wallet(self, addr: str):
        try:
            sqlite_insert_query = """INSERT INTO address_wallet (Address) VALUES (?)"""
            self.cursor.execute(sqlite_insert_query, (addr,))
            self.conn.commit()
        except sqlite3.IntegrityError:
            pass

    def get_address(self, block_number):
        try:
            trx = w3.eth.get_block(block_number, full_transactions=True)
            for i in trx.transactions:
                self.checker_address_type(i['from'])
                self.checker_address_type(i['to'])
            self.get_logs(trx.transactions[0]['blockHash'].hex())
            print(self.smart_contract)
        except:
            with open('last_block.txt', 'w') as file:
                file.write(str(block_number))

    def get_logs(self, block_hash):
        logs_block = w3.eth.get_logs({'blockHash': block_hash})
        for log in logs_block:
            trx = w3.eth.get_transaction_receipt(log['transactionHash'])
            result = client.make_request("trace_transaction", trx['transactionHash'])
            data = result['result']
            for item in data['action']:
                self.checker_address_type(item['to'])
                self.checker_address_type(item['from'])

    def checker_address_type(self, address):
        check = w3.eth.get_code(w3.to_checksum_address(address))
        if check == b'':
            self.address_wallet.add(address)
        else:
            self.smart_contract.add(address)

    def main_get_balances(self):
        for j in range(1998000, 1998010, self.max_threads):
            for i in range(self.max_threads):
                my_thread = threading.Thread(target=self.get_balance, args=(j + i,))
                print(j + i)
                self.list_th_get_balance.append(my_thread)
                print(self.list_th_get_balance)
            for i in self.list_th_get_balance:
                i.start()
            for i in self.list_th_get_balance:
                i.join()
            self.list_th_get_balance = []
            self.handler_balance()
            self.result_array = np.array([])

    def handler_balance(self):
        for item in self.result_array:
            self.insert_balance(item[0], item[1], item[2])

    def insert_balance(self, address, balance, block_num):
        balance_str = str(balance)
        # Or Table address_wallet
        update_query = f'''
        UPDATE smart_contract
        SET "{block_num}" = ?
        WHERE address = ?
        '''
        self.cursor.execute(update_query, (balance_str, address))
        self.conn.commit()
        print('INSERT IS TRUE')

    def get_balance(self, block_num):
        for row in self.list_address:
            while True:
                try:
                    balance = w3.eth.get_balance(Web3.to_checksum_address(row[0]), block_num)
                    print(balance)
                    array = np.array([(row[0], int(balance), int(block_num)), ])
                    if self.result_array.size == 0:
                        self.result_array = array
                    else:
                        self.result_array = np.vstack((self.result_array, array))
                        # print(self.result_array)
                    break
                except Exception as e:
                    # print(e)
                    continue


def run_data():
    conn = sqlite3.connect('Blockchain.db')
    cursor = conn.cursor()
    columns = ["Address TEXT PRIMARY KEY"] + [f'"{i}" TEXT' for i in range(1998000, 2000000)]
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS smart_contract (
        {", ".join(columns)}
    )
    '''
    cursor.execute(create_table_query)
    conn.commit()


if __name__ == '__main__':
    run_data()
    grabber_data = GrabberData()
    # grabber_data.main_get_address()
    grabber_data.main_get_balances()
