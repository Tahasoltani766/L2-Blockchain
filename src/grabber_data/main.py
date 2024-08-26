import asyncio
import multiprocessing
from multiprocessing import freeze_support
import threading
import time
import numpy as np
from src.grabber_data.constants import w3, client, cursor, create_table_query, conn
from web3 import HTTPProvider, Web3

input_queue = multiprocessing.Queue()
output_queue = multiprocessing.Queue()


class GrabberData:
    def __init__(self):
        self.max_threads = 5
        self.address_wallet = set()
        self.smart_contract = set()
        self.result_array = np.array([])
        self.list_th_get_balance = []
        query = "SELECT Address FROM eth_balance"
        cursor.execute(query)
        self.list_address = cursor.fetchall()
        self.block_now = w3.eth.block_number()

    def main_get_address(self):
        list_th_get_address = []
        for j in range(0, self.block_now, self.max_threads):
            for i in range(self.max_threads):
                my_threads = threading.Thread(target=self.get_address, args=(i + j,))
                print(i + j)
                list_th_get_address.append(my_threads)
            for i in list_th_get_address:
                i.start()
            for i in list_th_get_address:
                i.join()
            list_th_get_address = []
            self.save_address_to_database()
            self.address_wallet = ()

    def save_address_to_database(self):
        for j in self.address_wallet:
            self.insert_address(j)

    def insert_address(self, addr: str):
        cursor.execute(create_table_query)
        sqlite_insert_query = """INSERT INTO eth_balance (Address) VALUES (%s)"""
        cursor.execute(sqlite_insert_query, (addr,))
        conn.commit()
        print('INSER ADDRESS :', addr)

    def get_address(self, block_number):
        try:
            print(f'START BLOCKNUMBER-{block_number}')
            trx = w3.eth.get_block(block_number, full_transactions=True)
            for i in trx.transactions:
                self.checker_address_type(i['from'])
                self.checker_address_type(i['to'])
            self.get_logs(trx.transactions[0]['blockHash'].hex())
            print(f'FINISH BLOCKNUMBER-{block_number}')

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
        for j in range(0, self.block_now, self.max_threads):
            for i in range(self.max_threads):
                my_thread = threading.Thread(target=self.get_balance, args=(j + i,))
                print(j + i)
                self.list_th_get_balance.append(my_thread)
            for i in self.list_th_get_balance:
                i.start()
            for i in self.list_th_get_balance:
                i.join()
            self.list_th_get_balance = []
            self.handler_balance()
            self.result_array = np.array([])

    def handler_balance(self):
        for item in self.result_array:
            self.insert_balance(item[0], str(item[1]), item[2])
        print('INSERT IS TRUE')

    def insert_balance(self, address, balance, block_num):
        update_query = f'''
        UPDATE eth_balance
        SET `{block_num}` = %s
        WHERE Address = %s
        '''
        cursor.execute(update_query, (balance, address))
        conn.commit()

    def get_balance(self, block_num):
        for row in self.list_address:
            while 1:
                try:
                    balance = w3.eth.get_balance(Web3.to_checksum_address(row[0]), block_num)
                    array = np.array([(row[0], int(balance), int(block_num)), ])
                    print(array)
                    if self.result_array.size == 0:
                        self.result_array = array
                    else:
                        self.result_array = np.vstack((self.result_array, array))
                    break
                except Exception as e:
                    pass

        conn.commit()

    def clean_null_data(self):
        cursor.execute("SELECT * FROM eth_balance")
        rows = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]  # Use index 0 for column names

        colcount = len(column_names)

        for row in rows:
            updated_row = dict()
            for i in range(colcount):
                if row[i] is None:
                    j = i + 1
                    while j < colcount:
                        if row[j] is not None:
                            updated_row[column_names[i]] = row[j]
                            break
                        j += 1
                    else:
                        updated_row[column_names[i]] = 0

            if updated_row:  # Update only if there are changes
                update_query = f"""
                               UPDATE eth_balance
                               SET {', '.join(f'`{col}` = %s' for col in updated_row)}
                               WHERE id = %s
                               """
                cursor.execute(update_query,
                               (*updated_row.values(), row[0]))  # Assuming 'id' is the first column in 'row'
                conn.commit()
                print('Finish Clean data')


class RealTime:
    def __init__(self):
        pass
    def get_logs(self, block_hash):
        txs = w3.eth.get_block(block_hash, True)
        for i in txs["transactions"]:
            trx_reciept = client.make_request("trace_transaction", [i['hash']])
            for item in trx_reciept['result']:
                try:
                    block_num = item['blockNumber']
                    action = item['action']
                    if int(action['value'], 16) == 0:
                        break
                    else:
                        balance = w3.eth.get_balance(Web3.to_checksum_address(action['from']), block_num)
                        if balance != 0:
                            input_queue.put({'Address': action['from'], 'Value': balance, 'BlockNumber': block_num})
                except Exception as e:
                    print(e)

    async def log_loop(self, event_filter, poll_interval, pool):
        while True:
            new_events = event_filter.get_new_entries()
            if new_events:
                for event in new_events:
                    pool.apply_async(self.get_logs, args=(event,))
            await asyncio.sleep(poll_interval)

def main_real_time():
    real_time = RealTime()
    tx_filter = w3.eth.filter('latest')
    max_processes = multiprocessing.cpu_count() - 4
    with multiprocessing.Pool(processes=max_processes) as pool:
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(
                asyncio.gather(
                    real_time.log_loop(tx_filter, 2, pool)
                )
            )
        finally:
            loop.close()


def main_grabber_data():
    grabber_data = GrabberData()
    grabber_data.main_get_address()
    time.sleep(5)
    grabber_data.main_get_balances()
    grabber_data.clean_null_data()
    print('Finish - grabber - data')

if __name__ == '__main__':
    freeze_support()
    # main_grabber_data()
    main_real_time()

# main_eth()