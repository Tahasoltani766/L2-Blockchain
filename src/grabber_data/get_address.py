import threading
import numpy as np
from constants import w3
import sqlite3
from web3 import HTTPProvider, Web3


def get_logs(block_hash):
    logs_block = w3.eth.get_logs({'blockHash': block_hash})
    for log in logs_block:
        trx = w3.eth.get_transaction_receipt(log['transactionHash'])
        trx_hash = trx['transactionHash']
        get_action(trx_hash)


def get_action(trx_hash):
    client = HTTPProvider('https://weathered-blue-leaf.quiknode.pro/36c5cef698f43f466f5874392ae470f3d6eac6a5/')
    result = client.make_request("trace_transaction", [trx_hash])
    data = result['result']
    for item in data['action']:
        checker_smart_contract(item['from'])
        checker_smart_contract(item['to'])


def get_address(block_num):
    try:
        print(block_num)
        trx = w3.eth.get_block(block_num, full_transactions=True)
        get_logs(trx.transactions[0]['blockHash'].hex())
        for i in trx.transactions:
            checker_smart_contract(i['from'])
            checker_smart_contract(i['to'])
    except Exception:
        with open('last_block.txt', 'w') as file:
            file.write(str(block_num))


def checker_smart_contract(address):
    check = w3.eth.get_code(w3.to_checksum_address(address))
    if check == b'':
        return address_wallet.add(address)
    else:
        smart_contracts.add(address)


def insert_address(addr: str):
    cursor.execute(create_table_query)
    sqlite_insert_query = """INSERT INTO test (Address) VALUES (?)"""
    cursor.execute(sqlite_insert_query, (addr,))
    conn.commit()


def get_balance(address, block_num):
    balance = w3.eth.get_balance(Web3.to_checksum_address(address), block_num)
    array = np.array([
        (address, balance,block_num),
    ],dtype=dtype)
    np.vstack([result_array, array])
    # insert_balance(balance=balance, address=address, clmn=block_num)


def insert_balance(address, balance, clmn):
    update_query = f'''
    UPDATE test
    SET "{clmn}" = ?
    WHERE address = ?
    '''
    cursor.execute(update_query, (balance, address))
    conn.commit()


def main_get_address():
    list_th_get_address = []
    max_threads = 20
    for j in range(1378939, 1379019, max_threads):
        print(j)
        for i in range(max_threads):
            mythread = threading.Thread(target=get_address, args=(j + i,))
            list_th_get_address.append(mythread)

        for i in list_th_get_address:
            i.start()
        for i in list_th_get_address:
            i.join()
        list_th_get_address = []


def save_address_to_database():
    for j in address_wallet:
        insert_address(j)
    print('INSERT IS TRUE!')


def main_get_balances():
    list_th_get_address = []
    query = "SELECT Address FROM test"
    cursor.execute(query)
    results = cursor.fetchall()
    max_threads = 5
    for j in range(0, 5, max_threads):
        for i in range(max_threads):
            for row in results:
                mythread = threading.Thread(target=get_balance, args=(row[0], j + i))
                list_th_get_address.append(mythread)
        for i in list_th_get_address:
            i.start()
        for i in list_th_get_address:
            i.join()
        list_th_get_address = []

    print('BALANCE IS TRUE!')


if __name__ == '__main__':
    dtype = [('address', 'U50'), ('balance', 'f4'), ('blocknum', 'i4')]
    result_array = np.empty((0,),dtype=dtype)


    address_wallet = set()
    smart_contracts = set()
    # main_get_address()

    conn = sqlite3.connect('Blockchain.db')
    cursor = conn.cursor()
    columns = ["Address TEXT"] + [f'"{i}" TEXT' for i in range(101)]
    create_table_query = f'''
        CREATE TABLE IF NOT EXISTS test (
            {", ".join(columns)}
        )
        '''
    # save_address_to_database()
    main_get_balances()
    print(result_array)
    # READ ADDRESS FROM DATABASE
