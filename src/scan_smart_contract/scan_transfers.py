import asyncio
import multiprocessing
from pprint import pprint
from collections import defaultdict
import mysql.connector
from web3 import Web3
import copy
from multiprocessing import freeze_support

w3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/9b8e587a802a4c5188165b33b1893c55'))

UNISWAP_CONTRACT_ADDRESS = '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D'
WETH_ADDRESS = '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
WETH_ABI = '[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"guy","type":"address"},{"name":"wad","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"wad","type":"uint256"}],"name":"withdraw","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[],"name":"deposit","outputs":[],"payable":true,"stateMutability":"payable","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"},{"name":"","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"payable":true,"stateMutability":"payable","type":"fallback"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":true,"name":"guy","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":true,"name":"dst","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"dst","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Deposit","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Withdrawal","type":"event"}]'
conn = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    database="blockchain"
)


class ScanAddress:
    def __init__(self):
        self.theater_contract = w3.eth.contract(address=WETH_ADDRESS, abi=WETH_ABI)
        self.list_txs = []
        self.result_data = []
        self.cursor = conn.cursor()

    def get_logs(self, block):
        print(block)
        hash = w3.eth.get_block(block)
        block_number = hash['number']
        logs = self.theater_contract.events.Transfer().get_logs(fromBlock=block_number, toBlock=block_number)
        for log in logs:
            if log.args.src == UNISWAP_CONTRACT_ADDRESS or log.args.dst == UNISWAP_CONTRACT_ADDRESS:
                wad = copy.copy(log.args.wad)
                if log.args.src == UNISWAP_CONTRACT_ADDRESS:
                    wad = -log.args.wad
                    data = {'address': log.args.dst,
                            'smart_contract': log.args.src,
                            'amount': wad,
                            'blockNumber': log.blockNumber
                            }
                    self.list_txs.append(data)
                else:
                    data = {'address': log.args.src,
                            'smart_contract': log.args.dst,
                            'amount': wad,
                            'blockNumber': log.blockNumber
                            }
                    self.list_txs.append(data)
        print(self.list_txs)
        self.scan_trx()
        self.run_database('Token-WETH-ADDRESS-Wallet')
        self.insert_data()
        self.result_data = []

    def insert_data(self):
        for entry in self.result_data:
            self.cursor.execute('''
                INSERT INTO `toekn-WETH-address-wallet` (address, smart_contract, balance,delta_balance ,block_number)
                VALUES (%s, %s, %s, %s,%s)
            ''', (
            entry['address'], entry['smart_contract'], str(entry['balance']), entry['delta'], entry['blockNumber']))
            print(' data inserted')

        conn.commit()

    def run_database(self, table_name):
        self.cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                id INTEGER PRIMARY KEY AUTO_INCREMENT,
                address TEXT NOT NULL,
                smart_contract TEXT NOT NULL,
                balance TEXT NOT NULL,
                delta_balance TEXT NOT NULL ,
                block_number INTEGER NOT NULL
            )
            ''')
        conn.commit()

    def scan_trx(self):
        results = {}
        for entry in self.list_txs:
            key = (entry['address'], entry['smart_contract'])

            if key not in results:
                results[key] = []

            results[key].append(entry)

        for key, transactions in results.items():
            block_num = transactions[0]['blockNumber']
            from_addr, to_addr = key
            delta_balance = sum(abs(t['amount']) for t in transactions)
            token_balance = self.theater_contract.functions.balanceOf(from_addr).call(block_identifier=block_num)
            dt = {'address': from_addr, 'smart_contract': to_addr, 'balance': token_balance, 'delta': delta_balance,
                  'blockNumber': block_num}
            self.result_data.append(dt)
            del block_num
        self.list_txs = []

sc = ScanAddress()
async def log_loop(event_filter, poll_interval, pool):
    while True:
        new_events = event_filter.get_new_entries()
        if new_events:
            for event in new_events:
                print('from event')
                pool.apply_async(sc.get_logs, args=(event.hex(),))
        await asyncio.sleep(poll_interval)

def main_real_time():
    tx_filter = w3.eth.filter('latest')
    max_processes = multiprocessing.cpu_count() - 4

    with multiprocessing.Pool(processes=max_processes) as pool:
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(
                asyncio.gather(
                    log_loop(tx_filter, 2, pool)
                )
            )
        finally:
            loop.close()


if __name__ == '__main__':
    freeze_support()
    # sa = ScanAddress()
    # block_num = w3.eth.block_number
    # sa.get_logs(block_num - 10)
    main_real_time()
