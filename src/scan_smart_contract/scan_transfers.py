from pprint import pprint
from collections import defaultdict
import sqlite3
from web3 import Web3
import copy

w3 = Web3(Web3.HTTPProvider('https://mainnet.infura.io/v3/9b8e587a802a4c5188165b33b1893c55'))

UNISWAP_CONTRACT_ADDRESS = '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D'
WETH_ADDRESS = '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
WETH_ABI = '[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"guy","type":"address"},{"name":"wad","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"wad","type":"uint256"}],"name":"withdraw","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[],"name":"deposit","outputs":[],"payable":true,"stateMutability":"payable","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"},{"name":"","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"payable":true,"stateMutability":"payable","type":"fallback"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":true,"name":"guy","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":true,"name":"dst","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"dst","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Deposit","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Withdrawal","type":"event"}]'
conn = sqlite3.connect('blockchain.db')


class ScanAddress:
    def __init__(self):
        self.theater_contract = w3.eth.contract(address=WETH_ADDRESS, abi=WETH_ABI)
        self.list_txs = []
        self.address_wallet_txs = []
        self.smart_contract_txs = []
        self.cursor = conn.cursor()

    def get_logs(self):
        logs = self.theater_contract.events.Transfer().get_logs(fromBlock=w3.eth.block_number, toBlock='latest')
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
        result_data = self.handler_data()
        self.check_address(result_data)
        self.run_database()
        self.insert_data()

    def check_address(self, data):
        for i in data:
            print(i)
            check = w3.eth.get_code(w3.to_checksum_address(i['address']))
            if check == b'':
                self.address_wallet_txs.append(i)
            else:
                self.smart_contract_txs.append(i)

    def insert_data(self):
        for entry in self.smart_contract_txs:
            self.cursor.execute('''
                INSERT INTO "WETH-SMART-CONTRACT" (address, smart_contract, amount, block_number)
                VALUES (?, ?, ?, ?)
            ''', (entry['address'], entry['smart_contract'], entry['amount'], entry['blockNumber']))
            print('SMART contract is inserted')
        for entry in self.address_wallet_txs:
            self.cursor.execute('''
                            INSERT INTO "WETH-ADDRESS" (address, smart_contract, amount, block_number)
                            VALUES (?, ?, ?, ?)
                        ''', (entry['address'], entry['smart_contract'], entry['amount'], entry['blockNumber']))
            print('address  wallet is inserted')

        conn.commit()


    def handler_data(self):
        summary = defaultdict(lambda: {'amount': 0, 'blockNumber': 0})

        for entry in self.list_txs:
            key = (entry['address'], entry['smart_contract'])
            summary[key]['amount'] += entry['amount']
            summary[key]['blockNumber'] = entry['blockNumber']

        result = [{'address': k[0], 'smart_contract': k[1], 'amount': v['amount'], 'blockNumber': v['blockNumber']} for k, v in
                  summary.items()]
        return result

    def run_database(self):
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS "WETH-ADDRESS" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL,
            smart_contract TEXT NOT NULL,
            amount REAL NOT NULL,
            block_number INTEGER NOT NULL
        )
        ''')

        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS "WETH-SMART-CONTRACT" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL,
            smart_contract TEXT NOT NULL,
            amount REAL NOT NULL,
            block_number INTEGER NOT NULL
        )
        ''')

        conn.commit()


if __name__ == '__main__':
    sa = ScanAddress()
    sa.get_logs()
