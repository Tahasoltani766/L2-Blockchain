import os
import sqlite3
import time
from collections import defaultdict
from pprint import pprint
from web3 import Web3, HTTPProvider
import numpy as np

w3 = Web3(Web3.HTTPProvider('https://black-convincing-patron.quiknode.pro/7c8c6aa9c9f5b3b026ce52bbad53f06695ab6b82/'))
client = HTTPProvider('https://black-convincing-patron.quiknode.pro/7c8c6aa9c9f5b3b026ce52bbad53f06695ab6b82/')


class ScanSmartContract:
    def __init__(self):
        self.extracted_data = []

    def get_logs(self, block_hash):
        txs = w3.eth.get_block(block_hash, True)
        for i in txs["transactions"]:
            trx_reciept = client.make_request("trace_transaction", [i['hash']])
            for item in trx_reciept['result']:
                try:
                    action = item['action']
                    if int(action['value'], 16) == 0:
                        break
                    else:
                        self.extracted_data.append({
                            'from': action['from'],
                            'to': action['to'],
                            'value': int(action['value'], 16),
                            'blockNumber': item['blockNumber']})
                except Exception as e:
                    print(e)
        print('RUN SCAN TRX')
        pprint(self.extracted_data)
        self.scan_trx()

    def scan_trx(self):
        results = {}

        for entry in self.extracted_data:
            key = (entry['from'], entry['to'])

            if key not in results:
                results[key] = []

            results[key].append(entry)

        for key, transactions in results.items():
            from_addr, to_addr = key

            delta_balance = sum(abs(t['value']) for t in transactions)
            res_balance = sum(t['value'] for t in transactions)

            print(f"from: {from_addr}, to: {to_addr}")
            print(f"res_balance: {res_balance}")
            print(f"delta_balance: {delta_balance}")
            print('-----------------------------------------')

        self.extracted_data = []

    def get_tokens(self):
        pass


ssc = ScanSmartContract()
ssc.get_logs('0xb8a3f7f5cfc1748f91a684f20fe89031202cbadcd15078c49b85ec2a57f43853')
# ssc.scan_trx()
