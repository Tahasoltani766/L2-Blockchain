from collections import defaultdict
from pprint import pprint
from web3 import Web3, HTTPProvider
from data import data , test_data

w3 = Web3(
    Web3.HTTPProvider('https://proportionate-magical-hexagon.quiknode.pro/8b1cedff54880ad8d4b1d521dfe4bc6756a2d165'))
client = HTTPProvider('https://proportionate-magical-hexagon.quiknode.pro/8b1cedff54880ad8d4b1d521dfe4bc6756a2d165')


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
        pprint(self.extracted_data)
        # self.scan_trx()

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
# ssc.get_logs('0xb8a3f7f5cfc1748f91a684f20fe89031202cbadcd15078c49b85ec2a57f43853')
ssc.scan_trx()


def check_address(self, data):
    for i in data:
        check = w3.eth.get_code(w3.to_checksum_address(i['address']))
        if check == b'':
            self.address_wallet_txs.append(i)
        else:
            self.smart_contract_txs.append(i)
    def handler_data(self):
        summary = defaultdict(lambda: {'amount': 0, 'blockNumber': 0})
        for entry in self.list_txs:
            key = (entry['address'], entry['smart_contract'])
            summary[key]['amount'] += entry['amount']
            summary[key]['blockNumber'] = entry['blockNumber']
        result = [{'address': k[0], 'smart_contract': k[1], 'amount': v['amount'], 'blockNumber': v['blockNumber']} for
                  k, v in
                  summary.items()]
        return result