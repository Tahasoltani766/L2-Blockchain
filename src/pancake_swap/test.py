import os
import threading
from web3 import Web3, HTTPProvider
from dotenv import load_dotenv
from web3.middleware import geth_poa_middleware
import asyncio

load_dotenv()
w3_url = os.getenv('QUICKNODE_URL')
web3_client = HTTPProvider(w3_url)
web3 = Web3(web3_client)
web3.middleware_onion.inject(geth_poa_middleware, layer=0)

with open("../ABI/PancakeSwap.abi", "r") as f:
    abi = f.read()
    f.close()

token_contract = web3.eth.contract(abi=abi, address=web3.to_checksum_address(os.getenv("CONTRACT_ADDRESS")))


def get_block(block_number):
    list_hash_tx = []
    trx = web3.eth.get_block(block_number, full_transactions=True)
    for item in trx.transactions:
        list_hash_tx.append((item['hash']).hex())
    return list_hash_tx


def get_logs(hash_trx):
    try:
        result = web3_client.make_request("trace_transaction", [hash_trx])
        data = result['result']
        data_dict = {hash_trx: []}
        for item in data:
            action = item['action']
            data_dict[hash_trx].append(action)
        print(data_dict)
    except:
        pass


# if __name__ == "__main__":
# hash_txs = get_block(41728702)
# list_th = []
# max_thread = 20
# for i in range(0, len(hash_txs), max_thread):
#     batch = hash_txs[i:i + max_thread]
#     for hash in batch:
#         my_thread = threading.Thread(target=get_logs, args=(hash,))
#         list_th.append(my_thread)
#     for i in list_th:
#         i.start()
#     for i in list_th:
#         i.join()
#     list_th = []