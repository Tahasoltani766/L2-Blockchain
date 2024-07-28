from constants import w3
from web3 import HTTPProvider, Web3



def get_logs(block_hash):
    w3 = Web3(HTTPProvider("https://weathered-blue-leaf.quiknode.pro/36c5cef698f43f466f5874392ae470f3d6eac6a5/"))
    logs_block = w3.eth.get_logs({'blockHash': block_hash})
    for log in logs_block:
        trx = w3.eth.get_transaction_receipt(log['transactionHash'])
        trx_hash = trx['transactionHash']
        get_acotion(trx_hash)

def get_acotion(trx_hash):
    client = HTTPProvider('https://weathered-blue-leaf.quiknode.pro/36c5cef698f43f466f5874392ae470f3d6eac6a5/')
    result = client.make_request("trace_transaction", [trx_hash])
    data = result['result']
    from_addresses = [item['action']['from'] for item in data]
    to_addresses = [item['action']['to'] for item in data]
    print(from_addresses, to_addresses)


get_logs('0xd93de0deef33d4d25e8f7d14c25ffebec85f7679a69f2f505179db0781c6960b')