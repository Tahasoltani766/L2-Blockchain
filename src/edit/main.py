import asyncio
from pprint import pprint

from web3 import Web3

from src.grabber_data.constants import w3,client

data_dict = dict()

def handler_real_time(event):
    trx = w3.eth.get_block(event, full_transactions=True)
    return trx.transactions


def analyse_logs(logs):
    global data_dict
    for log in logs:
        block_number = log['blockNumber']
        if data_dict.get(block_number) is None:
            data_dict[block_number] = dict()
        else:
            pass
        trx_reciept = client.make_request("trace_transaction", [log['hash']])
        for item in trx_reciept['result']:
            action = item['action']
            if action['from'] in data_dict[block_number].keys():
                pass
            else:
                data_dict[block_number][action['from']] = {
                    'balance': w3.eth.get_balance(Web3.to_checksum_address(action['from']), block_number),
                    'block_number': block_number
                }
            if action['to'] in data_dict[block_number].keys():
                pass
            else:
                data_dict[block_number][action['to']] = {
                    'balance': w3.eth.get_balance(Web3.to_checksum_address(action['to']), block_number),
                    'block_number': block_number
                }

    pprint(data_dict)
    data_dict = dict()


async def log_loop(event_filter, poll_interval):
    while True:
        for event in event_filter.get_new_entries():
            logs = handler_real_time(event)
            analyse_logs(logs)
        await asyncio.sleep(poll_interval)


def main_real_time():
    block_filter = w3.eth.filter('latest')
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(
            asyncio.gather(
                log_loop(block_filter, 2)))
    finally:
        loop.close()


main_real_time()
