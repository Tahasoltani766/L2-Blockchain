import asyncio
import os
from web3 import Web3, HTTPProvider
from dotenv import load_dotenv
import pprint
from threading import Thread

load_dotenv()
data_dict = dict()

w3_url = os.getenv('QUICKNODE_URL')

web3_client = HTTPProvider(w3_url)
web3 = Web3(web3_client)

with open("../ABI/ERC20.abi", "r") as f:
    abi = f.read()
    f.close()

token_contract = web3.eth.contract(abi=abi, address=web3.to_checksum_address(os.getenv("WETH_ADDRESS")))
my_contract_address = os.getenv("CONTRACT_ADDRESS")


def get_logs_block(start_block=None, end_block=None,):
    if start_block is None:
        start_block = 0
    if end_block is None:
        end_block = "latest"
    logs = token_contract.events.Transfer().get_logs(fromBlock=start_block, toBlock=end_block)
    return logs


checksum = web3.to_checksum_address


def analyse_logs(logs):
    global data_dict

    for log in logs:
        block_num = log.blockNumber
        if data_dict.get(block_num) is None:
            data_dict[block_num] = dict()
        else:
            pass
        transfer_event = dict(log.args)
        if checksum(transfer_event['dst']) == checksum(my_contract_address):
            if checksum(transfer_event['src']) in data_dict[block_num].keys():
                data_dict[block_num][checksum(transfer_event['src'])]['amount'] += transfer_event.get("wad", 0)
            else:
                data_dict[block_num][checksum(transfer_event['src'])] = {
                    'smart_contract': my_contract_address,
                    'amount': transfer_event.get("wad", 0),
                    'blockNumber': block_num
                }
        elif checksum(transfer_event['src']) == checksum(my_contract_address):
            if checksum(transfer_event['dst']) in data_dict[block_num].keys():
                data_dict[block_num][checksum(transfer_event['dst'])]['amount'] += transfer_event.get("wad", 0)
            else:
                data_dict[block_num][checksum(transfer_event['dst'])] = {
                    'smart_contract': my_contract_address,
                    'amount': transfer_event.get("wad", 0),
                    'blockNumber': block_num
                }
    pprint.pprint(data_dict)




def get_historical_transfer(iteration, start_block, end_block, block_cur):
    th_list = []
    for i in range(start_block, end_block, iteration):
        logs = get_logs_block(start_block, min(i + iteration, block_cur, end_block))
        start_block += iteration
        th = Thread(target=analyse_logs,args=(logs,))
        th.start()
        th_list.append(th)
    for i in th_list:
        i.join()
    return data_dict


def handler_real_time(event):
    logs = token_contract.events.Transfer().get_logs(block_hash=event.hex())
    return logs

async def log_loop(event_filter, poll_interval):
    while True:
        for event in event_filter.get_new_entries():
            logs = handler_real_time(event)
            analyse_logs(logs)

        await asyncio.sleep(poll_interval)

def main_real_time():
    block_filter = web3.eth.filter('latest')
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(
            asyncio.gather(
                log_loop(block_filter, 2)))
    finally:
        loop.close()

if __name__ == "__main__":
    # block_now = web3.eth.block_number
    # get_historical_transfer(2, block_now - 100, block_now, block_now)
    # pprint.pprint(data_dict)
    main_real_time()