import asyncio
import multiprocessing

from web3 import Web3

from constants import w3, client

k = 0.00000012
input_queue = multiprocessing.Queue()
output_queue = multiprocessing.Queue()


def calculate_point():
    data = input_queue.get()
    print(data)
    res = data['Value'] * k
    print("Address:", data['Address'], 'has this point:', res, ' In Block Number', data['BlockNumber'])
    # output_queue.put({'Address': data['Address'], 'point': res, 'block_num': data['BlockNumber']})


def get_logs(block_hash):
    txs = w3.eth.get_block(block_hash, True)
    for i in txs["transactions"]:
        trx_reciept = client.make_request("trace_transaction", [i['hash']])
        for item in trx_reciept['result']:
            try:
                block_num = item['blockNumber']
                action = item['action']
                # print(block_num)
                if int(action['value'], 16) == 0:
                    break
                else:
                    balance = w3.eth.get_balance(Web3.to_checksum_address(action['from']), block_num)
                    if balance != 0:
                        input_queue.put({'Address': action['from'], 'Value': balance, 'BlockNumber': block_num})
                        calculate_point()
            except Exception as e:
                print(e)


async def log_loop(event_filter, poll_interval, pool):
    while True:
        new_events = event_filter.get_new_entries()
        if new_events:
            for event in new_events:
                pool.apply_async(get_logs, args=(event,))
        await asyncio.sleep(poll_interval)


def main():
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
    main()
