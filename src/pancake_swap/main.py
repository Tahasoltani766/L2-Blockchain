import os
from web3 import Web3, HTTPProvider
from dotenv import load_dotenv
from web3.middleware import geth_poa_middleware
import threading

load_dotenv()
w3_url = os.getenv('QUICKNODE_URL')
web3_client = HTTPProvider(w3_url)
web3 = Web3(web3_client)
web3.middleware_onion.inject(geth_poa_middleware, layer=0)

with open("../ABI/PancakeSwap.abi", "r") as f:
    abi = f.read()
    f.close()

token_contract = web3.eth.contract(abi=abi, address=web3.to_checksum_address(os.getenv("CONTRACT_ADDRESS")))

def handle_event(event):
    print(event)


def main(filter_func):
    event = getattr(token_contract.events, filter_func)
    event_filter = event.create_filter(fromBlock="latest")
    while True:
        events = event_filter.get_new_entries()
        for event in events:
            handle_event(event)
        # asyncio.sleep(10)


if __name__ == '__main__':
    my_th_cliam = threading.Thread(target=main, args=('Claim',))
    my_th_bet_bull = threading.Thread(target=main, args=('BetBull',))
    my_th_bet_bear = threading.Thread(target=main, args=('BetBear',))

    my_th_cliam.start()
    my_th_bet_bear.start()
    my_th_bet_bull.start()

    my_th_cliam.join()
    my_th_bet_bear.join()
    my_th_bet_bull.join()


