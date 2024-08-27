import os
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
event_filter = token_contract.events.BetBull.create_filter(fromBlock="latest")


async def handle_event(event):
    print(event)


async def main():
    while True:
        events = event_filter.get_new_entries()
        for event in events:
            await handle_event(event)
        await asyncio.sleep(10)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())