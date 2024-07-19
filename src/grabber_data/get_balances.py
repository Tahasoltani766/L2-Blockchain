from constants import w3
from web3 import Web3


def get_balance(address):
    balance = w3.eth.get_balance(Web3.to_checksum_address(address), 6246064)
    print(balance)


print(w3.eth.get_code('0x01f9aAf5Cc8F752872980A773AFfE40eF180e37A'))