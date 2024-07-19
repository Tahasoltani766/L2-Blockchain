from constants import w3
import csv


def get_address(block_num):
    for j in range(6294025, block_num):
        try:
            trx = w3.eth.get_block(j, full_transactions=True)
            for i in trx.transactions:
                from_is_smart_contract = checker_smart_contract(i['from'])
                to_is_smart_contract = checker_smart_contract(i['to'])

                if not from_is_smart_contract:
                    address_wallet.add(i['from'])
                else:
                    smart_contracts.add(i['from'])

                if not to_is_smart_contract:
                    address_wallet.add(i['to'])
                else:
                    smart_contracts.add(i['to'])

        except Exception:
            with open('last_block.txt', 'w') as file:
                file.write(str(j))
            with open('address_wallet.csv', mode='w', newline='') as file:
                writer = csv.writer(file)
                for item in address_wallet:
                    writer.writerow([item])
            with open('smart_contract.csv', mode='w', newline='') as file:
                writer = csv.writer(file)
                for item in smart_contracts:
                    writer.writerow([item])
            break


def checker_smart_contract(address):
    check = w3.eth.get_code(w3.to_checksum_address(address))
    if check == b'':
        return False
    else:
        return True


if __name__ == '__main__':
    address_wallet = set()
    smart_contracts = set()
    get_address(6294027)
