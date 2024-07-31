from web3 import Web3,HTTPProvider
import sqlite3


# w3 = Web3(Web3.HTTPProvider('https://fabled-intensive-sanctuary.blast-mainnet.quiknode.pro/ae955c85386492533d343ac618b663699fd1fa2d/'))
w3 = Web3(Web3.HTTPProvider('https://falling-withered-sea.quiknode.pro/1422631309f1ddd81ec4b8c585935a848651d77a/'))
client = HTTPProvider('https://falling-withered-sea.quiknode.pro/1422631309f1ddd81ec4b8c585935a848651d77a/')

# conn = sqlite3.connect('Blockchain.db')
# cursor = conn.cursor()
# columns = ["Address TEXT PRIMARY KEY"] + [f'"{i}" TEXT' for i in range(5)]
# create_table_query = f'''
#            CREATE TABLE IF NOT EXISTS smart_contract (
#                {", ".join(columns)}
#            )
#            '''
# cursor.execute(create_table_query)
#
# for i in range(1998000, 2000000):
#     cursor.execute(f'ALTER TABLE smart_contract ADD COLUMN "{i}" TEXT')
# #
# conn.commit()
# conn.close()
# # print('Finish')
# # # conn.commit()