from web3 import Web3, HTTPProvider
import sqlite3

# w3 = Web3(Web3.HTTPProvider('https://fabled-intensive-sanctuary.blast-mainnet.quiknode.pro/ae955c85386492533d343ac618b663699fd1fa2d/'))
w3 = Web3(
    Web3.HTTPProvider('https://proportionate-magical-hexagon.quiknode.pro/8b1cedff54880ad8d4b1d521dfe4bc6756a2d165'))
client = HTTPProvider('https://proportionate-magical-hexagon.quiknode.pro/8b1cedff54880ad8d4b1d521dfe4bc6756a2d165')

conn = sqlite3.connect('example.db')
cursor = conn.cursor()
columns = ["Address TEXT PRIMARY KEY"] + [f'"{i}" TEXT' for i in range(20553975, 20554475)]
create_table_query = f'''
           CREATE TABLE IF NOT EXISTS my_table (
               {", ".join(columns)}
           )
           '''
cursor.execute(create_table_query)

# for i in range(1998000, 2000000):
#     cursor.execute(f'ALTER TABLE smart_contract ADD COLUMN "{i}" TEXT')
# #
# conn.commit()
# conn.close()
# print('Finish')
# # conn.commit()
