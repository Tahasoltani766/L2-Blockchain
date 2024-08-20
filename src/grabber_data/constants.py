from web3 import Web3, HTTPProvider
import sqlite3
import mysql.connector


# w3 = Web3(Web3.HTTPProvider('https://fabled-intensive-sanctuary.blast-mainnet.quiknode.pro/ae955c85386492533d343ac618b663699fd1fa2d/'))
w3 = Web3(
    Web3.HTTPProvider('https://proportionate-magical-hexagon.quiknode.pro/8b1cedff54880ad8d4b1d521dfe4bc6756a2d165'))
client = HTTPProvider('https://proportionate-magical-hexagon.quiknode.pro/8b1cedff54880ad8d4b1d521dfe4bc6756a2d165')

# conn = sqlite3.connect('example.db')
# columns = ["id INTEGER PRIMARY KEY AUTOINCREMENT"] + ["Address TEXT"] + [f'"{i}" INT' for i in range(20553975, 20553985)]
# create_table_query = f'''
#            CREATE TABLE IF NOT EXISTS my_table (
#                {", ".join(columns)}
#            )
#            '''
#
# cursor.execute(create_table_query)

conn = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    database="blockchain"
)
columns = ["id INT AUTO_INCREMENT PRIMARY KEY"] + ["Address VARCHAR(255)"] + [f'`{i}` VARCHAR(255)' for i in range(20553975, 20553978)]

create_table_query = f'''
    CREATE TABLE IF NOT EXISTS my_table (
        {", ".join(columns)}
    )
'''
cursor = conn.cursor()

cursor.execute(create_table_query)
