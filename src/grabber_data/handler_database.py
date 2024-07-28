import sqlite3

# def insert_address():



if __name__ == "__main__":
    conn = sqlite3.connect('example.db')
    cursor = conn.cursor()
    columns = ["Address TEXT"] + [f'"{i}" TEXT' for i in range(101)]

    create_table_query = f'''
        CREATE TABLE IF NOT EXISTS test (
            {", ".join(columns)}
        )
        '''
    cursor.execute(create_table_query)
    sqlite_insert_query = """INSERT INTO test
                              (Address) 
                               VALUES 
                              ('0x0000000000000000000000000000000000')"""

    count = cursor.execute(sqlite_insert_query)
    conn.commit()


    cursor.execute('PRAGMA table_info(test)')

    conn.commit()
    conn.close()

