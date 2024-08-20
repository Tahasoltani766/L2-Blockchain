import sqlite3

db_path = 'example.db'
table_name = 'my_table'
row_id = 6
column_index = 2

def get_loc():
    conn = sqlite3.connect('example.db')
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM my_table WHERE id = ?", (row_id,))
    row = cursor.fetchone()

    if row:
        value = row[column_index]
        print(f"The value at column index {column_index} is: {value}")
    else:
        print("No row found with the specified id.")

    def next_block_balance(self, column_index, row_id, number_block):
        conn = sqlite3.connect('example.db')
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM my_table WHERE id = ?", (row_id,))
        row = cursor.fetchone()
        value = row[column_index]
        if value == None:
            print(f"in row id {row_id} is: {value}", int(number_block)+1)


        # self.next_block_balance(int(null_cell['ColumnIndex']) + 1, null_cell['Row'],null_cell['Column'])