import asyncio
import multiprocessing
from constants import cursor, w3, client

class point():
    def __init__(self):
        query = "SELECT * FROM eth_balance"
        cursor.execute(query)
        self.rows = cursor.fetchall()
        self.column_names = [description[0] for description in cursor.description]
        self.k = 0.0003

    def handel_address(self):
        for row in self.rows:
            self.calculate_point(row[1], row[2:])

    def calculate_point(self, address, balance):
        res = balance * self.k
        print("Address:", address, 'has this point:', res)

    def get_logs(self, block_hash):
        txs = w3.eth.get_block(block_hash, True)
        for i in txs["transactions"]:
            trx_reciept = client.make_request("trace_transaction", [i['hash']])
            for item in trx_reciept['result']:
                try:
                    action = item['action']
                    print(
                        action
                    )
                    if int(action['value'], 16) == 0:
                        break
                    else:
                        # TODO What is Balance Here ?!
                        print(action['from'], int(action['value'], 16))
                        # self.calculate_point(action['from'], int(action['value'], 16))
                except Exception as e:
                    print(e)

    async def log_loop(self, event_filter, poll_interval, pool):
        while True:
            new_events = event_filter.get_new_entries()
            if new_events:
                for event in new_events:
                    pool.apply_async(self.get_logs, args=(event,))
            await asyncio.sleep(poll_interval)

    def main_real_time(self):
        tx_filter = w3.eth.filter('latest')
        max_processes = multiprocessing.cpu_count() - 3
        with multiprocessing.Pool(processes=max_processes) as pool:
            loop = asyncio.get_event_loop()
            try:
                loop.run_until_complete(
                    asyncio.gather(
                        self.log_loop(tx_filter, 2, pool)
                    )
                )
            finally:
                loop.close()


if __name__ == "__main__":
    point = point()
    # point.handel_address()
    point.main_real_time()
