from configparser import RawConfigParser
from pprint import pprint
from time import sleep

import gevent
import requests
from gevent.queue import Queue

from persistence import FilePersistent


class BitcoinDepositService(object):
    def __init__(self, _persistent=None, _base_url='https://chain.api.btc.com/v3'):
        self.tasks = Queue()

        # Persistent is for saving and loading the progress,
        # the data can be saved in a local file or the database
        self.persistent = _persistent or FilePersistent()

        # TODO: extract transaction fetcher
        self.base_url = _base_url

        self.session = requests.Session()

    def get_block(self, block_height='latest'):
        """
        Get the detail info of a block

        :param block_height: either an integer or 'latest'
        :return: the block dict returned by BTC.com
        """
        url = '%s/block/%s' % (self.base_url, block_height)
        rv = self.session.get(url).json()

        if rv['err_msg']:
            raise Exception(rv['err_msg'])
        else:
            return rv['data']

    def generate_block_transaction_urls(self, block_height):
        # Get the total count of this block
        url = '%s/block/%s/tx' % (self.base_url, block_height)
        rv = self.session.get(url)
        data = rv.json()['data']

        transactions = data['list']
        page = data['page']
        page_size = data['pagesize']
        total_count = data['total_count']

        # Get each pages
        for i in range(1, int(total_count / page_size) + 1):
            paginated_url = url + '?page=' + str(i)
            self.tasks.put_nowait(paginated_url)
            gevent.sleep(.5)

    def process_transaction(self, transaction):

        outputs = transaction['outputs']

        for output in outputs:
            if output['spent_by_tx']:
                pprint('Output is spent, skip')
            else:
                pprint({"addresses": output["addresses"], "value": output["value"]})

    def worker(self, n):
        while not self.tasks.empty():
            url = self.tasks.get()
            rv = self.session.get(url)

            if rv.status_code != 200:
                # Hit the rate limit, retry.
                # Note that there is not max retry times.
                self.tasks.put_nowait(url)

                # Wait for the next URL
                continue

            # All is well, process the transactions
            data = rv.json()['data']

            transactions = data['list']
            for transaction in transactions:
                self.process_transaction(transaction)

            gevent.sleep(.5)

    def run(self):

        # Pick up the progress
        block_height = self.persistent.get_last_processed_block() + 1

        config = RawConfigParser()
        config.read('worker.cfg')

        MIN_CONFIRMATION_COUNT = config.getint('deposit', 'min_confirmation_count')

        # Main event loop
        while True:
            try:
                block = self.get_block(block_height)

                sleep(.5)

                latest_block = self.get_block()

                # TODO: define a block class to abstract the dict
                block_height = block['height']
                latest_block_height = latest_block['height']

                if latest_block_height - MIN_CONFIRMATION_COUNT < block_height:
                    # TODO: define a more specific error class
                    raise Exception('Confirmation is less than required minimum: %d', MIN_CONFIRMATION_COUNT)

                gevent.spawn(self.generate_block_transaction_urls, block_height).join()
                gevent.spawn(self.worker, 'steve').join()

                # Save the checkpoint
                self.persistent.set_last_processed_block(block_height)
            except Exception as e:
                # TODO: capture the aforementioned error class
                sleep(config.getfloat('deposit', 'block_times'))


if __name__ == '__main__':
    srv = BitcoinDepositService()
    srv.run()
