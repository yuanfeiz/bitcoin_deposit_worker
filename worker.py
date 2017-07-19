from configparser import RawConfigParser
from pprint import pprint
from time import sleep

import gevent
import requests
from gevent.queue import Queue

from persistence import FilePersistent


s = requests.Session()

base_url = 'https://chain.api.btc.com/v3'


# Get the latest block
def get_block(block_height='latest'):
    url = '%s/block/%s' % (base_url, block_height)
    rv = s.get(url).json()

    if rv['err_msg']:
        raise Exception(rv['err_msg'])
    else:
        return rv['data']


tasks = Queue()


def generate_block_transaction_urls(block_height):
    # Get the total count of this block
    url = '%s/block/%s/tx' % (base_url, block_height)
    rv = s.get(url)
    data = rv.json()['data']

    transactions = data['list']
    page = data['page']
    page_size = data['pagesize']
    total_count = data['total_count']

    # Get each pages
    for i in range(1, int(total_count / page_size) + 1):
        paginated_url = url + '?page=' + str(i)
        tasks.put_nowait(paginated_url)
        gevent.sleep(.5)


def process_transaction(transaction):
    outputs = transaction['outputs']

    for output in outputs:
        if output['spent_by_tx']:
            pprint('Output is spent, skip')
        else:
            pprint({"addresses": output["addresses"], "value": output["value"]})


def worker(n):
    while not tasks.empty():
        url = tasks.get()
        rv = s.get(url)

        if rv.status_code != 200:
            # Hit the rate limit, retry.
            # Note that there is not max retry times.
            tasks.put_nowait(url)

            # Wait for the next URL
            continue

        # All is well, process the transactions
        data = rv.json()['data']

        pprint('Worker %s got url %s' % (n, url))

        transactions = data['list']
        for transaction in transactions:
            process_transaction(transaction)

        gevent.sleep(.5)

if __name__ == '__main__':
    # Persistent is for saving and loading the progress, the data can be saved in a local file or the database
    persistent = FilePersistent()

    # Pick up the progress
    block_height = persistent.get_last_processed_block() + 1

    config = RawConfigParser()
    config.read('worker.cfg')

    MIN_CONFIRMATION_COUNT = config.getint('deposit', 'min_confirmation_count')

    # Main event loop
    while True:
        try:
            block = get_block(block_height)

            sleep(.5)

            latest_block = get_block()

            if latest_block['height'] - MIN_CONFIRMATION_COUNT < block['height']:
                # TODO: define a more specific error class
                raise Exception('Confirmation is less than required minimum: %d', MIN_CONFIRMATION_COUNT)

            gevent.spawn(generate_block_transaction_urls, ).join()
            gevent.spawn(worker, 'steve').join()

            # Commit the changes
            persistent.set_last_processed_block()
        except Exception as e:
            # TODO: capture the aforementioned error class
            sleep(config.getfloat('deposit', 'block_times'))
