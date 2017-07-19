from pprint import pprint
from time import sleep

import gevent
import requests
from gevent.queue import Queue

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
            pprint(transaction['outputs'])

        gevent.sleep(.5)


block = get_block()
pprint(block)

MIN_CONFIRMATIONS = 6

latest_block_height = block['height']

til_block_height = latest_block_height - MIN_CONFIRMATIONS

sleep(.5)

w = gevent.spawn(generate_block_transaction_urls, til_block_height).join()

gevent.joinall([
    gevent.spawn(worker, 'steve'),
    gevent.spawn(worker, 'john'),
    gevent.spawn(worker, 'nancy'),
])
