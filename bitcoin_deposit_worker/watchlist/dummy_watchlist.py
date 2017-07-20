from bitcoin_deposit_worker.watchlist import Watchlist


class DummyWatchlist(Watchlist):
    """
    Dummy Watchlist that returns a defined set of recipient addresses
    """
    def __init__(self):
        pass

    def get_receipts(self):
        return ['38CzuFGy9Xo3gYMbGpMbWC7y8EZaSgPBwv']
