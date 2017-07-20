from bitcoin_deposit_worker.persistence.progress_persistent import ProgressPersistent


class FilePersistent(ProgressPersistent):
    """
    Using file to persistent the last processed block
    """

    def __init__(self, _path="bitcoin_deposit_worker.dat", _start=None):
        self.path = _path
        self.start = _start

    def get_last_processed_block(self):
        # `start` takes the priority
        if self.start is not None:
            return self.start

        # If `start` is not specified, the block height saved in the data file is returned
        with open(self.path, 'r') as f:
            return int(f.read())

    def set_last_processed_block(self, last_processed_block):
        with open(self.path, 'w+') as f:
            f.write(str(last_processed_block))
            f.truncate()
