from persistence.progress_persistent import ProgressPersistent


class FilePersistent(ProgressPersistent):
    """
    Using file to persistent the last processed block
    """

    def __init__(self, path="bitcoin_deposit_worker.dat"):
        self.path = path
        self.file = open(self.path, 'wr')

    def close(self):
        self.file.flush()
        self.file.close()

    def get_last_processed_block(self):
        pass

    def set_last_processed_block(self, last_processed_block):
        pass