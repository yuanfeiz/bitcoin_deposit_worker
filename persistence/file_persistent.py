from persistence.progress_persistent import ProgressPersistent


class FilePersistent(ProgressPersistent):
    """
    Using file to persistent the last processed block
    """

    def __init__(self, path="bitcoin_deposit_worker.dat"):
        self.path = path
        self.file = open(self.path, 'w')

    def close(self):
        self.file.flush()
        self.file.close()

    def get_last_processed_block(self):
        return int(self.file.read())


    def set_last_processed_block(self, last_processed_block):
        self.file.write(last_processed_block)
        self.file.flush()
