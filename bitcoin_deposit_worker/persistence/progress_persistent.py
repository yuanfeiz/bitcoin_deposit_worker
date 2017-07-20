class ProgressPersistent(object):
    """
    Persistent the Bitcoin deposit monitor sync progress.
    """

    def get_last_processed_block(self):
        raise NotImplemented

    def set_last_processed_block(self, last_processed_block_height):
        """
        :type last_processed_block_height: int
        """
        raise NotImplemented
