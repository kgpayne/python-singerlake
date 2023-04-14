from singerlake.store import BaseStore


class SingerLake:
    def __init__(self, store: BaseStore):
        self.store = store
