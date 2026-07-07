import time

class Timer:
    def __init__(self, name: str = __name__ ):
        self.start_time = None
        self.name = name
        self.reset()

    @property
    def elapsed(self):
        return time.time() - self.start_time

    def reset(self):
        self.start_time = time.time()