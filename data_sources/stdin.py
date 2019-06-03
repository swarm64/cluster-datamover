
import sys


class Stdin:
    def __init__(self):
        pass

    def get_line(self):
        for line in sys.stdin:
            yield line
