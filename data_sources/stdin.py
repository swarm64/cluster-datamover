
import sys


class Stdin:
    def __init__(self, table_name):
        self.table_name = table_name

    def get_line(self):
        for line in sys.stdin:
            key, data = line.split(' ', maxsplit=1)
            yield {self.table_name: (key, data.rstrip())}
