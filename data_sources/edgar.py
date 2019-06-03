
import os
import re

from datetime import datetime
from glob import glob
from zipfile import ZipFile


class Edgar:
    def __init__(self):
        self.directory = '.'

    @staticmethod
    def transform(data):
        ip1, ip2, ip3, user = data[0].split('.')

        timestamp = '{} {} +0000'.format(data[1], data[2])
        timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S %z').timestamp()

        return [
            str(int(timestamp)),                 # timestamp
            '{}.{}.{}.0'.format(ip1, ip2, ip3),  # ip
            user,                                # user
            data[4],                             # cik
            data[5],                             # accession
            data[6],                             # doc
            data[7],                             # code
            data[8] if data[8] else "0",         # size
            data[9],                             # idx
            data[10],                            # norefer
            data[11],                            # noagent
            data[12],                            # find
            data[13],                            # crawler
            data[14]                             # browser
        ]

    def get_line(self):
        path = os.path.join(self.directory, '*.zip')
        all_files = sorted(glob(path))

        def line_to_list(line):
            return line.decode('utf-8').rstrip().split(',')

        for fpath in all_files:
            with ZipFile(fpath) as zip_file:
                content = zip_file.namelist()
                csv_file_name = [path for path in zip_file.namelist() if 'csv' in path][0]
                # print('Sending ' + csv_file_name)
                with zip_file.open(csv_file_name) as csv_file:
                     header = line_to_list(csv_file.readline())
                     lines = []
                     n = 0
                     for line in csv_file:
                         if b'"' in line:
                             continue
                         elif b'e+' in line:
                             continue

                         line = re.sub(b'\.[0-9],', b',', line)
                         data = line_to_list(line)
                         data = Edgar.transform(data)
                         n += 1

                         yield f'{data[0]} {"|".join(data)}\n'
