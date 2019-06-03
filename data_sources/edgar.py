#!/usr/bin/env python3

import os

from datetime import datetime
from glob import glob
from zipfile import ZipFile


class Edgar:
    def __init__(self, workers, batch_size, directory):
        self.workers = workers
        self.batch_size = batch_size
        self.directory = directory

    @staticmethod
    def transform(data):
        ip1, ip2, ip3, user = data[0].split('.')

        timestamp = '{} {} +0000'.format(data[1], data[2])
        timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S %z').timestamp()

        return [
            int(timestamp),                      # timestamp
            '{}.{}.{}.0'.format(ip1, ip2, ip3),  # ip
            user,                                # user
            int(data[4]),                        # cik
            data[5],                             # accession
            data[6],                             # doc
            int(data[7]),                        # code
            int(data[8]) if data[8] else 0,      # size
            int(data[9]) == 1,                   # idx
            int(data[10]) == 1,                  # norefer
            int(data[11]) == 1,                  # noagent
            int(data[12]),                       # find
            int(data[13]) == 1,                  # crawler
            data[14]                             # browser
        ]

    def somename(self):
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
                     for line in csv_file:
                         if b'"' in line:
                             continue
                         elif b'e+' in line:
                             continue

                         line = re.sub(b'\.[0-9],', b',', line)
                         data = line_to_list(line)
                         data = Edgar.transform(data)
                         lines.append(str(data[0]) + ' ' + '|'.join([str(item) for item in data]) + '\n')

                         if len(lines) % args.batch_size == 0:
                            sender = Sender(args)
                            sender.work(lines)
                            sender.connection.close()
                            lines = []
