#!/usr/bin/env python3

import argparse
import json
import struct
import time
import uuid

import pika
import xxhash

from data_sources import edgar, stdin, taxi


class Sender:
    def __init__(self, args):
        parameters = pika.URLParameters(args.rmq_dsn)
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

        self.db = args.db
        self.exchange = args.exchange
        self.rpc_exchange = args.rpc_exchange
        self.receivers = args.receivers
        self.batch_size = args.batch_size
        self.flush_size = args.flush_size

        self.channel.exchange_declare(exchange=self.exchange, exchange_type='direct')

        self.data_source = args.data_source
        self.end_msg = ['EOF'] * self.receivers

        self.rpc_channel = self.connection.channel()
        self.rpc_channel.exchange_declare(exchange=self.rpc_exchange, exchange_type='fanout')

        self.rpc_callback_queue = self.rpc_channel.queue_declare('', exclusive=True).method.queue
        self.comm_ids = {}

    def send_to_receivers(self, comm_id, data):
        for receiver_id in range(self.receivers):
            self.channel.basic_publish(exchange=self.exchange,
                                       routing_key=f'{receiver_id}.{comm_id}',
                                       body=data[receiver_id])

    def get_receiver_id_from_key(self, key):
        key = struct.unpack('>q', xxhash.xxh64(int(key).to_bytes(8, byteorder='little')).digest())[0]
        return (key + self.receivers - 1) % self.receivers

    def prepare_receivers(self, table):
        comm_id = str(uuid.uuid4())
        rpc_id = str(uuid.uuid4())
        request = {
            'comm_id': comm_id,
            'db': self.db,
            'table': table,
            'exchange': self.exchange,
        }
        self.rpc_channel.basic_publish(exchange=self.rpc_exchange,
                                       routing_key='',
                                       properties=pika.BasicProperties(reply_to=self.rpc_callback_queue, correlation_id=rpc_id),
                                       body=json.dumps(request))
      
        results = []  
        for message in self.rpc_channel.consume(self.rpc_callback_queue, auto_ack=True):
            method, properties, body = message

            if properties.correlation_id == rpc_id:
                results.append(body == b'STARTED')

            if len(results) == self.receivers:
                break

        if not all(results):
            print('Not all workers could be started. Aborting.')
            exit(1)

        return comm_id

    def distribute_data(self, data, flush):
        for table, values in data.items():
            if table not in self.comm_ids:
                comm_id = self.prepare_receivers(table)
                assert comm_id, 'No comm_id'
                self.comm_ids[table] = comm_id

            self.send_to_receivers(self.comm_ids[table], values)

            if flush:
                self.send_to_receivers(self.comm_ids[table], self.end_msg)
                self.comm_ids.pop(table, None)

    def append_data(self, tables_data, receivers_data):
        for table, data in tables_data.items():
            target_receiver_id = self.get_receiver_id_from_key(data[0])
            receivers_data[table][target_receiver_id] += data[1] + '\n'

    def work(self, data_source):
        receivers_data = {}
        tables_data = next(data_source.get_line())
        for table_name in tables_data.keys():
            receivers_data[table_name] = [''] * self.receivers

        self.append_data(tables_data, receivers_data)
        n = 1
        comm_id = None

        for tables_data in data_source.get_line():
            self.append_data(tables_data, receivers_data)
            n += 1

            if n % self.batch_size == 0:
                flush = (n % self.flush_size == 0)
                self.distribute_data(receivers_data, flush)
                for table_name in receivers_data.keys():
                    receivers_data[table_name] = [''] * self.receivers

        print('Remainder')
        self.distribute_data(receivers_data, True)

if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument('--rmq-dsn', required=True, help='The DSN of RabbitMQ')
    args.add_argument('--receivers', type=int, required=True, help='How many nodes are receiving')
    args.add_argument('--exchange', required=True, help='The name of the exchange')
    args.add_argument('--rpc-exchange', required=True, help='The name of the exchange for RPCs')
    args.add_argument('--table', required=True, help='The table that is currently sent')
    args.add_argument('--db', required=True, help='The name of the target DB.')
    args.add_argument('--batch-size', type=int, default=100, help='Size of batches to form')
    args.add_argument('--flush-size', type=int, default=10000, help='Send a flush after that many rows')
    args.add_argument('--data-source', choices=('stdin', 'edgar', 'taxi'), default='stdin', required=True, help=(
        'From where to take data.'
    ))
    args.add_argument('--edgar-file')
    args.add_argument('--taxi-file')
    args = args.parse_args()

    if args.data_source == 'stdin':
        data_source = stdin.Stdin(args.table)
    elif args.data_source == 'edgar':
        data_source = edgar.Edgar(args.edgar_file)
    elif args.data_source == 'taxi':
        data_source = taxi.Taxi(args.taxi_file)

    sender = Sender(args)
    sender.work(data_source)
    sender.connection.close()
