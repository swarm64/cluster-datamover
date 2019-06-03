#!/usr/bin/env python3

import argparse
import json
import time
import uuid

import pika

from data_sources import edgar, stdin


class Sender:
    def __init__(self, args):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=args.rmq_host))
        self.channel = self.connection.channel()

        self.db = args.db
        self.exchange = args.exchange
        self.rpc_exchange = args.rpc_exchange
        self.receivers = args.receivers
        self.chunk_id = args.chunk_id
        self.batch_size = args.batch_size
        self.flush_size = args.flush_size

        self.channel.exchange_declare(exchange=self.exchange, exchange_type='direct')

        self.table = args.table
        self.data_source = args.data_source
        self.end_msg = ['EOF'] * self.receivers

    def send_to_receivers(self, data):
        for receiver_id in range(self.receivers):
            self.channel.basic_publish(exchange=self.exchange,
                                       routing_key=f'{receiver_id}.{self.chunk_id}',
                                       body=data[receiver_id])

    def get_receiver_id_from_key(self, key):
        return (int(key) + self.receivers - 1) % self.receivers

    def prepare_receivers(self):
        rpc_channel = self.connection.channel()
        rpc_channel.exchange_declare(exchange=self.rpc_exchange, exchange_type='fanout')

        rpc_callback_queue = rpc_channel.queue_declare('', exclusive=True).method.queue

        rpc_id = str(uuid.uuid4())
        request = {
            'db': self.db,
            'table': self.table,
            'exchange': self.exchange,
            'worker_id': self.chunk_id
        }
        rpc_channel.basic_publish(exchange=self.rpc_exchange,
                                  routing_key='',
                                  properties=pika.BasicProperties(reply_to=rpc_callback_queue, correlation_id=rpc_id),
                                  body=json.dumps(request))
      
        results = []  
        for message in rpc_channel.consume(rpc_callback_queue, auto_ack=True):
            method, properties, body = message

            if properties.correlation_id == rpc_id:
                results.append(body == b'STARTED')

            if len(results) == self.receivers:
                break

        if not all(results):
            print('Not all workers could be started. Aborting.')
            exit(1)

    def work(self, data_source):
        self.prepare_receivers()
        lines = [''] * self.receivers
        n = 0

        for line in data_source.get_line():
            key, data = line.split(' ', 1)

            receiver_id = self.get_receiver_id_from_key(key)
            lines[receiver_id] += data
            n += 1

            if n % self.batch_size == 0:
                self.send_to_receivers(lines)
                lines = [''] * self.receivers

            if n % self.flush_size == 0:
                self.send_to_receivers(self.end_msg)
                self.prepare_receivers()

        # Send remaining buffer
        print('Remainder')
        self.prepare_receivers()
        self.send_to_receivers(lines)
        self.send_to_receivers(self.end_msg)

if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument('--rmq-host', required=True, help='The host of RabbitMQ')
    args.add_argument('--receivers', type=int, required=True, help='How many nodes are receiving')
    args.add_argument('--exchange', required=True, help='The name of the exchange')
    args.add_argument('--rpc-exchange', required=True, help='The name of the exchange for RPCs')
    args.add_argument('--chunk-id', required=True, help='Chunk ID for correct routing')
    args.add_argument('--table', required=True, help='The table that is currently sent')
    args.add_argument('--db', required=True, help='The name of the target DB.')
    args.add_argument('--batch-size', type=int, default=100, help='Size of batches to form')
    args.add_argument('--flush-size', type=int, default=10000, help='Send a flush after that many rows')
    args.add_argument('--data-source', choices=('stdin', 'edgar'), default='stdin', required=True, help=(
        'From where to take data.'
    ))
    args = args.parse_args()

    if args.data_source == 'stdin':
        data_source = stdin.Stdin()
    elif args.data_source == 'edgar':
        data_source = edgar.Edgar()

    sender = Sender(args)
    sender.work(data_source)
    sender.connection.close()
