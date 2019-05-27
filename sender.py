#!/usr/bin/env python3

import argparse
import sys
import time

import pika


class Sender:
    def __init__(self, args):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=args.rmq_host))
        self.channel = self.connection.channel()

        self.exchange = args.exchange
        self.receivers = args.receivers
        self.chunk_id = args.chunk_id
        self.batch_size = args.batch_size

        self.channel.exchange_declare(exchange=self.exchange, exchange_type='direct')

        self.begin_msg = [f'BEGIN-BEGIN-BEGIN {args.table}'] * self.receivers
        self.end_msg = ['EOF-EOF-EOF'] * self.receivers

    def send_to_receivers(self, data):
        for receiver_id in range(self.receivers):
            self.channel.basic_publish(exchange=self.exchange,
                                       routing_key=f'{receiver_id}.{self.chunk_id}',
                                       body=data[receiver_id])

    def get_receiver_id_from_key(self, key):
        return (int(key) + self.receivers - 1) % self.receivers

    def work(self):
        self.send_to_receivers(self.begin_msg)

        lines = [''] * self.receivers
        n = 0
        for line in sys.stdin:
            key, data = line.split(' ', 1)

            receiver_id = self.get_receiver_id_from_key(key)
            lines[receiver_id] += data
            n += 1

            if n % self.batch_size == 0:
                self.send_to_receivers(lines)
                lines = [''] * self.receivers

        # Send remaining buffer
        print('Remainder')
        self.send_to_receivers(lines)
        self.send_to_receivers(self.end_msg)

if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument('--rmq-host', required=True, help='The host of RabbitMQ')
    args.add_argument('--receivers', type=int, required=True, help='How many nodes are receiving')
    args.add_argument('--exchange', required=True, help='The name of the exchange')
    args.add_argument('--chunk-id', required=True, help='Chunk ID for correct routing')
    args.add_argument('--table', required=True, help='The table that is currently sent')
    args.add_argument('--batch-size', type=int, default=100, help='Size of batches to form')
    args = args.parse_args()

    sender = Sender(args)
    sender.work()
    sender.connection.close()


# #!/usr/bin/env python3
# 
# import argparse
# import sys
# 
# import pika
# 
# 
# def send(args):
#     connection = pika.BlockingConnection(pika.ConnectionParameters(host=args.rmq_host))
#     channel = connection.channel()
# 
#     channel.exchange_declare(exchange=args.exchange, exchange_type='direct')
# 
#     receivers = args.receivers
#     # base_key = f'{args.chunk_id}.{args.table}'
#     base_key = f'{args.chunk_id}'
# 
#     for receiver_id in range(receivers):
#         channel.basic_publish(exchange=args.exchange, routing_key=f'{receiver_id}.{base_key}', body=f'BEGIN {args.table}')
# 
#     for line in sys.stdin:
#         key, data = line.split(' ', 1)
#         receiver_id = (int(key) + receivers - 1) % receivers
#         channel.basic_publish(exchange=args.exchange, routing_key=f'{receiver_id}.{base_key}', body=data)
# 
#     for receiver_id in range(receivers):
#         channel.basic_publish(exchange=args.exchange, routing_key=f'{receiver_id}.{base_key}', body='EOF')
# 
#     connection.close()
# 
# if __name__ == '__main__':
#     args = argparse.ArgumentParser()
#     args.add_argument('--rmq-host', required=True, help='The host of RabbitMQ')
#     args.add_argument('--receivers', type=int, required=True, help='How many nodes are receiving')
# #    args.add_argument('--partitions', type=int, required=True, help='How many partitions each receivers has')
#     args.add_argument('--exchange', required=True, help='The name of the exchange')
#     args.add_argument('--chunk-id', required=True, help='Chunk ID for correct routing')
#     args.add_argument('--table', required=True, help='The table that is currently sent')
#     args = args.parse_args()
#     send(args)
