#!/usr/bin/env python3

import argparse
import json
import shlex
import subprocess
import multiprocessing

import pika


class Receiver:
    def __init__(self, args):
        self.args = args
        self.receiver_id = args.receiver_id
        self.rpc_exchange = args.rpc_exchange
        self.rmq_dsn = args.rmq_dsn
        self.dsn = args.dsn

    def _get_connection(self):
        parameters = pika.URLParameters(self.rmq_dsn)
        return pika.BlockingConnection(parameters)

    def receive(self, request, event, value):
        connection = self._get_connection()
        channel = connection.channel()

        data_exchange = request['exchange']
        channel.exchange_declare(exchange=data_exchange, exchange_type='direct')
        queue_name = channel.queue_declare('', exclusive=True).method.queue
        
        comm_id = request['comm_id']
        channel.queue_bind(exchange=data_exchange, queue=queue_name, routing_key=f'{self.receiver_id}.{comm_id}')

        db = request['db']
        table = request['table']
        cmd = shlex.split(f'psql {self.dsn}/{db} -c "COPY {table} FROM STDIN WITH DELIMITER $$|$$"')
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE)

        try:
            p.wait(timeout=1)
        except subprocess.TimeoutExpired:
            pass

        value.value = p.returncode or 0
        event.set()

        if value.value > 0:
            # Early exit
            print(f'Could not start receiver {comm_id}: {p.stderr.read()}')
            return

        print(f'Started receiver {comm_id}.')
        for message in channel.consume(queue_name, auto_ack=True):
            if not message:
                continue

            _, _, body = message

            if body == b'EOF':
                p.stdin.write(b'\.\n')
                p.stdin.flush()
                p.stdin.close()
                break

            else:
                p.stdin.write(body)

        print(f'Stopped receiver {comm_id}.')
        channel.close()

    def fulfill_request(self, request):
        print(f'Received: {request}')
        value = multiprocessing.Value('i', 0)

        event = multiprocessing.Event()
        p = multiprocessing.Process(target=self.receive, args=(request, event, value))
        p.daemon = True
        p.start()

        # Wait for the process to be fully functional
        event.wait()
        return value.value

    def wait(self):
        connection = self._get_connection()
        channel = connection.channel()

        channel.exchange_declare(exchange=self.rpc_exchange, exchange_type='fanout')
        
        result = channel.queue_declare('', exclusive=True)
        channel.queue_bind(exchange=self.rpc_exchange, queue=result.method.queue)
        
        for message in channel.consume(result.method.queue, auto_ack=True):
            method, properties, body = message
 
            request = json.loads(body.decode('utf-8'))
            retval = self.fulfill_request(request)

            channel.basic_publish(exchange='', routing_key=properties.reply_to,
                                  properties=pika.BasicProperties(correlation_id=properties.correlation_id),
                                  body='STARTED' if retval == 0 else 'FAILED')


if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument('--rmq-dsn', required=True, help='The DSN of RabbitMQ')
    args.add_argument('--receiver-id', required=True, help='The ID of the receiver')
    args.add_argument('--rpc-exchange', required=True, help='The name of the RPC exchange')
    args.add_argument('--dsn', required=True, help='The DB to connect to')
    args = args.parse_args()
    Receiver(args).wait()
