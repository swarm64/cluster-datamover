#!/usr/bin/env python3

import argparse
import shlex
import subprocess
import multiprocessing

import pika


def receive(args, num_worker):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=args.rmq_host))
    channel = connection.channel()
    
    channel.exchange_declare(exchange=args.exchange, exchange_type='direct')
    
    result = channel.queue_declare('', exclusive=True)
    queue_name = result.method.queue
    
    receiver = args.receiver_id
    channel.queue_bind(exchange=args.exchange, queue=queue_name, routing_key=f'{receiver}.{num_worker}')
    
    p = None
    table = None
    print(f'Starting receiver {num_worker}.')
    delivery_tags = []
    for message in channel.consume(queue_name, auto_ack=True):
        if not message:
            continue

        method, properties, body = message
        if b'BEGIN-BEGIN-BEGIN' in body:
            table = body.decode('utf-8').split(' ')[1]

            print(f'{num_worker} opens PSQL connection to {table}')
            cmd = shlex.split(f'psql {args.dsn} -c "COPY {table} FROM STDIN WITH DELIMITER $$|$$"')
            p = subprocess.Popen(cmd, stdin=subprocess.PIPE)

        elif body == b'EOF-EOF-EOF':
            print(f'{num_worker} closes PSQL connection')
            p.stdin.write(b'\.\n')
            p.stdin.flush()
            p.stdin.close()
            p = None

        else:
            p.stdin.write(body)


if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument('--rmq-host', required=True, help='The host of RabbitMQ')
    args.add_argument('--receiver-id', required=True, help='The ID of the receiver')
    args.add_argument('--exchange', required=True, help='The name of the exchange')
    args.add_argument('--dsn', required=True, help='The DB to connect to')
    args.add_argument('--num-workers', required=True, type=int, help='How many worker processes to spawn')
    args = args.parse_args()

    with multiprocessing.Pool(processes=args.num_workers) as pool:
        p_args = [(args, idx) for idx in range(args.num_workers)]
        pool.starmap(receive, p_args)
