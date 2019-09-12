# Summary

This tool can be used to ingest data into a cluster via RabbitMQ.


## Prerequisites

* RabbitMQ
* Python >3.6
* PSQL on multiple cluster instances
* Partitioned schema on the nodes such, that the partition key is hashed with a
  custom modulo function


## How to use

1. Have your partitions such, that the partition key can be used as routing
   key. For instance, on 2 datanodes with 4 partitions *in total*, ensure, that
   all data hitting node #1 with `key modulo 4 partitions` goes to partitions
   0 and 1, whereas on node #2 it would go to partitions 2 and 3.

2. On each node, start a receiver, e.g. assume you have 2 nodes, then you would
   start one receiver on each:

    ./receiver.py --rmq-dsn <rabbitmq-dsn> --receiver-id 0 --rpc-exchange myrpc --dsn <db-dsn>
    ./receiver.py --rmq-dsn <rabbitmq-dsn> --receiver-id 1 --rpc-exchange myrpc --dsn <db-dsn>

   Note the different receiver IDs.

3. Start the ingestion on the sender node with two receiving data nodes and for
   data coming from a textfile one would do:

    cat mydata | ./sender.py --rmq-dsn <rabbitmq-dsn> --receivers 2 \
        --exchange myexchange --rpc-exchange myrpc \
        --table <table-name> --db <dbname> \
        --data-source stdin
