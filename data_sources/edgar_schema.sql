{% set nodes = nodes|int %}
{% set partitions = partitions|int %}
{% set node = node|int %}

DROP DATABASE IF EXISTS edgar;
CREATE DATABASE edgar;
\c edgar

CREATE EXTENSION swarm64da;

CREATE FUNCTION hash_swarm64_bigint(bigint, bigint) RETURNS bigint AS 'SELECT $1::bigint' LANGUAGE SQL IMMUTABLE STRICT;
CREATE OPERATOR CLASS swarm64_hash_op_class_bigint FOR TYPE bigint USING hash AS FUNCTION 2 hash_swarm64_bigint(bigint, bigint);

CREATE TABLE edgar(
    ts BIGINT
  , ip inet
  , "user" CHAR(3)
  , cik INT
  , accession CHAR(20)
  , doc VARCHAR(255)
  , code SMALLINT
  , size INT
  , idx BOOLEAN
  , norefer BOOLEAN
  , noagent BOOLEAN
  , find SMALLINT
  , crawler BOOLEAN
  , browser CHAR(3)
) PARTITION BY HASH (ts swarm64_hash_op_class_bigint);

{% for partition in range(partitions) %}

{% set modulus = nodes * partitions %}
{% set remainder = partition * nodes + node %}

CREATE FOREIGN TABLE edgar_prt_{{ remainder }} PARTITION OF edgar
FOR VALUES WITH (MODULUS {{ modulus }}, REMAINDER {{ remainder }})
SERVER swarm64da_server OPTIONS (
    optimized_columns 'ts'
  , optimization_level_target '900'
);

{% endfor %}
