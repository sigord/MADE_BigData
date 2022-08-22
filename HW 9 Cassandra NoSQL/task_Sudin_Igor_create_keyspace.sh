#!/usr/bin/env bash
CASSANDRA_ENDPOINT=$1
KEYSPACE_NAME=$2
CQL_CREATE_KEYSPACE="CREATE KEYSPACE $KEYSPACE_NAME WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 2};"
CQL_DROP="DROP KEYSPACE IF EXISTS $KEYSPACE_NAME;"
echo $CQL_DROP|cqlsh $CASSANDRA_ENDPOINT
echo $CQL_CREATE_KEYSPACE|cqlsh $CASSANDRA_ENDPOINT
