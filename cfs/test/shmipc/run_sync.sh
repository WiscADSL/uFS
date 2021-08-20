#!/bin/bash

ROUNDS=100
RING_SIZE=64
BIN_DIR=$1
XARGS=$2
${BIN_DIR}/shmipc_test_server -r${ROUNDS} -s${RING_SIZE} -c $XARGS &
sleep 2
${BIN_DIR}/shmipc_test_client -r${ROUNDS} -s${RING_SIZE} $XARGS &
wait
