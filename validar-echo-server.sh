#!/usr/bin/env bash

MSG="testing echo server"
SERVER_NAME="server"
NETWORK_NAME="tp0_testing_net"
PORT="12345"

RESPONSE="$(docker run --rm --network "${NETWORK_NAME}" busybox sh -c "echo -n '${MSG}' | nc ${SERVER_NAME} ${PORT}")"

if [[ "${RESPONSE}" == "${MSG}" ]]; then
  echo "action: test_echo_server | result: success"
  exit 1
else
  echo "action: test_echo_server | result: fail"
  exit 0
fi
