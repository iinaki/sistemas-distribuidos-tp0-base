#!/usr/bin/env bash

set -euo pipefail

MSG="testing echo server"
SERVER_NAME="server"
NETWORK_NAME="tp0_testing_net"
PORT="12345"

# Verificar que el server esté corriendo
if ! docker ps --format '{{.Names}}' | grep -qx "${SERVER_NAME}"; then
  echo "action: test_echo_server | result: fail"
  exit 1
fi

RESPONSE="$(docker run --rm --network "${NETWORK_NAME}" busybox sh -c "echo -n '${MSG}' | nc -w 3 ${SERVER_NAME} ${PORT}" || true)"

if [[ "${RESPONSE}" == "${MSG}" ]]; then
  echo "action: test_echo_server | result: success"
  exit 0
else
  echo "action: test_echo_server | result: fail"
  exit 1
fi
