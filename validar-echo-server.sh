#!/usr/bin/env bash

# Como usar: ./validar-echo-server.sh [mensaje]

set -euo pipefail

MSG=$1
SERVER_NAME="server"
NETWORK_NAME="tp0_testing_net"
PORT="12345"

# Verificar que el server esté corriendo
if ! docker ps --format '{{.Names}}' | grep -qx "${SERVER_NAME}"; then
  echo "action: test_echo_server | result: fail"
  exit 1
fi

# Esperar a que el server acepte conexiones (con un retry de 10)
# ATTEMPTS=10
# SLEEP_SECS=1
# READY=0
# for _ in $(seq 1 "${ATTEMPTS}"); do
#   if docker run --rm --network "${NETWORK_NAME}" busybox sh -c "nc -w 1 -z ${SERVER_NAME} ${PORT}" >/dev/null 2>&1; then
#     READY=1
#     break
#   fi
#   sleep "${SLEEP_SECS}"
# done

# if [[ "${READY}" -ne 1 ]]; then
#   echo "SERVER NO ACEPTONUNCA LAS CONEXIONES"
#   echo "action: test_echo_server | result: fail"
#   exit 1
# fi

RESPONSE="$(docker run --rm --network "${NETWORK_NAME}" busybox sh -c "echo -n '${MSG}' | nc -w 3 ${SERVER_NAME} ${PORT}" || true)"

if [[ "${RESPONSE}" == "${MSG}" ]]; then
  echo "action: test_echo_server | result: success"
  exit 0
else
  echo "action: test_echo_server | result: fail"
  exit 1
fi
