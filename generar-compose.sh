#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Uso: $0 <archivo-salida.yaml> <cantidad-clientes>"
    exit 1
fi

OUTPUT_FILE="$1"
CLIENT_COUNT="$2"

echo "Nombre del archivo de salida: $OUTPUT_FILE"
echo "Cantidad de clientes: $CLIENT_COUNT"

cat > "$OUTPUT_FILE" <<EOF
name: tp0
services:
  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
    networks:
      - testing_net
    volumes:
      - ./server/config.ini:/config.ini
EOF

for i in $(seq 1 "$CLIENT_COUNT"); do
    cat >> "$OUTPUT_FILE" <<EOF
  client${i}:
    container_name: client${i}
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID=${i}
      - CLI_LOG_LEVEL=DEBUG
    networks:
      - testing_net
    depends_on:
      - server
    volumes:
      - ./client/config.yaml:/config.yaml
      - ./.data/dataset/agency-${i}.csv:/agency.csv
EOF
done

cat >> "$OUTPUT_FILE" <<EOF
networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
EOF

echo "Archivo '$OUTPUT_FILE' generado con $CLIENT_COUNT clientes."
