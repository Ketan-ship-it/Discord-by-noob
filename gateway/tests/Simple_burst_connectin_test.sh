#!/usr/bin/env bash

URL="ws://10.114.190.72:9002"
CONNECTIONS=50

echo "Starting $CONNECTIONS WebSocket connections..."

for i in $(seq 1 $CONNECTIONS); do
    wscat -c "$URL" >/dev/null 2>&1 &
    sleep 0.02
done

wait