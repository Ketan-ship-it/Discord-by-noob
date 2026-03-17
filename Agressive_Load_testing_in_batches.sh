#!/usr/bin/env bash

URL="ws://10.114.190.72:9002"
CONNECTIONS=1000
BATCH=50

echo "Launching $CONNECTIONS connections in batches of $BATCH..."

for ((i=1;i<=CONNECTIONS;i++)); do
(
    wscat -c "$URL" >/dev/null 2>&1
) &

if (( $i % $BATCH == 0 )); then
    echo "Launched $i connections..."
    sleep 1
fi

done

echo "All connections launched."
wait