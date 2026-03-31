#!/usr/bin/env bash

URL="ws://10.114.190.72:9002"
COUNT=500

echo "Testing $COUNT handshakes..."

start=$(date +%s)

for i in $(seq 1 $COUNT); do
(
    wscat -c "$URL" -x "ping" >/dev/null 2>&1
) &
done

wait

end=$(date +%s)

echo "Completed $COUNT connections in $((end-start)) seconds"