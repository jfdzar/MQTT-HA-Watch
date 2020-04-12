#!/bin/bash
# This bash script is generate to be executed in a cronjob
# Goal is to check if the process is runnning and execute it if not

echo "MQTT-HA-Watch started"
PROCESS_NUM=$(ps -ef | grep "python3 `dirname "$0"`/MQTT-HA-Watch.py" | grep -v `basename $0` | grep -v "grep" | wc -l)
ts=`date +%T`

if [ $PROCESS_NUM -gt 0 ]; then
        echo "$ts: Process is running"
else
        echo "$ts: Process is not running. Starting..."
        python3 "`dirname "$0"`/MQTT-HA-Watch.py &

fi