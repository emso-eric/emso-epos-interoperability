#!/bin/bash

while : ; do
  resp=$(ps -aux | grep python3 | grep geo2coverage)
  if [ ${#resp} -lt 1 ]; then
    echo "Launching API"
    nohup python3 geo2coverage.py > /dev/null 2>&1 &
  fi
  sleep 1
done