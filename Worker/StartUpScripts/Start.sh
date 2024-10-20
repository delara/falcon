#!/bin/bash
echo "starting brokers"
./StartBrokers.sh
sleep 3s
./StartWorkers.sh
