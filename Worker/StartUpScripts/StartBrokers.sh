#!/bin/bash
echo "Starting Cluster Manager Broker.................................."
ssh -o StrictHostKeyChecking=no cluster-mgr-canada 'bash  /home/ubuntu/brokers/ResetBrokers.sh' > /home/ubuntu/automation/logs/broker/cm.log &
sleep 5

echo "Starting Network Brokers.........................................."
ssh -o StrictHostKeyChecking=no cloud-canada 'bash  /home/ubuntu/brokers/ResetBrokers.sh' > /home/ubuntu/automation/logs/broker/cloud.log &
ssh -o StrictHostKeyChecking=no edge-canada 'bash  /home/ubuntu/brokers/ResetBrokers.sh' > /home/ubuntu/automation/logs/broker/edge1.log &
ssh -o StrictHostKeyChecking=no edge2-canada 'bash  /home/ubuntu/brokers/ResetBrokers.sh' > /home/ubuntu/automation/logs/broker/edge2.log &

