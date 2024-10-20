#!/bin/bash

echo "Killing CLOUD broker in Canada"
ssh -o StrictHostKeyChecking=no cloud-canada 'bash  /home/ubuntu/brokers/KillBrokers.sh'


echo "Killing EDGE broker in Canada"
ssh -o StrictHostKeyChecking=no edge-canada 'bash  /home/ubuntu/brokers/KillBrokers.sh'

echo "Killing EDGE2 broker in Canada"
ssh -o StrictHostKeyChecking=no edge2-canada 'bash  /home/ubuntu/brokers/KillBrokers.sh'

echo "Killing Cluster Manager Broker............"
ssh -o StrictHostKeyChecking=no cluster-mgr-canada 'bash  /home/ubuntu/brokers/KillBrokers.sh'
