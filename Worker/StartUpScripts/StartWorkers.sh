#!/bin/bash

cloud_broker_ip="10.70.24.82"
edge_broker_ip="10.70.24.138"
edge2_broker_ip="10.70.24.135"
cm_ip="10.70.24.53"

echo "Starting cloud worker"
ssh -o StrictHostKeyChecking=no cloud-canada "java -jar /home/ubuntu/tm/Worker.jar W001 ${cloud_broker_ip} W001 ${cm_ip} linux ${cloud_broker_ip} ${cloud_docker_ip} 0" >  /home/ubuntu/automation/logs/worker/cloud.log &

echo "Starting edge worker"
ssh -o StrictHostKeyChecking=no edge-canada "java -jar /home/ubuntu/tm/Worker.jar W002 ${edge_broker_ip} W001 ${cm_ip} linux ${cloud_broker_ip} ${edge_docker_ip} 256" >  /home/ubuntu/automation/logs/worker/edge.log &

echo "Starting edge2 worker"
ssh -o StrictHostKeyChecking=no edge2-canada "java -jar /home/ubuntu/tm/Worker.jar W003 ${edge2_broker_ip} W001 ${cm_ip} linux ${cloud_broker_ip} ${edge2_docker_ip} 256" >  /home/ubuntu/automation/logs/worker/edge2.log &

