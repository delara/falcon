#!/bin/bash
ssh -o StrictHostKeyChecking=no cloud-docker-canada 'docker rm -f $(docker ps -aq)' > /home/ubuntu/automation/logs/docker/cloud.log &
ssh -o StrictHostKeyChecking=no cloud-docker-canada 'docker system prune -f' > /home/ubuntu/automation/logs/docker/cloud.log &

ssh -o StrictHostKeyChecking=no edge-docker-canada 'docker rm -f $(docker ps -aq)' > /home/ubuntu/automation/logs/docker/edge.log &
ssh -o StrictHostKeyChecking=no edge-docker-canada 'docker system prune -f' > /home/ubuntu/automation/logs/docker/edge.log &

ssh -o StrictHostKeyChecking=no edge2-docker-canada 'docker rm -f $(docker ps -aq)' > /home/ubuntu/automation/logs/docker/edge2.log &
ssh -o StrictHostKeyChecking=no edge2-docker-canada 'docker system prune -f' > /home/ubuntu/automation/logs/docker/edge2.log &
