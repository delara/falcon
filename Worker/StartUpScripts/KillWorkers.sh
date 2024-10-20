#!/bin/bash
echo "Killing Cloud worker in Canada"
ssh -o StrictHostKeyChecking=no cloud-canada 'pkill -9 -f Worker.jar'

echo "Killing Edge worker in Canada"
ssh -o StrictHostKeyChecking=no edge-canada 'pkill -9 -f Worker.jar'

echo "Killing Edge2 worker in Canada"
ssh -o StrictHostKeyChecking=no edge2-canada 'pkill -9 -f Worker.jar'
