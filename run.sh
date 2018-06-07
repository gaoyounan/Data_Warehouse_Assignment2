#!/bin/bash  
cd /home/ubuntu/assignment/a2/
echo 123 >> testFile  
source /etc/profile && python DB_A2_SocialSent.py >> ppp.out 2>&1
