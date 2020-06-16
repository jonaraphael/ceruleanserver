#!/bin/bash

runuser -l ubuntu -c "screen -dmS poller"
runuser -l ubuntu -c "screen -S poller -X stuff 'source activate^M'"
runuser -l ubuntu -c "screen -S poller -X stuff 'conda activate cerulean^M'"
runuser -l ubuntu -c "screen -S poller -X stuff 'cd /home/ubuntu/ceruleanserver^M'"
runuser -l ubuntu -c "screen -S poller -X stuff 'git pull https://cerulean-ec2:36dace444877f17f7a9789eb130541d69b915b95@github.com/jonaraphael/ceruleanserver.git deployment^M'"
runuser -l ubuntu -c "screen -S poller -X stuff 'python ceruleanserver/poller.py^M'"
