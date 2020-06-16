#!/bin/bash

runuser -l ubuntu -c "git pull"
runuser -l ubuntu -c "screen -dmS poller"
runuser -l ubuntu -c "screen -S poller -X stuff 'source activate^M'"
runuser -l ubuntu -c "screen -S poller -X stuff 'conda activate cerulean^M'"
runuser -l ubuntu -c "screen -S poller -X stuff 'cd /home/ubuntu/ceruleanserver^M'"
runuser -l ubuntu -c "screen -S poller -X stuff 'python ceruleanserver/poller.py^M'"
