#!/bin/bash

source /etc/profile
source ~/.bash_profile

shdir=$PWD

cd $shdir/../


nohup python $shdir/../sync/sync_main.py >> logs/sync.out 2>&1 &


