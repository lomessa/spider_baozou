#!/bin/bash

source /etc/profile
source ~/.bash_profile

shdir=$PWD

cd $shdir/../


nohup python $shdir/../crawler/crawler_youku.py >> logs/youku.out 2>&1 &


