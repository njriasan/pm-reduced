#!/usr/bin/env bash
#Configure web server

#Pull the latest changes from the repo
cd /usr/src/app && git pull origin master 

#TODO: start cron jobs
# change python environment
source activate emission-pm

# launch the webapp
./e-mission-py.bash pm.py
