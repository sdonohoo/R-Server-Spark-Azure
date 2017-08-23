#!/bin/bash
# ARGS: $1=username
set -e

echo "Changing to user folder ..."
cd /home/$1/
wait 

sudo apt install subversion

svn export https://github.com/sdonohoo/R-Server-Spark-Azure/trunk/RStudioServer

hadoop fs -mkdir /example/data/MRSSampleData
hadoop fs -copyFromLocal /usr/lib64/microsoft-r/3.3/lib64/R/library/RevoScaleR/SampleData/* /example/data/MRSSampleData

echo "Success"
