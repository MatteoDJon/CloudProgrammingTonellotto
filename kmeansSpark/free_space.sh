#!/bin/bash

originalpath=$PWD

cd /tmp/hadoop-hadoop/nm-local-dir/usercache/hadoop/filecache && sudo rm -rf *
cd /opt/yarn/local/usercache/hadoop/filecache && sudo rm -rf *

# hdfs dfsadmin -safemode leave

# docker rmi $(docker images -a |  awk '{print $3}')
# docker rm $(docker ps -a |  awk '{print $1}')

df -h

cd ${originalpath}