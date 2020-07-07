#!/bin/bash

originalpath=$PWD

cd /tmp/hadoop-hadoop/nm-local-dir/usercache/hadoop/filecache && sudo rm -rf *
cd /opt/yarn/local/usercache/hadoop/filecache && sudo rm -rf *

# hdfs dfsadmin -safemode leave

# docker rmi $(docker images -a | awk '{print $3}')
# docker rm $(docker ps -a | awk '{print $1}')

namenode=$(cat /etc/hostname | grep "hadoop-namenode")
if [ "${namenode}" = "hadoop-namenode" ]; then
    stop-yarn.sh
    start-yarn.sh
else
    echo "Run me in the namenode to stop and restart yarn"
fi

df -h

cd ${originalpath}
