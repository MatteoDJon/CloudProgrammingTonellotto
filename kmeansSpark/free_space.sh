#!/bin/bash

originalpath=$PWD

namenode=$(cat /etc/hostname | grep "hadoop-namenode")
if [ "${namenode}" = "hadoop-namenode" ]; then
    stop-yarn.sh
    start-yarn.sh

    cd /opt/yarn/local/usercache/hadoop/filecache && sudo rm -rf *

    hdfs dfsadmin -safemode leave
else

    cd /tmp/hadoop-hadoop/nm-local-dir/usercache/hadoop/filecache && sudo rm -rf *
fi

df -h

cd ${originalpath}

# *** Some useful commands ***

# hdfs dfsadmin -safemode leave

# docker rmi $(docker images -a | awk '{print $3}')
# docker rm $(docker ps -a | awk '{print $1}')

# find / -type d -name "usercache" -print 2>/dev/null

# du -sh *