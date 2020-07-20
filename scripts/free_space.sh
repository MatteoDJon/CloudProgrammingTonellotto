#!/bin/bash

originalpath=$PWD

namenode=$(cat /etc/hostname | grep "hadoop-namenode")
if [ "${namenode}" = "hadoop-namenode" ]; then
    stop-yarn.sh
    stop-dfs.sh
    start-dfs.sh
    start-yarn.sh

    cd /opt/yarn/local/usercache/hadoop/filecache && rm -rf *
    cd /opt/hadoop/logs && rm -rf *

    hadoop fs -rm /mr-history/tmp/hadoop/*
    hadoop fs -rm /spark-logs/*

    hdfs dfsadmin -safemode leave
else

    cd /tmp/hadoop-hadoop/nm-local-dir/usercache/hadoop/filecache && rm -rf *
    cd /opt/hadoop/logs && rm -rf *
    hdfs dfsadmin -safemode leave
fi

df -h

cd ${originalpath}

# *** Some useful commands ***

# du -sh *

# find / -type d -name "usercache" -print 2>/dev/null

# docker rmi $(docker images -a | awk '{print $3}')
# docker rm $(docker ps -a | awk '{print $1}')

# rm -rf /opt/hadoop/logs/*

# hadoop fs -rm /mr-history/tmp/hadoop/*
# hadoop fs -rm /spark-logs/*

# hadoop fs -ls /spark-logs | awk '{print $5}' | awk '{sum += $1} END {print sum/(1024*1024*1024) " GiB"}'
# hadoop fs -ls /mr-history/tmp/hadoop | awk '{print $5}' | awk '{sum += $1} END {print sum/(1024*1024*1024) " GiB"}'
