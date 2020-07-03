#!/bin/bash
KMEANS=/home/hadoop/CloudProgrammingTonellotto/kmeans

mvn clean -f "${KMEANS}/pom.xml"
mvn package -f "${KMEANS}/pom.xml"
nohup hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver 3 7 input_data_d\=3_n\=100000.txt centroids_d\=3_n\=100000.txt > '/home/hadoop/test_log.log' &
hadoop fs -cat centroids_d\=3_n\=100000.txt
