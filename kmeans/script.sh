#!/bin/bash
KMEANS=/home/hadoop/CloudProgrammingTonellotto/kmeans

mvn clean -f "${KMEANS}/pom.xml"
mvn package -f "${KMEANS}/pom.xml"
hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver 6 9 data.txt centroids.txt
hadoop fs -cat centroids.txt