#!/bin/bash
KMEANS=/home/hadoop/fede/CloudProgrammingTonellotto/kmeans

mvn clean -f "${KMEANS}/pom.xml"
mvn package -f "${KMEANS}/pom.xml"
hadoop fs -rm -r output
hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.KMeans
hadoop fs -cat output/part-r-00000

