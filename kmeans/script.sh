#!/bin/bash
mvn clean -f "/home/hadoop/CloudProgrammingTonellotto/kmeans/pom.xml"
mvn package -f "/home/hadoop/CloudProgrammingTonellotto/kmeans/pom.xml"
hadoop fs -rm -r output
hadoop jar /home/hadoop/CloudProgrammingTonellotto/kmeans/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.KMeans
