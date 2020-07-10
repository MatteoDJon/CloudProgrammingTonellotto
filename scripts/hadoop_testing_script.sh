#!/bin/bash
KMEANS=/home/hadoop/CloudProgrammingTonellotto/kmeans

mvn clean -f "${KMEANS}/pom.xml"
mvn package -f "${KMEANS}/pom.xml"

# d = 3 k = 7
hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver 3 7 input_data_d\=3_n\=1000.txt hadoop_centroids_d\=3_n\=1000_k\=7.txt > "${KMEANS}/testlog/d=3_n=1000_k=7.log" 
hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver 3 7 input_data_d\=3_n\=10000.txt hadoop_centroids_d\=3_n\=10000_k\=7.txt > "${KMEANS}/testlog/d=3_n=10000_k=7.log" 
hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver 3 7 input_data_d\=3_n\=100000.txt hadoop_centroids_d\=3_n\=100000_k\=7.txt > "${KMEANS}/testlog/d=3_n=100000_k=7.log" 

# d = 3 k = 13
hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver 3 13 input_data_d\=3_n\=1000.txt hadoop_centroids_d\=3_n\=1000_k\=13.txt > "${KMEANS}/testlog/d=3_n=1000_k=13.log" 
hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver 3 13 input_data_d\=3_n\=10000.txt hadoop_centroids_d\=3_n\=10000_k\=13.txt > "${KMEANS}/testlog/d=3_n=10000_k=13.log" 
hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver 3 13 input_data_d\=3_n\=100000.txt hadoop_centroids_d\=3_n\=100000_k\=13.txt > "${KMEANS}/testlog/d=3_n=100000_k=13.log" 
# d = 7 k = 13
hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver 7 13 input_data_d\=7_n\=1000.txt hadoop_centroids_d\=7_n\=1000_k\=13.txt > "${KMEANS}/testlog/d=7_n=1000_k=13.log" 
hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver 7 13 input_data_d\=7_n\=10000.txt hadoop_centroids_d\=7_n\=10000_k\=13.txt > "${KMEANS}/testlog/d=7_n=10000_k=13.log" 
hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver 7 13 input_data_d\=7_n\=100000.txt hadoop_centroids_d\=7_n\=100000_k\=13.txt > "${KMEANS}/testlog/d=7_n=100000_k=13.log"

# d = 7 k = 7
hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver 7 7 input_data_d\=7_n\=1000.txt hadoop_centroids_d\=7_n\=1000_k\=7.txt > "${KMEANS}/testlog/d=7_n=1000_k=7.log" 
hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver 7 7 input_data_d\=7_n\=10000.txt hadoop_centroids_d\=7_n\=10000_k\=7.txt > "${KMEANS}/testlog/d=7_n=10000_k=7.log" 
hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver 7 7 input_data_d\=7_n\=100000.txt hadoop_centroids_d\=7_n\=100000_k\=7.txt > "${KMEANS}/testlog/d=7_n=100000_k=7.log"
