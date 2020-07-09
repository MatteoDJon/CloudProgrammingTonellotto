#!/bin/bash
KMEANS=/home/hadoop/fede/CloudProgrammingTonellotto/kmeans

mvn clean -f "${KMEANS}/pom.xml"
mvn package -f "${KMEANS}/pom.xml"

n=100000
d=7
k=13

inputfile="data_n=${n}_d=${d}_k=${k}.txt"
inputpath="data-stdev0.2/${inputfile}"

outputfile="centroids_n=${n}_d=${d}_k=${k}.txt"
outputpath="hadoop/result-stdev0.2/${outputfile}"

hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver ${d} ${k} ${inputpath} ${outputpath} ${n}

echo "*** Initial Centroids ***"
hadoop fs -cat UniformSampling/part-r-00000

echo "*** Final Centroids ***"
hadoop fs -cat ${outputpath}
