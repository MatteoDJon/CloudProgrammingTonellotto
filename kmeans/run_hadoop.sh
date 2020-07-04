#!/bin/bash
KMEANS=/home/hadoop/fede/CloudProgrammingTonellotto/kmeans

mvn clean -f "${KMEANS}/pom.xml"
mvn package -f "${KMEANS}/pom.xml"

n=100000
d=3
k=13

inputfile="data_n=${n}_d=${d}_k=${k}.txt"
inputpath="data-stdev0.2/${inputfile}"

outputfile="result_n=${n}_d=${d}_k=${k}.txt"
outputpath="result-stdev0.2/${outputfile}"

hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver ${d} ${k} ${inputpath} ${outputpath}
# hadoop fs -cat ${outputpath}
