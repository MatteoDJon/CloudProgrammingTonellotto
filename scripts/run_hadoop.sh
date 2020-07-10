#!/bin/bash
KMEANS=/home/hadoop/CloudProgrammingTonellotto/kmeans

mvn clean -f "${KMEANS}/pom.xml"
mvn package -f "${KMEANS}/pom.xml"

n=$1
d=$2
k=$3

if [ "${n}" = "" ] || [ "${d}" = "" ] || [ "${k}" = "" ]; then
    n=1000
    d=3
    k=7
fi

inputfile="data_n=${n}_d=${d}_k=${k}.txt"
inputpath="data/${inputfile}"

outputfile="centroids_n=${n}_d=${d}_k=${k}.txt"
outputpath="hadoop/result/${outputfile}"

hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver ${d} ${k} ${inputpath} ${outputpath} ${n}

echo "*** Initial Centroids ***"
hadoop fs -cat UniformSampling/part-r-00000

echo "*** Final Centroids ***"
hadoop fs -cat ${outputpath}
