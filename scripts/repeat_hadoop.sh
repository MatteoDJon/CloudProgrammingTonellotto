#!/bin/bash

n=$1
d=$2
k=$3

if [ "${n}" = "" ] || [ "${d}" = "" ] || [ "${k}" = "" ]; then
    n=1000
    d=3
    k=7
fi

# define root directory
KMEANS=/home/hadoop/CloudProgrammingTonellotto/kmeans

# package
mvn clean -f "${KMEANS}/pom.xml"
mvn package -f "${KMEANS}/pom.xml"

# define hdfs input and output paths
inputfile="data_n=${n}_d=${d}_k=${k}.txt"
inputpath="data/${inputfile}"
outputfile="centroids_n=${n}_d=${d}_k=${k}.txt"
outputpath="hadoop/result/${outputfile}"

# repeat
for i in {1..2}
do
    # cmd to run
    hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver ${d} ${k} ${inputpath} ${outputpath} ${n}

    # check success
    RESULT=$?
    if [ $RESULT -eq 0 ]; then
        echo "Repetition ${i} succeded"
    else
        # free some space and retry
        ./free_space.sh
        echo "Retrying repetition ${i}"
        hadoop jar ${KMEANS}/target/KMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.Driver ${d} ${k} ${inputpath} ${outputpath} ${n}
    fi
done
