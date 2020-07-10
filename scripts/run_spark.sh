#!/bin/bash
KMEANSSPARK=/home/hadoop/CloudProgrammingTonellotto/kmeansSpark

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

outputdir="centroids_n=${n}_d=${d}_k=${k}.d"
outputpath="spark/result/${outputdir}"

python3 ${KMEANSSPARK}/kmeans.py ${d} ${k} ${inputpath} ${outputpath} ${n}

echo "*** Final Centroids ***"
hadoop fs -cat ${outputpath}/part-00000

hadoop fs -rm -r ${outputpath}