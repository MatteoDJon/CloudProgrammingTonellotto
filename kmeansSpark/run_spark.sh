#!/bin/bash
KMEANSSPARK=/home/hadoop/fede/CloudProgrammingTonellotto/kmeansSpark

n=1000
d=3
k=7

inputfile="data_n=${n}_d=${d}_k=${k}.txt"
inputpath="data-stdev0.2/${inputfile}"

outputdir="spark_centroids_n=${n}_d=${d}_k=${k}.d"
outputpath="spark/result-stdev0.2/${outputfile}"

hadoop fs -rm -r ${outputpath}
python3 ${KMEANSSPARK}/kmeans.py ${d} ${k} ${inputpath} ${outputpath} ${n}
