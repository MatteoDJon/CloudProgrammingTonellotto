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
KMEANSSPARK=/home/hadoop/CloudProgrammingTonellotto/kmeansSpark

# define hdfs input and output paths
inputfile="data_n=${n}_d=${d}_k=${k}.txt"
inputpath="data/${inputfile}"
outputdir="centroids_n=${n}_d=${d}_k=${k}.d"
outputpath="spark/result/${outputdir}"

# repeat
for i in {1..30}
do
    # free space and run command
    ./free_space.sh
    python3 ${KMEANSSPARK}/kmeans.py ${d} ${k} ${inputpath} ${outputpath} ${n}

    # check success
    RESULT=$?
    if [ $RESULT -eq 0 ]; then
        echo "Repetition ${i} succeded"
    else
        echo "Repetition ${i} failed"
    fi

    # backup and truncate nohup.out
    cp nohup.out "nohup-spark-${i}.out"
    echo "" > nohup.out

done
