#!/bin/bash

points=(1000 10000 100000)
dimensions=(3 7)
centroids=(7 13)

for n in "${points[@]}"
do
    for d in "${dimensions[@]}"
    do
        for k in "${centroids[@]}"
        do
            # echo "data_n=${n}_d=${d}_k=${k}.txt"
            python3 clusters_generator.py ${n} ${d} ${k}
        done
    done
done
