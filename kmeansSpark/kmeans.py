from __future__ import print_function

import sys
import random
import time
from argparse import ArgumentParser

import numpy as np

from pyspark.sql import SparkSession
from Point import Point
from pyspark import SparkContext
import os
from pprint import pprint

def parseVector(line):
    return np.array([float(x) for x in line.split(' ')])


def find_closest_point(p, centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(0, len(centers)):
        tempDist = p.distance(centers[i])
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex


def compare_centroids(old, new):
    if(len(old) != len(new)):
        return float('inf')
    tempError = 0.0
    # TODO for f, b in zip(foo, bar):
    for i in range(0, len(old)):
        oldCentroid = old[i]
        newCentroid = new[i]
        dist = oldCentroid.computeSquaredDistance(newCentroid)
        norm1 = oldCentroid.computeSquaredNorm()
        norm2 = newCentroid.computeSquaredNorm()
        minNorm = min(norm1, norm2)
        tempError += (dist / minNorm)
    print("----------------------------")
    print("Error:" + str(tempError))
    print("----------------------------")
    return tempError


def sum_two_points(point1, point2):
    return point1.sumPoint(point2)


def add_tuples_values(a, b):
    pointOne = a[0]
    pointTwo = b[0]
    resultPoint = pointOne
    resultPoint.sumPoint(pointTwo)
    sumCount = (a[1] + b[1])
    return resultPoint, sumCount

"""
def is_valid_file(parser, arg):
    if not os.path.isfile(arg):
        parser.error(f"The file {arg} does not exist")
    else:
        return arg

def is_valid_dir(parser, arg):
    if not os.path.isdir(arg):
        parser.error(f"The directory {arg} does not exist")
    else:
        return arg
"""


def hdfs_delete_path(spark, path):
    # without using 3rd party libraries
    sc = spark.sparkContext
    fs = (sc._jvm.org
          .apache.hadoop
          .fs.FileSystem
          .get(sc._jsc.hadoopConfiguration()))
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(path), True)

def is_valid_file(parser, arg):
    return arg

def is_valid_dir(parser, arg):
    return arg

if __name__ == "__main__":

    parser = ArgumentParser(
        "Spark k-Means",
        description="Execute the k-Means clustering algorithm")
    parser.add_argument("d", type=int, help="dimension of points")
    parser.add_argument("k", type=int, help="number of centers")
    parser.add_argument(
        "inputfile",
        help="input file",
        metavar="FILE",
        type=lambda f: is_valid_file(parser, f))
    parser.add_argument(
        "outputdir",
        help="output directory",
        type=lambda d: is_valid_dir(parser, d))
    parser.add_argument("n", type=int, help="number of points")

    args = parser.parse_args()

    K = args.k
    d = args.d
    data_path = args.inputfile
    output_path = args.outputdir
    n = args.n

    spark = SparkSession\
        .builder\
        .appName("PythonKMeans")\
        .getOrCreate()
    sc = SparkContext.getOrCreate()
    sc.addPyFile("./Point.py")

    lines = sc.textFile(data_path)

    currentTime = time.time()
    start_time = currentTime

    initial_centroids = [Point(line,d) for line in lines.takeSample(withReplacement=True, num=K)]

    sampling_end = time.time()

    # pprint(initial_centroids)
    # pprint([p.printPoint() for p in initial_centroids])
    # pprint([str(p) for p in initial_centroids])

    CONVERGENCE_THRESHOLD = 1e-2
    old_centroids = []
    new_centroids = initial_centroids
    iteration = 0
    MAX_ITERATIONS = 100

    parallelizedPoints = lines.map(lambda line: Point(line,d)).cache()

    while(compare_centroids(old_centroids, new_centroids) > CONVERGENCE_THRESHOLD and iteration < MAX_ITERATIONS):
        old_centroids = new_centroids
        closest = parallelizedPoints.map(
            lambda p: (find_closest_point(p, new_centroids), (p, 1)))
        '''
        print("---------------------------------")
        print("Mapper Output")
        for row in closest.collect():
            print(str(row[0]) + "," + row[1]
                  [0].printPoint() + "," + str(row[1][1]))
        print("---------------------------------")
        '''
        pointStats = closest.reduceByKey(add_tuples_values)
        '''
        print("---------------------------------")
        print("Reducer Output")
        for row in pointStats.collect():
            print(str(row[0]) + "," + row[1]
                  [0].printPoint() + "," + str(row[1][1]))
        print("---------------------------------")
        print("---------------------------------")
        print("New Centroids")
        '''
        newPoint = pointStats.map(
            lambda p: (
                p[0], p[1][0].getAverage(
                    p[1][1])))
        '''
        for row in newPoint.collect():
            print(str(row[0]) + "," + row[1].printPoint())
        print("--------------------------------")
        '''
        new_centroids = [i for i in range(K)]
        for row in newPoint.collect():
            new_centroids[row[0]] = row[1]
        iteration += 1

    # delete output directory (if any)
    hdfs_delete_path(spark, output_path)

    sc.parallelize([str(c) for c in new_centroids], 1).saveAsTextFile(output_path)

    end_time = time.time()

    spark.stop()

    metrics = {
        "total_exec_time": end_time - start_time,
        "sampling_exec_time": sampling_end - start_time,
        "kmeans_exec_time": end_time - sampling_end,
        "iterations": iteration,
        "centroids_last_movement": compare_centroids(old_centroids, new_centroids),
        "successful": compare_centroids(old_centroids, new_centroids) <= CONVERGENCE_THRESHOLD,
    }

    performance = "%.2f,%.2f,%.2f,%d,%.9f,%s\n" % (
        metrics["total_exec_time"],
        metrics["sampling_exec_time"],
        metrics["kmeans_exec_time"],
        metrics["iterations"],
        metrics["centroids_last_movement"],
        str(metrics["successful"]).lower(), )

    with open(f"spark_n={n}_d={d}_k={K}.csv", "a") as performance_file:
        performance_file.write(performance)
