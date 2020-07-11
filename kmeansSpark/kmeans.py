from __future__ import print_function

import sys
import time

from argparse import ArgumentParser
from pathlib import Path
from pyspark.sql import SparkSession
from Point import Point
from pyspark import SparkContext
import os


def find_closest_centroid(point, centroids):
    nearest_cluster_id = 0
    min_distance = float("inf")

    for i in range(0, len(centroids)):
        d = point.distance(centroids[i])
        if d < min_distance:
            min_distance = d
            nearest_cluster_id = i

    return nearest_cluster_id


def compare_centroids(old, new):
    if len(old) != len(new):
        return float("inf")

    max_distance_sq = 0.0
    for old_centroid, new_centroid in zip(old, new):
        distance_sq = old_centroid.computeSquaredDistance(new_centroid)
        max_distance_sq = max(max_distance_sq, distance_sq)

    return max_distance_sq


def sum_point_count_pair(a, b):
    p1 = a[0]
    p2 = b[0]
    sum_point = p1
    sum_point.sumPoint(p2)
    sum_count = a[1] + b[1]
    return sum_point, sum_count


def hdfs_delete(spark, path):
    # without using 3rd party libraries or subprocess
    sc = spark.sparkContext
    fs = (sc._jvm.org
          .apache.hadoop
          .fs.FileSystem
          .get(sc._jsc.hadoopConfiguration()))
    fs.delete(sc._jvm.org.apache.hadoop.fs.Path(path), True)


if __name__ == "__main__":

    parser = ArgumentParser(
        "Spark k-Means",
        description="Execute the k-Means clustering algorithm")
    parser.add_argument("d", type=int, help="dimension of points")
    parser.add_argument("k", type=int, help="number of centers")
    parser.add_argument(
        "inputfile",
        help="input file",
        metavar="FILE")
    parser.add_argument(
        "outputdir",
        help="output directory")
    parser.add_argument("n", type=int, help="number of points")

    args = parser.parse_args()

    k = args.k
    d = args.d
    input_data = args.inputfile
    output_path = args.outputdir
    n = args.n

    dot = os.path.dirname(os.path.abspath(__file__))

    spark = SparkSession\
        .builder\
        .appName("PythonKMeans")\
        .getOrCreate()
    sc = SparkContext.getOrCreate()
    sc.addPyFile(os.path.join(dot, "Point.py"))

    start_time = time.time()

    # read input data
    lines = sc.textFile(input_data)

    # parse a sample of k lines, use as initial centroids
    initial_centroids = [Point(line, d) for line in lines.takeSample(True, k)]

    sampling_end = time.time()

    # parse points and cache them
    parallelizedPoints = lines.map(lambda line: Point(line, d)).cache()

    old_centroids = []
    new_centroids = initial_centroids

    CONVERGENCE_THRESHOLD = 1e-4
    dist = float("inf")

    MAX_ITERATIONS = 100
    iteration = 0

    while dist > CONVERGENCE_THRESHOLD and iteration < MAX_ITERATIONS:
        old_centroids = new_centroids

        # <nearest_cluster_id, (point, 1)>
        closest = parallelizedPoints.map(
            lambda p: (find_closest_centroid(p, new_centroids), (p, 1)))

        # compute sum of points and count (key by key)
        pointStats = closest.reduceByKey(sum_point_count_pair)

        # <cluster_id, new_centroid>
        newPoint = pointStats.map(
            lambda p: (
                p[0], p[1][0].getAverage(
                    p[1][1])))

        new_centroids = [i for i in range(k)]
        for cluster_id, centroid in newPoint.collect():
            new_centroids[cluster_id] = centroid

        dist = compare_centroids(old_centroids, new_centroids)
        iteration += 1

    # delete output directory (if any)
    hdfs_delete(spark, output_path)

    # store results
    sc.parallelize([str(c) for c in new_centroids], 1).saveAsTextFile(output_path)

    end_time = time.time()

    spark.stop()

    # compute performance metrics
    metrics = {
        "total_exec_time": end_time - start_time,
        "sampling_exec_time": sampling_end - start_time,
        "kmeans_exec_time": end_time - sampling_end,
        "iterations": iteration,
        "centroids_last_movement": dist,
        "successful": dist <= CONVERGENCE_THRESHOLD,
    }

    # write performance to csv file
    with open(f"spark_n={n}_d={d}_k={k}.csv", "a") as performance_file:

        performance = "%.2f,%.2f,%.2f,%d,%.9f,%s\n" % (
            metrics["total_exec_time"],
            metrics["sampling_exec_time"],
            metrics["kmeans_exec_time"],
            metrics["iterations"],
            metrics["centroids_last_movement"],
            str(metrics["successful"]).lower(), )

        performance_file.write(performance)
