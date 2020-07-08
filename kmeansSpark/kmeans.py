from __future__ import print_function

import sys
import random
import time

import numpy as np
from pyspark.sql import SparkSession
from Point import Point
from pyspark import SparkContext


def parseVector(line):
    return np.array([float(x) for x in line.split(' ')])


def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(0, len(centers)):
        tempDist = p.distance(centers[i])
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex


def compareCentroids(old, new):
    if(len(old) != len(new)):
        return float('inf')
    tempError = 0.0
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


def sumTwoPoints(point1, point2):
    return point1.sumPoint(point2)


def add_tuples_values(a, b):
    pointOne = a[0]
    pointTwo = b[0]
    resultPoint = pointOne
    resultPoint.sumPoint(pointTwo)
    sumCount = (a[1] + b[1])
    return resultPoint, sumCount


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PythonKMeans")\
        .getOrCreate()
    sc = SparkContext.getOrCreate()
    sc.addPyFile("./Point.py")

    K = int(sys.argv[2])
    d = int(sys.argv[1])
    
    data_path = sys.argv[3]
    output_path = sys.argv[4]

    n = sys.argv[5]

    print("Parametri di ingresso k=" + str(K) + " d=" + str(d) + " path=" + data_path)

    lines = sc.textFile(data_path)
    
    currentTime = time.time()
    print("Tempo inizio algoritmo: " + str(currentTime))
    start_time = currentTime

    kPoints = [Point(line,d) for line in lines.takeSample(withReplacement=True, num=K)]

    sampling_end = time.time()

    print(kPoints)

    CONVERGENCE_THRESHOLD = 1e-5
    oldCentroids = []
    newCentroids = kPoints
    iteration = 0
    MAX_ITERATIONS = 100

    parallelizedPoints = lines.map(lambda line: Point(line,d)).cache()

    while(compareCentroids(oldCentroids, newCentroids) > CONVERGENCE_THRESHOLD and iteration < MAX_ITERATIONS):
        oldCentroids = newCentroids
        closest = parallelizedPoints.map(
            lambda p: (closestPoint(p, newCentroids), (p, 1)))
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
        newCentroids = [i for i in range(K)]
        for row in newPoint.collect():
            newCentroids[row[0]] = row[1]
        iteration += 1

    """
    filesystem = sc._jvm.org.apache.hadoop.fs.FileSystem
    fs = filesystem.get(sc._jsc.hadoopConfiguration())
    path = sc._jvm.org.apache.hadoop.fs.Path
    output_path_for_hadoop = sc._jvm.org.apache.hadoop.fs.Path(output_path)
    if (fs.exists(output_path_for_hadoop)):
        fs.delete(output_path_for_hadoop, True);
    sc.parallelize([p.printPoint() for p in newCentroids], 1).saveAsTextFile(output_path)
    """

    sc.parallelize([p.printPoint() for p in newCentroids], 1).saveAsTextFile(output_path)

    end_time = time.time() # to prevent FileAlreadyExistsException
    print("Tempo fine algoritmo: " + str(end_time))
    print("Numero iterazioni: " + str(iteration) )
    print("Durata algoritmo: " + str(end_time - start_time))

    spark.stop()

    metrics = {
        "total_exec_time": end_time - start_time,
        "sampling_exec_time": sampling_end - start_time,
        "kmeans_exec_time": end_time - sampling_end,
        "iterations": iteration,
        "centroids_last_movement": compareCentroids(oldCentroids, newCentroids),
        "successful": compareCentroids(oldCentroids, newCentroids) <= CONVERGENCE_THRESHOLD,
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
