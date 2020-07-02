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

    print("Parametri di ingresso k=" + str(K) + " d=" + str(d) + " path=" + data_path)

    lines = sc.textFile(data_path)
    
    currentTime = time.time()
    print("Tempo inizio algoritmo: " + str(currentTime))
    startTime = currentTime

    kPoints = [Point(line,d) for line in lines.takeSample(withReplacement=True, num=K)]

    minError = 1e-9
    oldCentroids = []
    newCentroids = kPoints
    count = 0
    maxIterations = 100

    parallelizedPoints = lines.map(lambda line: Point(line,d)).cache()

    while(compareCentroids(oldCentroids, newCentroids) > minError and count < maxIterations):
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
        count += 1

    currentTime = time.time() # to prevent FileAlreadyExistsException
    sc.parallelize([p.printPoint() for p in newCentroids], 1).saveAsTextFile(output_path)
    print("Tempo fine algoritmo: " + str(currentTime))
    print("Numero iterazioni: " + str(count) )
    print("Durata algoritmo: " + str(currentTime - startTime))
    spark.stop()
