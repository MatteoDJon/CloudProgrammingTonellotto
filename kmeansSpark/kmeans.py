
from __future__ import print_function

import sys
import random

import numpy as np
from pyspark.sql import SparkSession
from Point import Point
from pyspark import SparkContext


def parseVector(line):
    return np.array([float(x) for x in line.split(' ')])

def closestPoint(p, centers):
	bestIndex = 0
	closest = float("+inf")
	for i in range(0,len(centers)):
		tempDist=p.distance(centers[i])
		if tempDist < closest:
			closest = tempDist
			bestIndex = i
	return bestIndex

def compareCentroids(old,new):
	if(len(old)!=len(new)):
		return float('inf')
	tempError=0.0
	for i in range(0,len(old)):
		oldCentroid=old[i]
		newCentroid=new[i]
		dist = oldCentroid.computeSquaredDistance(newCentroid)
		norm1=oldCentroid.computeSquaredNorm()
		norm2=newCentroid.computeSquaredNorm()
		minNorm=min(norm1,norm2)
		tempError+=(dist/minNorm)
	print("----------------------------")
	print("Error:"+str(tempError))
	print("----------------------------")
	return tempError

def sumTwoPoints(point1,point2):
	return point1.sumPoint(point2)

def add_tuples_values(a,b):
	pointOne=a[0]
	pointTwo=b[0]
	resultPoint=pointOne
	resultPoint.sumPoint(pointTwo)
	sumCount=(a[1]+b[1])
	return resultPoint,sumCount

if __name__ == "__main__":
	spark = SparkSession\
		.builder\
		.appName("PythonKMeans")\
		.getOrCreate()
	sc=SparkContext.getOrCreate()
	sc.addPyFile("/usr/local/spark/examples/src/main/python/kmeansSpark/Point.py")

	lines = spark.read.text("/usr/local/spark/examples/src/main/python/kmeansSpark/data.txt").rdd.map(lambda r: r[0]).collect()
	points = []
	for line in lines:
		points.append(Point(line))

	K = int("5")
	#convergeDist = float("1.0")
	kPoints = []
	for i in range(0,K):
		kPoints.append(random.SystemRandom().choice(points))
	print("-------------------------------")
	print("Initial Centroids")
	for point in kPoints:
		print(point.printPoint())
	print("-------------------------------")
	minError = 0.20
	oldCentroids = []
	newCentroids = []
	newCentroids = kPoints
	parallelizedPoints=sc.parallelize(points)
	
	while(compareCentroids(oldCentroids,newCentroids)>minError):
		oldCentroids=newCentroids
		closest=parallelizedPoints.map(lambda p:(closestPoint(p,newCentroids),(p,1)))
		print("---------------------------------")
		print("Mapper Output")
		for row in closest.collect():
			print(str(row[0])+","+row[1][0].printPoint()+","+str(row[1][1]))
		print("---------------------------------")
		pointStats = closest.reduceByKey(add_tuples_values)
		print("---------------------------------")
		print("Reducer Output")
		for row in pointStats.collect():
			print(str(row[0])+","+row[1][0].printPoint()+","+str(row[1][1]))
		print("---------------------------------")
		print("---------------------------------")
		print("New Centroids")
		newPoint=pointStats.map(lambda p:(p[0],p[1][0].getAverage(p[1][1])))
		for row in newPoint.collect():
			print(str(row[0])+","+row[1].printPoint())
		print("--------------------------------")
		newCentroids=[]
		for row in newPoint.collect():
			newCentroids.append(row[1])
		#oldCentroids=newCentroids

	fileOutput=open("/usr/local/spark/examples/src/main/python/kmeansSpark/output.txt","w+")
	for point in newCentroids:
		fileOutput.write(point.printPoint()+"\n")
	fileOutput.close()
	spark.stop()