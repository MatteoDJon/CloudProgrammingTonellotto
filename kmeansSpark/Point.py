import math


class Point:

    def __init__(self, line, d):
        self.d = d
        self.point = None

        components = line.split(" ")
        if len(components) != d:
            return

        self.point = [float(c) for c in components]


    def getDimension(self):
        return self.d

    def getComponents(self):
        return self.point

    def sumPoint(self, point):
        if(self.d != point.getDimension()):
            print("Dimension mismatch")
        otherPointComponents = point.getComponents()
        for i in range(0, self.d):
            self.point[i] += otherPointComponents[i]

    def getAverage(self, pointInCluster):
        for i in range(0, self.d):
            self.point[i] = (self.point[i] / pointInCluster)
        return self

    def computeSquaredDistance(self, point):
        if(self.d != point.getDimension()):
            print("Dimension mismatch")
        otherPointComponents = point.getComponents()
        distance = 0.0
        for i in range(0, self.d):
            distance += math.pow(self.point[i] - otherPointComponents[i], 2)
        return distance

    def computeSquaredNorm(self):
        sum = 0.0
        for i in range(0, self.d):
            sum += (self.point[i] * self.point[i])
        return sum

    def distance(self, point):
        if(self.d != point.getDimension()):
            print("Dimension mismatch")
        otherComponents = point.getComponents()
        distance = 0
        for i in range(0, self.d):
            distance += math.pow(self.point[i] - otherComponents[i], 2)
        return math.sqrt(distance)

    def comparePoint(self, otherPoint):
        otherComponents = otherPoint.getComponents()
        components = self.point
        if(otherPoint.getDimension() != self.d):
            return False
        for i in range(0, self.d):
            if(otherComponents[i] != components[i]):
                return False
        return True

    def __str__(self):
        return " ".join(str(c) for c in self.point)

    def __repr__(self):
        return str(self)
