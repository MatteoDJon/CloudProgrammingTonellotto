import math

class Point:
        
    def __init__(self,line):
        self.d = 6
        self.point = []
        components = line.split(" ")
        if (len(components) != self.d):
            print("Dimension not correct")
        for i in range(0, len(components)):
            self.point.append(float(components[i]))

    def getDimension(self):
        return len(self.point)
        
    def getComponents(self):
        return self.point
    
    def sumPoint(self,point):
        if(self.getDimension()!=point.getDimension()):
            print("Dimension mismatch")
        otherPointComponents=point.getComponents()
        for i in range(0,self.d):
            self.point[i]+=otherPointComponents[i]

    def getAverage(self,pointInCluster):
	for i in range(0,len(self.point)):
            self.point[i]=(self.point[i]/pointInCluster)
	return self
    
    def computeSquaredDistance(self,point):
        otherPointComponents=point.getComponents()
        distance=0.0
        for i in range(0,len(self.point)):
            distance+=math.pow(self.point[i]-otherPointComponents[i],2)
        return distance
	
    def computeSquaredNorm(self):
	sum=0.0
	for i in range(0,len(self.point)):
	    sum+=(self.point[i]*self.point[i])
	return sum
	
    def distance(self,point):
	otherComponents=point.getComponents()
	distance=0
	for i in range(0,len(self.point)):
	    distance+=math.pow(self.point[i]-otherComponents[i],2)
	return math.sqrt(distance)
        
    def printPoint(self):
        textPoint=""
        for i in range(0,len(self.point)-1):
            textPoint+=str(self.point[i])
            textPoint+=","
	textPoint+=str(self.point[len(self.point)-1])
        return textPoint
        
    def comparePoint(self,otherPoint):
	otherComponents = otherPoint.getComponents()
	components=self.point
	if(len(otherComponents)!=len(components)):
		return False
	for i in range(0,len(otherComponents)):
		if(otherComponents[i]!=components[i]):
			return False
	return True	        


