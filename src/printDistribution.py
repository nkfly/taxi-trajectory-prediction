import math
import numpy
polylines = []
destinations = []
LengthDistribution = []
digits = 3
digitsForDetail = 4
groupAmount = 100
groupingUpbound=0.02
normalizationParameter = 10 #tune this one for bigger dataset


def init():
	for trajectoryData in open('./train2.csv','r').readlines():
		startingPoint = trajectoryData.find('[[')+1
		if (startingPoint != 0) and (trajectoryData[startingPoint:-3].count('[') > 1): #ignore trajectoryData w/o moving
			tempResult = []
			for nodeAmount in xrange(trajectoryData[startingPoint:-3].count('[')):
				tempResult.append(coordinateTransf(trajectoryData[trajectoryData.find('[',startingPoint):trajectoryData.find(']',startingPoint)+1]))
				startingPoint = trajectoryData.find(']',startingPoint)+1
			polylines.append(tempResult)


def coordinateTransf(string): #"[1.233,2.445]"=>[1.23,2.45]
	temp = eval(string)
	result = [round(temp[0],digits),round(temp[1],digits)]
	return result

def printLengthDistribution():
	setDestination()
	setLengthDistribution()
	LengthDistribution.sort()
	distribution = [0]*groupAmount
	for routeLengthNum in xrange(len(LengthDistribution)):
		for groupByX in range(groupAmount):
			if (LengthDistribution[routeLengthNum] <= (groupByX+1)*groupingUpbound/groupAmount) and (LengthDistribution[routeLengthNum] >= groupByX*groupingUpbound/groupAmount):
				distribution[groupByX] += 1
	for n in range(len(distribution)):
		distribution[n]= [1]*(int(distribution[n]/normalizationParameter))
		print distribution[n]

def setDestination(): #(coordinate of the destinations,prob to reach this destinations,which polylines reach this destinations)
	for p in xrange(len(polylines)):
		isNew = 1
		for destinationInDetail in destinations:
			if destinationInDetail[0] == polylines[p][-1]:
				destinationInDetail[1] += 1
				destinationInDetail[2].append(p)
				isNew *= 0
		if isNew == 1: destinations.append([polylines[p][-1],1,[p]])
	for d in xrange(len(destinations)): destinations[d][1] = round(float(destinations[d][1])/len(polylines),digitsForDetail)

def routeLength(List):
	lengthSquare = 0
	for n in xrange(len(List)-1): lengthSquare += (List[n][0]-List[n+1][0])**2+(List[n][1]-List[n+1][1])**2
	return math.sqrt(lengthSquare)

def setLengthDistribution():
	for polyline in polylines: LengthDistribution.append(round(routeLength(polyline),digitsForDetail))
	return LengthDistribution

def lengthToProb(length):
	return float(LengthDistribution.count(round(length,digitsForDetail)))/len(LengthDistribution)

init()
printLengthDistribution()
