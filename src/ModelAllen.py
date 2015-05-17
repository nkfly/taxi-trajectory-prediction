import sys
import math
polyline = []
destination = []
testData = []
LengthDistribution = []
digits = 2
digitsForDetail = 4
degreeToMeter = 111000

def init():
	for n in open('./train2.csv','r'):
		startingPoint = n.find('[[')+1
		if (startingPoint != 0) and (n[startingPoint:-3].count('[') > 1): #ignore trajectory w/o moving
			tempResult = []
			for m in range(n[startingPoint:-3].count('[')):
				tempResult.append(coordinateTransf(n[n.find('[',startingPoint):n.find(']',startingPoint)+1]))
				startingPoint = n.find(']',startingPoint)+1
			polyline.append(tempResult)
	for m in open('./test.csv','r'):
		startingPoint = n.find('[[')+1
		if startingPoint != 0:
			tempResult = []
			for m in range(n[startingPoint:-3].count('[')):
				tempResult.append(coordinateTransf(n[n.find('[',startingPoint):n.find(']',startingPoint)+1]))
				startingPoint = n.find(']',startingPoint)+1
			testData.append(tempResult)
	setDestination()
	setLengthDistribution()

def coordinateTransf(string): #"[1.233,2.445]"=>[1.23,2.45]
	temp = eval(string)
	result = [round(temp[0],digits),round(temp[1],digits)]
	return result

def setDestination(): #(coordinate of the destination,prob to reach this destination,which polyline reach this destination)
	for p in range(len(polyline)):
		changeOrNot = 1
		for l in destination:
			if l[0] == polyline[p][-1]:
				l[1] += 1
				l[2].append(p)
				changeOrNot *= 0
		if changeOrNot == 1: destination.append([polyline[p][-1],1,[p]])
	for d in range(len(destination)): destination[d][1] = round(float(destination[d][1])/len(polyline),digitsForDetail)

def BayesMethod(testList):
	candidateList = range(len(destination))
	for dtn in range(len(destination)):
		candidateList[dtn]=[0,[]]
		for p in destination[dtn][2]:
			matchOrNot = 1
			for location in testList:
				if not location in polyline[p]: matchOrNot *= 0
			if matchOrNot == 1:
				candidateList[dtn][0] += round(1.0/len(destination[dtn][2]),3)
				candidateList[dtn][1].append(p)
	tempResult = lengthMethod(candidateList,testList)
	for t in range(len(tempResult)):
		tempResult[t] = tempResult[t][0]
	return destination[tempResult.index(max(tempResult))]

def lengthMethod(candidateList,testList):
	for a in range(len(candidateList)):
		if candidateList[a][0] > 0:
			tempResult = range(len(candidateList[a][1]))
			for m in range(len(candidateList[a][1])):
				polylinem = polyline[candidateList[a][1][m]]
				tempResult[m] = lengthToProb(routeLength(polylinem[polylinem.index(testList[0]):]))
			candidateList[a][0] *= max(tempResult)
	return candidateList

def routeLength(List):
	lengthSquare = 0
	for n in range(len(List)-1): lengthSquare += (List[n][0]-List[n+1][0])**2+(List[n][1]-List[n+1][1])**2
	return math.sqrt(lengthSquare)

def setLengthDistribution():
	for pl in polyline: LengthDistribution.append(round(routeLength(pl),digitsForDetail))
	return LengthDistribution
"""
def listAverage(List):
	ans = sum(List)/len(List)
	return ans
"""
def lengthToProb(length):
	return float(LengthDistribution.count(round(length,digitsForDetail)))/len(LengthDistribution)

"""
def listMode(List):
	tempList =[]
	for m in List:
		tempList.append(round(m,int(digits/2)))
	candidate = [0,0]
	Seen = []
	for l in tempList:
		if l not in Seen:
			if tempList.count(l) >= candidate[1]: candidate=[l,tempList.count(l)]
			Seen.append(l)
	return candidate
"""

init()
Ans = range(len(testData))
for t in range(len(testData)):
	Ans[t] = BayesMethod(testData[t])
print Ans
