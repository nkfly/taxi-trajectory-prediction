####weight of polyline similarity, calltype,....
########so far we normalize them as score/sum(socres)
####weight between "call type=B" and "origin stand"
########so far I assume weight(same call type=B)*2 = weight(same origin stand)
####feature to add: origin call
####How to combine sumOfpolylineSimilarityScore and sumOfCallTypeScore
########so far I sum them up
####Timestamp similarity
########similarity = 1/defference
import math
import numpy
polylines = []
destinations = []
testPolylineData = []
LengthDistribution = []
digits = 3
titles = []
metadata = []
testMetadata = []

def init():
	for trajectoryData in open('./trainSubset.csv','r'):#.readlines():
		startingPoint = trajectoryData.find('[[')+1
		startGettingMetadata = 1
		if (startingPoint != 0) and (trajectoryData[startingPoint:-3].count('[') > 1): #ignore trajectoryData w/o moving
			tempResult = []
			for metaDataNum in xrange(8):
				tempResult.append(trajectoryData[startGettingMetadata:trajectoryData.find('"',startGettingMetadata)])
				startGettingMetadata = trajectoryData.find(',',startGettingMetadata)+2
				if trajectoryData[startGettingMetadata-1]=='N':startGettingMetadata -= 1
			metadata.append(tempResult)
			tempResult = []
			for nodeAmount in xrange(trajectoryData[startingPoint:-3].count('[')):
				tempResult.append(coordinateTransf(trajectoryData[trajectoryData.find('[',startingPoint):trajectoryData.find(']',startingPoint)+1]))
				startingPoint = trajectoryData.find(']',startingPoint)+1
			polylines.append(tempResult)
	for testFileLine in open('./test.csv','r'):#.readlines():
		if testFileLine.find("[[") != -1: titles.append(testFileLine[1:testFileLine.index(",",1)-1])#store titles(e.g., T23) for file formatting
		startGettingPolyline = testFileLine.find('[[')+1
		startGettingMetadata = 1
		if startGettingPolyline != 0:
			tempResult = []
			for metaDataNum in xrange(8):
				tempResult.append(testFileLine[startGettingMetadata:testFileLine.find('"',startGettingMetadata)])
				startGettingMetadata = testFileLine.find(',',startGettingMetadata)+2
				if testFileLine[startGettingMetadata-1] == 'N': startGettingMetadata -= 1
			testMetadata.append(tempResult)
			tempResult = []
			for nodeAmount in xrange(testFileLine[startGettingPolyline:-3].count('[')):
				tempResult.append(coordinateTransf(testFileLine[testFileLine.find('[',startGettingPolyline):testFileLine.find(']',startGettingPolyline)+1]))
				startGettingPolyline = testFileLine.find(']',startGettingPolyline)+1
			testPolylineData.append(tempResult)
	setDestination()

def coordinateTransf(string): #"[1.233974,2.445438]"=>[1.23,2.45]
	temp = eval(string)
	result = [round(temp[0],digits),round(temp[1],digits)]
	return result

def isSameDirection(trajectory1,trajectory2):
	vector1 = [trajectory1[-1][0]-trajectory1[0][0],trajectory1[-1][-1]-trajectory1[0][-1]]
	vector2 = [trajectory2[-1][0]-trajectory2[0][0],trajectory2[-1][-1]-trajectory2[0][-1]]
	if vector1[0]*vector2[0]+vector1[1]*vector2[1]>0: return True
	else: return False

def setDestination(): #(coordinate of the destinations,prob to reach this destinations,which polylines reach this destinations)
	for polylineNum in xrange(len(polylines)):
		isNew = 1
		for destinationInDetail in destinations:
			if destinationInDetail[0] == polylines[polylineNum][-1]:
				destinationInDetail[1].append(polylineNum)
				isNew *= 0
		if isNew == 1: destinations.append([polylines[polylineNum][-1],[polylineNum]])

def BayesMethod(testPolylineData,testMetadata):
	scoreList = range(len(destinations))
	polylineSimilarityScore = polylineSimilarityScoring(testPolylineData)
	#callTypeScore = callTypeScoring(testMetadata)
	#dayTypescore = dayTypeScoring(testMetadata)
	#timeStampScore = timeStampScoring(testMetadata)
	for destinationNum in range(len(destinations)):
		sumOfpolylineSimilarityScore = 0
		#sumOfCallTypeScore = 0
		#sumOfDayTypeScore = 0
		#sumOfTimeStampScore = 0
		for trajectoryNum in destinations[destinationNum][1]:
			sumOfpolylineSimilarityScore += polylineSimilarityScore[trajectoryNum]
			#sumOfCallTypeScore += callTypeScore[trajectoryNum]
			#sumOfDayTypeScore += dayTypescore[trajectoryNum]
			#sumOfTimeStampScore += timeStampScore[trajectoryNum]
		scoreList[destinationNum] = sumOfpolylineSimilarityScore#+sumOfCallTypeScore+sumOfDayTypeScore+sumOfTimeStampScore
	return destinations[scoreList.index(max(scoreList))][0]

def polylineSimilarityScoring(testPolylineData):
	polylineSimilarityScore = [0]*len(polylines) #matchAmount is propotional to bayesian probability
	for node in testPolylineData:
		for polylineNum in xrange(len(polylines)):
			if node in polylines[polylineNum]: polylineSimilarityScore[polylineNum] += 1.0/len(polylines[polylineNum])/len(testPolylineData)
	for matchNum in xrange(len(polylineSimilarityScore)):
		if not isSameDirection(testPolylineData,polylines[matchNum]): polylineSimilarityScore[matchNum] = 0
	sumOfScore = sum(polylineSimilarityScore)
	if sumOfScore == 0: sumOfScore = 0.0000001
	for scoreNum in xrange(len(polylineSimilarityScore)): polylineSimilarityScore[scoreNum]/sumOfScore
	return polylineSimilarityScore
"""
def timeStampScoring(testMetadata):
	timeStampScore = [0]*len(polylines)
	for metadataNum in xrange(len(metadata)):
		timeStampScore[metadataNum] = 1.0/(int(testMetadata[5])-int(metadata[metadataNum][5]))
	sumOfScore = sum(timeStampScore)
	if sumOfScore == 0: sumOfScore = 0.0000001
	for scoreNum in xrange(len(timeStampScore)): timeStampScore[scoreNum]/sumOfScore
	return timeStampScore

def callTypeScoring(testMetadata):
	CallTypeSimilarityScore = [0]*len(polylines)
	if testMetadata[1] == 'A':
		for metadataNum in xrange(len(metadata)):
			if metadata[metadataNum][1] == 'A': CallTypeSimilarityScore[metadataNum] += 1
	elif testMetadata[1] == 'B':
		for metadataNum in xrange(len(metadata)):
			if metadata[metadataNum][1] == 'B': CallTypeSimilarityScore[metadataNum] += 1
			if metadata[metadataNum][3] == testMetadata[3]: CallTypeSimilarityScore[metadataNum] += 2
	elif testMetadata[1] == 'C':
		for metadataNum in xrange(len(metadata)):
			if metadata[metadataNum][1] == 'C': CallTypeSimilarityScore[metadataNum] += 1
	sumOfScore = sum(CallTypeSimilarityScore)
	if sumOfScore == 0: sumOfScore = 0.0000001
	for scoreNum in xrange(len(CallTypeSimilarityScore)): CallTypeSimilarityScore[scoreNum]/sumOfScore
	return CallTypeSimilarityScore

def dayTypeScoring(testMetadata):
	dayTypeSimilarityScore = [0]*len(polylines)
	for metadataNum in xrange(len(metadata)):
		if metadata[metadataNum][6] == testMetadata[6]: dayTypeSimilarityScore[metadataNum] += 1
	sumOfScore = sum(dayTypeSimilarityScore)
	if sumOfScore == 0: sumOfScore = 0.0000001
	for scoreNum in xrange(len(dayTypeSimilarityScore)): dayTypeSimilarityScore[scoreNum]/sumOfScore
	return dayTypeSimilarityScore
"""


def buildAnswerFile():
	answers = range(len(testPolylineData))
	for t in xrange(len(testPolylineData)):
		answers[t] = BayesMethod(testPolylineData[t],testMetadata[t])
	File = open('./ans.csv','w')
	File.write('TRIP_ID'+','+'LATITUDE'+','+'LONGITUDE'+'\n')
	for ans in xrange(len(answers)): File.write(str(titles[ans]) + ',' + str(answers[ans][0]) + ',' + str(answers[ans][1]) + '\n')
	File.close

init()
buildAnswerFile()