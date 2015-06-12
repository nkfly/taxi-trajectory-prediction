import csv
import numpy as np
from numpy.linalg import norm
import json
import sys
import argparse
from multiprocessing import Pool, Array, Process
import time


def main(args):
	parser = argparse.ArgumentParser()
	parser.add_argument("-t", type = str, default = './train.csv')
	parser.add_argument("-i", type = str, default = './test.csv')
	parser.add_argument("-o", type = str, default = './ans.csv')
	parser.add_argument("-d",type = int, default = 3)

	args = parser.parse_args(args)
	
	global trainFile
	trainFile = args.t
	global testFile
	testFile = args.i
	global output
	output = args.o
	global digits
	digits = args.d
	
	global GPSswitchDistance
	GPSswitchDistance = 0.01
	global abandonThreshold
	abandonThreshold = 0.8

	global polylines
	global testPolylines
	global metadata
	global testMetadata
	global destinations

	timePoint1 = time.time()

	metadata, polylines = parseFile(trainFile)	
	testMetadata, testPolylines = parseFile(testFile)

	timePoint2 = time.time()
	print timePoint2 - timePoint1

	destinations = setDestination(polylines)

	timePoint3 = time.time()
	print timePoint3 - timePoint2
	
	p = Pool()
	answer = p.map(BayesMethodCalculation,testPolylines)
	buildAnswerFile(answer)
	
	timePoint4 = time.time()
	print timePoint4 - timePoint3

def setDestination(polylines): #(coordinate of the destinations,prob to reach this destinations,which polylines reach this destinations)	
	global destinationListOriginal
	destinationListOriginal = [tuple(x[-1]) for x in polylines]
	destinations = list(set(destinationListOriginal))
	#for destinationNum in xrange(len(destinations)): destinations[destinationNum] = [np.array(destinations[destinationNum]),[i for i,x in enumerate(destinationListOriginal) if x == destinations[destinationNum]]]
	p=Pool()
	destinations = p.map(destinationMatching,destinations)
	p.close()
	return destinations

def destinationMatching(destination):
	global destinationListOriginal
	return [np.array(destination),[i for i,x in enumerate(destinationListOriginal) if x == destination]]

def parseFile(File):
	openedFile = csv.reader(open(File,'r'))
	next(openedFile)
	tmpMetadata = []
	tmpPolylines = []
	for line in openedFile:
		tmpMetadata.append(line[:-1])
		tmpPolylines.append(np.around(json.loads(line[-1]),digits))
	p = Pool()
	processedPolylines = p.map(preprocessing,tmpPolylines)
	p.close()
	return DeleteFalse(processedPolylines,tmpMetadata)

def preprocessing(polyline):
	if len(polyline)<2: return False
	
	#Errortype1: GPS error
	polylineOfNodes = [x+1 for x in range(len(polyline)-1)]
	errorLocations = []
	for nodeNum in polylineOfNodes:
		if norm(polyline[nodeNum]-polyline[nodeNum-1])>GPSswitchDistance:
			errorLocations.append(nodeNum)
			if len(errorLocations)>2: return False
	if len(errorLocations) == 2:
		errorLength = errorLocations[1]-errorLocations[0]
		correctVector = (polyline[errorLocations[1]]-polyline[errorLocations[0]-1])/(errorLength+1)
		startPoint = polyline[errorLocations[0]-1]
		for errNum in range(errorLength): polyline[errorLocations[0]+errNum] = startPoint+(errNum+1)*correctVector
	if len(errorLocations) == 1:
		if float(errorLocations[0])/len(polyline)>abandonThreshold:
			return False
			print "GPS error in the end"
		else: polyline = polyline[errorLocations[0]:]		
	
	#Errortype2: cycle polylines
	midPoint = polyline[len(polyline)/2]
	startToMid = midPoint-polyline[0]
	midToEnd = polyline[-1]-midPoint
	startToMidDotMidToEnd = np.dot(startToMid,midToEnd)
	if startToMidDotMidToEnd < -0.8*(norm(startToMid)*norm(midToEnd)): polyline = polyline[len(polyline)/2:]

	#Errortype3: start(or end) point = mid point
	elif startToMidDotMidToEnd == 0: return False
	return polyline

def DeleteFalse(polylines,metadata):
	for polylineNum in xrange(len(polylines)):
		if polylines[polylineNum] is False: metadata[polylineNum] = False
	return [m for m in metadata if m is not False],[p for p in polylines if p is not False]

def buildAnswerFile(answer):
	File = open(output,'w')
	File.write('TRIP_ID'+','+'LATITUDE'+','+'LONGITUDE'+'\n')
	for ans in xrange(len(answer)): File.write(str(testMetadata[ans][0]) + ',' + str(answer[ans][1]) + ',' + str(answer[ans][0]) + '\n')	
	File.close
	
def BayesMethodCalculation(testPolyline):
	scoreList = range(len(destinations))
	polylineSimilarityScore = polylineSimilarityScoring(testPolyline)
	#callTypeScore = callTypeScoring(testMetadata)
	#dayTypescore = dayTypeScoring(testMetadata)
	#timeStampScore = timeStampScoring(testMetadata)
	for destinationNum in xrange(len(destinations)):
		sumOfpolylineSimilarityScore = 0
		#sumOfCallTypeScore = 0
		#sumOfDayTypeScore = 0
		#sumOfTimeStampScore = 0
		for trajectoryNum in destinations[destinationNum][1]:		
			sumOfpolylineSimilarityScore += polylineSimilarityScore[trajectoryNum]
			#sumOfCallTypeScore += callTypeScore[trajectoryNum]
			#sumOfDayTypeScore += dayTypescore[trajectoryNum]
			#sumOfTimeStampScore += timeStampScore[trajectoryNum]
		scoreList[destinationNum] = sumOfpolylineSimilarityScore
		#scoreList[destinationNum] = sumOfpolylineSimilarityScore+sumOfCallTypeScore+sumOfDayTypeScore+sumOfTimeStampScore
	return destinations[scoreList.index(max(scoreList))][0]

def polylineSimilarityScoring(testPolyline):
	polylineSimilarityScore = [0]*len(polylines) #matchAmount is propotional to bayesian probability
	for polylineNum in xrange(len(polylines)):
		
		#train polyline must go the same way with test polyline
		if np.dot((testPolyline[-1]-testPolyline[0]),(polylines[polylineNum][-1]-polylines[polylineNum][0]))<=0: polylineSimilarityScore[polylineNum] = 0 
		
		#destination must be on the way of test polyline
		elif np.dot((testPolyline[-1]-testPolyline[0]),(polylines[polylineNum][0]-testPolyline[0]))<=0: polylineSimilarityScore[polylineNum] = 0
		
		else: polylineSimilarityScore[polylineNum] = len(list(node for node in testPolyline if node in polylines[polylineNum]))/float(len(polylines[polylineNum]))
	#sumOfScore = sum(polylineSimilarityScore)
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

main(sys.argv[1:])