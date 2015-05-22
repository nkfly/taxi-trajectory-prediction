def coordinateTransf(string, digits): #"[1.233,2.445]"=>[1.23,2.45]
	temp = eval(string)
	result = [round(temp[0],digits),round(temp[1],digits)]
	return result

def readPolyline(filname='trainSub.csv', digits = 2):
	polyline = []
	for n in open(filname,'r'):
		startingPoint = n.find('[[')+1
		if (startingPoint != 0) and (n[startingPoint:-3].count('[') > 1): #ignore trajectory w/o moving
			tempResult = []
			for m in range(n[startingPoint:-3].count('[')):
				tempResult.append(coordinateTransf(n[n.find('[',startingPoint):n.find(']',startingPoint)+1], digits))
				startingPoint = n.find(']',startingPoint)+1
			polyline.append(tempResult)
	return polyline

def posToClass(polyline):
	classDict = {}
	destinationDict = {}
	i = 0
	for trip in polyline:
	    key = str(trip[0][0])+','+str(trip[0][1])
	    if key not in classDict.keys():
	        classDict[key] = i
	        i = i+1
	    key = str(trip[len(trip)-1][0])+','+str(trip[len(trip)-1][1])
	    if key not in classDict.keys():
	        classDict[key] = i
	        i = i+1
	    if key not in destinationDict.keys():
	    	destinationDict[key] = 0
	    destinationDict[key] = destinationDict[key]+1
	return classDict, destinationDict

