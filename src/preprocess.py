import numpy as np
import sys
#sys.path.append("./src")
import time
import csv
import copy
def coordinateTransf(temp, digits): #"[1.233,2.445]"=>[1.23,2.45]
    result = [round(temp[0],digits),round(temp[1],digits)]
    return result

def readData(filename, digits):
    polylines = []
    metadata = []
    f = open(filename, 'r')
    attr = ["TRIP_ID","CALL_TYPE","ORIGIN_CALL","ORIGIN_STAND","TAXI_ID","TIMESTAMP","DAY_TYPE","MISSING_DATA","POLYLINE"]
    for row in csv.DictReader(f):
        tempResult = []
        for i in xrange(8):
            tempResult.append(row[attr[i]])
        metadata.append(tempResult)
        tmp = eval(row[attr[8]])
        for i in xrange(len(tmp)):
            tmp[i] = coordinateTransf(tmp[i] ,digits)
        polylines.append(tmp)
    return metadata, polylines



def posToClass(polylines):
    classDict = {}
    destinationDict = {}
    keyList = []
    c = 0
    for i in xrange(len(polylines)):
        if(len(polylines[i])==0):
            continue
        trip =  copy.copy(polylines[i])
        start = coordinateTransf(trip[0], 2)
        key = str(start[0])+','+str(start[1])
        if key not in classDict.keys():
            classDict[key] = c
            keyList.append(c)
            c = c+1
        end = coordinateTransf(trip[len(trip)-1], 2)
        key = str(end[0])+','+str(end[1])
        if key not in classDict.keys():
            classDict[key] = c
            keyList.append(c)
            c = c+1
        if key not in destinationDict.keys():
            destinationDict[key] = 0
        destinationDict[key] = destinationDict[key]+1
    return classDict, keyList, destinationDict

def stochasticTrans(trans):
    for i in xrange(trans.shape[0]):
        s = np.sum(trans[i])
        if s>0 :
            trans[i,:] /= s
        else:
            trans[i,:] = 1.0/trans.shape[0]
    return trans

def aperiodicTrans(trans,size):
    trans *=0.85
    unit = np.ones((size, size))
    unit *=0.15/trans.shape[0]
    trans +=unit
    return trans

def makeTrans(polylines, classDict, size):
    trans = np.zeros((size, size), dtype=int)
    l = len(polylines)
    for i in xrange(l):
        if(len(polylines[i])==0):
                continue
        trip =  copy.copy(polylines[i])
        start = coordinateTransf(trip[0], 2)
        key1 = str(start[0])+','+str(start[1])
        end = coordinateTransf(trip[len(trip)-1], 2)
        key2 = str(end[0])+','+str(end[1])
        trans[classDict[key1]][classDict[key2]]+=1
    # trans = stochasticTrans(trans)
    # aperiodicTrans(trans,size)
    return trans

def outputMatrix(filename, matrix, size):
	fp = open(filename, 'w')
	for i in xrange(size):
		print >>fp, i,
		for j in xrange(size):
			if matrix[i][j]>0:
				print >>fp, str(j)+':'+str(matrix[i][j]),
		print >>fp,''

def main():
	filename = sys.argv[1]
	metadata, polylines = readData(filename, 3)
	print 'metadata',len(metadata),'polylines',len(polylines)
	classDict, keyList, destinationDict = posToClass(polylines)
	print 'keyList',len(keyList),'classDict',len(classDict),'destinationDict',len(destinationDict)
	size = len(classDict)
	trans = makeTrans(polylines, classDict, size)
	output = sys.argv[2]
	outputMatrix(output, trans, size)

if __name__ == '__main__':
    main()
