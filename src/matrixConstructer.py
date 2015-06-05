import numpy as np
import sys
sys.path.append("./")
import copy
import preprocess
# import preprocess.coordinateTransf

readData = preprocess.readData
coordinateTransf = preprocess.coordinateTransf

def posToClass(polylines, digits):
    classDict = {}
    destinationDict = {}
    keyList = []
    c = 0
    for i in xrange(len(polylines)):
        if(len(polylines[i])==0):
            continue
        trip =  copy.copy(polylines[i])
        start = coordinateTransf(trip[0], digits)
        key = str(start[0])+','+str(start[1])
        if key not in classDict.keys():
            classDict[key] = c
            keyList.append(key)
            c = c+1
        end = coordinateTransf(trip[len(trip)-1], digits)
        key = str(end[0])+','+str(end[1])
        if key not in classDict.keys():
            classDict[key] = c
            keyList.append(key)
            c = c+1
        if key not in destinationDict.keys():
            destinationDict[key] = 0
        destinationDict[key] = destinationDict[key]+1
    return classDict, keyList, destinationDict

def makeTrans(polylines, classDict, size, digits):
    trans = np.zeros((size, size), dtype=int)
    l = len(polylines)
    for i in xrange(l):
        if(len(polylines[i])==0):
                continue
        trip =  copy.copy(polylines[i])
        start = coordinateTransf(trip[0], digits)
        key1 = str(start[0])+','+str(start[1])
        end = coordinateTransf(trip[len(trip)-1], digits)
        key2 = str(end[0])+','+str(end[1])
        trans[classDict[key1]][classDict[key2]]+=1
    # trans = stochasticTrans(trans)
    # aperiodicTrans(trans,size)
    return trans

def outputMatrix(filename, matrix, size):
    fp = open(filename, 'w')
    for i in xrange(size):
        if sum(matrix[i])==0:
            continue
        print >>fp, i,
        for j in xrange(size):
            if matrix[i][j]>0:
                print >>fp, str(j)+':'+str(matrix[i][j]),
        print >>fp,''
    fp.close()

def outputkeyList(filename, keyList):
    fp = open(filename, 'w')
    l = len(keyList)
    for i in xrange(l):
        print >>fp, keyList[i]
    fp.close()

def main():
    for i in xrange(len(sys.argv)):
        if sys.argv[i]=='-i':
            filename = sys.argv[i+1]
        elif sys.argv[i]=='-o':
            output = sys.argv[i+1]
        elif sys.argv[i]=='-d':
            digits = int(sys.argv[i+1])
        elif sys.argv[i]=='-k':
            keyListFile = sys.argv[i+1]

    metadata, polylines = readData(filename, digits)
    print 'metadata',len(metadata),'polylines',len(polylines)
    classDict, keyList, destinationDict = posToClass(polylines, digits)
    print 'keyList',len(keyList),'classDict',len(classDict),'destinationDict',len(destinationDict)
    size = len(classDict)
    trans = makeTrans(polylines, classDict, size, digits)
    outputkeyList(keyListFile, keyList)
    outputMatrix(output, trans, size)



if __name__ == '__main__':
    main()
