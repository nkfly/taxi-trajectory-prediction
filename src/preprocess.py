import csv
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
        # for i in xrange(len(tmp)):
        #     tmp[i] = coordinateTransf(tmp[i] ,digits)
        polylines.append(tmp)
    return metadata, polylines



