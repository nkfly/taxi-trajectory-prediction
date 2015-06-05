import csv
import sys
sample = int(sys.argv[2])
c = 0
filename = sys.argv[1]
fp = open('trainSub'+str(sample)+'.csv', 'wb')
with open(filename, 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    for row in spamreader:
        print >>fp, row[len(row)-1]
        if c == sample: break
        c += 1
fp.close()
