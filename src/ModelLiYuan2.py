import os.path
import json
import numpy
import math
from sklearn.ensemble import RandomForestClassifier
import sys
import datetime
import random


def coordinateTransf(point, digits): #"[1.233,2.445]"=>[1.23,2.45]
	result = str(round(point[0],digits))+str(round(point[1],digits))
	return result



def create_feature_vector(line, destination_to_label, label_to_point,point_num,train_keep_ratio):
	if 'True' in line: # missing data
		return False, None, None

	entries = line.strip().split('"')
	polylines = json.loads(entries[-2])

	if len(polylines) < 2: # dimension too low
		return False, None, None

	destination = coordinateTransf( polylines[-1], digits)
	if destination not in destination_to_label:
		destination_to_label[destination] = len(destination_to_label)
		label_to_point[destination_to_label[destination]] = [round(polylines[-1][0],digits),round(polylines[-1][1],digits)]

	len_to_keep = int(len(polylines)*train_keep_ratio)

	polylines = polylines[:len_to_keep]







	feature_vector = []

	if len_to_keep < point_num:
		for i in range(point_num - len_to_keep):
			feature_vector.append(0.0)
			feature_vector.append(0.0)
	else:
		len_to_keep = point_num
	
	for i in range(len(polylines) - len_to_keep, len(polylines)):
			feature_vector.append(polylines[i][0])
			feature_vector.append(polylines[i][1])



	return True, feature_vector, destination_to_label[destination]


def create_feature_vector_test(line, point_num):

	entries = line.strip().split('"')
	polylines = json.loads(entries[-2])

	feature_vector = []

	if len(polylines) < point_num:
		for i in range(point_num - len(polylines)):
			feature_vector.append(0.0)
			feature_vector.append(0.0)
	
	smaller = len(polylines) if len(polylines) < point_num else point_num

	for i in range(len(polylines) - smaller, len(polylines)):
			feature_vector.append(polylines[i][0])
			feature_vector.append(polylines[i][1])



	return True, feature_vector

def add_meta(line, feature_vector):
	entries = line.strip().split(',')
	call_type = entries[1].strip('"')

	if call_type == 'A':
		feature_vector.extend([1,0,0])
	elif call_type == 'B':
		feature_vector.extend([0,1,0])
	else:
		feature_vector.extend([0,0,1])

	timestamp = entries[5].strip('"')
	dtime = datetime.datetime.fromtimestamp(int(timestamp))
	weekday = dtime.weekday()

	for i in range(7):
		if weekday == i:
			feature_vector.append(1)
		else:
			feature_vector.append(0)

	hour = dtime.hour
	if hour >= 0 and hour <= 8:
		feature_vector.extend([1,0,0,0])
	elif hour <= 12:
		feature_vector.extend([0,1,0,0])
	elif hour <= 6:
		feature_vector.extend([0,0,1,0])
	else:
		feature_vector.extend([0,0,0,1])


	day_type = entries[6].strip('"')
	if day_type == 'A':
		feature_vector.extend([1,0,0])
	elif day_type == 'B':
		feature_vector.extend([0,1,0])
	else:
		feature_vector.extend([0,0,1])









if __name__ == '__main__':
	test_file = '../data/test.csv'
	train_file = '../data/train.csv'

	if not os.path.exists(test_file) or not os.path.exists(train_file):
		print 'please put the data folder in the same layer as src'
		exit(-1)


	
	digits = 3 # 1:305, 2:2732, 3:24966, 4:249482, 5:1239996

	# X = []
	# y = []

	point_num = 100
	train_keep_ratio = 0.7792


	answserDict = {}
	X = []
	y = []
	destination_to_label = {}
	label_to_point = {}
	with open(train_file, 'r') as f:
		f.readline()
		for line in f:
			entries = line.strip().split(',')
			timestamp = entries[5].strip('"')
			dtime = datetime.datetime.fromtimestamp(int(timestamp))
			weekday = dtime.weekday()


			isOk, feature_vector, label = create_feature_vector(line, destination_to_label, label_to_point,point_num,train_keep_ratio)
			if not isOk:
				continue


			add_meta(line, feature_vector)
			X.append(feature_vector)
			y.append(label)



	print 'training ... '
	clf = RandomForestClassifier(max_depth=15,n_estimators=10, n_jobs=20)
	# print len(X[1])
	clf.fit(X, y)

	X = []

	print 'prediction ...'

	with open(test_file, 'r') as f:
		f.readline()
		for line in f:
			entries = line.strip().split(',')
			timestamp = entries[5].strip('"')
			dtime = datetime.datetime.fromtimestamp(int(timestamp))
			weekday = dtime.weekday()



			isOk, feature_vector = create_feature_vector_test(line, point_num)
			if not isOk:
				print 'dangerous'
				continue

			add_meta(line, feature_vector)

			X.append(feature_vector)
	# print len(X[1])
	predictions = clf.predict(X)

	index = 0
	with open('answer.csv', 'w') as w:
		w.write('TRIP_ID,LATITUDE,LONGITUDE\n')
		with open('answer_sample.csv', 'r') as f:
			f.readline()
			for line in f:
				entries = line.strip().split(',')
				tid = entries[0].strip('"')
				point =  label_to_point[predictions[index]]
				# print label_to_point[index]

				w.write(entries[0] + ',' + str(point[1]) + ',' + str(point[0]) + '\n')
				index += 1










