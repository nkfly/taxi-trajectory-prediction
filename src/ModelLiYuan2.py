import os.path
import json
import numpy
import math
from sklearn.ensemble import RandomForestClassifier
import sys


def distance_measure(point1, point2):
	return math.sqrt((point1[0] - point2[0])**2 + (point1[1] - point2[1])**2)

def polyline_statistics(file_name):

	data = []
	with open(file_name, 'r') as f:
		f.readline()
		for line in f:
			entries = line.strip().split('"')
			polylines = json.loads(entries[-2])

			distance = 0.0
			for i in range(len(polylines)-1 ):
				distance += distance_measure(polylines[i], polylines[i+1])
			data.append(distance)


	return numpy.mean(data), numpy.std(data)



def coordinateTransf(point, digits): #"[1.233,2.445]"=>[1.23,2.45]
	result = str(round(point[0],digits))+str(round(point[1],digits))
	return result

def polylines_to_vector_representation(polylines, last_k_point):
	vector_representation = []
	vector_representation.extend(polylines[len(polylines)-last_k_point])

	for i in range(len(polylines) - last_k_point , len(polylines) - 1):
		vector_representation.append(polylines[i+1][0] - polylines[i][0])
		vector_representation.append(polylines[i+1][1] - polylines[i][1])


	return vector_representation


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



if __name__ == '__main__':
	test_file = '../data/test.csv'
	train_file = '../data/train.csv'

	if not os.path.exists(test_file) or not os.path.exists(train_file):
		print 'please put the data folder in the same layer as src'
		exit(-1)


	destination_to_label = {}
	label_to_point = {}
	digits = 3 # 1:305, 2:2732, 3:24966, 4:249482, 5:1239996

	X = []
	y = []

	point_num = 400
	train_keep_ratio = 0.8


	with open(train_file, 'r') as f:
		f.readline()
		for line in f:
			isOk, feature_vector, label = create_feature_vector(line, destination_to_label, label_to_point,point_num,train_keep_ratio)
			if not isOk:
				continue

			X.append(feature_vector)
			y.append(label)


	print 'training ...'
	clf = RandomForestClassifier(max_depth=15,n_estimators=100, n_jobs=20)
	# print len(X[1])
	clf.fit(X, y)

	X = []

	print 'prediction ...'
	with open(test_file, 'r') as f:
		f.readline()
		for line in f:
			isOk, feature_vector = create_feature_vector_test(line, point_num)
			if not isOk:
				print 'dangerous'
				continue

			X.append(feature_vector)
	# print len(X[1])
	predictions = clf.predict(X)


	index = 0
	print len(label_to_point)
	with open('answer3.csv', 'w') as w:
		w.write('TRIP_ID,LATITUDE,LONGITUDE\n')
		with open('cool_base.csv', 'r') as f:
			f.readline()
			for line in f:
				entries = line.strip().split(',')
				point =  label_to_point[predictions[index]]
				# print predictions[index]

				w.write(entries[0] + ',' + str(point[1]) + ',' + str(point[0]) + '\n')
				index += 1










