import os.path
import json
import numpy
import math
from sklearn import svm, datasets
from sklearn import cross_validation
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


def create_feature_vector(line, destination_to_label, dimension,train_keep_ratio):
	if 'True' in line: # missing data
		return False, None, None

	entries = line.strip().split('"')
	polylines = json.loads(entries[-2])

	destination = coordinateTransf( polylines[-1], digits)
	if destination not in destination_to_label:
		destination_to_label[destination] = len(destination_to_label)

	len_to_keep = len(polylines)*train_keep_ratio

	if len_to_keep

	feature_vector = []
	feature_vector.extend( polylines_to_vector_representation(polylines, last_k_point) )
	# feature_vector = polylines

	return True, feature_vector, destination_to_label[destination]



if __name__ == '__main__':
	test_file = '../data/test.csv'
	train_file = '../data/train.csv'

	if not os.path.exists(test_file) or not os.path.exists(train_file):
		print 'please put the data folder in the same layer as src'
		exit(-1)

	# train_mean, train_std = polyline_statistics(train_file)
	# test_mean, test_std = polyline_statistics(test_file)

	# print train_mean, train_std
	# print test_mean, test_std

	# print str(test_mean/train_mean * 100 ) + '%'

	maxLength = 0
	length2count = {}
	with open(train_file, 'r') as f:
		f.readline()
		for line in f:
			entries = line.strip().split('"')
			polylines = json.loads(entries[-2])

			length = len(polylines)

			if length > maxLength:
				maxLength = length

			if length not in length2count:
				length2count[length] = 0

			length2count[length] += 1


	with open(test_file, 'r') as f:
		f.readline()
		for line in f:
			entries = line.strip().split('"')
			polylines = json.loads(entries[-2])

			length = len(polylines)

			if length > maxLength:
				maxLength = length

			if length not in length2count:
				length2count[length] = 0

			length2count[length] += 1

	for length in sorted( length2count):
		print length, length2count[length]







	destination_to_label = {}
	digits = 2 # 1:305, 2:2732, 3:24966, 4:249482, 5:1239996

	X = []
	y = []

	dimension = 800
	train_keep_ratio = 0.8


	with open(train_file, 'r') as f:
		f.readline()
		for line in f:
			isOk, feature_vector, label = create_feature_vector(line, destination_to_label, dimension,train_keep_ratio)
			if not isOk:
				continue




	# # feature_vector_max = 0
	# # feature_vector_min = sys.maxint
	# # feature_vector_length_list = []

	# with open(train_file, 'r') as f:
	# 	header = [ column.strip('"') for column in f.readline().strip().split(',')]
	# 	print header
	# 	for line in f:
	# 		isOk, feature_vector, label = create_feature_vector(line, destination_to_label, last_k_point)
	# 		if not isOk:
	# 			continue
	# 		# feature_vector_len = len(feature_vector)

	# 		# if feature_vector_len > feature_vector_max:
	# 		# 	feature_vector_max = feature_vector_len
	# 		# if feature_vector_len < feature_vector_min:
	# 		# 	feature_vector_min = feature_vector_len

	# 		# feature_vector_length_list.append(feature_vector_len)
	# 		# print feature_vector
	# 		X.append(feature_vector)
	# 		y.append(label)
			


	# with open('train.svm', 'w') as w:
	# 	for i in range(len(X)):
	# 		w.write(str(y[i]) + ' ') 
	# 		feature_vector = X[i]
	# 		for j in range(1, len(feature_vector)):
	# 			w.write(str(j) + ':' + str(feature_vector[j]) + ' ')
	# 		w.write(str(len(feature_vector)) + ':' + str(feature_vector[-1]) + '\n')

	# print len(X)
	# X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size=0.2, random_state=0)
	# C = 1.0  # SVM regularization parameter
	# svc = svm.SVC(kernel='linear', C=C).fit(X_train, y_train)
	# score = clf.score(X_test, y_test)
	# print score
	# print feature_vector_max, feature_vector_min, numpy.mean(feature_vector_length_list), numpy.std(feature_vector_length_list)
	# print len(destination_to_label)



