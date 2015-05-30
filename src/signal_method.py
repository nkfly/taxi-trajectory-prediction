import json
import sys

def find_most_match(test_polylines, train_list):
	most_match_destination = None
	most_match_distance = sys.maxint
	index = 0
	for train_polylines in train_list: 
		if len(train_polylines) <= len(test_polylines):
			continue


		distance = calculate_distance(test_polylines, train_polylines)
		if distance < most_match_distance:
			most_match_distance = distance
			most_match_destination = train_polylines[-1]
	return most_match_destination



def calculate_distance(test_polylines, train_polylines):
	min_distance = sys.maxint
	for i in range(len(train_polylines) - len(test_polylines)):
		distance = 0.0
		for j in range(len(test_polylines)):
			distance += abs(test_polylines[j][0] - train_polylines[j+i][0])
			distance += abs(test_polylines[j][1] - train_polylines[j+i][1])

		if distance < min_distance:
			min_distance = distance
	return min_distance








if __name__ == '__main__':
	train_list = []
	with open('../data/train.csv', 'r') as f:
		f.readline()
		for line in f:
			entries = line.strip().split('"')
			train_polylines = json.loads(entries[-2].strip('"'))
			train_list.append(train_polylines)



	with open('cool_base.csv', 'w') as w:
		w.write('TRIP_ID,LATITUDE,LONGITUDE\n')
		with open('../data/test.csv', 'r') as f:
			f.readline()
			index = 0
			for line in f:
				entries = line.strip().split('"')
				tid = entries[1].strip('"')
				polylines = json.loads(entries[-2].strip('"'))
				most_match_destination = find_most_match(polylines, train_list)
				latitude = str(most_match_destination[0])
				longitude = str(most_match_destination[1])
				w.write(tid+','+latitude+','+longitude+'\n')

				print index
				index += 1




