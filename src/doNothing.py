import json

if __name__ == '__main__':
	with open('simple_base.csv', 'w') as w:
		w.write('TRIP_ID,LATITUDE,LONGITUDE\n')
		with open('../data/test.csv', 'r') as f:
			f.readline()
			for line in f:

				entries = line.strip().split('"')
				print entries
				tid = entries[1].strip('"')
				polylines = json.loads(entries[-2].strip('"'))
				latitude = str(polylines[-1][0])
				longitude = str(polylines[-1][1])
				w.write(tid+','+latitude+','+longitude+'\n')


