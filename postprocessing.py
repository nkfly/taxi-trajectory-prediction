

if __name__ == '__main__':
	with open('answer.csv', 'w') as w:
		w.write('TRIP_ID,LATITUDE,LONGITUDE\n')
		with open('cool_base.csv', 'r') as f:
			f.readline()
			for line in f:
				entries = line.strip().split(',')
				w.write(entries[0] + ',' + entries[2] + ',' + entries[1] + '\n')

