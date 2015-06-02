import json

def coordinateTransf(point, digits): #"[1.233,2.445]"=>[1.23,2.45]
	result = str(round(point[0],digits))+str(round(point[1],digits))
	return result


# 1:305, 2:2732, 3:24966, 4:249482, 5:1239996

if __name__  == '__main__':
	digits = 3
	with open('./language_model_input', 'w') as w:
		with open('../data/train.csv', 'r') as f:
			f.readline()
			for line in f:
				entries = line.strip().split('"')
				tid = entries[1].strip('"')
				polylines = json.loads(entries[-2].strip('"'))

				w.write( ' '.join(  [ coordinateTransf(point, digits) for point in polylines ]    ) )
				w.write('\n')

