#!/usr/bin/env python2
import sys
import numpy
from taxidataset import TaxiDataset

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: %s train_file test_file' % sys.argv[0])
        exit(1)

    # download packed binary file on http://wtf.csie.org/taxi-dataset.tar.xz and extract it
    # there comes 3 files: train.bin, test.bin, meta.bin
    train_dataset = TaxiDataset(sys.argv[1]) # load train.bin (for example)
    test_dataset = TaxiDataset(sys.argv[2])  # load test.bin  (for example)

    # indexing in (nth, item)
    trip_id = train_dataset[0, 'trip_id']
    call_type = train_dataset[0, 'call_type']
    origin_call = train_dataset[0, 'origin_call']
    origin_stand = train_dataset[0, 'origin_stand']
    taxi_id = train_dataset[0, 'taxi_id']
    timestamp = train_dataset[0, 'timestamp']
    day_type = train_dataset[0, 'day_type']
    missing_data = train_dataset[0, 'missing_data']
    polyline_size = train_dataset[0, 'polyline_size']
    polyline = train_dataset[0, 'polyline']

    print('# EXAMPLE: 1st record')
    print 'trip_id: %s' % trip_id
    print 'call_type: %s' % call_type
    print 'origin_call: %s' % origin_call
    print 'origin_stand: %s' % origin_stand
    print 'taxi_id: %s' % taxi_id
    print 'timestamp: %s' % timestamp
    print 'day_type: %s' % day_type
    print 'missing_data: %s' % missing_data
    print 'polyline_size: %s' % polyline_size
    print 'polyline: %s' % polyline
