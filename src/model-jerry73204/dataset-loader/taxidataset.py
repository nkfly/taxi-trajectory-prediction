#!/usr/bin/env python2
import numpy
import struct

class TaxiDataset:
    def __init__(self, path):
        with open(path, 'rb') as ftrips:
            # load compact binary file via numpy.fromfile
            num_trips, max_trip_id_length = struct.unpack('i i', ftrips.read(8))
            dtype_trip = [('trip_id', 'S%s' % max_trip_id_length),
                          ('call_type', numpy.int32),
                          ('origin_call', numpy.int32),
                          ('origin_stand', numpy.int32),
                          ('taxi_id', numpy.int32),
                          ('timestamp', numpy.int32),
                          ('day_type', numpy.int32),
                          ('missing_data', numpy.int32),
                          ('polyline_size', numpy.int32),
                          ('polyline_index', numpy.int32)]
            self.trips = numpy.fromfile(file=ftrips, dtype=dtype_trip, count=num_trips)

            # extract coordinates for 'polylines' part
            ftrips.seek(8 + num_trips * (max_trip_id_length + 36))
            dtype_coordinates = [('longitude', numpy.float64), ('latitude', numpy.float64)]
            self.coordinates = numpy.fromfile(file=ftrips, dtype=dtype_coordinates)

    def __getitem__(self, key):
        index, item = key

        if item in ('polyline', 9):
            trip = self.trips[index]
            begin = trip[9]
            end = begin + trip[8]
            return self.coordinates[begin:end]

        return self.trips[index][item]
