
import os
import sys
import re

from collections import namedtuple

def identity(value):
    return value

def round_2digits(value):
    if value:
        return round(float(value), 2)
    else:
        return 0.00

def round_lat_long(value):
    return round(float(value), 6)

def to_lower(value):
    if value:
        return value.lower()

Field = namedtuple('Field', ['name', 'converter'])

SCHEMAS = {
    1: (
        Field('vendor_id', identity),
        Field('tpep_pickup_datetime', identity),
        Field('tpep_dropoff_datetime', identity),
        Field('passenger_count', identity),
        Field('trip_distance', round_2digits),
        Field('pickup_longitude', round_lat_long),
        Field('pickup_latitude', round_lat_long),
        Field('rate_code_id', identity),
        Field('store_and_fwd_flag', to_lower),
        Field('dropoff_longitude', round_lat_long),
        Field('dropoff_latitude', round_lat_long),
        Field('payment_type', identity),
        Field('fare_amount', round_2digits),
        Field('extra', round_2digits),
        Field('mta_tax', round_2digits),
        Field('tip_amount', round_2digits),
        Field('tolls_amount', round_2digits),
        Field('total_amount', round_2digits)
    ),
    2: (
        Field('vendor_id', identity),
        Field('tpep_pickup_datetime', identity),
        Field('tpep_dropoff_datetime', identity),
        Field('passenger_count', identity),
        Field('trip_distance', round_2digits),
        Field('pickup_longitude', round_lat_long),
        Field('pickup_latitude', round_lat_long),
        Field('rate_code_id', identity),
        Field('store_and_fwd_flag', to_lower),
        Field('dropoff_longitude', round_lat_long),
        Field('dropoff_latitude', round_lat_long),
        Field('payment_type', identity),
        Field('fare_amount', round_2digits),
        Field('extra', round_2digits),
        Field('mta_tax', round_2digits),
        Field('tip_amount', round_2digits),
        Field('tolls_amount', round_2digits),
        Field('improvement_surcharge', round_2digits),
        Field('total_amount', round_2digits)
    )
}

class Taxi:
    def __init__(self, filepath):
        self.filepath = filepath

        year, month = [int(x) for x in re.search('([0-9]{4})-([0-9]{2})', self.filepath).groups()]
        if year < 2015:
            self.schema_version = 1
        elif year == 2015 or (year == 2016 and month < 7):
            self.schema_version = 2
        else:
            raise ValueError(f'No schema for year/month combination {year}/{month}')

        self.uuid = (((year - 2009) << 4) + month) << 54

    @staticmethod
    def transform(data, schema_version, uuid):
        schema = SCHEMAS[schema_version]
        data = [schema[idx].converter(value) for idx, value in enumerate(data)]

        for x in [data[5], data[6], data[9], data[10]]:
            if abs(x) > 1000:
                return None

        if schema_version == 1:
            # No improvement_surcharge in this version, set it to 0
            data.insert(17, 0.0)

        return {
            'trip_dates': [uuid, data[1], data[2]],
            'trip_pickup_locations': [uuid, data[5], data[6], 0],
            'trip_dropoff_locations': [uuid, data[9], data[10], 0],
            'trips': [
                uuid,
                data[1],
                data[2],
                data[5],
                data[6],
                data[9],
                data[10],
                data[3],
                data[4],
                data[7],
                data[11],
                data[12],
                data[13],
                data[14],
                data[15],
                data[16],
                data[17],
                data[18]
            ]
        }

    def get_line(self):
        def line_to_list(line):
            return line.rstrip().split(',')

        with open(self.filepath) as csv_file:
            # Skip the header
            csv_file.readline()
            lines = []
            n = 0
            for line in csv_file:
                data = line_to_list(line)
                if data == ['']:
                    continue

                try:
                    data = Taxi.transform(data, self.schema_version, self.uuid)
                except Exception as exc:
                    print(f'Ignoring error {exc}, data: {data}', file=sys.stderr)
                    continue

                if not data:
                    continue

                self.uuid += 1

                data = {key: [str(x) for x in item] for key, item in data.items()}
                data = {key: (item[0], f'{"|".join(item)}') for key, item in data.items()}

                yield data
