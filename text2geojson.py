#!/usr/bin/env python
""" Convert tab-separated geo census output to GeoJSON.

Run with --help flag for usage instructions.
"""

from sys import stdin, stdout, stderr
from csv import DictReader
from os.path import basename
from optparse import OptionParser
import simplejson

def parse_float(value, precision=None):
    # XXX: this is wrong
    return precision and round(float(value), precision) or float(value)

def make_feature(row, precision=None):
    return {'properties': row,
            'geometry':
            {'type': 'Point',
             'coordinates': [parse_float(row['Longitude'], precision), 
                             parse_float(row['Latitude'], precision)]}}

if __name__ == '__main__':

    parser = OptionParser(usage="""%%prog [options]
    """)
    parser.add_option('-o', '--output', dest='output',
                      help='Optional output filename, stdout if omitted.')
    parser.add_option('-i', '--indent', dest='indent',
                      help='Indentation string.')
    parser.add_option('-q', '--quiet', dest='verbose',
                      help='Be quieter than normal',
                      action='store_false')
    parser.add_option('-v', '--verbose', dest='verbose',
                      help='Be louder than normal',
                      action='store_true')
    parser.add_option('-p', '--precision', dest='precision', type='int',
                      help='Decimal precision for latitude and longitude values.')

    parser.set_defaults(indent='0', precision=5)

    options, args = parser.parse_args()

    input = len(args) and open(args.pop(0), 'r') or stdin
    output = options.output and open(options.output, 'w') or stdout
    indent = options.indent
    if indent.isdigit():
        indent = int(indent) * ' '

    rows = DictReader(input, dialect='excel-tab')
    features = []
    for row in rows:
        feature = make_feature(row, options.precision)
        features.append(feature)

    collection = {'type': 'FeatureCollection',
                  'features': features}
    print >> output, simplejson.dumps(collection, indent=indent)

