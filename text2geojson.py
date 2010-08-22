#!/usr/bin/env python
""" Convert tab-separated geo census output to GeoJSON.

Run with --help flag for usage instructions.
"""

from sys import stdin, stdout, stderr
from csv import DictReader
from os.path import basename
from optparse import OptionParser
from json import JSONEncoder
from re import compile

def make_feature(row, precision=None):
    return {'properties': row,
            'geometry':
            {'type': 'Point',
             'coordinates': (float(row['Longitude']),
                             float(row['Latitude']))}}

if __name__ == '__main__':

    parser = OptionParser(usage="""%prog [options]
    """)
    parser.add_option('-o', '--output', dest='output',
                      help='Optional output filename, stdout if omitted.')
    parser.add_option('-i', '--indent', dest='indent',
                      type='int', help='Optional number of spaces to indent.')
    parser.add_option('-p', '--precision', dest='precision', type='int',
                      help='Optional decimal precision for degree values.')
    parser.add_option('-q', '--quiet', dest='verbose',
                      help='Be quieter than normal',
                      action='store_false')
    parser.add_option('-v', '--verbose', dest='verbose',
                      help='Be louder than normal',
                      action='store_true')

    parser.set_defaults(indent=None, precision=5)

    options, args = parser.parse_args()

    input = len(args) and open(args.pop(0), 'r') or stdin
    output = options.output and open(options.output, 'w') or stdout
    indent = options.indent
    
    rows = DictReader(input, dialect='excel-tab')
    features = [make_feature(row, options.precision) for row in rows]

    collection = {'type': 'FeatureCollection',
                  'features': features}
    
    float_pat = compile(r'^-?\d+\.\d+$')
    float_fmt = '%%.%df' % options.precision
    encoded = JSONEncoder(indent=indent).iterencode(collection)

    for atom in encoded:
        if float_pat.match(atom):
            output.write(float_fmt % float(atom))
        else:
            output.write(atom)
