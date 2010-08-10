from sys import stdout, stderr
from os import SEEK_SET, SEEK_CUR, SEEK_END
from csv import reader, writer, DictReader
from os.path import basename
from optparse import OptionParser
from urlparse import urlparse, urljoin
from cStringIO import StringIO
from httplib import HTTPConnection
from zipfile import ZipFile
from itertools import izip

class RemoteFileObject:
    """ Implement enough of this to be useful:
        http://docs.python.org/release/2.5.2/lib/bltin-file-objects.html
        
        Pull data from a remote URL with HTTP range headers.
    """

    def __init__(self, url, block_size=(16 * 1024)):
        # scheme://host/path;parameters?query#fragment
        (scheme, host, path, parameters, query, fragment) = urlparse(url)
        
        self.host = host
        self.rest = path + (query and ('?' + query) or '')

        self.offset = 0
        self.length = self.get_length()
        self.chunks = {}
        
        self.block_size = block_size

    def get_length(self):
        """
        """
        conn = HTTPConnection(self.host)
        conn.request('GET', self.rest, headers={'Range': '0-1'})
        length = int(conn.getresponse().getheader('content-length'))
        print >> stderr, length, 'bytes in', basename(self.rest)
        return length

    def get_range(self, start, end):
        """
        """
        headers = {'Range': 'bytes=%(start)d-%(end)d' % locals()}

        conn = HTTPConnection(self.host)
        conn.request('GET', self.rest, headers=headers)
        return conn.getresponse().read()

    def read(self, count=None):
        """ Read /count/ bytes from the resource at the current offset.
        """
        if count is None:
            # to the end
            count = self.length - self.offset

        out = StringIO()

        while count:
            chunk_offset = self.block_size * (self.offset / self.block_size)
            
            if chunk_offset not in self.chunks:
                range = chunk_offset, min(self.length, self.offset + self.block_size) - 1
                self.chunks[chunk_offset] = StringIO(self.get_range(*range))
                
                loaded = 100.0 * self.block_size * len(self.chunks) / self.length
                print >> stderr, '%.1f%%' % min(100, loaded), 'of', basename(self.rest)

            chunk = self.chunks[chunk_offset]
            in_chunk_offset = self.offset % self.block_size
            in_chunk_count = min(count, self.block_size - in_chunk_offset)
            
            chunk.seek(in_chunk_offset, SEEK_SET)
            out.write(chunk.read(in_chunk_count))
            
            count -= in_chunk_count
            self.offset += in_chunk_count

        out.seek(0)
        return out.read()

    def seek(self, offset, whence=SEEK_SET):
        """ Seek to the specified offset.
            /whence/ behaves as with other file-like objects:
                http://docs.python.org/lib/bltin-file-objects.html
        """
        if whence == SEEK_SET:
            self.offset = offset
        elif whence == SEEK_CUR:
            self.offset += offset
        elif whence == SEEK_END:
            self.offset = self.length + offset

    def tell(self):
        return self.offset

def file_choice(table):
    """
    """
    file_name, column_offset = None, 5
    
    for row in DictReader(open('SF1.txt', 'r'), dialect='excel-tab'):
        curr_file, curr_table, cell_count = row.get('File Name'), row.get('Matrix Number'), int(row.get('Cell Count'))
        
        if curr_file != file_name:
            file_name, column_offset = curr_file, 5
    
        if curr_table == table:
            print >> stderr, row.get('Name'), 'in', row.get('Universe')
            return file_name, column_offset, cell_count
        
        column_offset += cell_count

def geo_lines(path):
    """
    """
    u = urljoin('http://www2.census.gov/census_2000/datasets/', path)
    f = RemoteFileObject(u, 256 * 1024)
    z = ZipFile(f)
    n = z.namelist()
    
    assert len(n) == 1, 'Expected one file, not %d: %s' % (len(n), repr(n))
    
    for line in z.open(n[0]):
        data = [('LOGRECNO', 18, 25), ('SUMLEV', 8, 11), ('GEOCOMP', 11, 13),
                ('LATITUDE', 310, 319), ('LONGITUDE', 319, 329),
                ('STATE', 29, 31), ('COUNTY', 31, 34), ('TRACT', 55, 61),
                ('BLOCK', 62, 66), ('NAME', 200, 290)]

        data = dict( [(key, line[s:e].strip()) for (key, s, e) in data] )
        
        lat, lon = data['LATITUDE'], data['LONGITUDE']
        data['LATITUDE'] = (lat[0] + lat[1:-6].lstrip('0') + '.' + lat[-6:]).lstrip('+')
        data['LONGITUDE'] = (lon[0] + lon[1:-6].lstrip('0') + '.' + lon[-6:]).lstrip('+')
        
        yield data

def data_lines(path):
    """
    """
    u = urljoin('http://www2.census.gov/census_2000/datasets/', path)
    f = RemoteFileObject(u, 256 * 1024)
    z = ZipFile(f)
    n = z.namelist()
    
    assert len(n) == 1, 'Expected one file, not %d: %s' % (len(n), repr(n))
    
    for row in reader(z.open(n[0])):
        yield row

summary_levels = {'state': '040', 'county': '050', 'tract': '080', 'block': '101'}
states = {'Alabama': 'AL', 'Alaska': 'AK', 'American Samoa': 'AS', 'Arizona': 'AZ',
    'Arkansas': 'AR', 'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'District of Columbia': 'DC', 'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
    'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS', 'Kentucky': 'KY',
    'Louisiana': 'LA', 'Maine': 'ME', 'Marshall Islands': 'MH', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
    'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV', 'New Hampshire': 'NH',
    'New Jersey': 'NJ', 'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC',
    'North Dakota': 'ND', 'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA',
    'Puerto Rico': 'PR', 'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD',
    'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT', 'Virginia': 'VA',
    'Washington': 'WA', 'West Virginia': 'WV', 'Wisconsin': 'WI', 'Wyoming': 'WY'}

parser = OptionParser()

parser.set_defaults(summary_level='county', table='P1')

parser.add_option('-g', '--geography', dest='summary_level',
                  help='Geographic summary level, e.g. "state", "040".',
                  type='choice', choices=summary_levels.keys() + summary_levels.values())

parser.add_option('-t', '--table', dest='table',
                  help='Table ID, e.g. "P1", "P12A", "PCT1", "H1".')

parser.add_option('-s', '--state', dest='state',
                  help='Optional state, e.g. "Alaska", "District of Columbia".',
                  type='choice', choices=states.keys())

options, args = parser.parse_args()

if options.summary_level in summary_levels:
    options.summary_level = summary_levels[options.summary_level]

file_name, column_offset, cell_count = file_choice(options.table)

print >> stderr, options.summary_level, options.state, options.table, file_name, column_offset, cell_count
print >> stderr, '-' * 32

if options.state:
    dir_name = options.state.replace(' ', '_')
    state_prefix = states.get(options.state).lower()
    geo_path = 'Summary_File_1/%s/%sgeo_uf1.zip' % (dir_name, state_prefix)
    data_path = 'Summary_File_1/%s/%s000%s_uf1.zip' % (dir_name, state_prefix, file_name)

else:
    geo_path = 'Summary_File_1/0Final_National/usgeo_uf1.zip'
    data_path = 'Summary_File_1/0Final_National/us000%s_uf1.zip' % file_name

out = writer(stdout, dialect='excel-tab')

row = ['Summary Level', 'Geographic Component', 'State FIPS', 'County FIPS', 'Tract', 'Block', 'Name', 'Latitude', 'Longitude']
row += ['%s%03d' % (options.table, cell) for cell in range(1, cell_count + 1)]

out.writerow(row)

for (geo, data) in izip(geo_lines(geo_path), data_lines(data_path)):

    if geo['GEOCOMP'] == '00':
        # Geographic Component "00" means the whole thing,
        # not e.g. "01" for urban or "43" for rural parts.

        if geo['SUMLEV'] == options.summary_level:
            assert geo['LOGRECNO'] == data[4], 'Wah'
            
            row = [geo[key] for key in ('SUMLEV', 'GEOCOMP', 'STATE', 'COUNTY', 'TRACT', 'BLOCK', 'NAME', 'LATITUDE', 'LONGITUDE')]
            row += data[column_offset:column_offset + cell_count]
            
            out.writerow(row)
            stdout.flush()
