# main.py
from collections import namedtuple
from datetime import datetime
from functools import partial


file_name = './nyc_parking_tickets_extract.csv'

with open(file_name) as f:
    column_headers = next(f).strip('\n').split(',')
    sample_data = next(f).strip('\n').split(',')

print(column_headers)
print(sample_data)

column_names = [header.replace(' ','_').lower() for header in column_headers]
print(column_names)
print(list(zip(column_names, sample_data)))
Ticket = namedtuple('Ticket',column_names)

# with open(file_name) as f:
#     next(f)
#     raw_data_row = next(f)
#
#
# print([raw_data_row])
def read_data():
    with open(file_name) as f:
        next(f)
        yield from f
raw_data = read_data()

def parse_int(value, *, default=None):
    try:
        return int(value)
    except ValueError:
        return default


# print(parse_int('test', default='not an interger'))
#
# print(parse_int(10, default='not an integer'))

def parse_date(value, *, default=None):
    date_format = '%m/%d/%Y'
    try:
        return datetime.strptime(value, date_format).date()
    except ValueError:
        return default


# print(parse_int('hello', default='N/A'))
# print(parse_date('3/28/2018'))
# print(parse_date('231212', default='N/A'))

def parse_string(value, *, default=None):
    try:
        cleaned = value.strip()
        if not cleaned:
            return default
        else:
            return cleaned
    except ValueError:
        return default


# print(parse_string('  helllo   '))
# print(parse_string('   ', default='N/A'))

column_parsers = (parse_int,
                  parse_string,
                  lambda x: parse_string(x, default=''),
                  partial(parse_string, default=''),
                  parse_date,
                  parse_int,
                  partial(parse_string, default=''),
                  parse_string,
                  lambda x: parse_string(x, default='')
                  )

def parse_row(row, *, default=None):
    fields = row.strip('\n').split(',')
    parsed_data = [func(field)
                   for func, field in zip(column_parsers, fields)]
    # return parsed_data
    if all(item is not None for item in parsed_data):
        return Ticket(*parsed_data)
    else:
        return default
rows = read_data()

print('-------')
for _ in range(5):
    row = next(rows)
    parsed_data = parse_row(row)
    print(parsed_data)

for row in read_data():
    parsed_row = parse_row(row)
    if parsed_row is None:
        print(list(zip(column_names, row.strip('\n').split(','))), end='\n\n')



def parsed_data():
    for row in read_data():
        parsed = parse_row(row)
        if parsed:
            yield parsed
parsed_rows =  parsed_data()

for _ in range(5):
    print(next(parsed_rows))


