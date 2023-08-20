from collections import defaultdict

import gzip
from pathlib import Path
import json
import pyarrow as pa
from pyarrow import json as pajson
from pyarrow import compute as pc
from pyarrow import parquet
import polars as pl
import pymarc

schema = json.load(Path("marc-schema.json").expanduser().open())

def parse_sub(val, field_name):
    sub = defaultdict(list)
    for d in val['subfields']:
        if len(d) > 1:
            raise IndexError("WHOA, MULTIPLE FIELDS")
        subname, subval = [*d.items()][0]
        if field_name not in schema['fields'] or subname not in schema['fields'][field_name]["subfields"] or \
           schema['fields'][field_name]["subfields"][subname]['repeatable']:
            sub[subname].append(subval)
        else:
            sub[subname] = subval
    for ind in 'ind1', 'ind2':
        try:
            sub[ind] = val[ind]
        except KeyError:
            pass
    return sub

def to_parquet(iterator, filename):
    with open("/tmp/f.json", "w") as fout:
        for i, d in enumerate(iterator):
            r = parse_dict_marc(d)

            fout.write(json.dumps(r) + "\n")
            if i % 100 == 0:
                print(i, end="\r")
    as_feather = pajson.read_json("/tmp/f.json")
    parquet.write_table(as_feather, dest,
                        compression = "zstd", compression_level = 10)
    Path("/tmp/f.json").unlink()


def parse_dict_marc(record):
    d = record
    r = defaultdict(list)
    r['leader'] = d['leader']
    for field_pair in d['fields']:
        if len(field_pair) > 1:
            print(field_pair)
            raise IndexError("HUH")
        field_name, val = [*field_pair.items()][0]  
        if isinstance(val, str):
            r[field_name] = val
            continue
        if not field_name in schema['fields'] or schema['fields'][field_name]['repeatable']:
            r[field_name].append(parse_sub(val, field_name))
        else:
            r[field_name] = parse_sub(val, field_name)