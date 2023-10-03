import json
import pyarrow as pa
from pathlib import Path

def marc_subfields_to_pa_fielddict(subfields, metadata={}):
  # Return 
  vals = []
  for k, v in subfields.items():
    # Everything in MARC is a string, even if it shouldn't be.
    dtype = pa.string()
    if v['repeatable']:
      # But some things are lists of strings.
      dtype = pa.list_(pa.string())
    vals.append(pa.field(k, dtype).with_metadata({
      **metadata,
      'label': v['label']
    }))
  return vals

def marc_to_indicator_fields(field):
  # Indicators are *dictionaries*--every value has a meaning
  # we know from the schema.
  # In Arrow, though, dictionary keys are ints: in MARC,
  # the keys are  
  # Since indicator values are only one character long,
  # we can use their ASCII codes secure in the knowledge that 
  # they'll never be larger than 127.

  indicator_fields = []
  for i_num in [1, 2]:
    if not f'indicator{i_num}' in field:
      continue
    if field[f'indicator{i_num}'] is None:
      continue
    code_meanings = [None for _ in range(127)]
    codes = field[f'indicator{i_num}']['codes']
    for k, v in codes.items():
      # ord to get the ascii code point.
      if (len(k)==3):
        start, end = k.split("-")
        for i in range(int(start), int(end) + 1):
          code_meanings[ord(str(i))] = v['label'].replace("Number of", str(i))
      else:
        code_meanings[ord(k)] = v['label']
    ind = pa.field(
      f'indicator{i_num}', pa.dictionary(pa.int8(), pa.string())
      ).with_metadata({'keys': json.dumps(code_meanings)})
    indicator_fields.append(ind)
  return indicator_fields


def marc_field_to_pa_field(field, tag):
  if tag == 'LDR':
    return pa.field(tag, pa.string())

  all_components = []
  indicators = marc_to_indicator_fields(field)
  try:
    subfields = marc_subfields_to_pa_fielddict(field['subfields'])
  except KeyError as e:
    if 'subfields' in str(e):
      subfields = []
    else:
      raise
  try:
    historical_subfields = marc_subfields_to_pa_fielddict(field['historical-subfields'], {'historical': True})
  except KeyError:
    historical_subfields = []
  if len(subfields) == 0:
    if len(indicators) > 0:
      # Just making sure there can't be indicators.
      raise ValueError("WHOA")
    field_type = pa.string()
  else:
    field_type = pa.struct(
      [
        *indicators,
        *subfields,
        *historical_subfields
      ]
    )
  if (field['repeatable']):
    field_type = pa.list_(field_type)

  pafield = pa.field(tag, field_type)
  return pafield.with_metadata({
    k: field[k] for k in ['label', 'url'] if k in field and field[k] is not None
   })

def jsonschema():
  schema = json.load((Path(__file__).parent / "marc-schema.json").expanduser().open())
  return schema

def build_schema():
  all = []
  fields = jsonschema()['fields']
  for k, field in fields.items():
    field = marc_field_to_pa_field(field, k)
    all.append(field)    
  return pa.schema(all)