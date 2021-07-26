import pandas as pd
import numpy as np
import requests
import os
from io import StringIO
from dotenv import load_dotenv

def call_api(url, limit, select = None, where = None):
  load_dotenv()
  params = {
    '$limit': limit, 
    '$$app_token': os.getenv('SOCRATA_TOKEN')
  }
  if select is not None:
    params['$select'] = select
  if where is not None:
    params['$where'] = where
  request = requests.get(url, params=params)
  data = StringIO(request.text)
  return pd.read_csv(data)

def call_acs(codes):
  fip_to_boro = {
    '061': '1',
    '005': '2',
    '047': '3',
    '081': '4',
    '085': '5'
  }
  codes_as_string = ','.join(codes.keys())
  column_names = codes.copy()
  column_names['tract'] = 'censustract'
  column_names['county'] = 'boro'
  acs_key = 'e0994badabac7e1ae20aa58162d3d5ba220caf6c'
  url = f'https://api.census.gov/data/2019/acs/acs5/profile?get={codes_as_string}&for=tract:*&in=state:36&in=county:047,005,061,081,085&key={acs_key}'
  res = requests.get(url)
  data = res.json()
  raw = pd.DataFrame(data[1:], columns=data[0])
  raw = raw.rename(columns=column_names)
  raw['boro'] = raw['boro'].apply(lambda x: fip_to_boro[x])
  for col in codes.values():
    raw[col] = raw[col].astype(float)
  raw['borotract'] = raw['boro']+raw['censustract']
  return raw.drop(columns='state')

def join_count(left, right, on, label):
  left[on] = left[on].astype(str)
  right[on] = right[on].astype(str)
  count = pd.DataFrame(right[on].value_counts().reset_index())
  count.columns = [on, label]
  result = left.merge(count, how="left", on=on)
  result[label] = result[label].fillna(0)
  return result

# def add_per_columns(data, features, aggregates):
#   for a in aggregates:
#     for b in features:
#       data[f'{b}_per_{a}'] = data[b] / data[a]
#   return data