import numpy as np
import helpers

codes = {
  'DP02_0068PE': 'percent_bachelors',
  'DP02_0062PE': 'percent_high_school',
  'DP03_0062E': 'median_income',
  'DP05_0001E': 'population',
  'DP05_0064PE': 'percent_white',
  'DP05_0065PE': 'percent_african_american',
  'DP05_0067PE': 'percent_asian',
  'DP05_0071PE': 'percent_latino',
  'DP05_0005PE': 'percent_under_5',
  'DP05_0006PE': 'percent_5_to_9'
}

tracts = helpers.call_acs(codes)
for key in codes.values():
  tracts[key] = tracts[key].clip(lower=0)
  tracts[key] = tracts[key].replace(0,np.nan)
tracts.to_parquet('./data/tracts.parquet')
tracts.to_csv('./data/tracts.csv', index=False)

