import helpers

lots = helpers.call_api(
    'https://data.cityofnewyork.us/resource/64uk-42ks.csv',
    900000,
    'cd,ct2010,address,yearbuilt,resarea,unitsres,borocode,bbl,numbldgs,assesstot,bldgarea,latitude,longitude,xcoord,ycoord,landuse'
)
lots = lots.dropna(subset=['ct2010','yearbuilt','resarea','unitsres','bbl','numbldgs','assesstot','bldgarea','latitude','longitude','xcoord','ycoord','landuse'])
lots.ct2010 = lots.ct2010*100
to_str = ['borocode','ct2010','cd','bbl']
for key in to_str:
  lots[key] = lots[key].astype(int).astype(str)
lots['ct2010'] = lots.ct2010.str.zfill(6)
lots['borotract'] = lots.borocode + lots.ct2010
lots = lots.rename(columns={
    'ct2010': 'tract',
    'yearbuilt': 'built',
    'unitsres': 'units',
    'borocode': 'boro',
    'numbldgs': 'buildings',
    'assesstot': 'value',
    'bldgarea': 'total_area',
    'resarea': 'res_area',
    'xcoord':'x',
    'ycoord':'y',
    'landuse': 'land_use'
})
lots['value_per_sqft'] = lots.value / lots.total_area
lots['sqft_per_unit'] = lots.res_area / lots.units
lots = lots[(lots['units'] > 0) & (lots['buildings'] < 4) & (lots['land_use'] > 1)]
lots.to_parquet('./data/all_lots.parquet')
lots.to_csv('./data/all_lots.csv', index=False)
lots = lots[lots['built'] <= 1960]
lots.to_parquet('./data/lots.parquet')
lots.to_csv('./data/lots.csv', index=False)