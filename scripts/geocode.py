import geopandas as gp
import pandas as pd

tracts = pd.read_parquet('./data/tracts_joined.parquet')
geo = gp.read_file('https://data.cityofnewyork.us/api/geospatial/fxpq-c8ku?method=export&format=GeoJSON')
geo = geo[['geometry','boro_ct2010']].rename(columns={'boro_ct2010': 'borotract'})
tracts =  geo.merge(tracts, on='borotract')

tracts.to_file('./data/tracts.geojson', driver="GeoJSON")
