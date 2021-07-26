import geopandas as gp
import pandas as pd

tracts = pd.read_parquet('./data/tracts_joined.parquet')
lots = pd.read_parquet('./data/lots_joined.parquet')
geo = gp.read_file('https://data.cityofnewyork.us/api/geospatial/fxpq-c8ku?method=export&format=GeoJSON')
geo = geo[['geometry','boro_ct2010']].rename(columns={'boro_ct2010': 'borotract'})
tracts =  geo.merge(tracts, on='borotract')
lots_geo = gp.read_file('./input/lots_clean.geojson').rename(columns={'BBL':'bbl'})
lots_geo['bbl'] = lots_geo['bbl'].astype(str)
lots = lots_geo.merge(lots, on='bbl')

tracts.to_file('./data/tracts.geojson', driver="GeoJSON")
lots.to_file('./data/lots.geojson', driver="GeoJSON")
