import pandas as pd

lots = pd.read_parquet('./data/lots_joined.parquet')
complaints = pd.read_parquet('./data/complaints.parquet')
lot_features = ['built', 'res_area', 'units', 'bbl',
       'value', 'total_area', 'latitude', 'longitude',
       'value_per_sqft', 'sqft_per_unit', 'percent_bachelors', 'percent_high_school',
       'median_income', 'population', 'percent_white', 'percent_african_american',
       'percent_asian', 'percent_latino', 'percent_under_5', 'percent_5_to_9']
complaints = complaints.merge(lots[lot_features], on='bbl', how='left')
complaints = complaints.dropna(subset=['longitude','latitude','built'])
complaints.dropna(subset=lot_features)
# print('Complaints that resulted in violation')
# print(len(complaints[complaints.violation > 0]))
# print('Complaints that did not result in violation')
# print(len(complaints[complaints.no_violation > 0]))
# print('Complaints where inspector could not gain entry')
# print(len(complaints[complaints.no_entry > 0]))
complaints.to_parquet('./data/complaints_joined.parquet')
to_dt = ['status_date','received_date']
for feature in to_dt:
  complaints[feature] = complaints[feature].dt.strftime('%Y/%m/%d %H:%m')
complaints.to_csv('./data/complaints_joined.csv', index=False)
