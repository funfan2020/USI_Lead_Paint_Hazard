import pandas as pd

lots = pd.read_parquet('./data/lots_joined.parquet')
complaints = pd.read_parquet('./data/complaints.parquet')
violations = pd.read_parquet('./data/violations.parquet')
lot_features = ['built', 'res_area', 'units', 'bbl',
       'value', 'total_area', 'value_per_sqft', 'sqft_per_unit', 'percent_bachelors', 'percent_high_school',
       'median_income', 'population', 'percent_white', 'percent_african_american',
       'percent_asian', 'percent_latino', 'percent_under_5', 'percent_5_to_9']

complaints = complaints.merge(lots[lot_features+['latitude', 'longitude']], on='bbl', how='left')
complaints = complaints.dropna(subset=['longitude','latitude','built'])
complaints.dropna(subset=lot_features)
complaints.to_parquet('./data/complaints_joined.parquet')

violations = violations.merge(lots[lot_features], on='bbl', how='left')
violations = violations.dropna(subset=['longitude','latitude','built'])
violations.dropna(subset=lot_features)
violations.to_parquet('./data/violations_joined.parquet')

to_dt = ['status_date','received_date']
for feature in to_dt:
  complaints[feature] = complaints[feature].dt.strftime('%Y/%m/%d %H:%m')
complaints.to_csv('./data/complaints_joined.csv', index=False)

to_dt = ['inspection_date','approved_date','original_certify_by_date','original_correct_by_date','new_certify_by_date','new_correct_by_date','certified_date','nov_issued_date','current_status_date']
for feature in to_dt:
  violations[feature] = violations[feature].dt.strftime('%Y/%m/%d %H:%m')
violations.to_csv('./data/violations_joined.csv', index=False)
