import pandas as pd
import helpers

lots = pd.read_parquet('./data/lots_joined.parquet')
all_lots = pd.read_parquet('./data/all_lots.parquet')
# violations = pd.read_csv('./data/violations.csv')
# complaints = pd.read_parquet('./data/complaints.parquet')
tracts = pd.read_parquet('./data/tracts.parquet')

lot_sum = lots[['borotract', 'no_entry_complaints', 'no_violation_complaints', 'violation_complaints', 'complaints', 'violations', 'no_entry_problems', 'no_violation_problems', 'violation_problems', 'res_area', 'units']].groupby(
    ['borotract'])[['no_entry_complaints', 'no_violation_complaints', 'violation_complaints', 'complaints', 'violations', 'no_entry_problems', 'no_violation_problems', 'violation_problems', 'res_area', 'units']].sum()
# all_lots_sum = all_lots[['borotract','units']].groupby(['borotract'])['units'].sum()
tracts = tracts.merge(lot_sum, how='left', on='borotract')
# tracts = tracts.merge(all_lots_sum, how='left', on='borotract')
all_borotract_units = all_lots.groupby(['borotract'])['units'].sum().reset_index()
all_borotract_units.borotract = all_borotract_units.borotract.astype(str)
all_borotract_units.rename(columns={'units':'all_units'},inplace=True)
tracts = tracts.merge(all_borotract_units, how='left', on="borotract")
tracts['complaints_per_unit'] = tracts.complaints / tracts.units
tracts['violations_per_unit'] = tracts.complaints / tracts.units
tracts['no_entry_complaints_per_unit'] = tracts.no_entry_complaints / tracts.units
tracts['no_violation_complaints_per_unit'] = tracts.no_violation_complaints / tracts.units
tracts['violation_complaints_per_unit'] = tracts.violation_complaints / tracts.units
tracts['violation_problems_per_unit'] = tracts.violation_problems / tracts.units
tracts['no_entry_problems_per_unit'] = tracts.no_entry_problems / tracts.units
tracts['no_violation_problems_per_unit'] = tracts.no_violation_problems / tracts.units
lot_count = pd.DataFrame(lots.borotract.value_counts().reset_index())
lot_count.columns = ['borotract','lots']
all_lot_count = pd.DataFrame(all_lots.borotract.value_counts().reset_index())
all_lot_count.columns = ['borotract','all_lots']
tracts = tracts.merge(lot_count, how='left', on="borotract")
tracts = tracts.merge(all_lot_count, how='left', on="borotract")
tracts['percent_lot_pre_1960'] = tracts.lots / tracts.all_lots * 100
tracts['pre_1960_units_per_lot'] = tracts.units / tracts.lots
tracts['all_units_per_lot'] = tracts.all_units / tracts.all_lots
tracts['percent_pre_1960'] = tracts.units / tracts.all_units * 100
tracts['persons_per_unit'] = tracts.population / tracts.all_units
median_age = lots.groupby(['borotract'])['built'].median().reset_index().rename(columns={'built':'median_age'})
median_age.borotract = median_age.borotract.astype(str)
median_value_per_sqft = lots.groupby(['borotract'])['value_per_sqft'].median().reset_index().rename(columns={'value_per_sqft':'median_value_per_sqft'})
median_value_per_sqft.borotract = median_value_per_sqft.borotract.astype(str)
median_sqft_per_unit = lots.groupby(['borotract'])['sqft_per_unit'].median().reset_index().rename(columns={'sqft_per_unit':'median_sqft_per_unit'})
median_sqft_per_unit.borotract = median_sqft_per_unit.borotract.astype(str)
tracts = tracts.merge(median_age, how='left', on="borotract").merge(median_value_per_sqft, how='left', on="borotract").merge(median_sqft_per_unit, how='left', on="borotract")
tracts.to_parquet('./data/tracts_joined.parquet')
tracts.to_csv('./data/tracts_joined.csv', index=False)
