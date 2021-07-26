import pandas as pd
import helpers

lots = pd.read_parquet('./data/lots.parquet')
violations = pd.read_csv('./data/violations.csv')
complaints = pd.read_parquet('./data/complaints.parquet')
problems = pd.read_parquet('./data/problems.parquet')
tracts = pd.read_parquet('./data/tracts.parquet')


lots = helpers.join_count(lots, violations, 'bbl', 'violations')
lots['has_violation'] = lots.apply(
    lambda x: 1 if x.violations > 0 else 0, axis=1)
lots['violations_per_unit'] = lots.violations / lots.units

complaint_pivot = complaints[['bbl', 'no_entry', 'no_violation', 'violation', 'no_entry_problems', 'no_violation_problems', 'violation_problems']].groupby(
    ['bbl'])['no_entry', 'no_violation', 'violation', 'no_entry_problems', 'no_violation_problems', 'violation_problems'].sum()
lots = lots.merge(complaint_pivot, how='left', on="bbl")
lots = helpers.join_count(lots, complaints, 'bbl', 'complaints')
# lots = helpers.join_count(lots, problems, 'bbl', 'problems')
lots = lots.rename(columns={
    'no_entry': 'no_entry_complaints',
    'no_violation': 'no_violation_complaints',
    'violation': 'violation_complaints'
})
lots['complaints_per_unit'] = lots.complaints / lots.units
lots['no_entry_complaints_per_unit'] = lots.no_entry_complaints / lots.units
lots['no_violation_complaints_per_unit'] = lots.no_violation_complaints / lots.units
lots['violation_complaints_per_unit'] = lots.violation_complaints / lots.units
lots = lots.merge(tracts.drop(
    columns=['boro', 'censustract']), how='left', on="borotract")
drop_nas = ['cd', 'tract', 'address', 'built', 'res_area', 'units', 'boro', 'bbl',
            'buildings', 'value', 'total_area', 'latitude', 'longitude', 'x', 'y', 'land_use',
            'borotract', 'value_per_sqft', 'sqft_per_unit']
lots = lots.dropna(subset=drop_nas)
lots = lots.fillna(0)
lots.to_parquet('./data/lots_joined.parquet')
lots.to_csv('./data/lots_joined.csv', index=False)
