
import pandas as pd
import helpers

problems = helpers.call_api(
    'https://data.cityofnewyork.us/resource/a2nx-4u46.csv',
    4210000,
    where="codeid in('2524','2525','2526','2518','2529','2531')",
)
lots = pd.read_parquet('./data/lots.parquet')

statuses = problems.statusdescription.unique()
status_dict = {}
for i in range(len(statuses)):
  status_dict[statuses[i]] = str(i)
problems['statuscode'] = problems.statusdescription.apply(lambda x: status_dict[x])
problems = problems[problems['statuscode'].isin(['0','1','2','3','4'])]
status_map = {
    '0': 'no_entry',
    '1': 'no_violation',
    '2': 'violation',
    '3': 'no_entry',
    '4': 'no_entry',
}
problems['result'] = problems.statuscode.apply(lambda x: status_map[x])
problems = problems[['problemid','complaintid','status','result','statusdate']].rename(columns={
    'problemid': 'problem_id',
    'complaintid': 'complaint_id',
    'statusdate': 'status_date'
})
problem_pivot = pd.pivot_table(problems[['complaint_id','result']],
           index='complaint_id',
           columns='result',
           margins=True,
           aggfunc=len).reset_index()[:-1].rename_axis(None, axis=1).fillna(0).rename(columns={'All':'problems'})
problem_pivot['has_no_entry'] = problem_pivot.apply(lambda x: 1 if x.no_entry > 0 else 0, axis=1)
problem_pivot['has_violation'] = problem_pivot.apply(lambda x: 1 if x.violation > 0 else 0, axis=1)
problem_pivot['has_no_violation'] = problem_pivot.apply(lambda x: 1 if x.no_violation > 0 and x.violation < 1 and x.no_entry < 1 else 0, axis=1)
complaints = helpers.call_api(
    'https://data.cityofnewyork.us/resource/uwyv-629c.csv',
    2450000,
)
complaints = complaints[complaints['complaintid'].isin(problems.complaint_id.unique())]
to_str = ['boroughid','block','lot']
for key in to_str:
  complaints[key] = complaints[key].astype(int).astype(str)
complaints['block'] = complaints.block.str.zfill(5)
complaints['lot'] = complaints.lot.str.zfill(4)
complaints['bbl'] = complaints.boroughid + complaints.block + complaints.lot
complaints = complaints[['complaintid','buildingid','bbl','receiveddate','status','statusdate']].rename(columns={
    'complaintid':'complaint_id',
    'buildingid':'building_id',
    'receiveddate':'received_date',
    'statusdate':'status_date'
})
to_dt = ['status_date','received_date']
complaints[to_dt] = complaints[to_dt].apply(pd.to_datetime)
complaints = complaints.merge(problem_pivot, on='complaint_id')
complaints = complaints.rename(columns={
    'no_entry': 'no_entry_problems',
    'no_violation':'no_violation_problems',
    'violation':'violation_problems',
    'has_no_entry':'no_entry',
    'has_violation':'violation',
    'has_no_violation':'no_violation'
})
complaints.to_parquet('./data/complaints.parquet')
problems.to_parquet('./data/problems.parquet')
for feature in to_dt:
  complaints[feature] = complaints[feature].dt.strftime('%Y/%m/%d %H:%m')
complaints.to_csv('./data/complaints.csv', index=False)
problems.to_csv('./data/problems.csv', index=False)