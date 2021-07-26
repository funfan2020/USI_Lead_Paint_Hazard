import pandas as pd
import helpers

lead_order_numbers = "'555','610','611','612','614','616','617'"
violations = helpers.call_api(
    'https://data.cityofnewyork.us/resource/wvxf-dwi5.csv',
    210000,
    where=f'ordernumber in({lead_order_numbers})'
)
violations.drop(['streetname','streetcode','housenumber','lowhousenumber','highhousenumber','apartment','block','lot','rentimpairing','boro'],inplace=True,axis=1)
to_int = ['boroid','bin','bbl','censustract','novid','zip']
violations = violations.dropna(subset=to_int)
violations.censustract = violations.censustract*100
for key in to_int:
  violations[key] = violations[key].astype(int).astype(str)
violations['censustract'] = violations.censustract.astype(str).str.zfill(6)
violations['borotract'] = violations.boroid + violations.censustract
violations = violations.rename(columns={
  'censustract': 'tract',
  'boroid': 'boro',
  'communityboard':'community_board',
  'councildistrict':'council_district',
  'inspectiondate': 'inspection_date',
  'approveddate': 'approved_date',
  'originalcertifybydate': 'original_certify_by_date',
  'originalcorrectbydate': 'original_correct_by_date',
  'newcertifybydate':'new_certify_by_date',
  'newcorrectbydate':'new_correct_by_date',
  'certifieddate':'certified_date',
  'novissueddate':'nov_issued_date',
  'currentstatusdate':'current_status_date',
})
to_dt = ['inspection_date','approved_date','original_certify_by_date','original_correct_by_date','new_certify_by_date','new_correct_by_date','certified_date','nov_issued_date','current_status_date']
violations[to_dt] = violations[to_dt].apply(pd.to_datetime)
violations.to_parquet('./data/violations.parquet')
for feature in to_dt:
  violations[feature] = violations[feature].dt.strftime('%Y/%m/%d %H:%m')
violations.to_csv('./data/violations.csv', index=False)