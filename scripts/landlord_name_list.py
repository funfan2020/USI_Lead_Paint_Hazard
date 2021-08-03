#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import datetime as dt
import urllib

urllib.request.urlretrieve ("https://data.cityofnewyork.us/resource/wvxf-dwi5.csv", "Housing_Maintenance_Code_Violations.csv")
data = pd.read_csv('Housing_Maintenance_Code_Violations.csv',low_memory=False)
data = data.drop_duplicates()

data618 = data.loc[(data['OrderNumber'] == '618')]
data618 = data618[['ViolationID', 'RegistrationID', 'BoroID', 'Block', 'Lot', 'BBL', 'BIN', 'BuildingID', 'HouseNumber', 'Apartment', 'OrderNumber', 'InspectionDate', 'ApprovedDate', 'OriginalCertifyByDate', 'NOVIssuedDate','CurrentStatusID', 'CurrentStatus', 'CurrentStatusDate']]
data618['BBL'] = data618.apply(lambda x: int(str(x[2])+str(x[3]).zfill(5)+str(x[4]).zfill(4)), axis=1)
data618['CurrentStatusDate'] = data618['CurrentStatusDate'].apply(lambda x: dt.datetime.strptime(x, '%m/%d/%Y'))
data618['ApprovedDate'] = data618['ApprovedDate'].apply(lambda x: dt.datetime.strptime(str(x), '%m/%d/%Y'))

# Only keep data after 2016
data2016 = data618[~((data618['CurrentStatusID'].isin([19, 20])) & (data618['ApprovedDate'] <= dt.datetime(2016, 1, 1)))]
data2016 = data2016[data2016['RegistrationID']!= 0].sort_values(by='ApprovedDate')

# The number of violations of 618 in each RegistrationID and its BBL
data_id = data2016[['RegistrationID', 'ViolationID']].groupby(by='RegistrationID').count().sort_values(by='ViolationID')    .rename({'ViolationID': 'violations'}, axis=1).reset_index()
data_id = data_id.merge(data2016[['RegistrationID', 'BBL']], on='RegistrationID').drop_duplicates()
BBLlist = list(data2016['BBL'].unique())
regID = list(data2016['RegistrationID'].unique())

urllib.request.urlretrieve ("https://data.cityofnewyork.us/resource/tesw-yqqr.csv", "Registration_Contacts.csv")
reg_raw = pd.read_csv('Registration_Contacts.csv', low_memory=False)
reg = reg_raw.loc[reg_raw['RegistrationID'].isin(regID)]
reg['Name'] = reg.apply(lambda x: x[['FirstName','MiddleInitial','LastName']].str.cat(sep=' '), axis=1)
reg['Address'] = reg.apply(lambda x: x[['BusinessZip', 'BusinessStreetName', 'BusinessHouseNumber', 'BusinessApartment']].str.cat(sep=','), axis=1)

# Only retain the four titles of'Agent','HeadOfficer','IndividualOwner', and'JointOwner'
reg = reg[reg['Type'].isin(['Agent', 'HeadOfficer', 'IndividualOwner', 'JointOwner'])]
owner_name = reg[['RegistrationID', 'Name','Address']].merge(data_id[['RegistrationID','BBL']], on='RegistrationID', how='left').drop_duplicates()

# Remove data with special characters in the name
owner_name = owner_name[~((owner_name['Name'].apply(lambda x: x==''))|(owner_name['Name'].apply(lambda x: '#'in x)))]

# All owners corresponding to each registered  id
owner_names = reg[['RegistrationID','Name']].groupby(by='RegistrationID', dropna=True).apply(lambda x: set(x['Name'].tolist()))
owner_names = owner_names.reset_index(level=None, drop=False, name='owner_names', inplace=False)

company_names = reg[['RegistrationID', 'CorporationName']].dropna().groupby(by='RegistrationID', dropna=True).apply(lambda x: set(x['CorporationName'].tolist()))
company_names = company_names.reset_index(level=None, drop=False, name='company_names', inplace=False)

address = reg[['RegistrationID', 'Address']].dropna().groupby(by='RegistrationID', dropna=True).apply(lambda x: max(x['Address'], key=len, default=''))
address = address.reset_index(level=None, drop=False, name='Address', inplace=False)

# ## The name of the contact and its registered company based on the RegistrationID
owner_corp_names = owner_names.merge(company_names, on='RegistrationID', how='left')
owner_corp_names = owner_corp_names.merge(data_id, on='RegistrationID', how='left')
owner_corp_names = owner_corp_names.merge(address, on='RegistrationID', how='left')

urllib.request.urlretrieve ("https://data.cityofnewyork.us/resource/64uk-42ks.csv", "Primary_Land_Use_Tax_Lot_Output__PLUTO_.csv")
pluto = pd.read_csv('Primary_Land_Use_Tax_Lot_Output__PLUTO_.csv',low_memory=False)
pluto = pluto[pluto['yearbuilt']<=1960]

data_with_name = owner_corp_names.merge(pluto[['bbl', 'ownername','numbldgs','unitsres','yearbuilt']], left_on='BBL', right_on='bbl')
data_with_name['company_names'] = data_with_name['company_names'].fillna(' ')

# For records with missing names, supplement with owner_corp_names data
data_with_name = data_with_name[data_with_name['unitsres']!=0]
def fill_name(x):
    if x['ownername'] in ['UNAVAILABLE OWNER','NAME NOT ON FILE']:
        if x['company_names'] !=' ':
            return list(x['company_names'])[0]
        return list(x['owner_names'])[0]
    return x['ownername']

data_with_name['ownername'] = data_with_name.apply(lambda x: fill_name(x), axis=1)
corpname_list = list(data_with_name['ownername'].unique())

violations = data_with_name[['ownername','violations']].groupby(by='ownername').sum().sort_values(by='violations')

pluto_adj = pluto.merge(data_with_name[['bbl', 'ownername']], on='bbl', how='left')
pluto_adj['ownername_x'] = pluto_adj.apply(lambda x: x['ownername_y'] if x['ownername_y'] else x['ownername_x'], axis=1)

owner_bbls = pluto_adj[['bbl','ownername_x']].groupby(by='ownername_x', dropna=True).apply(lambda x:set(x['bbl'].tolist()))
owner_bbls = owner_bbls.reset_index(level=None, drop=False, name='bbls', inplace=False)

# In PLUTO, only the company name is recorded for the company owner, 
# so the data is first aggregated according to the company name, and then the personal name is integrated.
pluto_in_list = pluto_adj.loc[(pluto_adj['ownername_x'].isin(corpname_list))]
total_units= pluto_in_list[['ownername_x','unitsres']].groupby(by='ownername_x').sum().sort_values(by='unitsres')
total_units = total_units.merge(owner_bbls,on='ownername_x',how='left')

#The number of violations per company and the total number of units they owned.
name_list = violations.merge(total_units, left_index=True, right_on='ownername_x')
name_list = name_list.merge(data_with_name[['BBL','owner_names', 'ownername', 'Address']], left_on='ownername_x', right_on='ownername',how='left')
name_list = name_list.drop_duplicates(subset='ownername')

name_list[['violations', 'unitsres', 'owner_names', 'ownername', 'Address','bbls']].rename(columns={'violations': 'Total Violations', 'unitsres': 'Total Units', 'owner_names': 'Owner Names', 'ownername': 'Company Name'}).to_csv(r'.\data\company_name_list.csv')

def sum_bbl(x):
    bbls = set()
    for i in x['bbls']:
        bbls = i|bbls
    return bbls
owner_name_list = name_list[['violations','unitsres','BBL','bbls']].merge(owner_name[['BBL','Name','Address']],on='BBL',how='left')
bbls_list = owner_name_list[['bbls','Name']].groupby(by='Name').apply(lambda x: sum_bbl(x))
bbls_list = bbls_list.reset_index(level=None, drop=False, name='bbls', inplace=False)

owner_name_list = name_list[['violations','unitsres','BBL']].merge(owner_name[['BBL','Name','Address']],on='BBL',how='left').drop_duplicates()
owner_name_list = owner_name_list[['violations','unitsres','Name']].groupby(by='Name').sum()
owner_name_list['Violation Rate'] = owner_name_list['violations'] /owner_name_list['unitsres'] 
owner_name_list=owner_name_list.merge(owner_name[['Address','Name']],on='Name').drop_duplicates()
owner_name_list = owner_name_list.merge(bbls_list,on='Name',how='left')

owner_name_list.rename(columns={'violations': 'Total Violations', 'unitsres': 'Total Units'}).to_csv(
    r'.\data\landlord_name_list.csv')
