# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 14:52:35 2019
@author: KatieSi
"""

# Import packages
import numpy as np
import pandas as pd
import pdsql
from datetime import datetime, timedelta

# Set Variables
ReportName= 'Water Inspection Prioritzation Model'
RunDate = datetime.now()
ConsentMaster = []
WAPMaster = []


# Set Risk Paramters
TermDate = '2018-10-01'
TermDate = datetime.strptime(TermDate, '%Y-%m-%d')



# Set Table Variables
# Consent Information
ConsentCol = [
        'B1_ALT_ID',
        'B1_APPL_STATUS',
        'fmDate',
        'toDate',
        'HolderAddressFullName',
        'HolderEcanID',
        'ChildAuthorisations',
        'ParentAuthorisations'
        ]
ConsentColNames = {
        'B1_ALT_ID' : 'ConsentNo'
        }
ConsentImportFilter = {
       'B1_PER_SUB_TYPE' : ['Water Permit (s14)'] ,
       'B1_APPL_STATUS' : ['Terminated - Replaced', 'Issued - Active', 'Issued - s124 Continuance']
#    'B1_APPL_STATUS' : ['Issued - Active', 'Issued - s124 Continuance']
           
        }
Consentwhere_op = 'AND'
ConsentDate_col = 'toDate'
Consentfrom_date = '2018-10-1' #dates are inclusive in PY and exclusive in SQL
ConsentServer = 'SQL2012Prod03'
ConsentDatabase = 'DataWarehouse'
ConsentTable = 'F_ACC_PERMIT'


Consent = pdsql.mssql.rd_sql(
                   server = ConsentServer,
                   database = ConsentDatabase, 
                   table = ConsentTable,
                   col_names = ConsentCol,
                   where_op = Consentwhere_op,
                   where_in = ConsentImportFilter,
                   date_col = ConsentDate_col,
                   from_date = Consentfrom_date
                   )

## ##### filter here for active before end date

Consent.rename(columns=ConsentColNames, inplace=True)
print(Consent.dtypes)
#filter
# consent list
ConsentMaster = Consent['ConsentNo'].values.tolist()


#Consent.loc[Consent.fmDate > TermDate ,'IssuedAfterThreshold'] = 'IssuedAfter'

# SQL code
# ,(case when (B1_APPL_STATUS ='Terminated - Replaced') then 'Child' else
#     (case when (B1_APPL_STATUS in ('Issued - Active', 'Issued - s124 Continuance') and fmDate >  '2018-10-01 00:00:00.000') then 'Parent' else NULL end)
#   end) 
# as ParentorChild
#   ,(case when (B1_APPL_STATUS ='Terminated - Replaced') then [ChildAuthorisations] else
#     (case when (B1_APPL_STATUS in ('Issued - Active', 'Issued - s124 Continuance') and fmDate >  '2018-10-01 00:00:00.000') then [ParentAuthorisations] else NULL end)
#   end) 
#   as ParentChildConsent

conditions = [
        Consent.B1_APPL_STATUS == 'Terminated - Replaced',
        ((Consent.B1_APPL_STATUS == 'Issued - Active') |
                (Consent.B1_APPL_STATUS == 'Issued - s124 Continuance')) &
            (Consent.fmDate > TermDate),
               ]
choices = ['Child' , 'Parent']
Consent['ParentorChild'] = np.select(conditions, choices, default = np.nan)


choices2 = [Consent['ChildAuthorisations'] ,Consent['ParentAuthorisations']]
Consent['ParentChildConsent'] = np.select(conditions, choices2, default = np.nan)




# WAP Information
WAPCol = [
        'WAP',
        'RecordNo',
        'Activity',
        'Max Rate Pro Rata (l/s)',
        'Max Rate for WAP (l/s)',
        'From Month',
        'To Month'        
        ]
WAPColNames = {
        'RecordNo' : 'ConsentNo',
        'Max Rate Pro Rata (l/s)' : 'MaxRateProRata',
        'Max Rate for WAP (l/s)' : 'MaxRateWAP',
        'From Month' : 'WAPFromMonth',
        'To Month' : 'WAPToMonth'
        }
WAPImportFilter = {
        'Activity' : ['Take Surface Water','Take Groundwater'],
        'RecordNo' : ConsentMaster
        }
WAPwhere_op = 'AND'
WAPServer = 'SQL2012Prod03'
#WAPServer = 'SQL2012Test01'
WAPDatabase = 'DataWarehouse'
WAPTable = 'D_ACC_Act_Water_TakeWaterWAPAlloc'

WAP = pdsql.mssql.rd_sql(
                   server = WAPServer,
                   database = WAPDatabase, 
                   table = WAPTable,
                   col_names = WAPCol,
                   where_op = WAPwhere_op,
                   where_in = WAPImportFilter
                   )
WAP.nunique()
WAP = WAP.drop_duplicates()
WAP.nunique()
WAP.rename(columns=WAPColNames, inplace=True)

WAPMaster = list(set(WAP['WAP'].values.tolist()))


#Aggregate WAP info to Consent level

# does this need to be by activity too?
WAP_base = WAP.groupby(
  ['ConsentNo', 'WAP', 'Activity'], as_index = False
  ).agg(
            {
            'MaxRateProRata' : 'count',
            'MaxRateWAP': 'count',
            'WAPFromMonth': 'count',
            'WAPToMonth' : 'count',
            }
        )

# 41 tripplets without multiple records
# 18 Tripplets without a pro Rata

WAP_base.rename(columns={
         'MaxRateProRata': 'MutipleProRata',
         'MaxRateWAP' : 'MultipleRate',
         'WAPFromMonth' : 'MultipleFromMonth',
         'WAPToMonth': 'MultipleToMonth'
         }, inplace = True)

WAP_agg = WAP.groupby(
  ['ConsentNo'], as_index=False
  ).agg(
          {
          'MaxRateProRata' : 'sum',
          'MaxRateWAP' : 'max',
          'WAPToMonth' : 'count',
          'WAPFromMonth' : 'count'
          }
        )

WAP_agg.rename(columns={
        'MaxRateProRata' : 'CombinedRate',
        'MaxRateWAP' : 'MaxRate',
        'WAPToMonth' : 'ToMonthCount',
        'WAPFromMonth' : 'FromMonthCount'
        }, inplace=True)

# twoways to calc column.
#which one works?
##option1 - doesn't work
#df['new column name'] = df['df column_name'].apply(lambda x: 'value if condition is met' if x condition else 'value if condition is not met')
#WAP_agg['MaxOfDifferentPeriodRate'] = WAP_agg['MonthCount'].apply(lambda x: WAP_agg['MaxRate'] if x > 1 else 'Null')

#option2
df.loc[df['A'] == df['B'], 'C'] = 0
WAP_agg.loc[WAP_agg['MonthCount'] > 1, 'MaxOfDifferentPeriodRate'] = WAP_agg['MaxRate']

#test table
list(WAP_agg)
WAP_agg.head()

#test counts 
Consent.shape
WAP['ConsentNo'].nunique()
WAP_agg.shape





# x = Location.groupby(['ConsentNo'])['CWMSZone'].aggregate('count')

# df.groupby('A').agg({'B': ['min', 'max'], 'C': 'sum'})

# rich_inventory = (
#     inventory
#     .groupBy('id')
#     .agg((F.min("firstyear").alias("Firstyear")),
#          (F.max("lastyear").alias("Lastyear")),
#          (F.count('core_element_flag').alias('all_element_count')),
#          (F.sum('core_element_flag').alias('core_element_count')),
#          (F.sum('precip_flag').alias('precip_flag'))
#         )
# )

# # Group the data frame by month and item and extract a number of stats from each group
# data.groupby(
#     ['month', 'item']
# ).agg(
#     {
#         # find the min, max, and sum of the duration column
#         'duration': [min, max, sum],
#          # find the number of network type entries
#         'network_type': "count",
#         # min, first, and number of unique dates per group
#         'date': [min, 'first', 'nunique']
#     }
# )

# grouped = data.groupby('month').agg("duration": [min, max, mean])
# grouped.columns = grouped.columns.droplevel(level=0)
# grouped.rename(columns={
#     "min": "min_duration", "max": "max_duration", "mean": "mean_duration"
# })
# grouped.head()

# grouped = data.groupby('month').agg("duration": [min, max, mean]) 
# # Using ravel, and a string join, we can create better names for the columns:
# grouped.columns = ["_".join(x) for x in grouped.columns.ravel()]



# Location Information
LocationCol = [
        'B1_ALT_ID',
        'AttributeValue'
        ]
LocationColNames = {
         'B1_ALT_ID' : 'ConsentNo',
         'AttributeValue' : 'CWMSZone' 
        }
LocationImportFilter = {
       'B1_ALT_ID' : ConsentMaster,
       'AttributeType' : ['CWMS Zone']
        }
#LocationStart = 
#LocationEnd = 
LocationServer = 'SQL2012Prod03'
LocationDatabase = 'DataWarehouse'
LocationTable = 'F_ACC_SpatialAttributes2'

Location = pdsql.mssql.rd_sql(
                   server = LocationServer,
                   database = LocationDatabase, 
                   table = LocationTable,
                   col_names = LocationCol,
                   where_in = LocationImportFilter
                   )
Location = Location.drop_duplicates()
Location.rename(columns=LocationColNames, inplace=True)

x = Location.groupby(['ConsentNo'])['CWMSZone'].aggregate('count')
x2 = x[x > 1] # 53 consents with more than one zone
x2.max() # 10
x2.mean() # 2.6

# data.groupby('month', as_index=False).agg({"duration": "sum"})
# should work better
Location_agg = Location.groupby('ConsentNo', as_index=False).agg({'CWMSZone': 'max'})
Location_agg.rename(columns = {'max':'CWMSZone'})

#old way
# Location.sort_values(by = ['CWMSZone'], inplace  = True)
# Location = Location.drop_duplicates(subset = 'ConsentNo', keep = 'last') #last in alphabet matches MAX() from SQL
# two consents w/o locations
# CRC185906
# CRC191696

# Full Effective Volume
FEVCol = [
        'RecordNo',
        'Activity',
        'Full Effective Annual Volume (m3/year)',
        'Allocation Block'
#        'FullEffectiveAnnualVolume_m3year'
        ]
FEVColNames = {
        'RecordNo': 'ConsentNo',
        'Full Effective Annual Volume (m3/year)' : 'FEVolume',
#        'FullEffectiveAnnualVolume_m3year' : 'FEVolume',
        'Allocation Block' : 'AllocationBlock'
        }
FEVImportFilter = {
        'Activity' : ['Take Surface Water','Take Groundwater'],
        'RecordNo' : ConsentMaster
        }
#FEVStart = 
#FEVEnd = 
FEVServer = 'SQL2012Prod03'
FEVDatabase = 'DataWarehouse'
FEVTable = 'D_ACC_Act_Water_TakeWaterAllocData'

FEV = pdsql.mssql.rd_sql(
                   server = FEVServer,
                   database = FEVDatabase, 
                   table = FEVTable,
                   col_names = FEVCol,
                   where_in = FEVImportFilter
                   )
FEV = FEV.drop_duplicates()
FEV.rename(columns=FEVColNames, inplace=True)


#truely or still AV?
FEV.loc[FEV['AllocationBlock'] == 'Adapt.Vol','AdMan'] = 'Adaptive Managment' 

WAP_agg.loc[WAP_agg['MonthCount'] > 1, 'MaxOfDifferentPeriodRate'] = WAP_agg['MaxRate']

# FEV = FEV.drop_duplicates(subset = ['ConsentNo', 'Activity','FEV'])
#duplicate CRC b/c of SW/ GW --- keep? - it gets grouped to CRC only later....


# Water User Groups (SMG)
WUGCol = [
        
        ]
WUGColNames = {
        
        }
WUGImportFilter = {
       
        }
WUGServer = 'SQL2012Prod03'
WUGDatabase = 
WUGTable = 






y = pd.merge(WAP, FEV, on =['ConsentNo','Activity'], how = 'left')
x = pd.merge(Consent, FEV, on ='ConsentNo', how = 'left')
z = pd.merge(WAP, Location, on ='ConsentNo', how = 'left')
w = pd.merge(Consent, Location, on ='ConsentNo', how = 'left')
v = pd.merge(Consent, WAP, on ='ConsentNo', how = 'inner')

#test sizes
Consent.shape
Location_agg.shape
Location_agg['ConsentNo'].nunique()
Location['ConsentNo'].nunique()
FEV.shape
FEV['ConsentNo'].nunique()
FEV['ConsentNo','Activity'].nunique()
WAP_agg.shape
WAP_agg['ConsentNo'].nunique()
WAP['ConsentNo'].nunique()
WAP['ConsentNo','Activity'].nunique()


# Create Baseline
#does it sep acitivity on wap/consent?
# do locations have activity?
#option1
Baseline = pd.merge(WAP_base, Consent, on = 'ConsentNo', how = 'inner')
Baseline = pd.merge(Consent, Location_agg, on = 'ConsentNo', how = 'left')
Baseline = pd.merge(Baseline, FEV, on = ['ConsentNo','Activity'], how = 'left')
Baseline = pd.merge(Baseline, WAP_agg, on =['ConsentNo','Activity'], how = 'left')

#option 2
Consent_base = pd.merge(Consent, Location_agg, on = 'ConsentNo', how = 'left')
Consent_base = pd.merge(Consent_base, WAP_agg, on = 'ConsentNo', how = 'left')
WAP_base = pd.merge(WAP_base, FEV, on = ['ConsentNo','Activity'], how = 'left')
Baseline = pd.merge(WAP_base, Consent_base, on = 'ConsentNo', how = 'inner')

TempConsent2 = Baseline


# add environmental risk
#SQL
#,(case when ((Activity like '%ground%' and FEVolume>=1500000) or (Activity like '%surface%' and CombinedRate>=100)) then 'High risk' else Null end) as EnvironmentalRisk
conditions = [
    (Baseline.Activity == 'ground') & (Baseline.FEVolume >=1500000),
    (Baseline.Activity == 'surface') & (Baseline.CombinedRate>=100)
             ]
choices = ['High risk','High risk']
Baseline['EnvironmentalRisk'] = np.select(conditions, choices, default = Null)


# label 5ls
#where 
#  (CombinedRate is null and MaxRate >= 5) 
#  or
#  CombinedRate>=5 -- what about where there is no proper rate, or rate is crazy small?

conditions = [
    (Baseline.CombinedRate == Null) & (Baseline.MaxRate >= 5),
    (Baseline.CombinedRate >= 5)
             ]
choices = ['Above','Above']
Baseline['FiveLS'] = np.select(conditions, choices, default = 'Under')

TempConsent = Baseline.loc[Baseline['FiveLS'] == 'Over']
                           
#
# SQL
# (Case when (COUNT(Distinct([From Month]))>1 or  COUNT( DISTINCT([To Month]))>1) then Max([Max Rate for WAP (l/s)]) else Null end) as MaxOfDifferentPeriodRate
conditions = [
    (Baseline.ToMonthCount > 1),
    (Baseline.FromMonthCount > 1)
             ]
choices = [Baseline['MaxRateWAP'],Baseline['MaxRateWAP']]
Baseline['FiveLS'] = np.select(conditions, choices, default = np.nan)
                           
                           

# Campaign Participants
CampaignCol = [
        
        ]
CampaignColNames = {
        
        }
CampaignImportFilter = {
       
        }
CampaignServer = 
CampaignDatabase = 
CampaignTable = 

# Telemetry Information
TelemetryCol = [
        
        ]
TelemetryColNames = {
        
        }
TelemetryImportFilter = {
       
        }
TelemetryServer = 
TelemetryDatabase = 
TelemetryTable = 

# Meter Information
MeterCol = [
        
        ]
MeterColNames = {
        
        }
MeterImportFilter = {
       
        }
MeterServer = 
MeterDatabase = 
MeterTable = 

# Water Use Summary
SummaryCol = [
        
        ]
SummaryColNames = {
        
        }
SummaryImportFilter = {
       
        }
SummaryServer = 'SQL2012Prod03'
SummaryDatabase = 
SummaryTable = 

# Inspection History
InspectionCol = [
        
        ]
InspectionColNames = {
        
        }
InspectionImportFilter = {
       
        }
InspectionServer = 'SQL2012Prod03'
InspectionDatabase = 'DataWarehouse'
InspectionTable = 


# Import Tables

# Consent Information
# Campaign Participants
# Telemetry Information
# Meter Information
# Water Use Summary
# Inspection History



# Calc complications

# Calc Risk

# Assign Inspection Priority

# Allocate Inspections

# Output Totals

# Output Inspections