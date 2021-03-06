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
TelemetryDate = '2019-01-01'
InspectionStartDate = '2018-07-13'
InspectionEndDate = '2019-11-01'


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
            (Consent.fmDate > TermDate)
               ]
choices = ['Child' , 'Parent']
Consent['ParentorChild'] = np.select(conditions, choices, default = np.NaN)

choices2 = [Consent['ChildAuthorisations'] ,Consent['ParentAuthorisations']]
Consent['ParentChildConsent'] = np.select(conditions, choices2, default = np.NaN)

#Consent.isnull().sum()
#Consent.replace(' ',np.NaN)


# WAP Information
WAPCol = [
        'WAP',
        'RecordNo',
        'Activity',
        'Max Rate Pro Rata (l/s)',
        'Max Rate for WAP (l/s)',
        'From Month',
        'To Month'          
        # 'MaxRateForWAP_ls',
        # 'FromMonth',
        # 'ToMonth'       
        ]
WAPColNames = {
        'RecordNo' : 'ConsentNo',
        'Max Rate Pro Rata (l/s)' : 'MaxRateProRata',
        'Max Rate for WAP (l/s)' : 'MaxRateWAP',
        'From Month' : 'WAPFromMonth',
        'To Month' : 'WAPToMonth'
#        'MaxRateForWAP_ls' : 'MaxRateWAP',
#        'FromMonth' : 'WAPFromMonth',
#        'ToMonth' : 'WAPToMonth'
        }
WAPImportFilter = {
        'Activity' : ['Take Surface Water','Take Groundwater'],
        'RecordNo' : ConsentMaster
        }
WAPwhere_op = 'AND'
WAPServer = 'SQL2012Prod03'
WAPDatabase = 'DataWarehouse'
WAPTable = 'D_ACC_Act_Water_TakeWaterWAPAlloc'
#WAPTable = 'D_ACC_Act_Water_TakeWaterWAPAllocation'

WAP = pdsql.mssql.rd_sql(
                   server = WAPServer,
                   database = WAPDatabase, 
                   table = WAPTable,
                   col_names = WAPCol,
                   where_op = WAPwhere_op,
                   where_in = WAPImportFilter
                   )
WAP.nunique()
WAP.rename(columns=WAPColNames, inplace=True)

WAPMaster = list(set(WAP['WAP'].values.tolist()))


#only two off

#Aggregate WAP info to Consent level


# Aggregate WAP table to consent, WAP, actvity tripplets
# does this need to be by activity too?
WAP_base_CAW = WAP.groupby(
  ['ConsentNo', 'WAP', 'Activity'], as_index = False
  ).agg(
            {
            'MaxRateProRata' : 'count',
            'MaxRateWAP': 'count',
            'WAPFromMonth': 'count',
            'WAPToMonth' : 'count',
            }
        )

# 41 tripplets with multiple records
# 18 Tripplets without a pro Rata

WAP_base_CAW.rename(columns={
         'MaxRateProRata': 'MutipleProRata',
         'MaxRateWAP' : 'MultipleRate',
         'WAPFromMonth' : 'MultipleFromMonth',
         'WAPToMonth': 'MultipleToMonth'
         }, inplace = True)

# Aggregate WAP table to consent level
WAP_agg_C = WAP.groupby(
  ['ConsentNo'], as_index=False
  ).agg(
          {
          'MaxRateProRata' : 'sum',
          'MaxRateWAP' : 'max',
          'WAPToMonth' : 'count',
          'WAPFromMonth' : 'count'
          }
        )

WAP_agg_C.rename(columns={
        'MaxRateProRata' : 'CombinedRate',
        'MaxRateWAP' : 'MaxRate',
        'WAPToMonth' : 'ToMonthCount',
        'WAPFromMonth' : 'FromMonthCount'
        }, inplace=True)

# Calc Fields
WAP_agg_C.loc[WAP_agg_C['ToMonthCount'] > 1, 'MaxOfDifferentPeriodRate'] = WAP_agg_C['MaxRate']

#test table
# list(WAP_agg_C)
# WAP_agg_C.head()

# #test counts 
# Consent.shape
# WAP['ConsentNo'].nunique()
# WAP_agg_C.shape


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
x2 = x[x > 1] # 54 consents with more than one zone
x2.shape[0] #55
x2.max() # 10
x2.mean() # 2.6

# Aggregate Location table to consent level
Location_agg_C = Location.groupby('ConsentNo', as_index=False).agg({'CWMSZone': 'max'})
Location_agg_C.rename(columns = {'max':'CWMSZone'}, inplace=True)
# two consents w/o locations
# CRC185906
# CRC191696

# Full Effective Volume
ConsentDetailsCol = [
        'RecordNumber',
        'Activity',
#        'RecordNo',        
#        'Full Effective Annual Volume (m3/year)',
#        'Allocation Block'
        'FullAnnualVolume_m3year',
        'AllocationBlock'
        ]
ConsentDetailsColNames = {
        'RecordNumber': 'ConsentNo',
#        'RecordNo': 'ConsentNo',       
#        'Full Effective Annual Volume (m3/year)' : 'FEVolume',
#        'Allocation Block' : 'AllocationBlock'        
        'FullAnnualVolume_m3year' : 'FEVolume',
        'AllocationBlock' : 'AllocationBlock'   
        }
ConsentDetailsImportFilter = {
        'Activity' : ['Take Surface Water','Take Groundwater'],
        'RecordNumber' : ConsentMaster       
#        'RecordNo' : ConsentMaster
        }
#FEVStart = 
#FEVEnd = 
ConsentDetailsServer = 'SQL2012Prod03'
ConsentDetailsDatabase = 'DataWarehouse'
ConsentDetailsTable = 'D_ACC_Act_Water_TakeWaterPermitVolume'#'D_ACC_Act_Water_TakeWaterAllocData'

ConsentDetails = pdsql.mssql.rd_sql(
                   server = ConsentDetailsServer,
                   database = ConsentDetailsDatabase, 
                   table = ConsentDetailsTable,
                   col_names = ConsentDetailsCol,
                   where_in = ConsentDetailsImportFilter
                   )
ConsentDetails = ConsentDetails.drop_duplicates()
ConsentDetails.rename(columns=ConsentDetailsColNames, inplace=True)


# Calculate fields
ConsentDetails.loc[ConsentDetails['AllocationBlock'] == 'Adapt.Vol','AdMan'] = 'Adaptive Managment' 

# FEV = FEV.drop_duplicates(subset = ['ConsentNo', 'Activity','FEV'])
#duplicate CRC b/c of SW/ GW --- keep? - it gets grouped to CRC only later....

TEST_TempConsent2 = pd.merge(Consent, WAP, on = ['ConsentNo'], how = 'inner')
TEST_TempConsent2 = pd.merge(TEST_TempConsent2, ConsentDetails, on = ['ConsentNo','Activity'], how = 'left')
TEST_TempConsent2 = pd.merge(TEST_TempConsent2, Location, on = ['ConsentNo'], how = 'left')
TEST_TempConsent2 = TEST_TempConsent2.groupby(['ConsentNo','Activity'], as_index = False).count()


# Water User Groups (SMG)
SMGCol = [
        'B1_ALT_ID',
        'StatusType',
        'HolderAddressFullName',
        'MonOfficer'
        ]
SMGColNames = {
        'B1_ALT_ID' : 'SMGNo',
        'MonOfficer' : 'SMGMonOfficer'
        }
SMGImportFilter = {
       'StatusType' : ['OPEN']
        }
SMGServer = 'SQL2012Prod03'
SMGDatabase = 'DataWarehouse'
SMGTable = 'F_ACC_SelfManagementGroup'

SMG = pdsql.mssql.rd_sql(
                   server = SMGServer,
                   database = SMGDatabase, 
                   table = SMGTable,
                   col_names = SMGCol,
                   where_in = SMGImportFilter
                   )
SMG = SMG.drop_duplicates()
SMG.rename(columns=SMGColNames, inplace=True)

RelCol = [
        'ParentRecordNo',
        'ChildRecordNo'
        ]
RelColNames = {
        'ParentRecordNo' : 'SMGNo',
        'ChildRecordNo' : 'ConsentNo'
        }
RelImportFilter = {
       
        }
RelServer = 'SQL2012Prod03'
RelDatabase = 'DataWarehouse'
RelTable = 'D_ACC_Relationships'

Rel = pdsql.mssql.rd_sql(
                   server = RelServer,
                   database = RelDatabase, 
                   table = RelTable,
                   col_names = RelCol,
                   where_in = RelImportFilter
                   )
Rel = Rel.drop_duplicates()
Rel.rename(columns=RelColNames, inplace=True)

TEST_WUG = pd.merge(SMG, Rel, on = 'SMGNo', how = 'inner')
# Matches SQL

# Calculate Consent Complexities
temp = pd.merge(WAP, Consent, on = 'ConsentNo', how = 'inner')

temp = temp.groupby(
  ['WAP'], as_index=False
  ).agg(
          {
            'MaxRateWAP' : 'max',
            'ConsentNo' : pd.Series.nunique,
            'HolderEcanID' : pd.Series.nunique,
          }
        )

temp.rename(columns = 
         {
          'MaxRateWAP' :'MaxRateSharedWAP',
          'ConsentNo' : 'RecordNoCount',
          'HolderEcanID' : 'ECNoCount'
                 },inplace=True)
#still contains terminated
# 8263 vs 8372

MultipleWAP_agg_W = temp[temp.RecordNoCount > 1]
# matches SQL

MultipleEC_agg_W = temp[temp.ECNoCount > 1]
# matches SQL



# DataLogger Information
DataLoggerCol = [
        'ID',
        'LoggerID',
        'Well_no',
        'DateDeinstalled',
        'DateInstalled',
        ]
DataLoggerColNames = {
        
        }
DataLoggerImportFilter = {
        'DateDeinstalled' : np.nan,
#        'LoggerID' : 'LoggerID' > 1
        }
DataLoggerServer = 'sql2012prod05'
DataLoggerDatabase = 'wells'
DataLoggerTable = 'EPO_Dataloggers'

DataLogger = pdsql.mssql.rd_sql(
                   server = DataLoggerServer,
                   database = DataLoggerDatabase, 
                   table = DataLoggerTable,
                   col_names = DataLoggerCol
#                   ,
#                   where_in = DataLoggerImportFilter
                   )

DataLogger = DataLogger[DataLogger.LoggerID > 1]
DataLogger = DataLogger[DataLogger['DateDeinstalled'].isnull()]
# Matches SQL into#DataLogger 


# Telemetry Information
TelemetryCol = [
        'WAP',
        'site',
        'end_date'
        ]
TelemetryColNames = {
        
        }
TelemetryImportFilter = {
       'folder' : ['Telemetry']
        }
Telemetry_date_col = 'end_date'
Telemetry_from_date = TelemetryDate
TelemetryServer = 'EDWProd01'
TelemetryDatabase = 'Hydro'
TelemetryTable = 'HilltopUsageSiteDataLog'

Telemetry = pdsql.mssql.rd_sql(
                   server = TelemetryServer,
                   database = TelemetryDatabase, 
                   table = TelemetryTable,
                   col_names = TelemetryCol,
                   where_in = TelemetryImportFilter,
                   date_col = Telemetry_date_col,
                   from_date = Telemetry_from_date
                   )

Telemetry['Telemetered'] = 1
Telemetry.groupby(['WAP','site']).count().shape

#TEST_Telemetry = pd.DataFrame(Telemetry['WAP'].unique(), columns='WAP')
#TEST_Telemetry.rename(columns = 'WAP', inplace=True)
#TEST_Telemetry.sort_values(by = ['WAP'], inplace=True)
#x= Telemetry.loc[Telemetry['WAP']]
#there is one null

# Well Details
WellDetailsCol = [
        'Well_No',
        'Status',
        'GWuseAlternateWell'
        ]
WellDetailsColNames = {
        'Well_No' : 'WAP',
        'Status' :'WellStatus'
        }
WellDetailsImportFilter = {
       
        }
WellDetailsServer = 'SQL2012prod05'
WellDetailsDatabase = 'Wells'
WellDetailsTable = 'Well_Details'

WellDetails = pdsql.mssql.rd_sql(
                   server = WellDetailsServer,
                   database = WellDetailsDatabase, 
                   table = WellDetailsTable,
                   col_names = WellDetailsCol,
                   where_in = WellDetailsImportFilter,
                   )
WellDetails.rename(columns=WellDetailsColNames, inplace=True)


# Waiver
WaiverCol = [
        'Well_No',
        'EPO_LAST_UPDATE',
        'WM_Tmp_Waiver'
        ]
WaiverColNames = {
        'Well_No' : 'WAP'
        }
WaiverImportFilter = {
       
        }
WaiverServer = 'SQL2012prod05'
WaiverDatabase = 'Wells'
WaiverTable = 'EPO_WELL_DETAILS'

Waiver = pdsql.mssql.rd_sql(
                   server = WaiverServer,
                   database = WaiverDatabase, 
                   table = WaiverTable,
                   col_names = WaiverCol,
                   where_in = WaiverImportFilter,
                   )
Waiver.rename(columns=WaiverColNames, inplace=True)



  # ,(Case when ([WM_Tmp_Waiver]=1) then 1 else 0 end) as Waiver
  # ,(Case when ([WM_Tmp_Waiver]=1 and [EPO_LAST_UPDATE] is not null) then [EPO_LAST_UPDATE] else NULL end) as WaiverEditDate
  # ,(Case when (WD.[Status]='Active') THEN 1 else 0 end) as WellStatus    --20181130 CHANGED!!![Well_Status]='AE'
  # ,(Case when (tel.wap is not null) then 1 else 0 end) as Telemetered

conditions = [
        (WellDetails.WM_Tmp_Waiver == 1) & (WellDetails.EPO_LAST_UPDATE == EPO.notnull(data["EPO_LAST_UPDATE"]))
               ]
choices = [WellDetails.EPO_LAST_UPDATE]
Consent['ParentorChild'] = np.select(conditions, choices, default = np.nan)
WellDetails['Waiver'] =np.select(WellDetails['WM_Tmp_Waiver'] == 1, 1, default = 0)
WellDetails['WellStatus'] =np.select(WellDetails['Status'] == 'Active', 1, default = 0)

# Create WAPDetails table
WAPDetails = pd.merge(WellDetails, DataLogger, on = 'WAP', how = 'left')
WAPDetails = pd.merge(WAPDetails, EPO, on = 'WAP', how = 'left')
WAPDetails = pd.merge(WAPDetails, Telemetry, on = 'WAP', how = 'left')

  # ,(Case when [GWuseAlternateWell] <> WD.Well_No then 1 else 0 end) as SharedMeter

WAPDetails['SharedMeter'] = np.select(WAPDetails['WAP'] == WAPDetails['GWuseAlternateWell'], 1, default = 0)

# Select
#   distinct (WD.Well_No)
#   ,COUNT(DL.ID) as CountDataloggers
#   ,MIN(DateInstalled) as MinDataLogDate
#   ,MAX(DateInstalled) as MaxDataLogDate

WAPDetails_agg_W = WAPDetails.groupby(
  ['WAP'], as_index=False
  ).agg(
          {
          'ID': 'count',
          'DateInstalled' : ['min', 'max']
          }
        )

WAPDetails_agg_W.columns = WAPDetails_agg_W.columns.droplevel(1)

WAPDetails_agg_W.rename(columns = 
         {
          'ID' :'CountDataloggers',
          'min' : 'MinDataLogDate',
          'max' : 'MaxDataLogDate'
                 },inplace=True)











y = pd.merge(WAP, FEV, on =['ConsentNo','Activity'], how = 'left')
x = pd.merge(Consent, FEV, on ='ConsentNo', how = 'left')
z = pd.merge(WAP, Location, on ='ConsentNo', how = 'left')
w = pd.merge(Consent, Location, on ='ConsentNo', how = 'left')
v = pd.merge(Consent, WAP, on ='ConsentNo', how = 'inner')

v.groupby(['ConsentNo','Activity']).aggregate('count').shape

#test sizes
Consent.shape
Location_agg_C.shape
Location_agg_C['ConsentNo'].nunique()
Location['ConsentNo'].nunique()
FEV.shape
FEV['ConsentNo'].nunique()
FEV['ConsentNo','Activity'].nunique()
WAP_agg_C.shape
WAP_agg_C['ConsentNo'].nunique()
WAP['ConsentNo'].nunique()
WAP.groupby(['ConsentNo','Activity']).aggregate('count').shape



# Create Baseline
#does it sep acitivity on wap/consent?
# do locations have activity?
#option1
Baseline = pd.merge(WAP_base_CAW, Consent, on = 'ConsentNo', how = 'inner')
Baseline = pd.merge(Consent, Location_agg_C, on = 'ConsentNo', how = 'left')
Baseline = pd.merge(Baseline, FEV, on = ['ConsentNo','Activity'], how = 'left')
Baseline = pd.merge(Baseline, WAP_agg_C, on =['ConsentNo','Activity'], how = 'left')

#option 2
Consent_base = pd.merge(Consent, Location_agg_C, on = 'ConsentNo', how = 'left')
Consent_base = pd.merge(Consent_base, WAP_agg_C, on = 'ConsentNo', how = 'left')
WAP_base_CAW = pd.merge(WAP_base_CAW, FEV, on = ['ConsentNo','Activity'], how = 'left')
Baseline = pd.merge(WAP_base_CAW, Consent_base, on = 'ConsentNo', how = 'inner')

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
                           
                           
#
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
        'Consent',
        'ErrorMsg',
        'TotalMissingRecord',
        'TotalVolumeAboveRestriction',
        'TotalDaysAboveRestriction',
        'PercentAnnualVolumeTaken',
        'MaxVolumeAboveNDayVolume',
        'MaxConsecutiveDayVolume',
        'NumberOfConsecutiveDays',
        'MaxTakeRate',
        'MaxRateTaken'
        ]
SummaryColNames = {
        'Consent' : 'ConsentNo'
        }
SummaryImportFilter = {
       
        }
SummaryServer = 'SQL2012Prod03'
SummaryDatabase =  'Hilltop'
SummaryTable = 'ComplianceSummary'

Summary = pdsql.mssql.rd_sql(
                   server = SummaryServer,
                   database = SummaryDatabase, 
                   table = SummaryTable,
                   col_names = SummaryCol,
                   where_in = SummaryImportFilter,
                   )
Summary.rename(columns=SummaryColNames, inplace=True)

#SQL
#  distinct Consent
#  ,([master].[dbo].[fRemoveExtraCharacters](ErrorMsg)) as ErrorMessage
#  ,[TotalMissingRecord] 
  # ,(Case when ([TotalVolumeAboveRestriction] is null or [TotalVolumeAboveRestriction] = 0 or [TotalDaysAboveRestriction] is null) then 0 else 
  #   (case when ([TotalVolumeAboveRestriction]>100 and [TotalDaysAboveRestriction] > 2) then 100 else 1 end) end) as LFNC
  # ,(case when ([PercentAnnualVolumeTaken] <=100 or [PercentAnnualVolumeTaken] is null ) then 0 else 
  #   (case when ([PercentAnnualVolumeTaken] >200 ) then 2000 else 
  #     (case when ([PercentAnnualVolumeTaken] >100 ) then 100 else 1 end)end)end) as AVNC
  # ,(case when ([MaxVolumeAboveNDayVolume] is null or (([MaxVolumeAboveNDayVolume]+[MaxConsecutiveDayVolume])/[MaxConsecutiveDayVolume]*100)<=100) then 0 else
  #   (case when (([NumberOfConsecutiveDays]=1 and (([MaxVolumeAboveNDayVolume]+[MaxConsecutiveDayVolume])/[MaxConsecutiveDayVolume]*100)>105) or 
  #     ([NumberOfConsecutiveDays]>1 and (([MaxVolumeAboveNDayVolume]+[MaxConsecutiveDayVolume])/[MaxConsecutiveDayVolume]*100)>120) ) then 100 else 
  #   1 end)end) as CDNC
  # ,(case when ([MaxTakeRate] is null or [MaxRateTaken] is null or ([MaxRateTaken]/[MaxTakeRate])*100<=100 ) then 0 else 
  #   (case when (([MaxRateTaken]/[MaxTakeRate])*100>105) then 100 else 1 end) end) as RoTNC

  # ,(case when ([TotalMissingRecord] is null or [TotalMissingRecord] = 0) then 0 else
  #     (case when ([TotalMissingRecord] > 0 and [TotalMissingRecord] <= 10) then 5 else 
  #   (case when ([TotalMissingRecord] > 100) then 10000 else 5000 end)end)end) as MRNC


conditions = [
        Consent.B1_APPL_STATUS == 'Terminated - Replaced',
        ((Consent.B1_APPL_STATUS == 'Issued - Active') |
                (Consent.B1_APPL_STATUS == 'Issued - s124 Continuance')) &
            (Consent.fmDate > TermDate),
               ]
choices = ['Child' , 'Parent']
Consent['ParentorChild'] = np.select(conditions, choices, default = np.nan)


#MRNC - zeros vs Nulls. SQL says 0 is null Python says false
HighThresholdMR = 100
LowThresholdMR = 10
HighRiskMR = 10000
MedRiskMR = 5
LowRiskMR = 0
OtherRiskMR = 5000

conditions = [
    (pd.isnull(Summary['TotalMissingRecord'])) | (Summary.TotalMissingRecord == 0),
    (Summary.TotalMissingRecord > HighThresholdMR),
    (Summary.TotalMissingRecord > 0) & (Summary.TotalMissingRecord <= LowThresholdMR)
             ]
choices = [LowRiskMR, HighRiskMR, MedRiskMR]
Summary['MRNC'] = np.select(conditions, choices, default = OtherRiskMR)



# CDNC
HighPercentThresholdCDV = 105
LowPercentThresholdCDV = 100
HighRiskCDV = 100
MedRiskCDV = 1
LowRiskCDV = 0

Summary['PercentOverCDV'] = (Summary.MaxVolumeAboveNDayVolume+Summary.MaxConsecutiveDayVolume)/Summary.MaxConsecutiveDayVolume*100
conditions = [
    (pd.isnull(Summary['MaxVolumeAboveNDayVolume'])) | (Summary.PercentOverCDV <= LowPercentThresholdCDV),
    ((Summary.NumberOfConsecutiveDays == 1) & (Summary.PercentOverCDV > HighPercentThresholdCDV)) | ((Summary.NumberOfConsecutiveDays > 1) & (Summary.PercentOverCDV > 120))
             ]
choices = [LowRiskCDV,HighRiskCDV]
Summary['CDNC'] = np.select(conditions, choices, default = MedRiskCDV)


#RoTNC
HighPercentThresholdRoT = 105
LowPercentThresholdRoT = 100
HighRiskRoT = 100
MedRiskRoT = 1
LowRiskRoT = 0

Summary['PercentOverRoT'] = (Summary.MaxRateTaken/Summary.MaxTakeRate)*100

conditions = [
    (pd.isnull(Summary['MaxTakeRate'])) | (pd.isnull(Summary['MaxRateTaken'])) | (Summary.PercentOverRoT<= LowPercentThresholdRoT),
    ((Summary.MaxRateTaken/Summary.MaxTakeRate)*100 > HighPercentThresholdRoT)
             ]
choices = [LowRiskRoT,HighRiskRoT]
Summary['RoTNC'] = np.select(conditions, choices, default = MedRiskRoT)


#AVNC
HighVolumeThresholdAV = 200
LowVolumeThresholdAV = 100
HighRiskAV = 2000
MedRiskAV = 100
LowRiskAV = 0
OtherRiskAV = 1

conditions = [
    (pd.isnull(Summary['PercentAnnualVolumeTaken'])) | (Summary.PercentAnnualVolumeTaken <= LowVolumeThresholdAV),
    (Summary.PercentAnnualVolumeTaken > HighVolumeThresholdAV),
    (Summary.PercentAnnualVolumeTaken > LowVolumeThresholdAV) & (Summary.PercentAnnualVolumeTaken <= HighVolumeThresholdAV)
             ]
choices = [LowRiskAV, HighRiskAV, MedRiskAV]
Summary['AVNC'] = np.select(conditions, choices, default = OtherRiskAV)


#LFNC
VolumeThresholdLF = 100
DaysThresholdLF = 2
HighRiskLF = 100
MedRiskLF = 1
LowRiskLF = 0

conditions = [
    (pd.isnull(Summary['TotalVolumeAboveRestriction'])) | (Summary.TotalVolumeAboveRestriction == 0) | (Summary.TotalDaysAboveRestriction == np.nan),
    (Summary.TotalVolumeAboveRestriction > VolumeThresholdLF) & (Summary.TotalDaysAboveRestriction > DaysThresholdLF)
             ]
choices = [LowRiskLF,HighRiskLF]
Summary['LFNC'] = np.select(conditions, choices, default = MedRiskLF)

#Summary.to_csv(r'D:\Implementation Support\Python Scripts\scripts\Export\Summary.csv')



# Inspection History
InspectionCol = [
        'InspectionID',
        'B1_ALT_ID',
        'Subtype',
        'PARENT_InspectionID',
        'CHILD_InspectionID',
        'InspectionStatus',
        'NextInspectionDate'
        ]
InspectionColNames = {
        'B1_ALT_ID' : 'ConsentNo',
        'Subtype' : 'InspectionSubtype',
        }
InspectionImportFilter = {
        'RecordNo' : ConsentMaster
        }
Inspection_date_col = 'NextInspectionDate' 
Inspection_from_date = InspectionStartDate
Inspection_to_date = InspectionEndDate
InspectionServer = 'SQL2012Prod03'
InspectionDatabase = 'DataWarehouse'
InspectionTable = 'D_ACC_Inspections '


Inspection = pdsql.mssql.rd_sql(
                   server = InspectionServer,
                   database = InspectionDatabase, 
                   table = InspectionTable,
                   col_names = InspectionCol,
                   where_in = InspectionImportFilter,
                   date_col = Inspection_date_col,
                   from_date = Inspection_from_date
                   )

left outer join
  DataWarehouse.[dbo].[D_ACC_InspectionRelationships] rel2
where 
  ins.Subtype = 'Water Use Data' 
  and
  ins.NextInspectionDate >='2018-07-13 00:00:00.000' --had a look at data and chose start and stop date based on that.
  and
  ins.NextInspectionDate <='2019-11-01 00:00:00.000' 
  and
  rel2.[PARENT_InspectionID] is null --resolved duplicates (23 consents)


# Import Tables

# Consent Information
# Campaign Participants
# Telemetry Information
# Meter Information
# Water Use Summary
# Inspection History


------data extract Hydro db

SELECT  
  Distinct [ExtSiteID]
  ,COUNT (Distinct([DateTime])) as DayCount
  ,Max([DateTime]) as MaxDate
into #CountWAPDays
FROM 
  [EDWProd01].[Hydro].[dbo].[TSDataNumericDaily]
where 
  [DatasetTypeID] in (9,12) 
  and 
  [DateTime] between '2018-07-01'and '2019-06-30'
group by 
  [ExtSiteID]

  SELECT 
  Wap.RecordNo
  ,COUNT(DISTINCT([ExtSiteID])) as SiteCount
    ,SUM([DayCount]) as DayCountSum
  ,MAX([MaxDate]) as MaxDate
into #DayCountSum

FROM 
  #CountWAPDays WDC 
  inner join  [DataWarehouse].[dbo].[D_ACC_Act_Water_TakeWaterWAPAlloc] Wap
  on WDC.ExtSiteID=wap.WAP
where Wap.Activity like 'Take%' 
group by Wap.RecordNo
# Calc complications

# Calc Risk

# Assign Inspection Priority

# Allocate Inspections

# Output Totals

# Output Inspections
© 2019 GitHub, Inc.
Terms
Privacy
Security
Status
Help
Contact GitHub
Pricing
API
Training
Blog
About
© 2019 GitHub, Inc.