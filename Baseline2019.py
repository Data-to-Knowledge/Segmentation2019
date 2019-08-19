# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 14:52:35 2019
@author: KatieSi
"""
#reset
#clear

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
IssuedDate = '2019-07-01'
TelemetryFromDate = '2018-07-01'# previous model Jan-1
TelemetryToDate = '2019-07-01' #non existant in previous model
InspectionStartDate = '2014-07-01' #'2018-07-13'
InspectionEndDate = '2019-07-01' #'2019-11-01'
SummaryTableRunDate = datetime.strptime('2019-08-08', '%Y-%m-%d').date()
VerificationLimit = '2014-07-01'


# Set Table Variables
# Consent Information
ConsentDetailsCol = [
        'B1_ALT_ID',
        'B1_APPL_STATUS',
        'fmDate',
        'toDate',
        'HolderAddressFullName',
        'HolderEcanID',
        'ChildAuthorisations',
        'ParentAuthorisations'
        ]
ConsentDetailsColNames = {
        'B1_ALT_ID' : 'ConsentNo',
        'B1_APPL_STATUS' : 'ConsentStatus'
        }
ConsentDetailsImportFilter = {
       'B1_PER_SUB_TYPE' : ['Water Permit (s14)'] ,
       'B1_APPL_STATUS' : ['Terminated - Replaced', 'Issued - Active', 'Issued - s124 Continuance']      
        }
ConsentDetailswhere_op = 'AND'
ConsentDetailsDate_col = 'toDate'
ConsentDetailsfrom_date = '2018-10-1' #dates are inclusive in PY and exclusive in SQL
ConsentDetailsServer = 'SQL2012Prod03'
ConsentDetailsDatabase = 'DataWarehouse'
ConsentDetailsTable = 'F_ACC_PERMIT'

ConsentDetails = pdsql.mssql.rd_sql(
                   server = ConsentDetailsServer,
                   database = ConsentDetailsDatabase, 
                   table = ConsentDetailsTable,
                   col_names = ConsentDetailsCol,
                   where_op = ConsentDetailswhere_op,
                   where_in =ConsentDetailsImportFilter,
                   date_col = ConsentDetailsDate_col,
                   from_date = ConsentDetailsfrom_date
                   )

ConsentDetails.rename(columns=ConsentDetailsColNames, inplace=True)
ConsentDetails['ConsentNo'] = ConsentDetails['ConsentNo'].str.strip().str.upper()

# remove consents issued after FY28/19
ConsentInfo = ConsentDetails[ConsentDetails['fmDate'] < '2019-07-01']

# consent list
ConsentMaster = list(set(ConsentDetails['ConsentNo'].values.tolist()))

ConsentBase = ConsentDetails[['ConsentNo']]
ConsentBase = ConsentBase.drop_duplicates()



#change info
conditions = [
        ConsentDetails.ConsentStatus == 'Terminated - Replaced',
        ((ConsentDetails.ConsentStatus == 'Issued - Active') |
                (ConsentDetails.ConsentStatus == 'Issued - s124 Continuance')) &
            (ConsentDetails.fmDate > TermDate),
               ]
choices = ['Child' , 'Parent']
ConsentDetails['ParentorChild'] = np.select(conditions, choices, default = np.nan)
choices2 = [ConsentDetails['ChildAuthorisations'] ,ConsentDetails['ParentAuthorisations']]
ConsentDetails['ParentChildConsent'] = np.select(conditions, choices2, default = np.nan)

print('ConsentMaster ', len(ConsentMaster),
      '\nConsentBase ', ConsentBase.shape[0],
      '\n\nConsentDetails Table ',
      ConsentDetails.shape,'\n',
      ConsentDetails.nunique(), '\n\n')


# WAP Limit Information
WAPLimitCol = [
        'WAP',
        'RecordNumber',
        'Activity',
        'MaxRateForWAP_ls',
        'AllocationRate_ls',
        'FromMonth',
        'ToMonth',
        'DailyVol_m3',
        'DailyIsCondition',
        'WeeklyVol_m3',
        'WeeklyIsCondition',
        '30DayVol_m3',
        '30DayIsCondition',
        '150DayVol_m3',
        '150DayIsCondition',
        'CustomVol_m3',
        'CustomPeriodDays'      
        ]
WAPLimitColNames = {
        'RecordNumber' : 'ConsentNo',
        'AllocationRate_ls' : 'MaxRateProRata',
        'MaxRateForWAP_ls' : 'WAPRate',
        'FromMonth' : 'WAPFromMonth',
        'ToMonth' : 'WAPToMonth'
        }
WAPLimitImportFilter = {
        'Activity' : ['Take Surface Water','Take Groundwater'],
        'RecordNumber' : ConsentMaster
        }
WAPLimitwhere_op = 'AND'
WAPLimitServer = 'SQL2012Prod03'
WAPLimitDatabase = 'DataWarehouse'
WAPLimitTable = 'D_ACC_Act_Water_TakeWaterWAPAllocation'

WAPLimit = pdsql.mssql.rd_sql(
                   server = WAPLimitServer,
                   database = WAPLimitDatabase, 
                   table = WAPLimitTable,
                   col_names = WAPLimitCol,
                   where_op = WAPLimitwhere_op,
                   where_in = WAPLimitImportFilter
                   )

WAPLimit.rename(columns=WAPLimitColNames, inplace=True)
WAPLimit['ConsentNo'] = WAPLimit['ConsentNo'] .str.strip().str.upper()
WAPLimit['Activity'] = WAPLimit['Activity'] .str.strip().str.lower()
WAPLimit['WAP'] = WAPLimit['WAP'] .str.strip().str.upper()

#create useful WAP lists
WAPMaster = list(set(WAPLimit['WAP'].values.tolist()))

WAPBase = WAPLimit[['WAP']]
WAPBase = WAPBase.drop_duplicates()

WAPLink = WAPLimit[['ConsentNo','Activity','WAP','WAPToMonth','WAPFromMonth','WAPRate']]
WAPLink = WAPLink.drop_duplicates()

Consent_WAP = WAPLimit[['ConsentNo','WAP',]]
Consent_WAP = Consent_WAP.drop_duplicates()

WAPRate = WAPLimit[['ConsentNo','WAP','WAPRate','MaxRateProRata']]
WAPRate = WAPRate.drop_duplicates()

print('WAPMaster ',len(WAPMaster), '\n',
      
      '\nWAPBase ', WAPBase.shape, '\n',
      WAPBase.nunique(), '\n\n',
      
      '\nWAPLink ', WAPLink.shape, '\n',
      WAPLink.nunique(), '\n\n',    
      
      '\nConsent_WAP ', Consent_WAP.shape, '\n',
      Consent_WAP.nunique(), '\n\n',
      
      '\nWAPRate ', WAPRate.shape, '\n',
      WAPRate.nunique(), '\n\n'     
            
      )

# Reshape Data


temp = WAPLimit.reindex(WAPLimit.index.repeat(repeats = 5))
temp['TempRow']=temp.groupby(level=0).cumcount()+1

conditions = [
        ((temp.TempRow == 1) & (temp['DailyIsCondition'].str.upper() == 'YES')),
        ((temp.TempRow == 2) & (temp['WeeklyIsCondition'].str.upper() == 'YES')),
        ((temp.TempRow == 3) & (temp['30DayIsCondition'].str.upper() == 'YES')),
        ((temp.TempRow == 4) & (temp['150DayIsCondition'].str.upper() == 'YES')),
        ((temp.TempRow == 5) & (temp['CustomVol_m3'].notnull()))
               ]
choicesNVol = [
               temp['DailyVol_m3'],
               temp['WeeklyVol_m3'],
               temp['30DayVol_m3'],
               temp['150DayVol_m3'],
               temp['CustomVol_m3']
               ]
choicesNDay = [1,7,30,150,temp['CustomPeriodDays']]

temp['WAPNVol'] = np.select(conditions, choicesNVol, default = np.nan)
temp['WAPNDay'] = np.select(conditions, choicesNDay, default = np.nan)

temp = temp[pd.notnull(temp['WAPNDay'])]

temp = pd.merge(temp, WAPLink, on = ['ConsentNo','Activity','WAP','WAPToMonth','WAPFromMonth','WAPRate'], how = 'outer')

temp['WAPLimit'] = 1

###need rate stuff too.
temp = temp.drop(['DailyVol_m3', 
            'DailyIsCondition', 
            'WeeklyVol_m3',
            'WeeklyIsCondition',
            '30DayVol_m3',
            '30DayIsCondition',
            '150DayVol_m3',
            '150DayIsCondition',
            'CustomVol_m3',
            'CustomPeriodDays',
            'TempRow'
            ], axis=1)

WAPLimit = temp.drop_duplicates(subset =
        ['ConsentNo',
         'WAP',
         'Activity',
         'WAPFromMonth',
         'WAPToMonth'],keep= 'first') #size is 5514 there are 17 duplicates 34 duplicates across all


print('\nWAPLimit Table',
      WAPLimit.shape,'\n',
      WAPLimit.nunique(), '\n\n')


#Consent Limit Information
ConsentLimitCol = [
        'RecordNumber',
        'Activity',
        'ConsentedAnnualVolume_m3year',
        'ConsentMaxRate_ls',
        'ConsentMaxVol_m3',
        'ConsentMaxConsecDays',
        'FishScreen',
        'ComplexAllocations',
        'HasALowflowRestrictionCondition'
        ]
ConsentLimitColNames = {
        'RecordNumber' : 'ConsentNo',
        'ConsentedAnnualVolume_m3year' : 'ConsentAnnualVol',
        'ConsentMaxRate_ls' : 'ConsentRate',
        'ConsentMaxVol_m3' : 'ConsentNVol',
        'ConsentMaxConsecDays' : 'ConsentNDay'
        }
ConsentLimitImportFilter = {
        'Activity' : ['Take Surface Water','Take Groundwater'],
        'RecordNumber' : ConsentMaster
        }
ConsentLimitwhere_op = 'AND'
ConsentLimitServer = 'SQL2012Prod03'
ConsentLimitDatabase = 'DataWarehouse'
ConsentLimitTable = 'D_ACC_Act_Water_TakeWaterPermitAuthorisation'

ConsentLimit = pdsql.mssql.rd_sql(
                   server = ConsentLimitServer,
                   database = ConsentLimitDatabase, 
                   table = ConsentLimitTable,
                   col_names = ConsentLimitCol,
                   where_op = ConsentLimitwhere_op,
                   where_in = ConsentLimitImportFilter
                   )

ConsentLimit.rename(columns=ConsentLimitColNames, inplace=True)
ConsentLimit['ConsentNo'] = ConsentLimit['ConsentNo'] .str.strip().str.upper()
ConsentLimit['Activity'] = ConsentLimit['Activity'] .str.strip().str.lower()

ConsentLimit['ConsentLimit'] = 1

ConsentLimit = ConsentLimit.dropna(subset=[
        'ConsentAnnualVol',
        'ConsentRate',
        'ConsentNVol',
        'ConsentNDay'], how='all')

print('\nConsentLimit Table',
      ConsentLimit.shape,'\n',
      ConsentLimit.nunique(), '\n\n')


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
Location['ConsentNo'] = Location['ConsentNo'] .str.strip().str.upper()

# Aggregate Location table to consent level
Location = Location.groupby('ConsentNo', as_index=False).agg({'CWMSZone': 'max'})
Location.rename(columns = {'max':'CWMSZone'}, inplace=True)

print('\nLocation Table ',
      Location.shape,'\n',
      Location.nunique(), '\n\n')


# Full Effective Volume
FEVCol = [
        'RecordNumber',
        'Activity',
        'FullAnnualVolume_m3year',
        ]
FEVColNames = {
        'RecordNumber': 'ConsentNo',       
        'FullAnnualVolume_m3year' : 'FEVolume'  
        }
FEVImportFilter = {
        'RecordNumber' : ConsentMaster       
        }
FEVServer = 'SQL2012Prod03'
FEVDatabase = 'DataWarehouse'
FEVTable = 'D_ACC_Act_Water_TakeWaterPermitVolume'#'D_ACC_Act_Water_TakeWaterAllocData'

FEV = pdsql.mssql.rd_sql(
                   server = FEVServer,
                   database = FEVDatabase, 
                   table = FEVTable,
                   col_names = FEVCol,
                   where_in = FEVImportFilter
                   )
FEV = FEV.drop_duplicates()
FEV.rename(columns=FEVColNames, inplace=True)
FEV['ConsentNo'] = FEV['ConsentNo'] .str.strip().str.upper()
FEV['Activity'] = FEV['Activity'] .str.strip().str.lower()


FEV = FEV.groupby(
  ['ConsentNo', 'Activity'], as_index = False
  ).agg({'FEVolume' : 'sum'})

## add environmental risk
##SQL
##,(case when ((Activity like '%ground%' and FEVolume>=1500000) or (Activity like '%surface%' and CombinedRate>=100)) then 'High risk' else Null end) as EnvironmentalRisk
#conditions = [
#    (Baseline.Activity == 'ground') & (Baseline.FEVolume >=1500000),
#    (Baseline.Activity == 'surface') & (Baseline.CombinedRate>=100)
#             ]
#choices = ['High risk','High risk']
#Baseline['EnvironmentalRisk'] = np.select(conditions, choices, default = Null)
#

print('\nFEV Table ',
      FEV.shape,'\n',
      FEV.nunique(), '\n\n')


# Adaptive Managment
AdaptiveManagementCol = [
        'RecordNumber',
        'AllocationBlock'
        ]
AdaptiveManagementColNames = {
        'RecordNumber': 'ConsentNo',
        'AllocationBlock' : 'AdMan'
        }
AdaptiveManagementImportFilter = {
        'AllocationBlock' : ['Adapt.Vol']
        }
AdaptiveManagementServer = 'SQL2012Prod03'
AdaptiveManagementDatabase = 'DataWarehouse'
AdaptiveManagementTable = 'D_ACC_Act_Water_TakeWaterPermitVolume'#'D_ACC_Act_Water_TakeWaterAllocData'

AdaptiveManagement = pdsql.mssql.rd_sql(
                   server = AdaptiveManagementServer,
                   database = AdaptiveManagementDatabase, 
                   table = AdaptiveManagementTable,
                   col_names = AdaptiveManagementCol,
                   where_in = AdaptiveManagementImportFilter
                   )
AdaptiveManagement = AdaptiveManagement.drop_duplicates()
AdaptiveManagement.rename(columns=AdaptiveManagementColNames, inplace=True)
AdaptiveManagement['ConsentNo'] = AdaptiveManagement['ConsentNo'] .str.strip().str.upper()

AdManMaster = list(set(AdaptiveManagement['ConsentNo'].values.tolist()))

#pull consent details on Adaptive Managment consents
ConsentAMCol = [
        'B1_ALT_ID',
        'B1_APPL_STATUS',
        'fmDate',
        'toDate',
        'HolderAddressFullName',
        'HolderEcanID',
        'MonOfficerDepartment',
        'MonOfficer'
        ]
ConsentAMColNames = {
        'B1_ALT_ID' : 'ConsentNo',
        'MonOfficerDepartment' : 'MonDepartment'
        }
ConsentAMImportFilter = {
       'B1_ALT_ID' : AdManMaster    
        }
ConsentAMServer = 'SQL2012Prod03'
ConsentAMDatabase = 'DataWarehouse'
ConsentAMTable = 'F_ACC_PERMIT'

ConsentAM = pdsql.mssql.rd_sql(
                   server = ConsentAMServer,
                   database = ConsentAMDatabase, 
                   table = ConsentAMTable,
                   col_names = ConsentAMCol,
                   where_in = ConsentAMImportFilter
                   )

ConsentAM.rename(columns=ConsentAMColNames, inplace=True)
ConsentAM['ConsentNo'] = ConsentAM['ConsentNo'].str.strip().str.upper()


AdaptiveManagementHistory = pd.merge(AdaptiveManagement, ConsentAM, on = 'ConsentNo', how = 'left')

LocationAMCol = [
        'B1_ALT_ID',
        'AttributeValue'
        ]
LocationAMColNames = {
         'B1_ALT_ID' : 'ConsentNo',
         'AttributeValue' : 'CWMSZone' 
        }
LocationAMImportFilter = {
       'B1_ALT_ID' : AdManMaster,
       'AttributeType' : ['CWMS Zone']
        }
LocationAMServer = 'SQL2012Prod03'
LocationAMDatabase = 'DataWarehouse'
LocationAMTable = 'F_ACC_SpatialAttributes2'

LocationAM = pdsql.mssql.rd_sql(
                   server = LocationAMServer,
                   database = LocationAMDatabase, 
                   table = LocationAMTable,
                   col_names = LocationAMCol,
                   where_in = LocationAMImportFilter
                   )

LocationAM = LocationAM.drop_duplicates()
LocationAM.rename(columns=LocationAMColNames, inplace=True)
LocationAM['ConsentNo'] = LocationAM['ConsentNo'] .str.strip().str.upper()

# Aggregate Location table to consent level
LocationAM = LocationAM.groupby('ConsentNo', as_index=False).agg({'CWMSZone': 'max'})
LocationAM.rename(columns = {'max':'CWMSZone'}, inplace=True)

AdaptiveManagementHistory = pd.merge(AdaptiveManagementHistory, LocationAM, on = 'ConsentNo', how = 'left')

AdaptiveManagementHistory.to_csv('AdaptiveManagementHistory.csv')

#AdaptiveManagement['toDate'] = pd.to_datetime(AdaptiveManagement['toDate'], errors = 'coerce')
#
#AdaptiveManagement = AdaptiveManagement[AdaptiveManagement['fmDate'] < '2019-07-01']
#AdaptiveManagement = AdaptiveManagement[AdaptiveManagement['toDate'] < '2018-10-01']
#AdaptiveManagement = AdaptiveManagement.drop([
#                                            'Activity',
#                                            'B1_APPL_STATUS',
#                                            'fmDate',
#                                            'toDate',
#                                            'HolderAddressFullName',
#                                            'HolderEcanID'
#                                            ], axis=1)


print('AdManMaster ', len(AdManMaster),
      '\n\nAdaptiveManagement Table ',
      AdaptiveManagement.shape,'\n',
      AdaptiveManagement.nunique(), '\n\n')


# Water User Groups (SMG)
SMGCol = [
        'B1_ALT_ID',
        'HolderAddressFullName',
        'MonOfficer'
        ]
SMGColNames = {
        'B1_ALT_ID' : 'WUGNo',
        'MonOfficer' : 'WUGMonOfficer',
        'HolderAddressFullName' :'WUGName'
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
SMG['WUGNo'] = SMG['WUGNo'] .str.strip().str.upper()

SMGMaster = SMG['WUGNo'].values.tolist()


RelCol = [
        'ParentRecordNo',
        'ChildRecordNo'
        ]
RelColNames = {
        'ParentRecordNo' : 'WUGNo',
        'ChildRecordNo' : 'ConsentNo'
        }
RelImportFilter = {
        'ParentRecordNo' : SMGMaster
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
Rel['WUGNo'] = Rel['WUGNo'] .str.strip().str.upper()
Rel['ConsentNo'] = Rel['ConsentNo'] .str.strip().str.upper()

WUG = pd.merge(SMG, Rel, on = 'WUGNo', how = 'inner')


print('SMGMaster ', len(SMGMaster),
      '\n\nWUG Table ',
      WUG.shape,'\n',
      WUG.nunique(), '\n\n')


# Shared WAP details
temp = pd.merge(WAPRate, ConsentDetails, on = 'ConsentNo', how = 'inner')

SharedWAP = temp.groupby(
  ['WAP'], as_index=False
  ).agg(
          {
            'WAPRate' : 'max',
            'MaxRateProRata': 'max',
            'ConsentNo' : pd.Series.nunique,
            'HolderEcanID' : pd.Series.nunique,
          }
        )
SharedWAP.rename(columns = 
         {
          'WAPRate' :'MaxRateSharedWAP',
          'MaxRateProRata': 'MaxProRataRateSharedWAP',
          'ConsentNo' : 'ConsentsOnWAP',
          'HolderEcanID' : 'ECsOnWAP'
                 },inplace=True)

print('\nSharedWAP Table ',
      SharedWAP.shape,'\n',
      SharedWAP.nunique(), '\n\n')
#NeighborlyWAP = SharedWAP[(SharedWAP.ConsentsOnWAP > 1) & (SharedWAP.ECsOnWAP > 1) & (SharedWAP.MaxRateSharedWAP >= 5.0)]



#Shared Consent Details
SharedConsent = temp.groupby(
        ['ConsentNo'], as_index=False
        ).agg(
                {
                'WAP' : pd.Series.nunique,
                }
            )
SharedConsent.rename(columns = 
         {
          'WAP' :'WAPsOnConsent',
         },inplace=True)

print('\nSharedConsent Table ',
      SharedConsent.shape,'\n',
      SharedConsent.nunique(), '\n\n')


# Telemetry Information
TelemetryCol = [
        'WAP',
        'site',
        'end_date'
        ]
TelemetryColNames = {
        'site' : 'Meter'
        }
TelemetryImportFilter = {
       'WAP' : WAPMaster
        }
Telemetry_date_col = 'end_date'
Telemetry_from_date = TelemetryFromDate
Telemetry_to_date = TelemetryToDate
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
                   from_date= Telemetry_from_date,
                   to_date = Telemetry_to_date
                   )

Telemetry.rename(columns=TelemetryColNames, inplace=True)
Telemetry['WAP'] = Telemetry['WAP'] .str.strip().str.upper()

Telemetry['Telemetered'] = 1

Telemetry = Telemetry.groupby(
        ['WAP'], as_index=False
        ).agg(
                {
                'Meter' : pd.Series.nunique,
                }
            )
Telemetry.rename(columns = 
         {
          'Meter' :'T_MetersRecieved',
         },inplace=True)

HydroUsageCol = [
        'ExtSiteID',
        'DatasetTypeID',
        'DateTime',
        'Value'
        ]
HydroUsageColNames = {
        'ExtSiteID' : 'WAP',
        'Value' : 'DailyVolume'
        }
HydroUsageImportFilter = {
       'DatasetTypeID' : ['9','12'],
       'ExtSiteID' : WAPMaster
        }
HydroUsage_date_col = 'DateTime'
HydroUsage_from_date = TelemetryFromDate
HydroUsage_to_date = TelemetryToDate
HydroUsageServer = 'EDWProd01'
HydroUsageDatabase = 'Hydro'
HydroUsageTable = 'TSDataNumericDaily'

HydroUsage = pdsql.mssql.rd_sql(
                   server = HydroUsageServer,
                   database = HydroUsageDatabase, 
                   table = HydroUsageTable,
                   col_names = HydroUsageCol,
                   where_in = HydroUsageImportFilter,
                   date_col = HydroUsage_date_col,
                   from_date= HydroUsage_from_date,
                   to_date = HydroUsage_to_date
                   )

HydroUsage.rename(columns=HydroUsageColNames, inplace=True)
HydroUsage['WAP'] = HydroUsage['WAP'] .str.strip().str.upper()
HydroUsage['Day'] = 1

HydroUsage = HydroUsage.groupby(
        ['WAP'], as_index=False
        ).agg(
                {
                'DailyVolume' : 'sum',
                'DateTime' : 'max',
                'Day' : 'count'
                })

HydroUsage.rename(columns = 
         {
          'DailyVolume' :'T_AnnualVolume',
          'DateTime' : 'T_LatestData',
          'Day' : 'T_DaysOfData'
         },inplace=True)

Telemetry = pd.merge(HydroUsage, Telemetry, on = 'WAP', how = 'outer')

Telemetry['T_AverageDaysOfData'] = Telemetry['T_DaysOfData']// Telemetry['T_MetersRecieved']


print('\nTelemetry Table ',
      Telemetry.shape,'\n',
      Telemetry.nunique(), '\n\n')


# Well Details
WellDetailsCol = [
        'Well_No',
        'Status',
        'GWuseAlternateWell'
        ]
WellDetailsColNames = {
        'Well_No' : 'WAP',
        'Status' : 'WellStatus'
        }
WellDetailsImportFilter = {
       'Well_No' : WAPMaster
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
WellDetails['WAP'] = WellDetails['WAP'] .str.strip().str.upper()

conditions = [
        WellDetails['WellStatus'] == 'Active'
               ]
choices = ['Active']
WellDetails['WellStatus'] = np.select(conditions, choices, default = np.nan)

WellDetails['SharedMeter'] = np.where(WellDetails['GWuseAlternateWell'] != WellDetails['WAP'], WellDetails['GWuseAlternateWell'], np.nan)

WellDetails = WellDetails.drop(['GWuseAlternateWell'], axis=1)

print('\nWellDetails Table ',
      WellDetails.shape,'\n',
      WellDetails.nunique(), '\n\n')


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
       'Well_No' : WAPMaster
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
Waiver['WAP'] = Waiver['WAP'] .str.strip().str.upper()

conditions = [
        Waiver['WM_Tmp_Waiver'] == 1
               ]
choices = [1]
Waiver['Waiver'] = np.select(conditions, choices, default = 0)

conditions = [
        ((Waiver['WM_Tmp_Waiver'] == 1) & (Waiver['EPO_LAST_UPDATE'].notnull()))
               ]
choices = [Waiver['EPO_LAST_UPDATE']]
Waiver['WaiverEditDate'] = np.select(conditions, choices, default = pd.NaT)
Waiver['WaiverEditDate'] = pd.to_datetime(Waiver['WaiverEditDate'], errors = 'coerce')


Waiver = Waiver.drop(['WM_Tmp_Waiver', 
            'EPO_LAST_UPDATE'
            ], axis=1)

print('\nWaiver Table ',
      Waiver.shape,'\n',
      Waiver.nunique(), '\n\n')


#Verification
VerificationCol = [
        'Well_No',
        'DateVerified'
        ]
VerificationColNames = {
        'Well_No' : 'WAP'
        }
VerificationImportFilter = {
        'Well_No' : WAPMaster
        }
VerificationServer = 'SQL2012prod05'
VerificationDatabase = 'Wells'
VerificationTable = 'EPO_WaterMeterVerificationHistory'

Verification = pdsql.mssql.rd_sql(
                   server = VerificationServer,
                   database = VerificationDatabase, 
                   table = VerificationTable,
                   col_names = VerificationCol
                   )
Verification.rename(columns=VerificationColNames, inplace=True)
Verification['WAP'] = Verification['WAP'] .str.strip().str.upper()

Verification = Verification.groupby(
        ['WAP'], as_index=False
        ).agg({'DateVerified' : 'max'})
Verification.rename(columns = 
         {'DateVerified' :'LatestVerification',
         },inplace=True)

conditions = [
        Verification['LatestVerification'] > VerificationLimit,
        (( Verification['LatestVerification'] <= VerificationLimit) | 
                (Verification['LatestVerification'].isnull()))
               ]
choices = ['No' , 'Yes']
Verification['VerificationOutOfDate'] = np.select(conditions, choices, default = np.nan)

print('\nVerification Table ',
      Verification.shape,'\n',
      Verification.nunique(), '\n\n')


#WELLS Meters
MeterCol = [
        'Well_No',
        'DateInstalled',
        'DateDeInstalled'
        ]
MeterColNames = {
        'Well_No' : 'WAP'
        }
MeterImportFilter = {
       'Well_No' : WAPMaster       
        }
MeterServer = 'SQL2012prod05'
MeterDatabase = 'Wells'
MeterTable = 'EPO_WaterMeters'

Meter = pdsql.mssql.rd_sql(
                   server = MeterServer,
                   database = MeterDatabase, 
                   table = MeterTable,
                   col_names = MeterCol
                   )
Meter.rename(columns=MeterColNames, inplace=True)
Meter['WAP'] = Meter['WAP'] .str.strip().str.upper()

#list of current meters
Meter = Meter[Meter.DateDeInstalled.isnull()]
Meter['MetersInstalled'] = 1

Meter = Meter.groupby(
        ['WAP'],as_index=False
        ).agg({'MetersInstalled' : 'count'})

print('\nMeter Table ',
      Meter.shape,'\n',
      Meter.nunique(), '\n\n')


# DataLogger Information
DataLoggerCol = [
        'ID',
        'LoggerID',
        'Well_no',
        'DateDeinstalled',
        'DateInstalled',
        ]
DataLoggerColNames = {
        'Well_no' : 'WAP'
        }
DataLoggerImportFilter = {
        'Well_No' : WAPMaster
        }
DataLoggerServer = 'sql2012prod05'
DataLoggerDatabase = 'wells'
DataLoggerTable = 'EPO_Dataloggers'

DataLogger = pdsql.mssql.rd_sql(
                   server = DataLoggerServer,
                   database = DataLoggerDatabase, 
                   table = DataLoggerTable,
                   col_names = DataLoggerCol,
                   where_in = DataLoggerImportFilter
                   )
DataLogger.rename(columns=DataLoggerColNames, inplace=True)
DataLogger['WAP'] = DataLogger['WAP'] .str.strip().str.upper()

#filter
DataLogger = DataLogger[DataLogger.LoggerID > 1]
DataLogger = DataLogger[DataLogger['DateDeinstalled'].isnull()]
DataLogger['DataLoggersInstalled'] = 1

DataLogger = DataLogger.groupby(['WAP'],as_index=False
                                ).agg({'DataLoggersInstalled' : 'count'})


print('\nDataLogger Table',
      DataLogger.shape,'\n',
      DataLogger.nunique(), '\n\n')


# Inspection History
InspectionCol = [
        'InspectionID',
        'B1_ALT_ID',
        'Subtype',
        'InspectionStatus',
        'NextInspectionDate',
        'InspectionCompleteDate', #not used previous year
        'GA_USERID',  #not used previous year
        'ResultComment',
        'G6_ACT_TYP'
        ]
InspectionColNames = {
        'B1_ALT_ID' : 'ConsentNo',
        'Subtype' : 'InspectionSubtype',
        'GA_USERID' : 'OfficerID',
        'G6_ACT_TYP' : 'RecordType'
        }
InspectionImportFilter = {
        'B1_ALT_ID' : ConsentMaster,
        }
Inspection_date_col = 'InspectionCompleteDate' 
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
Inspection.rename(columns=InspectionColNames, inplace=True)
Inspection['ConsentNo'] = Inspection['ConsentNo'] .str.strip().str.upper()

Inspection = Inspection[Inspection['RecordType'].str.contains('Inspection')]
Inspection['Inspection'] = 1

Inspection['NonCompliance'] = np.where(
        Inspection['InspectionStatus'].str.contains('on-comp'),1, np.nan)

Inspection['NonComplianceDate'] = np.where(
        Inspection['InspectionStatus'].str.contains('on-comp'),
        Inspection['InspectionCompleteDate'], pd.NaT)

Inspection['NonComplianceDate'] = pd.to_datetime(
        Inspection['NonComplianceDate'], errors = 'coerce')


NonCompliance = Inspection.groupby(
  ['ConsentNo'], as_index=False
  ).agg(
          {
          'Inspection' : 'count',
          'NonCompliance' : 'sum',
          'NonComplianceDate' : 'max',
          }
        )

NonCompliance.rename(columns = 
         {
          'Inspection' : 'TotalInspections',
          'NonCompliance' :'CountNonCompliance',
          'NonComplianceDate' : 'LatestNonComplianceDate',
                 },inplace=True)

print('\nNonCompliance Table ',
      NonCompliance.shape,'\n',
      NonCompliance.nunique(), '\n\n')


# Campaign Participants
CampaignConsent = pd.read_csv("CampaignConsents.csv") 
CampaignWAP = pd.read_csv("CampaignWAPs.csv")

CampaignConsent = CampaignConsent.drop_duplicates(subset =
        ['ConsentNo',],keep= 'first')
CampaignWAP = CampaignWAP.drop_duplicates(subset =
        ['WAP',],keep= 'first')

print('CampaignConsent ', CampaignConsent.shape, '\n',
      CampaignConsent.dtypes,'\n',
      CampaignConsent.nunique(), '\n\n')
print('CampaignWAP ', CampaignWAP.shape, '\n',
      CampaignWAP.dtypes,'\n',
      CampaignWAP.nunique(), '\n\n')


#Summary Table
ConsentSummaryCol = [
        'SummaryConsentID',
        'Consent',
        'Activity',
        'ErrorMsg',
#        'MaxAnnualVolume', #not used previous years
#        'MaxConsecutiveDayVolume', #not used previous years
#        'NumberOfConsecutiveDays', #not used previous years
        'MaxTakeRate', #not used previous years
#        'HasFlowRestrictions', #unreliable. not used previous years
#        'ComplexAllocation', #not used previous years
#        'TotalVolumeAboveRestriction',
#        'TotalDaysAboveRestriction',
#        'TotalVolumeAboveNDayVolume', #unreliable. not used previous years
#        'TotalDaysAboveNDayVolume', #unreliable. not used previous years
#        'MaxVolumeAboveNDayVolume', #unreliable. not used previous years
#        'MedianVolumeAboveNDayVolume', #unreliable. not used previous years
        'PercentAnnualVolumeTaken',
        'TotalTimeAboveRate', #not used previous years
        'MaxRateTaken',
        'MedianRateTakenAboveMaxRate' #not available in previous year
        ]
ConsentSummaryColNames = {
        'SummaryConsentID' : 'CS_ID',
        'Consent' : 'ConsentNo',
        'ErrorMsg' : 'CS_ErrorMSG',
        'MaxTakeRate' : 'ConsentRate',
        'PercentAnnualVolumeTaken': 'CS_PercentAnnualVolume',
        'TotalTimeAboveRate' : 'CS_TimeAboveRate', #not used previous years
        'MaxRateTaken' : 'CS_MaxRate',
        'MedianRateTakenAboveMaxRate' : 'CS_MedianRateAbove'
        }
ConsentSummaryImportFilter = {
       'RunDate' : SummaryTableRunDate
        }

ConsentSummary_date_col = 'RunDate'
ConsentSummary_from_date = SummaryTableRunDate
ConsentSummary_to_date = SummaryTableRunDate
ConsentSummaryServer = 'SQL2012Prod03'
ConsentSummaryDatabase =  'Hilltop'
ConsentSummaryTable = 'ComplianceSummaryConsent' #change after run is finished

ConsentSummary = pdsql.mssql.rd_sql(
                   server = ConsentSummaryServer,
                   database = ConsentSummaryDatabase, 
                   table = ConsentSummaryTable,
                   col_names = ConsentSummaryCol,
                   date_col = ConsentSummary_date_col,
                   from_date= ConsentSummary_from_date,
                   to_date = ConsentSummary_to_date
                   )

ConsentSummary.rename(columns=ConsentSummaryColNames, inplace=True)
ConsentSummary['ConsentNo'] = ConsentSummary['ConsentNo'] .str.strip().str.upper()
ConsentSummary['Activity'] = ConsentSummary['Activity'] .str.strip().str.lower()

ConsentSummary['CS_PercentMaxRate'] = (
        ConsentSummary['CS_MaxRate']/ConsentSummary['ConsentRate'])*100 
ConsentSummary['CS_PercentMedRate'] = (
        ConsentSummary['CS_MedianRateAbove']/ConsentSummary['ConsentRate'])*100

print('\nConsentSummary Table ',
      ConsentSummary.shape,'\n',
      ConsentSummary.nunique(), '\n\n')

# SummaryConsent = SummaryConsent[SummaryConsent['RunDate'] == SummaryTableRunDate]

WAPSummaryCol = [
        'SummaryWAPID',
        'Consent',
        'Activity',
        'WAP',
        'MeterName',
        'ErrorMsg',
#        'MaxAnnualVolume', #not used previous years
#        'MaxConsecutiveDayVolume', #not used previous years
#        'NumberOfConsecutiveDays', #not used previous years
        'MaxTakeRate', #not used previous years
        'FromMonth',#not used previous years
        'ToMonth',#not used previous years
#        'TotalVolumeAboveRestriction',
#        'TotalDaysAboveRestriction',
#        'TotalVolumeAboveNDayVolume', #unreliable. not used previous years
#        'TotalDaysAboveNDayVolume', #unreliable. not used previous years
#        'MaxVolumeAboveNDayVolume', #unreliable. not used previous years
#        'MedianVolumeTakenAboveMaxVolume', #unreliable. not used previous years
        'PercentAnnualVolumeTaken',
        'TotalTimeAboveRate', #not used previous years
        'MaxRateTaken',#not used previous years
        'MedianRateTakenAboveMaxRate', #not available in previous year
        'TotalMissingRecord'#unreliable.
        ]
WAPSummaryColNames = {
        'SummaryWAPID' : 'WS_ID',
        'Consent' : 'ConsentNo',
        'MaxTakeRate' : 'WAPRate',
        'FromMonth' : 'WAPFromMonth',
        'ToMonth' : 'WAPToMonth',
        'MeterName' : 'WS_MeterName',
        'ErrorMsg' : 'WS_ErrorMsg',
        'PercentAnnualVolumeTaken': 'WS_PercentAnnualVolume',
        'TotalTimeAboveRate' : 'WS_TimeAboveRate',
        'MaxRateTaken' : 'WS_MaxRate',#not used previous years
        'MedianRateTakenAboveMaxRate' : 'WS_MedianRateAbove', #not available in previous year
        'TotalMissingRecord' : 'WS_TotalMissingRecord'
        }
WAPSummaryImportFilter = {
       'RunDate' : SummaryTableRunDate
        }

WAPSummary_date_col = 'RunDate'
WAPSummary_from_date = SummaryTableRunDate
WAPSummary_to_date = SummaryTableRunDate
WAPSummaryServer = 'SQL2012Prod03'
WAPSummaryDatabase =  'Hilltop'
WAPSummaryTable = 'ComplianceSummaryWAP' #change after run is finished

WAPSummary = pdsql.mssql.rd_sql(
                   server = WAPSummaryServer,
                   database = WAPSummaryDatabase, 
                   table = WAPSummaryTable,
                   col_names = WAPSummaryCol,
                   date_col = WAPSummary_date_col,
                   from_date= WAPSummary_from_date,
                   to_date = WAPSummary_to_date
                   )

WAPSummary.rename(columns=WAPSummaryColNames, inplace=True)
WAPSummary['ConsentNo'] = WAPSummary['ConsentNo'] .str.strip().str.upper()
WAPSummary['Activity'] = WAPSummary['Activity'] .str.strip().str.lower()
WAPSummary['WAP'] = WAPSummary['WAP'] .str.strip().str.upper()

WAPSummary['WS_PercentMaxRoT'] = (
        WAPSummary['WS_MaxRate']/WAPSummary['WAPRate'])*100
WAPSummary['WS_PercentMedRoT'] = (
        WAPSummary['WS_MedianRateAbove']/WAPSummary.WAPRate)*100        

print('\nWAPSummary Table ',
      WAPSummary.shape,'\n',
      WAPSummary.nunique(), '\n\n')




#
## label 5ls
##where 
##  (CombinedRate is null and MaxRate >= 5) 
##  or
##  CombinedRate>=5 -- what about where there is no proper rate, or rate is crazy small?
#
#conditions = [
#    (Baseline.CombinedRate == Null) & (Baseline.MaxRate >= 5),
#    (Baseline.CombinedRate >= 5)
#             ]
#choices = ['Above','Above']
#Baseline['FiveLS'] = np.select(conditions, choices, default = 'Under')
#
#TempConsent = Baseline.loc[Baseline['FiveLS'] == 'Over']
#                 


          
##
## SQL
## (Case when (COUNT(Distinct([From Month]))>1 or  COUNT( DISTINCT([To Month]))>1) then Max([Max Rate for WAP (l/s)]) else Null end) as MaxOfDifferentPeriodRate
#conditions = [
#    (Baseline.ToMonthCount > 1),
#    (Baseline.FromMonthCount > 1)
#             ]
#choices = [Baseline['WAPRate'],Baseline['WAPRate']]
#Baseline['FiveLS'] = np.select(conditions, choices, default = np.nan)
                           
                           


###########################################################
#Joining Tables
###########################################################
# build Consent level information
AllLimit = pd.merge(ConsentLimit, WAPLimit, on = ['ConsentNo','Activity'], how = 'outer')
AllLimit = pd.merge(AllLimit, FEV, on = ['ConsentNo','Activity'], how = 'left')

#AllSummary = pd.merge(ConsentLimit, WAPSummary, on = ['ConsentNo','Activity'], how = 'outer')

Consent = pd.merge(ConsentBase, ConsentDetails, on = 'ConsentNo', how = 'left')
Consent = pd.merge(Consent, Location, on = 'ConsentNo', how = 'left')
Consent = pd.merge(Consent, AdaptiveManagement, on = 'ConsentNo', how = 'left')
Consent = pd.merge(Consent, WUG, on = 'ConsentNo', how = 'left')
Consent = pd.merge(Consent, NonCompliance, on = 'ConsentNo' , how = 'left' )
Consent = pd.merge(Consent, SharedConsent, on = 'ConsentNo' , how = 'left' )
Consent = pd.merge(Consent, CampaignConsent, on = 'ConsentNo' , how = 'left' )

WAP = pd.merge(WAPBase, WellDetails, on = 'WAP', how = 'left')
WAP = pd.merge(WAP, SharedWAP, on = 'WAP', how = 'left')
WAP = pd.merge(WAP, Meter, on = 'WAP', how = 'left')
WAP = pd.merge(WAP, DataLogger, on = 'WAP', how = 'left')
WAP = pd.merge(WAP, Verification, on = 'WAP', how = 'left')
WAP = pd.merge(WAP, Waiver, on = 'WAP', how = 'left')
WAP = pd.merge(WAP, Telemetry, on = 'WAP', how = 'left')
WAP = pd.merge(WAP, CampaignWAP, on = 'WAP', how = 'left')

Baseline = pd.merge(WAP, AllLimit, on = 'WAP', how = 'right')
Baseline = pd.merge(Consent, Baseline, on = 'ConsentNo', how = 'right')

#Baesline = pd.merge(Baseline, AllSummary, 
#        on = ['ConsentNo','WAP','Activity','WAPFromMonth','WAPToMonth','WAPRate'], 
#        how = 'left')

Baseline = pd.merge(Baseline, ConsentSummary, 
        on = ['ConsentNo','Activity','ConsentRate',], 
        how = 'left')
Baseline = pd.merge(Baseline, WAPSummary, 
        on = ['ConsentNo','WAP','Activity','WAPFromMonth','WAPToMonth','WAPRate'], 
        how = 'left')

Baseline.to_csv('Baseline.csv')

print('\nBaseline Table ',
      Baseline.shape,'\n',
      Baseline.nunique(), '\n\n')

####################################################################################
############################################################################




#© 2019 GitHub, Inc.
#Terms
#Privacy
#Security
#Status
#Help
#Contact GitHub
#Pricing
#API
#Training
#Blog
#About
#© 2019 GitHub, Inc.

#
#example = pd.DataFrame([
#        ['Mr. Smith', 'Mr. Smith','Mr. Jones','Mr. Jones','Mr. Jones'], 
#        ['CRC1','CRC1','CRC2','CRC3','CRC3'], 
#        ['NCx','NCy','NCx','NCy','NCz']
#        ]).T
#
#output = pd.DataFrame([
#        ['Mr. Smith', 'Dear Mr. Smith, there is NCx and NCy on CRC1'], 
#        ['Mr.Jones', 'Dear Mr. Jones, there is NCx on CRC2 and NCy and NCz on CRC3']
#        ])
#
#ex2 = example.groupby([0, 1])[2].apply(lambda x: ', and '.join(x))
#ex2 = example.groupby([0, 1])[2].apply(lambda x: ', and '.join(x)).reset_index()
#ex2['crc_nc'] = ex2[1] + ' has ' + ex2[2]
#ex2.groupby(0).crc_nc.apply(lambda x: ', and '.join(x))
#
#
#'CRC1 has NCx and NCy'
#'CRC2 has NCx. CRC3 has NCy and NCz'