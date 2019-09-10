# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 14:52:35 2019
@author: KatieSi
"""

##############################################################################
### Import Packages
##############################################################################

import numpy as np
import pandas as pd
import pdsql
from datetime import datetime, timedelta, date


##############################################################################
### Set Variables
##############################################################################

ReportName= 'Water Inspection Prioritzation Model - Baseline'
RunDate = str(date.today())
#ConsentMaster = []
#WAPMaster = []


##############################################################################
### Set Risk Paramters
##############################################################################

TermDate = '2018-10-01'
TermDate = datetime.strptime(TermDate, '%Y-%m-%d')
IssuedDate = '2019-07-01'
TelemetryFromDate = '2018-07-01'# previous model Jan-1
TelemetryToDate = '2019-06-30' #non existant in previous model
InspectionStartDate = '2016-07-01' #'2018-07-13'
InspectionEndDate = '2019-07-01' #'2019-11-01'
SummaryTableRunDate = '2019-09-06'#'2019-08-20'
VerificationLimit = '2014-07-01'
WaiverLimit = '2018-07-01'


##############################################################################
### Import Data and Calculate Statistics
##############################################################################

##############################################################################
### Import Targeted Consents


### Import Base Consent Information
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
       'B1_APPL_STATUS' : ['Terminated - Replaced',
                           'Issued - Active',
                           'Issued - s124 Continuance']      
        }
ConsentDetailswhere_op = 'AND'
ConsentDetailsDate_col = 'toDate'
ConsentDetailsfrom_date = '2018-10-1' #dates are inclusive in PY - exclusive in SQL
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

# Format data
ConsentDetails.rename(columns=ConsentDetailsColNames, inplace=True)
ConsentDetails['ConsentNo'] = ConsentDetails['ConsentNo'].str.strip().str.upper()

# Remove consents issued after FY28/19
ConsentInfo = ConsentDetails[ConsentDetails['fmDate'] < '2019-07-01']

# Create list of consents active in this financial year
ConsentMaster = list(set(ConsentDetails['ConsentNo'].values.tolist()))

# Create dataframe of consents active in this financial year
ConsentBase = ConsentDetails[['ConsentNo']]
ConsentBase = ConsentBase.drop_duplicates()



# Calculate info on consents parent tree
conditions = [
        ConsentDetails.ConsentStatus == 'Terminated - Replaced',
        ((ConsentDetails.ConsentStatus == 'Issued - Active') |
                (ConsentDetails.ConsentStatus == 'Issued - s124 Continuance')) &
            (ConsentDetails.fmDate > TermDate),
               ]
choices = ['Child' , 'Parent']
ConsentDetails['ParentorChild'] = np.select(conditions, choices, default = np.nan)
choices2 = [ConsentDetails['ChildAuthorisations'] ,ConsentDetails['ParentAuthorisations']]
ConsentDetails['ParentChildConsent'] = np.select(conditions, choices2, default = ' ')

# Print overview of table
print('ConsentMaster ', len(ConsentMaster),
      '\nConsentBase ', ConsentBase.shape[0],
      '\n\nConsentDetails Table ',
      ConsentDetails.shape,'\n',
      ConsentDetails.nunique(), '\n\n')


##############################################################################
### Import WAPAllocation Limits

# Import table
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

# Format data
WAPLimit.rename(columns=WAPLimitColNames, inplace=True)
WAPLimit['ConsentNo'] = WAPLimit['ConsentNo'].str.strip().str.upper()
WAPLimit['Activity'] = WAPLimit['Activity'].str.strip().str.lower()
WAPLimit['WAP'] = WAPLimit['WAP'].str.strip().str.upper()

# Create list of WAPs on consents active in this financial year
WAPMaster = list(set(WAPLimit['WAP'].values.tolist()))

WAPBase = WAPLimit[['WAP']]
WAPBase = WAPBase.drop_duplicates()

WAPLink = WAPLimit[['ConsentNo',
                    'Activity',
                    'WAP',
                    'WAPToMonth',
                    'WAPFromMonth',
                    'WAPRate']]
WAPLink = WAPLink.drop_duplicates()

Consent_WAP = WAPLimit[['ConsentNo','WAP',]]
Consent_WAP = Consent_WAP.drop_duplicates()

WAPRate = WAPLimit[['ConsentNo','WAP','WAPRate','MaxRateProRata']]
WAPRate = WAPRate.drop_duplicates()

# Print overview of table
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

##############################################################################
### Reshape Data to ease access to NDay volume allocation limits

# Duplicate each record 5 times
temp = WAPLimit.reindex(WAPLimit.index.repeat(repeats = 5))

# Add a counter
temp['TempRow']=temp.groupby(level=0).cumcount()+1

# Move NDay volume info to new columns
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

# Remove rows w/o Nday volume
temp = temp[pd.notnull(temp['WAPNDay'])]

# Reinsert WAP allocation records without NDay volume allocation
temp = pd.merge(temp, WAPLink, on = ['ConsentNo',
                                     'Activity',
                                     'WAP',
                                     'WAPToMonth',
                                     'WAPFromMonth',
                                     'WAPRate'
                                     ], how = 'outer')

# Create marker for WAP level allocation rules
temp['WAPLimit'] = 1

# Drop unused columns
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

# Overwrite old WAP allocation limit table
WAPLimit = temp.drop_duplicates(subset =
        ['ConsentNo',
         'WAP',
         'Activity',
         'WAPFromMonth',
         'WAPToMonth'],keep= 'first')

# Print overview of table
print('\nWAPLimit Table',
      WAPLimit.shape,'\n',
      WAPLimit.nunique(), '\n\n')


##############################################################################
### Import ConsentAllocation Limits

# Import table
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
# Format data
ConsentLimit.rename(columns=ConsentLimitColNames, inplace=True)
ConsentLimit['ConsentNo'] = ConsentLimit['ConsentNo'] .str.strip().str.upper()
ConsentLimit['Activity'] = ConsentLimit['Activity'] .str.strip().str.lower()

# Create counter for consent level allocation rules
ConsentLimit['ConsentLimit'] = 1

# Drop unused records
ConsentLimit = ConsentLimit.dropna(subset=[
        'ConsentAnnualVol',
        'ConsentRate',
        'ConsentNVol',
        'ConsentNDay'], how='all')

# Print overview of table
print('\nConsentLimit Table',
      ConsentLimit.shape,'\n',
      ConsentLimit.nunique(), '\n\n')


##############################################################################
### Import Additional Consent Information

### Import Location Info
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
# Format data
Location = Location.drop_duplicates()
Location.rename(columns=LocationColNames, inplace=True)
Location['ConsentNo'] = Location['ConsentNo'] .str.strip().str.upper()

# Aggregate Location table to consent level
Location = Location.groupby('ConsentNo', as_index=False).agg({'CWMSZone': 'max'})
Location.rename(columns = {'max':'CWMSZone'}, inplace=True)

# Print overview of table
print('\nLocation Table ',
      Location.shape,'\n',
      Location.nunique(), '\n\n')


### Import Full Effective Volume Info
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
FEVTable = 'D_ACC_Act_Water_TakeWaterPermitVolume'

FEV = pdsql.mssql.rd_sql(
                   server = FEVServer,
                   database = FEVDatabase, 
                   table = FEVTable,
                   col_names = FEVCol,
                   where_in = FEVImportFilter
                   )
# Format data
FEV = FEV.drop_duplicates()
FEV.rename(columns=FEVColNames, inplace=True)
FEV['ConsentNo'] = FEV['ConsentNo'] .str.strip().str.upper()
FEV['Activity'] = FEV['Activity'] .str.strip().str.lower()

# Aggregate Location table to consent-activity level
FEV = FEV.groupby(
  ['ConsentNo', 'Activity'], as_index = False
  ).agg({'FEVolume' : 'sum'})

# Print overview of table
print('\nFEV Table ',
      FEV.shape,'\n',
      FEV.nunique(), '\n\n')


### Import Adaptive Managment Info
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
AdaptiveManagementTable = 'D_ACC_Act_Water_TakeWaterPermitVolume'

AdaptiveManagement = pdsql.mssql.rd_sql(
                   server = AdaptiveManagementServer,
                   database = AdaptiveManagementDatabase, 
                   table = AdaptiveManagementTable,
                   col_names = AdaptiveManagementCol,
                   where_in = AdaptiveManagementImportFilter
                   )
# Format data
AdaptiveManagement = AdaptiveManagement.drop_duplicates()
AdaptiveManagement.rename(columns=AdaptiveManagementColNames, inplace=True)
AdaptiveManagement['ConsentNo'] = AdaptiveManagement['ConsentNo'].str.strip().str.upper()

# Create list of consents in this financial year with Adaptive management 
AdManMaster = list(set(AdaptiveManagement['ConsentNo'].values.tolist()))

# Print overview of table
print('AdManMaster ', len(AdManMaster),
      '\n\nAdaptiveManagement Table ',
      AdaptiveManagement.shape,'\n',
      AdaptiveManagement.nunique(), '\n\n')

##############################################################################
### Create table of historical AdMan

# Pull consent details on Adaptive Managment consents
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
# Format data
ConsentAM.rename(columns=ConsentAMColNames, inplace=True)
ConsentAM['ConsentNo'] = ConsentAM['ConsentNo'].str.strip().str.upper()

# Link consent details to the AdMan consent
AdaptiveManagementHistory = pd.merge(
                                    AdaptiveManagement, 
                                    ConsentAM, 
                                    on = 'ConsentNo', 
                                    how = 'left')

# Pull location information on AdMan
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
# Format data
LocationAM = LocationAM.drop_duplicates()
LocationAM.rename(columns=LocationAMColNames, inplace=True)
LocationAM['ConsentNo'] = LocationAM['ConsentNo'] .str.strip().str.upper()

# Aggregate Location table to consent level
LocationAM = LocationAM.groupby(
                                'ConsentNo', as_index=False
                                ).agg({'CWMSZone': 'max'})
LocationAM.rename(columns = {'max':'CWMSZone'}, inplace=True)

# Add location information to AdMan
AdaptiveManagementHistory = pd.merge(
                                    AdaptiveManagementHistory, 
                                    LocationAM, 
                                    on = 'ConsentNo', 
                                    how = 'left')

# Export CSV for RMO to monitor AdMan
AdaptiveManagementHistory.to_csv('AdaptiveManagementHistory.csv')

# Print overview of table
print('\n\nAdaptiveManagementHistory Table ',
      AdaptiveManagementHistory.shape,'\n',
      AdaptiveManagementHistory.nunique(), '\n\n')

##############################################################################
### Create Water User Groups (SMG) Info

# Import Data
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
# Format data
SMG = SMG.drop_duplicates()
SMG.rename(columns=SMGColNames, inplace=True)
SMG['WUGNo'] = SMG['WUGNo'] .str.strip().str.upper()

# Create list of water user groups
SMGMaster = SMG['WUGNo'].values.tolist()

# Import information on WUG members
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
# Format data
Rel = Rel.drop_duplicates()
Rel.rename(columns=RelColNames, inplace=True)
Rel['WUGNo'] = Rel['WUGNo'] .str.strip().str.upper()
Rel['ConsentNo'] = Rel['ConsentNo'] .str.strip().str.upper()

# Add WUG info to member info
WUG = pd.merge(SMG, Rel, on = 'WUGNo', how = 'inner')

# Print overview of table
print('SMGMaster ', len(SMGMaster),
      '\n\nWUG Table ',
      WUG.shape,'\n',
      WUG.nunique(), '\n\n')

##############################################################################
### Calculate Model Complexities

# Join WAP limit info to consent details
temp = pd.merge(WAPRate, ConsentDetails, on = 'ConsentNo', how = 'inner')

# Aggregate to WAP level to calculate WAP complexities
SharedWAP = temp.groupby(
  ['WAP'], as_index=False
  ).agg(
          {
            'WAPRate' : 'max',
            'MaxRateProRata': 'sum',
            'ConsentNo' : pd.Series.nunique,
            'HolderEcanID' : pd.Series.nunique,
          }
        )
# Format data
SharedWAP.rename(columns = 
         {
          'WAPRate' :'MaxRateSharedWAP',
          'MaxRateProRata': 'CombinedWAPProRataRate',
          'ConsentNo' : 'ConsentsOnWAP',
          'HolderEcanID' : 'ECsOnWAP'
                 },inplace=True)

# Print overview of table
print('\nSharedWAP Table ',
      SharedWAP.shape,'\n',
      SharedWAP.nunique(), '\n\n')

# Aggregate to consent level to calculate consent complexities
SharedConsent = temp.groupby(
        ['ConsentNo'], as_index=False
        ).agg(
                {
                'WAP' : pd.Series.nunique,
                'MaxRateProRata': 'sum'
                }
            )
# Format data
SharedConsent.rename(columns = 
         {
          'WAP' :'WAPsOnConsent',
          'MaxRateProRata': 'CombinedConProRataRate'          
         },inplace=True)

# Print overview of table
print('\nSharedConsent Table ',
      SharedConsent.shape,'\n',
      SharedConsent.nunique(), '\n\n')

##############################################################################
### Import Timeseries Info from Hydro

# Import info on timeseries received
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
                   from_date= Telemetry_from_date
                   )
# Format data
Telemetry.rename(columns=TelemetryColNames, inplace=True)
Telemetry['WAP'] = Telemetry['WAP'] .str.strip().str.upper()

# Create counter for meters recieved
Telemetry['Telemetered'] = 1

# Aggrigate to WAP level to count meters per wap
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


# Import info on timeseries values
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
# Format data
HydroUsage.rename(columns=HydroUsageColNames, inplace=True)
HydroUsage['WAP'] = HydroUsage['WAP'] .str.strip().str.upper()

# Create counter for days when some data was recieved
HydroUsage['Day'] = 1

# Aggregate data to WAP level to get Annual volume and days of data
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

# Add meter information to timeseries info
Telemetry = pd.merge(HydroUsage, Telemetry, on = 'WAP', how = 'outer')

# fill missing meters
Telemetry = pd.merge(WAPBase, Telemetry, on = 'WAP', how = 'outer')


# Calculate average days on each meter
Telemetry['T_AverageDaysOfData'] = Telemetry['T_DaysOfData'] // Telemetry['T_MetersRecieved']
Telemetry['T_AverageMissingDays'] = 365 - Telemetry['T_AverageDaysOfData']


Telemetry['T_AverageMissingDays'] = Telemetry['T_AverageMissingDays'].fillna(value=365)

# Print overview of table
print('\nTelemetry Table ',
      Telemetry.shape,'\n',
      Telemetry.nunique(), '\n\n')

##############################################################################
### Import Well Info

# Import base info on each WAP
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
# Format data
WellDetails.rename(columns=WellDetailsColNames, inplace=True)
WellDetails['WAP'] = WellDetails['WAP'] .str.strip().str.upper()

# Reduce Well Status to Active and none.
conditions = [
        WellDetails['WellStatus'] == 'Active'
               ]
choices = ['Active']
WellDetails['WellStatus'] = np.select(conditions, choices, default = '')

# Calculate shared meters
WellDetails['SharedMeter'] = np.where(
        WellDetails['GWuseAlternateWell'] != WellDetails['WAP'],
        'Yes', '')

WellDetails = WellDetails.drop([
                                'GWuseAlternateWell'], axis=1)
# Print overview of table
print('\nWellDetails Table ',
      WellDetails.shape,'\n',
      WellDetails.nunique(), '\n\n')


### Import Waiver Info
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
# Format data
Waiver.rename(columns=WaiverColNames, inplace=True)
Waiver['WAP'] = Waiver['WAP'] .str.strip().str.upper()

# Calculate waiver record edit date
conditions = [
        ((Waiver['WM_Tmp_Waiver'] == 1) & (Waiver['EPO_LAST_UPDATE'].notnull()))
               ]
choices = [Waiver['EPO_LAST_UPDATE']]
Waiver['WaiverEditDate'] = np.select(conditions, choices, default = pd.NaT)
Waiver['WaiverEditDate'] = pd.to_datetime(Waiver['WaiverEditDate'], errors = 'coerce')

# Calculate valid waivers
conditions = [
        Waiver['WaiverEditDate'] >= WaiverLimit,
        Waiver['WaiverEditDate'] < WaiverLimit
               ]
choices = ['Yes', 'OutofDate']
Waiver['Waiver'] = np.select(conditions, choices, default = np.nan)

# Remove extra columns
Waiver = Waiver.drop(['WM_Tmp_Waiver', 
            'EPO_LAST_UPDATE'
            ], axis=1)

# Print overview of table
print('\nWaiver Table ',
      Waiver.shape,'\n',
      Waiver.nunique(), '\n\n')


### Import Verification Info
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
# Format data
Verification.rename(columns=VerificationColNames, inplace=True)
Verification['WAP'] = Verification['WAP'] .str.strip().str.upper()

# Filter to latest verification
Verification = Verification.groupby(
        ['WAP'], as_index=False
        ).agg({'DateVerified' : 'max'})
Verification.rename(columns = 
         {'DateVerified' :'LatestVerification',
         },inplace=True)

# Calculate valid verifications
conditions = [
        Verification['LatestVerification'] > VerificationLimit,
        (( Verification['LatestVerification'] <= VerificationLimit) | 
                (Verification['LatestVerification'].isnull()))
               ]
choices = ['No' , 'Yes']
Verification['VerificationOutOfDate'] = np.select(conditions, choices, default = 'Yes')

# Print overview of table
print('\nVerification Table ',
      Verification.shape,'\n',
      Verification.nunique(), '\n\n')


### Import WELLS Meters Info
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
# Format data
Meter.rename(columns=MeterColNames, inplace=True)
Meter['WAP'] = Meter['WAP'] .str.strip().str.upper()

# Remove meters that have been uninstalled
Meter = Meter[Meter.DateDeInstalled.isnull()]

# Create counter for meters
Meter['MetersInstalled'] = 1

# Aggrigate to WAP level for total installed meters per WAP 
Meter = Meter.groupby(
        ['WAP'],as_index=False
        ).agg({'MetersInstalled' : 'count'})


# Print overview of table
print('\nMeter Table ',
      Meter.shape,'\n',
      Meter.nunique(), '\n\n')


### Import DataLogger Info
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
# Format data
DataLogger.rename(columns=DataLoggerColNames, inplace=True)
DataLogger['WAP'] = DataLogger['WAP'] .str.strip().str.upper()

# Remove assumed dataloggers
DataLogger = DataLogger[DataLogger.LoggerID > 1]

# Remove uninstalled dataloggers
DataLogger = DataLogger[DataLogger['DateDeinstalled'].isnull()]

# Create counter for dataloggers
DataLogger['DataLoggersInstalled'] = 1

# Aggrigate to WAP level for total dataloggers per WAP
DataLogger = DataLogger.groupby(['WAP'],as_index=False
                                ).agg({'DataLoggersInstalled' : 'count'})

# Print overview of table
print('\nDataLogger Table',
      DataLogger.shape,'\n',
      DataLogger.nunique(), '\n\n')

##############################################################################
### Import Inspection History Info

# Import Inspections
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
# Format data
Inspection.rename(columns=InspectionColNames, inplace=True)
Inspection['ConsentNo'] = Inspection['ConsentNo'] .str.strip().str.upper()

# Reduce list to only Inspection type records
Inspection = Inspection[Inspection['RecordType'].str.contains('Inspection')]

# Create counter for inspections
Inspection['Inspection'] = 1

# Calculate inspections with Non-compliance grade
Inspection['NonCompliance'] = np.where(
        Inspection['InspectionStatus'].str.contains('on-comp'),1, np.nan)

# Calculate date of inspections with non-compliance grade
Inspection['NonComplianceDate'] = np.where(
        Inspection['InspectionStatus'].str.contains('on-comp'),
        Inspection['InspectionCompleteDate'], pd.NaT)
Inspection['NonComplianceDate'] = pd.to_datetime(
        Inspection['NonComplianceDate'], errors = 'coerce')

# Aggrigate to consent level to find latest inspection statistics
NonCompliance = Inspection.groupby(
  ['ConsentNo'], as_index=False
  ).agg(
          {
          'Inspection' : 'count',
          'InspectionCompleteDate' : 'max',
          'NonCompliance' : 'sum',
          'NonComplianceDate' : 'max',
          }
        )
NonCompliance.rename(columns = 
         {
          'Inspection' : 'TotalInspections',
          'InspectionCompleteDate' :'LatestInspection',
          'NonCompliance' :'CountNonCompliance',
          'NonComplianceDate' : 'LatestNonComplianceDate',
                 },inplace=True)

# Print overview of table
print('\nNonCompliance Table ',
      NonCompliance.shape,'\n',
      NonCompliance.nunique(), '\n\n')

##############################################################################
### Import information from teams not stored in datawarehouse

### Import Campaign Participants
# Import data from campaigns team
CampaignConsent = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\CampaignConsents.csv")
CampaignWAP = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\CampaignWAPs.csv")

# Remove duplicates
CampaignConsent = CampaignConsent.drop_duplicates(subset =
        ['ConsentNo',],keep= 'first')
CampaignWAP = CampaignWAP.drop_duplicates(subset =
        ['WAP',],keep= 'first')

# Print overview of tables
print('CampaignConsent ', CampaignConsent.shape, '\n',
      CampaignConsent.nunique(), '\n\n')
print('CampaignWAP ', CampaignWAP.shape, '\n',
      CampaignWAP.nunique(), '\n\n')

### Import Midseason Inspections
# Import data from inspections team
MidseasonInspections = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\MidseasonInspections.csv")

##############################################################################
### Import Summary Table

### Import Consent Summary Table
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
        'TotalDaysAboveNDayVolume', # added for second run
#        'MaxVolumeAboveNDayVolume', #unreliable. not used previous years
#        'MedianVolumeAboveNDayVolume', #unreliable. not used previous years
        'PercentAnnualVolumeTaken',
        'TotalTimeAboveRate', # not used previous years
        'MaxRateTaken', 
        'MedianRateTakenAboveMaxRate' #not available in previous year
        ]
ConsentSummaryColNames = {
        'SummaryConsentID' : 'CS_ID',
        'Consent' : 'ConsentNo',
        'ErrorMsg' : 'CS_ErrorMSG',
        'MaxTakeRate' : 'ConsentRate',
        'TotalDaysAboveNDayVolume' : 'CS_DaysAboveNday',
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
ConsentSummaryServer = 'SQL2012Test01'#'SQL2012Prod03'
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

# Format data
ConsentSummary.rename(columns=ConsentSummaryColNames, inplace=True)
ConsentSummary['ConsentNo'] = ConsentSummary['ConsentNo'] .str.strip().str.upper()
ConsentSummary['Activity'] = ConsentSummary['Activity'] .str.strip().str.lower()

# Calculate usage statistics
ConsentSummary['CS_PercentMaxRate'] = (
        ConsentSummary['CS_MaxRate']/ConsentSummary['ConsentRate'])*100 
ConsentSummary['CS_PercentMedRate'] = (
        ConsentSummary['CS_MedianRateAbove']/ConsentSummary['ConsentRate'])*100

# Print overview of table
print('\nConsentSummary Table ',
      ConsentSummary.shape,'\n',
      ConsentSummary.nunique(), '\n\n')

# SummaryConsent = SummaryConsent[SummaryConsent['RunDate'] == SummaryTableRunDate]

### Import WAP Summary Table
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
        'TotalDaysAboveNDayVolume', # Hope to use
#        'MaxVolumeAboveNDayVolume', #unreliable. not used previous years
#        'MedianVolumeTakenAboveMaxVolume', #unreliable. not used previous years
        'TotalTimeAboveRate', #not used previous years
        'MaxRateTaken',# Now unreliable. not used previous years
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
        'TotalDaysAboveNDayVolume' : 'WS_DaysAboveNday',
        'TotalTimeAboveRate' : 'WS_TimeAboveRate',
        'MaxRateTaken' : 'WS_MaxRate', # not used previous years
        'MedianRateTakenAboveMaxRate' : 'WS_MedianRateAbove', # not available in previous year
        'TotalMissingRecord' : 'WS_TotalMissingRecord'
        }
WAPSummaryImportFilter = {
       'RunDate' : SummaryTableRunDate
        }

WAPSummary_date_col = 'RunDate'
WAPSummary_from_date = SummaryTableRunDate
WAPSummary_to_date = SummaryTableRunDate
WAPSummaryServer = 'SQL2012Test01'#'SQL2012Prod03'
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
# Format data
WAPSummary.rename(columns=WAPSummaryColNames, inplace=True)
WAPSummary['ConsentNo'] = WAPSummary['ConsentNo'] .str.strip().str.upper()
WAPSummary['Activity'] = WAPSummary['Activity'] .str.strip().str.lower()
WAPSummary['WAP'] = WAPSummary['WAP'] .str.strip().str.upper()

# Calculate usage stattistics
WAPSummary['WS_PercentMaxRoT'] = (
        WAPSummary['WS_MaxRate']/WAPSummary['WAPRate'])*100
WAPSummary['WS_PercentMedRate'] = (
        WAPSummary['WS_MedianRateAbove']/WAPSummary.WAPRate)*100   
        
# Print overview of table
print('\nWAPSummary Table ',
      WAPSummary.shape,'\n',
      WAPSummary.nunique(), '\n\n')

                                              


##############################################################################
# Join Tables
##############################################################################

### Build Consent-Activity level allocation limit information
AllLimit = pd.merge(ConsentLimit, WAPLimit, on = ['ConsentNo','Activity'], how = 'outer')
AllLimit = pd.merge(AllLimit, FEV, on = ['ConsentNo','Activity'], how = 'left')

### Build Consent level information
Consent = pd.merge(ConsentBase, ConsentDetails, on = 'ConsentNo', how = 'left')
Consent = pd.merge(Consent, Location, on = 'ConsentNo', how = 'left')
Consent = pd.merge(Consent, AdaptiveManagement, on = 'ConsentNo', how = 'left')
Consent = pd.merge(Consent, WUG, on = 'ConsentNo', how = 'left')
Consent = pd.merge(Consent, NonCompliance, on = 'ConsentNo' , how = 'left' )
Consent = pd.merge(Consent, SharedConsent, on = 'ConsentNo' , how = 'left' )
Consent = pd.merge(Consent, CampaignConsent, on = 'ConsentNo' , how = 'left' )
Consent = pd.merge(Consent, MidseasonInspections, on = 'ConsentNo' , how = 'left' )

### Build WAP level information
WAP = pd.merge(WAPBase, WellDetails, on = 'WAP', how = 'left')
WAP = pd.merge(WAP, SharedWAP, on = 'WAP', how = 'left')
WAP = pd.merge(WAP, Meter, on = 'WAP', how = 'left')
WAP = pd.merge(WAP, DataLogger, on = 'WAP', how = 'left')
WAP = pd.merge(WAP, Verification, on = 'WAP', how = 'left')
WAP = pd.merge(WAP, Waiver, on = 'WAP', how = 'left')
WAP = pd.merge(WAP, Telemetry, on = 'WAP', how = 'left')
WAP = pd.merge(WAP, CampaignWAP, on = 'WAP', how = 'left')

### Merge 3 levels
Baseline = pd.merge(WAP, AllLimit, on = 'WAP', how = 'right')
Baseline = pd.merge(Consent, Baseline, on = 'ConsentNo', how = 'right')

### Add summary table information
Baseline = pd.merge(Baseline, ConsentSummary, 
        on = ['ConsentNo','Activity','ConsentRate',], 
        how = 'left')
Baseline = pd.merge(Baseline, WAPSummary, 
        on = ['ConsentNo','WAP','Activity','WAPFromMonth','WAPToMonth','WAPRate'], 
        how = 'left')

# Export Baseline
#Baseline.to_csv('Baseline' + RunDate + '.csv')

# Print overview of table
print('\nBaseline Table ',
      Baseline.shape,'\n',
      Baseline.nunique(), '\n\n')

###############################################################################
### End.
###############################################################################

"""
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
"""