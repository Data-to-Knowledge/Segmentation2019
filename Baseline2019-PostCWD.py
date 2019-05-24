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


# Set Risk Paramters


# Set Master Lists
ConsentMaster = []
WAPMaster = []

# Set Table Variables
# Consent Information
ConsentCol = [
        'B1_ALT_ID',
        'fmDate',
        'toDate',
        'HolderAddressFullName',
        'HolderEcanID'
        ]
ConsentColNames = {
        'B1_ALT_ID' : 'ConsentNo'
        }
ConsentImportFilter = {
       'B1_PER_SUB_TYPE' : ['Water Permit (s14)'] ,
       'B1_APPL_STATUS' : ['Terminated - Replaced', 'Issued - Active', 'Issued - s124 Continuance']
        }
Consentwhere_op = 'AND'
ConsentDate_col = 'toDate'
ConsentStart = '2018-10-1' #dates are inclusive in PY and exclusive in SQL
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
                   from_date = ConsentStart
                   )
Consent.rename(columns=ConsentColNames, inplace=True)
#filter
# consent list
ConsentMaster = Consent['ConsentNo'].values.tolist()


# WAP Information
WAPCol = [
        'RecordNo',
        'WAP',
        'Activity',
        'Max Rate Pro Rata (l/s)',
        'Max Rate for WAP (l/s)',
        'From Month',
        'To Month'        
        ]
WAPColNames = {
        'RecordNo' : 'ConsentNo'
        }
WAPImportFilter = {
        'Activity' : ['Take Surface Water','Take Groundwater'],
        'RecordNo' : ConsentMaster
        }
WAPwhere_op = 'AND'
WAPServer = 'SQL2012Prod03'
WAPDatabase = 'DataWarehouse'
WAPTable = 'D_ACC_Act_Water_TakeWaterWAPAlloc'

WAP = pdsql.mssql.rd_sql(
                   server = WAPServer,
                   database = WAPDatabase, 
                   table = WAPTable,
                   col_names = WAPCol,
                   where_op = 'AND',
                   where_in = WAPImportFilter
                   )
WAP.rename(columns=WAPColNames, inplace=True)

WAPMaster = list(set(WAP['WAP'].values.tolist()))


# Location Information
LocationCol = [
        'B1_ALT_ID',
        'AttributeValue'
        ]
LocationColNames = {
         'B1_ALT_ID' : 'RecordNo',
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

Location.rename(columns=LocationColNames, inplace=True)

x = Location.groupby(['RecordNo'])['CWMSZone'].aggregate('count')
x2 = x[x > 1] # 53 consents with more than one zone
x2.max() # 10
x2.mean() # 2.6

Location.sort_values(by = ['CWMSZone'], inplace  = True)
Location.drop_duplicates(subset = 'RecordNo', keep = 'last') #last in alphabet matches MAX() from SQL
# two consents w/o locations
# CRC185906
# CRC191696

# Full Effective Volume
FEVCol = [
        
        ]
FEVColNames = {
        
        }
FEVImportFilter = {
       
        }
FEVStart = 
FEVEnd = 
FEVServer = 
FEVDatabase = 
FEVTable = 

# Campaign Participants
CampaignCol = [
        
        ]
CampaignColNames = {
        
        }
CampaignImportFilter = {
       
        }
CampaignStart = 
CampaignEnd = 
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
TelemetryStart = 
TelemetryEnd = 
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
MeterStart = 
MeterEnd = 
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
SummaryStart = 
SummaryEnd = 
SummaryServer = 
SummaryDatabase = 
SummaryTable = 

# Inspection History
InspectionCol = [
        
        ]
InspectionColNames = {
        
        }
InspectionImportFilter = {
       
        }
InspectionStart = 
InspectionEnd = 
InspectionServer = 
InspectionDatabase = 
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