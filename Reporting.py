# -*- coding: utf-8 -*-
"""
Created on Fri Sep  6 13:17:04 2019

@author: KatieSi
"""


##############################################################################
### Import Packages
##############################################################################

import os
import pandas as pd
import pdsql
from datetime import date

##############################################################################
### Set Variables
##############################################################################

ReportName= 'Water Segmentation Inspections'
RunDate = str(date.today())
InspectionFile = 'SegmentationInspections.csv'
input_path = r'\\punakorero@SSL\DavWWWRoot\groups\regimp\Projects\WaterUseReporting\InputFiles'
output_path = r'\\punakorero@SSL\DavWWWRoot\groups\regimp\Projects\WaterUseReporting\SegmentationReports'


##############################################################################
#### Import Data
##############################################################################

SegInsp = pd.read_csv(os.path.join(input_path, InspectionFile))

SegInsp_List = SegInsp['InspectionID'].values.tolist()
Insp_col = ['InspectionID',
            'InspectionStatus',
            'B1_ALT_ID',
            'GA_FNAME',
            'GA_LNAME',
            'GA_USERID',
            'R3_DEPTNAME'
            ]
Insp_ColNames = {
        'B1_ALT_ID' : 'ConsentNo',
        'R3_DEPTNAME' : 'Zone'
        }
SegOutcomes = pdsql.mssql.rd_sql(
                    server = 'SQL2012PROD03',
                    database = 'DataWarehouse',
                    table = 'D_ACC_Inspections',
                    col_names = Insp_col,
                    where_in = {'InspectionID': SegInsp_List})


##############################################################################
### Format data
##############################################################################

SegOutcomes.rename(columns = Insp_ColNames, inplace=True)
SegOutcomes['ConsentNo'] = SegOutcomes['ConsentNo'].str.strip().str.upper()


##############################################################################
### Find Deleted Consents
##############################################################################

SegMissing_Count = SegInsp.shape[0]-SegOutcomes.shape[0]
SegOutcomes = pd.merge(SegInsp, SegOutcomes,
                        on= ['InspectionID','ConsentNo'],
                        how='outer')
SegOutcomes['InspectionStatus'] = SegOutcomes['InspectionStatus'].fillna('Missing Inspection')


##############################################################################
### Find Inspections with Missing Information
##############################################################################

SegOutcomes['Zone'] = SegOutcomes['Zone'].fillna('Missing Zone')
SegOutcomes['GA_FNAME'] = SegOutcomes['GA_FNAME'].fillna('Missing Officer')
SegOutcomes['GA_LNAME'] = SegOutcomes['GA_LNAME'].fillna('Missing Officer')
SegOutcomes['GA_USERID'] = SegOutcomes['GA_USERID'].fillna('Missing Officer')


##############################################################################
### Create Total counts
##############################################################################

### Calculate Counts per fortnight
TotalInsp = SegOutcomes.groupby(
        ['Fortnight'],as_index=False)['ConsentNo'].aggregate('count')
TotalInsp.rename(columns ={'ConsentNo' : 'Total'}, inplace=True)

### Calculate counts per zone
ZoneTotalInsp = SegOutcomes.groupby(
        ['Zone'],as_index=False)['ConsentNo'].aggregate('count')
ZoneTotalInsp.rename(columns ={'ConsentNo' : 'ZoneTotal'}, inplace=True)

### Calculate counts per zone/fortnight
FortnightTotalInsp = SegOutcomes.groupby(
        ['Zone','Fortnight'],as_index=False)['ConsentNo'].aggregate('count')
FortnightTotalInsp.rename(columns ={'ConsentNo' : 'FortnightTotal'}, inplace=True)

### Add Total counts to list
SegOutcomes = pd.merge(SegOutcomes, TotalInsp,
                        on=['Fortnight'],
                        how='left')
SegOutcomes = pd.merge(SegOutcomes, ZoneTotalInsp,
                        on=['Zone'],
                        how='left')
SegOutcomes = pd.merge(SegOutcomes, FortnightTotalInsp,
                        on=['Zone','Fortnight'],
                        how='left')


##############################################################################
### Create Zone Grade output
##############################################################################

### aggrigate to zone/grade level
ZoneGrades = SegOutcomes.groupby(['Zone','InspectionStatus','Fortnight']
                        ).agg({
                                  'Fortnight' : 'count'
                                })

### Format output
ZoneGrades = ZoneGrades.unstack()
ZoneGrades.fillna(0, inplace= True)


##############################################################################
### Create All Grade output
##############################################################################

### Aggregate to grade level
AllGrades = SegOutcomes.groupby(['InspectionStatus','Fortnight']
                        ).agg({
                                  'Fortnight' : 'count'
                                })

### Format output
AllGrades = AllGrades.unstack()
AllGrades.fillna(0, inplace= True)


##############################################################################
### Export Results
##############################################################################

ZoneGrades.to_csv(os.path.join(output_path, 'ZoneGrades_' +  RunDate + '.csv'))

AllGrades.to_csv(os.path.join(output_path, 'AllGrades_' + RunDate + '.csv'))





