# -*- coding: utf-8 -*-
"""
Created on Fri Sep  6 13:17:04 2019

@author: KatieSi
"""


##############################################################################
### Import Packages
##############################################################################

import numpy as np
import pandas as pd
import pdsql
from datetime import date


##############################################################################
### Set Variables
##############################################################################

ReportName= 'Water Segmentation Inspections'
RunDate = str(date.today())
InspectionFile = 'SegmentationInspectionExamples.csv'

##############################################################################
#### Import Data
##############################################################################

SegInsp = pd.read_csv(
        r'D:\\Implementation Support\\Python Scripts\\scripts\\Import\\'+
        InspectionFile)
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
                        on='InspectionID',
                        how='outer')
SegOutcomes['InspectionStatus'] = SegOutcomes['InspectionStatus'].fillna('Missing Inspection')


##############################################################################
### Create Total counts
##############################################################################

### Calculate Counts per fortnight
TotalInsp = SegOutcomes.groupby(['Fortnight'],as_index=False)['ConsentNo'].aggregate('count')
TotalInsp.rename(columns ={'ConsentNo' : 'Total'}, inplace=True)

### Calculate counts per zone
ZoneTotalInsp = SegOutcomes.groupby(['Zone'],as_index=False)['ConsentNo'].aggregate('count')
ZoneTotalInsp.rename(columns ={'ConsentNo' : 'ZoneTotal'}, inplace=True)

### Calculate counts per zone/fortnight
FortnightTotalInsp = SegOutcomes.groupby(['Zone','Fortnight'],as_index=False)['ConsentNo'].aggregate('count')
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
ZoneGrades = SegOutcomes.groupby(['ZoneTotal','Zone','InspectionStatus','Fortnight']
                        ).agg({
                                  'ConsentNo' : 'count',
                                  'FortnightTotal' : 'max'
                                })

### Calculate percentages
ZoneGrades['Fortnight'] = ZoneGrades.ConsentNo/ZoneGrades.FortnightTotal
ZoneGrades['Fortnight'] = pd.Series(["{0:.0f}%".format(val * 100) for val in ZoneGrades['Fortnight']], index = ZoneGrades.index)

#### Format output
ZoneGrades = ZoneGrades.drop(['ConsentNo', 
            'FortnightTotal'
            ], axis=1)
ZoneGrades = ZoneGrades.unstack()
ZoneGrades.fillna(0, inplace= True)


##############################################################################
### Create All Grade output
##############################################################################

### Aggregate to grade level
AllGrades = SegOutcomes.groupby(['InspectionStatus','Fortnight']
                        ).agg({
                                  'ConsentNo' : 'count',
                                  'Total' : 'max'
                                })

### Calculate percentages
AllGrades['Fortnight'] = AllGrades.ConsentNo/AllGrades.Total
AllGrades['Fortnight'] = pd.Series(["{0:.0f}%".format(val * 100) for val in AllGrades['Fortnight']], index = AllGrades.index)

### Format output
AllGrades = AllGrades.drop(['ConsentNo', 
            'Total'
            ], axis=1)
AllGrades = AllGrades.unstack()
AllGrades.fillna(0, inplace= True)


##############################################################################
### Export Results
##############################################################################
ZoneGrades.to_csv(
        r'D:\\Implementation Support\\Python Scripts\\scripts\\Export\\'
        'ZoneGrades' + RunDate + '.csv')
AllGrades.to_csv(
        r'D:\\Implementation Support\\Python Scripts\\scripts\\Export\\'
        'AllGrades' + RunDate + '.csv')









