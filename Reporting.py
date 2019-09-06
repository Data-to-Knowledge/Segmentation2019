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
from datetime import datetime, timedelta, date


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

# Format data
SegOutcomes.rename(columns = Insp_ColNames, inplace=True)
SegOutcomes['ConsentNo'] = SegOutcomes['ConsentNo'].str.strip().str.upper()


SegMissing_Count = SegInsp.shape[0]-SegOutcomes.shape[0]


SegOutcomes = pd.merge(SegInsp, SegOutcomes, 
                        on='InspectionID',
                        how='outer')

SegOutcomes['InspectionStatus'] = SegOutcomes['InspectionStatus'].fillna('Missing Inspection')

TotalInsp = SegOutcomes.groupby(['Fortnight'],as_index=False)['ConsentNo'].aggregate('count')

TotalInsp.rename(columns ={'ConsentNo' : 'Total'}, inplace=True)

ZoneTotalInsp = SegOutcomes.groupby(['Zone'],as_index=False)['ConsentNo'].aggregate('count')

ZoneTotalInsp.rename(columns ={'ConsentNo' : 'ZoneTotal'}, inplace=True)

FortnightTotalInsp = SegOutcomes.groupby(['Zone','Fortnight'],as_index=False)['ConsentNo'].aggregate('count')

FortnightTotalInsp.rename(columns ={'ConsentNo' : 'FortnightTotal'}, inplace=True)


SegOutcomes = pd.merge(SegOutcomes, TotalInsp, 
                        on=['Fortnight'],
                        how='left')

SegOutcomes = pd.merge(SegOutcomes, ZoneTotalInsp, 
                        on=['Zone'],
                        how='left')

SegOutcomes = pd.merge(SegOutcomes, FortnightTotalInsp, 
                        on=['Zone','Fortnight'],
                        how='left')



ZoneGrades = SegOutcomes.groupby(['ZoneTotal','Zone','InspectionStatus','Fortnight']
                        ).agg({
                                  'ConsentNo' : 'count',
                                  'FortnightTotal' : 'max'
                                })

ZoneGrades['Fortnight'] = ZoneGrades.ConsentNo/ZoneGrades.FortnightTotal

ZoneGrades['Fortnight'] = pd.Series(["{0:.0f}%".format(val * 100) for val in ZoneGrades['Fortnight']], index = ZoneGrades.index)

ZoneGrades = ZoneGrades.drop(['ConsentNo', 
            'FortnightTotal'
            ], axis=1)

ZoneGrades = ZoneGrades.unstack()
ZoneGrades.fillna(0, inplace= True)


AllGrades = SegOutcomes.groupby(['InspectionStatus','Fortnight']
                        ).agg({
                                  'ConsentNo' : 'count',
                                  'Total' : 'max'
                                })

AllGrades['Fortnight'] = AllGrades.ConsentNo/AllGrades.Total

AllGrades['Fortnight'] = pd.Series(["{0:.0f}%".format(val * 100) for val in AllGrades['Fortnight']], index = AllGrades.index)

AllGrades = AllGrades.drop(['ConsentNo', 
            'Total'
            ], axis=1)

AllGrades = AllGrades.unstack()
AllGrades.fillna(0, inplace= True)


ZoneGrades.to_csv(
        r'D:\\Implementation Support\\Python Scripts\\scripts\\Export\\'
        'ZoneGrades' + RunDate + '.csv')


AllGrades.to_csv(
        r'D:\\Implementation Support\\Python Scripts\\scripts\\Export\\'
        'AllGrades' + RunDate + '.csv')









