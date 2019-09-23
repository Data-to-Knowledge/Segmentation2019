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
from datetime import datetime, timedelta, date
import random


##############################################################################
### Set Variables
##############################################################################

# Base Variable
ReportName= 'Water Inspection Prioritzation Model - Inspection Allocation 1'
RunDate = str(date.today())

### Baseline File
InspectionFile = 'InspectionList2019-09-18.csv'
SegmentationFile = 'Segmentation2019-09-18.csv'
SegmentationNoteFile = 'SegmentationNote2019-09-18.csv'


### Allocation totals per fortnight
FirstRunCountF1 = 625
FirstRunCountF2 = 616

FortnightDate1 = '2019-09-09'
FortnightDate2 = '2019-09-23'


##############################################################################
### Import data
##############################################################################

#Uplaod baseline
InspectionList = pd.read_csv(
        r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\" +
        InspectionFile)

Segmentation = pd.read_csv(
        r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\" +
        SegmentationFile)

SegmentationNote = pd.read_csv(
        r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\" +
        SegmentationNoteFile)


##############################################################################
### Select First Run of Inspections
##############################################################################

### Remove unecessary inspections
InspectionList = InspectionList[InspectionList['InspectionNecessity'] != 0]

### Remove Midseason Inspections
MidSeasonCount = Segmentation[Segmentation['InspectionID'].notnull()]

### Remove Number of Midseason Inspections from total
MidSeasonCount = len(MidSeasonCount[['ConsentNo']].drop_duplicates())
Fortnight1 = FirstRunCountF1-MidSeasonCount
Fortnight2 = FirstRunCountF2

### Reduce list to First Push Inspections
FirstInspections = InspectionList[(InspectionList['InspectionAssignment'] == 1 )]

### Choose Inspections for Fornight 1
F1Inspections = FirstInspections.sample(n=Fortnight1, weights = 'TotalRisk', random_state = 1)
F1Inspections['Fortnight'] = 1

F1InspectionsList = F1Inspections[['ConsentNo','Fortnight']]

FirstInspections = pd.merge(FirstInspections, F1InspectionsList, on = 'ConsentNo', how = 'left')
FirstInspections = FirstInspections[(FirstInspections['Fortnight'] != 1)]
FirstInspections = FirstInspections.drop(['Fortnight'], axis=1)

### Choose Inspections for Fornight 1
F2Inspections = FirstInspections.sample(n=Fortnight2, weights = 'TotalRisk', random_state = 1)
F2Inspections['Fortnight'] = 2
F2InspectionsList = F2Inspections[['ConsentNo','Fortnight']]


InspectionAllocations = pd.concat([
                                F1InspectionsList,
                                F2InspectionsList
                                ])

InspectionAllocations = pd.merge(InspectionAllocations, InspectionList, on = 'ConsentNo', how = 'left')


Officers = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\Officers.csv")



InspectionAllocations.loc[InspectionAllocations['Fortnight'] == 1, 'RMO'] = random.choices(Officers['RMO'],Officers['F1Weight'], k= Fortnight1)
InspectionAllocations.loc[InspectionAllocations['Fortnight'] == 2, 'RMO'] = random.choices(Officers['RMO'],Officers['F2Weight'], k= FirstRunCountF2)

FirstPush = InspectionAllocations[InspectionAllocations['Fortnight'].notnull()]

FirstPush.to_csv('FirstPush.csv')

MidseasonInspections = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\MidseasonInspections.csv")

MidseasonInspectionList = Segmentation[Segmentation['MidSeasonRisk'] == 'Warning']

MidseasonInspectionList = MidseasonInspectionList.groupby(
        ['ConsentNo'], as_index=False
        ).agg({
                'HolderAddressFullName': 'max',
                'CWMSZone' :'max',
                'TotalRisk': 'max',
                'InspectionAssignment' : 'max',
                'InspectionNecessity' : 'min',
                'AdMan' : 'max',
                'ComplexityLevel' : 'max'
                })

MidseasonInspectionList['RMO'] = random.choices(Officers['RMO'],Officers['F1Weight'], k=len(MidseasonInspectionList))

MidseasonInspectionList = pd.merge(MidseasonInspectionList, MidseasonInspections, on = 'ConsentNo', how = 'left')

MidseasonInspectionList.to_csv('MidseasonInspectionList.csv')

##############################################################################
### First Allocation
##############################################################################


InspectionAllocations = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\FirstPush-Edit.csv")

Officers = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\Officers.csv")

OfficerLink = Officers[['RMO','AccelaUserID']]


InspectionAllocations = pd.merge(InspectionAllocations, OfficerLink, on = 'RMO', how = 'left')

InspectionAllocations = pd.merge(InspectionAllocations, SegmentationNote, on = 'ConsentNo', how = 'left')

InspectionAllocations = pd.merge(InspectionAllocations, InspectionList, on = 'ConsentNo', how = 'left')

conditions = [
	(InspectionAllocations['Fortnight'] == 1),
	(InspectionAllocations['Fortnight'] == 2)
			 ]
choices = [FortnightDate1, FortnightDate2]
InspectionAllocations['FortnightDate'] = np.select(conditions, choices, default = np.nan)

InspectionAllocations['Consent'] = InspectionAllocations['ConsentNo']
InspectionAllocations['InspectionType'] = 'Desk Top Inspection'
InspectionAllocations['InspectionSubType'] = 'Water Use Data'
InspectionAllocations['ScheduledDate']  = InspectionAllocations['FortnightDate']
InspectionAllocations['InspectionDate']  = ' '
InspectionAllocations['InspectionStatus'] = 'Scheduled'
InspectionAllocations['AssignedTo'] = InspectionAllocations['AccelaUserID']
InspectionAllocations['Department'] = ' '
InspectionAllocations['GeneralComments'] = InspectionAllocations['InspectionNote']

ScheduleInspections = InspectionAllocations[[
                                              'Consent',
                                              'InspectionType',
                                              'InspectionSubType',
                                              'ScheduledDate',
                                              'InspectionDate',
                                              'InspectionStatus',
                                              'AssignedTo',
                                              'Department',
                                              'GeneralComments',
                                              'ConsentsOnWAP' 
                                             ]]
ScheduleInspections.to_csv('WaterSegmentationInspections' + RunDate +'.csv')

