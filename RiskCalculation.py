# -*- coding: utf-8 -*-
"""
Created on Mon 19 August 2019
@author: KatieSi
"""

##############################################################################
### Import Packages
##############################################################################

import numpy as np
import pandas as pd
import pdsql
from datetime import datetime, timedelta, date
import random


##############################################################################
### Set Variables
##############################################################################

# Base Variable
ReportName= 'Water Inspection Prioritzation Model - Risk Calculation'
RunDate = str(date.today())

### Baseline File
BaselineFile = 'Baseline2019-09-05.csv'

### Allocation totals per fortnight
FirstRunCountF1 = 625
FirstRunCountF2 = 616
SecondRunCountF3 = 632
SecondRunCountF4 = 632
SecondRunCountF5 = 616


FortnightDate1 = '2019-09-09'
FortnightDate2 = '2019-09-23'


##############################################################################
### Import data
##############################################################################

#Iplaod baseline
Segmentation = pd.read_csv(
        r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\" +
        BaselineFile)


##############################################################################
### Calculate Consent Level Warnings on Complexity
##############################################################################

### Campaign Warning
HighRiskCamp = 'Warning'
LowRiskCamp = ''

conditions = [
	((Segmentation['ConsentProject'].isnull()) &
	(Segmentation['WAPProject'].isnull()))
             ]
choices = [LowRiskCamp]
Segmentation['CampaignRisk'] = np.select(
                                            conditions, 
                                            choices, 
                                            default = HighRiskCamp)


### Midseason Warning
Segmentation.loc[Segmentation['InspectionID'].notnull(), 'MidSeasonRisk'] = 'Warning'


### Water User Group Warning
HighRiskWUG = 'Warning'
LowRiskWUG = ''

conditions = [
	(Segmentation['WUGNo'].isnull())
			 ]
choices = [LowRiskWUG]
Segmentation['WUGRisk'] = np.select(conditions, choices, default = HighRiskWUG)


### Adaptive Management Warning
HighRiskAM = 'Warning'
LowRiskAM = ''

conditions = [
	(Segmentation['AdMan'].isnull())		 
			 ]
choices = [LowRiskAM]
Segmentation['AMRisk'] = np.select(conditions, choices, default = HighRiskAM)


### Complex Conditions Warning
HighRiskConditions = 'Warning'
LowRiskConditions = ''

conditions = [
	(Segmentation['ComplexAllocations'] == 'No' ),
    (Segmentation['ComplexAllocations'].isnull() )
			 ]
choices = [LowRiskConditions,np.nan]
Segmentation['ConditionRisk'] = np.select(
                                            conditions, 
                                            choices, 
                                            default = HighRiskConditions)



##############################################################################
### Calculate WAP Level Warnings on Complexity
##############################################################################

### Waiver Warning
HighRiskWaiver = 'Warning'
LowRiskWaiver = ''

conditions = [
	(Segmentation['Waiver'] == 'Yes' ),
			 ]
choices = [HighRiskWaiver]
Segmentation['WaiverRisk'] = np.select(
                                        conditions, 
                                        choices, 
                                        default = LowRiskWaiver)


### Shared Meter Warning
HighRiskSharedMeter = 'Warning'
LowRiskSharedMeter = ''

conditions = [
	(Segmentation['SharedMeter'] == 'Yes' )
			 ]
choices = [HighRiskSharedMeter]
Segmentation['SharedMeterRisk'] = np.select(
                                            conditions, 
                                            choices, 
                                            default = LowRiskSharedMeter)


### Multiple Meter Warning
HighRiskMultiMeter = 'Warning'
LowRiskMultiMeter = ''

conditions = [
	(Segmentation['T_MetersRecieved'] > 1)
			 ]
choices = [HighRiskMultiMeter]
Segmentation['MultipleMeterRisk'] = np.select(
                                            conditions, 
                                            choices, 
                                            default = LowRiskMultiMeter)

                            
### Shared WAP Warning
HighRiskSharedWAP = 'Warning'
LowRiskSharedWAP = ''

conditions = [
	(Segmentation['ConsentsOnWAP'] > 1 ),
			 ]
choices = [HighRiskSharedWAP]
Segmentation['SharedWAPRisk'] = np.select(
                                            conditions, 
                                            choices, 
                                            default = LowRiskSharedWAP)


### Under 5ls Warning
HighRiskUnder5ls = 'Warning'
LowRiskSharedUnder5ls = ''

conditions = [
	(Segmentation['WAPRate'] < 5),
			 ]
choices = [HighRiskUnder5ls]
Segmentation['Under5lsRisk'] = np.select(
                                        conditions, 
                                        choices, 
                                        default = LowRiskSharedUnder5ls)


##############################################################################
### Calculate Consent Level Priorities
##############################################################################

### Inspection Date Priority
DateThresholdLatestInspection = datetime.strptime('2018-07-01', '%Y-%m-%d')
HighRiskLatestInspection = 3
MedRiskLatestInspection = 1
LowRiskLatestInspection = 0

Segmentation['LatestInspection'] = pd.to_datetime(
                                        Segmentation['LatestInspection'], 
                                        errors='coerce')

conditions = [
	(Segmentation['LatestInspection'] <= DateThresholdLatestInspection),
	(Segmentation['LatestInspection'] > DateThresholdLatestInspection),
    (Segmentation['LatestInspection'].isnull())
			 ]
choices = [LowRiskLatestInspection, 
           MedRiskLatestInspection,
           HighRiskLatestInspection]
Segmentation['LatestInspectionRisk'] = np.select(
                                                conditions, 
                                                choices, 
                                                default = np.nan)


### Non-Compliant Inspection Date Priority
DateThresholdLatestNonComp = datetime.strptime('2018-07-01', '%Y-%m-%d')

HighRiskLatestNonComp = 3
MedRiskLatestNonComp = 1
LowRiskLatestNonComp = 0


Segmentation['LatestNonComplianceDate'] = pd.to_datetime(
                                        Segmentation['LatestNonComplianceDate'], 
                                        errors='coerce')

conditions = [
	(Segmentation['LatestNonComplianceDate'] < DateThresholdLatestNonComp),
	(Segmentation['LatestNonComplianceDate'] >= DateThresholdLatestNonComp),
    (Segmentation['LatestNonComplianceDate'].isnull())
			 ]
choices = [HighRiskLatestNonComp,MedRiskLatestNonComp,LowRiskLatestNonComp]
Segmentation['LatestNonComplianceRisk'] = np.select(
                                                    conditions, 
                                                    choices, 
                                                    default = np.nan)

### Non-Compliant Inspection Counts Priority
HighThresholdNonComp = 3
LowThresholdNonComp = 1
HighRiskNonComp = 3
MedRiskNonComp = 1
LowRiskNonComp = 0

conditions = [
  	(Segmentation['CountNonCompliance'] < LowThresholdNonComp),  
	(Segmentation['CountNonCompliance'] > HighThresholdNonComp)
			]
choices = [LowRiskNonComp, HighRiskNonComp]
Segmentation['CountNonComplianceRisk'] = np.select(
                                                    conditions, 
                                                    choices, 
                                                    default = MedRiskNonComp)


##############################################################################
### Calculate WAP Level Priorities
##############################################################################

### Verification Priority
HighRiskVerification = 3
MedRiskVerification = 1
LowRiskVerification = 0

conditions = [
	(Segmentation.VerificationOutOfDate == 'No'),
	(Segmentation.LatestVerification.isnull()),		 
			 ]
choices = [LowRiskVerification, HighRiskVerification]
Segmentation['VerificationRisk'] = np.select(
                                            conditions,
                                            choices, 
                                            default = MedRiskVerification)


### Missing Data Days Priority
HighThresholdTMR = 10
LowThresholdTMR = 0
HighRiskTMR = 3
MedRiskTMR = 1
LowRiskTMR = 0

conditions = [
        (Segmentation['T_AverageMissingDays'] > HighThresholdTMR),
        (Segmentation['T_AverageMissingDays'] == LowThresholdTMR),
        ((Segmentation['T_AverageMissingDays'] > LowThresholdTMR) & 
        (Segmentation['T_AverageMissingDays'] <= HighThresholdTMR))
             ]
choices = [HighRiskTMR, LowRiskTMR, MedRiskTMR]
Segmentation['T_MRNC'] = np.select(conditions, choices, default = np.nan)


### Missing Data Records Priority
HighThresholdMR = 10
LowThresholdMR = 0
HighRiskMR = 3
MedRiskMR = 1
LowRiskMR = 0

conditions = [
        (Segmentation['WS_TotalMissingRecord'] > HighThresholdMR),
        (Segmentation['WS_TotalMissingRecord'] == LowThresholdMR),
        ((Segmentation['WS_TotalMissingRecord'] > LowThresholdMR) & 
        (Segmentation['WS_TotalMissingRecord'] <= HighThresholdMR))
             ]
choices = [HighRiskMR, LowRiskMR, MedRiskMR]
Segmentation['WS_MRNC'] = np.select(conditions, choices, default = np.nan)


##############################################################################
### Calculate Usage Priorities
##############################################################################

### Percent Annual Volume Used priority
HighVolumeThresholdAV = 200
MedVolumeThresholdAV = 110
LowVolumeThresholdAV = 100
NoiseRiskAV = 'Warning'
HighRiskAV = 3
MedRiskAV = 1
LowRiskAV = 0

conditions = [
        (Segmentation['CS_PercentAnnualVolume'] > HighVolumeThresholdAV),
        (Segmentation['CS_PercentAnnualVolume'] < LowVolumeThresholdAV),
        ((Segmentation['CS_PercentAnnualVolume'] > LowVolumeThresholdAV) & 
        (Segmentation['CS_PercentAnnualVolume'] <= MedVolumeThresholdAV)),
        ((Segmentation['CS_PercentAnnualVolume'] > MedVolumeThresholdAV) & 
        (Segmentation['CS_PercentAnnualVolume'] <= HighVolumeThresholdAV))
             ]
choices = [NoiseRiskAV, LowRiskAV, MedRiskMR, HighRiskAV]
Segmentation['CS_AVNC'] = np.select(conditions, choices, default = np.nan)


### Median Rate of Take Over Consent Limit Priority
HighThresholdMaxRoT = 115
LowThresholdMaxRoT = 105
HighRiskMaxRoT = 3 # was 100
MedRiskMaxRoT = 1
LowRiskMaxRoT = 0
   
conditions = [
        (Segmentation['CS_PercentMedRate'] > HighThresholdMaxRoT),
        (Segmentation['CS_PercentMedRate'] < LowThresholdMaxRoT),
        ((Segmentation['CS_PercentMedRate'] > LowThresholdMaxRoT) & 
        (Segmentation['CS_PercentMedRate'] <= HighThresholdMaxRoT))
             ]
choices = [HighRiskMaxRoT, LowRiskMaxRoT, MedRiskMaxRoT]
Segmentation['CS_MedRateNC'] = np.select(conditions, choices, default = np.nan)


### Median Rate of Take Over WAP Limit Priority
conditions = [
        (Segmentation['WS_PercentMedRate'] > HighThresholdMaxRoT),
        (Segmentation['WS_PercentMedRate'] < LowThresholdMaxRoT),
        ((Segmentation['WS_PercentMedRate'] > LowThresholdMaxRoT) & 
        (Segmentation['WS_PercentMedRate'] <= HighThresholdMaxRoT))
             ]
choices = [HighRiskMaxRoT, LowRiskMaxRoT, MedRiskMaxRoT]
Segmentation['WS_MedRateNC'] = np.select(conditions, choices, default = np.nan)


### Days Over Rate Consent Limit Priority
HighThresholdTimeRoT = 14
LowThresholdTimeRoT = 1
HighRiskTimeRoT = 3
MedRiskTimeRoT = 1
LowRiskTimeRoT = 0

conditions = [
        (Segmentation['CS_TimeAboveRate'] > HighThresholdTimeRoT),
        (Segmentation['CS_TimeAboveRate'] < LowThresholdTimeRoT),
        ((Segmentation['CS_TimeAboveRate'] > LowThresholdTimeRoT) & 
        (Segmentation['CS_TimeAboveRate'] <= HighThresholdTimeRoT))
             ]
choices = [HighRiskTimeRoT, LowRiskTimeRoT, MedRiskTimeRoT]
Segmentation['CS_TimeRoTNC'] = np.select(conditions, choices, default = np.nan)


### Days Over Rate Consent Limit Priority
conditions = [
        (Segmentation['WS_TimeAboveRate'] > HighThresholdTimeRoT),
        (Segmentation['WS_TimeAboveRate'] < LowThresholdTimeRoT),
        ((Segmentation['WS_TimeAboveRate'] > LowThresholdTimeRoT) & 
        (Segmentation['WS_TimeAboveRate'] <= HighThresholdTimeRoT))
             ]
choices = [HighRiskTimeRoT, LowRiskTimeRoT, MedRiskTimeRoT]
Segmentation['WS_TimeRoTNC'] = np.select(conditions, choices, default = np.nan)


### Days Over NDay Consent Limit Priority
HighThresholdTimeRoT = 14
LowThresholdTimeRoT = 1
HighRiskTimeRoT = 3
MedRiskTimeRoT = 1
LowRiskTimeRoT = 0

conditions = [
        (Segmentation['CS_DaysAboveNday'] > HighThresholdTimeRoT),
        (Segmentation['CS_DaysAboveNday'] < LowThresholdTimeRoT),
        ((Segmentation['CS_DaysAboveNday'] > LowThresholdTimeRoT) & 
        (Segmentation['CS_DaysAboveNday'] <= HighThresholdTimeRoT))
             ]
choices = [HighRiskTimeRoT, LowRiskTimeRoT, MedRiskTimeRoT]
Segmentation['CS_DayNDVNC'] = np.select(conditions, choices, default = np.nan)


### Days Over NDay Consent Limit Priority
conditions = [
        (Segmentation['WS_DaysAboveNday'] > HighThresholdTimeRoT),
        (Segmentation['WS_DaysAboveNday'] < LowThresholdTimeRoT),
        ((Segmentation['WS_DaysAboveNday'] > LowThresholdTimeRoT) & 
        (Segmentation['WS_DaysAboveNday'] <= HighThresholdTimeRoT))
             ]
choices = [HighRiskTimeRoT, LowRiskTimeRoT, MedRiskTimeRoT]
Segmentation['WS_DayNDVNC'] = np.select(conditions, choices, default = np.nan)


##############################################################################
### Assign Known Catagories
##############################################################################

### Assign Inspection Run Catagories
# 1 - first fornight push
# 2 - not first fornight push
conditions = [
        ((Segmentation['ConditionRisk'] == 'Warning') |
                (Segmentation['WUGRisk'] == 'Warning') |
                (Segmentation['SharedMeterRisk'] == 'Warning')) 
             ]
choices = [2]
Segmentation['InspectionAssignment'] = np.select(
                                                conditions, 
                                                choices, 
                                                default = 1)



### Assign Inspection Necessity Catagories
# 0 - do not inspect
# 1 - Sample
# 2 - must inspect
conditions = [
        ((Segmentation['CampaignRisk'] == 'Warning') |
               (Segmentation['WUGRisk'] == 'Warning')|
               (Segmentation['Under5lsRisk'] == 'Warning')|
               (Segmentation['WaiverRisk'] == 'Warning')|
               (Segmentation['MidSeasonRisk'] == 'Warning')),
         ((Segmentation['AMRisk'] == 'Warning')
             )
             ]
choices = [0,2]
Segmentation['InspectionNecessity'] = np.select(
                                                conditions, 
                                                choices, 
                                                default = 1)



##############################################################################
### Calculate Overall Priorities and Complexities
##############################################################################

### Calculate Overall Complexity
Segmentation['ComplexityLevel'] = (Segmentation[[
                                                'ConditionRisk',
                                                'SharedMeterRisk',
                                                'MultipleMeterRisk',
                                                'SharedWAPRisk',
                                                'AMRisk'
                                                ]]== 'Warning').sum(axis=1)


### Calculate Overall Priority
Segmentation['TotalRisk'] = Segmentation[[
                                        'VerificationRisk',
                                        'LatestInspectionRisk',
                                        'LatestNonComplianceRisk',
                                        'CountNonComplianceRisk',
                                        'T_MRNC',
                                        'CS_AVNC',
                                        'CS_MedRateNC',
                                        'CS_TimeRoTNC', 
#                                       'CS_DayNDVNC',
#                                       'WS_MRNC',                                          
#                                       'WS_MedRateNC',
#                                       'WS_TimeRoTNC'
#                                       'WS_DayNDVNC',
                                        ]].sum(axis=1)


##############################################################################
### Create Table of Inspections
##############################################################################

InspectionList = Segmentation[Segmentation['InspectionNecessity'] != 0]

InspectionList = InspectionList.groupby(
        ['ConsentNo'], as_index=False
        ).agg({
                'HolderAddressFullName': 'max',
                'CWMSZone' :'max',
                'TotalRisk': 'max',
                'ConsentsOnWAP' : 'max',
                'WAPsOnConsent' : 'max',
                'InspectionAssignment' : 'max',
                'InspectionNecessity' : 'min',
                'AdMan' :'max',
                'ComplexityLevel' : 'max'
                })

##############################################################################
### Create Inspection Comment
##############################################################################


### Create Base Notes 
Segmentation['IntroNote'] = ('This consent has been assessed through the 2018/19 inspection allocation model. ')
Segmentation['ClosingNote'] = ('General comments for CMR**')

### Create consent level notes
Segmentation['CampaignNote'] = np.where(Segmentation['CampaignRisk'] == 'Warning',
                                        'Consent is part of an ongoing project and was not selected for inspection. ',
                                            ' ')

Segmentation['AdManNote'] = np.where(Segmentation['AMRisk'] == 'Warning', 
            'Contains Adaptive Management Conditions. ',' ')

Segmentation['WUGNote'] = np.where(Segmentation['WUGNo'].notnull(),
                            ('Member of Water User Group ' + 
                            Segmentation['WUGNo'] + '-' +
                            Segmentation['WUGName'] +
                            '. Monitoring Officer : '+
                            Segmentation['WUGMonOfficer'] + '. '
                            ).astype(str),
                             ' ')

Segmentation['ParentNote'] = np.where(Segmentation['ParentChildConsent'].notnull(), 
                                ('Consent is related to '+ 
                                 Segmentation['ParentChildConsent'] + 
                                 ' on Parent tree. ').astype(str)
                                ,' ')

Segmentation['WAPCountNote'] = np.where(Segmentation['WAPsOnConsent'] > 1,
                                (Segmentation['WAPsOnConsent'].astype(str) +
                                ' WAPS on the consent. ').astype(str),
                                 ' ')

Segmentation['ConsentNote']  = Segmentation[[
                                            'IntroNote',
                                            'CampaignNote',
                                            'AdManNote',
                                            'WUGNote',
                                            'ParentNote',
                                            'WAPCountNote'
                                            ]].astype(str).apply(lambda x: ''.join(x), axis=1)


### Create WAP level notes                             
Segmentation['SharedMeterNote'] = np.where(Segmentation['SharedMeterRisk'] == 'Warning', 
                            (Segmentation['WAP'] +
                            ' contains data from other WAPs. '), 
                            ' ')

Segmentation['MultipleMeterNote'] = np.where(
                            Segmentation['T_MetersRecieved'] > 1, 
                            (Segmentation['WAP'] +
                                 ' has multiple meters reading the usage. '), 
                            ' ')

Segmentation['SharedWAPNote'] = np.where(Segmentation['ConsentsOnWAP'] > 1, 
                            (Segmentation['WAP'] +
                            ' exists on '+
                            Segmentation['ConsentsOnWAP'].astype(str) +
                            ' consents active this season. '),
                            ' ')

Segmentation['VerificationNote'] = np.where(Segmentation['VerificationOutOfDate'] != 'No',
                            ('One of the Verification records  for ' +
                            Segmentation['WAP'] +
                            'is either out of date or does not exist. ').astype(str),
                            ' ')

Segmentation['PoorDataNote'] = np.where(((Segmentation['CS_PercentAnnualVolume'] > 200) | 
                                        (Segmentation['CS_PercentMedRate'] > 200 )),
                                ('Suspected data spike or data quality issues on ' +
                                 Segmentation['WAP'] + '. '),
                                 ' ')
                                
Segmentation['WAPNote']  = Segmentation[[
                                            'SharedMeterNote',
                                            'MultipleMeterNote',
                                            'SharedWAPNote',
                                            'VerificationNote',
                                            'PoorDataNote'
                                            ]].astype(str).apply(lambda x: ''.join(x), axis=1)                                

SegmentationNote = Segmentation[['ConsentNo','WAP','ConsentNote','WAPNote','ClosingNote']]

SegmentationNote = SegmentationNote.groupby(
        ['ConsentNo','ConsentNote','ClosingNote']
        )['WAPNote'].apply(lambda x: ' - '.join(x)).reset_index()

SegmentationNote['InspectionNote'] = (SegmentationNote['ConsentNote'] +
                                     '  --  ' + 
                                     SegmentationNote['WAPNote'] + 
                                     '  --  ' +
                                     SegmentationNote['ClosingNote'])

SegmentationNote = SegmentationNote[['ConsentNo','InspectionNote']]


##############################################################################
### Select First Run of Inspections
##############################################################################

### Remove Midseason Inspections
MidSeasonCount = Segmentation[Segmentation['InspectionID'].notnull()]

### Remove Number of Midseason Inspections from total
MidSeasonCount = len(MidSeasonCount[['ConsentNo']].drop_duplicates())
Fortnight1 = FirstRunCountF1-MidSeasonCount
Fortnight2 = FirstRunCountF2

### Remove all 2nd Run Inspections
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

##############################################################################
### Second Push
#############################################################################
MidseasonInspectionsList = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\MidseasonInspectionList.csv")

FirstPush = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\FirstPush.csv")

FirstPushList = FirstPush[['ConsentNo','Fortnight']]



SecondInspections = InspectionList

SecondInspections = pd.merge(SecondInspections, FirstPushList, on = 'ConsentNo', how = 'left')
SecondInspections = SecondInspections[SecondInspections['Fortnight'].isnull()]

SecondInspections = SecondInspections.drop(['Fortnight'], axis=1)

NecessaryInspections = SecondInspections[(SecondInspections['InspectionNecessity'] == 2)]
SampledInspections = SecondInspections[(SecondInspections['InspectionNecessity'] != 2)]



NecessaryInspections['Fortnight'] = np.random.randint(low=3, high=6, size=len(NecessaryInspections))

NecessaryInspectionsCount =  NecessaryInspections.groupby(
                                            ['Fortnight'], as_index = False
                                            ).agg({'ConsentNo' : 'count'})


NecessaryInspectionsList = NecessaryInspections[['ConsentNo','Fortnight']]

NecessaryGroup3 = NecessaryInspectionsCount[NecessaryInspectionsCount['Fortnight'] == 3].iloc[0,1]
NecessaryGroup4 = NecessaryInspectionsCount[NecessaryInspectionsCount['Fortnight'] == 4].iloc[0,1]
NecessaryGroup5 = NecessaryInspectionsCount[NecessaryInspectionsCount['Fortnight'] == 5].iloc[0,1]

WUGInspections = Segmentation.groupby(
        ['WUGNo'], as_index=False
        ).agg({
                'WUGName': 'max',
                'CWMSZone' :'max',
                'TotalRisk': 'max',               
                'InspectionAssignment' : 'max',
                'InspectionNecessity' : 'min',
                'ConsentsOnWAP' : 'max',
                'ComplexityLevel' : 'max'
                })
 
WUGInspections['Fortnight'] = np.random.randint(low=3, high=6, size=len(WUGList))



WUGInspectionList = WUGInspections[['WUGNo','Fortnight']]

WUGInspectionCount =  WUGInspections.groupby(
                                        ['Fortnight'], as_index = False
                                        ).agg({'WUGNo' : 'count'})

WUGGroup3 = WUGInspectionCount[WUGInspectionCount['Fortnight'] == 3].iloc[0,1]
WUGGroup4 = WUGInspectionCount[WUGInspectionCount['Fortnight'] == 4].iloc[0,1]
WUGGroup5 = WUGInspectionCount[WUGInspectionCount['Fortnight'] == 5].iloc[0,1]


Fortnight3 = SecondRunCountF3-NecessaryGroup3 - WUGGroup3
Fortnight4 = SecondRunCountF4-NecessaryGroup4 - WUGGroup4
Fortnight5 = SecondRunCountF5-NecessaryGroup5 - WUGGroup5



F3Inspections = SampledInspections.sample(n=Fortnight3, weights = 'TotalRisk', random_state = 1)
F3Inspections['Fortnight'] = 3
F3InspectionsList = F3Inspections[['ConsentNo','Fortnight']]

SampledInspections = pd.merge(SampledInspections, F3InspectionsList, on = 'ConsentNo', how = 'left')
SampledInspections = SampledInspections[(SampledInspections['Fortnight'] != 3)]
SampledInspections = SampledInspections.drop(['Fortnight'], axis=1)


F4Inspections = SampledInspections.sample(n=Fortnight4, weights = 'TotalRisk', random_state = 1)
F4Inspections['Fortnight'] = 4
F4InspectionsList = F4Inspections[['ConsentNo','Fortnight']]

SampledInspections = pd.merge(SampledInspections, F4InspectionsList, on = 'ConsentNo', how = 'left')
SampledInspections = SampledInspections[(SampledInspections['Fortnight'] != 4)]
SampledInspections = SampledInspections.drop(['Fortnight'], axis=1)


F5Inspections = SampledInspections.sample(n=Fortnight5, weights = 'TotalRisk', random_state = 1)
F5Inspections['Fortnight'] = 5
F5InspectionsList = F4Inspections[['ConsentNo','Fortnight']]

SampledInspections = pd.merge(SampledInspections, F5InspectionsList, on = 'ConsentNo', how = 'left')
SampledInspections = SampledInspections[(SampledInspections['Fortnight'] != 5)]


SampledInspections['Fortnight'] = 0

NoInspectionsList = SampledInspections[['ConsentNo','Fortnight']]





InspectionAllocations = pd.concat([
                                NecessaryInspectionsList,
                                F1InspectionsList,
                                F2InspectionsList, 
                                F3InspectionsList,
                                F4InspectionsList,
                                F5InspectionsList,
                                NoInspectionsList
#                                WUGInspectionList
                                ])



InspectionAllocations = pd.merge(InspectionAllocations, InspectionList, on = 'ConsentNo', how = 'left')


Officers = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\Officers.csv")


InspectionAllocations.loc[InspectionAllocations['Fortnight'] == 3, 'RMO'] = random.choices(Officers['RMO'],Officers['F1Weight'], k= (SecondRunCount-WUGGroup3))
InspectionAllocations.loc[InspectionAllocations['Fortnight'] == 2, 'RMO'] = random.choices(Officers['RMO'],Officers['F2Weight'], k= FirstRunCount)

FirstPush = InspectionAllocations[InspectionAllocations['Fortnight'].notnull()]

FirstPush.to_csv('FirstPush.csv')


Fortnight3 = SecondRunCount-NecessaryGroup3 - WUGGroup3
Fortnight4 = SecondRunCount-NecessaryGroup4 - WUGGroup4
Fortnight5 = SecondRunCount-NecessaryGroup5 - WUGGroup5


tempList = InspectionAllocations

tempList['RMO'] = random.choices(Officers['RMO'],Officers['F1Weight'], k=len(tempList))

import random
tempList.loc[tempList['Fortnight'] == 1, 'RMO'] = random.choices(Officers['RMO'],Officers['F1Weight'], k= Fortnight1)
tempList.loc[tempList['Fortnight'] == 2, 'RMO'] = random.choices(Officers['RMO'],Officers['F2Weight'], k= FirstRunCount)

FortnightTotals = tempList.groupby(['Fortnight'], as_index = False
                 ).agg({'ConsentNo': 'count'})


Officers
Key, Officer, Team, Weight



Officers = pd.DataFrame([
        ['Mr. AAA','Mr. BB','Mr. X'], 
        [1,1,0.5]]).T

Officers.columns = ['RMO','Weights']
tempList = F4InspectionsList
tempList['RMO'] = random.choices(Officers['RMO'],Officers['Weights'], k=len(tempList))
     




Officers.sum(axis = 0, skipna = True)


tempList = F4InspectionsList


tempList.groupby(['RMO'], as_index = False
                 ).agg({'ConsentNo': 'count'})


RMO =  ['Mr. Smith','Mr. Jones','Mr. X']
Weights =   [0.5,0.2,0.3]   
        

random.choices(RMO,Weights, k=30)
#
Officers[1]

#SecondRun = SecondInspections.sample(n=FirstRunCount, weights = 'TotalRisk', random_state = 1)


#df.sample(n=2, weights='num_specimen_seen', random_state=1)


import random
random.choices(['one', 'two', 'three'], [0.2, 0.3, 0.5], k=10)






FirstInspections
Accela user ID
Note
Other columns

extra single inspections

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



#temp['WAPsToInspect'] = temp['WAPsOnConsent'] - temp['Waiver']
#
#
#conditions = [
#    (pd.isnull(ConsentSummary['CS_TimeAboveRate'])) | 
#        (ConsentSummary['CS_TimeAboveRate'] <= LowThresholdTimeRoT),
#    (ConsentSummary['CS_TimeAboveRate'] > HighThresholdTimeRoT)
#             ]
#choices = [LowRiskTimeRoT,HighRiskTimeRoT]
#ConsentSummary['CS_TimeRoTNC'] = np.select(conditions, choices, default = MedRiskTimeRoT)
#
#conditions = [
#    (pd.isnull(WAPSummary['WS_TimeAboveRate'])) | 
#        (WAPSummary['WS_TimeAboveRate'] <= LowThresholdTimeRoT),
#    (WAPSummary['WS_TimeAboveRate'] > HighThresholdTimeRoT)
#             ]
#choices = [LowRiskTimeRoT,HighRiskTimeRoT]
#WAPSummary['WS_TimeRoTNC'] = np.select(conditions, choices, default = MedRiskTimeRoT)
#





#Inspection_agg = Inspection.groupgy(
#	['ConsentNo'], as_index = False
#	).agg(
#			{
#			'IDRisk' : 'max',
#			'IGRisk' : 'max',
#			'InspectionID': 'count'	
#			}
#		  )







# Meter Risk

# Count WAP
# count Meter installed
# count meter verified
# percent meter installed
# percent meter verified





## Meter Waiver


### No Telemetry
#HighDateThresholdNT = datetime.strptime('2018-07-01', '%Y-%m-%d')
#LowDateThresholdNT = datetime.strptime('2018-01-01', '%Y-%m-%d') ###check
#HighRiskNT = 2
#MedRiskNT = 1
#LowRiskNT = 0
#
#conditions = [
#	(Meter.TelemetryEnd > LowDateThresholdNT),
#	(Meter.TelemetryEnd < HighDateThresholdNT) | (Meter.TelemetryEnd isnull)		 
#			 ]
#choices = [LowRiskNT, HighRiskNT]
#Meter['NTRisk'] = np.select(conditions, choices, default = MedRiskNT)





# WAP risk






#
#
## Summay Risk
##RoTNC
#HighPercentThresholdRoT = 115
#LowPercentThresholdRoT = 105
#HighRiskRoT = 2
#MedRiskRoT = 1
#LowRiskRoT = 0
#
#Segmentation['PercentOverRoT'] = ((
#	Segmentation.MaxRateTaken/Segmentation.MaxTakeRate)*100)
#
#conditions = [
#    (Segmentation.MaxTakeRate == np.nan) | (Segmentation.MaxRateTaken == np.nan) | (Segmentation.PercentOverRoT<= LowPercentThresholdRoT),
#    ((Segmentation.MaxRateTaken/Segmentation.MaxTakeRate)*100 > HighPercentThresholdRoT)
#             ]
#choices = [LowRiskRoT,HighRiskRoT]
#Segmentation['RoTRisk'] = np.select(conditions, choices, default = MedRiskRoT)
#
#
## CDNC
#HighPercentThresholdCDV = 110
#LowPercentThresholdCDV = 105
#HighRiskCDV = 2
#MedRiskCDV = 1
#LowRiskCDV = 0
#
#Segmentation['PercentOverCDV'] = ((
#	Segmentation.MaxVolumeAboveNDayVolume+
#	Segmentation.MaxConsecutiveDayVolume)/
#	Segmentation.MaxConsecutiveDayVolume*100
#	)
#
#conditions = [
#    (Segmentation.MaxVolumeAboveNDayVolume == np.nan) | (Segmentation.PercentOverCDV <= LowPercentThresholdCDV),
#    ((Segmentation.NumberOfConsecutiveDays == 1) & (Segmentation.PercentOverCDV > HighPercentThresholdCDV)) | ((Segmentation.NumberOfConsecutiveDays > 1) & (Segmentation.PercentOverCDV > 120))
#             ]
#choices = [LowRiskCDV,HighRiskCDV]
#Segmentation['CDNC'] = np.select(conditions, choices, default = MedRiskCDV)
#
#
##AVNC
#NoiseThresholdAV = 200
#HighVolumeThresholdAV = 110
#LowVolumeThresholdAV = 100
#HighRiskAV = 2
#MedRiskAV = 1
#LowRiskAV = 0
#OtherRiskAV = 9
#
#conditions = [
#    (Summary.PercentAnnualVolumeTaken == np.nan) | (Summary.PercentAnnualVolumeTaken <= LowVolumeThresholdAV),
#    (Summary.PercentAnnualVolumeTaken > NoiseThresholdAV),
#    (Summary.PercentAnnualVolumeTaken > LowVolumeThresholdAV) & (Summary.PercentAnnualVolumeTaken <= HighVolumeThresholdAV),
#    (Summary.PercentAnnualVolumeTaken > HighVolumeThresholdAV) & (Summary.PercentAnnualVolumeTaken <= NoiseThresholdAV)
#             ]
#choices = [LowRiskAV, LowRiskAV, MedRiskAV, HighRiskAV]
#Summary['AVRisk'] = np.select(conditions, choices, default = OtherRiskAV)
#
#
##LFNC
#HighVolumeThresholdLF = 115
#LowVolumeThresholdLF = 105
#HighRiskLFV = 2
#MedRiskLFV = 1
#LowRiskLFV = 0
#
#conditions = [
#    (Summary.TotalVolumeAboveRestriction == np.nan) | (Summary.TotalVolumeAboveRestriction == 0),
#    (Summary.TotalVolumeAboveRestriction > HighVolumeThresholdLF) 
#             ]
#choices = [LowRiskLFV,HighRiskLFV]
#Summary['LFVRisk'] = np.select(conditions, choices, default = MedRiskLFV)
#
#
#HighDaysThresholdLF = 2
#LowDaysThresholdLF = 0
#HighRiskLFD = 2
#MedRiskLFD = 1
#LowRiskLFD = 0
#
#conditions = [
#    (Summary.TotalDaysAboveRestriction == np.nan) | Summary.TotalDaysAboveRestriction == 0),
#    (Summary.TotalDaysAboveRestriction > HighDaysThresholdLF)
#             ]
#choices = [LowRiskLFD,HighRiskLFD]
#Summary['LFDRisk'] = np.select(conditions, choices, default = MedRiskLFD)
#
#
##MRNC
#HighThresholdMR = 200
#LowThresholdMR = 10
#HighRiskMR = 10000
#MedRiskMR = 5
#LowRiskMR = 0
#OtherRiskMR = 5000
#
#conditions = [
#    (Summary.TotalMissingRecord == np.nan) | (Summary.TotalMissingRecord == 0),
#    (Summary.TotalMissingRecord > HighThresholdMR)
#    (Summary.TotalMissingRecord > 0) & (Summary.TotalMissingRecord <= LowThresholdMR),
#             ]
#choices = [LowRiskMR, HighRiskMR, MedRiskAV]
#Summary['MRNC'] = np.select(conditions, choices, default = OtherRiskAV)
#
#
#
#
#
#
#
#
#WAP = pd.merge(WAP, Meter_agg, on = 'WAP', how = 'left')
#
#CalculatedRisk = pd.merge(Consent, WAP_agg, on = 'ConsentNo', how = 'left')
#
#CalculatedRisk = pd.merge(CalculatedRisk, Activity_agg, on = 'ConsentNo', how = 'left')
#CalculatedRisk = pd.merge(CalculatedRisk, Inspection_agg, on 'ConsentNo', how = 'left')
#CalculatedRisk = pd.merge(CalculatedRisk, )
                                
Segmentation['CampNote'] = np.nan
Segmentation.loc[Segmentation['CampaignRisk'] == 'Warning', 'CampNote'] = 'fancy phrase'


