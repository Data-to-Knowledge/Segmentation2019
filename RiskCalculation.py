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
from datetime import datetime, timedelta, date


##############################################################################
### Set Variables
##############################################################################

# Base Variable
ReportName= 'Water Inspection Prioritzation Model - Risk Calculation'
RunDate = str(date.today())

### Baseline File
BaselineFile = 'Baseline2019-09-18.csv'


##############################################################################
### Import data
##############################################################################

#Uplaod baseline
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
choices = [LowRiskConditions,'']
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


### Inactive Well Warning
HighRiskInactiveWell = 'Warning'
LowRiskInactiveWell = ''

conditions = [
	(Segmentation['WellStatus'] == 'Inactive' ),
			 ]
choices = [HighRiskInactiveWell]
Segmentation['InactiveWellRisk'] = np.select(
                                        conditions, 
                                        choices, 
                                        default = LowRiskInactiveWell)

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
# 1 - first  push
# 2 - second push
# 3 - third push
conditions = [
        ((Segmentation['ConditionRisk'] == 'Warning') |
                (Segmentation['WUGRisk'] == 'Warning') |
                (Segmentation['SharedMeterRisk'] == 'Warning')),
        ((Segmentation['SharedWAPRisk'] == 'Warning') |
        (Segmentation['WUGRisk'] == 'Warning'))
             ]
choices = [2,3]
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
               (Segmentation['Under5lsRisk'] == 'Warning') |
               (Segmentation['InactiveWellRisk'] == 'Warning') |
               (Segmentation['WaiverRisk'] == 'Warning') |
               (Segmentation['MidSeasonRisk'] == 'Warning')),
         ((Segmentation['AMRisk'] == 'Warning') |
          (Segmentation['WUGRisk'] == 'Warning')      
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
                                                'SharedWAPRisk'
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
                                        'CS_DayNDVNC', #not used in first push                                          
                                        'WS_MedRateNC', #not used in first push
                                        'WS_TimeRoTNC', #not used in first push
                                        'WS_DayNDVNC', #not used in first push
                                        ]].sum(axis=1)


##############################################################################
### Save Output
##############################################################################

### Format Segmentation

SegmentationOut = Segmentation[[
                                'ConsentNo',
                                'WAP',
                                'CWMSZone',
                                'HolderAddressFullName',
                                'InspectionID',
                                'CampaignRisk',
                                'MidSeasonRisk',                               
                                'WUGRisk',                                
                                'AMRisk',                                
                                'ConditionRisk',                                
                                'WaiverRisk',                                
                                'InactiveWellRisk',                                
                                'SharedMeterRisk',
                                'MultipleMeterRisk',
                                'SharedWAPRisk',
                                'Under5lsRisk',
                                'LatestInspectionRisk',
                                'LatestNonComplianceRisk',
                                'CountNonComplianceRisk',
                                'VerificationRisk',
                                'T_MRNC',
                                'CS_AVNC',
                                'CS_MedRateNC',
                                'WS_MedRateNC',
                                'CS_TimeRoTNC',
                                'WS_TimeRoTNC',
                                'CS_DayNDVNC',
                                'WS_DayNDVNC',              
                                'WAPsOnConsent',               
                                'ConsentsOnWAP',                                
                                'InspectionAssignment',
                                'InspectionNecessity',
                                'ComplexityLevel',
                                'TotalRisk'
                                ]]

### Export Segmentation
SegmentationOut.to_csv(
        r'D:\\Implementation Support\\Python Scripts\\scripts\\Import\\'+
        'Segmentation' + RunDate + '.csv')

##############################################################################
### Create Table of Inspections
##############################################################################

InspectionList = Segmentation[Segmentation['InspectionNecessity'] != 0]

InspectionList = InspectionList.groupby(
        ['ConsentNo'], as_index=False
        ).agg({
                'HolderAddressFullName': 'max',
                'CWMSZone' :'max',
                'AdMan' :'max',
                'WUGNo' : 'max', 
                'MidSeasonRisk' : 'max', 
                'CampaignRisk' : 'max',
                'TotalRisk': 'max',
                'InspectionAssignment' : 'max',
                'InspectionNecessity' : 'min',
                'WAPsOnConsent' : 'max',               
                'ConsentsOnWAP' : 'max',
                'ComplexityLevel' : 'max'
                })


### Export Inspection List
InspectionList.to_csv(
        r'D:\\Implementation Support\\Python Scripts\\scripts\\Import\\'+
        'InspectionList' + RunDate + '.csv')


##############################################################################
### Create Inspection Note
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


### combine WAP and consent notes
SegmentationNote = Segmentation[['ConsentNo','WAP','ConsentNote','WAPNote','ClosingNote']]

SegmentationNote = SegmentationNote.groupby(
        ['ConsentNo','ConsentNote','ClosingNote']
        )['WAPNote'].apply(lambda x: ' - '.join(x)).reset_index()

SegmentationNote['InspectionNote'] = (SegmentationNote['ConsentNote'] +
                                     '  --  ' + 
                                     SegmentationNote['WAPNote'] + 
                                     '  --  ' +
                                     SegmentationNote['ClosingNote'])


### Join note back to segmentation file
SegmentationNote = SegmentationNote[['ConsentNo','InspectionNote']]

### Export Segmentation
SegmentationNote.to_csv(
        r'D:\\Implementation Support\\Python Scripts\\scripts\\Import\\'+
        'SegmentationNote' + RunDate + '.csv')


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