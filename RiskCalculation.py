# -*- coding: utf-8 -*-
"""
Created on Mon 19 August 2019
@author: KatieSi
"""
#reset
#clear

# Import packages
import numpy as np
import pandas as pd
import pdsql
from datetime import datetime, timedelta
import random

# Set Variables
ReportName= 'Water Inspection Prioritzation Model'
RunDate = datetime.now()
FirstRunCount = 540
SecondRunCount = 520

#Iplaod baseline
#Segmentation = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Export\\Baseline.csv")
Segmentation = Baseline

# Consent Risk



## Campaign Risk
HighRiskCamp = 'Warning'
LowRiskCamp = ''

conditions = [
	((Segmentation['ConsentProject'].isnull()) &
	(Segmentation['WAPProject'].isnull()))# could be double counted by inspections
             ]
choices = [LowRiskCamp]
Segmentation['CampaignRisk'] = np.select(conditions, choices, default = HighRiskCamp)

# Midseason Risk
Segmentation.loc[Segmentation['InspectionID'].notnull(), 'MidSeasonRisk'] = 'Warning'


##Water User Group
HighRiskWUG = 'Warning'
LowRiskWUG = ''

conditions = [
	(Segmentation['WUGNo'].isnull())
			 ]
choices = [LowRiskWUG]
Segmentation['WUGRisk'] = np.select(conditions, choices, default = HighRiskWUG)


## Adaptive Management
HighRiskAM = 'Warning'
LowRiskAM = ''

conditions = [
	(Segmentation['AdMan'].isnull())		 
			 ]
choices = [LowRiskAM]
Segmentation['AMRisk'] = np.select(conditions, choices, default = HighRiskAM)



##Location Risk

#requires this in main body:
#Location_agg = Location.groupby(
#	'ConsentNo', as_index=False
#	).agg({
#			'CWMSZone': 'max'}
#			'CWMSZone': 'count')
#Location_agg.rename(columns = {'max':'CWMSZone', 'count' : 'Locations' }, inplace=True)

#HighRiskLoc = 1
#MedRiskLoc
#LowRiskLoc = 0
#
#conditions = [
#	(Baseline.LocationCount == 1),
#	(Baseline.LocationCount isnull)		 
#			 ]
#choices = [LowRiskLoc, HighRiskLoc]
#Baseline['LocRisk'] = np.select(conditions, choices, default = MedRiskLoc)


## Allocation
HighGWThresholdAllo = 1500000
LowGWThresholdAllo = 0 #optional
HighSWThresholdAllo = 100
LowSWThresholdAllo = 0 #optional

HighRiskAllo = 0
MedRiskAllo = 0
LowRiskAllo = 0

conditions = [
	((Segmentation['Activity'] == 'take groundwater') & (Segmentation['FEVolume'] >= HighGWThresholdAllo)),
    ((Segmentation['Activity'] == 'take groundwater') & (Segmentation['FEVolume'] < HighGWThresholdAllo)),
    ((Segmentation['Activity'] == 'take groundwater') & (Segmentation['FEVolume'].isnull())),    
#	((Segmentation['Activity'] == 'take groundwater') & (Segmentation['FEVolume'] < LowGWThresholdAllo)),
	((Segmentation['Activity'] == 'take surface water') & 
          (
                  (Segmentation['CombinedWAPProRataRate'] >= HighSWThresholdAllo) | 
                  (Segmentation['CombinedConProRataRate'] >= HighSWThresholdAllo) |
                  (Segmentation['ConsentRate'] >= HighSWThresholdAllo)
                  )),
    ((Segmentation['Activity'] == 'take surface water') & 
          (
                  (Segmentation['CombinedWAPProRataRate'].isnull()) & 
                  (Segmentation['CombinedConProRataRate'].isnull()) &
                  (Segmentation['ConsentRate'].isnull())
                  ))
#	((Segmentation['Activity'] == 'take surface water') & (Segmentation['FEVolume'] < LowSWThresholdAllo))
			 ]
choices = [HighRiskAllo,
           LowRiskAllo,
           np.nan,
#           LowRiskAllo, 
           HighRiskAllo, 
           np.nan
#           LowRiskAllo
]
Segmentation['AllocationRisk'] = np.select(conditions, choices, default = LowRiskAllo)


#Complex Conditions Risk
HighRiskConditions = 'Warning'
LowRiskConditions = ''

conditions = [
	(Segmentation['ComplexAllocations'] == 'No' ),
    (Segmentation['ComplexAllocations'].isnull() )
			 ]
choices = [LowRiskConditions,np.nan]
Segmentation['ConditionRisk'] = np.select(conditions, choices, default = HighRiskConditions)

## Verification
HighRiskVerification = 3
MedRiskVerification = 1
LowRiskVerification = 0

conditions = [
	(Segmentation.VerificationOutOfDate == 'No'),
	(Segmentation.LatestVerification.isnull()),		 
			 ]
choices = [LowRiskVerification, HighRiskVerification]
Segmentation['VerificationRisk'] = np.select(conditions, choices, default = MedRiskVerification)

#Waiver Risk
HighRiskWaiver = 'Warning'
LowRiskWaiver = ''

conditions = [
	(Segmentation['Waiver'] == 'Yes' ),
			 ]
choices = [HighRiskWaiver]
Segmentation['WaiverRisk'] = np.select(conditions, choices, default = LowRiskWaiver)

#Shared Meter
HighRiskSharedMeter = 'Warning'
LowRiskSharedMeter = ''

conditions = [
	(Segmentation['SharedMeter'] == 'Yes' )
			 ]
choices = [HighRiskSharedMeter]
Segmentation['SharedMeterRisk'] = np.select(conditions, choices, default = LowRiskSharedMeter)

#Multiple Meter
HighRiskMultiMeter = 'Warning'
LowRiskMultiMeter = ''

conditions = [
	(Segmentation['T_MetersRecieved'] > 1)
			 ]
choices = [HighRiskSharedMeter]
Segmentation['MultipleMeterRisk'] = np.select(conditions, choices, default = LowRiskSharedMeter)

                            
#Shared WAP
HighRiskSharedWAP = 'Warning'
LowRiskSharedWAP = ''

conditions = [
	(Segmentation['ConsentsOnWAP'] > 1 ),
			 ]
choices = [HighRiskSharedWAP]
Segmentation['SharedWAPRisk'] = np.select(conditions, choices, default = LowRiskSharedWAP)


#Risk under 5ls
HighRiskUnder5ls = 'Warning'
LowRiskSharedUnder5ls = ''

conditions = [
	(Segmentation['WAPRate'] < 5),
			 ]
choices = [HighRiskSharedMeter]
Segmentation['Under5lsRisk'] = np.select(conditions, choices, default = LowRiskSharedMeter)

# Inspection History Risk

#needs to considor inspections scheduled but never done and inspections on going
#latest inspection date
#count inspections
#count non-compliance
#count compliance

#Inspection Date Risk
DateThresholdLatestInspection = datetime.strptime('2018-07-01', '%Y-%m-%d')
HighRiskLatestInspection = 3
MedRiskLatestInspection = 1
LowRiskLatestInspection = 0

conditions = [
	(Segmentation['LatestInspection'] <= DateThresholdLatestInspection),
	(Segmentation['LatestInspection'] > DateThresholdLatestInspection),
    (Segmentation['LatestInspection'].isnull())
			 ]
choices = [LowRiskLatestInspection, MedRiskLatestInspection,HighRiskLatestInspection]
Segmentation['LatestInspectionRisk'] = np.select(conditions, choices, default = np.nan)


# Non Comp Date risk
DateThresholdLatestNonComp = datetime.strptime('2018-07-01', '%Y-%m-%d')

HighRiskLatestNonComp = 3
MedRiskLatestNonComp = 1
LowRiskLatestNonComp = 0

conditions = [
	(Segmentation['LatestNonComplianceDate'] < DateThresholdLatestNonComp),
	(Segmentation['LatestNonComplianceDate'] >= DateThresholdLatestNonComp),
    (Segmentation['LatestNonComplianceDate'].isnull())
			 ]
choices = [HighRiskLatestNonComp,MedRiskLatestNonComp,LowRiskLatestNonComp]
Segmentation['LatestNonComplianceRisk'] = np.select(conditions, choices, default = np.nan)


##Non comp counts
##noncmp  records
#0	4128
#1	1970
#2	588
#3	247
#4	44
#5	49
#6	4
#7	9

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
Segmentation['CountNonComplianceRisk'] = np.select(conditions, choices, default = MedRiskNonComp)


# No Data Days (Hydro)
HighThresholdTMR = 10 # was 100
LowThresholdTMR = 0
HighRiskTMR = 3 #was 10000
MedRiskTMR = 1 # was 5
LowRiskTMR = 0
#OtherRiskMR = 5000

conditions = [
        (Segmentation['T_AverageMissingDays'] > HighThresholdTMR),
        (Segmentation['T_AverageMissingDays'] == LowThresholdTMR),
        ((Segmentation['T_AverageMissingDays'] > LowThresholdTMR) & 
        (Segmentation['T_AverageMissingDays'] <= HighThresholdTMR))
             ]
choices = [HighRiskTMR, LowRiskTMR, MedRiskTMR]
Segmentation['T_MRNC'] = np.select(conditions, choices, default = np.nan)



#MRNC - zeros vs Nulls. SQL says 0 is null Python says false
HighThresholdMR = 10 # was 100
LowThresholdMR = 0
HighRiskMR = 3 #was 10000
MedRiskMR = 1 # was 5
LowRiskMR = 0
#OtherRiskMR = 5000

conditions = [
        (Segmentation['WS_TotalMissingRecord'] > HighThresholdMR),
        (Segmentation['WS_TotalMissingRecord'] == LowThresholdMR),
        ((Segmentation['WS_TotalMissingRecord'] > LowThresholdMR) & 
        (Segmentation['WS_TotalMissingRecord'] <= HighThresholdMR))
             ]
choices = [HighRiskMR, LowRiskMR, MedRiskMR]
Segmentation['WS_MRNC'] = np.select(conditions, choices, default = np.nan)


#AVNC
HighVolumeThresholdAV = 200
MedVolumeThresholdAV = 110
LowVolumeThresholdAV = 100
NoiseRiskAV = 'Warning'
HighRiskAV = 3 #was 2000
MedRiskAV = 1 # was 100
LowRiskAV = 0
#OtherRiskAV = 1

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



#MaxRoTNC
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


conditions = [
        (Segmentation['WS_PercentMedRate'] > HighThresholdMaxRoT),
        (Segmentation['WS_PercentMedRate'] < LowThresholdMaxRoT),
        ((Segmentation['WS_PercentMedRate'] > LowThresholdMaxRoT) & 
        (Segmentation['WS_PercentMedRate'] <= HighThresholdMaxRoT))
             ]
choices = [HighRiskMaxRoT, LowRiskMaxRoT, MedRiskMaxRoT]
Segmentation['WS_MedRateNC'] = np.select(conditions, choices, default = np.nan)


#conditions = [
#    (pd.isnull(ConsentSummary['ConsentRate'])) | 
#        (pd.isnull(ConsentSummary['CS_MaxRate'])) | 
#        (ConsentSummary['CS_PercentMaxRate'] <= LowThresholdMaxRoT),
#    (ConsentSummary['CS_PercentMaxRate'] > HighThresholdMaxRoT)
#             ]
#choices = [LowRiskMaxRoT,HighRiskMaxRoT]
#ConsentSummary['CS_MaxRoTNC'] = np.select(conditions, choices, default = MedRiskMaxRoT)

#conditions = [
#    (pd.isnull(WAPSummary['WAPRate'])) | 
#        (pd.isnull(WAPSummary['WS_MaxRate'])) | 
#        (WAPSummary['WS_PercentMaxRoT'] <= LowThresholdMaxRoT),
#    (WAPSummary['WS_PercentMaxRoT'] > HighThresholdMaxRoT)
#             ]
#choices = [LowRiskMaxRoT,HighRiskMaxRoT]
#WAPSummary['WS_MaxRoTNC'] = np.select(conditions, choices, default = MedRiskMaxRoT)


##MedRoTNC
#HighThresholdMedRoT = 105
#LowThresholdMedRoT = 100
#HighRiskMedRoT = 100
#MedRiskMedRoT = 1
#LowRiskMedRoT = 0
#  
#conditions = [
#    (pd.isnull(ConsentSummary['ConsentRate'])) | 
#        (pd.isnull(ConsentSummary['CS_MedianRateAbove'])) | 
#        (ConsentSummary['CS_PercentMedRate'] <= LowThresholdMedRoT),
#    (ConsentSummary['CS_PercentMedRate'] > HighThresholdMedRoT)
#             ]
#choices = [LowRiskMedRoT,HighRiskMedRoT]
#ConsentSummary['CS_MedRoTNC'] = np.select(conditions, choices, default = MedRiskMedRoT)
#
#conditions = [
#    (pd.isnull(WAPSummary['WAPRate'])) | 
#        (pd.isnull(WAPSummary['WS_MedianRateAbove'])) | 
#        (WAPSummary['WS_PercentMedRoT'] <= LowThresholdMedRoT),
#    (WAPSummary['WS_PercentMedRoT'] > HighThresholdMedRoT)
#             ]
#choices = [LowRiskMedRoT,HighRiskMedRoT]
#WAPSummary['WS_MedRoTNC'] = np.select(conditions, choices, default = MedRiskMedRoT)


#TimeRoTNC
HighThresholdTimeRoT = 14 #advice from Complaince
LowThresholdTimeRoT = 1 #advice from complaince
HighRiskTimeRoT = 3 # waas 100
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

conditions = [
        (Segmentation['WS_TimeAboveRate'] > HighThresholdTimeRoT),
        (Segmentation['WS_TimeAboveRate'] < LowThresholdTimeRoT),
        ((Segmentation['WS_TimeAboveRate'] > LowThresholdTimeRoT) & 
        (Segmentation['WS_TimeAboveRate'] <= HighThresholdTimeRoT))
             ]
choices = [HighRiskTimeRoT, LowRiskTimeRoT, MedRiskTimeRoT]
Segmentation['WS_TimeRoTNC'] = np.select(conditions, choices, default = np.nan)


conditions = [
        ((Segmentation['ConditionRisk'] == 'Warning') |
                (Segmentation['WUGRisk'] == 'Warning') |
                (Segmentation['SharedMeterRisk'] == 'Warning')) 
             ]
choices = [2]
Segmentation['InspectionAssignment'] = np.select(conditions, choices, default = 1)
# 1 - first fornight push
# 2 - not first fornight push

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
Segmentation['InspectionNecessity'] = np.select(conditions, choices, default = 1)
# 0 - do not inspect
# 1 - Sample
# 2 - must inspect


Segmentation['ComplexityLevel'] = (Segmentation[[
                                                'ConditionRisk',
                                                'SharedMeterRisk',
                                                'MultipleMeterRisk',
                                                'SharedWAPRisk'
                                                ]]== 'Warning').sum(axis=1)


Segmentation['TotalRisk'] = Segmentation[[
                                        'VerificationRisk',
                                        'LatestInspectionRisk',
                                        'LatestNonComplianceRisk',
                                        'CountNonComplianceRisk',
                                        'T_MRNC',
#                                       'WS_MRNC',
                                        'CS_AVNC',
                                        'CS_MedRateNC',
#                                       'WS_MedRateNC',
                                        'CS_TimeRoTNC',
#                                       'WS_TimeRoTNC'
                                        ]].sum(axis=1)

InspectionList = Segmentation[Segmentation['InspectionNecessity'] != 0]

InspectionList = InspectionList.groupby(
        ['ConsentNo'], as_index=False
        ).agg({
                'HolderAddressFullName': 'max',
                'CWMSZone' :'max',
                'TotalRisk': 'max',
                'InspectionAssignment' : 'max',
                'InspectionNecessity' : 'min',
                'ComplexityLevel' : 'max'
                })



MidSeasonCount = Segmentation[Segmentation['InspectionID'].notnull()]
MidSeasonCount = len(MidSeasonCount[['ConsentNo']].drop_duplicates())
Fortnight1 = FirstRunCount-MidSeasonCount
Fortnight2 = FirstRunCount

FirstInspections = InspectionList[(InspectionList['InspectionAssignment'] == 1 )]

F1Inspections = FirstInspections.sample(n=Fortnight1, weights = 'TotalRisk', random_state = 1)

F1Inspections.to_csv('F1Inspections.csv')
# import inspection numbers


F1Inspections['Fortnight'] = 1

#inspection ID added back on....

F1InspectionsList = F1Inspections[['ConsentNo','Fortnight']]

FirstInspections = pd.merge(FirstInspections, F1InspectionsList, on = 'ConsentNo', how = 'left')
FirstInspections = FirstInspections[(FirstInspections['Fortnight'] != 1)]
FirstInspections = FirstInspections.drop(['Fortnight'], axis=1)

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
InspectionAllocations.loc[InspectionAllocations['Fortnight'] == 2, 'RMO'] = random.choices(Officers['RMO'],Officers['F2Weight'], k= FirstRunCount)

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
                'ComplexityLevel' : 'max'
                })

MidseasonInspectionList['RMO'] = random.choices(Officers['RMO'],Officers['F1Weight'], k=len(MidseasonInspectionList))

MidseasonInspectionList = pd.merge(MidseasonInspectionList, MidseasonInspections, on = 'ConsentNo', how = 'left')

MidseasonInspectionList.to_csv('MidseasonInspectionList.csv')


#############################################################################
MidseasonInspectionsList = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\MidseasonInspectionsList.csv")

InspectionAllocations = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\FirstPush.csv")



SecondInspections = InspectionList

SecondInspections = pd.merge(SecondInspections, InspectionAllocations, on = 'ConsentNo', how = 'left')
SecondInspections = SecondInspections[(SecondInspections['Fortnight'] != 1)|(SecondInspections['Fortnight'] != 2)]

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


Fortnight3 = SecondRunCount-NecessaryGroup3 - WUGGroup3
Fortnight4 = SecondRunCount-NecessaryGroup4 - WUGGroup4
Fortnight5 = SecondRunCount-NecessaryGroup5 - WUGGroup5



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
#                                F5InspectionsList,
                                NoInspectionsList
                                ])

InspectionList = pd.merge(InspectionList, InspectionAllocations, on = 'ConsentNo', how = 'left')


Officers = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\Officers.csv")



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




# mark which were selected and assigened - InspID and Monitoring Officer
#campaign Level Notes
Segmentation['IntroNote'] = ('This consent has been assessed through the 2018/19 inspection allocation model. ')
Segmentation['CampaignNote'] = np.where(Segmentation['CampaignRisk'] == 'Warning',
                                        'Consent is part of an ongoing project and was not selected for inspection. ',
                                            ' ')

Segmentation['AdManNote'] = np.where(Segmentation['AMRisk'] == 'Warning', 
            'Contains Adaptive Management Conditions. ',' ')
Segmentation['WUGNote'] = np.where(Segmentation['WUGNo'].notnull(),
                            (
                            'Member of Water User Group ' + 
                            Segmentation['WUGNo'] + '-' +
                            Segmentation['WUGName'] +
                            '. Monitoring Officer : '+
                            Segmentation['WUGMonOfficer'] + '. '
                            ).astype(str),' ')

Segmentation['ParentNote'] = np.where(Segmentation['ParentChildConsent'] != '', 
                                ('Consent is related to '+ 
                                 Segmentation['ParentChildConsent'] + 
                                 ' on Parent tree. ').astype(str)
                            ,' ')


Segmentation['WAPCountNote'] = np.where(Segmentation['WAPsOnConsent'] > 1,
                                (Segmentation['WAPsOnConsent'].astype(str) +
                                ' WAPS on the consent. ').astype(str),
                                 ' ')
#Segmentation['ConsentNote2'] = (
#                                Segmentation['IntroNote'] +
#                                Segmentation['CampaignNote'] +
#                                Segmentation['AdManNote']+
#                                Segmentation['WUGNote'] +                                
#                                Segmentation['ParentNote'] +
#                                Segmentation['WAPCountNote']
#                                )
#
Segmentation['ConsentNote']  = Segmentation[[
                                            'IntroNote',
                                            'CampaignNote',
                                            'AdManNote',
                                            'WUGNote',
                                            'ParentNote',
                                            'WAPCountNote'
                                            ]].astype(str).apply(lambda x: ''.join(x), axis=1)

#data["College"]= data["College"].str.cat(new, sep =", ", na_rep = na_string) 

#WAP level notes                                
Segmentation['SharedMeterNote'] = np.where(Segmentation['SharedMeterNote'].notnull(), (
                            Segmentation['WAP'] +
                            ' contains data from other WAPs. '), 
                            '')

Segmentation['MultipleMeterNote'] = np.where(Segmentation['T_MetersRecieved'] > 1, (
                            Segmentation['WAP'] +
                            ' has multiple meters reading the usage. '), 
                            '')

Segmentation['SharedWAPNote'] = np.where(Segmentation['ConsentsOnWAP'] > 1, (
                            Segmentation['WAP'] +
                            ' exists on '+
                            Segmentation['ConsentsOnWAP'].astype(str) +
                            ' consents active this season. '),
                            '')

Segmentation['VerificationNote'] = np.where(Segmentation['VerificationOutOfDate'] != 'Yes',
                            'Verification record is either out of date or does not exist. ',
                            '')

Segmentation['PoorDataNote'] = np.where(((Segmentation['CS_PercentAnnualVolume'] > 200) | 
                                        (Segmentation['CS_PercentMedRate'] > 200 )),
                                ('Suspected data spike or data quality issues on ' +
                                 Segmentation['WAP'] + '. '),
                                 '')
                                
Segmentation['WAPNote']  = Segmentation[[
                                            'SharedMeterNote',
                                            'MultipleMeterNote',
                                            'SharedWAPNote',
                                            'VerificationNote',
                                            'PoorDataNote'
                                            ]].astype(str).apply(lambda x: ''.join(x), axis=1)                                

SegmentationNote = Segmentation[['ConsentNo','WAP','ConsentNote','WAPNote']]
SegmentationNote = SegmentationNote.groupby(['ConsentNo','ConsentNote'])['WAPNote'].apply(lambda x: '\n'.join(x)).reset_index()
SegmentationNote['InspectionNote'] = temp['ConsentNote'] + '\n\n' + temp['WAPNote']


FirstInspections

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


