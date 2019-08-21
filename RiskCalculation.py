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


# Set Variables
ReportName= 'Water Inspection Prioritzation Model'
RunDate = datetime.now()

#Iplaod baseline
#Segmentation = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Export\\Baseline.csv")
Segmentation = Baseline

# Consent Risk



## Campaign Risk
HighRiskCamp = 'Warning'
LowRiskCamp = np.nan

conditions = [
	((Segmentation['ConsentProject'].isnull()) &
	(Segmentation['WAPProject'].isnull()))# could be double counted by inspections
             ]
choices = [LowRiskCamp]
Segmentation['CampaignRisk'] = np.select(conditions, choices, default = HighRiskCamp)


##Water User Group
HighRiskWUG = 'Warning'
LowRiskWUG = np.nan

conditions = [
	(Segmentation['WUGNo'].isnull())
			 ]
choices = [LowRiskWUG]
Segmentation['WUGRisk'] = np.select(conditions, choices, default = HighRiskWUG)


## Adaptive Management
HighRiskAM = 'Warning'
LowRiskAM = np.nan

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
LowRiskConditions = 0

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
HighRiskWaiver = -100
LowRiskWaiver = 0

conditions = [
	(Segmentation['Waiver'] == 'Yes' ),
			 ]
choices = [HighRiskWaiver]
Segmentation['WaiverRisk'] = np.select(conditions, choices, default = LowRiskWaiver)

#Shared Meter
HighRiskSharedMeter = 'Warning'
LowRiskSharedMeter = np.nan

conditions = [
	(Segmentation['SharedMeter'] == 'Yes' )
			 ]
choices = [HighRiskSharedMeter]
Segmentation['SharedMeterRisk'] = np.select(conditions, choices, default = LowRiskSharedMeter)


#Shared WAP
HighRiskSharedWAP = 'Warning'
LowRiskSharedWAP = np.nan

conditions = [
	(Segmentation['ConsentsOnWAP'] > 1 ),
			 ]
choices = [HighRiskSharedWAP]
Segmentation['SharedWAPRisk'] = np.select(conditions, choices, default = LowRiskSharedWAP)


#Risk under 5ls
HighRiskUnder5ls = -100
LowRiskSharedUnder5ls = 0

conditions = [
	((Segmentation['MaxRateProRata'] < 5) | (Segmentation['WAPRate'] < 5)),
    ((Segmentation['MaxRateProRata'] >= 5) | (Segmentation['WAPRate'] >= 5))
			 ]
choices = [HighRiskSharedMeter,LowRiskSharedMeter]
Segmentation['Under5lsRisk'] = np.select(conditions, choices, default = np.nan)

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
NoiseRiskAV = -5
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


conditions = [
        (Segmentation['WS_PercentAnnualVolume'] > HighVolumeThresholdAV),
        (Segmentation['WS_PercentAnnualVolume'] < LowVolumeThresholdAV),
        ((Segmentation['WS_PercentAnnualVolume'] > LowVolumeThresholdAV) & 
        (Segmentation['WS_PercentAnnualVolume'] <= MedVolumeThresholdAV)),
        ((Segmentation['WS_PercentAnnualVolume'] > MedVolumeThresholdAV) & 
        (Segmentation['WS_PercentAnnualVolume'] <= HighVolumeThresholdAV))
             ]
choices = [NoiseRiskAV, LowRiskAV, MedRiskMR, HighRiskAV]
Segmentation['WS_AVNC'] = np.select(conditions, choices, default = np.nan)



#MaxRoTNC
HighThresholdMaxRoT = 115
LowThresholdMaxRoT = 105
HighRiskMaxRoT = 3 # was 100
MedRiskMaxRoT = 1
LowRiskMaxRoT = 0
   
conditions = [
        (Segmentation['CS_PercentMaxRate'] > HighThresholdMaxRoT),
        (Segmentation['CS_PercentMaxRate'] < LowThresholdMaxRoT),
        ((Segmentation['CS_PercentMaxRate'] > LowThresholdMaxRoT) & 
        (Segmentation['CS_PercentMaxRate'] <= HighThresholdMaxRoT))
             ]
choices = [HighRiskMaxRoT, LowRiskMaxRoT, MedRiskMaxRoT]
Segmentation['CS_MaxRoTNC'] = np.select(conditions, choices, default = np.nan)


conditions = [
        (Segmentation['WS_PercentMaxRoT'] > HighThresholdMaxRoT),
        (Segmentation['WS_PercentMaxRoT'] < LowThresholdMaxRoT),
        ((Segmentation['WS_PercentMaxRoT'] > LowThresholdMaxRoT) & 
        (Segmentation['WS_PercentMaxRoT'] <= HighThresholdMaxRoT))
             ]
choices = [HighRiskMaxRoT, LowRiskMaxRoT, MedRiskMaxRoT]
Segmentation['WS_MaxRoTNC'] = np.select(conditions, choices, default = np.nan)


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
        (Segmentation['CampaignRisk'] == 'Warning'),
         ((Segmentation['AMRisk'] == 'Warning') |
             (Segmentation['WUGRisk'] == 'Warning'))
             ]
choices = [0,2]
Segmentation['InspectionNecessity'] = np.select(conditions, choices, default = 1)
# 0 - do not inspect
# 1 - Sample
# 2 - must inspect





Segmentation['DoNotInspectWAP'] = np.where(
        (Segmentation['WaiverRisk'] == 'Warning') | 
        (Segmentation['Under5lsRisk'] == 'Warning'), 
        1, np.nan)

Segmentation['DoNotInspectConsent'] = np.where(
        Segmentation['CampaignRisk'] == 'Warning', 
        'Warning', np.nan)


        
        
        )
col_list = [
        'VerificationRisk',
        'LatestInspectionRisk',
        'LatestNonComplianceRisk',
        'CountNonComplianceRisk',
        'T_MRNC',
#        'WS_MRNC',
        'CS_AVNC',
        'CS_MaxRoTNC',
#        'WS_MaxRoTNC',
        'CS_TimeRoTNC',
#        'WS_TimeRoTNC'
            ]

Segmentation['TotalRisk'] = Segmentation[col_list].sum(axis=1)

temp = Segmentation.groupby(
        ['ConsentNo'], as_index=False
        ).agg({
                'TotalRisk': 'max',
                'DoNotInspectConsent' : 'max',
                'DoNotInspectWAP' : 'sum',
                'Waiver' : 'count',
                'WAPsOnConsent' : 'max',
                'InspectionAssignment' : 'max'
                'InspectionNecessity' : 'min'
                })


temp['WAPsToInspect'] = temp['WAPsOnConsent'] - temp['Waiver']
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


