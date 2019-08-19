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
# Consent Risk
## Campaign Risk

HighRiskCamp = 1
LowRiskCamp = 0

conditions = [
	(
	(Baseline.RSCCamp isnotnull) |
	(Baseline.TACamp isnotnull) |
	(Baseline.STCamp isnotnull) |
	(Baseline.NDCamp isnotnull) |
	(Baseline.PDCCamp isnotnull) |
	(Baseline.Seg18Camp isnotnull)# could be double counted by inspections
	) 
             ]
choices = [HighRiskCamp, HighRiskCamp, HighRiskCamp, HighRiskCamp]
Baseline['CampRisk'] = np.select(conditions, choices, default = LowRiskCamp)


##Water User Group

HighRiskWUG = 1
LowRiskWUG = 0

conditions = [
	(Baseline.SMGNo isnotnull)
			 ]
choices = [HighRiskWUG]
Baseline['WUGRisk'] = np.select(conditions, choices, default = LowRiskWUG)

##Location Risk

#requires this in main body:
#Location_agg = Location.groupby(
#	'ConsentNo', as_index=False
#	).agg({
#			'CWMSZone': 'max'}
#			'CWMSZone': 'count')
#Location_agg.rename(columns = {'max':'CWMSZone', 'count' : 'Locations' }, inplace=True)

HighRiskLoc = 1
MedRiskLoc
LowRiskLoc = 0

conditions = [
	(Baseline.LocationCount == 1),
	(Baseline.LocationCount isnull)		 
			 ]
choices = [LowRiskLoc, HighRiskLoc]
Baseline['LocRisk'] = np.select(conditions, choices, default = MedRiskLoc)


# Activity Risk
## Adaptive Management
HighRiskAM = 1
LowRiskAM = 0

conditions = [
	(Activity.AdMan isnotnull)		 
			 ]
choices = [HighRiskAM]
Activity['AMRisk'] = np.select(conditions, choices, default = LowRiskAM)

## Allocation
HighGWThresholdAllo = 1500000
LowGWThresholdAllo = 0 #optional
HighSWThresholdAllo = 100
LowSWThresholdAllo = 0 #optional

HighRiskAllo = 2
MedRiskAllo = 1
LowRiskAllo = 0

conditions = [
	(Activity.Activity == 'GW') & (Activity.FEVolume >= HighGWThresholdAllo),
	(Activity.Activity == 'GW') & (Activity.FEVolume < LowGWThresholdAllo),
	(Activity.Activity == 'SW') & (Activity.FEVolume >= HighSWThresholdAllo),
	(Activity.Activity == 'SW') & (Activity.FEVolume < LowSWThresholdAllo)
			 ]
choices [HighRiskAllo, LowRiskAllo, HighRiskAllo, LowRiskAllo]
Activity['AlloRisk'] = np.select(conditions, choices, default = MedRiskAllo)

Activity_agg = Activity.groupby(
	['ConsentNo'], as_index = False
	).agg(
			{
			'AMRisk' : 'max',
			'AlloRisk' : 'max',
			'Activity' : 'count'
			}
		)



# Meter Risk

# Count WAP
# count Meter installed
# count meter verified
# percent meter installed
# percent meter verified

## Meter Install
HighRiskMI = 2
MedRiskMI = 1
LowRiskMI = 0

conditions = [
	(Meter.InstallXXX == X),
	(Meter.XXSTUFXX isnull),		 
			 ]
choices = [LowRiskMI, HighRiskMI]
Meter['MIRisk'] = np.select(conditions, choices, defalult = MedRiskMI)

## Meter Verifcation
DateThresholdMV = ('2014-07-01', '%Y-%m-%d')
HighRiskMV = 2
MedRiskMV = 1
LowRiskMV = 0

conditions = [
	(Meter.VerficationDate > DateThresholdMV),
	(Meter.VerficationDate isnull)
			 ]
choices = [LowRiskMV, HighRiskMV, default = MedRiskMV]
Meter['MVRisk'] = np.select(conditions, choices, default = MedRiskMV)

## Meter Waiver


## No Telemetry
HighDateThresholdNT = datetime.strptime('2018-07-01', '%Y-%m-%d')
LowDateThresholdNT = datetime.strptime('2018-01-01', '%Y-%m-%d') ###check
HighRiskNT = 2
MedRiskNT = 1
LowRiskNT = 0

conditions = [
	(Meter.TelemetryEnd > LowDateThresholdNT),
	(Meter.TelemetryEnd < HighDateThresholdNT) | (Meter.TelemetryEnd isnull)		 
			 ]
choices = [LowRiskNT, HighRiskNT]
Meter['NTRisk'] = np.select(conditions, choices, default = MedRiskNT)


Meter_agg = Meter.groupby(
  ['WAP'], as_index = False
  ).agg(
            {
            'MIRisk' : 'max',
            'MVRisk': 'max',
            'NTRisk': 'max',
            'MWRisk' : 'max',
            'Meter' : 'count'            
            }
        )



# WAP risk




# Inspection History Risk

#needs to considor inspections scheduled but never done and inspections on going
#latest inspection date
#count inspections
#count non-compliance
#count compliance

##Inspection Date Risk
HighDateThresholdID = datetime.strptime('2018-07-01', '%Y-%m-%d')
LowDateThresholdID = datetime.strptime('2016-07-01', '%Y-%m-%d')
HighRiskID = 2
MedRiskID = 1
LowRiskID = 0

conditions = [
	(Inspection.InspectionDate < LowDateThresholdID),
	(Inspection.InspectionDate > HighDateThresholdIH)
			 ]
choices = [LowRiskID,HighRiskID]
Inspection['IDRisk'] = np.select(conditions, choices, default = MedRiskID)

##Inspection Grade Risk
HighThresholdIG = ['','','',] #check wording
LowThresholdIG = 'Complies' #check wording
HighRiskIG = 2
MedRiskIG = 1
LowRiskIG = 0

conditions = [
	(Inspection.InspectionGrade == LowThresholdIG),
	(Inspection.Grade isin(HighThresholdIG))		
			]
choices = ['LowRiskIG, HighRiskIG']
Inspection['IGRisk'] = np.select(conditions, choices, default = MedRiskIG)


Inspection_agg = Inspection.groupgy(
	['ConsentNo'], as_index = False
	).agg(
			{
			'IDRisk' : 'max',
			'IGRisk' : 'max',
			'InspectionID': 'count'	
			}
		  )



# Summay Risk
#RoTNC
HighPercentThresholdRoT = 115
LowPercentThresholdRoT = 105
HighRiskRoT = 2
MedRiskRoT = 1
LowRiskRoT = 0

Summary['PercentOverRoT'] = ((
	Summary.MaxRateTaken/Summary.MaxTakeRate)*100)

conditions = [
    (Summary.MaxTakeRate == np.nan) | (Summary.MaxRateTaken == np.nan) | (Summary.PercentOverRoT<= LowPercentThresholdRoT),
    ((Summary.MaxRateTaken/Summary.MaxTakeRate)*100 > HighPercentThresholdRoT)
             ]
choices = [LowRiskRoT,HighRiskRoT]
Summary['RoTRisk'] = np.select(conditions, choices, default = MedRiskRoT)


# CDNC
HighPercentThresholdCDV = 110
LowPercentThresholdCDV = 105
HighRiskCDV = 2
MedRiskCDV = 1
LowRiskCDV = 0

Summary['PercentOverCDV'] = ((
	Summary.MaxVolumeAboveNDayVolume+
	Summary.MaxConsecutiveDayVolume)/
	Summary.MaxConsecutiveDayVolume*100
	)

conditions = [
    (Summary.MaxVolumeAboveNDayVolume == np.nan) | (Summary.PercentOverCDV <= LowPercentThresholdCDV),
    ((Summary.NumberOfConsecutiveDays == 1) & (Summary.PercentOverCDV > HighPercentThresholdCDV)) | ((Summary.NumberOfConsecutiveDays > 1) & (Summary.PercentOverCDV > 120))
             ]
choices = [LowRiskCDV,HighRiskCDV]
Summary['CDNC'] = np.select(conditions, choices, default = MedRiskCDV)


#AVNC
NoiseThresholdAV = 200
HighVolumeThresholdAV = 110
LowVolumeThresholdAV = 100
HighRiskAV = 2
MedRiskAV = 1
LowRiskAV = 0
OtherRiskAV = 9

conditions = [
    (Summary.PercentAnnualVolumeTaken == np.nan) | (Summary.PercentAnnualVolumeTaken <= LowVolumeThresholdAV),
    (Summary.PercentAnnualVolumeTaken > NoiseThresholdAV),
    (Summary.PercentAnnualVolumeTaken > LowVolumeThresholdAV) & (Summary.PercentAnnualVolumeTaken <= HighVolumeThresholdAV),
    (Summary.PercentAnnualVolumeTaken > HighVolumeThresholdAV) & (Summary.PercentAnnualVolumeTaken <= NoiseThresholdAV)
             ]
choices = [LowRiskAV, LowRiskAV, MedRiskAV, HighRiskAV]
Summary['AVRisk'] = np.select(conditions, choices, default = OtherRiskAV)


#LFNC
HighVolumeThresholdLF = 115
LowVolumeThresholdLF = 105
HighRiskLFV = 2
MedRiskLFV = 1
LowRiskLFV = 0

conditions = [
    (Summary.TotalVolumeAboveRestriction == np.nan) | (Summary.TotalVolumeAboveRestriction == 0),
    (Summary.TotalVolumeAboveRestriction > HighVolumeThresholdLF) 
             ]
choices = [LowRiskLFV,HighRiskLFV]
Summary['LFVRisk'] = np.select(conditions, choices, default = MedRiskLFV)


HighDaysThresholdLF = 2
LowDaysThresholdLF = 0
HighRiskLFD = 2
MedRiskLFD = 1
LowRiskLFD = 0

conditions = [
    (Summary.TotalDaysAboveRestriction == np.nan) | Summary.TotalDaysAboveRestriction == 0),
    (Summary.TotalDaysAboveRestriction > HighDaysThresholdLF)
             ]
choices = [LowRiskLFD,HighRiskLFD]
Summary['LFDRisk'] = np.select(conditions, choices, default = MedRiskLFD)


#MRNC
HighThresholdMR = 200
LowThresholdMR = 10
HighRiskMR = 10000
MedRiskMR = 5
LowRiskMR = 0
OtherRiskMR = 5000

conditions = [
    (Summary.TotalMissingRecord == np.nan) | (Summary.TotalMissingRecord == 0),
    (Summary.TotalMissingRecord > HighThresholdMR)
    (Summary.TotalMissingRecord > 0) & (Summary.TotalMissingRecord <= LowThresholdMR),
             ]
choices = [LowRiskMR, HighRiskMR, MedRiskAV]
Summary['MRNC'] = np.select(conditions, choices, default = OtherRiskAV)








WAP = pd.merge(WAP, Meter_agg, on = 'WAP', how = 'left')

CalculatedRisk = pd.merge(Consent, WAP_agg, on = 'ConsentNo', how = 'left')

CalculatedRisk = pd.merge(CalculatedRisk, Activity_agg, on = 'ConsentNo', how = 'left')
CalculatedRisk = pd.merge(CalculatedRisk, Inspection_agg, on 'ConsentNo', how = 'left')
CalculatedRisk = pd.merge(CalculatedRisk, )


