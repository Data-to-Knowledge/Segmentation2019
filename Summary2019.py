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
ReportName= 'Summary Tables'
RunDate = datetime.now()

# Set Risk Paramters
SummaryTableRunDate = datetime.strptime('2019-08-08', '%Y-%m-%d').date()


# Water Use Summary
ConsentSummaryCol = [
        'SummaryConsentID',
        'Consent',
        'Activity',
        'ErrorMsg',
#        'MaxAnnualVolume', #not used previous years
#        'MaxConsecutiveDayVolume', #not used previous years
#        'NumberOfConsecutiveDays', #not used previous years
        'MaxTakeRate', #not used previous years
#        'HasFlowRestrictions', #unreliable. not used previous years
#        'ComplexAllocation', #not used previous years
#        'TotalVolumeAboveRestriction',
#        'TotalDaysAboveRestriction',
#        'TotalVolumeAboveNDayVolume', #unreliable. not used previous years
#        'TotalDaysAboveNDayVolume', #unreliable. not used previous years
#        'MaxVolumeAboveNDayVolume', #unreliable. not used previous years
#        'MedianVolumeAboveNDayVolume', #unreliable. not used previous years
        'PercentAnnualVolumeTaken',
        'TotalTimeAboveRate', #not used previous years
        'MaxRateTaken',
        'MedianRateTakenAboveMaxRate' #not available in previous year
        ]
ConsentSummaryColNames = {
        'SummaryConsentID' : 'CS_ID',
        'Consent' : 'ConsentNo',
        'ErrorMsg' : 'CS_ErrorMSG',
        'MaxTakeRate' : 'ConsentRate',
        'PercentAnnualVolumeTaken': 'CS_PercentAnnualVolume',
        'TotalTimeAboveRate' : 'CS_TimeAboveRate', #not used previous years
        'MaxRateTaken' : 'CS_MaxRate',
        'MedianRateTakenAboveMaxRate' : 'CS_MedianRateAbove'
        }
ConsentSummaryImportFilter = {
       'RunDate' : SummaryTableRunDate
        }

ConsentSummary_date_col = 'RunDate'
ConsentSummary_from_date = SummaryTableRunDate
ConsentSummary_to_date = SummaryTableRunDate
ConsentSummaryServer = 'SQL2012Prod03'
ConsentSummaryDatabase =  'Hilltop'
ConsentSummaryTable = 'ComplianceSummaryConsent' #change after run is finished

ConsentSummary = pdsql.mssql.rd_sql(
                   server = ConsentSummaryServer,
                   database = ConsentSummaryDatabase, 
                   table = ConsentSummaryTable,
                   col_names = ConsentSummaryCol,
                   date_col = ConsentSummary_date_col,
                   from_date= ConsentSummary_from_date,
                   to_date = ConsentSummary_to_date
                   )

ConsentSummary.rename(columns=ConsentSummaryColNames, inplace=True)
ConsentSummary['ConsentNo'] = ConsentSummary['ConsentNo'] .str.strip().str.upper()
ConsentSummary['Activity'] = ConsentSummary['Activity'] .str.strip().str.lower()

ConsentSummary['CS_PercentMaxRate'] = (
        ConsentSummary['CS_MaxRate']/ConsentSummary['ConsentRate'])*100 
ConsentSummary['CS_PercentMedRate'] = (
        ConsentSummary['CS_MedianRateAbove']/ConsentSummary['ConsentRate'])*100

print('\nConsentSummary Table ',
      ConsentSummary.shape,'\n',
      ConsentSummary.nunique(), '\n\n')

# SummaryConsent = SummaryConsent[SummaryConsent['RunDate'] == SummaryTableRunDate]

WAPSummaryCol = [
        'SummaryWAPID',
        'Consent',
        'Activity',
        'WAP',
        'MeterName',
        'ErrorMsg',
#        'MaxAnnualVolume', #not used previous years
#        'MaxConsecutiveDayVolume', #not used previous years
#        'NumberOfConsecutiveDays', #not used previous years
        'MaxTakeRate', #not used previous years
        'FromMonth',#not used previous years
        'ToMonth',#not used previous years
#        'TotalVolumeAboveRestriction',
#        'TotalDaysAboveRestriction',
#        'TotalVolumeAboveNDayVolume', #unreliable. not used previous years
#        'TotalDaysAboveNDayVolume', #unreliable. not used previous years
#        'MaxVolumeAboveNDayVolume', #unreliable. not used previous years
#        'MedianVolumeTakenAboveMaxVolume', #unreliable. not used previous years
        'PercentAnnualVolumeTaken',
        'TotalTimeAboveRate', #not used previous years
        'MaxRateTaken',#not used previous years
        'MedianRateTakenAboveMaxRate', #not available in previous year
        'TotalMissingRecord'#unreliable.
        ]
WAPSummaryColNames = {
        'SummaryWAPID' : 'WS_ID',
        'Consent' : 'ConsentNo',
        'MaxTakeRate' : 'WAPRate',
        'FromMonth' : 'WAPFromMonth',
        'ToMonth' : 'WAPToMonth',
        'MeterName' : 'WS_MeterName',
        'ErrorMsg' : 'WS_ErrorMsg',
        'PercentAnnualVolumeTaken': 'WS_PercentAnnualVolume',
        'TotalTimeAboveRate' : 'WS_TimeAboveRate',
        'MaxRateTaken' : 'WS_MaxRate',#not used previous years
        'MedianRateTakenAboveMaxRate' : 'WS_MedianRateAbove', #not available in previous year
        'TotalMissingRecord' : 'WS_TotalMissingRecord'
        }
WAPSummaryImportFilter = {
       'RunDate' : SummaryTableRunDate
        }

WAPSummary_date_col = 'RunDate'
WAPSummary_from_date = SummaryTableRunDate
WAPSummary_to_date = SummaryTableRunDate
WAPSummaryServer = 'SQL2012Prod03'
WAPSummaryDatabase =  'Hilltop'
WAPSummaryTable = 'ComplianceSummaryWAP' #change after run is finished

WAPSummary = pdsql.mssql.rd_sql(
                   server = WAPSummaryServer,
                   database = WAPSummaryDatabase, 
                   table = WAPSummaryTable,
                   col_names = WAPSummaryCol,
                   date_col = WAPSummary_date_col,
                   from_date= WAPSummary_from_date,
                   to_date = WAPSummary_to_date
                   )

WAPSummary.rename(columns=WAPSummaryColNames, inplace=True)
WAPSummary['ConsentNo'] = WAPSummary['ConsentNo'] .str.strip().str.upper()
WAPSummary['Activity'] = WAPSummary['Activity'] .str.strip().str.lower()
WAPSummary['WAP'] = WAPSummary['WAP'] .str.strip().str.upper()

WAPSummary['WS_PercentMaxRoT'] = (
        WAPSummary['WS_MaxRate']/WAPSummary['WAPRate'])*100
WAPSummary['WS_PercentMedRoT'] = (
        WAPSummary['WS_MedianRateAbove']/WAPSummary.WAPRate)*100        

print('\nWAPSummary Table ',
      WAPSummary.shape,'\n',
      WAPSummary.nunique(), '\n\n')


############################################################################
Segmentation = pd.merge(Baseline, ConsentSummary, 
        on = ['ConsentNo','Activity'], 
        how = 'left')
Segmentation = pd.merge(Segmentation, WAPSummary, 
        on = ['ConsentNo','WAP','Activity','WAPFromMonth','WAPToMonth'], 
        how = 'left')

print('\nSegmentation Table ',
      Segmentation.shape,'\n',
      Segmentation.nunique(), '\n\n')


# many examples of limits on Accela not matching from hilltop
# 219 record w/o ConsentSummary but has ConsentLimit
# 2 records w/ ConsentSummary but no ConsentLimit
# 2312 records w/o WAPSummary but has WAPLimit
# 3266 records w/ WAPSummary but no REAL WAPLimits

# 4027 records have expected Summary - Limits matching

############################################################################


#SQL
#  distinct Consent
#  ,([master].[dbo].[fRemoveExtraCharacters](ErrorMsg)) as ErrorMessage
#  ,[TotalMissingRecord] 
  # ,(Case when ([TotalVolumeAboveRestriction] is null or [TotalVolumeAboveRestriction] = 0 or [TotalDaysAboveRestriction] is null) then 0 else 
  #   (case when ([TotalVolumeAboveRestriction]>100 and [TotalDaysAboveRestriction] > 2) then 100 else 1 end) end) as LFNC
  # ,(case when ([PercentAnnualVolumeTaken] <=100 or [PercentAnnualVolumeTaken] is null ) then 0 else 
  #   (case when ([PercentAnnualVolumeTaken] >200 ) then 2000 else 
  #     (case when ([PercentAnnualVolumeTaken] >100 ) then 100 else 1 end)end)end) as AVNC
  # ,(case when ([MaxVolumeAboveNDayVolume] is null or (([MaxVolumeAboveNDayVolume]+[MaxConsecutiveDayVolume])/[MaxConsecutiveDayVolume]*100)<=100) then 0 else
  #   (case when (([NumberOfConsecutiveDays]=1 and (([MaxVolumeAboveNDayVolume]+[MaxConsecutiveDayVolume])/[MaxConsecutiveDayVolume]*100)>105) or 
  #     ([NumberOfConsecutiveDays]>1 and (([MaxVolumeAboveNDayVolume]+[MaxConsecutiveDayVolume])/[MaxConsecutiveDayVolume]*100)>120) ) then 100 else 
  #   1 end)end) as CDNC
  # ,(case when ([MaxTakeRate] is null or [MaxRateTaken] is null or ([MaxRateTaken]/[MaxTakeRate])*100<=100 ) then 0 else 
  #   (case when (([MaxRateTaken]/[MaxTakeRate])*100>105) then 100 else 1 end) end) as RoTNC

  # ,(case when ([TotalMissingRecord] is null or [TotalMissingRecord] = 0) then 0 else
  #     (case when ([TotalMissingRecord] > 0 and [TotalMissingRecord] <= 10) then 5 else 
  #   (case when ([TotalMissingRecord] > 100) then 10000 else 5000 end)end)end) as MRNC





#MRNC - zeros vs Nulls. SQL says 0 is null Python says false
HighThresholdMR = 100
LowThresholdMR = 10
HighRiskMR = 10000
MedRiskMR = 5
LowRiskMR = 0
OtherRiskMR = 5000

conditions = [
    (pd.isnull(WAPSummary['WS_TotalMissingRecord'])) | 
        (WAPSummary['WS_TotalMissingRecord'] == 0),
    (WAPSummary['WS_TotalMissingRecord'] > HighThresholdMR),
    (WAPSummary['WS_TotalMissingRecord'] > 0) & 
        (WAPSummary['WS_TotalMissingRecord'] <= LowThresholdMR)
             ]
choices = [LowRiskMR, HighRiskMR, MedRiskMR]
WAPSummary['WS_MRNC'] = np.select(conditions, choices, default = OtherRiskMR)


#AVNC
HighVolumeThresholdAV = 200
LowVolumeThresholdAV = 100
HighRiskAV = 2000
MedRiskAV = 100
LowRiskAV = 0
OtherRiskAV = 1

conditions = [
    (pd.isnull(ConsentSummary['CS_PercentAnnualVolume'])) | 
        (ConsentSummary['CS_PercentAnnualVolume'] <= LowVolumeThresholdAV),
    (ConsentSummary['CS_PercentAnnualVolume'] > HighVolumeThresholdAV),
    (ConsentSummary['CS_PercentAnnualVolume'] > LowVolumeThresholdAV) & 
        (ConsentSummary['CS_PercentAnnualVolume'] <= HighVolumeThresholdAV)
             ]
choices = [LowRiskAV, HighRiskAV, MedRiskAV]
ConsentSummary['CS_AVNC'] = np.select(conditions, choices, default = OtherRiskAV)

conditions = [
    (pd.isnull(WAPSummary['WS_PercentAnnualVolume'])) | 
        (WAPSummary['WS_PercentAnnualVolume'] <= LowVolumeThresholdAV),
    (WAPSummary['WS_PercentAnnualVolume'] > HighVolumeThresholdAV),
    (WAPSummary['WS_PercentAnnualVolume'] > LowVolumeThresholdAV) & 
        (WAPSummary['WS_PercentAnnualVolume'] <= HighVolumeThresholdAV)
             ]
choices = [LowRiskAV, HighRiskAV, MedRiskAV]
WAPSummary['WS_AVNC'] = np.select(conditions, choices, default = OtherRiskAV)


#MaxRoTNC
HighThresholdMaxRoT = 105
LowThresholdMaxRoT = 100
HighRiskMaxRoT = 100
MedRiskMaxRoT = 1
LowRiskMaxRoT = 0
   
conditions = [
    (pd.isnull(ConsentSummary['ConsentRate'])) | 
        (pd.isnull(ConsentSummary['CS_MaxRate'])) | 
        (ConsentSummary['CS_PercentMaxRate'] <= LowThresholdMaxRoT),
    (ConsentSummary['CS_PercentMaxRate'] > HighThresholdMaxRoT)
             ]
choices = [LowRiskMaxRoT,HighRiskMaxRoT]
ConsentSummary['CS_MaxRoTNC'] = np.select(conditions, choices, default = MedRiskMaxRoT)

conditions = [
    (pd.isnull(WAPSummary['WAPRate'])) | 
        (pd.isnull(WAPSummary['WS_MaxRate'])) | 
        (WAPSummary['WS_PercentMaxRoT'] <= LowThresholdMaxRoT),
    (WAPSummary['WS_PercentMaxRoT'] > HighThresholdMaxRoT)
             ]
choices = [LowRiskMaxRoT,HighRiskMaxRoT]
WAPSummary['WS_MaxRoTNC'] = np.select(conditions, choices, default = MedRiskMaxRoT)


#MedRoTNC
HighThresholdMedRoT = 105
LowThresholdMedRoT = 100
HighRiskMedRoT = 100
MedRiskMedRoT = 1
LowRiskMedRoT = 0
  
conditions = [
    (pd.isnull(ConsentSummary['ConsentRate'])) | 
        (pd.isnull(ConsentSummary['CS_MedianRateAbove'])) | 
        (ConsentSummary['CS_PercentMedRate'] <= LowThresholdMedRoT),
    (ConsentSummary['CS_PercentMedRate'] > HighThresholdMedRoT)
             ]
choices = [LowRiskMedRoT,HighRiskMedRoT]
ConsentSummary['CS_MedRoTNC'] = np.select(conditions, choices, default = MedRiskMedRoT)

conditions = [
    (pd.isnull(WAPSummary['WAPRate'])) | 
        (pd.isnull(WAPSummary['WS_MedianRateAbove'])) | 
        (WAPSummary['WS_PercentMedRoT'] <= LowThresholdMedRoT),
    (WAPSummary['WS_PercentMedRoT'] > HighThresholdMedRoT)
             ]
choices = [LowRiskMedRoT,HighRiskMedRoT]
WAPSummary['WS_MedRoTNC'] = np.select(conditions, choices, default = MedRiskMedRoT)


#TimeRoTNC
HighThresholdTimeRoT = 20*24*60 #advice from Complaince
LowThresholdTimeRoT = 1*24*60 #advice from complaince
HighRiskTimeRoT = 100
MedRiskTimeRoT = 1
LowRiskTimeRoT = 0

conditions = [
    (pd.isnull(ConsentSummary['CS_TimeAboveRate'])) | 
        (ConsentSummary['CS_TimeAboveRate'] <= LowThresholdTimeRoT),
    (ConsentSummary['CS_TimeAboveRate'] > HighThresholdTimeRoT)
             ]
choices = [LowRiskTimeRoT,HighRiskTimeRoT]
ConsentSummary['CS_TimeRoTNC'] = np.select(conditions, choices, default = MedRiskTimeRoT)

conditions = [
    (pd.isnull(WAPSummary['WS_TimeAboveRate'])) | 
        (WAPSummary['WS_TimeAboveRate'] <= LowThresholdTimeRoT),
    (WAPSummary['WS_TimeAboveRate'] > HighThresholdTimeRoT)
             ]
choices = [LowRiskTimeRoT,HighRiskTimeRoT]
WAPSummary['WS_TimeRoTNC'] = np.select(conditions, choices, default = MedRiskTimeRoT)







#
#
#
#
## CDNC
#HighPercentThresholdCDV = 105
#LowPercentThresholdCDV = 100
#HighRiskCDV = 100
#MedRiskCDV = 1
#LowRiskCDV = 0
#
#Summary['PercentOverCDV'] = (Summary.MaxVolumeAboveNDayVolume+Summary.MaxConsecutiveDayVolume)/Summary.MaxConsecutiveDayVolume*100
#conditions = [
#    (pd.isnull(Summary['MaxVolumeAboveNDayVolume'])) | (Summary.PercentOverCDV <= LowPercentThresholdCDV),
#    ((Summary.NumberOfConsecutiveDays == 1) & (Summary.PercentOverCDV > HighPercentThresholdCDV)) | ((Summary.NumberOfConsecutiveDays > 1) & (Summary.PercentOverCDV > 120))
#             ]
#choices = [LowRiskCDV,HighRiskCDV]
#Summary['CDNC'] = np.select(conditions, choices, default = MedRiskCDV)
#
#
##LFNC
#VolumeThresholdLF = 100
#DaysThresholdLF = 2
#HighRiskLF = 100
#MedRiskLF = 1
#LowRiskLF = 0
#
#conditions = [
#    (pd.isnull(Summary['TotalVolumeAboveRestriction'])) | 
#        (Summary.TotalVolumeAboveRestriction == 0) | 
#        (Summary.TotalDaysAboveRestriction == np.nan),
#    (Summary.TotalVolumeAboveRestriction > VolumeThresholdLF) & 
#        (Summary.TotalDaysAboveRestriction > DaysThresholdLF)
#             ]
#choices = [LowRiskLF,HighRiskLF]
#Summary['LFNC'] = np.select(conditions, choices, default = MedRiskLF)
#
##Summary.to_csv(r'D:\Implementation Support\Python Scripts\scripts\Export\Summary.csv')