
--Baseline 2019
--Tables have been updated from 2018 edition
--		Temp tables moved to Hydro
--		Hydro was moved to production
--Verifications have been added
--Parameters have been updated
--		Dates
--		Match Rainbow


-----which consents are considered

SELECT 
	distinct (Wap.[RecordNo])
	,[B1_APPL_STATUS]
	,(case when (B1_APPL_STATUS ='Terminated - Replaced') then 'Child' else
		(case when (B1_APPL_STATUS in ('Issued - Active', 'Issued - s124 Continuance') and fmDate >  '2018-10-01 00:00:00.000') then 'Parent' else NULL end)
	end) as ParentorChild
	,(case when (B1_APPL_STATUS ='Terminated - Replaced') then [ChildAuthorisations] else
		(case when (B1_APPL_STATUS in ('Issued - Active', 'Issued - s124 Continuance') and fmDate >  '2018-10-01 00:00:00.000') then [ParentAuthorisations] else NULL end)
	end) as ParentChildConsent
	,MAX(CWMSZone.CWMSZone) as CWMSZone
	,fmDate
	,toDate
	,Per.HolderAddressFullName
    ,Wap.[Activity]
    ,SUM([Max Rate Pro Rata (l/s)]) as CombinedRate
	,MAX([Max Rate for WAP (l/s)]) as MaxRate
	,(Case when (COUNT(Distinct([From Month]))>1 or  COUNT( DISTINCT([To Month]))>1) then Max([Max Rate for WAP (l/s)]) else Null end) as MaxOfDifferentPeriodRate
	--,CAST(Con.[Consented Annual Volume (m3/year)] as Float) as CVolume
	,CAST(Dat.[Full Effective Annual Volume (m3/year)] as Float) as FEVolume
--into 
--	#tempAllConsents2 
FROM 
	[DataWarehouse].[dbo].[D_ACC_Act_Water_TakeWaterWAPAlloc] Wap --rate of take
	inner join [DataWarehouse].[dbo].[F_ACC_PERMIT] Per --permit
		on Wap.RecordNo=Per.B1_ALT_ID
	--left outer join [DataWarehouse].[dbo].[D_ACC_Act_Water_TakeWaterConsent] Con --consented annual volume
	--	on Wap.RecordNo=Con.RecordNo
	left outer join [DataWarehouse].[dbo].[D_ACC_Act_Water_TakeWaterAllocData] Dat --Effective annual volume Changed to left outer join!!!!!!!!!!!!!!!!!
		on Wap.RecordNo=Dat.RecordNo and Wap.Activity=Dat.Activity
	join (SELECT [B1_ALT_ID], [AttributeValue] as CWMSZone
			  FROM [DataWarehouse].[dbo].[F_ACC_SpatialAttributes2]
			  where [AttributeType] = 'CWMS Zone'
			 group by [B1_ALT_ID], [AttributeValue]
			) as CWMSZone on CWMSZone.B1_ALT_ID = per.B1_ALT_ID
where 
	Wap.Activity like 'Take%' 
	--and 
	--Con.Activity like 'Take%' 
	and 
	((B1_APPL_STATUS in ('Issued - Active', 'Issued - s124 Continuance') and fmDate <  '2019-06-30 00:00:00.000')
	or
	(B1_APPL_STATUS ='Terminated - Replaced' and toDate >  '2018-10-01 00:00:00.000' )) --changed to first of October
	and [B1_PER_SUB_TYPE]='Water Permit (s14)' 

group by Wap.[RecordNo]
	,[B1_APPL_STATUS]
	,ChildAuthorisations
	,[ParentAuthorisations]
	--,CWMSZone.CWMSZone
	,fmDate
	,toDate
	,Per.HolderAddressFullName
    ,Wap.[Activity]
	,[Full Effective Annual Volume (m3/year)] 



select
	distinct RecordNo
	,CWMSZone
	,[B1_APPL_STATUS]
	,ParentorChild
	,ParentChildConsent
	,fmDate
	,toDate
	,HolderAddressFullName
	,Activity
	,CombinedRate
	,FEVolume
	,(case when ((Activity like '%ground%' and FEVolume>=1500000) or (Activity like '%surface%' and CombinedRate>=100)) then 'High risk' else Null end) as EnvironmentalRisk
	,MaxRate
into 
	#consents
from 
	#tempAllConsents2
where 
	(CombinedRate is null and MaxRate >= 5) 
	or
	CombinedRate>=5 -- what about where there is no proper rate, or rate is crazy small?



----With WAP details for joining on Well details etc

select 
	distinct Co.RecordNo
	,CWMSZone
	,[B1_APPL_STATUS]
	,ParentorChild
	,ParentChildConsent
	,fmDate
	,toDate
	,HolderAddressFullName
	,Co.Activity
	,CombinedRate
	,FEVolume
	,EnvironmentalRisk
	,MaxRate
	,WAP 
into 
	#WAP
from 
	#consents Co
	inner join [DataWarehouse].[dbo].[D_ACC_Act_Water_TakeWaterWAPAlloc] WAP
		on Co.RecordNo=WAP.RecordNo
where 
	Wap.Activity like 'Take%' 


---------------------------------------------
-------Adaptive management consents

select 
	distinct (B1_ALT_ID)
	,wad.Activity
	,HolderAddressFullName
	,wad.[Allocation Block]
	,wad.[Full Effective Annual Volume (m3/year)]
into #ConditionsAdMan
from 
	[DataWarehouse].[dbo].[F_ACC_PERMIT] Per
	inner join [DataWarehouse].[dbo].[D_ACC_Act_Water_TakeWaterAllocData] wad
	on Per.B1_ALT_ID=wad.RecordNo
where
	[B1_PER_SUB_TYPE]='Water Permit (s14)' 
	and
	wad.[Allocation Block]='AV'

--select 
--	distinct [B1_ALT_ID] as RecordNo
----into 
----	#ConditionsAdMan
--from 
--	[DataWarehouse].[dbo].[D_ACC_Conditions]
--where
--	[ConditiontxtText] like '%adaptive management%' --Block allocation AM???

--------WUG

 SELECT 
	SMG.[B1_ALT_ID]
	,SMG.StatusType
	,SMG.[HolderAddressFullName]
	,SMG.[MonOfficer] as SMGMonOfficer
	,Rel.[ChildRecordNo]
into #WUG
FROM 
	[DataWarehouse].[dbo].[F_ACC_SelfManagementGroup] SMG
	Inner join  [DataWarehouse].[dbo].[D_ACC_Relationships] Rel
		on SMG.B1_ALT_ID=Rel.[ParentRecordNo]
	inner join [DataWarehouse].dbo.F_ACC_PERMIT per
		on Rel.[ChildRecordNo]=Per.B1_ALT_ID
where SMG.StatusType='OPEN' 


----- shared WAPs > Consents
SELECT 
   DISTINCT ([WAP])
   ,MAX([Max Rate for WAP (l/s)]) as MaxRateSharedWAP
	,Count(distinct ([RecordNo])) as RecordNoCount
	,Count(distinct([HolderEcanID])) as ECNoCount
into 
	#AllCount1
FROM 
	[DataWarehouse].[dbo].[D_ACC_Act_Water_TakeWaterWAPAlloc] wap
	inner join datawarehouse.dbo.F_ACC_Permit per
		on wap.RecordNo=per.B1_ALT_ID
where 
	Activity like 'Take%' and 
	[B1_APPL_STATUS] in ('issued - Active', 'Issued - S124 Continuance')
group by 
	WAP


Select 
	Distinct wap.recordno
into 
	#SharedWAP
from
	#AllCount1 AC
	inner join 
	[DataWarehouse].[dbo].[D_ACC_Act_Water_TakeWaterWAPAlloc] wap
	  on AC.WAP=Wap.WAP
	inner join datawarehouse.dbo.F_ACC_Permit per
		on wap.RecordNo=per.B1_ALT_ID
where 
	RecordNoCount >1
	and
	Activity like 'Take%' 
	and 
	[B1_APPL_STATUS] in ('issued - Active', 'Issued - S124 Continuance')



Select 
	Distinct wap.recordno
into 
	#SharedWAPMultiCH
from
	#AllCount1 AC
	inner join 
	[DataWarehouse].[dbo].[D_ACC_Act_Water_TakeWaterWAPAlloc] wap
	  on AC.WAP=Wap.WAP
	inner join datawarehouse.dbo.F_ACC_Permit per
		on wap.RecordNo=per.B1_ALT_ID
where 
	ECNoCount > 1
	and
	Activity like 'Take%' 
	and 
	[B1_APPL_STATUS] in ('issued - Active', 'Issued - S124 Continuance')



select
	Distinct SW.RecordNo as ConsentSharedWAP
	,(case when (MCH.RecordNo is not null) then 'Multiple' else 'Single' end) as CountCHSharedWAP
into 
	#SharedWAPCH
from
	#SharedWAP SW
	left outer join #SharedWAPMultiCH MCH
		on SW.RecordNo=MCH.RecordNo


--MaxRate on SharedWAP

Select 
	Distinct wap.recordno
into 
	#SharedWAPForRate
from
	#AllCount1 AC
	inner join 
	[DataWarehouse].[dbo].[D_ACC_Act_Water_TakeWaterWAPAlloc] wap
	  on AC.WAP=Wap.WAP
	inner join datawarehouse.dbo.F_ACC_Permit per
		on wap.RecordNo=per.B1_ALT_ID
where 
	RecordNoCount >1
	and
	ECNoCount = 1
	and
	Activity like 'Take%' 
	and 
	[B1_APPL_STATUS] in ('issued - Active', 'Issued - S124 Continuance')
	and 
	MaxRateSharedWAP=[Max Rate for WAP (l/s)]


-----------------------------------------------
---Measured, use data logger or water meter
select
	* 
into 
	#DataLogger 
from 
	sql2012prod05.wells.dbo.EPO_Dataloggers -- THIS IS NOT LOOKING AT METERS, THERE IS A GAP!!!
where 
	DateDeinstalled is null 
	AND 
	[LoggerID] <> 1 


-----Telemetry
SELECT 
	distinct [wap]
into 
	#Telemetry
FROM 
	[EDWProd01].[Hydro].[dbo].[HilltopUsageSiteDataLog] -- production copy from hydro
--	[Water_TEMP].[dbo].[HilltopUsageSiteDataLog20180710] --Static copy from dev01.hydro db
where 
	end_date > '2019-01-01 00:00:00.000' 
	and 
	folder = 'Telemetry' 
	--AND 
	--[hts_file] NOT LIKE '%.hts'

-----Combine all details to WAP
Select
	distinct (WD.Well_No)
	,COUNT(DL.ID) as CountDataloggers
	,MIN(DateInstalled) as MinDataLogDate
	,MAX(DateInstalled) as MaxDataLogDate
	,(Case when ([WM_Tmp_Waiver]=1) then 1 else 0 end) as Waiver
	,(Case when ([WM_Tmp_Waiver]=1 and [EPO_LAST_UPDATE] is not null) then [EPO_LAST_UPDATE] else NULL end) as WaiverEditDate
	,(Case when [GWuseAlternateWell] <> WD.Well_No then 1 else 0 end) as SharedMeter
	,(Case when (WD.[Status]='Active') THEN 1 else 0 end) as WellStatus    --20181130 CHANGED!!![Well_Status]='AE'
	,(Case when (tel.wap is not null) then 1 else 0 end) as Telemetered
into 
	#WAPDetails1
From
	SQL2012prod05.Wells.dbo.Well_Details WD
	left outer join
	#DataLogger DL
		on WD.Well_No=DL.Well_No
	left outer join 
	sql2012prod05.wells.dbo.[EPO_WELL_DETAILS] Epo
		on WD.Well_No=Epo.Well_No
	left outer join #Telemetry tel
		on WD.Well_No=tel.WAP
group by 
	WD.Well_No
	,[WM_Tmp_Waiver]
	,[EPO_LAST_UPDATE]
	,[GWuseAlternateWell]
	,WD.[Status]
	,tel.wap



select 
	distinct WAP.RecordNo
	,CWMSZone
	,[B1_APPL_STATUS]
	,ParentorChild
	,ParentChildConsent
	,fmDate
	,toDate
	,WAP.HolderAddressFullName
	,WAP.Activity
	,CombinedRate
	,FEVolume
	,EnvironmentalRisk
	,(Case when (AM.B1_ALT_ID is not null) then 'AdMan' else NULL end) as AdMan
	,WUG.[B1_ALT_ID] as WUGRecordNo
	,(case when (ConsentSharedWAP is not null) then 'SharedWAP' else NULL end) as SharedWAP
	,(case when (SWR.RecordNo is not null) then 'Main consent' else NULL end) as SharedWAPMainConsent 
	,CountCHSharedWAP
	,Count(Distinct(WAP.WAP)) as WAPCountACC
	,SUM(WellStatus) as CountWellStatusActive
	,SUM(Waiver)  as WaiverCount
	,MIN(WaiverEditDate) as MinWaiverDate --chosen minimum, confirm!
	,SUM(CountDataloggers) as CountDataloggers
	,MIN(MinDataLogDate) as MinDataLogDate
	,MAX(MaxDataLogDate) as MaxDataLogDate
	,SUM(Telemetered) as CountTelemetered
	,SUM(SharedMeter) as CountSharedMeter

into 
	#Baseline
from 
	#WAP WAP 
	left outer join #WAPDetails1 WD
		on WAP.WAP=WD.Well_No
	left outer join #ConditionsAdMan AM
		on WAP.RecordNo=AM.B1_ALT_ID
	left outer join #WUG WUG
		on WAP.RecordNo=WUG.[ChildRecordNo]
	left outer join #SharedWAPCH SW
		on WAP.RecordNo=SW.ConsentSharedWAP
	left outer join #SharedWAPForRate SWR
		on WAP.RecordNo=SWR.RecordNo
group by 
	WAP.RecordNo
	,CWMSZone
	,[B1_APPL_STATUS]
	,ParentorChild
	,ParentChildConsent
	,fmDate
	,toDate
	,WAP.HolderAddressFullName
	,WAP.Activity
	,CombinedRate
	,FEVolume
	,EnvironmentalRisk
	,AM.B1_ALT_ID
	,WUG.[B1_ALT_ID]
	,ConsentSharedWAP
	,SWR.RecordNo
	,CountCHSharedWAP






select 
	--c.*
	distinct Consent
	,([master].[dbo].[fRemoveExtraCharacters](ErrorMsg)) as ErrorMessage
	,[TotalMissingRecord] 
	,(Case when ([TotalVolumeAboveRestriction] is null or [TotalVolumeAboveRestriction] = 0 or [TotalDaysAboveRestriction] is null) then 0 else 
		(case when ([TotalVolumeAboveRestriction]>100 and [TotalDaysAboveRestriction] > 2) then 100 else 1 end) end) as LFNC
	,(case when ([PercentAnnualVolumeTaken] <=100 or [PercentAnnualVolumeTaken] is null ) then 0 else 
		(case when ([PercentAnnualVolumeTaken] >200 ) then 2000 else 
			(case when ([PercentAnnualVolumeTaken] >100 ) then 100 else 1 end)end)end) as AVNC
	,(case when ([MaxVolumeAboveNDayVolume] is null or (([MaxVolumeAboveNDayVolume]+[MaxConsecutiveDayVolume])/[MaxConsecutiveDayVolume]*100)<=100) then 0 else
		(case when (([NumberOfConsecutiveDays]=1 and (([MaxVolumeAboveNDayVolume]+[MaxConsecutiveDayVolume])/[MaxConsecutiveDayVolume]*100)>105) or ([NumberOfConsecutiveDays]>1 and (([MaxVolumeAboveNDayVolume]+[MaxConsecutiveDayVolume])/[MaxConsecutiveDayVolume]*100)>120) ) then 100 else 
		1 end)end) as CDNC
	,(case when ([MaxTakeRate] is null or [MaxRateTaken] is null or ([MaxRateTaken]/[MaxTakeRate])*100<=100 ) then 0 else 
		(case when (([MaxRateTaken]/[MaxTakeRate])*100>105) then 100 else 1 end) end) as RoTNC

	,(case when ([TotalMissingRecord] is null or [TotalMissingRecord] = 0) then 0 else
			(case when ([TotalMissingRecord] > 0 and [TotalMissingRecord] <= 10) then 5 else 
		(case when ([TotalMissingRecord] > 100) then 10000 else 5000 end)end)end) as MRNC


into #SummaryComplianceRisk
from 
	Hilltop.dbo.ComplianceSummary c



------data extract Hydro db

SELECT  
	Distinct [ExtSiteID]
	,COUNT (Distinct([DateTime])) as DayCount
	,Max([DateTime]) as MaxDate
into #CountWAPDays
FROM 
	[EDWProd01].[Hydro].[dbo].[TSDataNumericDaily]
where 
	[DatasetTypeID] in (9,12) 
	and 
	[DateTime] between '2018-07-01'and '2019-06-30'
group by 
	[ExtSiteID]


SELECT 
	Wap.RecordNo
	,COUNT(DISTINCT([ExtSiteID])) as SiteCount
    ,SUM([DayCount]) as DayCountSum
	,MAX([MaxDate]) as MaxDate
into #DayCountSum

FROM 
	#CountWAPDays WDC 
	inner join  [DataWarehouse].[dbo].[D_ACC_Act_Water_TakeWaterWAPAlloc] Wap
	on WDC.ExtSiteID=wap.WAP
where Wap.Activity like 'Take%' 
group by Wap.RecordNo







---------------------------------------
--add inspection history 16/17
select 
	distinct (ins.B1_ALT_ID) as RecordNo
	--,ins.InspectionID
	--,ins.NextInspectionDate
	,ins.[InspectionStatus]
	--,rel.[PARENT_InspectionID]
	--,rel.[CHILD_InspectionID]
	--,rel2.[PARENT_InspectionID]
	--,rel2.[CHILD_InspectionID]
into #Inspection1617
from 
	DataWarehouse.dbo.D_ACC_Inspections ins
	left outer join
	DataWarehouse.[dbo].[D_ACC_InspectionRelationships] rel
	on ins.inspectionID=rel.[PARENT_InspectionID]
	left outer join
	DataWarehouse.[dbo].[D_ACC_InspectionRelationships] rel2
	on ins.inspectionID=rel2.[CHILD_InspectionID]
where 
	ins.Subtype = 'Water Use Data' 
	and
	ins.NextInspectionDate >='2018-07-13 00:00:00.000' --had a look at data and chose start and stop date based on that.
	and
	ins.NextInspectionDate <='2019-11-01 00:00:00.000' 
	and
	rel2.[PARENT_InspectionID] is null --resolved duplicates (23 consents)

-----------------------------------------------------------------------------------
---daily alert raised over 17/18
select
	distinct B1_ALT_ID as recordNo
	--,InspectionID
	--,Subtype
	--,NextInspectionDate
	--,GA_FNAME + ' ' + GA_LNAME as RMO
	--,[InspectionStatus]
into #dailyalert
from  
	DataWarehouse.dbo.D_ACC_Inspections
where 
	Subtype = 'Water Use - Alert'
	and 
	NextInspectionDate >='2018-07-13 00:00:00.000' 
	and
	NextInspectionDate <='2019-06-30 00:00:00.000'

--Combine all
Select 
	B.*
	,SCR.*
	,DCS.SiteCount
	,DCS.DayCountSum
	,(case when (ins.RecordNo is not null) then 'Monitored' else NULL end) as Monitored1617
	,ins.[InspectionStatus] as InspectionStatus1617
	,(case when (da.RecordNo is not null) then 'Alert Raised' else NULL end) as DailyAlert
	,LFNC+AVNC+CDNC+RoTNC+MRNC as Sumtotal
	,DCS.MaxDate
into #allstuff
from
	#Baseline B
	left outer join #SummaryComplianceRisk SCR
	on B.Recordno=SCR.Consent
	left outer join #DayCountSum DCS
	on B.Recordno=DCS.RecordNo
	left outer join #Inspection1617 ins
	on B.RecordNo=ins.RecordNo
	left outer join #dailyalert da
	on B.RecordNo=da.RecordNo

Select *
From #allstuff

--select * --SPECIFY
--from 
--	#allstuff
--where Recordno='CRC000946.1'


----testing of compplianceSummary--missing rows-------------------------------------------
--select * into #ComplianceSummaryArchive
--FROM [Hilltop].[dbo].[ComplianceSummary_Archive]
--where [Archived]='2018-08-09'

--select 
--	tac.recordNo as BaselineConsent,
--	B1_APPL_Status as BaselineStatus, 
--	cs.Consent as SummaryTableConsent,
--	cs.Errormsg as SummaryTableErrorMsg,
--	csa.Consent as ArchiveSummaryTableConsent,
--	csa.Errormsg as ArchiveSummaryTableErrorMsg,
--	DayCountSum,
--	SiteCount
--from 
--#tempAllConsents2 tac
--left outer join Hilltop.dbo.ComplianceSummary cs
--on tac.RecordNo=cs.Consent
--left outer join #ComplianceSummaryArchive CSA
--on tac.RecordNo=csa.Consent
--left outer join #DayCountSum DC
--on tac.RecordNo=DC.RecordNo
--order by tac.RecordNo





----Missing data meh

--select
--	recordNo
--	,fmDate
--	,toDate
--	,MinDataLogDate
--	,MaxDataLogDate
--	,HolderAddressFullName
--	,EnvironmentalRisk
--	,WAPCountACC
--	,CountWellStatusActive
--	,WaiverCount
--	,CountDataloggers
--	,CountSharedMeter
--	,ErrorMessage
--	,TotalMissingRecord
--	,SiteCount
--	,DayCountSum
--	,DayCountSum/SiteCount as AverageHTDays
--	,ROUND((DayCountSum/SiteCount),0,-2) as RoundedAverage
--	,year(MaxDate) as YearofMonth
--	,month (MaxDate) as MonthOfYear
--from 
--	#allstuff
--where ErrorMessage is null and recordNo in ('CRC000596.2',
--'CRC001382',
--'CRC011950',
--'CRC011952',
--'CRC012702',
--'CRC030636.1',
--'CRC032255',
--'CRC040617.1',
--'CRC042159.2',
--'CRC042462',
--'CRC042709.3',
--'CRC050332.2',
--'CRC051182',
--'CRC054077',
--'CRC060049.2',
--'CRC062825.2',
--'CRC071625',
--'CRC082977.1',
--'CRC090977',
--'CRC092661',
--'CRC100157',
--'CRC101186',
--'CRC130286',
--'CRC136479',
--'CRC136961',
--'CRC141836',
--'CRC145750',
--'CRC153072',
--'CRC158088',
--'CRC161070',
--'CRC161071',
--'CRC181211',
--'CRC182404',
--'CRC182799',
--'CRC185695',
--'CRC175439',
--'CRC175731',
--'CRC180564',
--'CRC180567',
--'CRC185697',
--'CRC960085',
--'CRC962188',
--'CRC970137.3',
--'CRC981199',
--'CRC981425')