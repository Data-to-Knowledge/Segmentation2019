--[?24/?05/?2019 1:31 PM]  Ilja van Nieuwpoort:  
--here's verification scripty bit:
select
	
	distinct Well_No
	,MAX(DateVerified) as MaxDateVerified
--into #Verification1
from sql2012prod05.Wells.[dbo].[EPO_WaterMeterVerificationHistory] 
where [VerificationFormChecked] = 1 --this means that someone at ECan has accepted the verification as valid
group by
	Well_No
order by Well_No 
 
--this gives you the max date by Well_No, not by meter, which does not make sense in multi meter situation in that case you need to join it back onto the meter with --WMLH_Index 
-- on well but not meter
--WMLH_Index



select
	
	distinct WMLH_Index
	,Well_No
	,MAX(DateVerified) as MaxDateVerified
--into #Verification2
from sql2012prod05.Wells.[dbo].[EPO_WaterMeterVerificationHistory] 
where [VerificationFormChecked] = 1 --this means that someone at ECan has accepted the verification as valid
group by
	WMLH_Index, Well_No
order by Well_No

-- join on active water meters.