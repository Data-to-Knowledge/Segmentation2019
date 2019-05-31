Select   Well_No
,ID
		 ,WaterMeterID
		,SerialNumber as meterSerialNumber
		,WaterMeterSize
		,M3_Pulse 
		,waterMeterCategory
		 ,CASE WHEN WaterMeterID = 1 THEN 'Not Installed'
				WHEN WaterMeterID not in (9,10,11,44) and SerialNumber is not null and DateInstalled is not null
					THEN 'Complete' 
		  ELSE 'Not Complete' END as wmInstall
	     ,CASE WHEN WaterMeterSize is not null and M3_Pulse is not null THEN 'Complete' else 'Not Complete' END as wmDetails
		 ,DateInstalled
		 ,DateDeInstalled
		 ,waterMeterType
		 ,dateMeterVerified
		 ,Verifier
		 ,WM_PipeDiameter
From
(
Select	WM.Well_no
	   ,WM.ID
	   ,WM.WaterMeterID
	   ,WM.SerialNumber
	   ,WM.DateInstalled
	   ,WM.DateDeInstalled
	   ,WMT.ShortType as waterMeterCategory
	   ,WMT.WaterMeter_Type as waterMeterType
	   ,vH.DateVerified as dateMeterVerified
		,vH.Verifier
		,WM.WaterMeterSize
		,WM.M3_Pulse
		,WM.[WM_PipeDiameter]
From [dbo].[EPO_WaterMeters] WM
inner join [dbo].[WaterMeter_Types] WMT
on wM.WaterMeterID = WMT.ID
left join [dbo].[EPO_WaterMeterVerificationHistory] vH
on WM.ID = vH.WMLH_Index
and WM.Well_no = VH.Well_no
and DateVerified = (Select max(DateVerified)
					from [dbo].[EPO_WaterMeterVerificationHistory] 
					where WMLH_index = WM.ID
					and Well_no = WM.Well_no
					and [VerificationFormChecked] = 1
					) 
and vH.[VerificationFormChecked] = 1
Group by WM.Well_no
		,WM.ID
		,WM.WaterMeterID
		,WM.SerialNumber
		,WM.DateInstalled
		,WMT.ShortType
		,vH.DateVerified
		,vH.Verifier
		,WM.WaterMeterSize
		,WM.M3_Pulse
		,WM.WM_PipeDiameter
		,WM.DateDeInstalled
		,WMT.WaterMeter_Type
) as allData	
where DateDeInstalled is null