# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 14:52:35 2019

@author: KatieSi
"""


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
