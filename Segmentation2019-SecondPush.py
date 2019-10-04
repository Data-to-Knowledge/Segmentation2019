# -*- coding: utf-8 -*-
"""
Created on Thu Apr 18 14:52:35 2019

@author: KatieSi
"""
##############################################################################
### Import Packages
##############################################################################

import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
import random
import pdsql
import networkx as nx


##############################################################################
### Set Variables
##############################################################################

# Base Variables
ReportName= 'Water Inspection Prioritzation Model - Inspection Allocation 1'
RunDate = str(date.today())

### Import Files
InspectionFile = 'InspectionList2019-09-23.csv'
SegmentationFile = 'Segmentation2019-09-23.csv'
SegmentationNoteFile = 'SegmentationNote2019-09-23.csv'
InspectionAllocationFile = 'FirstSegmentationInspections-Edited.csv'
OWLConsentsFile = 'OWLConsents.csv'


### File Location
input_path = r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\"
output_path = r"D:\\Implementation Support\\Python Scripts\\scripts\\Export\\"

### WUG Variables
WUGRMO = 'JANAH'
WUGFortnight = 3

### OWL Variables
OWLRMO = 'HANNAHDU'
OWLFortnight = 3


### Allocation totals per fortnight
FirstRunCountF1 = 625
FirstRunCountF2 = 616
SecondRunCountF3 = 325 #TBC
SecondRunCountF4 = 315 #TBC
SecondRunCountF5 = 325 #TBC


FortnightDate1 = '2019-09-09'
FortnightDate2 = '2019-09-23'
FortnightDate3 = '2019-10-07'
FortnightDate4 = '2019-10-21'
FortnightDate5 = '2019-11-04'

#
#falsely created ( WUG )
#CRC143124	1524406	NICKV
#CRC151689	1530047	RUBYD
#
#Falsely removed ( Adman )
#CRC168335	Adapt.Vol
#CRC173343	Adapt.Vol

#falsely created ( OWL )
#CRC000265.2	1520130	LEE-ANNM
#CRC162722	1519806	LEE-ANNM

##############################################################################
### Import data
##############################################################################

#Import data

InspectionList = pd.read_csv(os.path.join(input_path, InspectionFile))

Segmentation = pd.read_csv(os.path.join(input_path, SegmentationFile))

SegmentationNote = pd.read_csv(os.path.join(input_path, SegmentationNoteFile))

InspectionAllocation = pd.read_csv(os.path.join(input_path, InspectionAllocationFile))

OWLConsents = pd.read_csv(os.path.join(input_path, OWLConsentsFile))


##############################################################################
### Find current RMO on inspections
##############################################################################

Allo_Insp_List = InspectionAllocation['InspectionID'].values.tolist()

Insp_col = [          
            'InspectionID',
            'InspectionStatus',
            'GA_USERID'
            ]
Insp_ColNames = {
        'GA_USERID' : 'RMO'
        }
temp = pdsql.mssql.rd_sql(
                    server = 'SQL2012PROD03',
                    database = 'DataWarehouse',
                    table = 'D_ACC_Inspections',
                    col_names = Insp_col,
                    where_in = {'InspectionID': Allo_Insp_List})
temp.rename(columns = Insp_ColNames, inplace=True)

temp['RMO'] = temp['RMO'].fillna('Missing Officer')
temp['InspectionStatus'] = temp['InspectionStatus'].fillna('Missing Inspection')

InspectionAllocation = pd.merge(InspectionAllocation, temp, on = 'InspectionID', how = 'left')

### Print number of inspections from Push 1
print('First Push Inspections: ', InspectionAllocation.InspectionID.count())


##############################################################################
### Select Second Run of Inspections
##############################################################################

### Remove unecessary inspections
SecondInspections = InspectionList[InspectionList['InspectionNecessity'] != 0]

### remove all inspections already assigned
SecondInspections = pd.merge(SecondInspections, InspectionAllocation, on = 'ConsentNo', how = 'left')
SecondInspections = SecondInspections[SecondInspections['InspectionID'].isnull()]
SecondInspections = SecondInspections.drop(
        ['Fortnight', 
         'ScheduledDate'
            ], axis=1)


###############################################################################
### Create WUG inspections
###############################################################################

### List WUG inspections
WUGInspections = SecondInspections[(SecondInspections['WUGNo'].notnull())]

WUGInspections = WUGInspections[['ConsentNo']]

WUGInspections['RMO'] = WUGRMO
WUGInspections['Fortnight'] = WUGFortnight
WUGInspections['ScheduledDate'] = FortnightDate3
WUGInspections['InspectionStatus'] = 'Pending'

### Print number of WUG inspections
print('WUG Inspections: ', WUGInspections.ConsentNo.count())


###############################################################################
### Remove Opua Members inspections
###############################################################################

### List WUG inspections


OWLInspections = pd.merge(SecondInspections, OWLConsents, on = 'ConsentNo', how = 'left')
OWLInspections = OWLInspections[(OWLInspections['OWLConsent'].notnull())]

OWLInspections = OWLInspections[['ConsentNo']]

OWLInspections['RMO'] = OWLRMO
OWLInspections['Fortnight'] = OWLFortnight
OWLInspections['ScheduledDate'] = FortnightDate3
OWLInspections['InspectionStatus'] = 'Pending'

### Print number of WUG inspections
print('OWL Inspections: ', OWLInspections.ConsentNo.count())

##############################################################################
### Create assigned inspections lists
##############################################################################

WInspections = WUGInspections.copy()
WInspections = WInspections[['ConsentNo','RMO','InspectionStatus']]

OInspections = OWLInspections.copy()
OInspections = OInspections[['ConsentNo','RMO','InspectionStatus']]

AInspections = InspectionAllocation.copy()
AInspections = AInspections[['ConsentNo','RMO','InspectionStatus']]

LinkedInspections = AInspections.append([WInspections, OInspections], ignore_index = True)


##############################################################################
### Find associations between consents
##############################################################################

### Create assocations list for WAP
temp = Segmentation[['ConsentNo','WAP']]
temp = temp[temp['WAP'].notnull()]
AW1 = temp.copy()
AW2 = temp.copy()
AW2.columns= ['AssociatedConsent', 'WAP']
df = pd.merge(AW1, AW2, on = 'WAP', how = 'left')
df= df[['ConsentNo','AssociatedConsent']]
df = df.drop_duplicates()

### Create assocations list for EC holder
temp = Segmentation[['ConsentNo','HolderAddressFullName']]
temp = temp[temp['HolderAddressFullName'].notnull()]
AE1 = temp.copy()
AE2 = temp.copy()
AE2.columns= ['AssociatedConsent', 'HolderAddressFullName']
df2 = pd.merge(AE1, AE2, on = 'HolderAddressFullName', how = 'left')
df2= df2[['ConsentNo','AssociatedConsent']]
df2 = df2.drop_duplicates()

### Calculate groups of assoicated consents
df.columns= ['ConsentNo', 'AssociatedConsent']
G = nx.from_pandas_edgelist(df, 'ConsentNo','AssociatedConsent')
subgraphs = list(nx.connected_components(G))
grouplist = list(range(len(subgraphs)))

def defineGrouping(x):
    return grouplist[[n for n,i in enumerate(subgraphs) if x in i][0]]

df['GroupNo'] = df.AssociatedConsent.map(defineGrouping)

df= df[['ConsentNo','GroupNo']]
df = df.drop_duplicates()

dfcount = df.groupby(
  ['GroupNo'], as_index=False
  ).agg(
          {
          'ConsentNo' : 'count'
          })
    
dfcount.columns= ['GroupNo', 'GroupSize']
dfcount = dfcount.drop_duplicates()  

### join lists
df = pd.merge(df, dfcount, on = 'GroupNo', how = 'left')
  
### Print stats for spot check
print('Number of Groups: ', df.GroupNo.max())
print('Largest Group: ', dfcount.GroupSize.max())


### Join associations to supporting information
AssociatedConsents = pd.merge(df, InspectionList, on = 'ConsentNo', how = 'left')

### Mark allocated inspections
AssociatedConsents = pd.merge(AssociatedConsents, LinkedInspections, on = 'ConsentNo', how = 'left')

### Output table
AssociatedConsents.to_csv(
        r'D:\\Implementation Support\\Python Scripts\\scripts\\Import\\'+
        'AssociatedConsents' + RunDate + '.csv')


##############################################################################
### Find associated Consents already allocated
##############################################################################

AAInspections = AssociatedConsents.copy()

AAInspections = AAInspections[AAInspections['RMO'].notnull()]

AAInspectionsList = AAInspections['GroupNo'].values.tolist()

NextAAInspections = AssociatedConsents[AssociatedConsents['GroupNo'].isin(AAInspectionsList)]

NextAAInspections = NextAAInspections[NextAAInspections['GroupSize'] > 1]



Problems = AAInspections.groupby(
  ['GroupNo'], as_index=False
  ).agg(
          {
          'RMO' : pd.Series.nunique
          })

Problems.rename(columns = {'RMO':'RMOCount'}, inplace=True)
Problems = Problems[Problems['RMOCount'] > 1 ]

NextAAInspections = pd.merge(NextAAInspections, Problems, on = 'GroupNo', how = 'left')

print('Associated Inspections already allocated : ', NextAAInspections.RMO.count())
print('Associated Inspections to be allocated : ', NextAAInspections.ConsentNo.count() - NextAAInspections.RMO.count())




##########################################
Fortnight3 = SecondRunCountF3 / 4
#
#### Choose Inspections for Fornight 1
#F3Inspections = FirstInspections.sample(n=Fortnight3, weights = 'TotalRisk', random_state = 1)
#F3Inspections['Fortnight'] = 1
#
#F3InspectionsList = F1Inspections[['ConsentNo','Fortnight']]
#
#FirstInspections = pd.merge(FirstInspections, F1InspectionsList, on = 'ConsentNo', how = 'left')
#FirstInspections = FirstInspections[(FirstInspections['Fortnight'] != 1)]
#FirstInspections = FirstInspections.drop(['Fortnight'], axis=1)
#
#### Choose Inspections for Fornight 1
#F2Inspections = FirstInspections.sample(n=Fortnight2, weights = 'TotalRisk', random_state = 1)
#F2Inspections['Fortnight'] = 2
#F2InspectionsList = F2Inspections[['ConsentNo','Fortnight']]
#
#
#
#
#InspectionAllocations = pd.concat([
#                                F1InspectionsList,
#                                F2InspectionsList
#                                ])
#
#InspectionAllocations = pd.merge(InspectionAllocations, InspectionList, on = 'ConsentNo', how = 'left')
#
#
#Officers = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\Officers.csv")
#
#
#
#InspectionAllocations.loc[InspectionAllocations['Fortnight'] == 1, 'RMO'] = random.choices(Officers['RMO'],Officers['F1Weight'], k= Fortnight1)
#InspectionAllocations.loc[InspectionAllocations['Fortnight'] == 2, 'RMO'] = random.choices(Officers['RMO'],Officers['F2Weight'], k= FirstRunCountF2)
#
#FirstPush = InspectionAllocations[InspectionAllocations['Fortnight'].notnull()]
#
#FirstPush.to_csv('FirstPush.csv')
#
#MidseasonInspections = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\MidseasonInspections.csv")
#
#MidseasonInspectionList = Segmentation[Segmentation['MidSeasonRisk'] == 'Warning']
#
#MidseasonInspectionList = MidseasonInspectionList.groupby(
#        ['ConsentNo'], as_index=False
#        ).agg({
#                'HolderAddressFullName': 'max',
#                'CWMSZone' :'max',
#                'TotalRisk': 'max',
#                'InspectionAssignment' : 'max',
#                'InspectionNecessity' : 'min',
#                'AdMan' : 'max',
#                'ComplexityLevel' : 'max'
#                })
#
#MidseasonInspectionList['RMO'] = random.choices(Officers['RMO'],Officers['F1Weight'], k=len(MidseasonInspectionList))
#
#MidseasonInspectionList = pd.merge(MidseasonInspectionList, MidseasonInspections, on = 'ConsentNo', how = 'left')
#
#MidseasonInspectionList.to_csv('MidseasonInspectionList.csv')
#
###############################################################################
#### First Allocation
###############################################################################
#
#
#InspectionAllocations = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\FirstPush-Edit.csv")
#
#Officers = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\Officers.csv")
#
#OfficerLink = Officers[['RMO','AccelaUserID']]
#
#
#InspectionAllocations = pd.merge(InspectionAllocations, OfficerLink, on = 'RMO', how = 'left')
#
#InspectionAllocations = pd.merge(InspectionAllocations, SegmentationNote, on = 'ConsentNo', how = 'left')
#
#InspectionAllocations = pd.merge(InspectionAllocations, InspectionList, on = 'ConsentNo', how = 'left')
#
#conditions = [
#	(InspectionAllocations['Fortnight'] == 1),
#	(InspectionAllocations['Fortnight'] == 2)
#			 ]
#choices = [FortnightDate1, FortnightDate2]
#InspectionAllocations['FortnightDate'] = np.select(conditions, choices, default = np.nan)
#
#InspectionAllocations['Consent'] = InspectionAllocations['ConsentNo']
#InspectionAllocations['InspectionType'] = 'Desk Top Inspection'
#InspectionAllocations['InspectionSubType'] = 'Water Use Data'
#InspectionAllocations['ScheduledDate']  = InspectionAllocations['FortnightDate']
#InspectionAllocations['InspectionDate']  = ' '
#InspectionAllocations['InspectionStatus'] = 'Scheduled'
#InspectionAllocations['AssignedTo'] = InspectionAllocations['AccelaUserID']
#InspectionAllocations['Department'] = ' '
#InspectionAllocations['GeneralComments'] = InspectionAllocations['InspectionNote']
#
#ScheduleInspections = InspectionAllocations[[
#                                              'Consent',
#                                              'InspectionType',
#                                              'InspectionSubType',
#                                              'ScheduledDate',
#                                              'InspectionDate',
#                                              'InspectionStatus',
#                                              'AssignedTo',
#                                              'Department',
#                                              'GeneralComments',
#                                              'ConsentsOnWAP' 
#                                             ]]
#ScheduleInspections.to_csv('WaterSegmentationInspections' + RunDate +'.csv')
#
###############################################################################
#### Second Push
##############################################################################
#MidseasonInspectionsList = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\MidseasonInspectionList.csv")
#
#FirstPush = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\FirstPush.csv")
#
#FirstPushList = FirstPush[['ConsentNo','Fortnight']]
#
#
#
#SecondInspections = InspectionList
#
#SecondInspections = pd.merge(SecondInspections, FirstPushList, on = 'ConsentNo', how = 'left')
#SecondInspections = SecondInspections[SecondInspections['Fortnight'].isnull()]
#
#SecondInspections = SecondInspections.drop(['Fortnight'], axis=1)
#
#NecessaryInspections = SecondInspections[(SecondInspections['InspectionNecessity'] == 2)]
#SampledInspections = SecondInspections[(SecondInspections['InspectionNecessity'] != 2)]
#
#
#
#NecessaryInspections['Fortnight'] = np.random.randint(low=3, high=6, size=len(NecessaryInspections))
#
#NecessaryInspectionsCount =  NecessaryInspections.groupby(
#                                            ['Fortnight'], as_index = False
#                                            ).agg({'ConsentNo' : 'count'})
#
#
#NecessaryInspectionsList = NecessaryInspections[['ConsentNo','Fortnight']]
#
#NecessaryGroup3 = NecessaryInspectionsCount[NecessaryInspectionsCount['Fortnight'] == 3].iloc[0,1]
#NecessaryGroup4 = NecessaryInspectionsCount[NecessaryInspectionsCount['Fortnight'] == 4].iloc[0,1]
#NecessaryGroup5 = NecessaryInspectionsCount[NecessaryInspectionsCount['Fortnight'] == 5].iloc[0,1]
#
#WUGInspections = Segmentation.groupby(
#        ['WUGNo'], as_index=False
#        ).agg({
#                'WUGName': 'max',
#                'CWMSZone' :'max',
#                'TotalRisk': 'max',               
#                'InspectionAssignment' : 'max',
#                'InspectionNecessity' : 'min',
#                'ConsentsOnWAP' : 'max',
#                'ComplexityLevel' : 'max'
#                })
# 
#WUGInspections['Fortnight'] = np.random.randint(low=3, high=6, size=len(WUGList))
#
#
#
#WUGInspectionList = WUGInspections[['WUGNo','Fortnight']]
#
#WUGInspectionCount =  WUGInspections.groupby(
#                                        ['Fortnight'], as_index = False
#                                        ).agg({'WUGNo' : 'count'})
#
#WUGGroup3 = WUGInspectionCount[WUGInspectionCount['Fortnight'] == 3].iloc[0,1]
#WUGGroup4 = WUGInspectionCount[WUGInspectionCount['Fortnight'] == 4].iloc[0,1]
#WUGGroup5 = WUGInspectionCount[WUGInspectionCount['Fortnight'] == 5].iloc[0,1]
#
#
#Fortnight3 = SecondRunCountF3-NecessaryGroup3 - WUGGroup3
#Fortnight4 = SecondRunCountF4-NecessaryGroup4 - WUGGroup4
#Fortnight5 = SecondRunCountF5-NecessaryGroup5 - WUGGroup5
#
#
#
#F3Inspections = SampledInspections.sample(n=Fortnight3, weights = 'TotalRisk', random_state = 1)
#F3Inspections['Fortnight'] = 3
#F3InspectionsList = F3Inspections[['ConsentNo','Fortnight']]
#
#SampledInspections = pd.merge(SampledInspections, F3InspectionsList, on = 'ConsentNo', how = 'left')
#SampledInspections = SampledInspections[(SampledInspections['Fortnight'] != 3)]
#SampledInspections = SampledInspections.drop(['Fortnight'], axis=1)
#
#
#F4Inspections = SampledInspections.sample(n=Fortnight4, weights = 'TotalRisk', random_state = 1)
#F4Inspections['Fortnight'] = 4
#F4InspectionsList = F4Inspections[['ConsentNo','Fortnight']]
#
#SampledInspections = pd.merge(SampledInspections, F4InspectionsList, on = 'ConsentNo', how = 'left')
#SampledInspections = SampledInspections[(SampledInspections['Fortnight'] != 4)]
#SampledInspections = SampledInspections.drop(['Fortnight'], axis=1)
#
#
#F5Inspections = SampledInspections.sample(n=Fortnight5, weights = 'TotalRisk', random_state = 1)
#F5Inspections['Fortnight'] = 5
#F5InspectionsList = F4Inspections[['ConsentNo','Fortnight']]
#
#SampledInspections = pd.merge(SampledInspections, F5InspectionsList, on = 'ConsentNo', how = 'left')
#SampledInspections = SampledInspections[(SampledInspections['Fortnight'] != 5)]
#
#
#SampledInspections['Fortnight'] = 0
#
#NoInspectionsList = SampledInspections[['ConsentNo','Fortnight']]
#
#
#
#
#
#InspectionAllocations = pd.concat([
#                                NecessaryInspectionsList,
#                                F1InspectionsList,
#                                F2InspectionsList, 
#                                F3InspectionsList,
#                                F4InspectionsList,
#                                F5InspectionsList,
#                                NoInspectionsList
##                                WUGInspectionList
#                                ])
#
#
#
#InspectionAllocations = pd.merge(InspectionAllocations, InspectionList, on = 'ConsentNo', how = 'left')
#
#
#Officers = pd.read_csv(r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\Officers.csv")
#
#
#InspectionAllocations.loc[InspectionAllocations['Fortnight'] == 3, 'RMO'] = random.choices(Officers['RMO'],Officers['F1Weight'], k= (SecondRunCount-WUGGroup3))
#InspectionAllocations.loc[InspectionAllocations['Fortnight'] == 2, 'RMO'] = random.choices(Officers['RMO'],Officers['F2Weight'], k= FirstRunCount)
#
#FirstPush = InspectionAllocations[InspectionAllocations['Fortnight'].notnull()]
#
#FirstPush.to_csv('FirstPush.csv')
#
#
#Fortnight3 = SecondRunCount-NecessaryGroup3 - WUGGroup3
#Fortnight4 = SecondRunCount-NecessaryGroup4 - WUGGroup4
#Fortnight5 = SecondRunCount-NecessaryGroup5 - WUGGroup5
#
#
#tempList = InspectionAllocations
#
#tempList['RMO'] = random.choices(Officers['RMO'],Officers['F1Weight'], k=len(tempList))
#
#import random
#tempList.loc[tempList['Fortnight'] == 1, 'RMO'] = random.choices(Officers['RMO'],Officers['F1Weight'], k= Fortnight1)
#tempList.loc[tempList['Fortnight'] == 2, 'RMO'] = random.choices(Officers['RMO'],Officers['F2Weight'], k= FirstRunCount)
#
#FortnightTotals = tempList.groupby(['Fortnight'], as_index = False
#                 ).agg({'ConsentNo': 'count'})
#
#
#Officers
#Key, Officer, Team, Weight
#
#
#
#Officers = pd.DataFrame([
#        ['Mr. AAA','Mr. BB','Mr. X'], 
#        [1,1,0.5]]).T
#
#Officers.columns = ['RMO','Weights']
#tempList = F4InspectionsList
#tempList['RMO'] = random.choices(Officers['RMO'],Officers['Weights'], k=len(tempList))
#     
#
#
#
#
#Officers.sum(axis = 0, skipna = True)
#
#
#tempList = F4InspectionsList
#
#
#tempList.groupby(['RMO'], as_index = False
#                 ).agg({'ConsentNo': 'count'})
#
#
#RMO =  ['Mr. Smith','Mr. Jones','Mr. X']
#Weights =   [0.5,0.2,0.3]   
#        
#
#random.choices(RMO,Weights, k=30)
##
#Officers[1]
#
##SecondRun = SecondInspections.sample(n=FirstRunCount, weights = 'TotalRisk', random_state = 1)
#
#
##df.sample(n=2, weights='num_specimen_seen', random_state=1)
#
#
#import random
#random.choices(['one', 'two', 'three'], [0.2, 0.3, 0.5], k=10)
#
#
#
#
#
#
#FirstInspections
#Accela user ID
#Note
#Other columns
#
#extra single inspections
