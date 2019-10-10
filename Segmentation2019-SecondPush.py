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
InspectionFile = 'InspectionList2019-10-07.csv'
SegmentationFile = 'Segmentation2019-10-07.csv'
SegmentationNoteFile = 'SegmentationNote2019-10-07.csv'
InspectionAllocationFile = 'FirstSegmentationInspections-Edited.csv'
OWLConsentsFile = 'OWLConsents.csv'
AssociatedInspectionsFile = 'AssociatedInspections.csv'
OfficersFile = 'Officers.csv'


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
SecondRunCountF3 = 0 
SecondRunCountF4 = 310 
SecondRunCountF5 = 320 


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

AssociatedInspections = pd.read_csv(os.path.join(input_path, AssociatedInspectionsFile))

Officers = pd.read_csv(os.path.join(input_path, OfficersFile))


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
         'ScheduledDate',
         'RMO',
         'InspectionStatus'
            ], axis=1)


###############################################################################
### Create WUG inspections
###############################################################################

### List WUG inspections
WUGInspections = SecondInspections[(SecondInspections['WUGNo'].notnull())]

WUGInspections = WUGInspections[['ConsentNo']]

WUGInspections['RMO'] = WUGRMO
WUGInspections['Fortnight'] = WUGFortnight
WUGInspections['ScheduledDate'] = FortnightDate4
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
OWLInspections['ScheduledDate'] = FortnightDate4
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

##############################################################################
### Calculated Terminated Associations
##############################################################################

AssociatedConsentsTerm = AssociatedConsents.groupby(
  ['GroupNo'], as_index=False
  ).agg(
          {
          'ConsentStatus' : 'max'
          })
    
AssociatedConsentsTerm['TerminatedNote'] = np.where(AssociatedConsentsTerm['ConsentStatus'] == 'Terminated - Replaced',
                                        'Some assoicated consents were terminated after October 2018',
                                            ' ')

### Join Terminated Associations to main set
AssociatedConsentsTerm = AssociatedConsentsTerm.drop(
        ['ConsentStatus'], axis=1)
AssociatedConsents = AssociatedConsents.drop(
        ['Unnamed: 0'], axis=1)
AssociatedConsents = pd.merge(AssociatedConsents, AssociatedConsentsTerm, on = 'GroupNo', how = 'left')

###Add Note on Associations
AssociatedConsents['AssociationNote'] = np.where(AssociatedConsents['GroupSize'] > 1,
                                        'There are ' +
                                        AssociatedConsents['GroupSize'].astype(str) +
                                        'consents associated by WAP to this consent'+
                                        AssociatedConsents['TerminatedNote'],
                                        ' '
                                        )
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
print('Associated Inspections not to be allocated : ', NextAAInspections.ConsentNo.count() - NextAAInspections.RMO.count())


NextAAInspections.to_csv(
        r'D:\\Implementation Support\\Python Scripts\\scripts\\Import\\'+
        'Problems' + RunDate + '.csv')

##############################################################################
### Import Assoicated consent allocations
##############################################################################

AssociatedInspections = pd.read_csv(os.path.join(input_path, AssociatedInspectionsFile))


print('Associated Inspections: ', AssociatedInspections.ConsentNo.count())

##############################################################################
### Create Fornight 4 inspection list
##############################################################################
ASInspections = AssociatedInspections.copy()
ASInspections = ASInspections[['ConsentNo','RMO']]

F4Inspections = ASInspections.append([WInspections, OInspections], ignore_index = True)

F4Inspections['ScheduledDate'] = FortnightDate4
F4Inspections['InspectionStatus'] = 'Pending'


print('F4 Inspections: ', F4Inspections.ConsentNo.count())

##############################################################################
### Create pick list for Fortnight 5 Inspections
##############################################################################

### remove associated consents
UnAssociatedConsents = SecondInspections[SecondInspections['ConsentsOnWAP'] == 1]   
    
### remove inspections already assigned
AllInspections = AInspections.append([F4Inspections], ignore_index = True,sort=True)

F5Consents = pd.merge(UnAssociatedConsents, AllInspections, on = 'ConsentNo', how = 'left')
F5Consents = F5Consents[F5Consents['RMO'].isnull()]
F5Consents = F5Consents.drop(
        [
         'ScheduledDate',
         'RMO',
         'InspectionStatus'
            ], axis=1)
    
### Remove Terminated consents
F5Consents = F5Consents[F5Consents['ConsentStatus'] != 'Terminated - Replaced']



##############################################################################
### Allocate Fornight 5 Inspections
##############################################################################
    
F5Inspections = F5Consents.sample(n=SecondRunCountF5, weights = 'TotalRisk', random_state = 1)
F5Inspections['Fortnight'] = 5
F5Inspections.loc[F5Inspections['Fortnight'] == 5, 'RMO'] = random.choices(Officers['AccelaUserID'],Officers['F5Weight'], k= SecondRunCountF5)
F5Inspections['ScheduledDate'] = FortnightDate5
F5Inspections['InspectionStatus'] = 'Pending'

F5Inspections = F5Inspections[['ConsentNo','RMO','InspectionStatus','ScheduledDate']]
#FirstInspections = pd.merge(FirstInspections, F1InspectionsList, on = 'ConsentNo', how = 'left')
#FirstInspections = FirstInspections[(FirstInspections['Fortnight'] != 1)]
#FirstInspections = FirstInspections.drop(['Fortnight'], axis=1)

#### Choose Inspections for Fornight 2
#F2Inspections = FirstInspections.sample(n=Fortnight2, weights = 'TotalRisk', random_state = 1)
#F2Inspections['Fortnight'] = 2
#F2InspectionsList = F2Inspections[['ConsentNo','Fortnight']]

#
#InspectionAllocations = pd.concat([
#                                F1InspectionsList,
#                                F2InspectionsList
#                                ])
print('F5 Inspections: ', F5Inspections.ConsentNo.count())


SecondInspectionAllocations = pd.concat([
                                F4Inspections,
                                F5Inspections
                                ])


SecondInspections = pd.merge(SecondInspections, SecondInspectionAllocations, on = 'ConsentNo', how = 'left')

SecondInspections = SecondInspections[SecondInspections['RMO'].notnull()]



print('Second Push Inspections: ', SecondInspections.ConsentNo.count())

SecondInspections = pd.merge(SecondInspections, df, on = 'ConsentNo', how = 'left')

SecondInspections.to_csv('SecondInspections-pending.csv')



#fomat for elvin.
###########################################

