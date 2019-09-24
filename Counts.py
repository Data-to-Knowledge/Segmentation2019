# -*- coding: utf-8 -*-
"""
Created on Wed Sep 25 09:33:16 2019

@author: KatieSi
"""

##############################################################################
### Import Packages
##############################################################################

import os
import pandas as pd


##############################################################################
### Set Variables
##############################################################################

# Base Variable
ReportName= 'Water Inspection Prioritzation Model - Risk Calculation'
RunDate = str(date.today())

### Baseline File
BaselineFile = 'Baseline2019-09-23.csv'
input_path = r"D:\\Implementation Support\\Python Scripts\\scripts\\Import\\"


##############################################################################
### Import data
##############################################################################

B = pd.read_csv(os.path.join(input_path, BaselineFile))


##############################################################################
### Calculate Stats
##############################################################################
 
B.ConsentNo.nunique()

B[B.WUGNo.notnull()].ConsentNo.nunique()
B.WUGNo.nunique()

B[B.AdMan.notnull()].ConsentNo.nunique()

B.ConsentProject.count()
B[B.ConsentProject == 'TLA project'].ConsentNo.nunique()
B[B.ConsentProject == 'Electricity Providor project'].ConsentNo.nunique()
B[B.ConsentProject == 'Irrigation Scheme project'].ConsentNo.nunique()

B[B.T_AverageMissingDays == 365 ].ConsentNo.nunique()

B[B.WAPProject == 'Pending Data Cleaning Project'].ConsentNo.nunique()
B[B.WAPProject == 'Scottech Data Project'].ConsentNo.nunique()

B[B.WAPRate < 5 ].ConsentNo.nunique()
B[B.WAPRate < 5 ].WAP.nunique()

B[B.Waiver == 'Yes'].ConsentNo.nunique()
B[B.Waiver == 'Yes'].WAP.nunique()

B[B.WellStatus == 'Inactive'].ConsentNo.nunique()
B[B.WellStatus == 'Inactive'].WAP.nunique()

B[B.MetersInstalled.isnull()].ConsentNo.nunique()
B[B.MetersInstalled.isnull()].WAP.nunique()


B[B.WUGNo.notnull() | B.AdMan.notnull()].ConsentNo.nunique()

B1 = B[B.WUGNo.isnull() & 
  B.AdMan.isnull() &
  B.ConsentProject.isnull() &
  B.WAPProject.isnull()
].('ConsentNo','WAP')


B2 = B[B.WAPRate > 5].ConsentNo  
B[B.Waiver != 'Yes'].ConsentNo.nunique()  
B[B.WellStatus != 'Inactive'].ConsentNo.nunique() 
  ].ConsentNo.nunique()  .ConsentNo.nunique() 
