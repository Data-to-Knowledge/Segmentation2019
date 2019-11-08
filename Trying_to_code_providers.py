### Import Provider Info - Latest provider available on hydro based on end_date - The column with provider information is Hts_file

import pdsql


##############################################
### Parameters

Provider_date_col = 'end_date'
Provider_from_date = '2019-07-01'
ProviderServer = 'EDWProd01'
ProviderDatabase = 'Hydro'
ProviderTable = 'HilltopUsageSiteDataLog'

ProviderCol = [
        'end_date'
		'wap',
		'hts_file'
        ]
ProviderColNames = {
        'wap' : 'WAP'
        }
ProviderImportFilter = {
        'wap' : WAPMaster
        }

###########################################
### Run queries

Provider = pdsql.mssql.rd_sql(
                   server = ProviderServer,
                   database = ProviderDatabase,
                   table = ProviderTable,
                   col_names = ProviderCol,
                   where_in = ProviderImportFilter,
                   date_col = Provider_date_col,
                   from_date= Provider_from_date
                   )
# Format data
Provider.rename(columns=ProviderColNames, inplace=True)
Provider['WAP'] = Provider['WAP'] .str.strip().str.upper()

# Filter to latest Provider
Provider = Provider.groupby(
        ['WAP'], as_index=False
        ).agg({'end_date' : 'max'})
Provider.rename(columns =
         {'end_date' :'LatestProvider',
         }, inplace=True)



# Print overview of table
print('\nProvider Table ',
      Provider.shape,'\n',
      Provider.nunique(), '\n\n')