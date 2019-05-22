import boto3  
session = boto3.Session(profile_name='saml')
client = session.client('glue',region_name='us-east-1')                    
responseGetDatabases = client.get_databases()                            
databaseList = responseGetDatabases['DatabaseList']                      
for databaseDict in databaseList:                                        
    databaseName = databaseDict['Name']                                  
    responseGetTables = client.get_tables( DatabaseName = databaseName ) 
    tableList = responseGetTables['TableList']                           
    for tableDict in tableList:                                          
         tableName = tableDict['Name']
         for col in tableDict['StorageDescriptor']['Columns'] :
             print databaseName + ',' + tableName + ',' + col['Name'] + ',' + col['Type']                       
