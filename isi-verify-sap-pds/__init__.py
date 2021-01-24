import logging
import azure.functions as func
import importlib
from pymongo import MongoClient
from azure.storage.blob import BlobClient
from azure.storage.blob import BlobServiceClient
import pandas as pd
import time
import os
from datetime import datetime


class runVerify:
    def run_verify(self):
        logging.info('starttime:'+str(datetime.now()))
        azure_secret_key=os.environ.get('AZURE_SECRET_KEY')
        azure_account_url=os.environ.get('AZURE_ACCOUNT_URL')
        logging.info('azure_account_url: '+str(azure_account_url))
        uri=os.environ.get('MONGO_URI')
        logging.info('ur: '+str(uri))
        connection_string=os.environ.get('AZURE_CONNECTION_STRING')
        logging.info('AZURE_CONNECTION_STRING: '+str(connection_string))
        container_name_file=os.environ.get('AZURE_CONTAINER_NAME')
        logging.info('container_name_file: '+str(container_name_file))
        container_name_error=os.environ.get('AZURE_CONTAINER_NAME_ERROR')
        logging.info('container_name_error: '+str(container_name_error))
        container_name_archive=os.environ.get('AZURE_CONTAINER_NAME_ARCHIVE')
        logging.info('container_name_archive: '+str(container_name_archive))
        client = MongoClient(uri)
        my_db=client.pro_hcm_db
        my_col = my_db['Partner']
        partnernotfound=''
        partnernum=''
        timestr = time.strftime("%Y%m%d")
        blobname='partner_'+ timestr+'.csv'
        error_file_name = 'partner_error_'+ timestr+'.csv'
        blob = BlobClient(account_url=azure_account_url,
                        container_name=container_name_file,
                        blob_name=blobname,
                        credential=azure_secret_key)
        bloberror = BlobClient(account_url=azure_account_url,
                        container_name=container_name_error,
                        blob_name=error_file_name,
                        credential=azure_secret_key)
        
        service = BlobServiceClient.from_connection_string(connection_string)
        try:
            if blob.exists()==True:
                logging.info('SAP partner file exists')
                data = blob.download_blob(encoding="UTF-8")
                logging.info('Reading from blob')
                x=data.readall()
                logging.info('Splitting csv files')
                csv_lines=x.split('\n')
                flag = False                
                df = pd.DataFrame()
                partnerdata=[]
                logging.info('Reading csv file line by line and checking in Mongo DB')
                for line in csv_lines:
                    cols = line.split(',')    
                    if flag == True:
                        # Break the line to get the columns
                        partnernum= cols[0].zfill(8)
                        csv_dict = {'PersNo' : partnernum}
                        # Check if we have documents in MongoDB
                        if my_col.count_documents({'_id':csv_dict['PersNo']},limit=1)!=0:
                            None
                        else:
                            logging.info('Checking if it is last record')
                            if partnernum!='00000000':
                                partnernotfound = partnernotfound + csv_dict['PersNo']+"- not found in PIPS \n"
                                partnerdata.append(partnernum)            
                                df = pd.DataFrame(partnerdata, columns = ['Partner_number'])
                            else:
                                None
                    flag=True
            else:
                logging.info('Blob does not exist')
            if partnernotfound!='': 
                logging.info('Writing to error file')
                if bloberror.exists()==True:
                    #Delete if blob error file already exists
                    bloberror.delete_blob()
                    bloberror.upload_blob(data=df.to_csv(index=False))
                else:
                    bloberror.upload_blob(data=df.to_csv(index=False))
                logging.info('Error file is created')
            else:
                None
            # Copy source file to archive folder
            source_container_name = container_name_file
            source_file_path = blobname
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            source_blob =azure_account_url+"/"+container_name_file+"/"+blobname 
            logging.info('Archiving process starting')
            target_container_name = container_name_archive
            target_file_path = blobname
            copied_blob = blob_service_client.get_blob_client(target_container_name, target_file_path) 
            copied_blob.start_copy_from_url(source_blob)
            logging.info('Archiving is complete')        
            # delete the source file
            logging.info('Removing the file after archiving')
            remove_blob = blob_service_client.get_blob_client(source_container_name, source_file_path)
            remove_blob.delete_blob()
            logging.info('End time: '+str(datetime.now()))
            
        except Exception as e:
            logging.info(e)

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('Python HTTP trigger function processed a request.')
        run = runVerify()
        run.run_verify()
        return func.HttpResponse(f"The HTTP triggered function executed successfully.")
    except Exception as e:
        return func.HttpResponse(f"The HTTP triggered function failed.")