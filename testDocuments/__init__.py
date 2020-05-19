import logging
import azure.functions as func
from __app__.shared_code import settings as config
from __app__.shared_code import helper
solr_unstructure_data=config.solr_unstructure_data
from datetime import datetime
from pytz import timezone
import json
import os 
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postProductCompliance function processing a request.')
        result=[]
        # print(f'AzureBlobStoragePath{os.environ.get("AzureBlobStoragePath")}')
        logging.info(f'{datetime.now()}')
        # logging.info(f'AzureBlobStorageSasToken{os.environ.get("AzureBlobStorageSasToken")}')
        mst = timezone('MST')
        # print("Time in MST:", datetime.now(mst))
 
        est = timezone('EST')
        # print("Time in EST:", datetime.now(est))
        
        utc = timezone('UTC')
        # print("Time in UTC:", datetime.now(utc))
        
        gmt = timezone('GMT')
        # print("Time in GMT:", datetime.now(gmt))
        
        hst = timezone('HST')
        # print("Time in HST:", datetime.now(hst))
        # found_data = get_all_documents()
        result = f'Time in MST: {datetime.now(mst)},Time in EST: {datetime.now(est)},Time in UTC: {datetime.now(utc)},Time in GMT: {datetime.now(gmt)},Time in HST: {datetime.now(hst)},Current time :{datetime.now()}'
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

# def get_all_documents():
    # try:
    #     params={"fl":"DATA_EXTRACT,PRODUCT,CATEGORY,PRODUCT_TYPE"}
    #     query=f'IS_RELEVANT:1'
    #     result_json,result_df=helper.get_data_from_core(solr_unstructure_data,query,params)
        
    #     return result_json
    # except Exception as e:
    #     pass

