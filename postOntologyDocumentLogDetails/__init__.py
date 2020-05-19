import logging
import azure.functions as func
from __app__.shared_code import settings as config
from __app__.shared_code import helper
import json
from datetime import datetime
import pandas as pd
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('Python get ontology document log details function processed a request.')
        result=[]
        req_body = req.get_json()
        found_data = get_log_details(req_body)
        result = json.dumps(found_data)
    except Exception as e:
        pass
    return func.HttpResponse(result,mimetype="application/json")

def get_log_details(req_body):
    try:
        id_key = req_body.get("id","")
        json_list=[]
        #find created details
        solr_query=f"solr_id:{id_key}"
        values,df=helper.get_data_from_core(config.solr_unstructure_data,solr_query)
        if len(values)>0:
            created_date=values[0].get("CREATED","-")
            created_by="PIH-Admin"
            sql_id=values[0].get("ID","-")
            json_list=helper.make_log_details(sql_id,created_by,created_date)
    except Exception as e:
        pass
    return json_list

