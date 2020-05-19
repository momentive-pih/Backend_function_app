import logging
import json
import azure.functions as func
import pandas as pd
import os 
import pysolr

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postBasicProperties function processing a request.')
        result=[]
        req_body = req.get_json()       
        # result = json.dumps(basic_details)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")
