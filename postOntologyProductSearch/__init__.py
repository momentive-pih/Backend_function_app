import logging
import json
import azure.functions as func
import pandas as pd
import os 
import pysolr
import datetime
from __app__.shared_code import settings as config
from __app__.shared_code import helper


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postOntologySearchProduct function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = helper.namrod_bdt_product_details(req_body)
        result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")