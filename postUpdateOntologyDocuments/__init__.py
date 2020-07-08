import logging
import azure.functions as func
from __app__.shared_code import settings as config
from __app__.shared_code import helper
import json
from datetime import datetime
from pytz import timezone
est = timezone('US/Eastern')
import pandas as pd
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('Python update ontology document function processed a request.')
        result=[]
        req_body = req.get_json()
        found_data = update_ontology_document(req_body[0])
        result = json.dumps(found_data)
    except Exception as e:
        pass
    return func.HttpResponse(result,mimetype="application/json")

def update_ontology_document(update_data):
    try:
        logging.info(f'doc body {update_data}')
        current_date=str(datetime.now(est))[:-9]
        conn=helper.SQL_connection()
        cursor=conn.cursor()
        skip_field=["ontology_Id","solr_Id","productName","product_Key","data_extract","category"]
        sql_id=update_data.get('ontology_Id','')
        solr_id=update_data.get('solr_Id','')
        product_name=update_data.get('productName','')
        product_type=update_data.get('product_Key','')
        user=update_data.get('updated_By','')
        data_extract_string=update_data.get('data_extract','')
        # category=update_data.get('category','')
        if sql_id !='' and solr_id !='':
            # updated_data_extract_string=get_updated_extract_string(data_extract_string,skip_field,update_data) 
            updated_data_extract_string=get_updated_extract_string(data_extract_string,update_data) 
            spec_id=get_spec_id_for_updated_value(product_name,product_type)
            # sql_updated_data_extract_string=
        update_value=f"is_relevant=1, product = '{product_name}',product_type='{product_type}',data_extract='{updated_data_extract_string}',updated='{current_date}',spec_id='{spec_id}'"
        update_query=f"update [momentive].[unstructure_processed_data] set {update_value} where id='{sql_id}'"
        logging.info(f'created update query {update_query}')
        # update_query=f"update [momentive].[unstructure_processed_data_latest_may9] set {update_value} where id='{sql_id}'"
        cursor.execute(update_query)
        # return "added"
    except Exception as e:
        try:
            conn.rollback()
        except Exception as e:
            pass
        logging.info(f'error in updating {e}')
        status_code=400
        message=f"cannot be updated because of {e}"
        # return {"status":status_code,"error":error}
    else:
        try:
            conn.commit()
            #update in change_audit_log table
            audit_status=helper.update_in_change_audit_log(sql_id,"Ontology Document",user,"update",current_date,product_type,product_name,updated_data_extract_string,"NULL")
            status_code=200
            doc={
            "solr_id":update_data.get("solr_Id","-"),
            "PRODUCT":product_name,
            "PRODUCT_TYPE":product_type,
            "DATA_EXTRACT":updated_data_extract_string,
            "UPDATED":current_date,
            "IS_RELEVANT":"1",
            "SPEC_ID":spec_id
            }
            logging.info(f'after solr doc {doc}')
            if audit_status=="updated in change audit log successfully":
                config.solr_unstructure_data.add([doc],fieldUpdates={"PRODUCT":"set","PRODUCT_TYPE":"set","DATA_EXTRACT":"set","UPDATED":"set","IS_RELEVANT":"set","SPEC_ID":"set"})
                
            message="updated successfully"
        except Exception as e:
            message="will be updated soon"
    return [{"status":status_code,"message":message}]

def get_updated_extract_string(data_extract_string,update_data):
    try:
        # object_data=json.loads(data_extract_string)
        object_data=data_extract_string
        extract_field=update_data["Extract_Field"]
        for item in extract_field:
            if item in object_data:
                object_data[item]=extract_field.get(item)
        dump_data=json.dumps(object_data)
        return dump_data
    except Exception as e:
        return ''

def get_spec_id_for_updated_value(product_name,product_type):
    try:
        product_query=helper.replace_character_for_querying([product_name])
        if product_type=="NAMPROD":
            query=f'TYPE:NAMPROD && SUBCT:REAL_SUB && TEXT1:({product_query}) && -TEXT6:X'
        elif product_type=="BDT":
            query=f'TYPE:MATNBR && TEXT3:({product_query}) && -TEXT6:X'
        values,df=helper.get_data_from_core(config.solr_product,query)
        if "TEXT2" in df.columns:
            spec_list=list(df["TEXT2"].unique())
            spec_str=";".join(spec_list)
        return spec_str
    except Exception as e:
        return ''
