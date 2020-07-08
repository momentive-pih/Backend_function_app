import logging
import azure.functions as func
from __app__.shared_code import settings as config
from __app__.shared_code import helper
import json
from datetime import datetime
from pytz import timezone
est = timezone('US/Eastern')
import pandas as pd

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('Python update ontology management function processed a request.')
        result=[]
        req_body = req.get_json()
        found_data = update_ontology_value(req_body)
        result = json.dumps(found_data)
    except Exception as e:
        pass
    return func.HttpResponse(result,mimetype="application/json")

def update_ontology_value(update_data):
    try:
        logging.info(f'body {update_data}')
        if "ontology_Id" in update_data:
            status=edit_ontology_value(update_data) 
            # pass
        else:
            status=add_ontology_value(update_data)  
        return status
    except Exception as e:
        pass

def add_ontology_value(add_data):
    try:
        current_date=str(datetime.now(est))[:-9]
        conn=helper.SQL_connection()
        cursor=conn.cursor()
        ontology_value=add_data.get('ontologySynonyms',"").replace("'","''")
        inser_value=f"'{add_data.get('synonymsProductName','')}','{add_data.get('synonymsProductType','')}','{ontology_value}','{add_data.get('synonymsCreatedBy','')}','{current_date}','{current_date}','NULL',1" 
        insert_query=f"insert into [momentive].[ontology] values ({inser_value})"
        cursor.execute(insert_query)
        # return "added"
    except Exception as e:
        try:
            conn.rollback()
        except Exception as e:
            pass
        logging.info(f'error in updating {e}')
        status_code=400
        message=f"cannot be added because of {e}"
    else:
        try:
            conn.commit()
            #finding ID from solr 
            query=f'-ID:\-'
            sql_id=0
            ontolgy_result,ontolgy_df=helper.get_data_from_core(config.solr_ontology,query)
            if "ID" in ontolgy_df.columns:
                ontolgy_df=ontolgy_df.replace({"-":"0"})
                ontolgy_df["ID"]= ontolgy_df["ID"].apply(pd.to_numeric)
                list_of_id=list(ontolgy_df["ID"].unique())
            sql_id=max(list_of_id)
            found_id=str(sql_id+1)
            product_synonyms=add_data.get("ontologySynonyms","")
            product=add_data.get("synonymsProductName","")
            product_type=add_data.get("synonymsProductType","")
            doc={"ONTOLOGY_KEY":add_data.get("synonymsProductName",""),
            "ID":str(sql_id+1), 
            "KEY_TYPE":add_data.get("synonymsProductType",""),
            "ONTOLOGY_VALUE":add_data.get("ontologySynonyms",""),
            "CREATED_BY":add_data.get('synonymsCreatedBy',''),
            "CREATED_DATE":current_date,
            "UPDATED_DATE":current_date,
            "PROCESSED_FLAG":"",
            "IS_RELEVANT":"1"}
            #update in change_audit_log table
            audit_status=helper.update_in_change_audit_log(found_id,"Ontology Management",add_data.get('synonymsCreatedBy',''),"insert",current_date,product_type,product,product_synonyms,"N")
            status_code=200
            if audit_status=="updated in change audit log successfully":
                config.solr_ontology.add([doc])
            message="added successfully"
        except Exception as e:
            message="will be added soon"
    return [{"status":status_code,"message":message}]
    
def edit_ontology_value(update_data):
    try:
        current_date=str(datetime.now(est))[:-9]
        conn=helper.SQL_connection()
        cursor=conn.cursor()
        ontology_value=update_data.get('ontologySynonyms',"").replace("'","''")
        update_value=f"ONTOLOGY_KEY = '{update_data.get('synonymsProductName','')}',KEY_TYPE='{update_data.get('synonymsProductType','')}',ONTOLOGY_VALUE='{ontology_value}',UPDATED_DATE='{current_date}',PROCESSED_FLAG='NULL'"
        update_query=f"update [momentive].[ontology] set {update_value} where id='{update_data.get('ontology_Id','-')}'"
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
    else:
        try:
            conn.commit()
            product_synonyms=update_data.get("ontologySynonyms","")
            product=update_data.get("synonymsProductName","")
            product_type=update_data.get("synonymsProductType","")
            #update in change_audit_log table
            audit_status=helper.update_in_change_audit_log(update_data.get('ontology_Id','-'),"Ontology Management",update_data.get("synonymsUpdatedBy","-"),"update",current_date,product_type,product,product_synonyms,"N")
            doc={
            "solr_id":update_data.get("solr_Id","-"),
            "ONTOLOGY_KEY":update_data.get("synonymsProductName",""),
            "KEY_TYPE":update_data.get("synonymsProductType",""),
            "ONTOLOGY_VALUE":update_data.get("ontologySynonyms",""),
            "UPDATED_DATE":current_date,
            "PROCESSED_FLAG":"NULL"
            }
            status_code=200
            if audit_status=="updated in change audit log successfully":
                config.solr_ontology.add([doc],fieldUpdates={"ONTOLOGY_KEY":"set","KEY_TYPE":"set","ONTOLOGY_VALUE":"set","UPDATED_DATE":"set"})
            message="updated successfully"
        except Exception as e:
            message="will be updated soon"
    return [{"status":status_code,"message":message}]

def delete_ontology_value(delete_data):
    try:
        conn=helper.SQL_connection()
        cursor=conn.cursor()
        delete_query=f"update [momentive].[ontology] set deleteflag=True where id={delete_data.get('ontologyId','-')}"
        cursor.execute(delete_query)
        conn.commit()
    except Exception as e:
        pass
    else:
        try:
            config.solr_ontology.delete(solr_id=delete_data.get('solr_Id','-'))
        except Exception as e:
            return "Will be deleted shortly"
    return "Deleted sucessfully"