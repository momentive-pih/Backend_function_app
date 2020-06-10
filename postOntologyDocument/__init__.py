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
        logging.info('postReportData function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = get_assigned_ontology_document(req_body[0])
        result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

def get_assigned_ontology_document(req_body):
    try:
        output_json=[]
        count=0
        sub_category=req_body.get("Category_details").get("Subcategory")
        out_template={
            "US-FDA":{
                "US-FDA":[],
                "category":"US-FDA"
            },"EU-FDA":{
                "EU-FDA":[],"category":"EU-FDA"
            },"Toxicology-summary":{
                "Toxicology-summary":[],"category":"Toxicology Summary"
            },"CIDP":{
                "CIDP":[],"category":"CIDP"
            },"Toxicology":{
                "Toxicology":[],"category":"Toxicology"
            }
        }
        category=config.ontology_assigned_category
        if sub_category=="assigned":
            all_details_json,spec_list,material_list = helper.construct_common_level_json(req_body)
            ontology_query=helper.unstructure_template(all_details_json,category)
        elif sub_category=="unassigned":
            spec_list=[]
            category_list=" || ".join(category)
            ontology_query=f'CATEGORY:({category_list}) && IS_RELEVANT:0'  
        params={"fl":config.unstructure_column_str}
        unstructure_values,unstructure_df=helper.get_data_from_core(config.solr_unstructure_data,ontology_query,params)        
        if len(unstructure_values)>0:
            for item in unstructure_values:
                try:
                    unstructure_category=item.get("CATEGORY","")
                    datastr=json.loads(item.get("DATA_EXTRACT",{}))
                    path=str(datastr.get("file_path",config.hypen_delimiter)).strip()
                    if path.lower().endswith("pdf"):
                        result_spec=item.get("SPEC_ID")
                        product=item.get("PRODUCT",config.hypen_delimiter)
                        product_type=item.get("PRODUCT_TYPE",config.hypen_delimiter)
                        spec_id=helper.finding_spec_details(spec_list,result_spec) 
                        filename=datastr.get("file_name",config.hypen_delimiter)
                        file_split=path.split("/")
                        file_source=''
                        for source in config.file_sources:
                            if source in file_split:
                                file_source=source
                                break
                        json_make={}
                        count+=1
                        date=datastr.get("Date",config.hypen_delimiter)
                        json_make["fileName"]=filename
                        json_make["file_source"]=file_source
                        json_make["category"]=unstructure_category
                        json_make["spec_Id"]=spec_id
                        if sub_category=="unassigned":
                            product="No-key-value"
                        json_make["productName"]=product
                        json_make["product_Key"]=product_type
                        json_make["id"]=count
                        json_make["createdDate"]=date
                        json_make["data_extract"]=datastr
                        json_make["sql_Id"]=item.get("ID",'0')
                        json_make["solr_Id"]=item.get("solr_id",config.hypen_delimiter)
                        path=helper.replace_char_in_url(path)
                        json_make["url"]=(config.blob_file_path)+path.replace("/dbfs/mnt/","")+(config.sas_token)
                        extract_field={}
                        extract_field["ontologyKey"]=product
                        for efield in datastr:
                            if efield not in config.otherfields:
                                extract_field[efield]=datastr.get(efield,config.hypen_delimiter)
                        json_make["Extract_Field"]=extract_field
                        out_template[unstructure_category][unstructure_category].append(json_make)
                except Exception as e:
                    pass
        for item in category:
            if len(out_template.get(item).get(item))>0:
                output_json.append(out_template.get(item))
        return output_json
    except Exception as e:
        return output_json