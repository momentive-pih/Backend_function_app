import logging
import json
import azure.functions as func
import os 
import pysolr
import pandas as pd
from __app__.shared_code import settings as config
from __app__.shared_code import helper

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postRestrictedSubstance function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = get_restricted_data_details(req_body[0])
        result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

def  get_restricted_data_details(req_body):
    try:
        logging.info("restricted_substance request"+f'{req_body}')
        all_details_json,spec_list,material_list = helper.construct_common_level_json(req_body)
        std,std_df,legal,legal_df = helper.make_common_query_for_std_legal_composition(all_details_json)
        sub_category=req_body.get("Category_details").get("Subcategory")
        category=config.restricted_dict.get(sub_category)
        restricted_substance_query=helper.unstructure_template(all_details_json,[category])
        params={"fl":config.unstructure_column_str}      
        unstructure_values,unstructure_df=helper.get_data_from_core(config.solr_unstructure_data,restricted_substance_query,params)
        restricted_details=[]
        if len(unstructure_values)>0:
            if sub_category=="GADSL":
                for item in unstructure_values:
                    gadsl_cas=item.get("PRODUCT","")
                    result_spec=item.get("SPEC_ID")
                    spec_id=helper.finding_spec_details(spec_list,result_spec)
                    product_type=item.get("PRODUCT_TYPE",config.hypen_delimiter)
                    std_wg,componant_type=helper.find_std_weight(gadsl_cas,product_type,spec_id,std_df)
                    if std_wg=='':
                        continue
                    data=json.loads(item.get("DATA_EXTRACT"))
                    gadsl_json={
                            "substance": str(data.get("Substance",config.hypen_delimiter)),
                            "cas_NO": gadsl_cas,
                            "class_action": "",
                            "reason_Code": str(data.get("Reason Code",config.hypen_delimiter)),
                            "source": str(data.get("Source (Legal requirements, regulations)",config.hypen_delimiter)),
                            "reporting_threshold": str(data.get("Reporting threshold (0.1% unless otherwise stated)",config.hypen_delimiter)),
                            "weight_Composition": std_wg,
                            "spec_Id":spec_id
                        }
                    restricted_details.append(gadsl_json)
                    del gadsl_json
        if sub_category=="CALPROP":
            for item in unstructure_values:
                try:
                    calprop_cas=item.get("PRODUCT","")
                    result_spec=item.get("SPEC_ID")
                    spec_id=helper.finding_spec_details(spec_list,result_spec)
                    product_type=item.get("PRODUCT_TYPE",config.hypen_delimiter)
                    std_wg,componant_type=helper.find_std_weight(calprop_cas,product_type,spec_id,std_df)
                    if std_wg=='':
                        continue
                    data=json.loads(item.get("DATA_EXTRACT"))
                    calprop_json={
                            "chemical": str(data.get("Chemical",config.hypen_delimiter)),
                            "type_Toxicity": str(data.get("Type of Toxicity",config.hypen_delimiter)),
                            "listing_Mechanism": str(data.get("Listing Mechanism",config.hypen_delimiter)),
                            "cas_NO": calprop_cas,
                            "date_Listed": str(data.get("Date Listed",config.hypen_delimiter)),
                            "NSRL_Data": str(data.get("NSRL or MADL (Ã¦g/day)a",config.hypen_delimiter)),
                            "weight_Composition": std_wg,
                            "componant_Type":componant_type,
                            "spec_Id":spec_id
                        }
                    restricted_details.append(calprop_json)
                    del calprop_json
                except Exception as e:
                    del calprop_json
            #find CAS details in generic json
            generic_cas_info_list=helper.get_generic_cas_details(all_details_json)
            for item in generic_cas_info_list:
                try:
                    cas_no=item.get("cas_no",config.hypen_delimiter)
                    generic_spec_list=[element for element in all_details_json if cas_no in all_details_json.get(element).get("cas_number")]
                    spec_str=(config.pipe_delimitter).join(generic_spec_list)
                    std_wg,componant_type=helper.find_std_weight(cas_no,"NUMCAS",spec_str,std_df)
                    if std_wg=='':
                        continue
                    calprop_json={
                            "chemical": str(item.get("chemical_name",config.hypen_delimiter)),
                            "type_Toxicity": str(item.get("toxicity_type",config.hypen_delimiter)),
                            "listing_Mechanism": str(item.get("listing_mechanism",config.hypen_delimiter)),
                            "cas_NO": cas_no,
                            "date_Listed": str(item.get("date_listed",config.hypen_delimiter)),
                            "NSRL_Data": str(item.get("NSRL_MADL",config.hypen_delimiter)),
                            "weight_Composition": std_wg,
                            "componant_Type":componant_type,
                            "spec_Id":spec_str
                        }
                    restricted_details.append(calprop_json)
                    del calprop_json
                except Exception as e:
                    del calprop_json

        return restricted_details
    except Exception as e:
        return []

