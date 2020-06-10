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
                    std_wg=find_std_weight(gadsl_cas,product_type,spec_id,std_df)
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
                    calprop_cas=item.get("PRODUCT","")
                    result_spec=item.get("SPEC_ID")
                    spec_id=helper.finding_spec_details(spec_list,result_spec)
                    product_type=item.get("PRODUCT_TYPE",config.hypen_delimiter)
                    std_wg=find_std_weight(calprop_cas,product_type,spec_id,std_df)
                    if std_wg=='':
                        continue
                    # for cas_item in std_value:
                    #     cas_value=cas_item.get("CAS")
                    #     cas_real_spec=cas_item.get("SUBID")
                    #     if cas_value==calprop_cas and cas_real_spec in spec_id:
                    #         std_wg=str(cas_item.get("CVALU","0"))+" "+str(cas_item.get("CUNIT",""))
                    #         break
                    data=json.loads(item.get("DATA_EXTRACT"))
                    calprop_json={
                            "chemical": str(data.get("Chemical",config.hypen_delimiter)),
                            "type_Toxicity": str(data.get("Type of Toxicity",config.hypen_delimiter)),
                            "listing_Mechanism": str(data.get("Listing Mechanism",config.hypen_delimiter)),
                            "cas_NO": calprop_cas,
                            "date_Listed": str(data.get("Date Listed",config.hypen_delimiter)),
                            "NSRL_Data": str(data.get("NSRL or MADL (Ã¦g/day)a",config.hypen_delimiter)),
                            "weight_Composition": std_wg,
                            "spec_Id":spec_id
                        }
                    restricted_details.append(calprop_json)
                    del calprop_json
        
        return restricted_details
    except Exception as e:
        return []

def find_std_weight(product,product_type,spec_id,std_df):
    #checking std compositon condition
    std_weight=''
    if product_type=="NUMCAS":
        specid_list=spec_id.split(config.pipe_delimitter)
        if "CAS" in list(std_df.columns) and "SUBID" in list(std_df.columns):
            std_find=std_df[(std_df["CAS"]==product) & (std_df["SUBID"].isin(specid_list))]
    if len(std_find)==0:
        return std_weight 
    else:
        if len(std_find)>0 and "CVALU" in list(std_find.columns):
            std_cvalue=std_find[["CVALU","CUNIT"]]
            std_cvalu_list=std_cvalue.values.tolist()
            for value,unit in std_cvalu_list:
                std_weight=helper.calculate_ppm_ppb(value,unit)
                break
    return f"{std_weight}%"