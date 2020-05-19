import logging
import json
import azure.functions as func
import pandas as pd
import os 
from __app__.shared_code import settings as config
from __app__.shared_code import helper


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postProductCompliance function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = get_product_compliance_details(req_body[0])
        result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

def get_product_compliance_details(req_body):
    try:
        logging.info("product compliance request"+f'{req_body}')
        # compliance_details=[]
        result=[]
        notification_details=[]
        all_details_json,spec_list,material_list = helper.construct_common_level_json(req_body)
        sub_category=req_body.get("Category_details").get("Subcategory")
        if sub_category=="Notification Status":
            notify={}
            spec_query=(config.or_delimiter).join(spec_list)
            query=f'SUBID:({spec_query})'
            params={"fl":config.notification_column_str}
            pcomp,pcomp_df=helper.get_data_from_core(config.solr_notification_status,query,params)
            if ("NOTIF" in list(pcomp_df.columns)) and len(pcomp)>0:
                phrase_key=(list(pcomp_df["NOTIF"].unique()))
                if("ADDIN" in list(pcomp_df.columns)):
                    phrase_key=phrase_key+(list(pcomp_df["ADDIN"].unique()))
                phrase_split=";".join(phrase_key)
                phrase_key=phrase_split.split(";")
                phrase_key_query=helper.replace_character_for_querying(phrase_key)
                query=f'PHRKY:({phrase_key_query})'
                params={"fl":config.phrase_column_str}
                key_value,key_value_df=helper.get_data_from_core(config.solr_phrase_translation,query,params)
            for item in pcomp:
                try:
                    notify={}
                    notify["regulatory_List"]=str(item.get("RLIST",config.hypen_delimiter)).strip()
                    ntfy_rg_value=str(item.get("NOTIF","")).strip()
                    ntfy_rg=ntfy_rg_value.split(";")
                    notify_value=[(config.hypen_delimiter)]
                    if ("PTEXT" in list(key_value_df.columns)) and ("PHRKY" in list(key_value_df.columns)):
                        ptext_df=key_value_df[key_value_df["PHRKY"].isin(ntfy_rg)]
                        notify_value=list(ptext_df["PTEXT"])
                    notify["regulatory_Basis"]="-"
                    notify["notification"]=(config.comma_delimiter).join(notify_value)
                    #add phrse text
                    add_value=str(item.get("ADDIN","")).strip()
                    add_rg=add_value.split(";")
                    add_list=[(config.hypen_delimiter)]
                    if ("PTEXT" in list(key_value_df.columns)) and ("PHRKY" in list(key_value_df.columns)):
                        add_df=key_value_df[key_value_df["PHRKY"].isin(add_rg)]
                        add_list=list(add_df["PTEXT"])
                    notify["additional_Info"]=(config.comma_delimiter).join(add_list)
                    notify["usage"]=str(item.get("ZUSAGE","-")).strip()
                    #find spec_id
                    subid=item.get("SUBID","-")
                    if(all_details_json.get(subid)):
                        nam_list=all_details_json.get(subid).get("namprod",[])
                        nam_str=(config.comma_delimiter).join(nam_list)
                    else:
                        nam_str=config.hypen_delimiter
                    notify["spec_id"]=subid+(config.hypen_delimiter)+nam_str
                    notification_details.append(notify)               
                except Exception as e:
                    pass     
            result=notification_details   
        elif sub_category=="AG Registration Status":
            eu_json_list=[]
            us_json_list=[]
            latin_list=[]
            category=config.ag_registration_list
            ag_reg_query=helper.unstructure_template(all_details_json,category)
            params={"fl":config.unstructure_column_str}
            unstructure_values,unstructure_df=helper.get_data_from_core(config.solr_unstructure_data,ag_reg_query,params)
            for item in unstructure_values:
                try:
                    json_make={}
                    result_spec=item.get("SPEC_ID")
                    spec_id=helper.finding_spec_details(spec_list,result_spec)
                    region=item.get("CATEGORY").strip()
                    datastr=json.loads(item.get("DATA_EXTRACT"))
                    if region=="EU_REG_STATUS":                              
                        json_make["product"]=str(item.get("PRODUCT",config.hypen_delimiter)).strip()
                        json_make["country"]=str(datastr.get("Country",config.hypen_delimiter)).strip()
                        json_make["holder"]=str(datastr.get("Holder",config.hypen_delimiter)).strip()
                        json_make["registration"]=str(datastr.get("Registration",config.hypen_delimiter)).strip()
                        json_make["expiry"]=str(datastr.get("Expiry",config.hypen_delimiter)).strip()
                        json_make["status"]=str(datastr.get("Status",config.hypen_delimiter)).strip()
                        json_make["certificate"]=str(datastr.get("Certificate",config.hypen_delimiter)).strip()
                        json_make["spec_id"]=spec_id
                        eu_json_list.append(json_make)
                    elif region=="US_REG_STATUS":
                        json_make["product"]=str(item.get("PRODUCT",config.hypen_delimiter)).strip()
                        json_make["EPA_Inert_Product_Listing"]=str(datastr.get("EPA Inert Product Listing",config.hypen_delimiter)).strip()
                        json_make["CA_DPR"]=str(datastr.get("CA DPR",config.hypen_delimiter)).strip()
                        json_make["CP_DA"]=str(datastr.get("CPDA",config.hypen_delimiter)).strip()
                        json_make["WSDA"]=str(datastr.get("WSDA",config.hypen_delimiter)).strip()
                        json_make["OMRI"]=str(datastr.get("OMRI",config.hypen_delimiter)).strip()
                        json_make["OMRI_Reneval_Date"]=str(datastr.get("OMRI Renewal Date",config.hypen_delimiter)).strip()
                        json_make["Canada_OMRI"]=str(datastr.get("Canada OMRI",config.hypen_delimiter)).strip()
                        json_make["PMRA"]=str(datastr.get("PMRA",config.hypen_delimiter)).strip()
                        json_make["spec_id"]=spec_id
                        us_json_list.append(json_make)
                    elif region=="LATAM_REG_STATUS":
                        json_make["product"]=str(item.get("PRODUCT",config.hypen_delimiter)).strip()
                        json_make["country"]=str(datastr.get("Country",config.hypen_delimiter)).strip()
                        json_make["registered_Name"]=str(datastr.get("Registered Name",config.hypen_delimiter)).strip()
                        json_make["date_Granted"]=str(datastr.get("Date Granted",config.hypen_delimiter)).strip()
                        json_make["date_Of_Expiry"]=str(datastr.get("Date of Expiry",config.hypen_delimiter)).strip()
                        json_make["registration_Holder"]=str(datastr.get("Holder",config.hypen_delimiter)).strip()
                        json_make["registration_Certificate"]=str(datastr.get("Registration Certificate (Location)",config.hypen_delimiter)).strip()
                        json_make["spec_id"]=spec_id
                        latin_list.append(json_make)
                    del json_make
                except Exception as e:
                    pass
            ag_make={}
            ag_make["complianceRegistrationEUData"]=eu_json_list
            ag_make["complianceRegistrationCanada_Data"]=us_json_list
            ag_make["complianceRegistrationLatin_Data"]=latin_list
            result.append(ag_make)

        return result
    except Exception as e:
        return result
