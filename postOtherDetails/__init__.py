import logging
import json
import azure.functions as func
import pandas as pd
import os 
from __app__.shared_code import settings as config
from __app__.shared_code import helper

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postOtherDetails function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = get_other_details(req_body[0])
        result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

def get_other_details(req_body):
    try:
        other_details=[]
        spec_json,spec_list=helper.spec_constructor(req_body)
        # all_details_json,spec_list,material_list = helper.construct_common_level_json(req_body)
        sub_category=req_body.get("Category_details").get("Subcategory")
        if sub_category=="allergen":
            other_details=get_allergen_details(spec_json,spec_list)
        elif sub_category=="bio_Compatibility_Testing_PRI":
            other_details=get_bio_compatibility_details(spec_json,spec_list)
        elif sub_category=="BSE_TSE_GMO":
            other_details=get_bse_details(spec_json,spec_list)
        elif sub_category=="EPA":
            other_details=get_epa_details(spec_json,spec_list)
        elif sub_category=="registrations_Company_Specific":
            all_details_json,spec_list,material_list = helper.construct_common_level_json(req_body)
            cas_list=[]
            for spec in all_details_json:
                cas_list+=all_details_json.get(spec).get("pure_spec_id")
            # cas_query=(config.or_delimiter).join(cas_list)
            other_details=get_registrations_company_details(spec_json,cas_list)
        elif sub_category=="product_Regulatory_Information":
            other_details=get_product_regulatory_details(spec_json,spec_list)
        elif sub_category=="phrase_translation":
            other_details=get_phrase_translation()
    except Exception as e:
        pass
    return other_details

def get_allergen_details(spec_json,spec_list):
    try:
        details=[]
        spec_query=(config.or_delimiter).join(spec_list)
        query=f'SUBID:({spec_query})'
        check_column=["ALERG","ALEST"]
        values,df=helper.get_data_from_core(config.solr_allergen,query)
        key_value,key_value_df=get_related_phrase_text(check_column,df)
        for data in values:
            try:
                json_make={}
                specid=data.get("SUBID","")      
                spec_nam_str=specid+(config.hypen_delimiter)+spec_json.get(specid,"")
                json_make["spec_Id"]=spec_nam_str
                json_make["usage"]=str(data.get("ZUSAGE",config.hypen_delimiter).strip())
                json_make["alerg"]=helper.finding_phrase_text(key_value_df,str(data.get("ALERG","")).strip())
                json_make["alest"]=helper.finding_phrase_text(key_value_df,str(data.get("ALEST","")).strip())    
                details.append(json_make)
            except Exception as e:
                pass
        return details
    except Exception as e:
        return []
def get_bio_compatibility_details(spec_json,spec_list):
    try:
        details=[]
        spec_query=(config.or_delimiter).join(spec_list)
        query=f'SUBID:({spec_query})'
        check_column=["BSTAT","BTEST"]
        values,df=helper.get_data_from_core(config.solr_biocompatibility_testing_pri,query)
        key_value,key_value_df=get_related_phrase_text(check_column,df)
        for data in values:
            try:
                json_make={}
                specid=data.get("SUBID","")      
                spec_nam_str=specid+(config.hypen_delimiter)+spec_json.get(specid,"")
                json_make["spec_Id"]=spec_nam_str
                json_make["usage"]=str(data.get("ZUSAGE",config.hypen_delimiter).strip())
                json_make["bstat"]=helper.finding_phrase_text(key_value_df,str(data.get("BSTAT","")).strip())
                json_make["btest"]=helper.finding_phrase_text(key_value_df,str(data.get("BTEST","")).strip())    
                details.append(json_make)
            except Exception as e:
                pass
        return details
    except Exception as e:
        return []
def get_bse_details(spec_json,spec_list):
    try:
        details=[]
        spec_query=(config.or_delimiter).join(spec_list)
        query=f'SUBID:({spec_query})'
        check_column=["BSTSG","BTGST"]
        values,df=helper.get_data_from_core(config.solr_bse_tse_gmo,query)
        key_value,key_value_df=get_related_phrase_text(check_column,df)
        for data in values:
            try:
                json_make={}
                specid=data.get("SUBID","")      
                spec_nam_str=specid+(config.hypen_delimiter)+spec_json.get(specid,"")
                json_make["spec_Id"]=spec_nam_str
                json_make["usage"]=str(data.get("ZUSAGE",config.hypen_delimiter).strip())
                json_make["bstsg"]=helper.finding_phrase_text(key_value_df,str(data.get("BSTSG","")).strip())
                json_make["btgst"]=helper.finding_phrase_text(key_value_df,str(data.get("BTGST","")).strip())    
                details.append(json_make)
            except Exception as e:
                pass
        return details
    except Exception as e:
        return []
def get_epa_details(spec_json,spec_list):
    try:
        details=[]
        spec_query=(config.or_delimiter).join(spec_list)
        query=f'SUBID:({spec_query})'
        check_column=["EPARG","EPAST"]
        values,df=helper.get_data_from_core(config.solr_epa,query)
        key_value,key_value_df=get_related_phrase_text(check_column,df)
        for data in values:
            try:
                json_make={}
                specid=data.get("SUBID","")      
                spec_nam_str=specid+(config.hypen_delimiter)+spec_json.get(specid,"")
                json_make["spec_Id"]=spec_nam_str
                json_make["usage"]=str(data.get("ZUSAGE",config.hypen_delimiter).strip())
                json_make["eparg"]=helper.finding_phrase_text(key_value_df,str(data.get("EPARG","")).strip())
                json_make["epast"]=helper.finding_phrase_text(key_value_df,str(data.get("EPAST","")).strip())    
                details.append(json_make)
            except Exception as e:
                pass
        return details
    except Exception as e:
        return []
def get_registrations_company_details(spec_json,spec_list):
    try:
        details=[]
        spec_query=(config.or_delimiter).join(spec_list)
        query=f'SUBID:({spec_query})'
        values,df=helper.get_data_from_core(config.solr_registration_company_specific,query)
        for data in values:
            try:
                json_make={}
                specid=data.get("SUBID","")      
                spec_nam_str=specid
                json_make["spec_Id"]=spec_nam_str
                json_make["usage"]=str(data.get("ZUSAGE",config.hypen_delimiter).strip())
                json_make["rebas"]=str(data.get("REBAS",config.hypen_delimiter).strip())
                json_make["revsn"]=str(data.get("REVSN",config.hypen_delimiter).strip())
                json_make["relst"]=str(data.get("RELST",config.hypen_delimiter).strip())
                json_make["bukrs"]=str(data.get("BUKRS",config.hypen_delimiter).strip())
                json_make["werks"]=str(data.get("WERKS",config.hypen_delimiter).strip())
                json_make["zprior"]=str(data.get("ZPRIOR",config.hypen_delimiter).strip())
                json_make["rdate"]=str(data.get("RDATE",config.hypen_delimiter).strip())
                json_make["expdt"]=str(data.get("EXPDT",config.hypen_delimiter).strip())
                json_make["regqy"]=str(data.get("REGQY",config.hypen_delimiter).strip())
                json_make["regnm"]=str(data.get("REGNM",config.hypen_delimiter).strip())
                json_make["regns"]=str(data.get("REGNS",config.hypen_delimiter).strip())
                json_make["conso"]=str(data.get("CONSO",config.hypen_delimiter).strip())
                json_make["stats"]=str(data.get("STATS",config.hypen_delimiter).strip())
                json_make["regph"]=str(data.get("REGPH",config.hypen_delimiter).strip())
                json_make["addin"]=str(data.get("ADDIN",config.hypen_delimiter).strip())
                json_make["remar"]=str(data.get("REMAR",config.hypen_delimiter).strip())
                details.append(json_make)
            except Exception as e:
                pass
        return details
    except Exception as e:
        return []
def get_product_regulatory_details(spec_json,spec_list):
    try:
        details=[]
        spec_query=(config.or_delimiter).join(spec_list)
        query=f'SUBID:({spec_query})'
        check_column=["PRIRG","CSTAT"]
        values,df=helper.get_data_from_core(config.solr_product_regulatory_information,query)
        key_value,key_value_df=get_related_phrase_text(check_column,df)
        for data in values:
            try:
                json_make={}
                specid=data.get("SUBID","")      
                spec_nam_str=specid+(config.hypen_delimiter)+spec_json.get(specid,"")
                json_make["spec_Id"]=spec_nam_str
                json_make["usage"]=str(data.get("ZUSAGE",config.hypen_delimiter).strip())
                json_make["prirg"]=helper.finding_phrase_text(key_value_df,str(data.get("PRIRG","")).strip())
                json_make["cstat"]=helper.finding_phrase_text(key_value_df,str(data.get("CSTAT","")).strip())    
                details.append(json_make)
            except Exception as e:
                pass
        return details
    except Exception as e:
        return []

def get_phrase_translation():
    try:
        details=[]
        query=f'*:*'
        values,df=helper.get_data_from_core(config.solr_phrase_translation,query)
        for data in values:
            json_make={}
            json_make["phrase_Key"]=str(data.get("PHRKY",config.hypen_delimiter).strip())
            json_make["phrase_Code"]=str(data.get("PHRCD",config.hypen_delimiter).strip())
            json_make["phrase_Graph"]=str(data.get("GRAPH",config.hypen_delimiter).strip())
            json_make["phrase_Text"]=str(data.get("PTEXT",config.hypen_delimiter).strip())
            details.append(json_make)
        return details
    except Exception as e:
        return [] 
def get_related_phrase_text(column_check,check_df):
    try:
        temp=pd.DataFrame()
        total_phrky=[]
        for key_column in column_check:
            try:
                if key_column in list(check_df.columns):
                    check_df[key_column]=check_df[key_column].astype(str).str.strip()
                    phrase_key=list(check_df[key_column].unique())
                    phrase_split=";".join(phrase_key)
                    total_phrky+=phrase_split.split(";") 
            except Exception as e:
                pass   
        #finding phrase text
        phrase_key_query=helper.replace_character_for_querying(total_phrky)
        query=f'PHRKY:({phrase_key_query})'
        params={"fl":config.phrase_column_str}
        key_value,key_value_df=helper.get_data_from_core(config.solr_phrase_translation,query,params)  
        return key_value,key_value_df
    except Exception as e:
        return [],temp

                


