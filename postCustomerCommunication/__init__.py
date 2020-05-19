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
        logging.info('postCustomerCommunication function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = get_customer_communication_details(req_body[0])
        result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

def get_customer_communication_details(req_body):
    try:
        all_details_json,spec_list,material_list = helper.construct_common_level_json(req_body)
        sub_category=req_body.get("Category_details").get("Subcategory")
        json_list=[]
        if sub_category in config.customer_communication_category:
            category=config.customer_communication_category.get(sub_category)
            communication_query=helper.unstructure_template(all_details_json,category)
            params={"fl":config.unstructure_column_str}
            unstructure_values,unstructure_df=helper.get_data_from_core(config.solr_unstructure_data,communication_query,params)        
            if len(unstructure_values)>0:
                count=0
                for item in unstructure_values:
                    try:
                        json_make={}
                        datastr={}
                        datastr=json.loads(item.get("DATA_EXTRACT",{}))
                        result_spec=item.get("SPEC_ID")
                        product=item.get("PRODUCT",config.hypen_delimiter)
                        product_type=item.get("PRODUCT_TYPE",config.hypen_delimiter)
                        spec_id=helper.finding_spec_details(spec_list,result_spec)     
                        if (sub_category in ["US FDA Letter","EU Food Contact"]):
                            path=str(datastr.get("file_path",config.hypen_delimiter)).strip()
                            if (path.lower().endswith("pdf")):
                                file_split=path.split("/")
                                file_source=''
                                for source in config.file_sources:
                                    if source in file_split:
                                        file_source=source
                                        break
                                count+=1
                                extract_field={}
                                for efield in datastr:
                                    if efield not in config.otherfields:
                                        extract_field[efield]=datastr.get(efield,config.hypen_delimiter)
                                json_make["Extract_Field"]=extract_field
                                filename=datastr.get("file_name",config.hypen_delimiter)          
                                date=datastr.get("Date",config.hypen_delimiter)
                                json_make["spec_Id"]=spec_id
                                json_make["fileName"]=filename
                                json_make["file_Source"]=file_source
                                json_make["product_Type"]=product_type
                                json_make["productName"]=product
                                json_make["id"]=count
                                json_make["createdDate"]=date
                                json_make["url"]=(config.blob_file_path)+path.replace("/dbfs/mnt/","")+(config.sas_token)
                                json_list.append(json_make)
                        elif sub_category=="Heavy Metals content":
                            datastr=json.loads(datastr)
                            path=datastr.get("file_path","")
                            file_split=path.split("/")
                            file_source=''
                            for source in config.file_sources:
                                if source in file_split:
                                    file_source=source
                                    break
                            json_make["spec_Id"]=spec_id
                            json_make["file_Source"]=file_source
                            json_make["product"]=product
                            json_make["product_Type"]=product_type
                            json_make["aka"]=datastr.get("AKA",config.hypen_delimiter)
                            json_make["batch"]=datastr.get("Batch #",config.hypen_delimiter)
                            json_make["sample"]=datastr.get("Sample #",config.hypen_delimiter)
                            json_make["system"]=datastr.get("System",config.hypen_delimiter)
                            json_make["date"]=datastr.get("Date",config.hypen_delimiter)
                            json_make["aluminium_Al"]=datastr.get("Aluminum (Al)",config.hypen_delimiter)
                            json_make["antimony_Sb"]=datastr.get("Antimony (Sb)",config.hypen_delimiter)
                            json_make["arsenic_As"]=datastr.get("Arsenic (As)",config.hypen_delimiter)
                            json_make["barium_Ba"]=datastr.get("Barium (Ba)",config.hypen_delimiter)
                            json_make["beryllium_Be"]=datastr.get("Beryllium (Be)",config.hypen_delimiter)
                            json_make["boron_B"]=datastr.get("Boron (B)",config.hypen_delimiter)
                            json_make["cadmium_Cd"]=datastr.get("Cadmium (Cd)",config.hypen_delimiter)
                            json_make["calcium_Ca"]=datastr.get("Calcium (Ca)",config.hypen_delimiter)
                            json_make["carbon"]=datastr.get("Carbon",config.hypen_delimiter)
                            json_list.append(json_make)
                    except Exception as e:
                        pass       
        # elif sub_category=="Communication History" and ("case_Number" not in req_body) and ("selected_level" in req_body):
        elif sub_category=="Communication History" and ("case_Number" not in req_body):
            logging.info(f'communication req body {req_body}')
            sfdc_query=helper.sfdc_template(all_details_json)
            params={"fl":config.sfdc_column_str}
            sfdc_values,sfdc_df=helper.get_data_from_core(config.solr_sfdc,sfdc_query,params)        
            if len(sfdc_values)>0 and ("CASENUMBER" in list(sfdc_df.columns)):
                if len(sfdc_df.columns)!=len(config.sfdc_column):
                    dummy=pd.DataFrame([],columns=config.sfdc_column)
                    sfdc_df=pd.concat([sfdc_df,dummy])
                case_df=sfdc_df[config.sfdc_case_call]
                case_df.drop_duplicates(inplace=True)
                case_df=case_df.fillna(config.hypen_delimiter)
                case_df=case_df.replace({"NULL":"-"})
                for index, row in case_df.iterrows():
                    json_make={}
                    json_make["case_Number"]=row["CASENUMBER"]
                    json_make["manufacturing_Plant"]=row["MANUFACTURINGPLANT"]
                    # customer_name=(row["ACCOUNTNAME"]).split("||")
                    # json_make["customer_Name"]=(config.comma_delimiter).join(customer_name)
                    json_make["customer_Name"]=row["ACCOUNTNAME"]
                    json_make["key"]=row["MATCHEDPRODUCTVALUE"]
                    json_make["product_Type"]=row["MATCHEDPRODUCTCATEGORY"]
                    json_make["topic"]=row["REASON"]
                    json_make["tier_2_Owner"]=row["SOP_TIER_2_OWNER__C"]
                    json_make["bu"]=row["BU"]
                    json_list.append(json_make) 
        elif sub_category=="Communication History" and ("case_Number" in req_body):
            selected_case=req_body.get("case_Number")
            sfdc_query=helper.sfdc_template(all_details_json)
            sfdc_query = f'(CASENUMBER:{selected_case}) && '+sfdc_query
            params={"fl":config.sfdc_email_call}
            sfdc_values,sfdc_df=helper.get_data_from_core(config.solr_sfdc,sfdc_query,params)
            for item in sfdc_values:
                json_make={}
                if item.get("CONTACTEMAIL",config.hypen_delimiter)!="NULL":
                    json_make["contact_Email"]=item.get("CONTACTEMAIL",config.hypen_delimiter)
                else:
                    json_make["contact_Email"]=config.hypen_delimiter
                json_make["email_Content"]=item.get("EMAILBODY",config.hypen_delimiter)
                json_make["email_Subject"]=item.get("EMAILSUBJECT","")
                attachment=str(item.get("EMAILATTACHMENT",""))
                attachment_split=attachment.split("|:|")
                add_doc=[]
                for att in attachment_split:
                    if att!="NULL" and att!='' and att!="Not Found":
                        path=att.split("/")
                        filename=(att[1:]).replace("?","%3F")
                        file=(config.blob_file_path)+filename+(config.sas_token)
                        add_doc.append({"name":path[-1],"url":file})
                json_make["attached_Docs"]=add_doc
                json_list.append(json_make)
        return json_list
    except Exception as e:
        return json_list
                        

