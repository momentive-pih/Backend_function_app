import logging
import json
import azure.functions as func
import pandas as pd
import os 
import pysolr
from __app__.shared_code import settings as config
from __app__.shared_code import helper

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postToxicology function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = get_toxicology_details(req_body[0])
        result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

def get_toxicology_details(req_body):
    try:
        all_details_json,spec_list,material_list = helper.construct_common_level_json(req_body)
        std,std_df,legal,legal_df = helper.make_common_query_for_std_legal_composition(all_details_json)
        sub_category=req_body.get("Category_details").get("Subcategory")
        json_list=[]
        if sub_category in config.toxicology_category:
            category=config.toxicology_dict.get(sub_category)
            toxicology_query=helper.unstructure_template(all_details_json,category)
            params={"fl":config.unstructure_column_str}
            unstructure_values,unstructure_df=helper.get_data_from_core(config.solr_unstructure_data,toxicology_query,params)        
            selant=[]
            silanes=[]  
            tox_study=[]           
            if len(unstructure_values)>0:
                for item in unstructure_values:
                    try:
                        json_make={}
                        result_spec=item.get("SPEC_ID")
                        ontology_value=item.get("ONTOLOGY_VALUE","")
                        spec_id=helper.finding_spec_details(spec_list,result_spec)
                        product=item.get("PRODUCT",config.hypen_delimiter)
                        product_type=item.get("PRODUCT_TYPE",config.hypen_delimiter)
                        datastr=json.loads(item.get("DATA_EXTRACT"))
                        category=item.get("CATEGORY","")
                        file_path=datastr.get("file_path",config.hypen_delimiter)
                        file_path=helper.replace_char_in_url(file_path)
                        file_split=file_path.split("/")
                        file_source=''
                        for source in config.file_sources:
                            if source in file_split:
                                file_source=source
                                break
                        std_find=[]
                        legal_find=[]
                        std_flag="No"
                        legal_flag="No"                     
                        json_make["product_Name"]=product
                        json_make["product_Type"]=product_type
                        json_make["file_Source"]=file_source
                        json_make["ontology_value"]=ontology_value
                        json_make["spec_Id"]=spec_id
                        if sub_category=="Study Title and Date":
                            #checking std and legal compositon condition
                            if product_type in ["NUMCAS"]:
                                specid_list=spec_id.split(config.pipe_delimitter)
                                if "CAS" in list(std_df.columns) and "SUBID" in list(std_df.columns):
                                    std_find=std_df[(std_df["CAS"]==product) & (std_df["SUBID"].isin(specid_list))]
                                elif "CAS" in list(legal_df.columns) and "SUBID" in list(legal_df.columns):
                                    legal_find=legal_df[(legal_df["CAS"]==product) & (legal_df["SUBID"].isin(specid_list))]
                                if len(std_find)==0 and len(legal_find)==0:
                                    continue 
                                else:
                                    if len(std_find)>0:
                                        std_flag="Yes"
                                    if len(legal_find)>0:
                                        legal_flag="Yes"
                                    json_make["standardComposition"]=std_flag
                                    json_make["legalComposition"]=legal_flag
                            json_make["test_Description"]=""
                            json_make["filename"]=datastr.get("file_name",config.hypen_delimiter)
                            if file_path !='':
                                path=config.blob_file_path+file_path.replace("/dbfs/mnt/","")+config.sas_token
                            else:
                                path=''
                            json_make["file_Path"]=path
                            extract_field={}
                            extract_field["study_Title"]=datastr.get("Study Title",config.hypen_delimiter)
                            extract_field["final_Report"]=datastr.get("Issue Date",config.hypen_delimiter)
                            json_make["extract_Field"]=extract_field
                            json_list.append(json_make)
                        elif sub_category=="Monthly Toxicology Study List" :
                            json_make["product_Commercial_Name"]=product
                            json_make["date"]=datastr.get("date",config.hypen_delimiter)
                            if category=="tox_study_silanes":
                                studies=datastr.get("Studies",config.hypen_delimiter)
                                status=datastr.get("Status",config.hypen_delimiter)
                                comments=datastr.get("Comments",config.hypen_delimiter)
                                if studies !=None or status !=None or comments !=None:
                                    json_make["studies"]=studies
                                    json_make["status"]=status
                                    json_make["comments"]=comments
                                    json_make["test"]=config.hypen_delimiter
                                    json_make["actions"]=config.hypen_delimiter
                                    json_make["segment"]="Silanes"
                                else:
                                    continue       
                            elif category=="tox_study_selant":
                                json_make["test"]=datastr.get("Test",config.hypen_delimiter)
                                json_make["actions"]=datastr.get("Actions",config.hypen_delimiter)
                                json_make["studies"]=config.hypen_delimiter
                                json_make["status"]=config.hypen_delimiter
                                json_make["comments"]=config.hypen_delimiter
                                json_make["segment"]="Sealant"
                            json_list.append(json_make)                  
                        # elif sub_category=="Monthly Toxicology Study List" and category=="tox_study_selant":
                        #     json_make["test"]=datastr.get("Test",config.hypen_delimiter)
                        #     json_make["actions"]=datastr.get("Actions",config.hypen_delimiter)
                        #     json_make["date"]=datastr.get("date",config.hypen_delimiter)
                        #     selant.append(json_make)
                        elif sub_category=="Toxicology Summary":
                            if product_type in ["NUMCAS"]:
                                specid_list=spec_id.split(config.pipe_delimitter)
                                if "CAS" in list(std_df.columns) and "SUBID" in list(std_df.columns):
                                    std_find=std_df[(std_df["CAS"]==product) & (std_df["SUBID"].isin(specid_list))]
                                if len(std_find)==0:
                                    continue 
                                else:
                                    if len(std_find)>0 and "CVALU" in list(std_find.columns):
                                        std_cvalue=std_find[["CVALU","CUNIT"]]
                                        std_cvalu_list=std_cvalue.values.tolist()
                                        for value,unit in std_cvalu_list:
                                            cal_value=helper.calculate_ppm_ppb(value,unit)
                                            if cal_value>30:
                                                std_flag="Yes"
                                                json_make["standardComposition"]=std_flag
                                                json_make["compositionValue"]=value
                                                json_make["compositionUnit"]=unit
                                        if std_flag!='Yes':
                                            continue
                            json_make["date_Of_Issue"]=datastr.get("Date",config.hypen_delimiter)
                            path=config.blob_file_path+file_path.replace("/dbfs/mnt/","")+config.sas_token
                            json_make["filename"]=datastr.get("file_name",config.hypen_delimiter)
                            json_make["file_Path"]=path
                            json_list.append(json_make)
                    except Exception as e:
                        pass
            if sub_category=="Monthly Toxicology Study List":
                monthly_studies={}
                # if len(selant)>0:
                #     selant=sort_date(selant)
                # if len(silanes):
                #     silanes=sort_date(silanes) 
                if len(json_list) >0:
                    json_list=sort_date(json_list) 
                # monthly_studies["selant"]=selant
                # monthly_studies["silanes"]=silanes
                # json_list.append(monthly_studies)                    
        elif sub_category=="Toxicology Registration Tracker":
            query=f'*:*'
            tracker_values,tracker_df=helper.get_data_from_core(config.solr_registration_tracker,query)
            if len(tracker_values)>0:
                for data in tracker_values:
                    try:
                        json_make={}
                        json_make["product_Name"]=data.get("PRODUCTNAME",config.hypen_delimiter)
                        json_make["country_Name"]=data.get("COUNTRYNAME",config.hypen_delimiter)
                        json_make["tonnage_Band"]=data.get("TONNAGEBAND",config.hypen_delimiter)
                        json_make["study_Type"]=data.get("STUDYTYPE",config.hypen_delimiter)
                        json_make["test_Method"]=data.get("TESTMETHOD",config.hypen_delimiter)
                        json_make["test_Name"]=data.get("TESTNAME",config.hypen_delimiter)
                        json_make["estimated_Timing"]=data.get("ESTIMATEDTIMING",config.hypen_delimiter)
                        json_make["estimated_Cost"]=data.get("ESTIMATEDCOST",config.hypen_delimiter)
                        json_make["new_Estimates"]=data.get("NEWESTIMATES",config.hypen_delimiter)
                        json_list.append(json_make)
                    except Exception as e:
                        pass   
        return json_list
    except Exception as e:
        return []

def sort_date(values):
    try:
        if len(values)>0:
            # #sort desceding order
            result = json.dumps(values)
            df=pd.read_json(result,dtype=str)
            df['Date'] =pd.to_datetime(df['date'])
            sorted_df=df.sort_values(by=['Date'],ascending=False)  
            sorted_df=sorted_df.replace({"Nan":"-","nan":"-"})
            sorted_dict=json.loads(sorted_df.to_json(orient='index'))
            json_list=[]
            for item in sorted_dict:
                json_list.append(sorted_dict.get(item))
        return json_list
    except Exception as e:
        return []