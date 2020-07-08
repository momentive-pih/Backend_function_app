import logging
import azure.functions as func
import pandas as pd
import os 
import pysolr
import json 
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
        sub_category=req_body.get("Category_details").get("Subcategory")
        json_list=[]
        if sub_category in config.toxicology_category:
            all_details_json,spec_list,material_list = helper.construct_common_level_json(req_body)
            std,std_df,legal,legal_df = helper.make_common_query_for_std_legal_composition(all_details_json)
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
                        extract_data= item.get("DATA_EXTRACT","")  
                        import json
                        datastr=json.loads(extract_data)
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
                if len(json_list) >0:
                    json_list=sort_date(json_list)                
        elif sub_category=="Toxicology Registration Tracker":
            if ("tonnage_band") not in req_body and ("product" not in req_body) and ("country" not in req_body):
                product_list=[]
                tonnage_band_list=[]
                country_list=[]
                product_list=get_product_list()
                country_list=get_country_list()
                tonnage_band_list=get_tonnage_band_list()
                json={
                    "product":product_list,
                    "country":country_list,
                    "tonnage_band":tonnage_band_list
                }
                json_list.append(json)
            else:
                tonnage_band_limit=req_body.get("tonnage_band","ALL")
                product_name=req_body.get("product").get("name","")
                country=req_body.get("country",[])
                tracker_query=config.registration_tracker_query.format(tonnage_band_limit,product_name)
                group_test_query=config.select_query.format(config.view_connector,config.group_test_view_name)
                #get data from sql 
                tracker_sql_df,tracker_sql_list=helper.get_data_from_sql_table(tracker_query,"yes")
                group_test_df,group_test_list=helper.get_data_from_sql_table(group_test_query,"yes")
                if len(tracker_sql_df)>0:
                    # country_cost=get_country_subtotal(tracker_sql_df)
                    #find total cost 
                    if len(country)>0 and country[0] != "ALL":
                        tracker_sql_df["CountryName"]=tracker_sql_df["CountryName"].str.strip()
                        tracker_sql_df=tracker_sql_df[tracker_sql_df["CountryName"].isin(country)]
                    
                    if "EstimatedCost" in tracker_sql_df.columns and "Completed" in tracker_sql_df.columns:
                        cost_df=tracker_sql_df[tracker_sql_df["Completed"].str.contains("no",case=False)]
                        total_cost=cost_df["EstimatedCost"].sum()
                    else:
                        total_cost=0
                    study_type_cost=get_study_type_subtotal(tracker_sql_df)
                    total_estimated_time=get_estimated_time(tracker_sql_df,group_test_df)       
                    registartion_tracker_list=get_json_format_data(tracker_sql_list,country)
                    json_make={
                        "total_Cost":str(helper.set_two_decimal_points(total_cost)),
                        "study_Type_Cost":study_type_cost,
                        "total_Estimated_Time":str(helper.set_two_decimal_points(total_estimated_time)),
                        "registartion_Data":registartion_tracker_list
                    }
                    json_list.append(json_make)
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

def get_country_subtotal(tracker_sql_df):
    try: 
        country_cost={}       
        if ("CountryName" in tracker_sql_df.columns) and ("EstimatedCost" in tracker_sql_df.columns):
            country_list=list(tracker_sql_df["CountryName"].unique())
            for country in country_list:
                country_df=tracker_sql_df[tracker_sql_df["CountryName"]==country]
                total=country_df["EstimatedCost"].sum()
                country_cost[country]=total
        return country_cost
    except Exception as e:
        return {}
    
def get_study_type_subtotal(tracker_sql_df):
    try: 
        study_type_cost={}       
        if ("StudyType" in tracker_sql_df.columns) and ("EstimatedCost" in tracker_sql_df.columns):
            study_list=list(tracker_sql_df["StudyType"].unique())
            for study in study_list:
                study_df=tracker_sql_df[tracker_sql_df["StudyType"]==study]
                total=study_df["EstimatedCost"].sum()
                study_type_cost[study]=str(total)
        return study_type_cost
    except Exception as e:
        return {}

def get_estimated_time(tracker_sql_df,group_test_df):
    try:
        estimated_time=0
        if "TestMethod" in tracker_sql_df.columns and "TestMethod" in group_test_df and "ParallelTestMethod" in group_test_df:        
            if "Completed" in tracker_sql_df.columns:
                timing_tracker_df=tracker_sql_df[tracker_sql_df["Completed"].str.contains("no",case=False)]
                timing_tracker_df=timing_tracker_df.sort_values(by=['TestMethod'],ascending=True)  
                timing_tracker_df["TestMethod"]=timing_tracker_df["TestMethod"].str.strip()
                group_test_df["TestMethod"]=group_test_df["TestMethod"].str.strip()
                group_test_df["ParallelTestMethod"]=group_test_df["ParallelTestMethod"].str.strip()
            if len(timing_tracker_df)>0:
                marked_test=[]
                total_count=0
                org_test_method_list=list(timing_tracker_df["TestMethod"])
                test_method_list=list(timing_tracker_df["TestMethod"].unique())
                test_timing_df=timing_tracker_df[["TestMethod","EstimatedTiming"]]
                tracker_test_method_list=test_timing_df.values.tolist()
                test_time_json={}
                for test in test_method_list:
                    if test not in marked_test:
                        parallel_test_df=group_test_df[group_test_df["TestMethod"]==test]
                        parallel_test_list=list(parallel_test_df["ParallelTestMethod"].unique())
                        parallel_test_list.append(test)
                        total_time=0
                        time_list=[time for item,time in tracker_test_method_list if (item in parallel_test_list) and (item not in marked_test)]
                        # total_time=float(sum(time_list))
                        total_time=sum(time_list)
                        average_time=float(total_time/len(time_list))
                        estimated_time+=(org_test_method_list.count(test))*average_time
                        marked_test+=parallel_test_list
                        marked_test=list(set(marked_test))
                        
    except Exception as e:
        pass
    return estimated_time

def get_json_format_data(tracker_values,country):
    try:
        json_list=[]
        for data in tracker_values:
            try:
                json_make={}
                if data.get("CountryName",config.hypen_delimiter).strip() not in country and country[0] != "ALL":
                    continue
                json_make["country_Name"]=data.get("CountryName",config.hypen_delimiter)
                json_make["tonnage_Band"]=data.get("TonnageBand",config.hypen_delimiter)
                json_make["study_Type"]=data.get("StudyType",config.hypen_delimiter)
                json_make["test_Method"]=data.get("TestMethod",config.hypen_delimiter)
                json_make["test_Name"]=data.get("TestName",config.hypen_delimiter)
                json_make["estimated_Timing"]=str(data.get("EstimatedTiming",config.hypen_delimiter))
                json_make["estimated_Cost"]=str(data.get("EstimatedCost",config.hypen_delimiter))
                json_make["completed"]=data.get("Completed",config.hypen_delimiter)
                json_list.append(json_make)
            except Exception as e:
                pass 
    except Exception as e:
        pass 
    return json_list

def get_product_list():
    try:
        json_list=[]
        query=config.select_query.format(config.table_connector,config.tracker_product_table)
        df=helper.get_data_from_sql_table(query)
        if "ProductName" in df.columns:
            for item in list(df["ProductName"].unique()):
                json={}
                json["name"]=str(item).strip()
                json_list.append(json)
    except Exception as e:
        pass
    return json_list

def get_country_list():
    try:
        json_list=[]
        query=config.select_query.format(config.table_connector,config.country_table)
        df=helper.get_data_from_sql_table(query)
        json_list.append({"name":"ALL"})
        if "CountryName" in df.columns:
            for item in list(df["CountryName"].unique()):
                json={}
                json["name"]=str(item).strip()
                json_list.append(json)
    except Exception as e:
        pass
    return json_list

def get_tonnage_band_list():
    try:
        json_list=[]
        query=config.select_query.format(config.table_connector,config.tonnage_band_table)
        df=helper.get_data_from_sql_table(query)
        if ("TonnageBand" in df.columns) and ("TonnageBandBucket" in df.columns):
            band_df=df[["TonnageBand","TonnageBandBucket"]].drop_duplicates()
            band_list=band_df.values.tolist()
            json_list.append({"name":"ALL","band_Limit":"ALL"})
            for item,limit in band_list:
                json={}
                json["name"]=str(item).strip()
                json["band_Limit"]=limit
                json_list.append(json)
    except Exception as e:
        pass
    return json_list