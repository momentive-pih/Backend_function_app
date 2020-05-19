import logging
import json
import azure.functions as func
from . import get_spec_list
import pandas as pd
# from postAllProducts import views
import os 
from __app__.postselectedProducts import views
from __app__.shared_code import settings as config
from __app__.shared_code import helper


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postHomePageData function processing a request.')
        result=[]
        req_body = req.get_json()
        if len(req_body)>0:
            req_body_content= req_body[0]
            logging.info("home_page"+f'{req_body_content}')
            if "name" in req_body_content:
                basic_details=views.selected_products(req_body,"No")
                arranged_level_json = rearrange_json(basic_details)
                all_details_json,spec_list,material_list = helper.construct_common_level_json(arranged_level_json,"home_page")
                home_page_data=home_page_details(all_details_json,spec_list,arranged_level_json)
                result = json.dumps(home_page_data)
            else:
                all_details_json,spec_list,material_list = helper.construct_common_level_json(req_body_content)
                home_page_data=home_page_details(all_details_json,spec_list,req_body_content)
                result = json.dumps(home_page_data)   
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

def rearrange_json(basic_details):
    basic_data=basic_details.get("basic_properties")
    spec_json=basic_details.get("selected_spec_list")
    if len(basic_data)>0:
        common_json={}
        common_json["Spec_id"]=[]
        common_json["Spec_id"].append(spec_json[0])
        common_json["product_Level"]=basic_data[0].get("product_Level")
        common_json["Mat_Level"]=basic_data[0].get("material_Level")
        common_json["CAS_Level"]=basic_data[0].get("cas_Level")
        return common_json

def home_page_details(all_details_json,spec_list,arranged_level_json):
    try:
        category=["EU_REG_STATUS","US_REG_STATUS","LATAM_REG_STATUS","US-FDA","EU-FDA","GADSL","CAL-PROP","SAP-BW","Heavy metals","Toxicology","Toxicology-summary"]
        home_page_details={}
        product_attributes=[]
        product_compliance=[]
        customer_comm=[]
        toxicology=[]
        restricted_sub=[]
        sales_information=[]
        report_data=[]
        #unstrucure details
        home_page_query=helper.unstructure_template(all_details_json,category)
        params={"fl":config.unstructure_column_str}
        unstructure_values,unstructure_df=helper.get_data_from_core(config.solr_unstructure_data,home_page_query,params)
        if "CATEGORY" in list(unstructure_df.columns):
            founded_category=list(unstructure_df["CATEGORY"].unique())
        else:
            founded_category=[]
        # product and material - info     
        mat_str=''
        mat_str_list=[]
        product_list=[]
        for item in spec_list:
            for matid in arranged_level_json.get("Mat_Level"):
                try:
                    mat_spec_id=matid.get("real_Spec_Id")
                    if type(mat_spec_id)==str and (item in mat_spec_id):
                        mat_str_list.append(matid.get("bdt",config.hypen_delimiter)+(config.pipe_delimitter)+matid.get("material_Number",config.hypen_delimiter)+(config.pipe_delimitter)+matid.get("description",config.hypen_delimiter))
                    elif type(mat_spec_id)==list:
                        for inside_mat in mat_spec_id:
                            if item in inside_mat:
                                mat_str_list.append(matid.get("bdt",config.hypen_delimiter)+(config.pipe_delimitter)+matid.get("material_Number",config.hypen_delimiter)+(config.pipe_delimitter)+matid.get("description",config.hypen_delimiter))
                                break
                except Exception as e:
                    pass
            nam_list=all_details_json.get(item).get("namprod",[])
            if len(nam_list)>0:
                nam_str=(config.comma_delimiter).join(nam_list)
                product_list.append(item+config.hypen_delimiter+nam_str)
            else:
                product_list.append(item)
        if len(mat_str_list)>0 and len(mat_str_list)>3:
            mat_str_list=mat_str_list[:3]
            mat_str=(config.comma_delimiter).join(mat_str_list)+" and more.." 
        elif len(mat_str_list)>0:
            mat_str=(config.comma_delimiter).join(mat_str_list)
        else:
            mat_str=(config.hypen_delimiter)    
        if len(product_list)>3:
            product_list=product_list[:3]
        product_attributes.append({"image":config.home_icon_product_attributes})   
        product_attributes.append({"Product Identification": (config.comma_delimiter).join(product_list)})
        product_attributes.append({"Material Information":mat_str})
        product_attributes.append({"tab_modal": "compositionModal"})
        home_page_details["Product Attributes"]=product_attributes

        #product compliance
        negative_str=config.hypen_delimiter
        positive_str=config.hypen_delimiter
        spec_query=(config.or_delimiter).join(spec_list)
        query=f'SUBID:({spec_query})'
        params={"fl":config.notification_column_str}
        pcomp,pcomp_df=helper.get_data_from_core(config.solr_notification_status,query,params)
        if ("NOTIF" in list(pcomp_df.columns)) and len(pcomp)>0:
            phrase_key=(list(pcomp_df["NOTIF"].unique()))
            phrase_split=";".join(phrase_key)
            phrase_key=phrase_split.split(";")
            phrase_key_query=helper.replace_character_for_querying(phrase_key)
            query=f'PHRKY:({phrase_key_query})'
            params={"fl":config.phrase_column_str}
            key_value,key_value_df=helper.get_data_from_core(config.solr_phrase_translation,query,params)
            key_compare=key_value_df.values.tolist()
            negative_country=[]
            positive_country=[]
            others=[]
            for item in pcomp:
                try:
                    place=item.get("RLIST",config.hypen_delimiter)
                    key = str(item.get("NOTIF","")).strip()
                    key_text=helper.finding_phrase_text(key_value_df,key)
                    if key_text.lower().strip() in config.in_compliance_notification_status:
                        positive_country.append(place)
                    elif key_text.lower().strip() in config.not_in_compliance_notification_status:
                        negative_country.append(place)
                    # if ("y" in key_text.lower() and "positive" in key_text.lower()):
                    #     positive_country.append(place)
                    # elif ("n" in key_text.lower() and "negative" in key_text.lower()):
                    #     negative_country.append(place)
                    else:
                        others.append(place)
                except Exception as e:
                    pass
            # if len(negative_country)>4:
            #     negative_country=negative_country[:4]
            # if len(positive_country)>4:
            #     positive_country=positive_country[:4]
            # negative_str=(config.comma_delimiter).join(negative_country)
            # positive_str=(config.comma_delimiter).join(positive_country)
        product_compliance.append({"image":config.home_icon_product_compliance})
        product_compliance.append({"In Compliance Regulatory notification count":len(positive_country)})
        product_compliance.append({"Not in Compliance Regulatory notification count":len(negative_country)}) 
        product_compliance.append({"Other Regulatory notification count":len(others)}) 
        product_compliance.append({"tab_modal": "complianceModal"})          
        
        #ag registartion
        active_region=[]
        inactive_region=[]
        active_str="No active region found"
        for region in config.ag_registration_country:
            if region in founded_category:
                active_region.append(config.ag_registration_country.get(region))
            else:
                inactive_region.append(config.ag_registration_country.get(region))
        if len(active_region)>0:
            active_str=(config.comma_delimiter).join(active_region)
        product_compliance.append({"AG Registration active region status ":active_str})
        # product_compliance.append({"AG Registration inactive region status ":(config.comma_delimiter).join(inactive_region)})
        home_page_details["Product compliance"]=product_compliance
        
        #customer communication
        usflag="No letter found"
        euflag="No letter found"
        for data in config.us_eu_category:
            if (data in founded_category) and data=="US-FDA":
               usflag="Yes" 
            if (data in founded_category) and data=="EU-FDA":
               euflag="Yes"         
        customer_comm.append({"image": config.home_icon_customer_communication})
        customer_comm.append({"US FDA Compliance" : usflag})
        customer_comm.append({"EU Food Contact " : euflag})
        # customer_comm.append({"Top 3 Heavy Metal compositions":""})
        customer_comm.append({"tab_modal": "communicationModal"})
        home_page_details["Customer Communication"]=customer_comm

        #toxicology
        summary_flag="No"
        study_title=[]
        if "Toxicology-summary" in founded_category:
            summary_flag="Yes"
        for item in unstructure_values:
            try:
                if item.get("CATEGORY")=="Toxicology":
                    toxic_data=json.loads(item.get("DATA_EXTRACT",""))
                    study_str=toxic_data.get("Study Title","")
                    if study_str !="":
                        study_title.append(study_str)
            except Exception as e:
                pass
        if len(study_title)>3:
            study_title=study_title[:3]
        if len(study_title)>0:
            study_str=(config.comma_delimiter).join(study_title)
        else:
            study_str=config.hypen_delimiter
        toxicology.append({ "image" : config.home_icon_toxicology})
        toxicology.append({"Study Titles" : study_str})
        toxicology.append({"Toxicology Summary Report Available":summary_flag})
        # toxicology.append({"Pending Monthly Tox Studies": ""})
        toxicology.append({ "tab_modal": "toxicologyModal"})
        home_page_details["Toxicology"]=toxicology
        #restricted_sub
        gadsl_fg='No'
        cal_fg="No"
        for rest in config.restricted_sub_list:
            try:
                if (rest in founded_category) and rest=="GADSL":
                    gadsl_fg="Yes"
                if (rest in founded_category) and rest=="CAL-PROP":
                    cal_fg="Yes"
                if gadsl_fg=="Yes" and cal_fg=="Yes":
                    break
            except Exception as e:
                pass
        restricted_sub.append({"image": config.home_icon_restricted_substance})
        restricted_sub.append({"Components Present in GADSL": gadsl_fg})
        restricted_sub.append({"Components Present in Cal Prop 65":cal_fg})
        restricted_sub.append({"tab_modal": "restrictedSubstanceModal" })
        home_page_details["Restricted Substance"]=restricted_sub

        #sales_information
        sales_kg=0
        sales_country=[]
        sold_country=config.hypen_delimiter
        if "SAP-BW" in founded_category:
            for item in unstructure_values:
                try:
                    if item.get("CATEGORY")=="SAP-BW":
                        sap_data=json.loads(item.get("DATA_EXTRACT",""))
                        sold_str=sap_data.get("Sold-to Customer Country","")
                        if sold_str !="":
                            sales_country.append(sold_str)
                        year_2019=str(sap_data.get('Fiscal year/period',"-")).split(".")
                        if len(year_2019)>0 and year_2019[1]=="2019":
                            sales_kg=sales_kg+float(sap_data.get("SALES KG"))   
                except Exception as e:
                    pass            
        sales_country=list(set(sales_country))
        if len(sales_country)<5 and len(sales_country)>0:
            sold_country=", ".join(sales_country)
        elif len(sales_country)>4:
            sold_country=", ".join(sales_country[0:5])
            sold_country=sold_country+" and more.."
        sales_kg=helper.set_decimal_points(sales_kg)
        sales_kg=str(sales_kg)
        sales_information.append({"image":config.home_icon_sales_info})       
        sales_information.append({"Total sales in 2019(Kg)" :sales_kg})
        sales_information.append({"Regions where sold" :sold_country})
        sales_information.append({"tab_modal": "salesModal"})
        home_page_details["Sales Information"]=sales_information

        #report data
        report_flag='No'
        spec_list_query=(config.or_delimiter).join(spec_list)
        params={"fl":config.report_column_str}
        core=config.solr_document_variant
        query=f'SUBID:({spec_list_query})'
        report_values,report_df=helper.get_data_from_core(core,query,params)
        if len(report_values)>0:
            report_flag='Yes'
        report_data.append({ "image":config.home_icon_report_data})
        report_data.append({"Report Status" :report_flag})
        report_data.append({"tab_modal": "reportModal" })
        home_page_details["Report Data"]=report_data

    except Exception as e:
        logging.error(str(e))
    return home_page_details



    
