import json
import re
import pandas as pd
import pysolr
from __app__.shared_code import settings as config
from __app__.shared_code import helper
solr_product=config.solr_product
solr_notification_status=config.solr_notification_status
solr_unstructure_data=config.solr_unstructure_data
solr_document_variant=config.solr_document_variant
junk_column=config.junk_column
product_column=config.product_column
product_nam_category=config.product_nam_category
product_rspec_category = config.product_rspec_category
product_namsyn_category = config.product_namsyn_category
material_number_category = config.material_number_category
material_bdt_category = config.material_bdt_category
cas_number_category = config.cas_number_category
cas_pspec_category = config.cas_pspec_category
cas_chemical_category = config.cas_chemical_category
category_with_key=config.category_with_key
category_type = config.category_type
search_category = config.search_category
selected_categories=config.selected_categories
querying_solr_data=helper.querying_solr_data
product_level_creation=helper.product_level_creation
solr_product_params=config.solr_product_params
replace_character_for_querying=helper.replace_character_for_querying
pipe_delimitter=config.pipe_delimitter
finding_cas_details_using_real_specid=helper.finding_cas_details_using_real_specid
finding_product_details_using_real_specid=helper.finding_product_details_using_real_specid
finding_material_details_using_real_specid=helper.finding_material_details_using_real_specid

def selected_products(data_json,searched_product_flag="yes"):
    try:
        searched_product_list=[]
        selected_spec_list=[]
        properties={}
        count=0
        params=solr_product_params
        product_count=0
        material_count=0
        cas_count=0
        column_add=[]
        product_level_flag=''
        material_level_flag=''
        cas_level_flag=''
        add_df=pd.DataFrame()
        material_df=pd.DataFrame()
        cas_df=pd.DataFrame()
        prod_df=pd.DataFrame()
        return_data={}    
        for item in data_json:
            search_value = item.get("name")
            search_value_split = search_value.split(" | ")
            search_column = item.get("type")
            search_key = item.get("key")
            search_column_split = search_column.split(" | ")
            search_group = item.get("group").split("(")
            search_group = search_group[0].strip()
            column_add.append(search_column)
            count+=1
            if search_group == "PRODUCT-LEVEL":
                product_level_flag = 's'
                product_count = count
                product_rspec = search_value_split[search_column_split.index("REAL-SPECID")]
                product_name = search_value_split[search_column_split.index("NAM PROD")]
                product_synonyms = search_value_split[search_column_split.index("SYNONYMS")]
                product_level_json={"real_Spec_Id":product_rspec,"namprod":product_name,"synonyms":product_synonyms}                     
            if search_group == "MATERIAL-LEVEL":
                material_level_flag = 's'
                material_count = count
                material_number = search_value_split[search_column_split.index("MATERIAL NUMBER")]
                #add proceeding zeros
                if len(material_number)!=18:
                    material_number = helper.add_proceeding_zeros(material_number)
                material_bdt = search_value_split[search_column_split.index("BDT")]
                material_description = search_value_split[search_column_split.index("DESCRIPTION")]
                material_level_json = {"material_Number":material_number,"bdt":material_bdt,"description":material_description}
            if search_group == "CAS-LEVEL":
                cas_level_flag = 's'
                cas_count = count
                cas_pspec = search_value_split[search_column_split.index("PURE-SPECID")]  
                cas_number = search_value_split[search_column_split.index("CAS NUMBER")]
                cas_chemical = search_value_split[search_column_split.index("CHEMICAL-NAME")]  
                cas_level_json = {"pure_Spec_Id":cas_pspec,"cas_Number":cas_number,"chemical_Name":cas_chemical}
        if len(data_json)<=2:                              
            if product_level_flag=='s' and product_count==1:
                real_spec_list=[product_rspec]
                if material_level_flag=='' and cas_level_flag=='':                     
                    #to find material level details
                    material_df=finding_material_details_using_real_specid(real_spec_list,params)
                    material_df=material_df.sort_values(by=['TEXT1']) 
                    org_mat_df=material_df.copy()
                    if searched_product_flag=="yes":
                        searched_product_list=searched_product_list+product_level_creation(org_mat_df,material_number_category,"","","MAT*","MATERIAL-LEVEL","yes")
                    #to find cas level details
                    cas_df,spec_rel_list=finding_cas_details_using_real_specid(real_spec_list,params)
                    cas_df=cas_df.sort_values(by=['TEXT2'])  
                    if searched_product_flag=="yes":
                        #Keeping NUMCAS sub category as only pure sub as per client reqeust for search engine
                        searched_product_list=searched_product_list+product_level_creation(cas_df,cas_number_category,"NUMCAS","PURE_SUB","CAS*","CAS-LEVEL")
                        # searched_product_list=searched_product_list+product_level_creation(cas_df,cas_number_category,"","","CAS*","CAS-LEVEL","yes")
                    properties,selected_spec_list=basic_properties("product_level","","",product_level_json,material_df,cas_df,spec_rel_list,real_spec_list)

                elif material_level_flag=='s' and material_count==2 and cas_level_flag=='':
                    #to find cas level details
                    cas_df,spec_rel_list=finding_cas_details_using_real_specid(real_spec_list,params)
                    cas_df=cas_df.sort_values(by=['TEXT2'])  
                    if searched_product_flag=="yes":
                         searched_product_list=searched_product_list+product_level_creation(cas_df,cas_number_category,"NUMCAS","PURE_SUB","CAS*","CAS-LEVEL")                       
                        # searched_product_list=searched_product_list+product_level_creation(cas_df,cas_number_category,"","","CAS*","CAS-LEVEL","yes")
                    properties,selected_spec_list=basic_properties("product_level","material_level","",product_level_json,material_level_json,cas_df,spec_rel_list,real_spec_list)

                elif cas_level_flag=='s' and cas_count==2 and material_level_flag=='':
                    #to find material level details
                    material_df=finding_material_details_using_real_specid(real_spec_list,params)
                    material_df=material_df.sort_values(by=['TEXT1'])   
                    org_mat_df=material_df.copy()   
                    if searched_product_flag=="yes":
                        searched_product_list=searched_product_list+product_level_creation(org_mat_df,material_number_category,"","","MAT*","MATERIAL-LEVEL","yes")
                    properties,selected_spec_list=basic_properties("product_level","","cas_level",product_level_json,material_df,cas_level_json,"",real_spec_list)
            
            elif material_level_flag =='s' and material_count==1:
                #finding real spec id
                query=f'TYPE:MATNBR && TEXT1:{material_number} && -TEXT6:X'
                temp_df=querying_solr_data(query,params)
                real_spec_list = list(temp_df["TEXT2"].unique())
                if product_level_flag =='' and cas_level_flag=='':
                    #find product details
                    prod_df=finding_product_details_using_real_specid(real_spec_list,params)
                    prod_df=prod_df.sort_values(by=['TEXT2'])             
                    if searched_product_flag=="yes":
                        searched_product_list=searched_product_list+product_level_creation(prod_df,product_rspec_category,"","","RSPEC*","PRODUCT-LEVEL","yes")                          
                    #cas level details
                    cas_df,spec_rel_list=finding_cas_details_using_real_specid(real_spec_list,params)
                    cas_df=cas_df.sort_values(by=['TEXT2'])  
                    if searched_product_flag=="yes":
                        searched_product_list=searched_product_list+product_level_creation(cas_df,cas_number_category,"NUMCAS","PURE_SUB","CAS*","CAS-LEVEL")
                        # searched_product_list=searched_product_list+product_level_creation(cas_df,cas_number_category,"","","CAS*","CAS-LEVEL","yes")
                    properties,selected_spec_list=basic_properties("","material_level","",prod_df,material_level_json,cas_df,spec_rel_list,real_spec_list)

                elif product_level_flag =='s' and product_count ==2 and cas_level_flag=='':
                    real_spec_list = [product_rspec]
                    #cas level details
                    cas_df,spec_rel_list=finding_cas_details_using_real_specid(real_spec_list,params)
                    cas_df=cas_df.sort_values(by=['TEXT2'])  
                    if searched_product_flag=="yes":
                        searched_product_list=searched_product_list+product_level_creation(cas_df,cas_number_category,"NUMCAS","PURE_SUB","CAS*","CAS-LEVEL")  
                        # searched_product_list=searched_product_list+product_level_creation(cas_df,cas_number_category,"","","CAS*","CAS-LEVEL","yes")
                    properties,selected_spec_list=basic_properties("product_level","material_level","",product_level_json,material_level_json,cas_df,spec_rel_list,real_spec_list)  
                elif cas_level_flag=='s' and cas_count==2 and product_level_flag=='':
                    #find product details
                    prod_df=finding_product_details_using_real_specid(real_spec_list,params)  
                    prod_df=prod_df.sort_values(by=['TEXT2'])           
                    if searched_product_flag=="yes":
                        searched_product_list=searched_product_list+product_level_creation(prod_df,product_rspec_category,"","","RSPEC*","PRODUCT-LEVEL","yes")                          
                    properties,selected_spec_list=basic_properties("","material_level","cas_level",prod_df,material_level_json,cas_level_json,spec_rel_list,real_spec_list)

            elif cas_level_flag=='s' and cas_count==1:
                #finding real spec id
                query=f'TYPE:SUBIDREL && TEXT1:{cas_pspec} && SUBCT:REAL_SUB && -TEXT6:X'
                temp_df=querying_solr_data(query,params)
                spec_rel_list=temp_df[["TEXT1","TEXT2"]].values.tolist()
                real_spec_list = list(temp_df["TEXT2"].unique())
                if product_level_flag =='' and material_level_flag=='':
                    #find product details
                    prod_df=finding_product_details_using_real_specid(real_spec_list,params)           
                    #same pure-spec will be act as real-spec
                    # query=f'TYPE:NAMPROD && TEXT2:{cas_pspec} && SUBCT:PURE_SUB'
                    # pure_real_df=querying_solr_data(query,params)
                    # prod_df=pd.concat([prod_df,pure_real_df])
                    prod_df=prod_df.sort_values(by=['TEXT2'])  
                    if searched_product_flag=="yes":
                        searched_product_list=searched_product_list+product_level_creation(prod_df,product_rspec_category,"","","RSPEC*","PRODUCT-LEVEL","yes")
                    #to find material level details
                    material_df=finding_material_details_using_real_specid(real_spec_list,params)
                    material_df=material_df.sort_values(by=['TEXT1'])  
                    org_mat_df=material_df.copy()
                    if searched_product_flag=="yes":
                        searched_product_list=searched_product_list+product_level_creation(org_mat_df,material_number_category,"","","MAT*","MATERIAL-LEVEL","yes")
                    properties,selected_spec_list=basic_properties("","","cas_level",prod_df,material_df,cas_level_json,spec_rel_list,real_spec_list)

                elif product_level_flag =='s' and product_count ==2 and material_level_flag=='':
                    #to find material level details
                    real_spec_list=[product_rspec]
                    material_df=finding_material_details_using_real_specid(real_spec_list,params)
                    material_df=material_df.sort_values(by=['TEXT1'])  
                    org_mat_df=material_df.copy()
                    if searched_product_flag=="yes":
                        searched_product_list=searched_product_list+product_level_creation(org_mat_df,material_number_category,"","","MAT*","MATERIAL-LEVEL","yes")
                    properties,selected_spec_list=basic_properties("product_level","","cas_level",product_level_json,material_df,cas_level_json,spec_rel_list,real_spec_list)

                elif material_level_flag=='s' and material_count==2 and product_level_flag=='':
                    #find product details
                    prod_df=finding_product_details_using_real_specid(real_spec_list,params)
                    prod_df=prod_df.sort_values(by=['TEXT2'])  
                    if searched_product_flag=="yes":
                        searched_product_list=searched_product_list+product_level_creation(prod_df,product_rspec_category,"","","RSPEC*","PRODUCT-LEVEL","yes")                                     
                    properties,selected_spec_list=basic_properties("","material_level","cas_level",prod_df,material_level_json,cas_level_json,spec_rel_list,real_spec_list)
        else:
            searched_product_list=[]
            properties,selected_spec_list=basic_properties("product_level","material_level","cas_level",product_level_json,material_level_json,cas_level_json,[],[product_rspec])
        return_data["search_List"]=searched_product_list
        return_data["basic_properties"]=[properties]
        return_data["selected_spec_list"]=selected_spec_list
        return return_data
    except Exception as e:
        return return_data

def basic_properties(p_flag,m_flag,c_flag,product_info,material_info,cas_info,spec_rel_list=[],real_spec_list=[]):
    try:
        result={}
        json_make={}
        json_list=[]
        active_material=0
        spec_nam_json={}
        selected_spec_list=[]
        # spec_active_mat_json={}
        if p_flag=="product_level":
            specid=product_info.get("real_Spec_Id")
            namprod=product_info.get("namprod")
            spec_nam_json[specid]=[]
            spec_nam_json[specid].append(namprod)
            result["product_Level"]=[product_info]
        else:
            columns=["TEXT1","TEXT2","TEXT3"]
            product_info=product_info[columns]
            product_info=product_info.drop_duplicates()
            product_info=product_info.fillna("-")
            product_result=product_info.values.tolist()
            for namprod,spec,syn in product_result:
                try:
                    json_make["real_Spec_Id"]=spec
                    json_make["namprod"]=namprod
                    json_make["synonyms"]=syn
                    json_list.append(json_make)
                    if spec_nam_json.get(spec) == None:
                        spec_nam_json[spec]=[]
                        spec_nam_json[spec].append(namprod)
                    else:
                        nam_list=spec_nam_json.get(spec)
                        nam_list.append(namprod)
                        spec_nam_json[spec]=list(set(nam_list))              
                    json_make={}
                except Exception as e:
                    pass
            result["product_Level"]=json_list

            #sorting specid on ascending
            temp_json={}
            for key in sorted(spec_nam_json.keys()):
                temp_json[key]=spec_nam_json.get(key)
            spec_nam_json=temp_json
            json_list=[]    


        def spec_id_namprod_combination(real,spec_nam_list,flag,flag_json):              
            for nam in spec_nam_json.get(real):
                if flag=="material":
                    json_make={
                        "material_Number":flag_json.get("material_Number"),
                        "description":flag_json.get("description"),
                        "bdt":flag_json.get("bdt")
                    }   
                else:
                    json_make={
                        "pure_Spec_Id":flag_json.get("pure_Spec_Id"),
                        "cas_Number":flag_json.get("cas_Number"),
                        "chemical_Name":flag_json.get("chemical_Name")
                    }  
                json_make["real_Spec_Id"]=real+pipe_delimitter+nam
                spec_nam_list.append(json_make)
                del json_make
            return spec_nam_list

        if m_flag=="material_level":
            # active_material+=1
            spec_nam_list=[]
            for specid in real_spec_list:
                try:
                    if(spec_nam_json.get(specid)):
                        temp_json=material_info
                        new_list=spec_id_namprod_combination(specid,spec_nam_list,"material",temp_json)
                        material_info["spec_Nam_List"]=new_list
                        material_info["real_Spec_Id"]=specid+" - "+(", ".join(spec_nam_json.get(specid))) 
                except Exception as e:
                    pass                   
            result["material_Level"]=[material_info]
        else:
            columns=["TEXT1","TEXT4","TEXT3","TEXT2"]
            material_info=material_info[columns]
            material_info=material_info.drop_duplicates()
            material_info=material_info.fillna("-")
            material_result=material_info.values.tolist()
            for number,desc,bdt,specid in material_result:
                try:
                    spec_nam_list=[]
                    json_make["material_Number"]=number
                    json_make["description"]=desc
                    json_make["bdt"]=bdt  
                    if(spec_nam_json.get(specid)):
                        temp_json=json_make
                        new_list=spec_id_namprod_combination(specid,spec_nam_list,"material",temp_json)
                        json_make["spec_Nam_List"]=new_list
                        json_make["real_Spec_Id"]=specid+" - "+(", ".join(spec_nam_json.get(specid)))  
                    else:
                        json_make={}
                        continue 
                           
                    json_list.append(json_make)
                    desc=desc.strip()
                    json_make={}
                except Exception as e:
                    pass
            result["material_Level"]=json_list
            json_list=[]
        
        if c_flag=="cas_level":
            spec_nam_list=[]
            for real in real_spec_list:
                try:
                    each_spec_nam_list=[]
                    if(spec_nam_json.get(real)):
                        temp_json=cas_info
                        new_list=spec_id_namprod_combination(real,each_spec_nam_list,"cas",temp_json)
                        for item in new_list:
                            spec_nam_list.append(item)
                    else:
                        continue
                except Exception as e:
                    pass
            cas_info["spec_Nam_List"]=spec_nam_list
            result["cas_Level"]=[cas_info]
        else:
            columns=["TEXT2","TEXT1","TEXT3"]
            cas_info=cas_info[columns]
            cas_info=cas_info.drop_duplicates()
            cas_info=cas_info.fillna("-")
            cas_info=cas_info.groupby(['TEXT2','TEXT1'])['TEXT3'].apply(', '.join).reset_index()
            cas_result=cas_info.values.tolist()
            for pspec,cas,chemical in cas_result:
                try:     
                    json_make["pure_Spec_Id"]=pspec
                    json_make["cas_Number"]=cas
                    json_make["chemical_Name"]=chemical   
                    real_spec_list=[real for pure,real in spec_rel_list if pure==pspec]
                    real_spec_list=list(set(real_spec_list))
                    spec_nam_list=[]               
                    for real in real_spec_list:
                        each_spec_nam_list=[]
                        if(spec_nam_json.get(real)):
                            temp_json=json_make
                            new_list=spec_id_namprod_combination(real,each_spec_nam_list,"cas",temp_json)
                            for item in new_list:
                                spec_nam_list.append(item)
                        else:
                            continue
                    json_make["spec_Nam_List"]=spec_nam_list
                    json_list.append(json_make)
                    json_make={}
                except Exception as e:
                    pass
            result["cas_Level"]=json_list
            json_list=[] 
     
        #setting spec list
        count=0
        for item in spec_nam_json:
            for data in spec_nam_json.get(item):
                count+=1
                json_make["id"]=count
                json_make["name"]=item+pipe_delimitter+data
                json_list.append(json_make)
                json_make={}
        selected_spec_list=json_list
        return result,selected_spec_list
    except Exception as e:
        return result,selected_spec_list
