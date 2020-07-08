import pandas as pd
import json
import logging
from . import settings as config
solr_product=config.solr_product
product_column = config.product_column
# unwanted_fields=["obsolete"]

def querying_solr_data(query,params):
    try:
        # logging.info(f'processing querying_solr_data function in helper file')
        df_product_combine=pd.DataFrame()      
        response = solr_product.search(query,**params)
        result = json.dumps(list(response))
        df_product_combine=pd.read_json(result,dtype=str)
        if len(df_product_combine.columns)!=len(product_column):
            dummy=pd.DataFrame([],columns=product_column)
            df_product_combine=pd.concat([df_product_combine,dummy])
        df_product_combine=df_product_combine.fillna("-")   
        df_product_combine=df_product_combine.replace({"nan":"-"})
        return df_product_combine
    except Exception as e:
        # loggin.error(f'error in processing querying_solr_data function {e}')
        return df_product_combine

def intial_search_data(query,check_columns,params={}):
    try:
        # logging.info(f'processing querying_solr_data function in helper file')
        params["rows"]=config.max_rows
        response = solr_product.search(query,**params)
        result = json.dumps(list(response))
        df_product_combine=pd.read_json(result,dtype=str)   
        if len(df_product_combine.columns)!=len(check_columns):
            dummy=pd.DataFrame([],columns=check_columns)
            df_product_combine=pd.concat([df_product_combine,dummy])
        df_product_combine=df_product_combine.fillna("-")   
        df_product_combine=df_product_combine.replace({"nan":"-"})
        return df_product_combine
    except Exception as e:
        return df_product_combine

def get_data_from_core(core,query,params={}):
    try:
        params["rows"]=config.max_rows
        core_df=pd.DataFrame()  
        response = core.search(query,**params)
        data_list=list(response)
        result = json.dumps(data_list)
        core_df=pd.read_json(result,dtype=str)
        core_df=core_df.replace({"nan":"-"})
        return data_list,core_df
    except Exception as e:
        return [],core_df

def namrod_bdt_product_details(req_body):
    try:
        search_data = req_body.get('SearchData')
        data=search_data.lstrip()
        search=replace_character_for_querying([data])
        query=f'(TEXT1:{search}* || TEXT3:{search}*) && -TEXT6:X && TYPE:(NAMPROD || MATNBR) && SUBCT:REAL_SUB'
        product_list,df_product=get_data_from_core(solr_product,query,{"fl":"TYPE, TEXT1, TEXT3"})
        all_product=[]
        if "TEXT1" in df_product.columns:
            df_namprod=df_product[(df_product["TYPE"]=="NAMPROD") & (~df_product["TEXT1"].isin(["-"]))]
            namprod=list(df_namprod["TEXT1"].unique())
            for item in namprod:
                namrow={"name":item,"type":"NAMPROD","key":"NAM*"}
                all_product.append(namrow)
        if "TEXT3" in df_product.columns:
            df_bdt=df_product[(df_product["TYPE"]=="MATNBR") & (~df_product["TEXT3"].isin(["-"]))]
            bdt=list(df_bdt["TEXT3"].unique())
            for item in bdt:
                bdtrow={"name":item,"type":"BDT","key":"BDT*"}
                all_product.append(bdtrow)
        return all_product
    except Exception as e:
        return []

# def namrod_bdt_product_details():
#     try:
#         query=f'(TEXT1:* || TEXT3:*) && -TEXT6:X && TYPE:(NAMPROD || MATNBR) && SUBCT:REAL_SUB'
#         product_list,df_product=get_data_from_core(solr_product,query,{"fl":"TYPE, TEXT1, TEXT3"})
#         all_product=[]
#         if "TEXT1" in df_product.columns:
#             df_namprod=df_product[(df_product["TYPE"]=="NAMPROD") & (~df_product["TEXT1"].isin(["-"]))]
#             namprod=list(df_namprod["TEXT1"].unique())
#             for item in namprod:
#                 namrow={"name":item,"type":"NAMPROD","key":"NAM*"}
#                 all_product.append(namrow)
#         if "TEXT3" in df_product.columns:
#             df_bdt=df_product[(df_product["TYPE"]=="MATNBR") & (~df_product["TEXT3"].isin(["-"]))]
#             bdt=list(df_bdt["TEXT3"].unique())
#             for item in bdt:
#                 bdtrow={"name":item,"type":"BDT","key":"BDT*"}
#                 all_product.append(bdtrow)
#         return all_product
#     except Exception as e:
#         return []

def product_level_creation(product_df,product_category_map,type,subct,key,level_name,filter_flag="no"):
    try:
        json_list=[]
        if filter_flag=="no":
            if type !='' and subct !='':
                if subct == "all":
                    subct=["REAL_SUB","PURE_SUB"]
                else:
                    subct=[subct]
                temp_df=product_df[(product_df["TYPE"]==type) & (product_df["SUBCT"].isin(subct))]
                temp_df=temp_df.drop(columns=["TYPE","SUBCT"])
            else:
                temp_df=product_df[(product_df["TYPE"]==type)]
                temp_df=temp_df.drop(columns=["TYPE"])
        else:
            temp_df=product_df
        if key in ["MAT*","BDT*"]:
            temp_df=remove_proceeding_zeros(temp_df)
        temp_df.drop_duplicates(inplace=True)
        # temp_df=temp_df.replace({"nan":"-"})
        total_count=0
        display_category=''
        json_category=''
        extract_column=[]
        for column,category in product_category_map:
            try:
                extract_column.append(column)               
                col_count=list(temp_df[column].unique())
                if '-' in col_count:
                    col_count = list(filter(('-').__ne__, col_count))
                category_count = len(col_count)
                total_count+=category_count
                display_category+=category+" - "+str(category_count)+" | "
                json_category+= category+" | " 
            except Exception as e:
                pass
        display_category=display_category[:-3] 
        json_category=json_category[:-3]       
        temp_df=temp_df[extract_column].values.tolist()
        for value1,value2,value3 in temp_df:
            value = str(value1).strip() + " | "+str(value2).strip()+" | "+str(value3).strip()
            out_dict={"name":value,"type":json_category,"key":key,"group":level_name+" ("+display_category+")"+" - "+str(total_count) }
            json_list.append(out_dict)
        # #print(json_list)
        return json_list
    except Exception as e:
        return json_list

def replace_character_for_querying(value_list):
    try:
        replace={" ":"\ ","/":"\/","*":"\*","(":"\(",")":"\)",":":"\:","[":"\[","]":"\]"}
        replaced_list=[data.translate(str.maketrans(replace)) for data in value_list if (data!=None and str(data)!='-')]
        replaced_query=" || ".join(replaced_list)
        return replaced_query
    except Exception as e:
        pass

def replace_char_in_url(path):
    try:
        replace={"?":"%3F","#":"%23"}
        return path.translate(str.maketrans(replace))
    except Exception as e:
        return path

def finding_cas_details_using_real_specid(product_rspec,params):
    try:
        product_rspec=" || ".join(product_rspec)
        query=f'TYPE:SUBIDREL && TEXT2:({product_rspec}) && SUBCT:REAL_SUB && -TEXT6:X'
        spec_rel_df=querying_solr_data(query,params) 
        spec_rel_list=spec_rel_df[["TEXT1","TEXT2"]].values.tolist()
        column_value = list(spec_rel_df["TEXT1"].unique())
        spec_query=" || ".join(column_value)
        query=f'TYPE:NUMCAS && SUBCT:(PURE_SUB || REAL_SUB) && TEXT2:({spec_query}) && -TEXT6:X'
        cas_df=querying_solr_data(query,params)                 
        #real spec will act as pure spec componant
        query=f'TYPE:NUMCAS && TEXT2:({product_rspec}) && -TEXT6:X'
        real_pure_spec_df=querying_solr_data(query,params)
        cas_df=pd.concat([cas_df,real_pure_spec_df])
        return cas_df,spec_rel_list
    except Exception as e:
        pass

def finding_product_details_using_real_specid(product_rspec,params):
    try:
        product_rspec=" || ".join(product_rspec)
        query=f'TYPE:NAMPROD && SUBCT:REAL_SUB && TEXT2:({product_rspec}) && -TEXT6:X'
        prod_df=querying_solr_data(query,params)
        return prod_df
    except Exception as e:
        pass

def finding_material_details_using_real_specid(product_rspec,params):
    try:
        product_rspec=" || ".join(product_rspec)
        query=f'TYPE:MATNBR && TEXT2:({product_rspec}) && -TEXT6:X'
        material_df=querying_solr_data(query,params)
        # #remove proceding zeros
        # material_df=remove_proceeding_zeros(material_df)
        return material_df
    except Exception as e:
        pass

def construct_common_level_json(json_array,home_flag=""):
    try:
        all_details={}
        spec_list=[]
        material_list=[]
        last_specid=''
        for item in json_array.get("Spec_id"):
            try:
                spec_nam_id=item.get("name")
                spec_id_split=item.get("name").split(config.pipe_delimitter)
                if len(spec_id_split)>0:
                    spec_id=spec_id_split[0].strip()
                    if last_specid!=spec_id:
                        spec_list.append(spec_id)
                        all_details[spec_id]={}
                    else:
                        continue
                #product level classify
                if(json_array.get("product_Level")):
                    for prod in json_array.get("product_Level"):
                        try:
                            prod_spec=prod.get("real_Spec_Id")
                            synonyms=prod.get("synonyms").strip()
                            namprod=prod.get("namprod").strip()
                            if prod_spec==spec_id:
                                all_details=item_arrange(all_details,prod_spec,"namprod",namprod)  
                                all_details=item_arrange(all_details,prod_spec,"synonyms",synonyms)
                            #print(all_details)
                        except Exception as e:
                            pass
                #material level classify
                if(json_array.get("Mat_Level")): 
                    try:
                        for matid in json_array.get("Mat_Level"):         
                            bdt=matid.get("bdt")
                            material_number=matid.get("material_Number")
                            material_list.append(material_number)
                            if home_flag=="":
                                mat_spec_list=matid.get("real_Spec_Id")
                                if spec_nam_id in mat_spec_list:
                                    all_details=item_arrange(all_details,spec_id,"material_number",material_number)
                                    all_details=item_arrange(all_details,spec_id,"bdt",bdt)
                            elif home_flag=="home_page":
                                home_mat_spec_list=matid.get("spec_Nam_List")
                                for data in home_mat_spec_list:
                                    mat_spec_nam=data.get("real_Spec_Id")
                                    if mat_spec_nam==spec_nam_id:
                                        all_details=item_arrange(all_details,spec_id,"material_number",material_number)
                                        all_details=item_arrange(all_details,spec_id,"bdt",bdt)
                                        break
                    except Exception as e:
                        pass
                #cas level classify
                if(json_array.get("CAS_Level")): 
                    for casid in json_array.get("CAS_Level"):
                        pure_spec=casid.get("pure_Spec_Id")
                        cas_number=casid.get("cas_Number")
                        chemical_name=casid.get("chemical_Name")
                        if home_flag=="":
                            cas_spec_list=casid.get("real_Spec_Id")
                            if spec_nam_id in cas_spec_list:
                                all_details=item_arrange(all_details,spec_id,"pure_spec_id",pure_spec)
                                all_details=item_arrange(all_details,spec_id,"cas_number",cas_number)
                                all_details=item_arrange(all_details,spec_id,"chemical_name",chemical_name)
                        elif home_flag=="home_page":
                            home_cas_spec=casid.get("spec_Nam_List")
                            for data in home_cas_spec:
                                cas_spec_nam=data.get("real_Spec_Id")
                                if cas_spec_nam==spec_nam_id:
                                    all_details=item_arrange(all_details,spec_id,"pure_spec_id",pure_spec)
                                    all_details=item_arrange(all_details,spec_id,"cas_number",cas_number)
                                    all_details=item_arrange(all_details,spec_id,"chemical_name",chemical_name)
                                    break
                last_specid=spec_id
            except Exception as e:
                pass
        #print(all_details)
        return all_details,spec_list,list(set(material_list))
    except Exception as e:
        pass
        
def item_arrange(all_details,spec_id,prod_type,prod_value):
    try:
        if(all_details.get(spec_id).get(prod_type)):
            prod_list=all_details.get(spec_id).get(prod_type)
            if prod_value != "-" and len(prod_list)>0:
                if prod_value not in prod_list:
                    prod_list.append(prod_value)
                    # prod_list=list(set(prod_list))
                    all_details[spec_id][prod_type]=prod_list
        else:
            all_details[spec_id][prod_type]=[]
            if prod_value != "-":
                all_details[spec_id][prod_type].append(prod_value)
        return all_details
    except Exception as e:
        pass

def unstructure_template(all_details,category):
    try:
        unstructure_query=''
        product_map={"namprod":"NAMPROD","bdt":"BDT","material_number":"MATNBR","cas_number":"NUMCAS"}
        product_section_list=[]
        or_delimiter=config.or_delimiter
        spec_id_section=''
        spec_id_section_list=[]
        category_query=replace_character_for_querying(category)
        for specid in all_details:
            try:
                product_type_query=''
                product_list=[]
                product_value_query=''
                product_query=''
                product_section_list=[]
                product_section_template=''
                spec_query=f'SPEC_ID:*{specid}*'
                for prod_type in all_details.get(specid):
                    try:
                        if prod_type in product_map and len(all_details.get(specid).get(prod_type))>0:
                            product_type_query=f'PRODUCT_TYPE:{product_map.get(prod_type)}'
                            product_list=all_details.get(specid).get(prod_type)
                            replaced_query=replace_character_for_querying(product_list)
                            product_value_query=f'PRODUCT:({replaced_query})'
                            product_query=f'({product_type_query} && {product_value_query})'
                            product_section_list.append(product_query)
                    except Exception as e:
                        pass
                if len(product_section_list)!=0:
                    product_section_template=or_delimiter.join(product_section_list)
                    spec_id_section=f'({spec_query} && ({product_section_template}))'
                    spec_id_section_list.append(spec_id_section)
            except Exception as e:
                pass
        if len(spec_id_section_list)>0:
            spec_id_section_query=or_delimiter.join(spec_id_section_list)
            unstructure_query=f'IS_RELEVANT:1 && CATEGORY:({category_query}) && ({spec_id_section_query})'
    except Exception as e:
        pass
    logging.info("unstrucure_query"+str(unstructure_query))
    return unstructure_query

def finding_spec_details(spec_list,unstructure_spec):
    try:
        result_spec=[]
        if ";" in unstructure_spec:
            for id in spec_list:
                if id in unstructure_spec:
                    result_spec.append(id)
        else:
            result_spec.append(unstructure_spec) 
    except Exception as e:
        pass
    return (config.pipe_delimitter).join(result_spec)

def sfdc_template(all_details):
    try:
        unstructure_query=''
        product_map={"namprod":"NAMPROD","bdt":"BDT","material_number":"MATERIAL\ NUMBER","cas_number":"NUMCAS","synonyms":"SYNONYMS","chemical_name":"CHEMICAL\ NAME","pure_spec_id":"PURE-SPECID"}
        product_section_list=[]
        or_delimiter=config.or_delimiter
        spec_id_section=''
        spec_id_section_list=[]
        for specid in all_details:
            try:
                product_type_query=''
                product_list=[]
                product_value_query=''
                product_query=''
                product_section_list=[]
                product_section_template=''
                spec_query=f'REALSPECID:*{specid}*'
                for prod_type in all_details.get(specid):
                    try:
                        if prod_type in product_map and len(all_details.get(specid).get(prod_type))>0:
                            product_type_query=f'MATCHEDPRODUCTCATEGORY:{product_map.get(prod_type)}'
                            product_list=all_details.get(specid).get(prod_type)
                            replaced_query=replace_character_for_querying(product_list)
                            product_value_query=f'MATCHEDPRODUCTVALUE:({replaced_query})'
                            product_query=f'({product_type_query} && {product_value_query})'
                            product_section_list.append(product_query)
                    except Exception as e:
                        pass
                #adding real spec_id
                product_type_query=f'MATCHEDPRODUCTCATEGORY:REAL-SPECID'
                product_value_query=f'MATCHEDPRODUCTVALUE:({specid})'
                product_query=f'({product_type_query} && {product_value_query})'
                product_section_list.append(product_query)
                if len(product_section_list)!=0:
                    product_section_template=or_delimiter.join(product_section_list)
                    spec_id_section=f'({spec_query} && ({product_section_template}))'
                    spec_id_section_list.append(spec_id_section)
            except Exception as e:
                pass
        if len(spec_id_section_list)>0:
            spec_id_section_query=or_delimiter.join(spec_id_section_list)
            sfdc_query=f'({spec_id_section_query})'
    except Exception as e:
        pass
    logging.info("sfdc_query"+str(sfdc_query))
    return sfdc_query 

def finding_phrase_text(key_value_df,value):
    try:
        text_str=config.hypen_delimiter
        if (value!='' and value!=None and value!='None'):
            key_list=value.split(";")      
            if ("PHRKY" in list(key_value_df.columns)) and ("PTEXT" in list(key_value_df.columns)):
                text_df=key_value_df[key_value_df["PHRKY"].isin(key_list)]
                text_str=";".join(list(text_df["PTEXT"].unique()))
    except Exception as e:
        pass
    return text_str

def set_decimal_points(value):
    if type(value)!=float:
        value=float(value)
    return "{:.4f}".format(value)

def set_two_decimal_points(value):
    if type(value)!=float:
        value=float(value)
    return "{:.2f}".format(value)

def calculate_ppm_ppb(value,unit):
    if unit.lower()=="ppm":
        sub_value=(config.ppm)*(float(value))
    elif unit.lower()=="ppb":
        sub_value=(config.ppb)*(float(value))
    else:
        sub_value=float(value)
    return sub_value

def sort_json_values(json_list,sort_column,asc=True):
    try:
        json_list=[]
        if len(json_list)>0:
            dumps_json = json.dumps(json_list)
            json_df=pd.read_json(dumps_json)
            sorted_df=json_df.sort_values(by=sort_column,ascending=asc)  
            sorted_dict=json.loads(sorted_df.to_json(orient='index'))
            for item in sorted_dict:
                json_list.append(sorted_dict.get(item))
    except Exception as e:
        pass
    return json_list

def SQL_connection():
    try:
        import pyodbc
        connection_string=config.sql_url_config
        try:
            sql_conn = pyodbc.connect(connection_string)
            return sql_conn
            # execute query and save data in pandas df
        except Exception as error:
            pass
    except Exception as error:
            pass

def spec_constructor(req_body):
    try:
        last_specid=''
        namlist=[]
        speclist_json={}
        total_spec=[]
        spec_body=req_body.get("Spec_id",[])
        for item in spec_body:           
            spec_details=item.get("name").split(config.pipe_delimitter)
            if len(spec_details)>0:
                spec_id=spec_details[0]
                namprod=str(spec_details[1]).strip()
            if spec_id!='':
                total_spec.append(spec_id)
            if (last_specid!=spec_id) and last_specid!='':
                namstr=", ".join(namlist)
                speclist_json[last_specid]=namstr
                namlist=[]
                if namprod != "-":
                    namlist.append(namprod)        
            else:
                if namprod != "-":
                    namlist.append(namprod)    
            last_specid=spec_id
        namstr=", ".join(namlist)
        speclist_json[last_specid]=namstr
        return speclist_json,list(set(total_spec))
    except Exception as e:
        return speclist_json,list(set(total_spec))

def find_ontology_value(search_value):
    try:
        query=f'KEY_TYPE:* && ONTOLOGY_VALUE:{search_value}'
        ontolgy_result,ontolgy_df=get_data_from_core(config.solr_ontology,query)
        if "KEY_TYPE" in ontolgy_df.columns and "ONTOLOGY_KEY" in ontolgy_df.columns:
            search_key=list(ontolgy_df["KEY_TYPE"].unique())
            search_value=list(ontolgy_df["ONTOLOGY_KEY"].unique())
            return search_value[0],search_key[0]
        else:
            return "",""
    except Exception as e:
        return "",""

def update_in_change_audit_log(row_id,entity_name,user,action,date,product,product_type,product_synonyms,processed_flag):
    try:
        # current_date=datetime.now()
        conn=SQL_connection()
        cursor=conn.cursor()
        inser_value=f"'{row_id}','{entity_name}','{user}','{action}','{date}','{product}','{product_type}','{product_synonyms}','{processed_flag}'" 
        insert_query=f"insert into [momentive].[change_audit_log] values ({inser_value})"
        cursor.execute(insert_query)
    except Exception as e:
        conn.rollback()
        return "Cannot be updated due to some issue"
    else:
        conn.commit()
        return "updated in change audit log successfully"

def log_sort_date(df,column_name):
    try:
        df['Date'] =pd.to_datetime(df[column_name])
        sorted_df=df.sort_values(by=['Date'],ascending=False)  
        sorted_dict=json.loads(sorted_df.to_json(orient='index'))
        json_list=[]
        for item in sorted_dict:
            json_list.append(sorted_dict.get(item))
        return json_list
    except Exception as e:
        return []

def make_log_details(id_key,created_by,created_date,product_type='',product='',data_value=''):
    try:
        json_list=[]
        conn=SQL_connection()
        # cursor=conn.cursor()
        query=config.log_detail_query.format(str(id_key))
        change_audit_log_df = pd.read_sql(query,conn)
        if len(change_audit_log_df)>0:
            change_audit_log_df["updated_date"]=change_audit_log_df["updated_date"].astype(str)
            items=log_sort_date(change_audit_log_df,"updated_date")
            for data in items:
                json_make={}
                # json_make["created_By"]=created_by
                # json_make["created_Date"]=created_date
                json_make["updated_By"]=data.get("user_name")
                json_make["updated_Date"]=data.get("updated_date")
                json_make["product_type"]=data.get("product_type")
                json_make["product"]=data.get("product")
                json_make["synonyms_Extract_Data"]=data.get("synonyms/extract")
                json_list.append(json_make)  
        # if action='':
        else:
            json_make={}
            json_make["updated_By"]=created_by
            json_make["updated_Date"]=created_date
            json_make["product_type"]=product_type
            json_make["product"]=product
            json_make["synonyms_Extract_Data"]=data_value
            json_list.append(json_make)
    except Exception as e:
        pass
    return json_list

def remove_proceeding_zeros(product_df):
    try:
        product_df["TEXT1"]=product_df["TEXT1"].str.lstrip("0")
        return product_df
    except Exception as e:
        return product_df

def add_proceeding_zeros(material_number):
    try:
        zero_add=""
        len_material_number=len(material_number)
        if len_material_number !=18:
            add_zeros=18-len_material_number
        for value in range(add_zeros):
            zero_add+="0"
        return zero_add+material_number
    except Exception as e:
        return material_number

def make_common_query_for_std_legal_composition(all_details):
    try:
        spec_query_list=[]
        spec_query_str=''
        for specid in all_details:
            try:
                spec_query=f'SUBID:*{specid}*'
                product_list=all_details.get(specid).get("pure_spec_id")
                replaced_query=replace_character_for_querying(product_list)
                product_value_query=f'CSUBI:({replaced_query})'
                product_query=f'({spec_query} && {product_value_query})'
                spec_query_list.append(product_query)
            except Exception as e:
                pass
        spec_query_str=(config.or_delimiter).join(spec_query_list)   
        std,std_df=get_data_from_core(config.solr_std_composition,spec_query_str)
        legal_query_str=f"({spec_query_str}) && (ZUSAGE:REACH\:\ REG_REACH)"
        legal,legal_df=get_data_from_core(config.solr_legal_composition,legal_query_str)
        return std,std_df,legal,legal_df    
    except Exception as e:
        pass
def get_data_from_sql_table(query,get_list=""):
    try:
        conn=SQL_connection()
        table_df=pd.read_sql(query,conn)
        table_df.drop_duplicates(inplace=True)
        table_df=table_df.fillna("-")
        if get_list!='':
            table_list=table_df.to_json(orient='records')
            table_json_list=json.loads(table_list)
            return table_df,table_json_list
        else:
            return table_df
    except Exception as e:
        pass

def find_std_weight(product,product_type,spec_id,std_df):
    #checking std compositon condition
    std_weight=''
    componant_type=''
    if product_type=="NUMCAS":
        specid_list=spec_id.split(config.pipe_delimitter)
        if "CAS" in list(std_df.columns) and "SUBID" in list(std_df.columns):
            std_find=std_df[(std_df["CAS"]==product) & (std_df["SUBID"].isin(specid_list))]
    if len(std_find)==0:
        return std_weight,componant_type
    else:
        if len(std_find)>0 and ("CVALU" in std_find.columns) and ("COMPT" in std_find.columns):
            std_cvalue=std_find[["CVALU","CUNIT","COMPT"]]
            std_cvalu_list=std_cvalue.values.tolist()
            for value,unit,type in std_cvalu_list:
                weight=calculate_ppm_ppb(value,unit)
                std_weight=set_decimal_points(weight)
                componant_type=type
                break
    return f"{std_weight}%",componant_type

def get_generic_cas_details(all_details_json):
    try:
        all_cas=[]
        find_cas_details=[]
        for item in all_details_json:
            all_cas+=all_details_json.get(item).get("cas_number")
        all_cas=list(set(all_cas))
        query=config.select_query.format(config.table_connector,config.generic_cas_table)
        cas_df=get_data_from_sql_table(query)
        cas_df["cas_no"]=cas_df["cas_no"].str.strip()
        find_cas_df=cas_df[cas_df["cas_no"].isin(all_cas)]
        find_cas_details=json.loads(find_cas_df.to_json(orient='records'))
    except Exception as e:
        pass
    return find_cas_details     