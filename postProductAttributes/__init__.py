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
        found_data = get_product_attributes(req_body[0])
        result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

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

def get_product_attributes(req_body):
    try:  
        product_attributes_result=[]
        json_list=[]
        sub_category=req_body.get("Category_details").get("Subcategory")    
        validity=req_body.get("Category_details").get("validity")
        if sub_category=="Basic Information":
            all_details_json,spec_list,material_list = helper.construct_common_level_json(req_body)
            idtxt=[]
            #finding Relables
            spec_join=(config.or_delimiter).join(spec_list)
            spec_query=f'SUBID:({spec_join})'
            params={"fl":config.relable_column_str}
            result,result_df=helper.get_data_from_core(config.solr_substance_identifier,spec_query,params) 
            if len(result_df.columns)!=len(config.relable_column):
                dummy=pd.DataFrame([],columns=config.relable_column)
                result_df=pd.concat([result_df,dummy])
            result_df=result_df.fillna("-")  
            result_df=result_df.replace({"NULL":"-"})
            for item in all_details_json:
                json_make={}
                json_make["spec_id"]=item
                json_make["product_Identification"]=(config.comma_delimiter).join(all_details_json.get(item).get("namprod",[]))   
                idtxt_df=result_df[(result_df["IDCAT"]=="NAM") & (result_df["IDTYP"]=="PROD_RLBL") & (result_df["LANGU"].isin(["E","","-"])) & (result_df["SUBID"]==item)]
                idtxt=list(idtxt_df["IDTXT"].unique())
                if len(idtxt)>0:
                    json_make["relabels"]=(config.comma_delimiter).join(idtxt)
                else:
                    json_make["relabels"]="-"
                json_list.append(json_make)
            product_attributes_result.append({"basic_details":json_list})
            #product Application
            json_list=[]
            category=["Prod-App"]
            prod_query=helper.unstructure_template(all_details_json,category)
            params={"fl":config.unstructure_column_str}
            unstructure_values,unstructure_df=helper.get_data_from_core(config.solr_unstructure_data,prod_query,params)        
            if len(unstructure_values)>0:
                try:
                    for data in unstructure_values:
                        json_make={}
                        product=data.get("PRODUCT",config.hypen_delimiter)
                        product_type=data.get("PRODUCT_TYPE",config.hypen_delimiter)
                        datastr=json.loads(data.get("DATA_EXTRACT",{}))
                        result_spec=data.get("SPEC_ID")
                        spec_id=helper.finding_spec_details(spec_list,result_spec)
                        path=datastr.get("image_path")
                        if path != None:
                            if path.lower().endswith('pdf'):
                                file_type='pdf'
                            elif path.lower().endswith('png'):
                                file_type='png'
                            else:
                                file_type='others'
                            file_split=path.split("/")
                            file_source=''
                            for source in config.file_sources:
                                if source in file_split:
                                    file_source=source
                                    break
                            filename=datastr.get("file_name",config.hypen_delimiter)
                            if '.pdf' in filename:
                                filename=filename[:-4]
                            json_make["filename"]=filename
                            json_make["file_source"]=file_source
                            json_make["file_Type"]=file_type
                            json_make["product"]=product
                            json_make["product_Type"]=product_type
                            path=helper.replace_char_in_url(path)
                            json_make["prod_App"]=config.blob_file_path+path.replace("/dbfs/mnt/","")+config.sas_token
                            json_make["spec_Id"]=spec_id
                            json_list.append(json_make)  
                        else:
                            continue        
                except Exception as e:
                    pass
            product_attributes_result.append({"product_Application":json_list})     
        elif sub_category=="GHS Labeling":
            spec_json,spec_list=spec_constructor(req_body)
            spec_join=(config.or_delimiter).join(spec_list)
            spec_query=f'SUBID:({spec_join})'
            ghs_values,ghs_df=helper.get_data_from_core(config.solr_ghs_labeling_list_data,spec_query)
            total_phrky=[]
            if len(ghs_values)>0:
                for key_column in config.ghs_label:
                    try:
                        if key_column in list(ghs_df.columns):
                            phrase_key=list(ghs_df[key_column].unique())
                            phrase_split=";".join(phrase_key)
                            total_phrky+=phrase_split.split(";") 
                    except Exception as e:
                        pass   
                #finding phrase text
                # phrase_key_query=(config.or_delimiter).join(total_phrky)
                phrase_key_query=helper.replace_character_for_querying(total_phrky)
                query=f'PHRKY:({phrase_key_query})'
                params={"fl":config.phrase_column_str}
                key_value,key_value_df=helper.get_data_from_core(config.solr_phrase_translation,query,params)          
                for data in ghs_values:
                    try:
                        json_make={}
                        specid=data.get("SUBID","")      
                        spec_nam_str=specid+(config.hypen_delimiter)+spec_json.get(specid,"")
                        json_make["spec_Id"]=spec_nam_str
                        json_make["usage"]=str(data.get("ZUSAGE",config.hypen_delimiter).strip())
                        json_make["regulatory_Basis"]=helper.finding_phrase_text(key_value_df,str(data.get("REBAS","")).strip())
                        json_make["signal_Word"]=helper.finding_phrase_text(key_value_df,str(data.get("SIGWD","")).strip())
                        json_make["hazard_Statements"]=helper.finding_phrase_text(key_value_df,str(data.get("HAZST","")).strip())
                        json_make["prec_Statements"]=helper.finding_phrase_text(key_value_df,str(data.get("PRSTG","")).strip())
                        json_make["prstp"]=helper.finding_phrase_text(key_value_df,str(data.get("PRSTP","")).strip())
                        json_make["prstr"]=helper.finding_phrase_text(key_value_df,str(data.get("PRSTR","")).strip())
                        json_make["prsts"]=helper.finding_phrase_text(key_value_df,str(data.get("PRSTS","")).strip())
                        json_make["prstd"]=helper.finding_phrase_text(key_value_df,str(data.get("PRSTD","")).strip())
                        add_info=helper.finding_phrase_text(key_value_df,str(data.get("ADDIN","")).strip())
                        remarks=helper.finding_phrase_text(key_value_df,str(data.get("REMAR","")).strip())
                        add_remarks=config.hypen_delimiter
                        if (add_info!=config.hypen_delimiter) and (remarks!=config.hypen_delimiter):
                            add_remarks=add_info+(config.comma_delimiter)+remarks
                        elif(add_info!=config.hypen_delimiter):
                            add_remarks=add_info
                        elif(remarks!=config.hypen_delimiter):
                            add_remarks=remarks
                        json_make["additional_Information_remarks"]=add_remarks
                        #symbols
                        symbols=[]
                        path_list=[]
                        symbol_text=[]
                        text_list=[]
                        symbol_value=str(data.get("SYMBL","")).strip()
                        key_list=symbol_value.split(';')
                        if len(key_list)>0 and ("PHRKY" in list(key_value_df.columns)) and ("GRAPH" in list(key_value_df.columns)):
                            text_df=key_value_df[key_value_df["PHRKY"].isin(key_list)]
                            path_list=list(text_df["GRAPH"].unique())
                            text_list=list(text_df["PTEXT"].unique())
                        if len(path_list)>0:
                            for file in path_list:
                                path=(config.ghs_image_path)+file+(config.sas_token)
                                symbols.append({"name":path})                  
                        json_make["symbols"]=symbols
                        json_make["symbols_Text"]=(config.comma_delimiter).join(text_list)
                        if str(data.get("ZUSAGE",config.hypen_delimiter).strip()).upper() != 'PUBLIC: REG_EU':
                            json_list.append(json_make)
                    except Exception as e:
                        pass
            product_attributes_result.append({"ghs_Labeling":json_list})  
        elif sub_category in ["Structures and Formulas","Flow Diagrams"]:
            chem_structure=[]
            molecular_formula=[]
            molecular_weight=[]
            man_flow_dg=[]
            synthesis_dg=[]
            all_details_json,spec_list,material_list = helper.construct_common_level_json(req_body)
            std,std_df,legal,legal_df = helper.make_common_query_for_std_legal_composition(all_details_json)
            if sub_category=="Structures and Formulas":
                un_category=config.structure_category
            else:
                un_category=["man_flow_diagram","syn_flow_diagram"]
            query=helper.unstructure_template(all_details_json,un_category)
            params={"fl":config.unstructure_column_str}
            unstructure_values,unstructure_df=helper.get_data_from_core(config.solr_unstructure_data,query,params)        
            if len(unstructure_values)>0:
                for item in unstructure_values:
                    try:
                        json_make={}
                        datastr={}
                        category=item.get("CATEGORY",config.hypen_delimiter)
                        datastr=json.loads(item.get("DATA_EXTRACT",{}))
                        result_spec=item.get("SPEC_ID")
                        product=item.get("PRODUCT",config.hypen_delimiter)
                        product_type=item.get("PRODUCT_TYPE",config.hypen_delimiter)
                        spec_id=helper.finding_spec_details(spec_list,result_spec) 
                        path=datastr.get("file_path",config.hypen_delimiter)
                        path=helper.replace_char_in_url(path)
                        std_find=[]
                        legal_find=[]
                        std_flag="No"
                        legal_flag="No"
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
                        if path.lower().endswith('pdf'):
                            file_type='pdf'
                        elif path.lower().endswith('png'):
                            file_type='png'
                        else:
                            file_type='others'
                        file_split=path.split("/")
                        file_source=''
                        for source in config.file_sources:
                            if source in file_split:
                                file_source=source
                                break
                        json_make["spec_Id"]=spec_id
                        json_make["file_Source"]=file_source
                        json_make["product_Type"]=product_type
                        json_make["productName"]=product
                        if category=="Chemical Structure":
                            filename=datastr.get("file_path",config.hypen_delimiter).split("/")
                            if len(filename)>0:
                                json_make["fileName"]=filename[-1]
                            else:
                                json_make["fileName"]=config.hypen_delimiter
                            json_make["file_Path"]=(config.blob_file_path)+path.replace("/dbfs/mnt/","")+(config.sas_token)
                            json_make["file_Type"]=file_type
                            chem_structure.append(json_make)
                        elif category=="molecular formula":
                            path=datastr.get("image_path")
                            if path != None:
                                if path.lower().endswith('pdf'):
                                    file_type='pdf'
                                elif path.lower().endswith('png'):
                                    file_type='png'
                                else:
                                    file_type='others'
                                json_make["fileName"]=datastr.get("file_name",config.hypen_delimiter)
                                json_make["file_Path"]=(config.blob_file_path)+path.replace("/dbfs/mnt/","")+(config.sas_token)  
                                json_make["file_Type"]=file_type
                                molecular_formula.append(json_make)  
                            else:
                                continue             
                        elif category=="Molecular-Weight":
                            json_make["fileName"]=datastr.get("file_name",config.hypen_delimiter)
                            json_make["file_Path"]=(config.blob_file_path)+path.replace("/dbfs/mnt/","")+(config.sas_token)  
                            json_make["file_Type"]=file_type
                            weight=datastr.get("Molecular Weight")
                            if weight != None:
                                json_make["moelcular_Weight"]=weight
                            else:
                                continue
                            molecular_weight.append(json_make)   
                        elif category=="man_flow_diagram":
                            filename=datastr.get("file_path",config.hypen_delimiter).split("/")
                            if len(filename)>0:
                                json_make["fileName"]=filename[-1]
                            else:
                                json_make["fileName"]=config.hypen_delimiter
                            json_make["file_Path"]=(config.blob_file_path)+path.replace("/dbfs/mnt/","")+(config.sas_token)
                            json_make["file_Type"]=file_type
                            man_flow_dg.append(json_make)
                            
                        elif category=="syn_flow_diagram":
                            filename=datastr.get("file_path",config.hypen_delimiter).split("/")
                            if len(filename)>0:
                                json_make["fileName"]=filename[-1]
                            else:
                                json_make["fileName"]=config.hypen_delimiter
                            json_make["file_Path"]=(config.blob_file_path)+path.replace("/dbfs/mnt/","")+(config.sas_token)
                            json_make["file_Type"]=file_type
                            synthesis_dg.append(json_make)
                            json_make={}
                    except Exception as e:
                        pass
            if sub_category=="Structures and Formulas":        
                product_attributes_result.append({"chemical_Structure":chem_structure})
                product_attributes_result.append({"molecular_Formula":molecular_formula})
                product_attributes_result.append({"molecular_Weight":molecular_weight})
            else:
                product_attributes_result.append({"manufacture_Flow":man_flow_dg})
                product_attributes_result.append({"synthesis_Diagram":synthesis_dg})
        elif sub_category=="Composition":
            logging.info(f"product_attributes_request_body {req_body}")
            all_details_json,spec_list,material_list = helper.construct_common_level_json(req_body)
            idtxt=[]
            #finding Relables
            spec_join=(config.or_delimiter).join(spec_list)
            spec_query=f'SUBID:({spec_join})'
            params={"fl":config.relable_column_str}
            result,result_df=helper.get_data_from_core(config.solr_substance_identifier,spec_query,params) 
            if len(result_df.columns)!=len(config.relable_column):
                dummy=pd.DataFrame([],columns=config.relable_column)
                result_df=pd.concat([result_df,dummy])
            result_df=result_df.fillna("-")  
            result_df=result_df.replace({"NULL":"-"})
            for item in all_details_json:
                try:
                    json_make={}
                    json_make["spec_id"]=item
                    nam_df=result_df[(result_df["IDCAT"]=="NAM") & (result_df["IDTYP"]=="PROD") & (result_df["DELFLG"]!='X')]
                    nam_list=list(nam_df["IDTXT"].unique())
                    if len(nam_list)>0:
                        product_identify=(config.comma_delimiter).join(nam_list)
                    else:
                        product_identify=config.hypen_delimiter
                    json_make["product_Identification"]=product_identify
                    namprod_str=(config.comma_delimiter).join(all_details_json.get(item).get("namprod",[]))   
                    idtxt_df=result_df[(result_df["IDCAT"]=="NAM") & (result_df["IDTYP"]=="PROD_RLBL") & (result_df["LANGU"].isin(["E","","-"])) & (result_df["SUBID"]==item)]
                    idtxt=list(idtxt_df["IDTXT"].unique())
                    if len(idtxt)>0:
                        json_make["relabels"]=(config.comma_delimiter).join(idtxt)
                    else:
                        json_make["relabels"]="-"
                except Exception as e:
                    pass
            #finding inciname
            query=f'TYPE:MATNBR && TEXT2:({spec_join}) && -TYPE:SUBIDREL && -TEXT6:X'
            params={"fl":config.solr_product_column}
            mat_values,mat_df=helper.get_data_from_core(config.solr_product,query,params) 
            bdt=[]
            if "TEXT3" in mat_df.columns:
                bdt=list(mat_df["TEXT3"].unique())
            display_inci_name=[]
            # for spec in all_details_json:
            #     bdt+=all_details_json.get(spec).get("bdt",[])
            bdt_query=helper.replace_character_for_querying(bdt)
            query=f'BDTXT:({bdt_query}) && SUBID:({spec_join})'
            inci_values,inci_df=helper.get_data_from_core(config.solr_inci_name,query) 
            inci_df.drop_duplicates(inplace=True)
            if "INCINAME" in list(inci_df.columns) and "BDTXT" in list(inci_df.columns):
                bdtxt_df=inci_df[["BDTXT","INCINAME"]]
                bdtxt_df.drop_duplicates(inplace=True)
                bdtxt_list=bdtxt_df.values.tolist()
                for bdtxt,inci in bdtxt_list:
                    temp=bdtxt+(config.pipe_delimitter)+inci
                    display_inci_name.append(temp)           
                           
            json_make["INCI_name"]=(config.comma_delimiter).join(display_inci_name)
            json_list.append(json_make)
            #finding material level
            spec_with_namprod=f"{spec_list[0]} - {namprod_str}"
            materials=[]            
            active_material=[]
            all_material=[]
            # if material_query!='':
            for item in mat_values:
                try:
                    json_make={}
                    material_number=item.get("TEXT1",config.hypen_delimiter)
                    description=item.get("TEXT4",config.hypen_delimiter)
                    bdt=item.get("TEXT3",config.hypen_delimiter)
                    if str(item.get("TEXT5")).strip() != 'X':
                        json_make["material_Number"]=material_number
                        json_make["description"]=description
                        json_make["bdt"]=bdt
                        json_make["real_Spec_Id"]=spec_with_namprod
                        active_material.append(json_make)
                        json_make={}
                    json_make["material_Number"]=material_number
                    json_make["description"]=description
                    json_make["bdt"]=bdt
                    json_make["real_Spec_Id"]=spec_with_namprod
                    all_material.append(json_make)
                except Exception as e:
                    pass     
            #Finding usage for compositon 
            cas_list=[]     
            for spec in all_details_json:
                cas_list+=all_details_json.get(spec).get("pure_spec_id")
            cas_query=(config.or_delimiter).join(cas_list)
            spec_query=(config.or_delimiter).join(spec_list)  
            std_hund_list,usage_catgory,legal_list,legal_usage=find_zusage(all_details_json,cas_query,spec_query)           
            if len(usage_catgory)>0:
                validity=usage_catgory[0]
                std_values=find_std_hundrd_composition_details(validity,cas_query,spec_query,req_body,all_details_json,spec_with_namprod)
            else:
                std_values=[]
                std_hund_list=[]
            if len(legal_usage)>0:
                validity=legal_usage[0]   
                legal_values=find_legal_composition_details(validity,cas_query,spec_query,req_body,all_details_json,cas_list,spec_with_namprod)
            else:
                legal_values={"legal_composition":[],"svt":[]}
                legal_list=[]
            #finding default value for std composition
            json_make={}
            json_make["product_Level"]=json_list
            json_make["active_material"]=active_material
            json_make["all_material"]=all_material
            json_make["std_hund_usage"]=std_hund_list
            json_make["legal_usage"]=legal_list
            json_make["std_values"]=std_values
            json_make["legal_values"]=legal_values
            product_attributes_result=[json_make]
        elif sub_category in ["Standard, 100 % & INCI Composition","Legal Composition"]:
            all_details_json,spec_list,material_list = helper.construct_common_level_json(req_body)
            cas_list=[]
            spec_list=[]
            for spec in all_details_json:
                spec_list.append(spec)
                cas_list+=all_details_json.get(spec).get("pure_spec_id")
            cas_query=(config.or_delimiter).join(cas_list)
            spec_query=(config.or_delimiter).join(spec_list)
            if validity is None:
                std_hund_list,usage_catgory,legal_list,legal_usage=find_zusage(all_details_json,cas_query,spec_query)
                if sub_category=="Standard, 100 % & INCI Composition":
                    return std_hund_list
                elif sub_category=="Legal Composition":
                    return legal_list        
            if validity is not None:
                if sub_category=="Standard, 100 % & INCI Composition":
                    std_values=find_std_hundrd_composition_details(validity,cas_query,spec_query,req_body,all_details_json)
                    return std_values           
                elif sub_category=="Legal Composition":
                    legal_values=find_legal_composition_details(validity,cas_query,spec_query,req_body,all_details_json,cas_list)
                    return legal_values                  
        return product_attributes_result
    except Exception as e:
        return product_attributes_result

def find_zusage(all_details_json,cas_query,spec_query):   
    try:                 
        std_usage=[]
        hundrd_usage=[]
        legal_usage=[]
        usage_catgory=[]
        if cas_query!='' and spec_query!='':
            query=f'CSUBI:({cas_query}) && SUBID:({spec_query})'
            std_hund_list=[]
            legal_list=[]
            std_values,std_df=helper.get_data_from_core(config.solr_std_composition,query) 
            # std_df=std_df[std_df["CSUBI"].isin(cas_list)]       
            if "ZUSAGE" in list(std_df.columns):
                std_usage=list(std_df["ZUSAGE"].unique())
            hund_values,hund_df=helper.get_data_from_core(config.solr_hundrd_composition,query)
            # hund_df=hund_df[hund_df["CSUBI"].isin(cas_list)]
            if "ZUSAGE" in list(hund_df.columns):
                hundrd_usage=list(hund_df["ZUSAGE"].unique())           
            usage_catgory=std_usage+hundrd_usage
            edit_std_hund=[]
            for i in list(set(usage_catgory)):
                json_make={}
                json_make["name"]=i
                std_hund_list.append(json_make) 
                edit_std_hund.append(i)  
            legal_values,legal_df=helper.get_data_from_core(config.solr_legal_composition,query)  
            # legal_df=legal_df[legal_df["CSUBI"].isin(cas_list)] 
            legal_usage_edit=[]
            if "ZUSAGE" in list(legal_df.columns):
                legal_usage=list(legal_df["ZUSAGE"].unique())
            for i in list(set(legal_usage)):
                json_make={}
                json_make["name"]=i
                if 'PUBLIC: REG_EU' not in i: 
                    legal_usage_edit.append(i)
                    legal_list.append(json_make)
        return std_hund_list,edit_std_hund,legal_list,legal_usage_edit
    except Exception as e:
        return [],[]
    
def find_std_hundrd_composition_details(validity,cas_query,spec_query,req_body,all_details_json,spec_with_namprod=""):
    zusage_value=helper.replace_character_for_querying([validity])
    #finding product details
    if spec_with_namprod=="":
        spec_with_namprod=get_specid_namprod_details(all_details_json)
    query=f'CSUBI:({cas_query}) && ZUSAGE:({zusage_value}) && SUBID:({spec_query})'  
    json_list=[]  
    # std_result=[]
    # hundrd_result=[]
    # inci_result=[]
    std_values,std_df=helper.get_data_from_core(config.solr_std_composition,query)        
    hund_values,hund_df=helper.get_data_from_core(config.solr_hundrd_composition,query)
    cidp_query=helper.unstructure_template(all_details_json,["CIDP"])
    params={"fl":config.unstructure_column_str}
    cidp_values,cidp_df=helper.get_data_from_core(config.solr_unstructure_data,cidp_query,params)        
    for item in req_body.get("CAS_Level"):
        real_spec_list=item.get("real_Spec_Id")
        for real in real_spec_list:
            if spec_query in real:
                std_flag=''
                hundrd_flag=''
                inci_flag=''
                json_make={} 
                std_iupac_name=config.hypen_delimiter
                hundrd_iupac_name=config.hypen_delimiter
                for std in std_values:
                    if (std.get("CSUBI").strip()==item.get("pure_Spec_Id")):
                        std_flag='s'
                        json_make["std_Componant_Type"]=std.get("COMPT","-")
                        json_make["std_value"]=helper.set_decimal_points(std.get("CVALU",0))
                        json_make["std_unit"]=std.get("CUNIT","-")
                        std_iupac_name=std.get("NAM_IUPAC_EN","-")
                        std_cas_name=std.get("NAM_CAS_EN","-")
                        json_make["std_cal_value"]=helper.calculate_ppm_ppb(std.get("CVALU",0),std.get("CUNIT","-"))
                for hundrd in hund_values:
                    if hundrd.get("CSUBI").strip()==item.get("pure_Spec_Id"):
                        hundrd_flag='s'
                        json_make["hundrd_Componant_Type"]=hundrd.get("COMPT","-")
                        json_make["hundrd_value"]=helper.set_decimal_points(hundrd.get("CVALU",0))
                        json_make["hundrd_unit"]=hundrd.get("CUNIT","-")
                        hundrd_iupac_name=std.get("NAM_IUPAC_EN","-")
                        hundrd_cas_name=std.get("NAM_CAS_EN","-")
                        json_make["hundrd_cal_value"]=helper.calculate_ppm_ppb(hundrd.get("CVALU",0),hundrd.get("CUNIT","-"))
                for inci in cidp_values:
                    data=json.loads(inci.get("DATA_EXTRACT"))
                    inci_cas_number=data.get("CAS Number ").strip()
                    if inci_cas_number==item.get("cas_Number"):
                        inci_flag='s'
                        json_make["inci_Componant_Type"]="Active"
                        json_make["inci_value_unit"]=data.get("Target Composition","-")
                if std_flag =='':
                    json_make["std_Componant_Type"]='-'
                    json_make["std_value"]=0
                    json_make["std_unit"]="-"
                    json_make["std_cal_value"]=0
                if hundrd_flag=='':
                    json_make["hundrd_Componant_Type"]="-"
                    json_make["hundrd_value"]=0
                    json_make["hundrd_unit"]="-"
                    json_make["hundrd_cal_value"]=0
                if inci_flag=='':
                    json_make["inci_Componant_Type"]="-"
                    json_make["inci_value_unit"]="-"
                if std_flag=='s' or hundrd_flag=='s' or inci_flag=='s':
                    json_make["real_Spec_Id"]=spec_with_namprod
                    json_make["pure_spec_Id"]=str(item.get("pure_Spec_Id"))
                    json_make["cas_Number"]=item.get("cas_Number")
                    if (str(item.get("chemical_Name")).strip()=="-" or str(item.get("chemical_Name")).strip()==""):
                        if std_cas_name!="-":
                            chemical_name=std_cas_name
                        else:
                            chemical_name=std_iupac_name  
                    else:
                        chemical_name=item.get("chemical_Name")
                    json_make["ingredient_Name"]=chemical_name
                    json_list.append(json_make)
                break
    if len(json_list)>0:
        total_std_value=0
        total_hundrd_value=0
        total_inci_value=0
        for item in json_list:
            try:
                if float(item.get("std_cal_value")) >0:
                    total_std_value+=float(item.get("std_cal_value"))
                if float(item.get("hundrd_cal_value")) >0:
                    total_hundrd_value+=float(item.get("hundrd_cal_value"))
                if item.get("inci_value_unit") !="-":
                    inci_list=[incv for incv in str(item.get("inci_value_unit")) if(incv.isdigit() or incv==".")]
                    inci_str="".join(inci_list)
                    total_inci_value+=float(inci_str)
            except Exception as e:
                pass
        # #sort desceding order
        if len(json_list)>0:
            sorted_dict=sort_cvalue(json_list,"std_cal_value")
            # std_hund_result = json.dumps(json_list)
            # std_hund_df=pd.read_json(std_hund_result,dtype=str)
            # sorted_df=std_hund_df.sort_values(by=['std_cal_value'],ascending=False)  
            # sorted_dict=json.loads(sorted_df.to_json(orient='index'))
            json_list=[]
            for item in sorted_dict:
                json_list.append(sorted_dict.get(item))
        json_make={}
        json_make["pure_spec_Id"]="Total"
        json_make["cas_Number"]=""
        json_make["ingredient_Name"]=""
        json_make["std_Componant_Type"]=""
        json_make["std_value"]=helper.set_decimal_points(total_std_value)
        json_make["std_cal_value"]=""
        json_make["std_unit"]=""
        json_make["hundrd_Componant_Type"]=""
        json_make["hundrd_value"]=helper.set_decimal_points(total_hundrd_value)
        json_make["hundrd_cal_value"]=""
        json_make["hundrd_unit"]=""
        json_make["inci_Componant_Type"]=""
        if str(total_inci_value) == '0':
            inci_total_decimal_value="-"
        else:
            inci_total_decimal_value=helper.set_decimal_points(total_inci_value)
        json_make["inci_value_unit"]=inci_total_decimal_value
        json_list.append(json_make)
    return json_list

def find_legal_composition_details(validity,cas_query,spec_query,req_body,all_details_json,cas_list,spec_with_namprod=""):
    try:
        if spec_with_namprod=="":
            spec_with_namprod=get_specid_namprod_details(all_details_json)
        legal_comp={"legal_composition":[],"svt":[]}
        zusage_value=helper.replace_character_for_querying([validity])
        query=f'CSUBI:({cas_query}) && ZUSAGE:({zusage_value}) && SUBID:({spec_query})'  
        json_list=[]  
        legal_values,legal_df=helper.get_data_from_core(config.solr_legal_composition,query)        
        legal_df=legal_df[legal_df["CSUBI"].isin(cas_list)]
        legal_svt_spec=[]
        legal_comp={}
        total_legal_value=0
        for item in req_body.get("CAS_Level"):
            real_spec_list=item.get("real_Spec_Id")
            for real in real_spec_list:
                if spec_query in real:
                    for data in legal_values:
                        json_make={}
                        if data.get("CSUBI")==item.get("pure_Spec_Id"):
                            legal_svt_spec.append(item.get("pure_Spec_Id"))
                            json_make["real_Spec_Id"]=spec_with_namprod
                            json_make["pure_spec_Id"]=str(item.get("pure_Spec_Id"))
                            json_make["cas_Number"]=item.get("cas_Number")
                            if (str(item.get("chemical_Name")).strip()=="-" or str(item.get("chemical_Name")).strip()==""):
                                if data.get("NAM_CAS_EN","-") != '-':
                                    chemical_name=data.get("NAM_CAS_EN","-")
                                else:
                                    chemical_name=data.get("NAM_IUPAC_EN","-")
                            else:
                                chemical_name=item.get("chemical_Name")
                            json_make["ingredient_Name"]=chemical_name
                            json_make["legal_Componant_Type"]=data.get("COMPT","-")
                            json_make["legal_value"]=helper.set_decimal_points(data.get("CVALU",0))
                            json_make["legal_cal_value"]=helper.calculate_ppm_ppb(data.get("CVALU",0),data.get("CUNIT","-"))
                            if data.get("CVALU","-") !="-":
                                total_legal_value+=float(json_make.get("legal_cal_value",0))
                            json_make["legal_unit"]=data.get("CUNIT","-")
                            json_list.append(json_make) 
                    break 
        if len(json_list)>0:
            sorted_dict=sort_cvalue(json_list,"legal_cal_value")
            json_list=[]
            for item in sorted_dict:
                json_list.append(sorted_dict.get(item))
            json_make={}
            json_make["pure_spec_Id"]="Total"
            json_make["cas_Number"]=""
            json_make["ingredient_Name"]=""
            json_make["legal_Componant_Type"]=""
            json_make["legal_cal_value"]=""
            json_make["legal_value"]=helper.set_decimal_points(total_legal_value)
            json_make["legal_unit"]=""
            json_list.append(json_make)
        legal_comp["legal_composition"]=json_list
        if validity=='REACH: REG_REACH':
            json_list=[]
            json_make={}
            svt_result=[]
            if len(legal_svt_spec)>0:
                subid_list=(config.or_delimiter).join(legal_svt_spec)
                query=f'SUBID:({subid_list})'
                svt_result,svt_df=helper.get_data_from_core(config.solr_substance_volume_tracking,query)        
            presence_id=[]
            if "SUBID" in svt_df.columns:
                presence_id=list(svt_df["SUBID"].unique())
            for sub in presence_id:
                json_make["real_Spec_Id"]=spec_with_namprod
                json_make["pure_spec_Id"]=sub
                svt_total_2018=0
                svt_total_2019=0
                svt_total_2020=0
                for data in svt_result:
                    if sub==data.get("SUBID","-"):
                        reg_value=data.get("REGLT","-")
                        reg_year=data.get("QYEAR","-").strip()
                        if reg_value=="SVT_TE":
                            if reg_year=="2018":
                                json_make["SVT_TE_eight"]=helper.set_decimal_points(data.get("CUMQT",0))
                            if reg_year=="2019":
                                json_make["SVT_TE_nine"]=helper.set_decimal_points(data.get("CUMQT",0))
                            if  reg_year=="2020":
                                json_make["SVT_TE_twenty"]=helper.set_decimal_points(data.get("CUMQT",0))
                            json_make["amount_limit_SVT_TE"]=helper.set_decimal_points(data.get("AMTLT",0))
                        if reg_value=="SVT_AN":
                            if reg_year=="2018":
                                json_make["SVT_AN_eight"]=helper.set_decimal_points(data.get("CUMQT",0))
                            if reg_year=="2019":
                                json_make["SVT_AN_nine"]=helper.set_decimal_points(data.get("CUMQT",0))
                            if  reg_year=="2020":
                                json_make["SVT_AN_twenty"]=helper.set_decimal_points(data.get("CUMQT",0))
                            json_make["amount_limit_SVT_AN"]=helper.set_decimal_points(data.get("AMTLT",0))
                        if reg_value=="SVT_LV":
                            if reg_year=="2018":
                                svt_total_2018+=float(data.get("CUMQT","-"))
                                json_make["SVT_LV_eight"]=helper.set_decimal_points(svt_total_2018)
                            if reg_year=="2019":
                                svt_total_2019+=float(data.get("CUMQT","-"))
                                json_make["SVT_LV_nine"]=helper.set_decimal_points(svt_total_2019)
                            if  reg_year=="2020":
                                svt_total_2020+=float(data.get("CUMQT","-"))
                                json_make["SVT_LV_twenty"]=helper.set_decimal_points(svt_total_2020)
                            json_make["amount_limit_SVT_LV"]=helper.set_decimal_points(data.get("AMTLT",0))                   
                json_list.append(json_make)
                json_make={}
            json_make={}
            for item in range(len(json_list)):
                if json_list[item].get("SVT_TE_eight") is None:
                    json_list[item]["SVT_TE_eight"]="-"
                if json_list[item].get("SVT_TE_nine") is None:
                    json_list[item]["SVT_TE_nine"]="-"
                if json_list[item].get("SVT_TE_twenty") is None:
                    json_list[item]["SVT_TE_twenty"]="-"
                if json_list[item].get("amount_limit_SVT_TE") is None:
                    json_list[item]["amount_limit_SVT_TE"]="-"
                if json_list[item].get("SVT_AN_eight") is None:
                    json_list[item]["SVT_AN_eight"]="-"
                if json_list[item].get("SVT_AN_nine") is None:
                    json_list[item]["SVT_AN_nine"]="-"
                if json_list[item].get("SVT_AN_twenty") is None:
                    json_list[item]["SVT_AN_twenty"]="-"
                if json_list[item].get("amount_limit_SVT_AN") is None:
                    json_list[item]["amount_limit_SVT_AN"]="-"
                if json_list[item].get("SVT_LV_eight") is None:
                    json_list[item]["SVT_LV_eight"]="-"
                if json_list[item].get("SVT_LV_nine") is None:
                    json_list[item]["SVT_LV_nine"]="-"
                if json_list[item].get("SVT_LV_twenty") is None:
                    json_list[item]["SVT_LV_twenty"]="-"
                if json_list[item].get("amount_limit_SVT_LV") is None:
                    json_list[item]["amount_limit_SVT_LV"]="-"
            legal_comp["svt"]=json_list          
        else:
            legal_comp["svt"]=[]
        return legal_comp
    except Exception as e:
        return legal_comp

def get_specid_namprod_details(all_details_json):
    try:
        spec_str=''
        for spec_id in all_details_json:
            namprod_str=(config.comma_delimiter).join(all_details_json.get(spec_id).get("namprod",[]))   
            spec_str= f'{spec_id}{config.hypen_delimiter}{namprod_str}' 
            break   
        # edit_spec_query=spec_query.replace("SUBID","TEXT2")
        # namprod_query=f'TYPE:NAMPROD && TEXT2:({spec_query}) && SUBCT:REAL_SUB && -TEXT6:X'
        # nam_values,nam_df=helper.get_data_from_core(config.solr_product,namprod_query) 
        # if "TEXT1" in nam_df.columns and "TEXT2" in nam_df.columns:
        #     spec_id=list(nam_df["TEXT2"].unique())[0]
        #     namprod=list(nam_df["TEXT1"].unique())
        #     namprod_str=(config.comma_delimiter).join(namprod)
        #     spec_str=f'{spec_id}{config.hypen_delimiter}{namprod_str}'
        return spec_str
    except Exception as e:
        return spec_str

def sort_cvalue(json_list,sort_column):
    try:
        # #sort desceding order
        result = json.dumps(json_list)
        df=pd.read_json(result,dtype=str)
        df[sort_column]=df[sort_column].astype(float)
        sorted_df=df.sort_values(by=[sort_column],ascending=False)  
        sorted_dict=json.loads(sorted_df.to_json(orient='index'))
        return sorted_dict
    except Exception as e:
        return json_list