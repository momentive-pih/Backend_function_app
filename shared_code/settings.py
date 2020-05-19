import pysolr
import os
solr_url_config=os.environ["CUSTOMCONNSTR_SOLRCONNECTIONSTRING"]
sql_url_config=os.environ["CUSTOMCONNSTR_SQLCONNECTIONSTRING"]
#Solar url connection and access
solr_product= pysolr.Solr(solr_url_config+"/product_information/", timeout=10,verify=False)
solr_notification_status=pysolr.Solr(solr_url_config+'/sap_notification_status/', timeout=10,verify=False)
solr_unstructure_data=pysolr.Solr(solr_url_config+'/unstructure_processed_data/',always_commit=True, timeout=10,verify=False)
solr_document_variant=pysolr.Solr(solr_url_config+'/sap_document_variant/', timeout=10,verify=False)
solr_ghs_labeling_list_data=pysolr.Solr(solr_url_config+'/sap_ghs_labeling_list_data/', timeout=10,verify=False)
solr_ontology=pysolr.Solr(solr_url_config+'/ontology/',always_commit=True,timeout=10,verify=False)
solr_substance_identifier=pysolr.Solr(solr_url_config+'/sap_substance_identifier/',timeout=10,verify=False)
solr_phrase_translation=pysolr.Solr(solr_url_config+'/sap_phrase_translation/',timeout=10,verify=False)
solr_inci_name=pysolr.Solr(solr_url_config+'/inci_name_prod/',timeout=10,verify=False)
solr_std_composition=pysolr.Solr(solr_url_config+'/sap_standard_composition/',timeout=10,verify=False)
solr_hundrd_composition=pysolr.Solr(solr_url_config+'/sap_hundrd_percent_composition/',timeout=10,verify=False)
solr_legal_composition=pysolr.Solr(solr_url_config+'/sap_legal_composition/',timeout=10,verify=False)
solr_substance_volume_tracking=pysolr.Solr(solr_url_config+'/sap_substance_volume_tracking/',timeout=10,verify=False)
solr_registration_tracker=pysolr.Solr(solr_url_config+'/registration_tracker/',timeout=10,verify=False)
solr_sfdc=pysolr.Solr(solr_url_config+'/sfdc_identified_case/',timeout=10,verify=False)
#other cores
solr_allergen=pysolr.Solr(solr_url_config+'/sap_allergen/',timeout=10,verify=False)
solr_biocompatibility_testing_pri=pysolr.Solr(solr_url_config+'/sap_biocompatibility_testing_pri/',timeout=10,verify=False)
solr_bse_tse_gmo=pysolr.Solr(solr_url_config+'/sap_bse_tse_gmo/',timeout=10,verify=False)
solr_epa=pysolr.Solr(solr_url_config+'/sap_epa/',timeout=10,verify=False)
solr_product_regulatory_information=pysolr.Solr(solr_url_config+'/sap_product_regulatory_information/',timeout=10,verify=False)
solr_registration_company_specific=pysolr.Solr(solr_url_config+'/sap_registration_company_specific/',timeout=10,verify=False)
# Internationalization
junk_column=["solr_id","_version_"]
product_column = ["TYPE","TEXT1","TEXT2","TEXT3","TEXT4","TEXT5","TEXT6","SUBCT"]
spec_column_match=["TEXT1","TEXT2","TEXT3"]
spec_column_query="TEXT1,TEXT2,TEXT3"
material_column_match=["TEXT1","TEXT3","TEXT4"]
plain_material_column=["TYPE","TEXT1","TEXT3","TEXT4"]
plain_spec_column=["TYPE","TEXT1","TEXT2","TEXT3","SUBCT"]
ontology_spec_column=["TYPE","TEXT2","SUBCT"]
ontology_material_column=["TYPE","TEXT1"]
ontology_bdt_column=["TYPE","TEXT3"]
ontology_text1_column=["TYPE","TEXT1","SUBCT"]
ontology_text3_column=["TYPE","TEXT3","SUBCT"]
material_column_query="TEXT1,TEXT3,TEXT4"
product_nam_category = [["TEXT1","NAM PROD"],["TEXT2","REAL-SPECID"],["TEXT3","SYNONYMS"]]
product_rspec_category = [["TEXT2","REAL-SPECID"],["TEXT1","NAM PROD"],["TEXT3","SYNONYMS"]]
product_namsyn_category = [["TEXT3","SYNONYMS"],["TEXT2","REAL-SPECID"],["TEXT1","NAM PROD"]]
material_number_category = [["TEXT1","MATERIAL NUMBER"],["TEXT3","BDT"],["TEXT4","DESCRIPTION"]]
material_bdt_category = [["TEXT3","BDT"],["TEXT1","MATERIAL NUMBER"],["TEXT4","DESCRIPTION"]]
cas_number_category = [["TEXT1","CAS NUMBER"],["TEXT2","PURE-SPECID"],["TEXT3","CHEMICAL-NAME"]]
cas_pspec_category = [["TEXT2","PURE-SPECID"],["TEXT1","CAS NUMBER"],["TEXT3","CHEMICAL-NAME"]]
cas_chemical_category = [["TEXT3","CHEMICAL-NAME"],["TEXT2","PURE-SPECID"],["TEXT1","CAS NUMBER"]]
category_with_key=[["NAM*","TEXT1","NAMPROD","REAL_SUB","PRODUCT-LEVEL",product_nam_category,spec_column_match,spec_column_query],
                ["RSPEC*","TEXT2","NAMPROD","REAL_SUB","PRODUCT-LEVEL",product_rspec_category,spec_column_match,spec_column_query],
                ["SYN*","TEXT3","NAMPROD","REAL_SUB","PRODUCT-LEVEL",product_namsyn_category,spec_column_match,spec_column_query],
                ["MAT*","TEXT1","MATNBR","REAL_SUB","MATERIAL-LEVEL",material_number_category,material_column_match,material_column_query],
                ["BDT*","TEXT3","MATNBR","REAL_SUB","MATERIAL-LEVEL",material_bdt_category,material_column_match,material_column_query],
                ["CAS*","TEXT1","NUMCAS","PURE_SUB","CAS-LEVEL",cas_number_category,spec_column_match,spec_column_query],
                ["CHEM*","TEXT3","NUMCAS","PURE_SUB","CAS-LEVEL",cas_chemical_category,spec_column_match,spec_column_query], 
                ["PSPEC*","TEXT2","NUMCAS","PURE_SUB","CAS-LEVEL",cas_pspec_category,spec_column_match,spec_column_query],            
                ["SPEC*","TEXT2","NAMPROD","REAL_SUB","PRODUCT-LEVEL",product_rspec_category,spec_column_match,spec_column_query],
                ["SPEC*","TEXT2","NUMCAS","PURE_SUB","CAS-LEVEL",cas_pspec_category,spec_column_match,spec_column_query]]
category_type = ["MATNBR","NUMCAS","NAMPROD"]
search_category = ["TEXT1","TEXT2","TEXT3"]
selected_categories=["BDT*","MAT*","NAM*","CAS*","CHEM*","RSPEC*","PSPEC*","SYN*","SPEC*"]
product_map={"namprod":"NAMPROD","bdt":"BDT","material_number":"MATERIAL\ NUMBER","cas_number":"NUMCAS","synonyms":"SYNONYMS","chemical_name":"CHEMICAL\ NAME","pure_spec_id":"PURE-SPECID"}
product_level=["namprod","synonyms"]
material_level=["material_number","bdt"]
cas_level=["cas_number","chemical_name","pure_spec_id"]
solr_product_column = ",".join(product_column)
max_rows=2147483647
solr_product_params={"rows":max_rows,"fl":solr_product_column}
pipe_delimitter=" | "
or_delimiter=" || "
hypen_delimiter=" - "
comma_delimiter=", "
ppm=0.0001
ppb=0.0000001
ag_registration_country={"EU_REG_STATUS":"EU Region","US_REG_STATUS":"US Canada","LATAM_REG_STATUS":"Latin America"}
ag_registration_list=["EU_REG_STATUS","US_REG_STATUS","LATAM_REG_STATUS"]
us_eu_category={"US-FDA":"US FDA Letter","EU-FDA":"EU Food Contact"} 
customer_communication_category={
            "US FDA Letter":["US-FDA"],
            "EU Food Contact":["EU-FDA"],
            "Heavy Metals content":["Heavy metals"]
        }      
structure_category=["Chemical Structure","molecular formula","Molecular-Weight"]
restricted_dict={"GADSL":"GADSL","CALPROP":"CAL-PROP"}
restricted_sub_list=["GADSL","CAL-PROP"]
toxicology_category=["Study Title and Date","Monthly Toxicology Study List","Toxicology Summary"]
toxicology_dict={
    "Study Title and Date":["Toxicology"],
    "Monthly Toxicology Study List":["tox_study_selant","tox_study_silanes"],
    "Toxicology Summary":["Toxicology-summary"]
}
ontology_assigned_template={
            "US-FDA":{
                "US-FDA":[],
                "category":"US-FDA"
            },"EU-FDA":{
                "EU-FDA":[],"category":"EU-FDA"
            },"Toxicology-summary":{
                "Toxicology-summary":[],"category":"Toxicology-summary"
            },"CIDP":{
                "CIDP":[],"category":"CIDP"
            },"Toxicology":{
                "Toxicology":[],"category":"Toxicology"
            }
        }
relable_column=["IDCAT","SUBID","IDTYP","LANGU","IDTXT","DELFLG"]
ghs_label=["REBAS","SYMBL","SIGWD","HAZST","PRSTG","REMAR","ADDIN"]
relable_column_str="IDCAT,SUBID,IDTYP,LANGU,IDTXT"
ontology_assigned_category=["US-FDA","EU-FDA","Toxicology-summary","CIDP","Toxicology"]
# sfdc_column_str="MATCHEDPRODUCTVALUE,MATCHEDPRODUCTCATEGORY,EMAILSUBJECT,CASENUMBER,SOP_TIER_2_OWNER__C,EMAILATTACHMENT,EMAILBODY,CONTACTEMAIL,REASON,MANUFACTURINGPLANT,BU,ACCOUNTNAME"
sfdc_column_str="MATCHEDPRODUCTVALUE,MATCHEDPRODUCTCATEGORY,CASENUMBER,SOP_TIER_2_OWNER__C,REASON,MANUFACTURINGPLANT,BU,ACCOUNTNAME"
sfdc_column=["MATCHEDPRODUCTVALUE","MATCHEDPRODUCTCATEGORY","EMAILSUBJECT","CASENUMBER","SOP_TIER_2_OWNER__C","EMAILATTACHMENT","EMAILBODY","CONTACTEMAIL","REASON","MANUFACTURINGPLANT","BU","ACCOUNTNAME"]
sfdc_case_call=["MATCHEDPRODUCTVALUE","MATCHEDPRODUCTCATEGORY","CASENUMBER","REASON","SOP_TIER_2_OWNER__C","ACCOUNTNAME","BU","MANUFACTURINGPLANT"]
sfdc_email_call="EMAILSUBJECT,CASENUMBER,EMAILATTACHMENT,EMAILBODY,CONTACTEMAIL"
report_column_str="SUBID,REPTY,RGVID,LANGU,VERSN,STATS,RELON"
unstructure_column_str="PRODUCT_TYPE,SPEC_ID,CREATED,DATA_EXTRACT,CATEGORY,PRODUCT,UPDATED,ID,solr_id,DATA_EXTRACT"
notification_column_str="NOTIF,ZUSAGE,ADDIN,RLIST,SUBID"
phrase_column_str="PHRKY,PTEXT,GRAPH"
otherfields=["file_path","Date","subject","file_name"]
file_sources=["sharepoint-pih","share-drive-pih","sales-force-pih","website-pih"]
# blob_file_path=f"https://devstorpih001.blob.core.windows.net/"
blob_file_path=os.environ.get("AzureBlobStoragePath")
ghs_image_path=blob_file_path+f"momentive-sources-pih/ghs-images-pih/"
# sas_token=f"?sv=2019-02-02&ss=b&srt=sco&sp=rl&se=2020-05-29T20:19:29Z&st=2020-04-02T12:19:29Z&spr=https&sig=aodIg0rDPVsNEJY7d8AerhD79%2FfBO9LZGJdx2j9tsCM%3D"
sas_token=os.environ.get("AzureBlobStorageSasToken")
home_icon_product_attributes=blob_file_path+f"momentive-sources-pih/home-page-icon-images-pih/productAttributes.jpg"+sas_token
home_icon_customer_communication=blob_file_path+f"momentive-sources-pih/home-page-icon-images-pih/customerCommunication.png"+sas_token
home_icon_product_compliance=blob_file_path+f"momentive-sources-pih/home-page-icon-images-pih/ProductCompliance.jpg"+sas_token
home_icon_report_data=blob_file_path+f"momentive-sources-pih/home-page-icon-images-pih/report.png"+sas_token
home_icon_restricted_substance=blob_file_path+f"momentive-sources-pih/home-page-icon-images-pih/restrictedsubstance.png"+sas_token
home_icon_sales_info=blob_file_path+f"momentive-sources-pih/home-page-icon-images-pih/salesInformation.jpg"+sas_token
home_icon_toxicology=blob_file_path+f"momentive-sources-pih/home-page-icon-images-pih/toxicology.jpg"+sas_token
report_data_region_code={
"MSDS DE":"Germany",	
"MSDS BE":"Belgium",	
"MSDS NL":"Netherlands",	
"MSDS GB":"Great Britain",	
"MSDS F":"France",	
"MSDS I":"Italy",
"MSDS ESP":"Spain",
"MSDS PT":"Portugal",
"MSDS GR":"Greece",
"MSDS CH":"Switzerland",
"MSDS AT":"Austria",
"MSDS PL":"Poland",
"MSDS CZ":"Czech Republic",	
"MSDS SLO":"Slovenia",	
"MSDS WRUS":"Belarussia",	
"MSDS RUS":"Russian Fed.",	
"MSDS RO":"Romania",	
"MSDS IRL":"Ireland",	
"MSDS DK":"Denmark",	
"MSDS SWED":"Sweden",	
"MSDS FIN":"Finland",	
"MSDS NORW":"Norway",	
"MSDS EST":"Estonia",	
"MSDS HUN":"Hungary",	
"MSDS SLOV":"Slovakia",	
"MSDS MONAC":"Monaco",	
"MSDS LICHT":"Liechtenstein",	
"MSDS LUX":"Luxembourg",	
"MSDS TURQ":"Turkey",	
"MSDS BG":"Bulgaria",	
"MSDS US":"USA",	
"MSDS JP":"Japan",	
"MSDS TH":"Thailand",	
"MSDS CA":"Canada",	
"MSDS LET":"Latvia",	
"MSDS LIT":"Lithuania",	
"MSDS KR":"Korea (South)",	
"MSDS SG":"Singapore",	
"MSDS CN":"China",	
"MSDS BR":"Brazil",	
"MSDS AU":"Australia",	
"MSDS TW":"Taiwan",	
"MSDS MY":"Malaysia",	
"MSDS AE":"Utd.Arab Emir.",	
"MSDS ID":"Indonesia",	
"MSDS NZ":"New Zealand",	
"MSDS VN":"Vietnam",	
"MSDS MX":"Mexico",
}
in_compliance_notification_status=["on or in compliance with the inventory",
"please contact your supplier for further information on the inventory status of this material.",
"if purchased from momentive performance materials gmbh in leverkusen, germany, all substances in this product have been registered by momentive performance materials gmbh or upstream in our supply chain or are exempt from registration under regulation (ec) no 1907/2006 (reach). for polymers, this includes the constituent monomers and other reactants.",
"if purchased from MPM gmbh in leverkusen, germany, all substances in this product have been registered by momentive performance materials gmbh or upstream in our supply chain or are exempt from registration under regulation (ec) no 1907/2006 (reach). for polymers, this includes the constituent monomers and other reactants.",
"all components are listed or exempted.",
"listed",
"y (positive listing)",
"y"]
not_in_compliance_notification_status=["not in compliance with the inventory.","at least one component is not listed.","n (negative listing)"]
log_detail_query="select * from [momentive].[change_audit_log] where row_id={}"