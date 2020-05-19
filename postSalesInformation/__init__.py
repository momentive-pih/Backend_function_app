import logging
import json
import azure.functions as func
import os 
import pysolr
from __app__.shared_code import settings as config
from __app__.shared_code import helper
solr_unstructure_data=config.solr_unstructure_data

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postSalesInformation function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = get_sales_data_details(req_body[0])
        result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

def get_sales_data_details(req_body):
    try:
        logging.info("sales info request"+f'{req_body}')
        print(req_body)
        category=["SAP-BW"]
        material_level_data=req_body.get("Mat_Level")
        all_details_json,spec_list,material_list=helper.construct_common_level_json(req_body)
        sale_info_query=helper.unstructure_template(all_details_json,category)
        params={"fl":config.unstructure_column_str}
        result,result_df=helper.get_data_from_core(solr_unstructure_data,sale_info_query,params)
        sales_list=[]
        sales_kg=0
        sales_org=[]
        region=[] 
        material_flag=''
        for mat_id in material_list:
            total_2017=0
            total_2018=0
            total_2019=0
            total_2020=0
            try:
                for data in result: 
                    try:
                        material_number=data.get("PRODUCT","")
                        if material_number==mat_id:
                            material_flag='s'
                            result_spec=data.get("SPEC_ID","")  
                            spec_id=helper.finding_spec_details(spec_list,result_spec)
                            data_extract=json.loads(data.get("DATA_EXTRACT",{}))
                            sales_org_str=data_extract.get("Sales Organization","")
                            if sales_org != None:
                                sales_org.append(sales_org_str)
                            sales_kg_str=data_extract.get("SALES KG",0)
                            if sales_kg_str!= None:
                                sales_kg=sales_kg+float(sales_kg_str)
                            sold_str=data_extract.get("Sold-to Customer Country","")
                            if sold_str!=None:
                                region.append(sold_str)
                            year_split=str(data_extract.get("Fiscal year/period","-")).split(".")
                            if len(year_split)>1:
                                year=year_split[1]
                                if year=="2017":
                                    total_2017=total_2017+float(sales_kg_str)
                                elif year=="2018":
                                    total_2018=total_2018+float(sales_kg_str)
                                elif year=="2019":
                                    total_2019=total_2019+float(sales_kg_str)
                                elif year=="2020":
                                    total_2020=total_2020+float(sales_kg_str)
                    except Exception as e:
                        pass
                        # material_number=data.get("PRODUCT","")
                if material_flag=='s':
                    desc=[]
                    bdt=[]       
                    for item in material_level_data:
                        try:
                            if item.get("material_Number")==mat_id:
                                desc.append(item.get("description",""))
                                bdt.append(item.get("bdt",""))
                        except Exception as e:
                            pass
                    if sales_kg != 0:
                        sales_kg=helper.set_decimal_points(sales_kg)
                    if total_2017 != 0:
                        total_2017=helper.set_decimal_points(total_2017)
                    if total_2018 != 0:
                        total_2018=helper.set_decimal_points(total_2018)
                    if total_2019 != 0:
                        total_2019=helper.set_decimal_points(total_2019)
                    if total_2020 != 0:
                        total_2020=helper.set_decimal_points(total_2020)
                    sales_json={
                        "material_number":mat_id,
                        "material_description":(config.comma_delimiter).join(list(set(desc))),
                        "basic_data":(config.comma_delimiter).join(list(set(bdt))),
                        "sales_Org":(config.comma_delimiter).join(list(set(sales_org))),
                        "past_Sales":str(sales_kg),
                        "spec_id":spec_id,
                        "region_sold":(config.comma_delimiter).join(list(set(region))),
                        "total_sale_2017":str(total_2017),
                        "total_sale_2018":str(total_2018),
                        "total_sale_2019":str(total_2019),
                        "total_sale_2020":str(total_2020)
                        }
                    sales_list.append(sales_json) 
                    sales_json={}
                    material_flag=''
                    sales_kg=0
                    sales_org=[]
                    region=[]
            except Exception as e:
                pass
        result_data={"saleDataProducts":sales_list}
        return [result_data]
    except Exception as e:
        pass
        return []
