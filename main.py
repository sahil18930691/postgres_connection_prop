# Import libraries
from fastapi import FastAPI, Request
from typing import Optional
from pydantic import BaseModel
import json
import pandas as pd


app = FastAPI()

from src.data.db_conn import load_db_table
from config1.config import get_project_root# Project root
PROJECT_ROOT = get_project_root()


class Item(BaseModel):
    building_id : str
    


@app.post("/transactions_building/")
async def create_item(item: Item):
    #print("id is {}".format(item.id))
    query1 = 'SELECT * FROM transactions_mh where building_id = \'{}\''.format(item.building_id)
    df = load_db_table(config_db = 'database.ini', query = query1)
    json_exist = df[["seller_details","purchaser_details"]]
    non_json = df.drop(["seller_details","purchaser_details"], axis = 1)

    #print(non_json)
    #print(df)
    #df.to_csv("data.csv")
    out = df.to_json(orient="records")
    
    
    #print(df.id[0])
    #json2 = {"id":int(df.id[0]),"record_id":df.record_id[0],"layer_path":df.layer_path[0],"source":df.source[0],"city":df.city[0],"owner_name":df.owner_name[0],"area":df.area[0],"cts_plot":df.cts_plot[0],"date_of_allotment":df.date_of_allotment[0],"node":df.node[0],"asset_type":df.asset_type[0],"date_of_agreement":df.date_of_agreement[0],"industry":df.industry[0],"plot_status":df.plot_status[0],"created_date":df.created_date[0],"updated_date":df.updated_date[0]}
    #json1 = {"id":int(df.id[0]),"record_id":df.record_id[0],"layer_path":df.layer_path[0],"source":df.source[0],"city":df.city[0],"owner_name":df.owner_name[0],"area":df.area[0],"cts_plot":df.cts_plot[0],"date_of_allotment":df.date_of_allotment[0],"node":df.node[0],"asset_type":df.asset_type[0],"date_of_agreement":df.date_of_agreement[0],"industry":df.industry[0],"plot_status":df.plot_status[0],"created_date":str(df.created_date[0]),"updated_date":str(df.updated_date[0])}
    #json1 = {"record_id":str(df.record_id[0]),"geocode_point":df.geocode_point[0],"building_id":df.building_id[0],"source":df.source[0],"record_status":df.record_status[0],"asset_type":df.asset_type[0],"transaction_type":df.transaction_type[0],"transaction_category":df.transaction_category[0],"year":str(df.year[0]),"village":df.village[0],"district":df.district[0],"village":df.village[0],"district":df.district[0],"document_number":df.document_number[0],"cts_plot":df.cts_plot[0],"unit_number":df.unit_number[0],"block":df.block[0],"tower_name":df.tower_name[0],"wing_number":df.wing_number[0],"property_name":df.property_name[0],"address":str(df.address[0]),"location":df.location[0],"locality":df.locality[0],"city":df.city[0],"pincode":df.pincode[0],"document_type":str(df.document_type[0]),"document_date":str(df.document_date[0]),"registration_date":str(df.registration_date[0]),"registration_index_date":str(df.registration_index_date[0]),"sro_code":str(df.sro_code[0]),"sro_name":str(df.sro_name[0]),"sro_status":str(df.sro_status[0]),"configuration":df.configuration[0],"floor_number":df.floor_number[0],"serial_volume":df.serial_volume[0],"remarks":df.remarks[0],"area":df.area[0],"rate_per_sqft":df.rate_per_sqft[0],"sale_rent_value":df.sale_rent_value[0]}
    #,"govt_deposit_value":df.govt_deposit_value[0],"stamp_duty":df.stamp_duty[0],"filling_value":df.filling_value[0],"date_of_submission":str(df.date_of_submission[0]),"seller_details":df.seller_details[0],"purchaser_details":df.purchaser_details[0],"verified_by":df.verified_by[0],"date_scraped":str(df.date_scraped[0]),"date_updated":str(df.date_updated[0]),"date_imported":str(df.date_imported[0])}
    #json1 = {"record_id":str(df.record_id[]),"geocode_point":df.geocode_point,"building_id":df.building_id}
    
    #data = json.dumps(out)
    data1 = json.loads(out.replace("\'", '"'))
    
    
    #print(type(data))
    #json=df.to_json()
    return data1
    '''
    
    data = json.dumps(out)
    #data1 = json.loads(data.replace("\'", '"'))
    return data'''


'''
from src.data.db_conn import load_db_table
from config1.config import get_project_root# Project root
PROJECT_ROOT = get_project_root()# Read database - PostgreSQL
query1 = 'SELECT * FROM land_records_landrecordnavimumbai where id=5 LIMIT 5'
df = load_db_table(config_db = 'database.ini', query = query1)
print(df)'''

@app.post("/transaction_details/")
async def create_item(item: Item):
    #print("id is {}".format(item.id))
    query1 = 'SELECT * FROM transactions_mh where building_id = \'{}\''.format(item.building_id)
    query2 = 'SELECT count(*) as count1 FROM transactions_mh where building_id = \'{}\''.format(item.building_id)
    query3 = 'SELECT avg(rate_per_sqft) FROM transactions_mh where building_id = \'{}\' and rate_per_sqft <> 0'.format(item.building_id)
    df = load_db_table(config_db = 'database.ini', query = query1)
    df1 = load_db_table(config_db = 'database.ini', query = query2)
    
    df2 = load_db_table(config_db = 'database.ini', query = query3)
    
    #json_exist = df[["seller_details","purchaser_details"]]
    #non_json = df.drop(["seller_details","purchaser_details"], axis = 1)

#Return all the transaction of a particular building id

    #print(non_json)
    #print(df)
    #df.to_csv("data.csv")
    
    df_output = df.to_json(orient="records")
    #df1_output = df1.to_json(orient="records")
    
    #df2_output = df2.to_json(orient="records")
    
    
    #print(df.id[0])
    #json2 = {"id":int(df.id[0]),"record_id":df.record_id[0],"layer_path":df.layer_path[0],"source":df.source[0],"city":df.city[0],"owner_name":df.owner_name[0],"area":df.area[0],"cts_plot":df.cts_plot[0],"date_of_allotment":df.date_of_allotment[0],"node":df.node[0],"asset_type":df.asset_type[0],"date_of_agreement":df.date_of_agreement[0],"industry":df.industry[0],"plot_status":df.plot_status[0],"created_date":df.created_date[0],"updated_date":df.updated_date[0]}
    #json1 = {"id":int(df.id[0]),"record_id":df.record_id[0],"layer_path":df.layer_path[0],"source":df.source[0],"city":df.city[0],"owner_name":df.owner_name[0],"area":df.area[0],"cts_plot":df.cts_plot[0],"date_of_allotment":df.date_of_allotment[0],"node":df.node[0],"asset_type":df.asset_type[0],"date_of_agreement":df.date_of_agreement[0],"industry":df.industry[0],"plot_status":df.plot_status[0],"created_date":str(df.created_date[0]),"updated_date":str(df.updated_date[0])}
    #json1 = {"record_id":str(df.record_id[0]),"geocode_point":df.geocode_point[0],"building_id":df.building_id[0],"source":df.source[0],"record_status":df.record_status[0],"asset_type":df.asset_type[0],"transaction_type":df.transaction_type[0],"transaction_category":df.transaction_category[0],"year":str(df.year[0]),"village":df.village[0],"district":df.district[0],"village":df.village[0],"district":df.district[0],"document_number":df.document_number[0],"cts_plot":df.cts_plot[0],"unit_number":df.unit_number[0],"block":df.block[0],"tower_name":df.tower_name[0],"wing_number":df.wing_number[0],"property_name":df.property_name[0],"address":str(df.address[0]),"location":df.location[0],"locality":df.locality[0],"city":df.city[0],"pincode":df.pincode[0],"document_type":str(df.document_type[0]),"document_date":str(df.document_date[0]),"registration_date":str(df.registration_date[0]),"registration_index_date":str(df.registration_index_date[0]),"sro_code":str(df.sro_code[0]),"sro_name":str(df.sro_name[0]),"sro_status":str(df.sro_status[0]),"configuration":df.configuration[0],"floor_number":df.floor_number[0],"serial_volume":df.serial_volume[0],"remarks":df.remarks[0],"area":df.area[0],"rate_per_sqft":df.rate_per_sqft[0],"sale_rent_value":df.sale_rent_value[0]}
    #,"govt_deposit_value":df.govt_deposit_value[0],"stamp_duty":df.stamp_duty[0],"filling_value":df.filling_value[0],"date_of_submission":str(df.date_of_submission[0]),"seller_details":df.seller_details[0],"purchaser_details":df.purchaser_details[0],"verified_by":df.verified_by[0],"date_scraped":str(df.date_scraped[0]),"date_updated":str(df.date_updated[0]),"date_imported":str(df.date_imported[0])}
    #json1 = {"record_id":str(df.record_id[]),"geocode_point":df.geocode_point,"building_id":df.building_id}
    
    #data = json.dumps(out)
    data1 = json.loads(df_output.replace("\'", '"'))
    

    json_data = {"transactions_count": int(df1.count1[0]), "average_price_persqft": int(df2.avg[0]), 'transactions': data1}
    
    
    #print(type(data))
    #json=df.to_json()
    return json_data


