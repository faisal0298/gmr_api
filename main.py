"""
docker build -t rdx.registry.easemyai.com/galasachin97/gmr_api:0.5.3 . --no-cache
docker push rdx.registry.easemyai.com/galasachin97/gmr_api:0.5.3
"""

import rdx
import os, sys
import mongoengine
from database.models import *
# from helpers.ai_metadata_handler import on_ai_call
from helpers.usecase_handler import load_params, pre_processing
# from helpers.widget_handler import on_widget_call
import helpers.usecase_handler as usecase_handler_object
from helpers.logger import console_logger
from fastapi import FastAPI, BackgroundTasks
import json
# from fastapi import Response
from fastapi import (
    APIRouter,
    HTTPException,
    Form,
    Query,
    File,
    Depends,
    UploadFile,
    Header,
    Request,
    Response,
)
from lxml import etree
import xml.etree.ElementTree as ET
import datetime
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
from helpers.serializer import *
from datetime import timedelta
import requests
from helpers.scheduler import backgroundTaskHandler
from dateutil.relativedelta import relativedelta
import copy
from helpers.read_timezone import read_timezone_from_file
from helpers.serializer import *
import xlsxwriter
from typing import Optional
from mongoengine.queryset.visitor import Q
from collections import defaultdict
import pandas as pd
import pytz
import shutil
from helpers.report_handler import generate_report
from helpers.bunker_report_handler import bunker_generate_report
from helpers.data_execution import DataExecutions
from service import host, db_port, username, password, ip
from helpers.mail import send_email, send_test_email
import cryptocode
from mongoengine import MultipleObjectsReturned
from io import BytesIO
from pymongo import MongoClient
from dotenv import load_dotenv, dotenv_values
from bson.objectid import ObjectId
load_dotenv() 

# mahabal starts
import tabula
import math
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import re
from collections import OrderedDict
import PyPDF2
# mahabal end


#### railway pdf upload start #####

import pdftotext
import re
import camelot
import json
import warnings

#### railway pdf upload end #####


#maps start

import googlemaps
import polyline
import json
from shapely.geometry import LineString, mapping
from shapely.geometry.polygon import Polygon

#maps end


### database setup
# host = os.environ.get("HOST", "192.168.1.57")
# db_port = int(os.environ.get("DB_PORT", 30000))
# username = os.environ.get("USERNAME", "gmr_api")
# password = os.environ.get("PASSWORD", "Q1hTpYkpYNRzsUVs")

### usecase setup
environment = os.environ.get("ENVIRONMENT", "test")
parent_ids = os.environ.get("PARENTS_IDS", ["gmr_ai"])
parent_ids = (
    parent_ids.strip("][").replace("'", "").split(", ")
    if type(parent_ids) == str
    else parent_ids
)

widget_ids = os.environ.get(
    "WIDGETS_IDS",
    [
        "gmr_table",
        "coal_test_table",
        "timestamp_wise"
    ],
)

service_id = os.environ.get("SERVICE_ID", "gmr_api")
server_ip = os.environ.get("IP", "192.168.1.57")
server_port = os.environ.get("PORT", "80")
db_name = os.environ.get("DB_NAME", "gmrDB")

client = MongoClient(f"mongodb://{host}:{db_port}/")
db = client.gmrDB.get_collection("gmrdata")
short_mine_collection = db.short_mine

proxies = {
    "http": None,
    "https": None
}

IST = pytz.timezone('Asia/Kolkata')

usecase_handler_object.handler = rdx.SocketHandler(
    service_id=service_id,
    parent_ids=parent_ids,
)

def convert_to_utc_format(date_time, format, timezone= "Asia/Kolkata",start = True):
    to_zone = tz.gettz(timezone)
    _datetime = datetime.datetime.strptime(date_time, format)

    if not start:
        _datetime =_datetime.replace(hour=23,minute=59)
    return _datetime.replace(tzinfo=to_zone).astimezone(datetime.timezone.utc).replace(tzinfo=None)


tags_meta = [{"name":"Coal Consumption",
              "description": "Coal Consumption Data"},
              {"name":"Coal Testing",
              "description": "Coal Testing And Sampling"},
              {"name":"Road Map",
              "description": "Road Map for Truck Journey"},
              {"name":"Road Map Request",
              "description": "Road Map for Request Journey"}]

router = FastAPI(title="GMR API's", description="Contains GMR Testing, Consumption, Roadmap and Roadmap Request Apis",
                 openapi_tags=tags_meta)

router.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

console_logger.debug(" ------ Usecase GMR API Started ! ------ ")

mongoengine.connect(
        db=db_name,
        host=host,
        port=db_port,
        authentication_source=db_name,
        alias="gmrDB-alias"
    )


@usecase_handler_object.handler.fetch_data
def subscribe_to_socket_server_data(data):    
    if "source" in data:
        if data["source"] in parent_ids:
            return on_ai_call(data)
        elif data["source"] in widget_ids:
            return on_widget_call(data)
    return 0


@usecase_handler_object.handler.add_camera_handler
def add_camera(data, *args, **kwargs):
    load_params(data)


@usecase_handler_object.handler.usecase_params_handler
def variable_initializer(data, *args, **kwargs):
    load_params(data)

timezone = read_timezone_from_file()


# entry = UsecaseParameters.objects.filter(Parameters__gmr_api__exists=True).first()
# testing_hr, testing_min = "00", "00"
# consumption_hr, consumption_min = "00", "00"
# testing_ip = None
# testing_timer = None
# historian_ip = None
# historian_timer = None

# entry = UsecaseParameters.objects.filter(Parameters__gmr_api__exists=True).first()
# if entry:
#     historian_ip = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Consumption IP')
#     historian_timer = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Consumption Duration')

#     testing_ip = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Testing IP')
#     testing_timer = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Testing Duration')
    
#     testing_scheduler = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Testing Scheduler', {}).get("time")
#     consumption_scheduler = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Consumption Scheduler', {}).get("time")

#     if testing_scheduler and isinstance(testing_scheduler, str):
#         testing_hr, testing_min = testing_scheduler.split(":")

#     if consumption_scheduler and isinstance(consumption_scheduler, str):
#         consumption_hr, consumption_min = consumption_scheduler.split(":")

# console_logger.debug(f"---- Coal Testing IP ----            {testing_ip}")
# console_logger.debug(f"---- Coal Testing Duration ----      {testing_timer}")
# console_logger.debug(f"---- Coal Consumption IP ----        {historian_ip}")
# console_logger.debug(f"---- Coal Consumption Duration ----  {historian_timer}")
# console_logger.debug(f"---- Coal Testing Hr ----            {testing_hr}")
# console_logger.debug(f"---- Coal Testing Min ----           {testing_min}")
# console_logger.debug(f"---- Coal Consumption Hr ----        {consumption_hr}")
# console_logger.debug(f"---- Coal Consumption Min ----       {consumption_min}")


#  x------------------------------    Historian Api's for Coal Consumption    ------------------------------------x


#### railway pdf upload start #####


def extract_pdf_data(file_path):
    with open(file_path, "rb") as f:
        pdf = pdftotext.PDF(f, raw=True)
        output_string = pdf[0]

    # Define all patterns in a dictionary
    patterns = {
        "RR_NO": r"RR NO\.\s*(\d+)",
        "RR_DATE": r"RR DATE\s*(\d{2}-\d{2}-\d{4})",
        "FREIGHT": r"FREIGHT:\s*([\d.]+)",
        "TOTAL_FREIGHT": r"TOTAL FREIGHT:\s*([\d.]+)",
        "SD": r"SD\s*([\d.]+)",
        "POLA": r"POLA\s*([\d.]+)",
        "GST": r"\*GST\s*(\d+)",
    }

    # Extract all data in one pass
    data_dictionary = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, output_string)
        data_dictionary[key] = match.group(1) if match else "Not found"
    return data_dictionary


# def outbond(pdf_path, table_path):
def outbond(pdf_path):

    abc = camelot.read_pdf(pdf_path, flavor="stream", compress=True, pages="all")

    filtered_tables_data = []
    for table in abc:
        df = table.df
        if df.applymap(lambda cell: "Wagon Details" in cell).any().any():
            filtered_tables_data.extend(df.to_dict(orient="records"))

    # print(filtered_tables_data)

    # with open(table_path, "w") as json_file:
    #     json.dump(filtered_tables_data, json_file, indent=4)

    return filtered_tables_data

#### railway pdf upload end #####


# ---------------------------------- Mahabal data start ----------------------------------------

def mahabal_rr_lot(pdf_path,page):
    try:
        flattened_table = []
        rake_and_lot = {}
        rake_and_lot["rake"] = None
        rake_and_lot["rr"] = None
        rake_and_lot["lot"] = None
        rake_and_lot["do"] = None

        area = [224.64, 174.24, 349.92, 293.76]
        tables = tabula.read_pdf(
            pdf_path,
            guess=False,
            lattice=False,
            stream=True,
            multiple_tables=False,
            area=area,
            pages=str(page),
        )

        for table in tables:

            flattened_table = [
                str(item)
                for sublist in table.values.tolist()
                for item in sublist
                if not (isinstance(item, float) and math.isnan(item))
            ]

            joined_string = " ".join(flattened_table)
            if "Rake" and "RR" in joined_string:
                match = re.search(r"Rake\s+(\d+)\s+RR\s+(\d+)", joined_string)
                if match:
                    rake = match.group(1)
                    rr = match.group(2)
                    rake_and_lot["rake"] = rake
                    rake_and_lot["rr"] = rr

            if "Lot" and "DO" in joined_string:
                match = re.search(r"Lot-\s?(\d+)\s+DO\s?(\d+)", joined_string)
                if match:
                    lot = match.group(1)
                    do = match.group(2)
                    rake_and_lot["lot"] = lot
                    rake_and_lot["do"] = do

    except Exception as e:
        print(e)
    return rake_and_lot


def mahabal_ulr(pdf_path,page):
    try:
        ulrtable = []
        date_and_report = {}
        date_and_report["date"] = None
        date_and_report["report_no"] = None

        ulr_area = [129.6, 174.24, 162, 553.68]
        tables = tabula.read_pdf(
            pdf_path,
            guess=False,
            lattice=False,
            stream=True,
            multiple_tables=False,
            area=ulr_area,
            pages=str(page),
        )

        for table in tables:
            report_no = ""
            date = ""

            for col in table.columns:
                if not col.startswith("Unnamed"):
                    ulrtable.append(col)

            for index, row in table.iterrows():
                if "Report No." in row.values:
                    for item in row.values:
                        if "Report No." in item:
                            report_no = item.split(":")[1].strip()
                if "Date" in row.values:
                    for item in row.values:
                        if "Date" in item:
                            date = item.split(":")[1].strip()

            ulrtable.append(report_no)
            ulrtable.append(date)

            for sublist in table.values.tolist():
                ulrtable.extend(sublist)

        joined_string = " ".join(str(v) for v in ulrtable)
        date_match = re.search(
            r"Date:\s*([^\d]*)(\d{2})[^\d]*(\d{2})[^\d]*(\d{4})", joined_string
        )
        report_no_match = re.search(
            r"Report\s+No\.\s*:\s*([A-Z\s-]+-\d+)", joined_string
        )
        if date_match:
            day = date_match.group(2)
            month = date_match.group(3)
            year = date_match.group(4)
            date = f"{day}.{month}.{year}"
            date_and_report["date"] = date

        if report_no_match:
            report_no = report_no_match.group(1)
            date_and_report["report_no"] = report_no

    except Exception as e:
        print(e)
    return date_and_report


def mahabal_parameter(pdf_path,page):
    try:
        para_table = []
        coal_data = {}
        total_moisture_adb = None
        total_moisture_arb = None
        moisture_inherent_adb = None
        moisture_inherent_arb = None
        ash_adb = None
        ash_arb = None
        volatile_adb = None
        volatile_arb = None
        fixed_carbon_adb = None
        fixed_carbon_arb = None
        gross_calorific_adb = None
        gross_calorific_arb = None

        area = [362.16, 64.8, 546.4, 552.96]
        tables = tabula.read_pdf(
            pdf_path,
            guess=True,
            stream=True,
            multiple_tables=False,
            area=area,
            pages=str(page),
        )

        for table in tables:
            para_table = [
                item
                for sublist in table.values.tolist()
                for item in sublist
                if not (isinstance(item, float) and math.isnan(item))
            ]

            joined_string = " ".join(para_table)
        para_table = list(joined_string.split(" "))

        if "Moisture" in para_table:
            total_moisture_index = para_table.index("Moisture")
            total_moisture_adb = para_table[total_moisture_index + 2]
            total_moisture_arb = para_table[total_moisture_index + 3]
        if "(Inherent)" in para_table:
            moisture_inherent_index = para_table.index("(Inherent)")
            moisture_inherent_adb = para_table[moisture_inherent_index + 2]
            moisture_inherent_arb = para_table[moisture_inherent_index + 3]
        if "Ash" in para_table:
            ash_index = para_table.index("Ash")
            ash_adb = para_table[ash_index + 2]
            ash_arb = para_table[ash_index + 3]
        if "Matter" in para_table:
            volatile_index = para_table.index("Matter")
            volatile_adb = para_table[volatile_index + 2]
            volatile_arb = para_table[volatile_index + 3]
        if "Carbon" in para_table:
            fixed_carbon_index = para_table.index("Carbon")
            fixed_carbon_adb = para_table[fixed_carbon_index + 2]
            fixed_carbon_arb = para_table[fixed_carbon_index + 3]
        if "Value" in para_table:
            gross_calorific_index = para_table.index("Value")
            gross_calorific_adb = para_table[gross_calorific_index + 2]
            gross_calorific_arb = para_table[gross_calorific_index + 3]

        coal_data["total_moisture_adb"] = total_moisture_adb
        coal_data["total_moisture_arb"] = total_moisture_arb
        coal_data["moisture_inherent_adb"] = moisture_inherent_adb
        coal_data["moisture_inherent_arb"] = moisture_inherent_arb
        coal_data["ash_adb"] = ash_adb
        coal_data["ash_arb"] = ash_arb
        coal_data["volatile_adb"] = volatile_adb
        coal_data["volatile_arb"] = volatile_arb
        coal_data["fixed_carbon_adb"] = fixed_carbon_adb
        coal_data["fixed_carbon_arb"] = fixed_carbon_arb
        coal_data["gross_calorific_adb"] = gross_calorific_adb
        coal_data["gross_calorific_arb"] = gross_calorific_arb

    except Exception as e:
        print(e)

    return coal_data
    

@router.post("/pdf_data_upload", tags=["Extra"])
async def extract_data_from_mahabal_pdf(response: Response, pdf_upload: Optional[UploadFile] = File(None)):
    try:
        if pdf_upload is None:
            return {"error": "No file uploaded"}
        contents = await pdf_upload.read()

        # Check if the file is empty
        if not contents:
            return {"error": "Uploaded file is empty"}
        
        # Verify file format (PDF)
        if not pdf_upload.filename.endswith('.pdf'):
            return {"error": "Uploaded file is not a PDF"}

        file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
        target_directory = f"static_server/gmr_ai/{file}"
        os.umask(0)
        os.makedirs(target_directory, exist_ok=True, mode=0o777)

        file_extension = pdf_upload.filename.split(".")[-1]
        file_name = f'pdf_{datetime.datetime.now().strftime("%Y-%m-%d:%H:%M")}.{file_extension}'
        full_path = os.path.join(os.getcwd(), target_directory, file_name)
        with open(full_path, "wb") as file_object:
            file_object.write(contents)
        
        pdfReader = PyPDF2.PdfReader(full_path)
        totalPages = len(pdfReader.pages)
        
        listData = []
        id = None
        
        list_data = []
        for page in range(1,totalPages+1):
            console_logger.debug(page)
            rrLot = mahabal_rr_lot(full_path, page)
            ulrData = mahabal_ulr(full_path, page)
            parameterData = mahabal_parameter(full_path, page)

            console_logger.debug(rrLot)
            console_logger.debug(ulrData)
            console_logger.debug(parameterData)

            api_data = {
                "Total_Moisture_%": None,
                "Inherent_Moisture_(Adb)_%": None,
                "Ash_(Adb)_%": None,
                "Volatile_Matter_(Adb)_%": None,
                "Gross_Calorific_Value_(Adb)_Kcal/Kg": None,
                "Ash_(Arb)_%": None,
                "Volatile_Matter_(Arb)_%": None,
                "Fixed_Carbon_(Arb)_%": None,
                "Gross_Calorific_Value_(Arb)_Kcal/Kg": None,
                "DO_No": None,
                "Lot_No": None, 
                "RR_No": None,
            }
            pdf_data = {
                "Third_Party_Total_Moisture_%": None,
                "Third_Party_Total_Moisture(adb)_%": None,
                "Third_Party_Inherent_Moisture_(Adb)_%": None,
                "Third_Party_Inherent_Moisture_(Arb)_%": None,
                "Third_Party_Ash_(Adb)_%": None,
                "Third_Party_Volatile_Matter_(Adb)_%": None,
                "Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg": None,
                "Third_Party_Ash_(Arb)_%": None,
                "Third_Party_Volatile_Matter_(Arb)_%": None,
                "Third_Party_Fixed_Carbon_(Arb)_%": None,
                "Third_Party_Fixed_Carbon_(Adb)_%": None,
                "Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg": None,
                "Third_Party_Report_No": None,
            }

        
            # rail data
            if rrLot != None and parameterData != None:
                if rrLot.get("rake") != None and rrLot.get("rr") != None:
                    try:
                        # coalTrainData = CoalTestingTrain.objects.get(rake_no=f"{int(rrLot.get('rake'))}", rrNo=rrLot.get("rr"))
                        querysetTrain = CoalTestingTrain.objects.filter(rake_no=f"{int(rrLot.get('rake'))}", rrNo=rrLot.get("rr"))
                        if querysetTrain.count() == 0:
                            console_logger.debug("no data available")
                            continue
                        if querysetTrain.count() == 1:
                            coalTrainData = querysetTrain.get()
                        else:
                            coalTrainData = querysetTrain.first() 
                        id = str(coalTrainData.id)
                        if coalTrainData.rrNo:
                            api_data["RR_No"] = coalTrainData.rrNo
                        if coalTrainData.rake_no:
                            api_data["Lot_No"] = coalTrainData.rake_no
                        for single_data in coalTrainData.parameters:
                            if single_data.get("parameter_Name") == "Total_Moisture":
                                api_data["Total_Moisture_%"] = single_data.get("val1")
                            elif single_data.get("parameter_Name") == "Inherent_Moisture_(Adb)":
                                api_data["Inherent_Moisture_(Adb)_%"] = single_data.get("val1")
                            elif single_data.get("parameter_Name") == "Ash_(Adb)":
                                api_data["Ash_(Adb)_%"] = single_data.get("val1")
                            elif single_data.get("parameter_Name") == "Volatile_Matter_(Adb)":
                                api_data["Volatile_Matter_(Adb)_%"] = single_data.get("val1")
                            elif single_data.get("parameter_Name") == "Gross_Calorific_Value_(Adb)":
                                api_data["Gross_Calorific_Value_(Adb)_Kcal/Kg"] = single_data.get(
                                    "val1"
                                )
                            elif single_data.get("parameter_Name") == "Ash_(Arb)":
                                api_data["Ash_(Arb)_%"] = single_data.get("val1")
                            elif single_data.get("parameter_Name") == "Volatile_Matter_(Arb)":
                                api_data["Volatile_Matter_(Arb)_%"] = single_data.get("val1")
                            elif single_data.get("parameter_Name") == "Fixed_Carbon_(Arb)":
                                api_data["Fixed_Carbon_(Arb)_%"] = single_data.get("val1")
                            elif single_data.get("parameter_Name") == "Gross_Calorific_Value_(Arb)":
                                api_data["Gross_Calorific_Value_(Arb)_Kcal/Kg"] = single_data.get(
                                    "val1"
                                )
                    except DoesNotExist as e:
                        pass
                    
                    if ulrData.get("report_no"):
                        pdf_data["Third_Party_Report_No"]= ulrData.get("report_no")
                    
                    for key, value in parameterData.items():
                        if value != '-':
                            if key == 'total_moisture_adb':
                                pdf_data["Third_Party_Total_Moisture(adb)_%"] = value
                            elif key == 'total_moisture_arb':
                                pdf_data["Third_Party_Total_Moisture_%"] = value
                            elif key == 'moisture_inherent_adb':
                                pdf_data["Third_Party_Inherent_Moisture_(Adb)_%"] = value
                            elif key == 'moisture_inherent_arb':
                                pdf_data["Third_Party_Inherent_Moisture_(Arb)_%"] = value
                            elif key == "ash_adb":
                                pdf_data["Third_Party_Ash_(Adb)_%"] = value
                            elif key == "ash_arb":
                                pdf_data["Third_Party_Ash_(Arb)_%"] = value
                            elif key == "volatile_adb":
                                pdf_data["Third_Party_Volatile_Matter_(Adb)_%"] = value
                            elif key == "volatile_arb":
                                pdf_data["Third_Party_Volatile_Matter_(Arb)_%"] = value
                            elif key == "fixed_carbon_adb":
                                pdf_data["Third_Party_Fixed_Carbon_(Adb)_%"] = value
                            elif key == "fixed_carbon_arb":
                                pdf_data["Third_Party_Fixed_Carbon_(Arb)_%"] = value
                            elif key == "gross_calorific_adb":
                                pdf_data["Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"] = value
                            elif key == "gross_calorific_arb":
                                pdf_data["Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"] = value
                    # dataDict = {"id": id, "api_data": api_data, "pdf_data": pdf_data}
                    list_data.append({"id": id, "api_data": api_data, "pdf_data": pdf_data})
                    # return list_data 
                # road data
                elif rrLot.get("lot") != None and rrLot.get("do") != None:
                    try:
                        # queryset = CoalTesting.objects.get(rake_no=f'LOT-{rrLot.get("lot")}', rrNo=rrLot.get("do"))
                        queryset = CoalTesting.objects.filter(rake_no=f'LOT-{rrLot.get("lot")}', rrNo=rrLot.get("do"))
                        if queryset.count() == 0:
                            console_logger.debug("no data available")
                            continue
                        if queryset.count() == 1:
                            coalRoadData = queryset.get()
                        else:
                            coalRoadData = queryset.first() 
                        id = str(coalRoadData.id)
                        if coalRoadData.rrNo:
                            api_data["DO_No"] = coalRoadData.rrNo
                        if coalRoadData.rake_no:
                            api_data["Lot_No"] = coalRoadData.rake_no
                        for single_data in coalRoadData.parameters:
                            if single_data.get("parameter_Name") == "Total_Moisture":
                                api_data["Total_Moisture_%"] = single_data.get("val1")
                            elif single_data.get("parameter_Name") == "Inherent_Moisture_(Adb)":
                                api_data["Inherent_Moisture_(Adb)_%"] = single_data.get("val1")
                            elif single_data.get("parameter_Name") == "Ash_(Adb)":
                                api_data["Ash_(Adb)_%"] = single_data.get("val1")
                            elif single_data.get("parameter_Name") == "Volatile_Matter_(Adb)":
                                api_data["Volatile_Matter_(Adb)_%"] = single_data.get("val1")
                            elif single_data.get("parameter_Name") == "Gross_Calorific_Value_(Adb)":
                                api_data["Gross_Calorific_Value_(Adb)_Kcal/Kg"] = single_data.get(
                                    "val1"
                                )
                            elif single_data.get("parameter_Name") == "Ash_(Arb)":
                                api_data["Ash_(Arb)_%"] = single_data.get("val1")
                            elif single_data.get("parameter_Name") == "Volatile_Matter_(Arb)":
                                api_data["Volatile_Matter_(Arb)_%"] = single_data.get("val1")
                            elif single_data.get("parameter_Name") == "Fixed_Carbon_(Arb)":
                                api_data["Fixed_Carbon_(Arb)_%"] = single_data.get("val1")
                            elif single_data.get("parameter_Name") == "Gross_Calorific_Value_(Arb)":
                                api_data["Gross_Calorific_Value_(Arb)_Kcal/Kg"] = single_data.get(
                                    "val1"
                                )
                    except DoesNotExist as e:
                        pass

                    if ulrData.get("report_no"):
                        pdf_data["Third_Party_Report_No"]= ulrData.get("report_no")
                    
                    for key, value in parameterData.items():
                        if value != '-':
                            if key == 'total_moisture_adb':
                                pdf_data["Third_Party_Total_Moisture(adb)_%"] = value
                            elif key == 'total_moisture_arb':
                                pdf_data["Third_Party_Total_Moisture_%"] = value
                            elif key == 'moisture_inherent_adb':
                                pdf_data["Third_Party_Inherent_Moisture_(Adb)_%"] = value
                            elif key == 'moisture_inherent_arb':
                                pdf_data["Third_Party_Inherent_Moisture_(Arb)_%"] = value
                            elif key == "ash_adb":
                                pdf_data["Third_Party_Ash_(Adb)_%"] = value
                            elif key == "ash_arb":
                                pdf_data["Third_Party_Ash_(Arb)_%"] = value
                            elif key == "volatile_adb":
                                pdf_data["Third_Party_Volatile_Matter_(Adb)_%"] = value
                            elif key == "volatile_arb":
                                pdf_data["Third_Party_Volatile_Matter_(Arb)_%"] = value
                            elif key == "fixed_carbon_adb":
                                pdf_data["Third_Party_Fixed_Carbon_(Adb)_%"] = value
                            elif key == "fixed_carbon_arb":
                                pdf_data["Third_Party_Fixed_Carbon_(Arb)_%"] = value
                            elif key == "gross_calorific_adb":
                                pdf_data["Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"] = value
                            elif key == "gross_calorific_arb":
                                pdf_data["Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"] = value
                    # dataDict = {"id": id, "api_data": api_data, "pdf_data": pdf_data}
                    list_data.append({"id": id, "api_data": api_data, "pdf_data": pdf_data})
            else:
                console_logger.debug("data not found")  
        return list_data      
    except DoesNotExist as e:
        console_logger.debug("No matching object found.")
        return HTTPException(status_code="404", detail="No matching object found in db")
    except MultipleObjectsReturned:
        pass
    #     console_logger.debug("multiple entry found for single rrno/dono")
    #     return HTTPException(status_code="400", detail="multiple entry found for single rrno/dono")
    except Exception as e:
        console_logger.debug("----- Excel error -----", e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug(
            "Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno)
        )
        return e


# ---------------------------------- Coal Consumption ----------------------------------------


consumption_headers = {
'ClientToken': 'Administrator',
'Content-Type': 'application/json'}

@router.get("/load_historian_data", tags=["Coal Consumption"])                                    # coal consumption
def extract_historian_data(start_date: Optional[str] = None, end_date: Optional[str] = None):
    success = False
    try:
        global consumption_headers, proxies
        entry = UsecaseParameters.objects.first()
        historian_ip = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Consumption IP') if entry else None
        historian_timer = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Consumption Duration') if entry else None

        console_logger.debug(f"---- Coal Consumption IP ----        {historian_ip}")

        if not end_date:
            end_date = (datetime.datetime.now(IST).replace(minute=00,second=00,microsecond=00).strftime("%Y-%m-%dT%H:%M:%S"))
        if not start_date:
            start_date = (datetime.datetime.now(IST).replace(minute=0,second=0,microsecond=0) - datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")

        console_logger.debug(f" --- Consumption Start Date --- {start_date}")
        console_logger.debug(f" --- Consumption End Date --- {end_date}")

        payload = json.dumps({
                    "StartTime": start_date,
                    "EndTime": end_date, 
                    "RetrievalType": "Aggregate", 
                    "RetrievalMode": "History", 
                    "TagID": ["2","3538","16","3536"],
                    "RetrieveBy": "ID"
                    })
        
        consumption_url = f"http://{historian_ip}/api/REST/HistoryData/LoadTagData"
        # consumption_url = "http://10.100.12.28:8093/api/REST/HistoryData/LoadTagData"
        try:
            response = requests.request("POST", url=consumption_url, headers=consumption_headers, data=payload, proxies=proxies)
            data = json.loads(response.text)

            for item in data["Data"]:
                tag_id = item["Data"]["TagID"]
                sum = item["Data"]["SUM"]
                created_date = item["Data"]["CreatedDate"]

                if tag_id in [16, 3538]:
                    sum_value = str(round(int(float(sum)) / 1000 , 2))
                elif tag_id in [2, 3536]:
                    sum_value = str(round(int(float(sum)) / 10000 , 2))

                if not Historian.objects.filter(tagid=tag_id, created_date=created_date):
                    console_logger.debug("adding data")
                    Historian(
                        tagid = tag_id,
                        sum = sum_value,
                        created_date = created_date,
                        ID=Historian.objects.count() + 1
                    ).save()
                else:
                    console_logger.debug("data already exists in historian")
                
            success = "completed"
        except requests.exceptions.Timeout:
            console_logger.debug("Request timed out!")
        except requests.exceptions.ConnectionError:
            console_logger.debug("Connection error")
    
    except Exception as e:
        success = False
        console_logger.debug("----- Coal Testing Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        success = e
        
    finally:
        console_logger.debug(f"success:{success}")
        SchedulerResponse("save consumption data", f"{success}")
        return {"message" : "Successful"} 
    

# @router.get("/historian_data", tags=["Historian"])
# def fetch_historian_data():
#     historian_tag_count = "http://10.100.12.28:8093/api/REST/Tag/Search?tagCount=100&keywords=level"
#     response = requests.request("GET", url = historian_tag_count, headers=consumption_headers)
#     return Response(response.content)



# @router.post("/historian_latest_value", tags=["Historian"])
# def load_historian_value():
#     payload = json.dumps({
#                 "Timestamps": "2023-03-20 15:30:00",
#                 "TagID": ["2","3538"],
#                 "RetrieveBy": "ID"})
#     historain_latest_value = "http://10.100.12.28:8093/api/REST/HistoryData/LoadLatestValue"
#     response = requests.request("POST", url = historain_latest_value, headers=consumption_headers, data=payload)
#     return Response(response.text)


@router.get("/coal_generation_graph", tags=["Coal Consumption"])
def coal_generation_analysis(response:Response, type: Optional[str] = "Daily",
                              Month: Optional[str] = None, 
                              Daily: Optional[str] = None, Year: Optional[str] = None):
    try:
        data={}
        UTC_OFFSET_TIMEDELTA = datetime.datetime.utcnow() - datetime.datetime.now()

        basePipeline = [
            {
                "$match": {
                    "created_date": {
                        "$gte": None,
                    },
                },
            },
            {
                "$project": {
                    "ts": {
                        "$hour": {"date": "$created_date"},
                    },
                    "tagid": "$tagid",
                    "sum": "$sum",
                    "_id": 0
                },
            },
            {
                "$group": {
                    "_id": {
                        "ts": "$ts",
                        "tagid": "$tagid"
                    },
                    "data": {
                        "$push": "$sum"
                    }
                }
            },
        ]

        if type == "Daily":

            date=Daily
            end_date =f'{date} 23:59:59'
            start_date = f'{date} 00:00:00'
            format_data = "%Y-%m-%d %H:%M:%S"
            endd_date=datetime.datetime.strptime(end_date,format_data)
            startd_date=datetime.datetime.strptime(start_date,format_data)

            basePipeline[0]["$match"]["created_date"]["$lte"] = (endd_date)
            basePipeline[0]["$match"]["created_date"]["$gte"] = (startd_date)
            

            result = {
                "data": {
                    "labels": [str(i) for i in range(1, 25)],
                    "datasets": [
                        {"label": "Unit 1", "data": [0 for i in range(1, 25)]},             # unit 1 = tagid_2
                        {"label": "Unit 2", "data": [0 for i in range(1, 25)]},             # unit 2 = tagid_3536
                    ],
                }
            }

        elif type == "Week":
            basePipeline[0]["$match"]["created_date"]["$gte"] = (
                datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                + UTC_OFFSET_TIMEDELTA
                - datetime.timedelta(days=7)
            )
            basePipeline[1]["$project"]["ts"] = {"$dayOfMonth": "$created_date"}
            result = {
                "data": {
                    "labels": [
                        (
                            basePipeline[0]["$match"]["created_date"]["$gte"]
                            + datetime.timedelta(days=i + 1)
                        ).strftime("%d")
                        for i in range(1, 8)
                    ],
                    "datasets": [
                        {"label": "Unit 1", "data": [0 for i in range(1, 8)]},              # unit 1 = tagid_2
                        {"label": "Unit 2", "data": [0 for i in range(1, 8)]},              # unit 2 = tagid_3536
                    ],
                }
            }

        elif type == "Month":

            date=Month
            format_data = "%Y - %m-%d"

            start_date = f'{date}-01'
            startd_date=datetime.datetime.strptime(start_date,format_data)
            
            end_date = startd_date + relativedelta( day=31)
            end_label = (end_date).strftime("%d")

            basePipeline[0]["$match"]["created_date"]["$lte"] = (end_date)
            basePipeline[0]["$match"]["created_date"]["$gte"] = (startd_date)
            basePipeline[1]["$project"]["ts"] = {"$dayOfMonth": "$created_date"}
            result = {
                "data": {
                    "labels": [
                        (
                            basePipeline[0]["$match"]["created_date"]["$gte"]
                            + datetime.timedelta(days=i + 1)
                        ).strftime("%d")
                        for i in range(-1, (int(end_label))-1)
                    ],
                    "datasets": [
                        {"label": "Unit 1", "data": [0 for i in range(-1, (int(end_label))-1)]},        # unit 1 = tagid_2
                        {"label": "Unit 2", "data": [0 for i in range(-1, (int(end_label))-1)]},        # unit 2 = tagid_3536
                    ],
                }
            }

        elif type == "Year":

            date=Year
            end_date =f'{date}-12-31 23:59:59'
            start_date = f'{date}-01-01 00:00:00'
            format_data = "%Y-%m-%d %H:%M:%S"
            endd_date=datetime.datetime.strptime(end_date,format_data)
            startd_date=datetime.datetime.strptime(start_date,format_data)

            basePipeline[0]["$match"]["created_date"]["$lte"] = (
                endd_date
            )
            basePipeline[0]["$match"]["created_date"]["$gte"] = (
                startd_date          
            )

            basePipeline[1]["$project"]["ts"] = {"$month": "$created_date"}
            result = {
                "data": {
                    "labels": [
                        (
                            basePipeline[0]["$match"]["created_date"]["$gte"]
                            + relativedelta(months=i)
                        ).strftime("%m")
                        for i in range(0, 12)
                    ],
                    "datasets": [
                        {"label": "Unit 1", "data": [0 for i in range(0, 12)]},                     # unit 1 = tagid_2
                        {"label": "Unit 2", "data": [0 for i in range(0, 12)]},                     # unit 2 = tagid_3536
                    ],
                }
            }

        output = Historian.objects().aggregate(basePipeline)
        outputDict = {}

        for data in output:
            if "_id" in data:
                ts = data["_id"]["ts"]
                tag_id = data["_id"]["tagid"]

                data_list = data.get('data', [])
                sum_list = []
                for item in data_list:
                    try:
                        sum_value = float(item)
                        sum_list.append(sum_value)
                    except ValueError:
                        pass
                
                if ts not in outputDict:
                    outputDict[ts] = {tag_id: sum_list}
                else:
                    if tag_id not in outputDict[ts]:
                        outputDict[ts][tag_id] = sum_list
                    else:
                        outputDict[ts][tag_id].append(sum_list)

        modified_labels = [i for i in range(1, 25)]

        for index, label in enumerate(result["data"]["labels"]):
            if type == "Week":
                modified_labels = [
                    (
                        basePipeline[0]["$match"]["created_date"]["$gte"]
                        + datetime.timedelta(days=i + 1)
                    ).strftime("%d-%m-%Y,%a")
                    for i in range(1, 8)
                ]
            
            elif type == "Month":
                modified_labels = [
                    (
                        basePipeline[0]["$match"]["created_date"]["$gte"]
                        + datetime.timedelta(days=i + 1)
                    ).strftime("%d/%m")
                    for i in range(-1, (int(end_label))-1)
                ]

            elif type == "Year":
                modified_labels = [
                    (
                        basePipeline[0]["$match"]["created_date"]["$gte"]
                        + relativedelta(months=i)
                    ).strftime("%b %y")
                    for i in range(0, 12)
                ]

            if int(label) in outputDict:
                for key, val in outputDict[int(label)].items():
                    total_sum = sum(val)
                    if key == 2:
                        result["data"]["datasets"][0]["data"][index] = total_sum
                    if key == 3536:
                        result["data"]["datasets"][1]["data"][index] = total_sum

        result["data"]["labels"] = copy.deepcopy(modified_labels)
        console_logger.debug(f"-------- Coal Generation Graph Response -------- {result}")
        return result
    
    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        return e


@router.get("/coal_consumption_graph", tags=["Coal Consumption"])
def coal_consumption_analysis(response:Response,type: Optional[str] = "Daily",
                              Month: Optional[str] = None, 
                              Daily: Optional[str] = None, Year: Optional[str] = None):
    try:
        data={}
        UTC_OFFSET_TIMEDELTA = datetime.datetime.utcnow() - datetime.datetime.now()

        basePipeline = [
            {
                "$match": {
                    "created_date": {
                        "$gte": None,
                    },
                },
            },
            {
                "$project": {
                    "ts": {
                        "$hour": {"date": "$created_date"},
                    },
                    "tagid": "$tagid",
                    "sum": "$sum",
                    "_id": 0
                },
            },
            {
                "$group": {
                    "_id": {
                        "ts": "$ts",
                        "tagid": "$tagid"
                    },
                    "data": {
                        "$push": "$sum"
                    }
                }
            },
        ]

        if type == "Daily":

            date=Daily
            end_date =f'{date} 23:59:59'
            start_date = f'{date} 00:00:00'
            format_data = "%Y-%m-%d %H:%M:%S"
            endd_date=datetime.datetime.strptime(end_date,format_data)
            startd_date=datetime.datetime.strptime(start_date,format_data)

            basePipeline[0]["$match"]["created_date"]["$lte"] = (endd_date)
            basePipeline[0]["$match"]["created_date"]["$gte"] = (startd_date)
            

            result = {
                "data": {
                    "labels": [str(i) for i in range(1, 25)],
                    "datasets": [
                        {"label": "Unit 1", "data": [0 for i in range(1, 25)]},             # unit 1 = tagid_16
                        {"label": "Unit 2", "data": [0 for i in range(1, 25)]},             # unit 2 = tagid_3538
                    ],
                }
            }

        elif type == "Week":
            basePipeline[0]["$match"]["created_date"]["$gte"] = (
                datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                + UTC_OFFSET_TIMEDELTA
                - datetime.timedelta(days=7)
            )
            basePipeline[1]["$project"]["ts"] = {"$dayOfMonth": "$created_date"}
            result = {
                "data": {
                    "labels": [
                        (
                            basePipeline[0]["$match"]["created_date"]["$gte"]
                            + datetime.timedelta(days=i + 1)
                        ).strftime("%d")
                        for i in range(1, 8)
                    ],
                    "datasets": [
                        {"label": "Unit 1", "data": [0 for i in range(1, 8)]},              # unit 1 = tagid_16
                        {"label": "Unit 2", "data": [0 for i in range(1, 8)]},              # unit 2 = tagid_3538
                    ],
                }
            }

        elif type == "Month":

            date=Month
            format_data = "%Y - %m-%d"

            start_date = f'{date}-01'
            startd_date=datetime.datetime.strptime(start_date,format_data)
            
            end_date = startd_date + relativedelta( day=31)
            end_label = (end_date).strftime("%d")

            basePipeline[0]["$match"]["created_date"]["$lte"] = (end_date)
            basePipeline[0]["$match"]["created_date"]["$gte"] = (startd_date)
            basePipeline[1]["$project"]["ts"] = {"$dayOfMonth": "$created_date"}
            result = {
                "data": {
                    "labels": [
                        (
                            basePipeline[0]["$match"]["created_date"]["$gte"]
                            + datetime.timedelta(days=i + 1)
                        ).strftime("%d")
                        for i in range(-1, (int(end_label))-1)
                    ],
                    "datasets": [
                        {"label": "Unit 1", "data": [0 for i in range(-1, (int(end_label))-1)]},        # unit 1 = tagid_16
                        {"label": "Unit 2", "data": [0 for i in range(-1, (int(end_label))-1)]},        # unit 2 = tagid_3538
                    ],
                }
            }

        elif type == "Year":

            date=Year
            end_date =f'{date}-12-31 23:59:59'
            start_date = f'{date}-01-01 00:00:00'
            format_data = "%Y-%m-%d %H:%M:%S"
            endd_date=datetime.datetime.strptime(end_date,format_data)
            startd_date=datetime.datetime.strptime(start_date,format_data)

            basePipeline[0]["$match"]["created_date"]["$lte"] = (
                endd_date
            )
            basePipeline[0]["$match"]["created_date"]["$gte"] = (
                startd_date          
            )

            basePipeline[1]["$project"]["ts"] = {"$month": "$created_date"}
            result = {
                "data": {
                    "labels": [
                        (
                            basePipeline[0]["$match"]["created_date"]["$gte"]
                            + relativedelta(months=i)
                        ).strftime("%m")
                        for i in range(0, 12)
                    ],
                    "datasets": [
                        {"label": "Unit 1", "data": [0 for i in range(0, 12)]},                     # unit 1 = tagid_16
                        {"label": "Unit 2", "data": [0 for i in range(0, 12)]},                     # unit 2 = tagid_3538
                    ],
                }
            }

        output = Historian.objects().aggregate(basePipeline)
        outputDict = {}

        for data in output:
            if "_id" in data:
                ts = data["_id"]["ts"]
                tag_id = data["_id"]["tagid"]

                data_list = data.get('data', [])
                sum_list = []
                for item in data_list:
                    try:
                        sum_value = float(item)
                        sum_list.append(sum_value)
                    except ValueError:
                        pass
                
                if ts not in outputDict:
                    outputDict[ts] = {tag_id: sum_list}
                else:
                    if tag_id not in outputDict[ts]:
                        outputDict[ts][tag_id] = sum_list
                    else:
                        outputDict[ts][tag_id].append(sum_list)

        modified_labels = [i for i in range(1, 25)]

        for index, label in enumerate(result["data"]["labels"]):
            if type == "Week":
                modified_labels = [
                    (
                        basePipeline[0]["$match"]["created_date"]["$gte"]
                        + datetime.timedelta(days=i + 1)
                    ).strftime("%d-%m-%Y,%a")
                    for i in range(1, 8)
                ]
            
            elif type == "Month":
                modified_labels = [
                    (
                        basePipeline[0]["$match"]["created_date"]["$gte"]
                        + datetime.timedelta(days=i + 1)
                    ).strftime("%d/%m")
                    for i in range(-1, (int(end_label))-1)
                ]

            elif type == "Year":
                modified_labels = [
                    (
                        basePipeline[0]["$match"]["created_date"]["$gte"]
                        + relativedelta(months=i)
                    ).strftime("%b %y")
                    for i in range(0, 12)
                ]

            if int(label) in outputDict:
                for key, val in outputDict[int(label)].items():
                    total_sum = sum(val)
                    if key == 16:
                        result["data"]["datasets"][0]["data"][index] = total_sum
                    if key == 3538:
                        result["data"]["datasets"][1]["data"][index] = total_sum

        result["data"]["labels"] = copy.deepcopy(modified_labels)
        console_logger.debug(f"-------- Coal Consumption Graph Response -------- {result}")
        return result
    
    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        return e


#  x------------------------------    Coal Quality Testing Api's    ------------------------------------x


# @router.get("/extract_coal_test", tags=["Coal Testing"])
# def coal_test(start_date: Optional[str] = None, end_date: Optional[str] = None):
#     # entry = UsecaseParameters.objects.filter(Parameters__gmr_api__exists=True).first()
#     entry = UsecaseParameters.objects.first()
#     testing_ip = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Testing IP') if entry else None
#     testing_timer = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Testing Duration') if entry else None
    
#     console_logger.debug(f"---- Coal Testing IP ----            {testing_ip}")
#     console_logger.debug(f"---- Coal Testing Duration ----      {testing_timer}")

#     payload={}
#     headers = {}
#     if not end_date:
#         end_date = datetime.date.today()                                      #  end_date will always be the current date

#     if not start_date:
#         no_of_day = testing_timer.split(":")[0]
#         start_date = (end_date-timedelta(int(no_of_day))).__str__()
        
#     console_logger.debug(f" --- Test Start Date --- {start_date}")
#     console_logger.debug(f" --- Test End Date --- {end_date}")

#     coal_testing_url = f"http://{testing_ip}/limsapi/api/SampleDetails/GetSampleRecord/GetSampleRecord?Fromdate={start_date}&todate={end_date}"
#     # coal_testing_url = f"http://172.21.96.145/limsapi/api/SampleDetails/GetSampleRecord/GetSampleRecord?Fromdate={start_date}&todate={end_date}"

#     response = requests.request("GET", url = coal_testing_url,headers=headers, data=payload)
#     testing_data = json.loads(response.text)

#     wcl_extracted_data = []
#     secl_extracted_data = []

#     for entry in testing_data["responseData"]:
#         if entry["supplier"] == "WCL" and entry["rrNo"] != "" and entry["rrNo"] != "NA":

#             data = {
#                 "sample_Desc": entry["sample_Desc"],
#                 "rrNo": entry["rrNo"],
#                 "rR_Qty": entry["rR_Qty"],
#                 "rake_No": entry["rake_No"],
#                 "supplier": entry["supplier"],
#                 "receive_date": entry["sample_Received_Date"],
#                 "parameters": [] 
#             }

#             for param in entry["sample_Parameters"]:
#                 param_info = {
#                     "parameter_Name": param.get('parameter_Name').title().replace(" ","_"),
#                     "unit_Val": param["unit_Val"].title().replace(" ",""),
#                     "test_Method": param["test_Method"],
#                     "val1": param["val1"]
#                 }
#                 data["parameters"].append(param_info)
#             wcl_extracted_data.append(data)

        
#         if entry["supplier"] == "SECL" and entry["rrNo"] != "" and entry["rrNo"] != "NA":

#             secl_data = {
#                 "sample_Desc": entry["sample_Desc"],
#                 "rrNo": entry["rrNo"],
#                 "rR_Qty": entry["rR_Qty"],
#                 "rake_No": entry["rake_No"],
#                 "supplier": entry["supplier"],
#                 "receive_date": entry["sample_Received_Date"],
#                 "parameters": [] 
#             }

#             for secl_param in entry["sample_Parameters"]:
#                 param_info = {
#                     "parameter_Name": secl_param.get('parameter_Name').title().replace(" ","_"),
#                     "unit_Val": secl_param["unit_Val"].title().replace(" ",""),
#                     "test_Method": secl_param["test_Method"],
#                     "val1": secl_param["val1"]
#                 }
#                 secl_data["parameters"].append(param_info)
#             secl_extracted_data.append(secl_data)

#     for entry in wcl_extracted_data:
#         CoalTesting(
#             location = entry["sample_Desc"].upper(),
#             rrNo = entry["rrNo"],
#             rR_Qty = entry["rR_Qty"],
#             rake_no = entry["rake_No"],
#             supplier = entry["supplier"],
#             receive_date = entry["receive_date"],
#             parameters = entry["parameters"],
#             ID = CoalTesting.objects.count() + 1
#         ).save()

#     for secl_entry in secl_extracted_data:
#         CoalTestingTrain(
#             location = secl_entry["sample_Desc"].upper(),
#             rrNo = secl_entry["rrNo"],
#             rR_Qty = secl_entry["rR_Qty"],
#             rake_no = secl_entry["rake_No"],
#             supplier = secl_entry["supplier"],
#             receive_date = secl_entry["receive_date"],
#             parameters = secl_entry["parameters"],
#             ID = CoalTestingTrain.objects.count() + 1
#         ).save()
    
#     return {"message" : "Successful"}


def coal_grade_data():
    coalData = CoalGrades.objects()
    if coalData:
        coalData.delete()
    dict_data = [
        {
            "start_value": "7000",
            "end_value": "",
            "grade": "G-1",
        },
        {
            "start_value": "6700",
            "end_value": "7000",
            "grade": "G-2",
        },
        {
            "start_value": "6400",
            "end_value": "6700",
            "grade": "G-3",
        },
        {
            "start_value": "6100",
            "end_value": "6400",
            "grade": "G-4",
        },
        {
            "start_value": "5800",
            "end_value": "6100",
            "grade": "G-5",
        },
        {
            "start_value": "5500",
            "end_value": "5800",
            "grade": "G-6",
        },
        {
            "start_value": "5200",
            "end_value": "5500",
            "grade": "G-7",
        },
        {
            "start_value": "4900",
            "end_value": "5200",
            "grade": "G-8",
        },
        {
            "start_value": "4600",
            "end_value": "4900",
            "grade": "G-9",
        },
        {
            "start_value": "4300",
            "end_value": "4600",
            "grade": "G-10",
        },
        {
            "start_value": "4000",
            "end_value": "4300",
            "grade": "G-11",
        },
        {
            "start_value": "3700",
            "end_value": "4000",
            "grade": "G-12",
        },
        {
            "start_value": "3400",
            "end_value": "3700",
            "grade": "G-13",
        },
        {
            "start_value": "3100",
            "end_value": "3400",
            "grade": "G-14",
        },
        {
            "start_value": "2800",
            "end_value": "3100",
            "grade": "G-15",
        },
        {
            "start_value": "2500",
            "end_value": "2800",
            "grade": "G-16",
        },
        {
            "start_value": "2200",
            "end_value": "2500",
            "grade": "G-17",
        },
    ]

    for single_data in dict_data:
        coalgrade = CoalGrades(
            grade=single_data["grade"],
            start_value=single_data["start_value"],
            end_value=single_data["end_value"],
        )
        coalgrade.save()

    return {"detail": "success"}


@router.get("/fetchcoalgrades", tags=["Coal Testing"])
def endpoint_to_fetch_coal_grades(response: Response):
    try:
        fetchData = coal_grade_data()
        return fetchData
    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        return e


@router.on_event("startup")
async def startup_event(bg_task=BackgroundTasks()):
    bg_task.add_task(coal_grade_data())
    return


# def SchedulerResponse(job_id, status):
#     SchedulerError(JobId=job_id, ErrorMsg=status).save()
#     if len(SchedulerError.objects()) > 1000:
#         for i in SchedulerError.objects()[-1:100]:
#             i.delete()

def SchedulerResponse(job_id, status):
    SchedulerError(JobId=job_id, ErrorMsg=status).save()
    if SchedulerError.objects.count() > 1000:
        old_errors = SchedulerError.objects.order_by('Created_at')[:100]
        for error in old_errors:
            error.delete()



@router.get("/extract_coal_test", tags=["Coal Testing"])
def coal_test(start_date: Optional[str] = None, end_date: Optional[str] = None):
    success = False
    try:
        global proxies
        # entry = UsecaseParameters.objects.filter(Parameters__gmr_api__exists=True).first()
        entry = UsecaseParameters.objects.first()
        testing_ip = entry.Parameters.get("gmr_api", {}).get("roi1", {}).get("Coal Testing IP") if entry else None
        testing_timer = entry.Parameters.get("gmr_api", {}).get("roi1", {}).get("Coal Testing Duration") if entry else None

        console_logger.debug(f"---- Coal Testing IP ----            {testing_ip}")
        console_logger.debug(f"---- Coal Testing Duration ----      {testing_timer}")

        payload = {}
        headers = {}
        current_time = datetime.datetime.now(IST)
        current_date = current_time.date()
        if not end_date:
            end_date = current_date.__str__()  #  end_date will always be the current date

        if not start_date:
            no_of_day = testing_timer.split(":")[0]
            start_date = (current_date - timedelta(int(no_of_day))).__str__()

        console_logger.debug(f" --- Test Start Date --- {start_date}")
        console_logger.debug(f" --- Test End Date --- {end_date}")
        console_logger.debug(ip)
        coal_testing_url = f"http://{ip}/api/v1/host/coal_extract_data?start_date={start_date}&end_date={end_date}"
        # coal_testing_url = f"http://{testing_ip}/limsapi/api/SampleDetails/GetSampleRecord/GetSampleRecord?Fromdate={start_date}&todate={end_date}"
        # coal_testing_url = f"http://172.21.96.145/limsapi/api/SampleDetails/GetSampleRecord/GetSampleRecord?Fromdate={start_date}&todate={end_date}"
        try:
            response = requests.request("GET", url=coal_testing_url, headers=headers, data=payload, proxies=proxies)
            
            # testing_data = json.loads(response.text)
            testing_data = response.json()
            # console_logger.debug(testing_data)
            wcl_extracted_data = []
            secl_extracted_data = []

            for entry in testing_data["responseData"]:
                if entry.get("supplier") == "WCL" and entry.get("rrNo") != "" and entry.get("rrNo") != "NA":

                    data = {
                        "sample_Desc": entry.get("sample_Desc"),
                        "rrNo": entry.get("rrNo"),
                        "rR_Qty": entry.get("rR_Qty"),
                        "rake_No": entry.get("rake_No"),
                        "supplier": entry.get("supplier"),
                        "receive_date": entry.get("sample_Received_Date"),
                        "parameters": [],
                    }

                    for param in entry.get("sample_Parameters"):
                        param_info = {
                            "parameter_Name": param.get("parameter_Name")
                            .title()
                            .replace(" ", "_"),
                            "unit_Val": param.get("unit_Val").title().replace(" ",""),
                            "test_Method": param.get("test_Method"),
                            "val1": param.get("val1"),
                        }

                        if param.get("parameter_Name").title() == "Gross Calorific Value (Adb)":
                            if param.get("val1"):
                                fetchCoalGrades = CoalGrades.objects()
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        single_coal_grades["start_value"]
                                        <= param.get("val1")
                                        <= single_coal_grades["end_value"]
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        param_info["grade"] = single_coal_grades["grade"]
                                    elif param.get("val1") > "7001":
                                        # console_logger.debug("G-1")
                                        param_info["grade"] = "G-1"
                                        break

                        data["parameters"].append(param_info)
                    wcl_extracted_data.append(data)

                if (
                    entry["supplier"] == "SECL"
                    and entry["rrNo"] != ""
                    and entry["rrNo"] != "NA"
                ):

                    secl_data = {
                        "sample_Desc": entry.get("sample_Desc"),
                        "rrNo": entry.get("rrNo"),
                        "rR_Qty": entry.get("rR_Qty"),
                        "rake_No": entry.get("rake_No"),
                        "supplier": entry.get("supplier"),
                        "receive_date": entry.get("sample_Received_Date"),
                        "parameters": [],
                    }

                    for secl_param in entry["sample_Parameters"]:
                        param_info = {
                            "parameter_Name": secl_param.get("parameter_Name")
                            .title()
                            .replace(" ", "_"),
                            "unit_Val": secl_param.get("unit_Val").title().replace(" ",""),
                            "test_Method": secl_param.get("test_Method"),
                            "val1": secl_param.get("val1"),
                        }
                        secl_data["parameters"].append(param_info)
                    secl_extracted_data.append(secl_data)

            for entry in wcl_extracted_data:
                if re.sub(r'\t', '', entry.get("sample_Desc")) != "":
                    # console_logger.debug(entry.get("rrNo").strip())
                    # console_logger.debug(entry.get("rake_No").upper().strip())
                    try:
                        coalTestRoadData = CoalTesting.objects.get(rrNo=entry.get("rrNo").strip(), rake_no=entry.get("rake_No").upper().strip())
                    except DoesNotExist as e:
                        # first re removes \t from string and second re will remove multiple space and will keep only single space
                        CoalTesting(
                            location=re.sub(r'\t', '', re.sub(' +', ' ', entry.get("sample_Desc").upper().strip())),
                            rrNo=entry.get("rrNo").strip(),
                            rR_Qty=entry.get("rR_Qty").strip(),
                            rake_no=entry.get("rake_No").upper().strip(),
                            supplier=entry.get("supplier").strip(),
                            receive_date=entry.get("receive_date"),
                            parameters=entry.get("parameters"),
                            ID=CoalTesting.objects.count() + 1,
                        ).save()

            for secl_entry in secl_extracted_data:
                if re.sub(r'\t', '', secl_entry.get("sample_Desc")) != "":
                    # console_logger.debug(entry.get("rrNo").strip())
                    # console_logger.debug(entry.get("rake_No").strip())
                    try:
                        coalTestRailData = CoalTestingTrain.objects.get(rrNo=secl_entry.get("rrNo").strip(), rake_no=secl_entry.get("rake_No").strip())
                    except DoesNotExist as e:
                        CoalTestingTrain(
                            location=re.sub(r'\t', '', re.sub(' +', ' ', secl_entry.get("sample_Desc").strip())),
                            rrNo=secl_entry.get("rrNo").strip(),
                            rR_Qty=secl_entry.get("rR_Qty").strip(),
                            rake_no=secl_entry.get("rake_No").strip(),
                            supplier=secl_entry.get("supplier").strip(),
                            receive_date=secl_entry.get("receive_date"),
                            parameters=secl_entry.get("parameters"),
                            ID=CoalTestingTrain.objects.count() + 1,
                        ).save()
            success = "completed"
        except requests.exceptions.Timeout:
            console_logger.debug("Request timed out!")
        except requests.exceptions.ConnectionError:
            console_logger.debug("Connection error")
        
    except Exception as e:
        success = False
        console_logger.debug("----- Coal Testing Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        success = e
    finally:
        SchedulerResponse("save testing data", f"{success}")
        return {"message": "Successful"}



@router.get("/coal_gcv_table", tags=["Coal Testing"])
def coal_wcl_gcv_table(
    response: Response,
    currentPage: Optional[int] = None,
    perPage: Optional[int] = None,
    search_text: Optional[str] = None,
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
    month_date: Optional[str] = None,
    type: Optional[str] = "display"):
    try:
        result = {"labels": [], "datasets": [], "total": 0, "page_size": 15}

        if type and type == "display":
            page_no = 1
            page_len = result["page_size"]

            if currentPage:
                page_no = currentPage

            if perPage:
                page_len = perPage
                result["page_size"] = perPage

            data = Q()

            if start_timestamp:
                start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__gte = start_date)
            # else:
            #     start_timestamp = (datetime.datetime.now() - datetime.timedelta(days=31)).strftime("%Y-%m-%d")
            #     data &= Q(receive_date__gte = convert_to_utc_format(start_timestamp,"%Y-%m-%d").strftime("%Y-%m-%d"))

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__lte = end_date)
            # else:
            #     end_timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
            #     data &= Q(receive_date__lte = convert_to_utc_format(end_timestamp,"%Y-%m-%d").strftime("%Y-%m-%d"))

            if search_text:
                if search_text.isdigit():
                    data &= (Q(rrNo__icontains=search_text))
                else:
                    data &= Q(location__icontains=search_text) | Q(rake_no__icontains=search_text)

            offset = (page_no - 1) * page_len
            # logs = CoalTesting.objects(data).order_by("-ID").skip(offset).limit(page_len)
            logs = CoalTesting.objects(data).order_by("-ID")

            if any(logs):
                aggregated_data = defaultdict(lambda: defaultdict(lambda: {"DO_Qty": 0, "Gross_Calorific_Value_(Adb)": 0, "Third_Party_Gross_Calorific_Value_(Adb)": 0, "Third_Party_Gross_Calorific_Value_(Adb)_count": 0, "count": 0}))

                for log in logs:
                    month = log.receive_date.strftime("%Y-%m")
                    payload = log.gradepayload()
                    mine = payload["Mine"]
                    if payload.get("DO_Qty"):
                        if payload.get("DO_Qty").count('.') > 1:
                            aggregated_data[month][mine]["DO_Qty"] += float(payload.get("DO_Qty")[:5])
                        else:
                            if "," in payload["DO_Qty"]:
                                aggregated_data[month][mine]["DO_Qty"] += float(payload["DO_Qty"].replace(",", ""))
                            else:
                                aggregated_data[month][mine]["DO_Qty"] += float(payload["DO_Qty"])
                    if payload.get("Gross_Calorific_Value_(Adb)"):
                        aggregated_data[month][mine]["Gross_Calorific_Value_(Adb)"] += float(payload["Gross_Calorific_Value_(Adb)"])
                    if payload.get("Third_Party_Gross_Calorific_Value_(Adb)"):
                        aggregated_data[month][mine]["Third_Party_Gross_Calorific_Value_(Adb)"] += float(payload["Third_Party_Gross_Calorific_Value_(Adb)"])
                        aggregated_data[month][mine]["Third_Party_Gross_Calorific_Value_(Adb)_count"] += 1
                    aggregated_data[month][mine]["count"] += 1

                dataList = [
                    {"month": month, "data": {
                        mine: {
                            "average_DO_Qty": data["DO_Qty"] / data["count"],
                            "average_Gross_Calorific_Value_(Adb)": data["Gross_Calorific_Value_(Adb)"] / data["count"],
                            "average_Third_Party_Gross_Calorific_Value_(Adb)": data["Third_Party_Gross_Calorific_Value_(Adb)"] / data["Third_Party_Gross_Calorific_Value_(Adb)_count"] if data["Third_Party_Gross_Calorific_Value_(Adb)"] != 0 else "",
                        } for mine, data in aggregated_data[month].items()
                    }} for month in aggregated_data
                ]
                coal_grades = CoalGrades.objects()  # Fetch all coal grades from the database

                # Iterate through each month's data
                for month_data in dataList:
                    for key, mine_data in month_data["data"].items():
                        if mine_data["average_Gross_Calorific_Value_(Adb)"] is not None:
                            for single_coal_grades in coal_grades:
                                if single_coal_grades["end_value"] != "":
                                    if (int(single_coal_grades["start_value"]) <= int(float(mine_data.get("average_Gross_Calorific_Value_(Adb)"))) <= int(single_coal_grades["end_value"]) and single_coal_grades["start_value"] != "" and single_coal_grades["end_value"] != ""):
                                        mine_data["average_GCV_Grade"] = single_coal_grades["grade"]
                                    elif int(mine_data["average_Gross_Calorific_Value_(Adb)"]) > 7001:
                                        mine_data["average_GCV_Grade"] = "G-1"
                                        break

                        if mine_data["average_Third_Party_Gross_Calorific_Value_(Adb)"] != "":
                            for single_coal_grades in coal_grades:
                                if single_coal_grades["end_value"] != "":
                                    if (int(single_coal_grades["start_value"]) <= int(float(mine_data.get("average_Third_Party_Gross_Calorific_Value_(Adb)"))) <= int(single_coal_grades["end_value"]) and single_coal_grades["start_value"] != "" and single_coal_grades["end_value"] != ""):
                                        mine_data["average_Third_Party_GCV_Grade"] = single_coal_grades["grade"]
                                    elif int(mine_data["average_Gross_Calorific_Value_(Adb)"]) > 7001:
                                        mine_data["average_Third_Party_GCV_Grade"] = "G-1"
                                        break
                # console_logger.debug(dataList)
                final_data = []
                if month_date:
                    filtered_data = [entry for entry in dataList if entry["month"] == month_date]
                    if filtered_data:
                        data = filtered_data[0]['data']  # Extracting the 'data' dictionary from the list
                        for mine, values in data.items():
                            dictData = {}
                            dictData['Mine'] = mine
                            dictData['DO_Qty'] = round(values['average_DO_Qty'], 2)
                            dictData['GWEL_Gross_Calorific_Value_(Adb)'] = round(values['average_Gross_Calorific_Value_(Adb)'], 2)
                            if values["average_Third_Party_Gross_Calorific_Value_(Adb)"] != "":    
                                dictData['GWEL_Gross_Calorific_Value_Grade_(Adb)'] = values['average_GCV_Grade']
                                dictData["Third_Party_Gross_Calorific_Value_(Adb)"] = round(values["average_Third_Party_Gross_Calorific_Value_(Adb)"], 2)
                                if values.get("average_Third_Party_GCV_Grade"):
                                    dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"] = str(values["average_Third_Party_GCV_Grade"])
                                dictData["Difference_Gross_Calorific_Value_(Adb)"] = str(abs(int(float(dictData["GWEL_Gross_Calorific_Value_(Adb)"])) - int(float(dictData["Third_Party_Gross_Calorific_Value_(Adb)"]))))
                                if values.get("average_Third_Party_GCV_Grade"):
                                    dictData["Difference_Gross_Calorific_Value_Grade_(Adb)"] = str(abs(int(dictData['Gross_Calorific_Value_Grade_(Adb)'].replace('G-', '')) - int(dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"].replace('G-', ''))))
                            final_data.append(dictData)
                    else:
                        console_logger.debug(
                            f"No data available for the given month: {month_date}"
                        )
                        return result
                else:
                    console_logger.debug("inside else")
                    filtered_data = [entry for entry in dataList]
                    for single_data in filtered_data:
                        for mine, values in single_data['data'].items():
                            dictData = {}
                            dictData['Mine'] = mine
                            dictData['DO_Qty'] = round(values['average_DO_Qty'], 2)
                            dictData['GWEL_Gross_Calorific_Value_(Adb)'] = round(values['average_Gross_Calorific_Value_(Adb)'], 2)
                            if values["average_Third_Party_Gross_Calorific_Value_(Adb)"] != "":
                                dictData['Gross_Calorific_Value_Grade_(Adb)'] = values['average_GCV_Grade']
                                dictData["Third_Party_Gross_Calorific_Value_(Adb)"] = round(values["average_Third_Party_Gross_Calorific_Value_(Adb)"], 2)
                                if values.get("average_Third_Party_GCV_Grade"):
                                    dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"] = str(values["average_Third_Party_GCV_Grade"])
                                dictData["Difference_Gross_Calorific_Value_(Adb)"] = str(abs(int(float(dictData["GWEL_Gross_Calorific_Value_(Adb)"])) - int(float(dictData["Third_Party_Gross_Calorific_Value_(Adb)"]))))
                                if values.get("average_Third_Party_GCV_Grade"):
                                    dictData["Difference_Gross_Calorific_Value_Grade_(Adb)"] = str(abs(int(dictData['Gross_Calorific_Value_Grade_(Adb)'].replace('G-', '')) - int(dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"].replace('G-', ''))))

                            final_data.append(dictData)
                # console_logger.debug(final_data)
                # Perform pagination here using list slicing
                start_index = (page_no - 1) * page_len
                end_index = start_index + page_len
                paginated_data = final_data[start_index:end_index]

                # result["labels"] = list(final_data[0].keys())

                unique_keys = OrderedDict()

                for data in paginated_data:
                    for key in data.keys():
                        unique_keys[key] = None

                result["labels"] = list(unique_keys.keys())

                result["total"] = len(final_data)
                result["datasets"] = paginated_data
                # console_logger.debug(result)
                return result
            else:
                return result
        
        elif type and type == "download":
            del type
            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)
            
            data = Q()

            if start_timestamp:
                start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__gte = start_date)

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__lte = end_date)
            
            if search_text:
                if search_text.isdigit():
                    data &= (Q(rrNo__icontains=search_text))
                else:
                    data &= Q(location__icontains=search_text) | Q(rake_no__icontains=search_text)

            usecase_data = CoalTesting.objects(data).order_by("-receive_date")
            count = len(usecase_data)
            path = None
            if usecase_data:
                try:
                    path = os.path.join(
                        "static_server",
                        "gmr_ai",
                        file,
                        "Roadwise_GCV_Grade_Comparision_Report_{}.xlsx".format(
                            datetime.datetime.now().strftime("%Y-%m-%d:%H:%M:%S"),
                        ),
                    )
                    filename = os.path.join(os.getcwd(), path)
                    workbook = xlsxwriter.Workbook(filename)
                    workbook.use_zip64()
                    cell_format2 = workbook.add_format()
                    cell_format2.set_bold()
                    cell_format2.set_font_size(10)
                    cell_format2.set_align("center")
                    cell_format2.set_align("vjustify")

                    worksheet = workbook.add_worksheet()
                    worksheet.set_column("A:AZ", 20)
                    worksheet.set_default_row(50)
                    cell_format = workbook.add_format()
                    cell_format.set_font_size(10)
                    cell_format.set_align("center")
                    cell_format.set_align("vcenter")

                    headers = [
                        "Sr.No",
                        "Mine",
                        "DO_Qty",
                        "GWEL_Gross_Calorific_Value_(Adb)",
                        "GWEL_Gross_Calorific_Value_Grade",
                        "Third_Party_Gross_Calorific_Value_(Adb)",
                        "Third_Party_Gross_Calorific_Value_(Adb)_grade",
                        "Difference_Gross_Calorific_Value(Adb)",
                        "Difference_Gross_Calorific_Value_Grade_(Adb)"
                    ]

                    for index, header in enumerate(headers):
                        worksheet.write(0, index, header, cell_format2)


                    if any(usecase_data):
                        aggregated_data = defaultdict(lambda: defaultdict(lambda: {"DO_Qty": 0, "Gross_Calorific_Value_(Adb)": 0, "Third_Party_Gross_Calorific_Value_(Adb)": 0, "Third_Party_Gross_Calorific_Value_(Adb)_count": 0, "count": 0}))

                        for log in usecase_data:
                            month = log.receive_date.strftime("%Y-%m")
                            payload = log.gradepayload()
                            mine = payload["Mine"]
                            if payload.get("DO_Qty"):
                                if payload.get("DO_Qty").count('.') > 1:
                                    aggregated_data[month][mine]["DO_Qty"] += float(payload.get("DO_Qty")[:5])
                                else:
                                    aggregated_data[month][mine]["DO_Qty"] += float(payload["DO_Qty"])
                            if payload.get("Gross_Calorific_Value_(Adb)"):
                                aggregated_data[month][mine]["Gross_Calorific_Value_(Adb)"] += float(payload["Gross_Calorific_Value_(Adb)"]) 
                            if payload.get("Third_Party_Gross_Calorific_Value_(Adb)"):
                                aggregated_data[month][mine]["Third_Party_Gross_Calorific_Value_(Adb)"] += float(payload["Third_Party_Gross_Calorific_Value_(Adb)"])
                                aggregated_data[month][mine]["Third_Party_Gross_Calorific_Value_(Adb)_count"] += 1
                
                            aggregated_data[month][mine]["count"] += 1

                        
                        # console_logger.debug(aggregated_data)

                        dataList = [
                            {"month": month, "data": {
                                mine: {
                                    "average_DO_Qty": data["DO_Qty"] / data["count"],
                                    "average_Gross_Calorific_Value_(Adb)": data["Gross_Calorific_Value_(Adb)"] / data["count"],
                                    "average_Third_Party_Gross_Calorific_Value_(Adb)": data["Third_Party_Gross_Calorific_Value_(Adb)"] / data["Third_Party_Gross_Calorific_Value_(Adb)_count"] if data["Third_Party_Gross_Calorific_Value_(Adb)"] != 0 else "",
                                } for mine, data in aggregated_data[month].items()
                            }} for month in aggregated_data
                        ]
                        coal_grades = CoalGrades.objects()  # Fetch all coal grades from the database

                        # Iterate through each month's data
                        for month_data in dataList:
                            for key, mine_data in month_data["data"].items():
                                if mine_data["average_Gross_Calorific_Value_(Adb)"] is not None:
                                    for single_coal_grades in coal_grades:
                                        if single_coal_grades["end_value"] != "":
                                            if (int(single_coal_grades["start_value"]) <= int(float(mine_data.get("average_Gross_Calorific_Value_(Adb)"))) <= int(single_coal_grades["end_value"]) and single_coal_grades["start_value"] != "" and single_coal_grades["end_value"] != ""):
                                                mine_data["average_GCV_Grade"] = single_coal_grades["grade"]
                                            elif int(mine_data["average_Gross_Calorific_Value_(Adb)"]) > 7001:
                                                mine_data["average_GCV_Grade"] = "G-1"
                                                break

                                if mine_data["average_Third_Party_Gross_Calorific_Value_(Adb)"] != "":
                                    for single_coal_grades in coal_grades:
                                        if single_coal_grades["end_value"] != "":
                                            if (int(single_coal_grades["start_value"]) <= int(float(mine_data.get("average_Third_Party_Gross_Calorific_Value_(Adb)"))) <= int(single_coal_grades["end_value"]) and single_coal_grades["start_value"] != "" and single_coal_grades["end_value"] != ""):
                                                mine_data["average_Third_Party_GCV_Grade"] = single_coal_grades["grade"]
                                            elif int(mine_data["average_Gross_Calorific_Value_(Adb)"]) > 7001:
                                                mine_data["average_Third_Party_GCV_Grade"] = "G-1"
                                                break
                    final_data = []
                    if month_date:
                        filtered_data = [entry for entry in dataList if entry["month"] == month_date]
                        if filtered_data:
                            data = filtered_data[0]['data']  # Extracting the 'data' dictionary from the list
                            for mine, values in data.items():
                                dictData = {}
                                dictData['Mine'] = mine
                                dictData['DO_Qty'] = round(values['average_DO_Qty'], 2)
                                dictData['Gross_Calorific_Value_(Adb)'] = round(values['average_Gross_Calorific_Value_(Adb)'], 2)
                                if values["average_Third_Party_Gross_Calorific_Value_(Adb)"] != "":    
                                    dictData['Gross_Calorific_Value_Grade_(Adb)'] = values['average_GCV_Grade']
                                    dictData["Third_Party_Gross_Calorific_Value_(Adb)"] = round(values["average_Third_Party_Gross_Calorific_Value_(Adb)"], 2)
                                    if values.get("average_Third_Party_GCV_Grade"):
                                        dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"] = str(values["average_Third_Party_GCV_Grade"])
                                    dictData["Difference_Gross_Calorific_Value_(Adb)"] = str(abs(int(float(dictData["Gross_Calorific_Value_(Adb)"])) - int(float(dictData["Third_Party_Gross_Calorific_Value_(Adb)"]))))
                                    if values.get("average_Third_Party_GCV_Grade"):
                                        dictData["Difference_Gross_Calorific_Value_Grade_(Adb)"] = str(abs(int(dictData['Gross_Calorific_Value_Grade_(Adb)'].replace('G-', '')) - int(dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"].replace('G-', ''))))
                                final_data.append(dictData)
                        else:
                            console_logger.debug(
                                f"No data available for the given month: {month_date}"
                            )
                            return {"message": f"No data available for the given month: {month_date}"}
                    else:
                        filtered_data = [entry for entry in dataList]
                        # data = filtered_data[0]['data']  # Extracting the 'data' dictionary from the list
                        for single_data in filtered_data:
                            for mine, values in single_data['data'].items():
                                dictData = {}
                                dictData['Mine'] = mine
                                dictData['DO_Qty'] = round(values['average_DO_Qty'], 2)
                                dictData['Gross_Calorific_Value_(Adb)'] = round(values['average_Gross_Calorific_Value_(Adb)'], 2)
                                if values["average_Third_Party_Gross_Calorific_Value_(Adb)"] != "":
                                    dictData['Gross_Calorific_Value_Grade_(Adb)'] = values['average_GCV_Grade']
                                    dictData["Third_Party_Gross_Calorific_Value_(Adb)"] = round(values["average_Third_Party_Gross_Calorific_Value_(Adb)"],2)
                                    if values.get("average_Third_Party_GCV_Grade"):
                                        dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"] = str(values["average_Third_Party_GCV_Grade"])
                                    dictData["Difference_Gross_Calorific_Value_(Adb)"] = str(abs(int(float(dictData["Gross_Calorific_Value_(Adb)"])) - int(float(dictData["Third_Party_Gross_Calorific_Value_(Adb)"]))))
                                    if values.get("average_Third_Party_GCV_Grade"):
                                        dictData["Difference_Gross_Calorific_Value_Grade_(Adb)"] = str(abs(int(dictData['Gross_Calorific_Value_Grade_(Adb)'].replace('G-', '')) - int(dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"].replace('G-', ''))))
                                final_data.append(dictData)
                    result["labels"] = list(final_data[0].keys())
                    result["total"] = len(final_data)
                    # final_data.append(countDict)
                    result["datasets"] = final_data

                    row = 1
                    for single_data in result["datasets"]:
                        worksheet.write(row, 0, count, cell_format)
                        worksheet.write(row, 1, single_data["Mine"])
                        worksheet.write(row, 2, single_data["DO_Qty"])
                        worksheet.write(row, 3, single_data["Gross_Calorific_Value_(Adb)"])
                        if single_data.get("Gross_Calorific_Value_Grade_(Adb)") != "" and single_data.get("Gross_Calorific_Value_Grade_(Adb)") != None:
                            worksheet.write(row, 4, str(single_data["Gross_Calorific_Value_Grade_(Adb)"]), cell_format)
                        if single_data.get("Third_Party_Gross_Calorific_Value_(Adb)") != "" and single_data.get("Third_Party_Gross_Calorific_Value_(Adb)") != None:
                            worksheet.write(row, 5, str(single_data["Third_Party_Gross_Calorific_Value_(Adb)"]), cell_format)
                        if single_data.get("Third_Party_Gross_Calorific_Value_(Adb)_grade") != "" and single_data.get("Third_Party_Gross_Calorific_Value_(Adb)_grade") != None:
                            worksheet.write(row, 6, str(single_data["Third_Party_Gross_Calorific_Value_(Adb)_grade"]), cell_format)
                        if single_data.get("Difference_Gross_Calorific_Value_(Adb)") != "" and single_data.get("Difference_Gross_Calorific_Value_(Adb)") != None:
                            worksheet.write(row, 7, str(single_data["Difference_Gross_Calorific_Value_(Adb)"]), cell_format)
                        if single_data.get("Difference_Gross_Calorific_Value_Grade_(Adb)") != "" and single_data.get("Difference_Gross_Calorific_Value_Grade_(Adb)") != None:
                            worksheet.write(row, 8, str(single_data["Difference_Gross_Calorific_Value_Grade_(Adb)"]), cell_format)
                        count -= 1
                        row += 1
                    workbook.close()

                    # console_logger.debug("Successfully {} report generated".format(service_id))
                    # console_logger.debug("sent data {}".format(path))
                    return {
                            "Type": "coal_test_download_event",
                            "Datatype": "Report",
                            "File_Path": path,
                        }
                except Exception as e:
                    console_logger.debug(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
                    console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
            else:
                console_logger.error("No data found")
                return {
                            "Type": "coal_test_download_event",
                            "Datatype": "Report",
                            "File_Path": path,
                        }
    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/coal_gcv_table_train", tags=["Coal Testing"])
def coal_wcl_gcv_table(
    response: Response,
    currentPage: Optional[int] = None,
    perPage: Optional[int] = None,
    search_text: Optional[str] = None,
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
    month_date: Optional[str] = None,
    type: Optional[str] = "display"):
    try:
        result = {"labels": [], "datasets": [], "total": 0, "page_size": 15}

        if type and type == "display":
            page_no = 1
            page_len = result["page_size"]

            if currentPage:
                page_no = currentPage

            if perPage:
                page_len = perPage
                result["page_size"] = perPage

            data = Q()

            if start_timestamp:
                start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__gte = start_date)
            # else:
            #     start_timestamp = (datetime.datetime.now() - datetime.timedelta(days=31)).strftime("%Y-%m-%d")
            #     data &= Q(receive_date__gte = convert_to_utc_format(start_timestamp,"%Y-%m-%d").strftime("%Y-%m-%d"))

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__lte = end_date)
            # else:
            #     end_timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
            #     data &= Q(receive_date__lte = convert_to_utc_format(end_timestamp,"%Y-%m-%d").strftime("%Y-%m-%d"))


            if search_text:
                if search_text.isdigit():
                    data &= (Q(rrNo__icontains=search_text))
                else:
                    data &= Q(location__icontains=search_text) | Q(rake_no__icontains=search_text)

            offset = (page_no - 1) * page_len
            # logs = CoalTestingTrain.objects(data).order_by("-ID").skip(offset).limit(page_len)
            logs = CoalTestingTrain.objects(data).order_by("-ID")

            if any(logs):
                aggregated_data = defaultdict(lambda: defaultdict(lambda: {"RR_Qty": 0, "Gross_Calorific_Value_(Adb)": 0, "Third_Party_Gross_Calorific_Value_(Adb)": 0, "Third_Party_Gross_Calorific_Value_(Adb)_count": 0, "count": 0}))

                for log in logs:
                    month = log.receive_date.strftime("%Y-%m")
                    payload = log.gradepayload()
                    mine = payload["Mine"]
                    if payload.get("RR_Qty"):
                        if payload.get("RR_Qty").count('.') > 1:
                            aggregated_data[month][mine]["RR_Qty"] += float(payload.get("RR_Qty")[:5])
                        else:
                            aggregated_data[month][mine]["RR_Qty"] += float(payload.get("RR_Qty"))
                    if payload.get("Gross_Calorific_Value_(Adb)"):
                        aggregated_data[month][mine]["Gross_Calorific_Value_(Adb)"] += float(payload["Gross_Calorific_Value_(Adb)"])
                    if payload.get("Third_Party_Gross_Calorific_Value_(Adb)"):
                        aggregated_data[month][mine]["Third_Party_Gross_Calorific_Value_(Adb)"] += float(payload["Third_Party_Gross_Calorific_Value_(Adb)"])
                        aggregated_data[month][mine]["Third_Party_Gross_Calorific_Value_(Adb)_count"] += 1
                    aggregated_data[month][mine]["count"] += 1

                dataList = [
                    {"month": month, "data": {
                        mine: {
                            "average_RR_Qty": data["RR_Qty"] / data["count"],
                            "average_Gross_Calorific_Value_(Adb)": data["Gross_Calorific_Value_(Adb)"] / data["count"],
                            "average_Third_Party_Gross_Calorific_Value_(Adb)": data["Third_Party_Gross_Calorific_Value_(Adb)"] / data["Third_Party_Gross_Calorific_Value_(Adb)_count"] if data["Third_Party_Gross_Calorific_Value_(Adb)"] != 0 else "",
                        } for mine, data in aggregated_data[month].items()
                    }} for month in aggregated_data
                ]
                coal_grades = CoalGrades.objects()  # Fetch all coal grades from the database

                # Iterate through each month's data
                for month_data in dataList:
                    for key, mine_data in month_data["data"].items():
                        if mine_data["average_Gross_Calorific_Value_(Adb)"] is not None:
                            for single_coal_grades in coal_grades:
                                if single_coal_grades["end_value"] != "":
                                    if (int(single_coal_grades["start_value"]) <= int(float(mine_data.get("average_Gross_Calorific_Value_(Adb)"))) <= int(single_coal_grades["end_value"]) and single_coal_grades["start_value"] != "" and single_coal_grades["end_value"] != ""):
                                        mine_data["average_GCV_Grade"] = single_coal_grades["grade"]
                                    elif int(mine_data["average_Gross_Calorific_Value_(Adb)"]) > 7001:
                                        mine_data["average_GCV_Grade"] = "G-1"
                                        break

                        if mine_data["average_Third_Party_Gross_Calorific_Value_(Adb)"] != "":
                            for single_coal_grades in coal_grades:
                                if single_coal_grades["end_value"] != "":
                                    if (int(single_coal_grades["start_value"]) <= int(float(mine_data.get("average_Third_Party_Gross_Calorific_Value_(Adb)"))) <= int(single_coal_grades["end_value"]) and single_coal_grades["start_value"] != "" and single_coal_grades["end_value"] != ""):
                                        mine_data["average_Third_Party_GCV_Grade"] = single_coal_grades["grade"]
                                    elif int(mine_data["average_Gross_Calorific_Value_(Adb)"]) > 7001:
                                        mine_data["average_Third_Party_GCV_Grade"] = "G-1"
                                        break
                final_data = []
                if month_date:
                    filtered_data = [entry for entry in dataList if entry["month"] == month_date]
                    if filtered_data:
                        data = filtered_data[0]['data']  # Extracting the 'data' dictionary from the list
                        for mine, values in data.items():
                            # console_logger.debug(values)
                            dictData = {}
                            dictData['Mine'] = mine
                            dictData['RR_Qty'] = round(values['average_RR_Qty'], 2)
                            dictData['GWEL_Gross_Calorific_Value_(Adb)'] = round(values['average_Gross_Calorific_Value_(Adb)'], 2)

                            if values["average_Third_Party_Gross_Calorific_Value_(Adb)"] != "":
                                # dictData['Gross_Calorific_Value_Grade_(Adb)'] = values['average_Third_Party_GCV_Grade']
                                dictData['GWEL_Gross_Calorific_Value_Grade_(Adb)'] = values['average_GCV_Grade']
                                dictData["Third_Party_Gross_Calorific_Value_(Adb)"] = round(values["average_Third_Party_Gross_Calorific_Value_(Adb)"], 2)
                                if values.get("average_Third_Party_GCV_Grade"):
                                    dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"] = str(values["average_Third_Party_GCV_Grade"])
                                dictData["Difference_Gross_Calorific_Value_(Adb)"] = str(abs(int(float(dictData["GWEL_Gross_Calorific_Value_(Adb)"])) - int(float(dictData["Third_Party_Gross_Calorific_Value_(Adb)"]))))
                                if values.get("average_Third_Party_GCV_Grade"):
                                    dictData["Difference_Gross_Calorific_Value_Grade_(Adb)"] = str(abs(int(dictData['Gross_Calorific_Value_Grade_(Adb)'].replace('G-', ''))) - int(dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"].replace('G-', '')))
                            final_data.append(dictData)
                    else:
                        # console_logger.debug("No data available for the given month:", month_date)
                        return result
                else:
                    console_logger.debug("inside else")
                    filtered_data = [entry for entry in dataList]
                    for single_data in filtered_data:
                        for mine, values in single_data['data'].items():
                            dictData = {}
                            dictData['Mine'] = mine
                            dictData['RR_Qty'] = round(values['average_RR_Qty'], 2)
                            dictData['GWEL_Gross_Calorific_Value_(Adb)'] = round(values['average_Gross_Calorific_Value_(Adb)'], 2)
                            if values["average_Third_Party_Gross_Calorific_Value_(Adb)"] != "":
                                # dictData['Gross_Calorific_Value_Grade_(Adb)'] = values['average_Third_Party_GCV_Grade']
                                dictData['GWEL_Gross_Calorific_Value_Grade_(Adb)'] = values['average_GCV_Grade']
                                dictData["Third_Party_Gross_Calorific_Value_(Adb)"] = round(values["average_Third_Party_Gross_Calorific_Value_(Adb)"], 2)
                                if values.get("average_Third_Party_GCV_Grade"):
                                    dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"] = str(values["average_Third_Party_GCV_Grade"])
                                dictData["Difference_Gross_Calorific_Value_(Adb)"] = str(abs(int(float(dictData["GWEL_Gross_Calorific_Value_(Adb)"])) - int(float(dictData["Third_Party_Gross_Calorific_Value_(Adb)"]))))
                                if values.get("average_Third_Party_GCV_Grade"):
                                    dictData["Difference_Gross_Calorific_Value_Grade_(Adb)"] = str(abs(int(dictData['Gross_Calorific_Value_Grade_(Adb)'].replace('G-', ''))) - int(dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"].replace('G-', '')))
                           
                            final_data.append(dictData)

                start_index = (page_no - 1) * page_len
                end_index = start_index + page_len
                paginated_data = final_data[start_index:end_index]

                # result["labels"] = list(final_data[0].keys())
                unique_keys = OrderedDict()

                for data in paginated_data:
                    for key in data.keys():
                        unique_keys[key] = None

                result["labels"] = list(unique_keys.keys())
                result["total"] = len(final_data)
                result["datasets"] = paginated_data

                return result
            else:
                return result
        
        elif type and type == "download":
            del type
            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)
            
            data = Q()

            if start_timestamp:
                start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__gte = start_date)

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__lte = end_date)

            if search_text:
                if search_text.isdigit():
                    data &= (Q(rrNo__icontains=search_text))
                else:
                    data &= Q(location__icontains=search_text) | Q(rake_no__icontains=search_text)

            usecase_data = CoalTestingTrain.objects(data).order_by("-receive_date")
            count = len(usecase_data)
            path = None
            if usecase_data:
                try:
                    path = os.path.join(
                        "static_server",
                        "gmr_ai",
                        file,
                        "Railwise_GCV_Grade_Comparision_Report_{}.xlsx".format(
                            datetime.datetime.now().strftime("%Y-%m-%d:%H:%M:%S"),
                        ),
                    )
                    filename = os.path.join(os.getcwd(), path)
                    workbook = xlsxwriter.Workbook(filename)
                    workbook.use_zip64()
                    cell_format2 = workbook.add_format()
                    cell_format2.set_bold()
                    cell_format2.set_font_size(10)
                    cell_format2.set_align("center")
                    cell_format2.set_align("vjustify")

                    worksheet = workbook.add_worksheet()
                    worksheet.set_column("A:AZ", 20)
                    worksheet.set_default_row(50)
                    cell_format = workbook.add_format()
                    cell_format.set_font_size(10)
                    cell_format.set_align("center")
                    cell_format.set_align("vcenter")

                    headers = [
                        "Sr.No",
                        "Mine",
                        "RR_Qty",
                        "GWEL_Gross_Calorific_Value_(Adb)",
                        "GWEL_Gross_Calorific_Value_Grade",
                        "Third_Party_Gross_Calorific_Value_(Adb)",
                        "Third_Party_Gross_Calorific_Value_(Adb)_grade",
                        "Difference_Gross_Calorific_Value_(Adb)",
                        "Difference_Gross_Calorific_Value_Grade_(Adb)"
                    ]

                    for index, header in enumerate(headers):
                        worksheet.write(0, index, header, cell_format2)

                    if any(usecase_data):
                        aggregated_data = defaultdict(lambda: defaultdict(lambda: {"RR_Qty": 0, "Gross_Calorific_Value_(Adb)": 0, "Third_Party_Gross_Calorific_Value_(Adb)": 0, "Third_Party_Gross_Calorific_Value_(Adb)_count": 0, "count": 0}))

                        for log in usecase_data:
                            month = log.receive_date.strftime("%Y-%m")
                            payload = log.gradepayload()
                            mine = payload["Mine"]
                            if payload.get("RR_Qty"):
                                if payload.get("RR_Qty").count('.') > 1:
                                    aggregated_data[month][mine]["RR_Qty"] += float(payload.get("RR_Qty")[:5])
                                else:
                                    aggregated_data[month][mine]["RR_Qty"] += float(payload.get("RR_Qty"))
                            if payload.get("Gross_Calorific_Value_(Adb)"):
                                aggregated_data[month][mine]["Gross_Calorific_Value_(Adb)"] += float(payload["Gross_Calorific_Value_(Adb)"])

                            if payload.get("Third_Party_Gross_Calorific_Value_(Adb)"):
                                aggregated_data[month][mine]["Third_Party_Gross_Calorific_Value_(Adb)"] += float(payload["Third_Party_Gross_Calorific_Value_(Adb)"])
                                aggregated_data[month][mine]["Third_Party_Gross_Calorific_Value_(Adb)_count"] += 1
                            aggregated_data[month][mine]["count"] += 1


                        dataList = [
                            {"month": month, "data": {
                                mine: {
                                    "average_RR_Qty": data["RR_Qty"] / data["count"],
                                    "average_Gross_Calorific_Value_(Adb)": data["Gross_Calorific_Value_(Adb)"] / data["count"],
                                    "average_Third_Party_Gross_Calorific_Value_(Adb)": data["Third_Party_Gross_Calorific_Value_(Adb)"] / data["Third_Party_Gross_Calorific_Value_(Adb)_count"] if data["Third_Party_Gross_Calorific_Value_(Adb)"] != 0 else "",
                                } for mine, data in aggregated_data[month].items()
                            }} for month in aggregated_data
                        ]
                        coal_grades = CoalGrades.objects()  # Fetch all coal grades from the database

                        # Iterate through each month's data
                        for month_data in dataList:
                            for key, mine_data in month_data["data"].items():
                                if mine_data["average_Gross_Calorific_Value_(Adb)"] is not None:
                                    for single_coal_grades in coal_grades:
                                        if single_coal_grades["end_value"] != "":
                                            if (int(single_coal_grades["start_value"]) <= int(float(mine_data.get("average_Gross_Calorific_Value_(Adb)"))) <= int(single_coal_grades["end_value"]) and single_coal_grades["start_value"] != "" and single_coal_grades["end_value"] != ""):
                                                mine_data["average_GCV_Grade"] = single_coal_grades["grade"]
                                            elif int(mine_data["average_Gross_Calorific_Value_(Adb)"]) > 7001:
                                                mine_data["average_GCV_Grade"] = "G-1"
                                                break
                        

                                if mine_data["average_Third_Party_Gross_Calorific_Value_(Adb)"] != "":
                                    for single_coal_grades in coal_grades:
                                        if single_coal_grades["end_value"] != "":
                                            if (int(single_coal_grades["start_value"]) <= int(float(mine_data.get("average_Third_Party_Gross_Calorific_Value_(Adb)"))) <= int(single_coal_grades["end_value"]) and single_coal_grades["start_value"] != "" and single_coal_grades["end_value"] != ""):
                                                mine_data["average_Third_Party_GCV_Grade"] = single_coal_grades["grade"]
                                            elif int(mine_data["average_Gross_Calorific_Value_(Adb)"]) > 7001:
                                                mine_data["average_Third_Party_GCV_Grade"] = "G-1"
                                                break
                    final_data = []
                    if month_date:
                        filtered_data = [entry for entry in dataList if entry["month"] == month_date]
                        if filtered_data:
                            data = filtered_data[0]['data']  # Extracting the 'data' dictionary from the list
                            for mine, values in data.items():
                                dictData = {}
                                dictData['Mine'] = mine
                                dictData['RR_Qty'] = str(values['average_RR_Qty'])
                                dictData['Gross_Calorific_Value_(Adb)'] = str(values['average_Gross_Calorific_Value_(Adb)'])
                                if values["average_Third_Party_Gross_Calorific_Value_(Adb)"] != "":    
                                    dictData['Gross_Calorific_Value_Grade_(Adb)'] = values['average_GCV_Grade']
                                    dictData["Third_Party_Gross_Calorific_Value_(Adb)"] = str(values["average_Third_Party_Gross_Calorific_Value_(Adb)"])
                                    if values.get("average_Third_Party_GCV_Grade"):
                                        dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"] = str(values["average_Third_Party_GCV_Grade"])
                                        dictData["Difference_Gross_Calorific_Value_Grade_(Adb)"] = str(abs(int(dictData['Gross_Calorific_Value_Grade_(Adb)'].replace('G-', ''))) - int(dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"].replace('G-', '')))
                                    dictData["Difference_Gross_Calorific_Value_(Adb)"] = str(abs(int(float(dictData["Gross_Calorific_Value_(Adb)"])) - int(float(dictData["Third_Party_Gross_Calorific_Value_(Adb)"]))))
                                final_data.append(dictData)
                        else:
                            console_logger.debug("No data available for the given month:", month_date)
                            return {"message": f"No data available for the given month: {month_date}"}
                    else:
                        console_logger.debug("inside else")
                        filtered_data = [entry for entry in dataList]
                        # data = filtered_data[0]['data']  # Extracting the 'data' dictionary from the list
                        for single_data in filtered_data:
                            for mine, values in single_data['data'].items():
                                dictData = {}
                                dictData['Mine'] = mine
                                dictData['RR_Qty'] = str(values['average_RR_Qty'])
                                dictData['Gross_Calorific_Value_(Adb)'] = str(values['average_Gross_Calorific_Value_(Adb)'])
                                if values["average_Third_Party_Gross_Calorific_Value_(Adb)"] != "":
                                    dictData['Gross_Calorific_Value_Grade_(Adb)'] = values['average_GCV_Grade']
                                    dictData["Third_Party_Gross_Calorific_Value_(Adb)"] = str(values["average_Third_Party_Gross_Calorific_Value_(Adb)"])
                                    if values.get("average_Third_Party_GCV_Grade"):
                                        dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"] = str(values["average_Third_Party_GCV_Grade"])
                                        dictData["Difference_Gross_Calorific_Value_Grade_(Adb)"] = str(abs(int(dictData['Gross_Calorific_Value_Grade_(Adb)'].replace('G-', ''))) - int(dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"].replace('G-', '')))
                                    dictData["Difference_Gross_Calorific_Value_(Adb)"] = str(abs(int(float(dictData["Gross_Calorific_Value_(Adb)"])) - int(float(dictData["Third_Party_Gross_Calorific_Value_(Adb)"]))))
                                final_data.append(dictData)
                    result["labels"] = list(final_data[0].keys())
                    result["total"] = len(final_data)
                    result["datasets"] = final_data

                    row = 1
                    for single_data in result["datasets"]:
                        worksheet.write(row, 0, count, cell_format)
                        worksheet.write(row, 1, single_data["Mine"])
                        worksheet.write(row, 2, single_data["RR_Qty"])
                        worksheet.write(row, 3, single_data["Gross_Calorific_Value_(Adb)"])
                        if single_data.get("Gross_Calorific_Value_Grade_(Adb)") != "" and single_data.get("Gross_Calorific_Value_Grade_(Adb)") != None:
                            worksheet.write(row, 4, str(single_data["Gross_Calorific_Value_Grade_(Adb)"]), cell_format)
                        if single_data.get("Third_Party_Gross_Calorific_Value_(Adb)") != "" and single_data.get("Third_Party_Gross_Calorific_Value_(Adb)") != None:
                            worksheet.write(row, 5, str(single_data["Third_Party_Gross_Calorific_Value_(Adb)"]), cell_format)
                        if single_data.get("Third_Party_Gross_Calorific_Value_(Adb)_grade") != "" and single_data.get("Third_Party_Gross_Calorific_Value_(Adb)_grade") != None:
                            worksheet.write(row, 6, str(single_data["Third_Party_Gross_Calorific_Value_(Adb)_grade"]), cell_format)
                        if single_data.get("Difference_Gross_Calorific_Value_(Adb)") != "" and single_data.get("Difference_Gross_Calorific_Value_(Adb)") != None:
                            worksheet.write(row, 7, str(single_data["Difference_Gross_Calorific_Value_(Adb)"]), cell_format)
                        if single_data.get("Difference_Gross_Calorific_Value_Grade_(Adb)") != "" and single_data.get("Difference_Gross_Calorific_Value_Grade_(Adb)") != None:
                            worksheet.write(row, 8, str(single_data["Difference_Gross_Calorific_Value_Grade_(Adb)"]), cell_format)
                        count -= 1
                        row += 1
                    workbook.close()

                    console_logger.debug("Successfully {} report generated".format(service_id))
                    console_logger.debug("sent data {}".format(path))
                    return {
                            "Type": "coal_test_download_event",
                            "Datatype": "Report",
                            "File_Path": path,
                        }
                except Exception as e:
                    console_logger.debug(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
                    console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
            else:
                console_logger.error("No data found")
                return {
                            "Type": "coal_test_download_event",
                            "Datatype": "Report",
                            "File_Path": path,
                        }
    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/road/fetchyearmonth", tags=["Coal Testing"])
def endpoint_to_fetch_road_year_month(response:Response):
    try:
        dataList = []
        fetchCoaldates = CoalTesting.objects()
        for fetchCoaldate in fetchCoaldates:
            yearMonth = datetime.datetime.strptime(str(fetchCoaldate.receive_date),'%Y-%m-%d %H:%M:%S').strftime('%Y-%m')
            if yearMonth not in dataList:
                dataList.append(yearMonth)
        return dataList
    except Exception as e:
        console_logger.debug("----- Road Vehicle Count Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e
    

@router.get("/rail/fetchyearmonth", tags=["Coal Testing"])
def endpoint_to_fetch_road_year_month(response:Response):
    try:
        dataList = []
        fetchCoaldates = CoalTestingTrain.objects()
        for fetchCoaldate in fetchCoaldates:
            yearMonth = datetime.datetime.strptime(str(fetchCoaldate.receive_date),'%Y-%m-%d %H:%M:%S').strftime('%Y-%m')
            if yearMonth not in dataList:
                dataList.append(yearMonth)
        return dataList
    except Exception as e:
        console_logger.debug("----- Road Vehicle Count Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.post("/update_wcl/testing", tags=["Coal Testing"])
def update_wcl_testing(response:Response,data: wclData):
    try:
        dataLoad = data.dict()
        fetchCoaltesting = CoalTesting.objects.get(id=dataLoad.get("id"))
        if fetchCoaltesting:
            for param in fetchCoaltesting.parameters:
                param_name = f"{param.get('parameter_Name')}_{param.get('unit_Val')}"
                if dataLoad.get("coal_data").get(param_name) is not None:
                    param["val1"] = dataLoad.get("coal_data").get(param_name)
        fetchCoaltesting.save()

        return {"detail": "success"}

    except Exception as e:
        console_logger.debug("----- Error updating WCL testing -----", e)
        response.status_code = 400
        return e


@router.post("/update_secl/testing", tags=["Coal Testing"])
def update_secl_testing(response:Response,data: seclData):
    try:
        dataLoad = data.dict()
        fetchCoaltesting = CoalTestingTrain.objects.get(id=dataLoad.get("id"))
        if fetchCoaltesting:
            for param in fetchCoaltesting.parameters:
                param_name = f"{param.get('parameter_Name')}_{param.get('unit_Val').replace(' ', '')}"
                if dataLoad.get("coal_data").get(param_name) is not None:
                    param["val1"] = dataLoad.get("coal_data").get(param_name)
        fetchCoaltesting.save()

        return {"detail": "success"}

    except Exception as e:
        console_logger.debug("----- Error updating SECL testing -----", e)
        response.status_code = 400
        return e


@router.post("/coal_test_wcl_addon", tags=["Coal Testing"])
# def wcl_addon_data(response: Response, data: WCLtest):
def wcl_addon_data(response: Response, paydata: WCLtestMain):
    try:
        console_logger.debug(paydata.dict())
        multyData = paydata.dict()
        for dataLoad in multyData["data"]:
            console_logger.debug(dataLoad)
            fetchCoaltesting = CoalTesting.objects.get(id=dataLoad.get("id"))
            fetchCoaltesting.third_party_report_no = dataLoad.get("coal_data").get("Third_Party_Report_No")
            if fetchCoaltesting:
                for param_name, param_value in dataLoad.get("coal_data").items():
                    # Check if the parameter exists already
                    if not any(param['parameter_Name'] == param_name.rsplit('_', 1)[0] for param in fetchCoaltesting.parameters):
                        # if param_name != "Third_Party_Gcv":
                        single_data = {
                            "parameter_Name": param_name.rsplit('_', 1)[0],
                            "unit_Val": param_name.rsplit('_', 1)[1],  # Add the unit value if available
                            "test_Method": "",  # Add the test method if available
                            "val1": param_value
                        }
                        fetchCoaltesting.parameters.append(single_data)

                for single_data in fetchCoaltesting.parameters:
                    param_name = f"{single_data.get('parameter_Name')}_{single_data.get('unit_Val').replace(' ', '')}"
                    if dataLoad.get("coal_data").get(param_name) is not None:
                        single_data["val1"] = dataLoad.get("coal_data").get(param_name)
                    
                    if single_data["parameter_Name"] == "Gross_Calorific_Value_(Adb)":
                        single_data["Third_Party_Gross_Calorific_Value_(Adb)"] = dataLoad.get("coal_data").get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg")
                        if dataLoad.get("coal_data").get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                            single_data["Gcv_Difference"] = str(abs(float(single_data["val1"]) - float(dataLoad.get("coal_data").get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"))))
        
                            fetchCoalGrades = CoalGrades.objects()
                            for single_coal_grades in fetchCoalGrades:
                                if (
                                    single_coal_grades["start_value"]
                                    <= dataLoad.get("coal_data").get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg")
                                    <= single_coal_grades["end_value"]
                                    and single_coal_grades["start_value"] != ""
                                    and single_coal_grades["end_value"] != ""
                                ):
                                    single_data["Third_Party_Grade"] = single_coal_grades["grade"]
                                elif dataLoad.get("coal_data").get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg") > "7001":
                                    single_data["Third_Party_Grade"] = single_coal_grades["grade"]
                                    break
                            grade_diff = str(abs(int(single_coal_grades["grade"].replace('G-', '')) - int(single_data["grade"].replace('G-', ''))))
                            single_data["Grade_Diff"] = grade_diff

                    else:
                        single_data["thrdgcv"] = None
                        single_data["gcv_difference"] = None
                        single_data["thrd_grade"] = None
                        single_data["grade_diff"] = None

            fetchCoaltesting.save()
        return {"detail": "success"}
    
    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.post("/coal_test_wcl_train_addon", tags=["Coal Testing"])
# def wcl_addon_data(response: Response, data: WCLtest):
def wcl_addon_data(response: Response, paydata: WCLtestMain):
    try:
        # dataLoad = data.dict()
        # console_logger.debug(paydata.dict())
        multyData = paydata.dict()
        for dataLoad in multyData["data"]:
            # console_logger.debug(dataLoad)
            fetchCoaltesting = CoalTestingTrain.objects.get(id=dataLoad.get("id"))
            fetchCoaltesting.third_party_report_no = dataLoad.get("coal_data").get("Third_Party_Report_No")
            if fetchCoaltesting:
                for param_name, param_value in dataLoad.get("coal_data").items():
                    # Check if the parameter exists already
                    if not any(param['parameter_Name'] == param_name.rsplit('_', 1)[0] for param in fetchCoaltesting.parameters):
                        # if param_name != "Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg":
                        single_data = {
                            "parameter_Name": param_name.rsplit('_', 1)[0],
                            "unit_Val": param_name.rsplit('_', 1)[1],  # Add the unit value if available
                            "test_Method": "",  # Add the test method if available
                            "val1": param_value
                        }
                        fetchCoaltesting.parameters.append(single_data)

                for single_data in fetchCoaltesting.parameters:
                    param_name = f"{single_data.get('parameter_Name')}_{single_data.get('unit_Val').replace(' ', '')}"
                    if dataLoad.get("coal_data").get(param_name) is not None:
                        single_data["val1"] = dataLoad.get("coal_data").get(param_name)
                    
                    if single_data["parameter_Name"] == "Gross_Calorific_Value_(Adb)":
                        single_data["Third_Party_Gross_Calorific_Value_(Adb)"] = dataLoad.get("coal_data").get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg")
                        if dataLoad.get("coal_data").get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                            single_data["Gcv_Difference"] = str(abs(float(single_data["val1"]) - float(dataLoad.get("coal_data").get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"))))


                            fetchCoalGrades = CoalGrades.objects()
                            for single_coal_grades in fetchCoalGrades:
                                if (
                                    single_coal_grades["start_value"]
                                    <= dataLoad.get("coal_data").get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg")
                                    <= single_coal_grades["end_value"]
                                    and single_coal_grades["start_value"] != ""
                                    and single_coal_grades["end_value"] != ""
                                ):
                                    single_data["Third_Party_Grade"] = single_coal_grades["grade"]
                                elif dataLoad.get("coal_data").get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg") > "7001":
                                    single_data["Third_Party_Grade"] = single_coal_grades["grade"]
                                    break
                            if single_data.get("grade"):
                                grade_diff = str(abs(int(single_coal_grades["grade"].replace('G-', '')) - int(single_data["grade"].replace('G-', ''))))
                                single_data["Grade_Diff"] = grade_diff

                    else:
                        single_data["thrdgcv"] = None
                        single_data["gcv_difference"] = None
                        single_data["thrd_grade"] = None
                        single_data["grade_diff"] = None

            fetchCoaltesting.save()
        return {"detail": "success"}
    
    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/coal_test_table", tags=["Coal Testing"])
def coal_wcl_test_table(response:Response,currentPage: Optional[int] = None, perPage: Optional[int] = None,
                    search_text: Optional[str] = None,
                    start_timestamp: Optional[str] = None,
                    end_timestamp: Optional[str] = None,
                    month_date: Optional[str] = None,
                    filter_type: Optional[str] = None, 
                    type: Optional[str] = "display"):
    try:
        result = {        
                "labels": [],
                "datasets": [],
                "total" : 0,
                "page_size": 15
        }
        
        if type and type == "display":

            data = Q()
            page_no = 1
            page_len = result["page_size"]

            if currentPage:
                page_no = currentPage

            if perPage:
                page_len = perPage
                result["page_size"] = perPage

            offset = (page_no - 1) * page_len

            
            if month_date:
                start_date = f'{month_date}-01'
                startd_date=datetime.datetime.strptime(start_date,"%Y-%m-%d")
                end_date = startd_date + relativedelta(day=31)
                data &= Q(receive_date__gte = startd_date)
                data &= Q(receive_date__lte = end_date)

            if search_text:
                if search_text.isdigit():
                    data &= (Q(rrNo__icontains=search_text))
                else:
                    data &= Q(location__icontains=search_text) | Q(rake_no__icontains=search_text)

            if start_timestamp:
                start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__gte = start_date)
            # else:
            #     start_timestamp = (datetime.datetime.now() - datetime.timedelta(days=31)).strftime("%Y-%m-%d")
            #     data &= Q(receive_date__gte = convert_to_utc_format(start_timestamp,"%Y-%m-%d").strftime("%Y-%m-%d"))

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__lte = end_date)
            # else:
            #     end_timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
            #     data &= Q(receive_date__lte = convert_to_utc_format(end_timestamp,"%Y-%m-%d").strftime("%Y-%m-%d"))


            logs = (
                CoalTesting.objects(data)
                .order_by("-ID")
                .skip(offset)
                .limit(page_len)                  
            )        

            if any(logs):
                for log in logs:
                    # result["labels"] = list(log.payload().keys())
                    result["labels"] = ["Sr.No","Mine","Lot_No","DO_No","DO_Qty", "Supplier", "Date", "Time","Id", "GWEL_Total_Moisture_%", 
                                        "GWEL_Inherent_Moisture_(Adb)_%", "GWEL_Ash_(Adb)_%", "GWEL_Volatile_Matter_(Adb)_%", "GWEL_Gross_Calorific_Value_(Adb)_Kcal/Kg", 
                                        "Ash_(Arb)_%", "GWEL_Volatile_Matter_(Arb)_%", "GWEL_Fixed_Carbon_(Arb)_%", "GWEL_Gross_Calorific_Value_(Arb)_Kcal/Kg",
                                        "Third_Party_Total_Moisture_%", "Third_Party_Inherent_Moisture_(Adb)_%", "Third_Party_Ash_(Adb)_%",
                                        "Third_Party_Volatile_Matter_(Adb)_%", "Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg",
                                        "Third_Party_Ash_(Arb)_%", "Third_Party_Volatile_Matter_(Arb)_%",
                                        "Third_Party_Fixed_Carbon_(Arb)_%", "Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg", "Third_Party_Report_No"]
                    result["datasets"].append(log.payload())
            result["total"] = (len(CoalTesting.objects(data)))
            # console_logger.debug(f"-------- Road Coal Testing Response -------- {result}")
            return result

        elif type and type == "download":
            del type

            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)
            
            data = Q()

            if start_timestamp:
                start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__gte = start_date)
           
            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__lte = end_date)
            
            if search_text:
                if search_text.isdigit():
                    data &= (Q(rrNo__icontains=search_text))
                else:
                    data &= Q(location__icontains=search_text) | Q(rake_no__icontains=search_text)

            usecase_data = CoalTesting.objects(data).order_by("-receive_date")
            count = len(usecase_data)
            path = None
            if usecase_data:
                try:
                    path = os.path.join(
                        "static_server",
                        "gmr_ai",
                        file,

                        "Roadwise_Coal_Lab_Test_Report_{}.xlsx".format(
                            datetime.datetime.now().strftime("%Y-%m-%d:%H:%M:%S"),
                        ),
                    )
                    filename = os.path.join(os.getcwd(), path)
                    workbook = xlsxwriter.Workbook(filename)
                    workbook.use_zip64()
                    cell_format2 = workbook.add_format()
                    cell_format2.set_bold()
                    cell_format2.set_font_size(10)
                    cell_format2.set_align("center")
                    cell_format2.set_align("vjustify")

                    worksheet = workbook.add_worksheet()
                    worksheet.set_column("A:AZ", 20)
                    worksheet.set_default_row(50)
                    cell_format = workbook.add_format()
                    cell_format.set_font_size(10)
                    cell_format.set_align("center")
                    cell_format.set_align("vcenter")

                    if filter_type == "gwel":
                        headers = ["Sr.No",
                                   "Mine",
                                   "Lot No",
                                   "DO No",
                                   "DO Qty", 
                                   "Supplier",
                                   "Date", 
                                   "Time",
                                   "GWEL Total Moisture%", 
                                   "GWEL Inherent Moisture (Adb)%", 
                                   "GWEL Ash (Adb)%", 
                                   "GWEL Volatile Matter (Adb)%", 
                                   "GWEL Gross Calorific Value (Adb) Kcal/Kg", 
                                   "GWEL Ash (Arb)%", 
                                   "GWEL Volatile Matter (Arb)%", 
                                   "GWEL Fixed Carbon (Arb)%", 
                                   "GWEL Gross Calorific Value (Arb) Kcal/Kg",
                                   "GWEL Grade (Adb)",
                                   ]
                    elif filter_type == "third_party":
                        headers = ["Sr.No",
                                   "Mine",
                                   "Lot No",
                                   "DO No",
                                   "DO Qty", 
                                   "Supplier", 
                                   "Date", 
                                   "Time",
                                   "Third Party Report No", 
                                   "Third Party Total Moisture%", 
                                   "Third Party Inherent Moisture (Adb)%", 
                                   "Third Party Ash (Adb)%", 
                                   "Third Party Volatile Matter (Adb)%", 
                                   "Third Party Gross Calorific Value (Adb) Kcal/Kg",  
                                   "Third Party Ash (Arb)%", 
                                   "Third Party Volatile Matter (Arb)%", 
                                   "Third Party Fixed Carbon (Arb)%", 
                                   "Third Party Gross Calorific Value (Arb) Kcal/Kg", 
                                   "Third Party Grade (Adb)",
                                    ]
                    elif filter_type == "all":
                        headers = ["Sr.No",
                                    "Mine",
                                    "Lot No",
                                    "DO No",
                                    "DO Qty", 
                                    "Supplier",
                                    "Date", 
                                    "Time",
                                    "GWEL Total Moisture%", 
                                    "GWEL Inherent Moisture (Adb)%", 
                                    "GWEL Ash (Adb)%", 
                                    "GWEL Volatile Matter (Adb)%", 
                                    "GWEL Gross Calorific Value (Adb) Kcal/Kg", 
                                    "GWEL Ash (Arb)%", 
                                    "GWEL Volatile Matter (Arb)%", 
                                    "GWEL Fixed Carbon (Arb)%", 
                                    "GWEL Gross Calorific Value (Arb) Kcal/Kg",
                                    "GWEL Grade (Adb)",
                                    "Third Party Report No", 
                                    "Third Party Total Moisture%", 
                                    "Third Party Inherent Moisture (Adb)%", 
                                    "Third Party Ash (Adb)%", 
                                    "Third Party Volatile Matter (Adb)%", 
                                    "Third Party Gross Calorific Value (Adb) Kcal/Kg",  
                                    "Third Party Ash (Arb)%", 
                                    "Third Party Volatile Matter (Arb)%", 
                                    "Third Party Fixed Carbon (Arb)%", 
                                    "Third Party Gross Calorific Value (Arb) Kcal/Kg",
                                    "Third Party Grade (Adb)",
                                    ]
                    else:
                        headers = ["Sr.No",
                                "Mine",
                                "Lot No",
                                "DO No",
                                "DO Qty", 
                                "Supplier", 
                                "Date", 
                                "Time"]

                    for index, header in enumerate(headers):
                        worksheet.write(0, index, header, cell_format2)

                    fetchCoalGrades = CoalGrades.objects()

                    for row, query in enumerate(usecase_data,start=1):
                        result = query.payload()
                        # console_logger.debug(result)
                        worksheet.write(row, 0, count, cell_format)
                        if filter_type == "gwel":
                            worksheet.write(row, 1, str(result["Mine"]), cell_format)
                            worksheet.write(row, 2, str(result["Lot_No"]), cell_format)
                            worksheet.write(row, 3, str(result["DO_No"]), cell_format)
                            worksheet.write(row, 4, str(result["DO_Qty"]), cell_format)
                            worksheet.write(row, 5, str(result["Supplier"]), cell_format)
                            worksheet.write(row, 6, str(result["Date"]), cell_format)
                            worksheet.write(row, 7, str(result["Time"]), cell_format)
                            worksheet.write(row, 8, str(result["Total_Moisture_%"]), cell_format)
                            worksheet.write(row, 9, str(result["Inherent_Moisture_(Adb)_%"]), cell_format)
                            worksheet.write(row, 10, str(result["Ash_(Adb)_%"]), cell_format)
                            worksheet.write(row, 11, str(result["Volatile_Matter_(Adb)_%"]), cell_format)
                            worksheet.write(row, 12, str(result["Gross_Calorific_Value_(Adb)_Kcal/Kg"]), cell_format)
                            worksheet.write(row, 13, str(result["Ash_(Arb)_%"]), cell_format)
                            worksheet.write(row, 14, str(result["Volatile_Matter_(Arb)_%"]), cell_format)
                            worksheet.write(row, 15, str(result["Fixed_Carbon_(Arb)_%"]), cell_format)
                            worksheet.write(row, 16, str(result["Gross_Calorific_Value_(Arb)_Kcal/Kg"]), cell_format)
                            if result.get("Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= int(result.get("Gross_Calorific_Value_(Adb)_Kcal/Kg"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 17, str(single_coal_grades["grade"]), cell_format)
                                    elif int(result.get("Gross_Calorific_Value_(Adb)_Kcal/Kg")) > 7001:
                                        worksheet.write(row, 17, "G-1", cell_format)

                        elif filter_type == "third_party":
                            worksheet.write(row, 1, str(result["Mine"]), cell_format)
                            worksheet.write(row, 2, str(result["Lot_No"]), cell_format)
                            worksheet.write(row, 3, str(result["DO_No"]), cell_format)
                            worksheet.write(row, 4, str(result["DO_Qty"]), cell_format)
                            worksheet.write(row, 5, str(result["Supplier"]), cell_format)
                            worksheet.write(row, 6, str(result["Date"]), cell_format)
                            worksheet.write(row, 7, str(result["Time"]), cell_format)
                            if result.get("Third_Party_Report_No"):
                                worksheet.write(row, 8, str(result["Third_Party_Report_No"]), cell_format)
                            if result.get("Third_Party_Total_Moisture_%"):
                                worksheet.write(row, 9, str(result["Third_Party_Total_Moisture_%"]), cell_format)
                            if result.get("Third_Party_Inherent_Moisture_(Adb)_%"):
                                worksheet.write(row, 10, str(result["Third_Party_Inherent_Moisture_(Adb)_%"]), cell_format)
                            if result.get("Third_Party_Ash_(Adb)_%"):
                                worksheet.write(row, 11, str(result["Third_Party_Ash_(Adb)_%"]), cell_format)
                            if result.get("Third_Party_Volatile_Matter_(Adb)_%"):
                                worksheet.write(row, 12, str(result["Third_Party_Volatile_Matter_(Adb)_%"]), cell_format)
                            if result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                                worksheet.write(row, 13, str(result["Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"]), cell_format)
                            if result.get("Third_Party_Ash_(Arb)_%"):
                                worksheet.write(row, 14, str(result["Third_Party_Ash_(Arb)_%"]), cell_format)
                            if result.get("Third_Party_Volatile_Matter_(Arb)_%"):
                                worksheet.write(row, 15, str(result["Third_Party_Volatile_Matter_(Arb)_%"]), cell_format)
                            if result.get("Third_Party_Fixed_Carbon_(Arb)_%"):
                                worksheet.write(row, 16, str(result["Third_Party_Fixed_Carbon_(Arb)_%"]), cell_format)
                            if result.get("Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"):
                                worksheet.write(row, 17, str(result["Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"]), cell_format)
                            if result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= int(result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 18, str(single_coal_grades["grade"]), cell_format)
                                    elif int(result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg")) > 7001:
                                        worksheet.write(row, 18, "G-1", cell_format)
                        elif filter_type == "all":
                            worksheet.write(row, 1, str(result["Mine"]), cell_format)
                            worksheet.write(row, 2, str(result["Lot_No"]), cell_format)
                            worksheet.write(row, 3, str(result["DO_No"]), cell_format)
                            worksheet.write(row, 4, str(result["DO_Qty"]), cell_format)
                            worksheet.write(row, 5, str(result["Supplier"]), cell_format)
                            worksheet.write(row, 6, str(result["Date"]), cell_format)
                            worksheet.write(row, 7, str(result["Time"]), cell_format)
                            worksheet.write(row, 8, str(result["Total_Moisture_%"]), cell_format)
                            worksheet.write(row, 9, str(result["Inherent_Moisture_(Adb)_%"]), cell_format)
                            worksheet.write(row, 10, str(result["Ash_(Adb)_%"]), cell_format)
                            worksheet.write(row, 11, str(result["Volatile_Matter_(Adb)_%"]), cell_format)
                            worksheet.write(row, 12, str(result["Gross_Calorific_Value_(Adb)_Kcal/Kg"]), cell_format)
                            worksheet.write(row, 13, str(result["Ash_(Arb)_%"]), cell_format)
                            worksheet.write(row, 14, str(result["Volatile_Matter_(Arb)_%"]), cell_format)
                            worksheet.write(row, 15, str(result["Fixed_Carbon_(Arb)_%"]), cell_format)
                            worksheet.write(row, 16, str(result["Gross_Calorific_Value_(Arb)_Kcal/Kg"]), cell_format)
                            if result.get("Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= int(result.get("Gross_Calorific_Value_(Adb)_Kcal/Kg"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 17, str(single_coal_grades["grade"]), cell_format)
                                    elif int(result.get("Gross_Calorific_Value_(Adb)_Kcal/Kg")) > 7001:
                                        worksheet.write(row, 17, "G-1", cell_format)
                            if result.get("Third_Party_Report_No"):
                                worksheet.write(row, 17, str(result["Third_Party_Report_No"]), cell_format)
                            if result.get("Third_Party_Total_Moisture_%"):
                                worksheet.write(row, 18, str(result["Third_Party_Total_Moisture_%"]), cell_format)
                            if result.get("Third_Party_Inherent_Moisture_(Adb)_%"):
                                worksheet.write(row, 19, str(result["Third_Party_Inherent_Moisture_(Adb)_%"]), cell_format)
                            if result.get("Third_Party_Ash_(Adb)_%"):
                                worksheet.write(row, 20, str(result["Third_Party_Ash_(Adb)_%"]), cell_format)
                            if result.get("Third_Party_Volatile_Matter_(Adb)_%"):
                                worksheet.write(row, 21, str(result["Third_Party_Volatile_Matter_(Adb)_%"]), cell_format)
                            if result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                                worksheet.write(row, 22, str(result["Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"]), cell_format)
                            if result.get("Third_Party_Ash_(Arb)_%"):
                                worksheet.write(row, 23, str(result["Third_Party_Ash_(Arb)_%"]), cell_format)
                            if result.get("Third_Party_Volatile_Matter_(Arb)_%"):
                                worksheet.write(row, 24, str(result["Third_Party_Volatile_Matter_(Arb)_%"]), cell_format)
                            if result.get("Third_Party_Fixed_Carbon_(Arb)_%"):
                                worksheet.write(row, 25, str(result["Third_Party_Fixed_Carbon_(Arb)_%"]), cell_format)
                            if result.get("Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"):
                                worksheet.write(row, 26, str(result["Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"]), cell_format)
                            if result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= int(result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 27, str(single_coal_grades["grade"]), cell_format)
                                    elif int(result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg")) > 7001:
                                        worksheet.write(row, 27, "G-1", cell_format)
                        else:
                            worksheet.write(row, 1, str(result["Mine"]), cell_format)
                            worksheet.write(row, 2, str(result["Lot_No"]), cell_format)
                            worksheet.write(row, 3, str(result["DO_No"]), cell_format)
                            worksheet.write(row, 4, str(result["DO_Qty"]), cell_format)
                            worksheet.write(row, 5, str(result["Supplier"]), cell_format)
                            worksheet.write(row, 6, str(result["Date"]), cell_format)
                            worksheet.write(row, 7, str(result["Time"]), cell_format)
                        count -= 1
                    workbook.close()

                    console_logger.debug("Successfully {} report generated".format(service_id))
                    console_logger.debug("sent data {}".format(path))
                    return {
                            "Type": "coal_test_download_event",
                            "Datatype": "Report",
                            "File_Path": path,
                        }
                except Exception as e:
                    console_logger.debug(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
                    console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
            else:
                return {
                            "Type": "coal_test_download_event",
                            "Datatype": "Report",
                            "File_Path": path,
                        }

    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e



@router.get("/coal_train_test_table", tags=["Coal Testing"])
def coal_secl_test_table(response:Response,currentPage: Optional[int] = None, perPage: Optional[int] = None,
                    search_text: Optional[str] = None,
                    start_timestamp: Optional[str] = None,
                    end_timestamp: Optional[str] = None,
                    month_date: Optional[str] = None,
                    filter_type: Optional[str] = None,
                    type: Optional[str] = "display"):
    try:
        result = {        
                "labels": [],
                "datasets": [],
                "total" : 0,
                "page_size": 15
        }
        
        if type and type == "display":

            data = Q()
            page_no = 1
            page_len = result["page_size"]

            if currentPage:
                page_no = currentPage

            if perPage:
                page_len = perPage
                result["page_size"] = perPage
            
            offset = (page_no - 1) * page_len

            if month_date:
                start_date = f'{month_date}-01'
                startd_date=datetime.datetime.strptime(start_date,"%Y-%m-%d")
                end_date = startd_date + relativedelta( day=31)
                data &= Q(receive_date__gte = startd_date)
                data &= Q(receive_date__lte = end_date)

            if search_text:
                if search_text.isdigit():
                    data &= (Q(rrNo__icontains=search_text))
                else:
                    data &= Q(location__icontains=search_text) | Q(rake_no__icontains=search_text)

            if start_timestamp:
                start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__gte = start_date)
            # else:
            #     start_timestamp = (datetime.datetime.now() - datetime.timedelta(days=31)).strftime("%Y-%m-%d")
            #     data &= Q(receive_date__gte = convert_to_utc_format(start_timestamp,"%Y-%m-%d").strftime("%Y-%m-%d"))

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__lte = end_date)
            # else:
            #     end_timestamp = datetime.datetime.now().strftime("%Y-%m-%d")
            #     data &= Q(receive_date__lte = convert_to_utc_format(end_timestamp,"%Y-%m-%d").strftime("%Y-%m-%d"))

            
            logs = (
                CoalTestingTrain.objects(data)
                .order_by("-ID")
                .skip(offset)
                .limit(page_len)                  
            )        

            if any(logs):
                for log in logs:
                    # result["labels"] = list(log.payload().keys())
                    result["labels"] = ["Sr.No", 
                    "Mine", 
                    "Lot_No", 
                    "RR_No", 
                    "RR_Qty", 
                    "Supplier", 
                    "Date", 
                    "Time", 
                    "Id", 
                    "GWEL_Total_Moisture_%", 
                    "GWEL_Inherent_Moisture_(Adb)_%", 
                    "GWEL_Ash_(Adb)_%", 
                    "GWEL_Volatile_Matter_(Adb)_%", 
                    "GWEL_Gross_Calorific_Value_(Adb)_Kcal/Kg", 
                    "GWEL_Ash_(Arb)_%", 
                    "GWEL_Volatile_Matter_(Arb)_%", 
                    "GWEL_Fixed_Carbon_(Arb)_%", 
                    "GWEL_Gross_Calorific_Value_(Arb)_Kcal/Kg", 
                    "Third_Party_Report_No", 
                    "Third_Party_Total_Moisture_%", 
                    "Third_Party_Inherent_Moisture_(Adb)_%", 
                    "Third_Party_Ash_(Adb)_%", 
                    "Third_Party_Volatile_Matter_(Adb)_%", 
                    "Third_Party_Ash_(Arb)_%", 
                    "Third_Party_Volatile_Matter_(Arb)_%",
                    "Third_Party_Fixed_Carbon_(Arb)_%",
                    "Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"]
                    result["datasets"].append(log.payload())

            result["total"] = (len(CoalTestingTrain.objects(data)))
            # console_logger.debug(f"-------- Rail Coal Testing Response -------- {result}")
            return result

        elif type and type == "download":
            del type

            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)

            data = Q()

            if start_timestamp:
                start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__gte = start_date)

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__lte = end_date)

            if search_text:
                if search_text.isdigit():
                    data &= (Q(rrNo__icontains=search_text))
                else:
                    data &= Q(location__icontains=search_text) | Q(rake_no__icontains=search_text)

            usecase_data = CoalTestingTrain.objects(data).order_by("-receive_date")
            count = len(usecase_data)
            path = None
            if usecase_data:
                try:
                    path = os.path.join(
                        "static_server",
                        "gmr_ai",
                        file,
                        "Railwise_Coal_Lab_Test_Report_{}.xlsx".format(
                            datetime.datetime.now().strftime("%Y-%m-%d:%H:%M:%S"),
                        ),
                    )
                    filename = os.path.join(os.getcwd(), path)
                    workbook = xlsxwriter.Workbook(filename)
                    workbook.use_zip64()
                    cell_format2 = workbook.add_format()
                    cell_format2.set_bold()
                    cell_format2.set_font_size(10)
                    cell_format2.set_align("center")
                    cell_format2.set_align("vjustify")

                    worksheet = workbook.add_worksheet()
                    worksheet.set_column("A:AZ", 20)
                    worksheet.set_default_row(50)
                    cell_format = workbook.add_format()
                    cell_format.set_font_size(10)
                    cell_format.set_align("center")
                    cell_format.set_align("vcenter")

                    if filter_type == "gwel":
                        headers = ["Sr.No",
                                "Mine",
                                "Lot No", 
                                "RR No", 
                                "RR Qty", 
                                "Supplier", 
                                "Date", 
                                "Time",
                                "GWEL Total Moisture%", 
                                "GWEL Inherent Moisture (Adb)%", 
                                "GWEL Ash (Adb)%", 
                                "GWEL Volatile Matter (Adb)%", 
                                "GWEL Gross Calorific Value (Adb) Kcal/Kg", 
                                "GWEL Ash (Arb)%", 
                                "GWEL Volatile Matter (Arb)%", 
                                "GWEL Fixed Carbon (Arb)%", 
                                "GWEL Gross Calorific Value (Arb) Kcal/Kg",
                                "GWEL Grade (Adb)",
                                ]
                    elif filter_type == "third_party":
                        headers = [
                               "Sr.No",
                               "Mine",
                               "Lot No", 
                               "RR No", 
                               "RR Qty", 
                               "Supplier",
                               "Date", 
                               "Time",
                               "Third Party Report No", 
                               "Third Party Total Moisture%", 
                               "Third Party Inherent Moisture (Adb)%", 
                               "Third Party Ash (Adb)%", 
                               "Third Party Volatile Matter (Adb)%", 
                               "Third Party Gross Calorific Value (Adb) Kcal/Kg",
                               "Third Party Ash (Arb)%", 
                               "Third Party Volatile Matter (Arb)%", 
                               "Third Party Fixed Carbon (Arb)%", 
                               "Third Party Gross Calorific Value (Arb) Kcal/Kg",
                               "Third Party Grade (Adb)",
                               ]
                    elif filter_type == "all":
                        headers = ["Sr.No",
                                "Mine",
                                "Lot No", 
                                "RR No", 
                                "RR Qty", 
                                "Supplier", 
                                "Date", 
                                "Time",
                                "GWEL Total Moisture%", 
                                "GWEL Inherent Moisture (Adb)%", 
                                "GWEL Ash (Adb)%", 
                                "GWEL Volatile Matter (Adb)%", 
                                "GWEL Gross Calorific Value (Adb) Kcal/Kg", 
                                "GWEL Ash (Arb)%", 
                                "GWEL Volatile Matter (Arb)%", 
                                "GWEL Fixed Carbon (Arb)%", 
                                "GWEL Gross Calorific Value (Arb) Kcal/Kg",
                                "GWEL Grade (Adb)",
                                "Third Party Report No", 
                                "Third Party Total Moisture%", 
                                "Third Party Inherent Moisture (Adb)%", 
                                "Third Party Ash (Adb)%", 
                                "Third Party Volatile Matter (Adb)%", 
                                "Third Party Gross Calorific Value (Adb) Kcal/Kg",
                                "Third Party Ash (Arb)%", 
                                "Third Party Volatile Matter (Arb)%", 
                                "Third Party Fixed Carbon (Arb)%", 
                                "Third Party Gross Calorific Value (Arb) Kcal/Kg",
                                "Third Party Grade (Adb)",
                                ]
                    else:
                        headers = [
                                "Sr.No",
                                "Mine",
                                "Lot No", 
                                "RR No", 
                                "RR Qty", 
                                "Supplier",
                                "Date", 
                                "Time"]

                    for index, header in enumerate(headers):
                        worksheet.write(0, index, header, cell_format2)
                    fetchCoalGrades = CoalGrades.objects()
                    for row, query in enumerate(usecase_data,start=1):
                        result = query.payload()
                        worksheet.write(row, 0, count, cell_format)
                        if filter_type == "gwel":
                            worksheet.write(row, 1, str(result["Mine"]), cell_format)
                            worksheet.write(row, 2, str(result["Lot_No"]), cell_format)
                            worksheet.write(row, 3, str(result["RR_No"]), cell_format)
                            worksheet.write(row, 4, str(result["RR_Qty"]), cell_format)
                            worksheet.write(row, 5, str(result["Supplier"]), cell_format)
                            worksheet.write(row, 6, str(result["Date"]), cell_format)
                            worksheet.write(row, 7, str(result["Time"]), cell_format)
                            worksheet.write(row, 8, str(result["Total_Moisture_%"]), cell_format)
                            worksheet.write(row, 9, str(result["Inherent_Moisture_(Adb)_%"]), cell_format)
                            worksheet.write(row, 10, str(result["Ash_(Adb)_%"]), cell_format)
                            worksheet.write(row, 11, str(result["Volatile_Matter_(Adb)_%"]), cell_format)
                            worksheet.write(row, 12, str(result["Gross_Calorific_Value_(Adb)_Kcal/Kg"]), cell_format)
                            worksheet.write(row, 13, str(result["Ash_(Arb)_%"]), cell_format)
                            worksheet.write(row, 14, str(result["Volatile_Matter_(Arb)_%"]), cell_format)
                            worksheet.write(row, 15, str(result["Fixed_Carbon_(Arb)_%"]), cell_format)
                            worksheet.write(row, 16, str(result["Gross_Calorific_Value_(Arb)_Kcal/Kg"]), cell_format)
                            if result.get("Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= int(result.get("Gross_Calorific_Value_(Adb)_Kcal/Kg"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 17, str(single_coal_grades["grade"]), cell_format)
                                    elif int(result.get("Gross_Calorific_Value_(Adb)_Kcal/Kg")) > 7001:
                                        worksheet.write(row, 17, "G-1", cell_format)
                            
                        elif filter_type == "third_party":
                            worksheet.write(row, 1, str(result["Mine"]), cell_format)
                            worksheet.write(row, 2, str(result["Lot_No"]), cell_format)
                            worksheet.write(row, 3, str(result["RR_No"]), cell_format)
                            worksheet.write(row, 4, str(result["RR_Qty"]), cell_format)
                            worksheet.write(row, 5, str(result["Supplier"]), cell_format)
                            worksheet.write(row, 6, str(result["Date"]), cell_format)
                            worksheet.write(row, 7, str(result["Time"]), cell_format)
                            if result.get("Third_Party_Report_No"):
                                worksheet.write(row, 8, str(result["Third_Party_Report_No"]), cell_format)
                            if result.get("Third_Party_Total_Moisture_%"):
                                worksheet.write(row, 9, str(result["Third_Party_Total_Moisture_%"]), cell_format)
                            if result.get("Third_Party_Inherent_Moisture_(Adb)_%"):
                                worksheet.write(row, 10, str(result["Third_Party_Inherent_Moisture_(Adb)_%"]), cell_format)
                            if result.get("Third_Party_Ash_(Adb)_%"):
                                worksheet.write(row, 11, str(result["Third_Party_Ash_(Adb)_%"]), cell_format)
                            if result.get("Third_Party_Volatile_Matter_(Adb)_%"):
                                worksheet.write(row, 12, str(result["Third_Party_Volatile_Matter_(Adb)_%"]), cell_format)
                            if result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                                worksheet.write(row, 13, str(result["Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"]), cell_format)
                            if result.get("Third_Party_Ash_(Arb)_%"):
                                worksheet.write(row, 14, str(result["Third_Party_Ash_(Arb)_%"]), cell_format)
                            if result.get("Third_Party_Volatile_Matter_(Arb)_%"):
                                worksheet.write(row, 15, str(result["Third_Party_Volatile_Matter_(Arb)_%"]), cell_format)
                            if result.get("Third_Party_Fixed_Carbon_(Arb)_%"):
                                worksheet.write(row, 16, str(result["Third_Party_Fixed_Carbon_(Arb)_%"]), cell_format)
                            if result.get("Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"):
                                worksheet.write(row, 17, str(result["Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"]), cell_format)
                            if result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= int(result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 18, str(single_coal_grades["grade"]), cell_format)
                                    elif int(result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg")) > 7001:
                                        worksheet.write(row, 18, "G-1", cell_format)
                        elif filter_type == "all":
                            worksheet.write(row, 1, str(result["Mine"]), cell_format)
                            worksheet.write(row, 2, str(result["Lot_No"]), cell_format)
                            worksheet.write(row, 3, str(result["RR_No"]), cell_format)
                            worksheet.write(row, 4, str(result["RR_Qty"]), cell_format)
                            worksheet.write(row, 5, str(result["Supplier"]), cell_format)
                            worksheet.write(row, 6, str(result["Date"]), cell_format)
                            worksheet.write(row, 7, str(result["Time"]), cell_format)
                            worksheet.write(row, 8, str(result["Total_Moisture_%"]), cell_format)
                            worksheet.write(row, 9, str(result["Inherent_Moisture_(Adb)_%"]), cell_format)
                            worksheet.write(row, 10, str(result["Ash_(Adb)_%"]), cell_format)
                            worksheet.write(row, 11, str(result["Volatile_Matter_(Adb)_%"]), cell_format)
                            worksheet.write(row, 12, str(result["Gross_Calorific_Value_(Adb)_Kcal/Kg"]), cell_format)
                            worksheet.write(row, 13, str(result["Ash_(Arb)_%"]), cell_format)
                            worksheet.write(row, 14, str(result["Volatile_Matter_(Arb)_%"]), cell_format)
                            worksheet.write(row, 15, str(result["Fixed_Carbon_(Arb)_%"]), cell_format)
                            worksheet.write(row, 16, str(result["Gross_Calorific_Value_(Arb)_Kcal/Kg"]), cell_format)
                            if result.get("Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= int(result.get("Gross_Calorific_Value_(Adb)_Kcal/Kg"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 17, str(single_coal_grades["grade"]), cell_format)
                                    elif int(result.get("Gross_Calorific_Value_(Adb)_Kcal/Kg")) > 7001:
                                        worksheet.write(row, 17, "G-1", cell_format)
                            if result.get("Third_Party_Report_No"):
                                worksheet.write(row, 17, str(result["Third_Party_Report_No"]), cell_format)
                            if result.get("Third_Party_Total_Moisture_%"):
                                worksheet.write(row, 18, str(result["Third_Party_Total_Moisture_%"]), cell_format)
                            if result.get("Third_Party_Inherent_Moisture_(Adb)_%"):
                                worksheet.write(row, 19, str(result["Third_Party_Inherent_Moisture_(Adb)_%"]), cell_format)
                            if result.get("Third_Party_Ash_(Adb)_%"):
                                worksheet.write(row, 20, str(result["Third_Party_Ash_(Adb)_%"]), cell_format)
                            if result.get("Third_Party_Volatile_Matter_(Adb)_%"):
                                worksheet.write(row, 21, str(result["Third_Party_Volatile_Matter_(Adb)_%"]), cell_format)
                            if result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                                worksheet.write(row, 22, str(result["Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"]), cell_format)
                            if result.get("Third_Party_Ash_(Arb)_%"):
                                worksheet.write(row, 23, str(result["Third_Party_Ash_(Arb)_%"]), cell_format)
                            if result.get("Third_Party_Volatile_Matter_(Arb)_%"):
                                worksheet.write(row, 24, str(result["Third_Party_Volatile_Matter_(Arb)_%"]), cell_format)
                            if result.get("Third_Party_Fixed_Carbon_(Arb)_%"):
                                worksheet.write(row, 25, str(result["Third_Party_Fixed_Carbon_(Arb)_%"]), cell_format)
                            if result.get("Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"):
                                worksheet.write(row, 26, str(result["Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"]), cell_format)
                            if result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= int(result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 27, str(single_coal_grades["grade"]), cell_format)
                                    elif int(result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg")) > 7001:
                                        worksheet.write(row, 27, "G-1", cell_format)
                        else:
                            worksheet.write(row, 1, str(result["Mine"]), cell_format)
                            worksheet.write(row, 2, str(result["Lot_No"]), cell_format)
                            worksheet.write(row, 3, str(result["RR_No"]), cell_format)
                            worksheet.write(row, 4, str(result["RR_Qty"]), cell_format)
                            worksheet.write(row, 5, str(result["Supplier"]), cell_format)
                            worksheet.write(row, 6, str(result["Date"]), cell_format)
                            worksheet.write(row, 7, str(result["Time"]), cell_format)
                        count -= 1
                    workbook.close()


                    console_logger.debug("Successfully {} report generated".format(service_id))
                    console_logger.debug("sent data {}".format(path))
                    return {
                            "Type": "coal_test_download_event",
                            "Datatype": "Report",
                            "File_Path": path,
                        }
                except Exception as e:
                    console_logger.debug(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
                    console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
            else:
                console_logger.error("No data found")
                return {
                            "Type": "coal_test_download_event",
                            "Datatype": "Report",
                            "File_Path": path,
                        }

    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


#  x------------------------------   Road Coal Journey API    ------------------------------------x


@router.get("/road_journey_table", tags=["Road Map"])
def gmr_table(response:Response, filter_data: Optional[List[str]] = Query([]), currentPage: Optional[int] = None, perPage: Optional[int] = None, search_text: Optional[str] = None, start_timestamp: Optional[str] = None, end_timestamp: Optional[str] = None, type: Optional[str] = "display", consumer_type: Optional[str] = "All"):
    try:
        result = {        
                "labels": [],
                "datasets": [],
                "total" : 0,
                "page_size": 15
        }
        
        if type and type == "display":

            page_no = 1
            page_len = result["page_size"]

            if currentPage:
                page_no = currentPage

            if perPage:
                page_len = perPage
                result["page_size"] = perPage

            # if start_timestamp:
            #     data["created_at__gte"] = datetime.datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M")

            # if end_timestamp:
            #     data["created_at__lte"] = datetime.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M")

            # if search_text:
            #     if search_text.isdigit():
            #         data["arv_cum_do_number__icontains"] = search_text
            #     else:
            #         data["vehicle_number__icontains"] = search_text

            # Constructing the base for query
            data = Q()

            # based on condition for timestamp playing with & and | 
            if start_timestamp:
                start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(created_at__gte = start_date)

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M","Asia/Kolkata",False)
                data &= Q(created_at__lte = end_date)

            if search_text:
                if search_text.isdigit():
                    data &= Q(arv_cum_do_number__icontains=search_text) | Q(delivery_challan_number__icontains=search_text)
                else:
                    data &= (Q(vehicle_number__icontains=search_text))
            
            if consumer_type and consumer_type != "All":
                data &= Q(type_consumer__icontains=consumer_type)

            offset = (page_no - 1) * page_len
            
            logs = (
                # Gmrdata.objects(**data)
                Gmrdata.objects(data)
                .order_by("-created_at")
                .skip(offset)
                .limit(page_len)
            )        
            if any(logs):
                for log in logs:
                    result["labels"] = list(log.payload().keys())
                    result["datasets"].append(log.payload())

            # result["total"]= len(Gmrdata.objects(**data))
            result["total"]= len(Gmrdata.objects(data))
            # console_logger.debug(f"-------- Road Journey Table Response -------- {result}")
            return result

        elif type and type == "download":
            del type

            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)

            # Constructing the base for query
            data = Q()

            # if start_timestamp:
            #     data &= Q(created_at__gte = datetime.datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M"))
            # if end_timestamp:
            #     data &= Q(created_at__lte = datetime.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M"))

            if start_timestamp:
                start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(created_at__gte = start_date)

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M","Asia/Kolkata",False)
                data &= Q(created_at__lte = end_date)
            
            if search_text:
                if search_text.isdigit():
                    data &= Q(arv_cum_do_number__icontains = search_text) | Q(delivery_challan_number__icontains = search_text)
                else:
                    data &= Q(vehicle_number__icontains = search_text)

            # usecase_data = Gmrdata.objects(**data).order_by("-created_at")
            usecase_data = Gmrdata.objects(data).order_by("-created_at")
            count = len(usecase_data)
            path = None
            if usecase_data:
                try:
                    path = os.path.join(
                        "static_server",
                        "gmr_ai",
                        file,
                        "Road_Report_{}.xlsx".format(
                            datetime.datetime.now().strftime("%Y-%m-%d:%H:%M:%S"),
                        ),
                    )
                    filename = os.path.join(os.getcwd(), path)
                    workbook = xlsxwriter.Workbook(filename)
                    workbook.use_zip64()
                    cell_format2 = workbook.add_format()
                    cell_format2.set_bold()
                    cell_format2.set_font_size(10)
                    cell_format2.set_align("center")
                    cell_format2.set_align("vjustify")

                    worksheet = workbook.add_worksheet()
                    worksheet.set_column("A:AZ", 20)
                    worksheet.set_default_row(50)
                    cell_format = workbook.add_format()
                    cell_format.set_font_size(10)
                    cell_format.set_align("center")
                    cell_format.set_align("vcenter")
                    headers = filter_data
                    header_indexes = {header: index for index, header in enumerate(headers)}
                    headers = [header.capitalize().replace("_", " ") for header in headers]
                    for index, header in enumerate(headers):
                        worksheet.write(0, index, header, cell_format2)
                    for row, query in enumerate(usecase_data,start=1):
                        result = query.payload()
                        worksheet.write(row, 0, count, cell_format)     
                        for header in filter_data:
                            if header in result:
                                worksheet.write(row, header_indexes[header], str(result[header]), cell_format)                        
                        
                        count-=1
                        
                    workbook.close()
                    console_logger.debug("Successfully {} report generated".format(service_id))
                    console_logger.debug("sent data {}".format(path))

                    return {
                            "Type": "gmr_road_journey_download_event",
                            "Datatype": "Report",
                            "File_Path": path,
                            }
                
                except Exception as e:
                    console_logger.debug(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
                    console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
            else:
                console_logger.error("No data found")
                return {
                        "Type": "gmr_road_journey_download_event",
                        "Datatype": "Report",
                        "File_Path": path,
                        }

    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.post("/save_dc_request_data", tags=["Road Map Request"])
async def store_dc_request_data(data:RequestData):
    try:
        challan_no = data.Delivery_Challan_Number
        record = Gmrdata.objects(delivery_challan_number = challan_no).order_by("-created_at").first()
        entry_exists = Gmrrequest.objects(delivery_challan_number=challan_no,vehicle_number__exists=True).order_by("-created_at").first()
        
        if not record:    
            raise HTTPException(status_code=404, detail="Record not found")
        record.dc_request = True
        record.save()

        if not entry_exists:
            dc_data = Gmrrequest(
                                    delivery_challan_number = challan_no,
                                    vehicle_number = data.Vehicle_Truck_Registration_No.upper().strip() if data.Vehicle_Truck_Registration_No else data.Vehicle_Truck_Registration_No.strip(),
                                    arv_cum_do_number = data.ARV_Cum_DO_Number,
                                    mine = data.Mine_Name.upper(),
                                    net_qty = data.Net_Qty,
                                    delivery_challan_date = data.Delivery_Challan_Date,
                                    total_net_amount = data.Total_Net_Amount_of_Figures.replace(",",""),
                                    vehicle_chassis_number = data.Chassis_No,
                                    certificate_expiry = data.Certificate_will_expire_on,
                                    request = "DC_Expiry_Request",
                                    created_at = datetime.datetime.utcnow(),
                                    ID=Gmrrequest.objects.count() + 1)
            dc_data.save()
            record_num = Gmrrequest.objects(delivery_challan_number=challan_no,record_id__exists=True).order_by("-created_at").first()
            console_logger.debug(record_num.record_id)
            return {"message": "Successful"}
        return {"message": "Entry with this challan Number exist"}

    except NotUniqueError:
        new_record_id = uuid.uuid4().hex
        dc_data = Gmrrequest(
                            record_id=new_record_id,
                            delivery_challan_number = challan_no,
                            vehicle_number = data.Vehicle_Truck_Registration_No.upper().strip() if data.Vehicle_Truck_Registration_No else data.fitness.Vehicle_Truck_Registration_No.strip(),
                            arv_cum_do_number = data.ARV_Cum_DO_Number,
                            mine = data.Mine_Name.upper(),
                            net_qty = data.Net_Qty,
                            delivery_challan_date = data.Delivery_Challan_Date,
                            total_net_amount = data.Total_Net_Amount_of_Figures.replace(",",""),
                            vehicle_chassis_number = data.Chassis_No,
                            certificate_expiry = data.Certificate_will_expire_on,
                            request = "DC_Expiry_Request",                                
                            created_at = datetime.datetime.utcnow(),
                            ID=Gmrrequest.objects.count() + 1)
        dc_data.save()
        record_num = Gmrrequest.objects(delivery_challan_number=challan_no,record_id__exists=True).order_by("-created_at").first()
        console_logger.debug(record_num.record_id)
        return {"message": "Successful"}


@router.get("/road/fitness_validation_table", tags=["Road Map Request"])
def fitness_dc_validation(
    response: Response,
    currentPage: Optional[int] = None,
    perPage: Optional[int] = None,
    search_text: Optional[str] = None,
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
    search_type: Optional[str] = "fitness"
):
    try:
        result = {
            "labels": [],
            "datasets": [],
            "total": 0,
            "page_size": 15
        }

        page_no = 1
        page_len = result["page_size"]

        if currentPage:
            page_no = currentPage

        if perPage:
            page_len = perPage
            result["page_size"] = perPage

        data = Q(expiry_validation=True)

        if start_timestamp:
            start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M", "Asia/Kolkata", False)
            data &= Q(created_at__gte=start_date)

        if end_timestamp:
            end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M", "Asia/Kolkata", False)
            data &= Q(created_at__lte=end_date)

        if search_text:
            if search_text.isdigit():
                data &= Q(arv_cum_do_number__icontains=search_text) | Q(delivery_challan_number__icontains=search_text)
            else:
                data &= Q(vehicle_number__icontains=search_text)

        if search_type == "fitness":
            data &= Q(request="Fitness_Expiry_Request")
        else:
            data &= Q(request="DC_Expiry_Request")

        offset = (page_no - 1) * page_len

        logs = (
            Gmrrequest.objects(data)
            .order_by("-created_at")
            .skip(offset)
            .limit(page_len)
        )

        if logs:
            for log in logs:
                result["labels"] = list(log.payload().keys())
                result["datasets"].append(log.payload())

        result["total"] = Gmrrequest.objects(data).count()
        return result

    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/road/record_table", tags=["Road Map Request"])
def fitness_dc_record(
    response: Response,
    currentPage: Optional[int] = None,
    perPage: Optional[int] = None,
    search_text: Optional[str] = None,
    start_timestamp: Optional[str] = None,
    end_timestamp: Optional[str] = None,
    type: Optional[str] = "display",
    search_type: Optional[str] = "fitness"
):
    try:
        result = {
            "labels": [],
            "datasets": [],
            "total": 0,
            "page_size": 15
        }

        if type == "display":
            page_no = 1
            page_len = result["page_size"]

            if currentPage:
                page_no = currentPage

            if perPage:
                page_len = perPage
                result["page_size"] = perPage

            data = Q(approved_at__ne=None)

            if search_type == "fitness":
                data &= Q(request="Fitness_Expiry_Request")

            elif search_type == "tare":
                data &= Q(request="Tare_Diff_Request")

            else:
                data &= Q(request="DC_Expiry_Request")

            if start_timestamp:
                start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M", "Asia/Kolkata", False)
                data &= Q(approved_at__gte=start_date)

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M", "Asia/Kolkata", False)
                data &= Q(approved_at__lte=end_date)

            if search_text:
                if search_text.isdigit():
                    data &= Q(arv_cum_do_number__icontains=search_text) | Q(delivery_challan_number__icontains=search_text)
                else:
                    data &= Q(vehicle_number__icontains=search_text)

            offset = (page_no - 1) * page_len

            logs = (
                Gmrrequest.objects(data)
                .order_by("-approved_at")
                .skip(offset)
                .limit(page_len)
            )

            if logs:
                for log in logs:
                    result["labels"] = list(log.history_payload().keys())
                    result["datasets"].append(log.history_payload())

            result["total"] = Gmrrequest.objects(data).count()
            return result

        elif type == "download":
            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)

            data = Q(approved_at__ne=None)

            if search_type == "fitness":
                data &= Q(request="Fitness_Expiry_Request")

            elif search_type == "tare":
                data &= Q(request="Tare_Diff_Request")
                
            else:
                data &= Q(request="DC_Expiry_Request")

            if start_timestamp:
                start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M", "Asia/Kolkata", False)
                data &= Q(approved_at__gte=start_date)

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M", "Asia/Kolkata", False)
                data &= Q(approved_at__lte=end_date)

            if search_text:
                if search_text.isdigit():
                    data &= Q(arv_cum_do_number__icontains=search_text) | Q(delivery_challan_number__icontains=search_text)
                else:
                    data &= Q(vehicle_number__icontains=search_text)

            usecase_data = Gmrrequest.objects(data).order_by("-approved_at")
            count = len(usecase_data)
            path = None

            if usecase_data:
                try:
                    path = os.path.join(
                        "static_server",
                        "gmr_ai",
                        file,
                        "Approval_Report_{}.xlsx".format(
                            datetime.datetime.now().strftime("%Y-%m-%d:%H:%M:%S"),
                        ),
                    )
                    filename = os.path.join(os.getcwd(), path)
                    workbook = xlsxwriter.Workbook(filename)
                    workbook.use_zip64()
                    cell_format2 = workbook.add_format()
                    cell_format2.set_bold()
                    cell_format2.set_font_size(10)
                    cell_format2.set_align("center")
                    cell_format2.set_align("vjustify")

                    worksheet = workbook.add_worksheet()
                    worksheet.set_column("A:AZ", 20)
                    worksheet.set_default_row(50)
                    cell_format = workbook.add_format()
                    cell_format.set_font_size(10)
                    cell_format.set_align("center")
                    cell_format.set_align("vcenter")

                    headers = [
                        "Sr.No",
                        "Request Type",
                        "Mine",
                        "Vehicle Number",
                        "Delivery Challan No",
                        "DO No",
                        "Vehicle Chassis No",
                        "Fitness Expiry",
                        "DC Date",
                        "Challan Net Wt(MT)",
                        "Total Net Amount",
                        "Remark",
                        "Request Time",
                        "Approval Time",
                        "TAT"
                    ]

                    for index, header in enumerate(headers):
                        worksheet.write(0, index, header, cell_format2)

                    for row, query in enumerate(usecase_data, start=1):
                        result = query.history_payload()
                        worksheet.write(row, 0, count, cell_format)
                        worksheet.write(row, 1, str(result["Request_type"]), cell_format)
                        worksheet.write(row, 2, str(result["Mine"]), cell_format)
                        worksheet.write(row, 3, str(result["Vehicle_Number"]), cell_format)
                        worksheet.write(row, 4, str(result["Delivery_Challan_No"]), cell_format)
                        worksheet.write(row, 5, str(result["DO_No"]), cell_format)
                        worksheet.write(row, 6, str(result["Vehicle_Chassis_No"]), cell_format)
                        worksheet.write(row, 7, str(result["Fitness_Expiry"]), cell_format)
                        worksheet.write(row, 8, str(result["DC_Date"]), cell_format)
                        worksheet.write(row, 9, str(result["Challan_Net_Wt(MT)"]), cell_format)
                        worksheet.write(row, 10, str(result["Total_net_amount"]), cell_format)
                        worksheet.write(row, 11, str(result["Remark"]), cell_format)
                        worksheet.write(row, 12, str(result["Request_Time"]), cell_format)
                        worksheet.write(row, 13, str(result["Approval_Time"]), cell_format)
                        worksheet.write(row, 14, str(result["TAT"]), cell_format)
                        count -= 1

                    workbook.close()
                    console_logger.debug("Successfully {} report generated".format(service_id))
                    console_logger.debug("sent data {}".format(path))

                    return {
                        "Type": "Request_Approval_download_event",
                        "Datatype": "Report",
                        "File_Path": path,
                    }

                except Exception as e:
                    console_logger.debug(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
                    console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))

            else:
                console_logger.error("No data found")
                return {
                    "Type": "Request_Approval_download_event",
                    "Datatype": "Report",
                    "File_Path": path,
                }

    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


def add_days_to_date(date_str, days):
    date_format = "%d-%m-%Y"
    date_obj = datetime.datetime.strptime(date_str, date_format)
    new_date_obj = date_obj + timedelta(days=days)
    return new_date_obj.strftime(date_format)


@router.put("/road/update_expiry_date", tags=["Road Map Request"])
async def update_fc_expiry_date(vehicle_number: str, remark: Optional[str] = None):
    try:
        record = Gmrdata.objects(vehicle_number = vehicle_number).order_by("-created_at").first()
        request_record = Gmrrequest.objects(vehicle_number = vehicle_number, expiry_validation=True).order_by("-created_at").first()

        if remark == None:
            remark = "Fitness Extended For 7 days"

        if request_record:
            request_record.expiry_validation = False
            request_record.approved_at =  datetime.datetime.utcnow()
            request_record.remark = remark
            request_record.save()

        if not record:
            raise HTTPException(status_code=404, detail="Record not found")
        
        record.certificate_expiry = add_days_to_date(record.certificate_expiry, 7)
        # record.fitness_verify = False
        record.save()

        return {"message": "Record updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/road/pass_expiry_date", tags=["Road Map Request"])
async def pass_fc_expiry_date(vehicle_number: str, remark: Optional[str] = None):
    try:
        record = Gmrdata.objects(vehicle_number = vehicle_number).order_by("-created_at").first()
        request_record = Gmrrequest.objects(vehicle_number = vehicle_number, expiry_validation=True).order_by("-created_at").first()

        if remark == None:
            remark = "Fitness Extension Declined"

        if request_record:
            request_record.expiry_validation = False
            request_record.approved_at =  datetime.datetime.utcnow()
            request_record.remark = remark
            request_record.save()

        if not record:
            raise HTTPException(status_code=404, detail="Record not found")
            
        return {"message": "Record updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/road/update_dc_expiry", tags=["Road Map Request"])
async def update_dc_expiry(challan_no: str, remark: Optional[str] = None):
    try:
        record = Gmrdata.objects(delivery_challan_number = challan_no).order_by("-created_at").first()
        request_record = Gmrrequest.objects(delivery_challan_number = challan_no, expiry_validation=True).order_by("-created_at").first()

        if remark == None:
            remark = "DC Approved"

        if request_record:
            request_record.expiry_validation = False
            request_record.approved_at =  datetime.datetime.utcnow()
            request_record.remark = remark
            request_record.save()

        if not record:
            raise HTTPException(status_code=404, detail="Record not found")
        
        record.gate_approved = True
        record.save()

        return {"message": "Record updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/road/pass_dc_expiry", tags=["Road Map Request"])
async def pass_dc_expiry(challan_no: str, remark: Optional[str] = None):
    try:
        record = Gmrdata.objects(delivery_challan_number = challan_no).order_by("-created_at").first()
        request_record = Gmrrequest.objects(delivery_challan_number = challan_no, expiry_validation=True).order_by("-created_at").first()

        if remark == None:
            remark = "DC Declined"

        if request_record:
            request_record.expiry_validation = False
            request_record.approved_at =  datetime.datetime.utcnow()
            request_record.remark = remark
            request_record.save()

        if not record:
            raise HTTPException(status_code=404, detail="Record not found")
            
        return {"message": "Record updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete_expiry", tags=["Road Map Request"])
def delete_expiry(delivery_challan_number: str):
    try:
        challan = Gmrrequest.objects.get(delivery_challan_number = delivery_challan_number)
        challan.delete()
        return {"message": "Fitness Expired Entry deleted successfully"}
    except DoesNotExist:
        raise HTTPException(status_code=404, detail="Entry not found")


@router.post("/save_tare_request_data", tags=["Road Map Request"])
async def store_tare_request_data(data:RequestData):
    try:
        challan_no = data.Delivery_Challan_Number
        record = Gmrdata.objects(delivery_challan_number = challan_no).order_by("-created_at").first()
        entry_exists = Gmrrequest.objects(delivery_challan_number=challan_no,vehicle_number__exists=True).order_by("-created_at").first()
        
        if not record:    
            raise HTTPException(status_code=404, detail="Record not found")
        record.tare_request = True
        record.save()

        if not entry_exists:
            tare_data = Gmrrequest(
                                    delivery_challan_number = challan_no,
                                    vehicle_number = data.Vehicle_Truck_Registration_No.upper().strip() if data.Vehicle_Truck_Registration_No else data.Vehicle_Truck_Registration_No.strip(),
                                    arv_cum_do_number = data.ARV_Cum_DO_Number,
                                    mine = data.Mine_Name.upper(),
                                    net_qty = data.Net_Qty,
                                    tare_qty = data.Tare_Qty,
                                    delivery_challan_date = data.Delivery_Challan_Date,
                                    total_net_amount = data.Total_Net_Amount_of_Figures.replace(",",""),
                                    vehicle_chassis_number = data.Chassis_No,
                                    certificate_expiry = data.Certificate_will_expire_on,
                                    request = "Tare_Diff_Request",
                                    created_at = datetime.datetime.utcnow(),
                                    ID=Gmrrequest.objects.count() + 1)
            tare_data.save()
            record_num = Gmrrequest.objects(delivery_challan_number=challan_no,record_id__exists=True).order_by("-created_at").first()
            console_logger.debug(record_num.record_id)
            return {"message": "Successful"}
        return {"message": "Entry with this challan Number exist"}

    except NotUniqueError:
        new_record_id = uuid.uuid4().hex
        tare_data = Gmrrequest(
                            record_id=new_record_id,
                            delivery_challan_number = challan_no,
                            vehicle_number = data.Vehicle_Truck_Registration_No.upper().strip() if data.Vehicle_Truck_Registration_No else data.Vehicle_Truck_Registration_No.strip(),
                            arv_cum_do_number = data.ARV_Cum_DO_Number,
                            mine = data.Mine_Name.upper(),
                            net_qty = data.Net_Qty,
                            tare_qty = data.Tare_Qty,
                            delivery_challan_date = data.Delivery_Challan_Date,
                            total_net_amount = data.Total_Net_Amount_of_Figures.replace(",",""),
                            vehicle_chassis_number = data.Chassis_No,
                            certificate_expiry = data.Certificate_will_expire_on,
                            request = "Tare_Diff_Request",                                
                            created_at = datetime.datetime.utcnow(),
                            ID=Gmrrequest.objects.count() + 1)
        tare_data.save()
        record_num = Gmrrequest.objects(delivery_challan_number=challan_no,record_id__exists=True).order_by("-created_at").first()
        console_logger.debug(record_num.record_id)
        return {"message": "Successful"}


@router.put("/road/approve_tare_req", tags=["Road Map Request"])
async def update_tare(challan_no: str, remark: Optional[str] = None):
    try:
        record = Gmrdata.objects(delivery_challan_number = challan_no).order_by("-created_at").first()
        request_record = Gmrrequest.objects(delivery_challan_number = challan_no, expiry_validation=True).order_by("-created_at").first()

        if remark == None:
            remark = "Tare Approved"

        if request_record:
            request_record.expiry_validation = False
            request_record.approved_at =  datetime.datetime.utcnow()
            request_record.remark = remark
            request_record.save()

        if not record:
            raise HTTPException(status_code=404, detail="Record not found")
        
        # record.gate_approved = True
        record.tare_request = False
        record.save()

        return {"message": "Record updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/road/decline_tare_req", tags=["Road Map Request"])
async def decline_tare_req(challan_no: str, remark: Optional[str] = None):
    try:
        record = Gmrdata.objects(delivery_challan_number = challan_no).order_by("-created_at").first()
        request_record = Gmrrequest.objects(delivery_challan_number = challan_no, expiry_validation=True).order_by("-created_at").first()

        if remark == None:
            remark = "Tare Declined"

        if request_record:
            request_record.expiry_validation = False
            request_record.approved_at =  datetime.datetime.utcnow()
            request_record.remark = remark
            request_record.save()

        if not record:
            raise HTTPException(status_code=404, detail="Record not found")
        
        record.tare_request = True
        record.save()
            
        return {"message": "Record updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/road/minewise_road_graph", tags=["Road Map"])
def minewise_road_analysis(response:Response,type: Optional[str] = "Daily",
                            Month: Optional[str] = None, Daily: Optional[str] = None, 
                            Year: Optional[str] = None):
    try:
        data={}
        timezone = pytz.timezone('Asia/Kolkata')

        basePipeline = [
            {
                "$match": {
                    "GWEL_Tare_Time": {
                        "$gte": None,
                    },
                },
            },
            {
                "$project": {
                    # "ts": {
                    #     "$hour": {"date": "$GWEL_Tare_Time", "timezone": timezone},
                    # },
                    "ts": None,
                    "mine": "$mine",
                    "actual_net_qty": "$actual_net_qty",
                    "_id": 0
                },
            },
            {
                "$group": {
                    "_id": {
                        "ts": "$ts",
                        "mine": "$mine"
                    },
                    "data": {
                        "$push": "$actual_net_qty"
                    }
                }
            },
        ]

        if type == "Daily":

            date = Daily
            end_date = f'{date} 23:59:59'
            start_date = f'{date} 00:00:00'
            format_data = "%Y-%m-%d %H:%M:%S"
            endd_date = convert_to_utc_format(end_date.__str__(), format_data)
            startd_date = convert_to_utc_format(start_date.__str__(), format_data)

            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date

            basePipeline[1]["$project"]["ts"] = {"$hour": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}
            

            result = {
                "data": {
                    "labels": [str(i) for i in range(1, 25)],
                    "datasets": [
                        {"label": "YEKONA", "data": [0 for i in range(1, 25)]},
                        {"label": "SASTI", "data": [0 for i in range(1, 25)]},
                        {"label": "PENGANGA", "data": [0 for i in range(1, 25)]},
                        {"label": "MUNGOLI", "data": [0 for i in range(1, 25)]},
                        {"label": "NEELJAY", "data": [0 for i in range(1, 25)]},             
                    ],
                }
            }

        elif type == "Week":
            start_date = (
                datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
                - datetime.timedelta(days=7)
            )
            end_date = datetime.datetime.utcnow().replace(hour=23, minute=59, second=59, microsecond=0)
            endd_date = end_date-datetime.timedelta(days=1)

            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = convert_to_utc_format(start_date.__str__(), "%Y-%m-%d %H:%M:%S")
            basePipeline[1]["$project"]["ts"] = {"$dayOfMonth": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}
            result = {
                "data": {
                    "labels": [
                        (
                            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"]
                            + datetime.timedelta(days=i + 1)
                        ).strftime("%d")
                        for i in range(1, 8)
                    ],
                    "datasets": [
                        {"label": "YEKONA", "data": [0 for i in range(1, 8)]},
                        {"label": "SASTI", "data": [0 for i in range(1, 8)]},
                        {"label": "PENGANGA", "data": [0 for i in range(1, 8)]},
                        {"label": "MUNGOLI", "data": [0 for i in range(1, 8)]},
                        {"label": "NEELJAY", "data": [0 for i in range(1, 8)]},
                    ],
                }
            }

        elif type == "Month":
            date = Month
            format_data = "%Y - %m-%d"
            start_date = f'{date}-01'
            startd_date = timezone.localize(datetime.datetime.strptime(start_date, format_data))

            end_date = startd_date + relativedelta(day=31)
            end_label = end_date.strftime("%d")

            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$lte"] = end_date
            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date
            # basePipeline[1]["$project"]["ts"] = {"$dayOfMonth": "$GWEL_Tare_Time"}
            basePipeline[1]["$project"]["ts"] = {"$dayOfMonth": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}

            result = {
                "data": {
                    "labels": [
                        (
                            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"]
                            + datetime.timedelta(days=i + 1)
                        ).strftime("%d")
                        for i in range(-1, (int(end_label))-1)
                    ],
                    "datasets": [
                        {"label": "YEKONA", "data": [0 for i in range(-1, (int(end_label))-1)]},
                        {"label": "SASTI", "data": [0 for i in range(-1, (int(end_label))-1)]},
                        {"label": "PENGANGA", "data": [0 for i in range(-1, (int(end_label))-1)]},
                        {"label": "MUNGOLI", "data": [0 for i in range(-1, (int(end_label))-1)]},
                        {"label": "NEELJAY", "data": [0 for i in range(-1, (int(end_label))-1)]},
                    ],
                }
            }

        elif type == "Year":

            date = Year
            end_date = f'{date}-12-31 23:59:59'
            start_date = f'{date}-01-01 00:00:00'
            format_data = "%Y-%m-%d %H:%M:%S"
            endd_date = timezone.localize(datetime.datetime.strptime(end_date, format_data))
            startd_date = timezone.localize(datetime.datetime.strptime(start_date, format_data))

            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date

            # basePipeline[1]["$project"]["ts"] = {"$month": "$GWEL_Tare_Time"}
            basePipeline[1]["$project"]["ts"] = {"$month": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}
            result = {
                "data": {
                    "labels": [
                        (
                            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"]
                            + relativedelta(months=i)
                        ).strftime("%m")
                        for i in range(0, 12)
                    ],
                    "datasets": [
                        {"label": "YEKONA", "data": [0 for i in range(0, 12)]},
                        {"label": "SASTI", "data": [0 for i in range(0, 12)]},
                        {"label": "PENGANGA", "data": [0 for i in range(0, 12)]},
                        {"label": "MUNGOLI", "data": [0 for i in range(0, 12)]},
                        {"label": "NEELJAY", "data": [0 for i in range(0, 12)]},
                    ],
                }
            }

        output = Gmrdata.objects().aggregate(basePipeline)
        outputDict = {}

        for data in output:
            if "_id" in data:
                ts = data["_id"]["ts"]
                mine = data["_id"]["mine"]
                # console_logger.debug(ts)
                data_list = data.get('data', [])
                sum_list = []
                for item in data_list:
                    if item is not None:
                        try:
                            sum_value = float(item)
                            sum_list.append(sum_value)
                        except ValueError:
                            pass
                    else:
                        sum_list.append(0)
                    
                if ts not in outputDict:
                    outputDict[ts] = {mine: sum_list}
                else:
                    if mine not in outputDict[ts]:
                        outputDict[ts][mine] = sum_list
                    else:
                        outputDict[ts][mine].append(sum_list)

        modified_labels = [i for i in range(len(result["data"]["labels"]))]

        for index, label in enumerate(result["data"]["labels"]):
            if type == "Week":
                modified_labels = [
                    (
                        basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"]
                        + datetime.timedelta(days=i + 1)
                    ).strftime("%d-%m-%Y,%a")
                    for i in range(1, 8)
                ]
            
            elif type == "Month":
                modified_labels = [
                    (
                        basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"]
                        + datetime.timedelta(days=i + 1)
                    ).strftime("%d/%m")
                    for i in range(-1, (int(end_label))-1)
                ]

            elif type == "Year":
                modified_labels = [
                    (
                        basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"]
                        + relativedelta(months=i)
                    ).strftime("%b %y")
                    for i in range(0, 12)
                ]
            if int(label) in outputDict:
                for key, val in outputDict[int(label)].items():

                    total_sum = sum(val)

                    if key == "YEKONA":
                        result["data"]["datasets"][0]["data"][index] = total_sum

                    if key == "SASTI":
                        result["data"]["datasets"][1]["data"][index] = total_sum

                    if key == "PENGANGA":
                        result["data"]["datasets"][2]["data"][index] = total_sum

                    if key == "MUNGOLI":
                        result["data"]["datasets"][3]["data"][index] = total_sum

                    if key == "NEELJAY":
                        result["data"]["datasets"][4]["data"][index] = total_sum

        result["data"]["labels"] = copy.deepcopy(modified_labels)
        # console_logger.debug(f"-------- Road Minewise Graph Response -------- {result}")
        return result
    
    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e



@router.get("/minewise_road_table", tags=["Road Map"])
def gmr_table(response: Response, currentPage: Optional[int] = None,
                perPage: Optional[int] = None,
                mine: Optional[str] = "All",
                start_timestamp: Optional[str] = None,
                end_timestamp: Optional[str] = None,
                type: Optional[str] = "display"):
    try:
        data = {}
        result = {        
                "labels": [],
                "datasets": [],
                "weight_total":[],
                "total" : 0,
                "page_size": 15
        }

        if type and type == "display":

            page_no = 1
            page_len = result["page_size"]

            if currentPage:
                page_no = currentPage

            if perPage:
                page_len = perPage
                result["page_size"] = perPage

            if start_timestamp:
                data["created_at__gte"] = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")

            if end_timestamp:
                data["created_at__lte"] = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M")

            if mine and mine != "All":
                data["mine__icontains"] = mine.upper()
            
            offset = (page_no - 1) * page_len

            logs = (
                Gmrdata.objects(**data)
                .order_by("mine", "arv_cum_do_number", "-created_at")
                .skip(offset)
                .limit(page_len)
            )

            overall_totals = {
                "Challan_Gross_Wt(MT)": 0,
                "Challan_Tare_Wt(MT)": 0,
                "Challan_Net_Wt(MT)": 0,
                "GWEL_Gross_Wt(MT)": 0,
                "GWEL_Tare_Wt(MT)": 0,
                "GWEL_Net_Wt(MT)": 0,
            }
            mine_grouped_data = {}

            if any(logs):
                for log in logs:
                    payload = log.payload()
                    result["labels"] = list(payload.keys())
                    mine_name = payload.get("Mines_Name")

                    if mine_name not in mine_grouped_data:
                        mine_grouped_data[mine_name] = []

                    mine_grouped_data[mine_name].append(payload)

                    for key in overall_totals:
                        value = payload.get(key)
                        if value is None:
                            value = 0.0
                        else:
                            try:
                                value = float(value)
                            except ValueError:
                                value = 0.0
                        
                        overall_totals[key] += value

                overall_totals = {key: str(overall_totals[key]) for key in overall_totals}

                for mine_name, records in mine_grouped_data.items():
                    result["datasets"].append({mine_name: records})
                result["weight_total"].append(overall_totals)

            result["total"] = Gmrdata.objects(**data).count()

            return result


        elif type and type == "download":
            del type

            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)

            if start_timestamp:
                data["created_at__gte"] = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")

            if end_timestamp:
                data["created_at__lte"] = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M")

            if mine and mine != "All":
                data["mine__icontains"] = mine.upper()

            usecase_data = Gmrdata.objects(**data).order_by("-created_at")
            count = len(usecase_data)
            path = None
            if usecase_data:
                try:
                    path = os.path.join(
                        "static_server",
                        "gmr_ai",
                        file,
                        "Minewise_Report_{}.xlsx".format(
                            datetime.datetime.now().strftime("%Y-%m-%d:%H:%M:%S"),
                        ),
                    )
                    filename = os.path.join(os.getcwd(), path)
                    workbook = xlsxwriter.Workbook(filename)
                    workbook.use_zip64()
                    cell_format2 = workbook.add_format()
                    cell_format2.set_bold()
                    cell_format2.set_font_size(10)
                    cell_format2.set_align("center")
                    cell_format2.set_align("vjustify")

                    worksheet = workbook.add_worksheet()
                    worksheet.set_column("A:AZ", 20)
                    worksheet.set_default_row(50)
                    cell_format = workbook.add_format()
                    cell_format.set_font_size(10)
                    cell_format.set_align("center")
                    cell_format.set_align("vcenter")

                    headers = [
                        "PO No",
                        "DO No",
                        "Mines Name",
                        "Vehicle No.",
                        "Total Net Amount",
                        "Gross Wt. as per challan (MT)",
                        "Tare Wt. as per challan (MT)",
                        "Net Wt. as per challan (MT)",
                        "Gross Wt. as per actual (MT)",
                        "Tare Wt. as per actual (MT)",
                        "Net Wt. as per actual (MT)",
                        "Vehicle In Time",
                        "Transit Loss",
                        "LOT",
                        "Line_Item"
                    ]

                    for index, header in enumerate(headers):
                        worksheet.write(0, index, header, cell_format2)
                    for row, query in enumerate(usecase_data,start=1):
                        result = query.payload()
                        worksheet.write(row, 0, count, cell_format)
                        worksheet.write(row, 1, str(result["PO_No"]), cell_format)
                        worksheet.write(row, 2, str(result["DO_No"]), cell_format)
                        worksheet.write(row, 3, str(result["Mines_Name"]), cell_format)
                        worksheet.write(row, 4, str(result["vehicle_number"]), cell_format)
                        worksheet.write(row, 5, str(result["Total_net_amount"]), cell_format)
                        worksheet.write(row, 6, str(result["Challan_Gross_Wt(MT)"]), cell_format)
                        worksheet.write(row, 7, str(result["Challan_Tare_Wt(MT)"]), cell_format)
                        worksheet.write(row, 8, str(result["Challan_Net_Wt(MT)"]), cell_format)
                        worksheet.write(row, 9, str(result["GWEL_Gross_Wt(MT)"]), cell_format)
                        worksheet.write(row, 10, str(result["GWEL_Tare_Wt(MT)"]), cell_format)
                        worksheet.write(row, 11, str(result["GWEL_Net_Wt(MT)"]), cell_format)
                        worksheet.write(row, 12, str(result["Vehicle_in_time"]), cell_format)
                        worksheet.write(row, 13, str(result["Transit_Loss"]), cell_format)
                        worksheet.write(row, 14, str(result["LOT"]), cell_format)
                        worksheet.write(row, 15, str(result["Line_Item"]), cell_format)
                        count-=1
                        
                    workbook.close()
                    console_logger.debug("Successfully {} report generated".format(service_id))
                    console_logger.debug("sent data {}".format(path))

                    return {
                            "Type": "Minewise_road_journey_download_event",
                            "Datatype": "Report",
                            "File_Path": path,
                            }
                
                except Exception as e:
                    console_logger.debug(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
                    console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
            else:
                console_logger.error("No data found")
                return {
                        "Type": "Minewise_road_journey_download_event",
                        "Datatype": "Report",
                        "File_Path": path,
                        }

    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/road/vehicle_scanned_count", tags=["Road Map"])
def daywise_vehicle_scanned_count(response:Response):
    try:
        current_time = datetime.datetime.now(IST)
        today = current_time.date()
        startdate = f'{today} 00:00:00'
        # from_ts = datetime.datetime.strptime(startdate,"%Y-%m-%d %H:%M:%S")
        from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")

        # vehicle_count = Gmrdata.objects(created_at__gte=from_ts, created_at__ne=None).count()
        vehicle_count = Gmrdata.objects(GWEL_Tare_Time__gte=from_ts, GWEL_Tare_Time__ne=None).count()

        return {"title": "Today's Mine Vehicle Scanned",
                "icon" : "vehicle",
                "data": vehicle_count,
                "last_updated": today}

    except Exception as e:
        console_logger.debug("----- Vehicle Scanned Count Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/road/vehicle_count", tags=["Road Map"])
def daywise_vehicle_count(response:Response):
    try:
        current_time = datetime.datetime.now(IST)
        today = current_time.date()
        startdate = f'{today} 00:00:00'
        # from_ts = datetime.datetime.strptime(startdate,"%Y-%m-%d %H:%M:%S")
        from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")

        vehicle_in_count = Gmrdata.objects(GWEL_Tare_Time__gte=from_ts, GWEL_Tare_Time__ne=None).count()
        vehicle_out_count = Gmrdata.objects(GWEL_Tare_Time__gte=from_ts, GWEL_Tare_Time__ne=None).count()

        return {"title": "Today's Gate Vehicle",
                "icon" : "vehicle",
                "data":f"In: {vehicle_in_count} | Out: {vehicle_out_count}",
                "last_updated": today}

    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/road/grn_coal", tags=["Road Map"])
def daywise_grn_receive(response:Response):
    try:
        current_time = datetime.datetime.now(IST)
        today = current_time.date()
        startdate = f'{today} 00:00:00'
        # from_ts = datetime.datetime.strptime(startdate,"%Y-%m-%d %H:%M:%S")
        from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")

        pipeline = [
                    {
                        "$match": {
                            "GWEL_Tare_Time": {"$gte": from_ts},
                            "net_qty": {"$ne": None}
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "total_net_qty": {
                                "$sum": {
                                    "$toDouble": "$net_qty"
                                }
                            }
                        }
                    }]
        
        result = Gmrdata.objects.aggregate(pipeline)

        total_coal = 0
        for doc in result:
            total_coal = doc["total_net_qty"]

        return {"title": "Today's Total GRN Coal(MT)",
                "icon" : "coal",
                "data": round(total_coal,2),
                "last_updated": today}

    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e
    

@router.get("/road/transit_loss_card", tags=["Road Map"])
def daywise_transit_loss(response:Response):
    try:
        current_time = datetime.datetime.now(IST)
        today = current_time.date()
        startdate = f'{today} 00:00:00'
        # from_ts = datetime.datetime.strptime(startdate,"%Y-%m-%d %H:%M:%S")
        from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")
    
        pipeline = [
            {
                '$match': {
                    'GWEL_Tare_Time': {
                        '$gte': from_ts
                    }
                }
            }, {
                '$group': {
                    '_id': None, 
                    'net_qty': {
                        '$sum': {
                            '$toDouble': '$net_qty'
                        }
                    }, 
                    'actual_net_qty': {
                        '$sum': {
                            '$toDouble': '$actual_net_qty'
                        }
                    }
                }
            }, {
                '$project': {
                    'net_qty': 1, 
                    'actual_net_qty': 1, 
                    'transit_loss': {
                        '$subtract': [
                            '$actual_net_qty', '$net_qty'
                        ]
                    }
                }
            }
        ]
        
        result = Gmrdata.objects.aggregate(pipeline)

        transit_loss = 0
        for doc in result:
            transit_loss = doc["transit_loss"]

        return {"title": "Today's Total Transit Loss (MT)",
                "icon" : "coal",
                "data": round(transit_loss, 2),
                "last_updated": today}

    except Exception as e:
        console_logger.debug("----- Transit Loss Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/road/gwel_coal", tags=["Road Map"])
def daywise_gwel_receive(response:Response):
    try:
        current_time = datetime.datetime.now(IST)
        today = current_time.date()
        startdate = f'{today} 00:00:00'
        # from_ts = datetime.datetime.strptime(startdate,"%Y-%m-%d %H:%M:%S")
        from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")


        pipeline = [
                    {
                        "$match": {
                            "GWEL_Tare_Time": {"$gte": from_ts},
                            "actual_net_qty": {"$ne": None}
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "total_actual_net_qty": {
                                "$sum": {
                                    "$toDouble": "$actual_net_qty"
                                }
                            }
                        }
                    }]
        
        result = Gmrdata.objects.aggregate(pipeline)

        total_coal = 0
        for doc in result:
            total_coal = doc["total_actual_net_qty"]

        return {"title": "Today's Total GWEL Coal(MT)",
                "icon" : "coal",
                "data": round(total_coal,2),
                "last_updated": today}

    except Exception as e:
        console_logger.debug("----- Total GWEL Coal Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/road/coal_generation", tags=["Road Map"])
def daywise_coal_generation(response: Response):
    try:
        current_time = datetime.datetime.now(IST)
        today = current_time.date()
        startdate = f'{today} 00:00:00'
        from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")

        pipeline = [
            {
                "$match": {
                    "created_date": {"$gte": from_ts},
                    "tagid": {"$in": [3536, 2]},
                    "sum": {"$ne": None}
                }
            },
            {
                "$group": {
                    "_id": "$tagid",
                    "total_sum": {
                        "$sum": {
                            "$toDouble": "$sum"
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_sum_all": {
                        "$sum": "$total_sum"
                    }
                }
            }
        ]

        result = Historian.objects.aggregate(pipeline)

        total_sum = 0
        for doc in result:
            total_sum = doc["total_sum_all"]
        
        # result = total_sum / 10000
        result = total_sum

        return {
            "title": "Today's Average Generation(MW)",
            "icon" : "energy",
            "data": round(result, 2),
            "last_updated": today
        }

    except Exception as e:
        console_logger.debug(f"----- Coal Generation Error -----{e}")
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return {"error": str(e)}


@router.get("/road/coal_consumption", tags=["Road Map"])
def daywise_coal_consumption(response: Response):
    try:
        current_time = datetime.datetime.now(IST)
        today = current_time.date()
        startdate = f'{today} 00:00:00'
        from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")

        pipeline = [
            {
                "$match": {
                    "created_date": {"$gte": from_ts},
                    "tagid": {"$in": [16,3538]},
                    "sum": {"$ne": None}
                }
            },
            {
                "$group": {
                    "_id": "$tagid",
                    "total_sum": {
                        "$sum": {
                            "$toDouble": "$sum"
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_sum_all": {
                        "$sum": "$total_sum"
                    }
                }
            }
        ]

        result = Historian.objects.aggregate(pipeline)

        total_sum = 0
        for doc in result:
            total_sum = doc["total_sum_all"]
        
        # result = total_sum / 10000
        result = total_sum

        return {
            "title": "Today's Total Coal Consumption(MT)",
            "icon" : "coal",
            "data": round(result, 2),
            "last_updated": today
        }

    except Exception as e:
        console_logger.debug(f"----- Coal Consumption Error ----- {e}")
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return {"error": str(e)}


@router.get("/road/minewise_road_report", tags=["Road Map"])
def road_report(response:Response,start_timestamp: Optional[str] = None,
                end_timestamp: Optional[str] = None,
                type: Optional[str] = "display"):
    try:
        current_time = datetime.datetime.now(IST)
        today = current_time.date()
        startdate = f'{today} 00:00:00'
        # from_ts = datetime.datetime.strptime(startdate,"%Y-%m-%d %H:%M:%S")
        from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")


        data = {"data": {}, "Total": {"mine_vehicle_scanned": 0, 
                                      "Gate_vehicle_in":0,
                                      "Gate_vehicle_out":0,
                                      "GRN_Coal(MT)": 0, 
                                      "GWEL_Coal(MT)": 0, 
                                      "Transit_Loss(MT)": 0}}

        pipeline = [
            {
                "$facet": {
                    "weight": [
                        {
                            "$match": {
                                "GWEL_Tare_Time": {"$gte": from_ts}
                            }
                        },
                        {
                            "$group": {
                                "_id": "$mine",
                                "net_qty": {
                                    "$sum": {
                                        "$toDouble": "$net_qty"
                                    }
                                },
                                "actual_net_qty": {
                                    "$sum": {
                                        "$toDouble": "$actual_net_qty"
                                    }
                                }
                            }
                        },
                        {
                            "$project": {
                                "net_qty": 1,
                                "actual_net_qty": 1,
                                "transit": {
                                    "$subtract": [
                                        "$actual_net_qty", "$net_qty"
                                    ]
                                }
                            }
                        }
                    ],
                    "scanned": [
                        {
                            "$match": {
                                "GWEL_Tare_Time": {"$gte": from_ts}
                            }
                        },
                        {
                            "$group": {
                                "_id": "$mine",
                                "vehicle_count": {
                                    "$sum": 1
                                }
                            }
                        },
                        {
                            "$project": {
                                "vehicle_count": "$vehicle_count"
                            }
                        }
                    ],
                    "vehicle_in": [
                        {
                            "$match": {
                                "GWEL_Tare_Time": {"$gte": from_ts}
                            }
                        },
                        {
                            "$group": {
                                "_id": "$mine",
                                "vehicle_count": {
                                    "$sum": 1
                                }
                            }
                        },
                        {
                            "$project": {
                                "mine": "$_id",
                                "vehicle_in_count": "$vehicle_count",
                                "_id": 0
                            }
                        }
                    ],
                    "vehicle_out": [
                        {
                            "$match": {
                                "GWEL_Tare_Time": {"$gte": from_ts}
                            }
                        },
                        {
                            "$group": {
                                "_id": "$mine",
                                "vehicle_count": {
                                    "$sum": 1
                                }
                            }
                        },
                        {
                            "$project": {
                                "mine": "$_id",
                                "vehicle_out_count": "$vehicle_count",
                                "_id": 0
                            }
                        }
                    ]
                }
            },
            {
                "$project": {
                    "weight": 1,
                    "scanned": 1,
                    "vehicle_in": 1,
                    "vehicle_out": 1
                }
            }
        ]

        if type == "display":
            if start_timestamp:
                end_date = f'{start_timestamp} 23:59:59'
                start_date = f'{start_timestamp} 00:00:00'
                format_data = "%Y-%m-%d %H:%M:%S"

                startd_date = convert_to_utc_format(start_date, format_data)
                endd_date = convert_to_utc_format(end_date, format_data)

                pipeline[0]["$facet"]["weight"][0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
                pipeline[0]["$facet"]["weight"][0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date

                pipeline[0]["$facet"]["scanned"][0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
                pipeline[0]["$facet"]["scanned"][0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date

                pipeline[0]["$facet"]["vehicle_in"][0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
                pipeline[0]["$facet"]["vehicle_in"][0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date

                pipeline[0]["$facet"]["vehicle_out"][0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
                pipeline[0]["$facet"]["vehicle_out"][0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date

            combined_pipeline_data = list(Gmrdata.objects.aggregate(pipeline))[0]
            # mine_data = list(Gmrdata.objects.aggregate(mine_pipeline))

            weight_data = combined_pipeline_data.get("weight", [])
            scanned_data = combined_pipeline_data.get("scanned", [])
            vehicle_in_data = combined_pipeline_data.get("vehicle_in", [])
            vehicle_out_data = combined_pipeline_data.get("vehicle_out", [])

            scanned_dict = {item["_id"]: item["vehicle_count"] for item in scanned_data}
            vehicle_in_dict = {item["mine"]: item["vehicle_in_count"] for item in vehicle_in_data}
            vehicle_out_dict = {item["mine"]: item["vehicle_out_count"] for item in vehicle_out_data}

            for mine in weight_data:
                mine_name = mine["_id"]
                net_qty = mine["net_qty"]
                actual_net_qty = mine["actual_net_qty"]
                transit_loss = mine["transit"]

                scanned_count = scanned_dict.get(mine_name, 0)
                vehicle_in_count = vehicle_in_dict.get(mine_name, 0)
                vehicle_out_count = vehicle_out_dict.get(mine_name, 0)

                data["data"][mine_name] = {
                    "mine_vehicle_scanned": scanned_count,
                    "Gate_vehicle_in": vehicle_in_count,
                    "Gate_vehicle_out": vehicle_out_count,
                    "GRN_Coal(MT)": round(net_qty, 2),
                    "GWEL_Coal(MT)": round(actual_net_qty, 2),
                    "Transit_Loss(MT)": round(transit_loss, 2)
                }

                data["Total"]["mine_vehicle_scanned"] += scanned_count
                data["Total"]["Gate_vehicle_in"] += vehicle_in_count
                data["Total"]["Gate_vehicle_out"] += vehicle_out_count
                data["Total"]["GRN_Coal(MT)"] += round(net_qty, 2)
                data["Total"]["GWEL_Coal(MT)"] += round(actual_net_qty, 2)
                data["Total"]["Transit_Loss(MT)"] += round(transit_loss, 2)


            return data

        elif type and type == "download":
            del type

            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)

            path = os.path.join(
                        "static_server",
                        "gmr_ai",
                        file,
                        "mine_count_Report_{}.xlsx".format(
                            datetime.datetime.now().strftime("%Y-%m-%d:%H:%M:%S"),
                        ),
                    )

            if start_timestamp and end_timestamp:
                end_date = f'{start_timestamp} 23:59:59'
                start_date = f'{start_timestamp} 00:00:00'
                format_data = "%Y-%m-%d %H:%M:%S"

                startd_date = convert_to_utc_format(start_date, format_data)
                endd_date = convert_to_utc_format(end_date, format_data)

                pipeline[0]["$facet"]["weight"][0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
                pipeline[0]["$facet"]["weight"][0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date

                pipeline[0]["$facet"]["scanned"][0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
                pipeline[0]["$facet"]["scanned"][0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date

                pipeline[0]["$facet"]["vehicle_in"][0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
                pipeline[0]["$facet"]["vehicle_in"][0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date

                pipeline[0]["$facet"]["vehicle_out"][0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
                pipeline[0]["$facet"]["vehicle_out"][0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date

            combined_pipeline_data = list(Gmrdata.objects.aggregate(pipeline))[0]
            # mine_data = list(Gmrdata.objects.aggregate(mine_pipeline))

            weight_data = combined_pipeline_data.get("weight", [])
            scanned_data = combined_pipeline_data.get("scanned", [])
            vehicle_in_data = combined_pipeline_data.get("vehicle_in", [])
            vehicle_out_data = combined_pipeline_data.get("vehicle_out", [])

            scanned_dict = {item["_id"]: item["vehicle_count"] for item in scanned_data}
            vehicle_in_dict = {item["mine"]: item["vehicle_in_count"] for item in vehicle_in_data}
            vehicle_out_dict = {item["mine"]: item["vehicle_out_count"] for item in vehicle_out_data}

            for mine in weight_data:
                mine_name = mine["_id"]
                net_qty = mine["net_qty"]
                actual_net_qty = mine["actual_net_qty"]
                transit_loss = mine["transit"]

                scanned_count = scanned_dict.get(mine_name, 0)
                vehicle_in_count = vehicle_in_dict.get(mine_name, 0)
                vehicle_out_count = vehicle_out_dict.get(mine_name, 0)

                data["data"][mine_name] = {
                    "mine_vehicle_scanned": scanned_count,
                    "Gate_vehicle_in": vehicle_in_count,
                    "Gate_vehicle_out": vehicle_out_count,
                    "GRN_Coal(MT)": round(net_qty, 2),
                    "GWEL_Coal(MT)": round(actual_net_qty, 2),
                    "Transit_Loss(MT)": round(transit_loss, 2)
                }

                data["Total"]["mine_vehicle_scanned"] += scanned_count
                data["Total"]["Gate_vehicle_in"] += vehicle_in_count
                data["Total"]["Gate_vehicle_out"] += vehicle_out_count
                data["Total"]["GRN_Coal(MT)"] += round(net_qty, 2)
                data["Total"]["GWEL_Coal(MT)"] += round(actual_net_qty, 2)
                data["Total"]["Transit_Loss(MT)"] += round(transit_loss, 2)

            df_data = pd.DataFrame.from_dict(data['data'], orient='index')

            total_df = pd.DataFrame.from_dict({'Total': data['Total']}).T
            df_data = pd.concat([df_data, total_df], axis=0)
            df_data.columns = df_data.columns.str.replace('_', ' ')

            df_data.to_excel(path, sheet_name='Report')

            console_logger.debug("Successfully {} report generated".format(service_id))
            console_logger.debug("sent data {}".format(path))

            return {
                    "Type": "Minewise_report_download_event",
                    "Datatype": "Report",
                    "File_Path": path,
                    }
        
        else:
            console_logger.error("No data found")
            return {
                    "Type": "Minewise_report_download_event",
                    "Datatype": "Report",
                    "File_Path": path,
                    }

    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


def daywise_in_vehicle_count_datewise(date):
    try:
        startdate = f'{date} 00:00:00'
        enddate = f'{date} 23:59:59'
        # from_ts = datetime.datetime.strptime(startdate,"%Y-%m-%d %H:%M:%S")
        # to_ts = datetime.datetime.strptime(enddate,"%Y-%m-%d %H:%M:%S")
        from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")
        to_ts = convert_to_utc_format(enddate, "%Y-%m-%d %H:%M:%S")

        vehicle_count = Gmrdata.objects(GWEL_Tare_Time__gte=from_ts, GWEL_Tare_Time__lte=to_ts, GWEL_Tare_Time__ne=None).count()

        return {"title": "Vehicle in count",
                "data": vehicle_count}
    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


def daywise_grn_receive_datewise(date):
    try:
        startdate = f'{date} 00:00:00'
        enddate = f'{date} 23:59:59'

        from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")
        to_ts = convert_to_utc_format(enddate, "%Y-%m-%d %H:%M:%S")

        pipeline = [
                    {
                        "$match": {
                            "GWEL_Tare_Time": {"$gte": from_ts, "$lte": to_ts},
                                "net_qty": {"$ne": None}
                            }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "total_net_qty": {
                                "$sum": {
                                    "$toDouble": "$net_qty"  # Convert net_qty to a numeric type before summing
                                }
                            }
                        }
                    }]
        console_logger.debug(pipeline)
        result = Gmrdata.objects.aggregate(pipeline)

        total_coal = 0
        for doc in result:
            total_coal = doc["total_net_qty"]

        # console_logger.debug({"title": "Total GRN Coal(MT)",
        #         "data": round(total_coal,2)})

        return {"title": "Total GRN Coal(MT)",
                "data": round(total_coal,2)}

    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


def daywise_gwel_receive_pdf_datewise(date):
    try:
        startdate = f'{date} 00:00:00'
        enddate = f'{date} 23:59:59'
        from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")
        to_ts = convert_to_utc_format(enddate, "%Y-%m-%d %H:%M:%S")
        pipeline = [
            {
                "$match": {
                    "GWEL_Tare_Time": {"$gte": from_ts, "$lte": to_ts},  # Combine conditions into one
                    "actual_net_qty": {"$ne": None}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_actual_net_qty": {
                        "$sum": {
                            "$toDouble": "$actual_net_qty"  # Convert actual_net_qty to a numeric type before summing
                        }
                    }
                }
            }
        ]
        # pipeline = [
        #     {
        #         "$match": {
        #             "GWEL_Tare_Time": {"$gte": from_ts, "$lte": to_ts},
        #             "actual_net_qty": {"$ne": None}
        #         }
        #     },
        #     {
        #         "$addFields": {
        #             "actual_net_qty": {
        #                 "$cond": {
        #                     "if": {"$isNumber": "$actual_net_qty"},
        #                     "then": "$actual_net_qty",
        #                     "else": 0
        #                 }
        #             }
        #         }
        #     },
        #     {
        #         "$group": {
        #             "_id": None,
        #             "total_actual_net_qty": {
        #                 "$sum": {
        #                     "$toDouble": {
        #                         "$ifNull": ["$actual_net_qty", 0]  # Handle NaN values
        #                     }
        #                 }
        #             }
        #         }
        #     }
        # ]
        
        result = Gmrdata.objects.aggregate(pipeline)

        total_coal = 0
        for doc in result:
            total_coal = doc["total_actual_net_qty"]


        return {"title": "Total GWEL Coal(MT)",
                "data": round(total_coal, 2)}

    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return "Error occurred: {}".format(e)


def daywise_out_vehicle_count_datewise(date):
    try:
        startdate = f'{date} 00:00:00'
        enddate = f'{date} 23:59:59'

        from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")
        to_ts = convert_to_utc_format(enddate, "%Y-%m-%d %H:%M:%S")

        # vehicle_count = Gmrdata.objects(created_at__gte=from_ts, created_at__lte=to_ts, vehicle_out_time__ne=None).count()
        vehicle_count = Gmrdata.objects(GWEL_Tare_Time__gte=from_ts, GWEL_Tare_Time__lte=to_ts, GWEL_Tare_Time__ne=None).count()

        return {"title": "Vehicle out count",
                "data": vehicle_count}

    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


def bar_graph_data(specified_date):
    try:
        if specified_date:
            
            specified_date = datetime.datetime.strptime(specified_date, "%Y-%m-%d")
            # specified_date = convert_to_utc_format(specified_date, "%Y-%m-%d")
            start_of_month = specified_date.replace(day=1)
            start_of_month = datetime.datetime.strftime(start_of_month, '%Y-%m-%d')
            end_of_month = datetime.datetime.strftime(specified_date, '%Y-%m-%d')

            # Query for CoalTesting objects
            fetchCoalTesting = CoalTesting.objects(
                receive_date__gte= datetime.datetime.strptime(start_of_month, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"), receive_date__lte= datetime.datetime.strptime(end_of_month, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M")
            )
            # Query for CoalTestingTrain objects
            fetchCoalTestingTrain = CoalTestingTrain.objects(
                receive_date__gte = datetime.datetime.strptime(start_of_month, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"), receive_date__lte= datetime.datetime.strptime(end_of_month, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M")
            )

            # Query for GMRData objects
            # fetchGmrData = Gmrdata.objects(created_at__gte=datetime.datetime.strptime(start_of_month, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"), created_at__lte=datetime.datetime.strptime(end_of_month, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"))
            fetchGmrData = Gmrdata.objects(
                GWEL_Tare_Time__gte=f"{start_of_month}T00:00:00",
                GWEL_Tare_Time__lte=f"{end_of_month}T23:59:59"
            )
            fetchRailData = RailData.objects(
                created_at__gte=f"{start_of_month}T00:00:00",
                created_at__lte=f"{end_of_month}T23:59:59"
            )
            rrNo_values = {}

            # Iterate through fetched CoalTesting objects
            for single_coal_testing in fetchCoalTesting:
                rrNo = single_coal_testing.rrNo
                location = single_coal_testing.location
                for param in single_coal_testing.parameters:
                    if param["parameter_Name"] == "Gross_Calorific_Value_(Arb)":
                        if param["val1"] != None and param["val1"] != "":
                            calorific_value = float(param["val1"])
                            break
                else:
                    continue

                # Aggregate values based on rrNo
                if rrNo in rrNo_values:
                    rrNo_values[location] += calorific_value
                else:
                    rrNo_values[location] = calorific_value

            # Iterate through fetched CoalTestingTrain objects
            for single_coal_testing_train in fetchCoalTestingTrain:
                rrNo = single_coal_testing_train.rrNo
                location = single_coal_testing_train.location
                for param in single_coal_testing_train.parameters:
                    if param["parameter_Name"] == "Gross_Calorific_Value_(Arb)":
                        if param["val1"] != None:
                            calorific_value = float(param["val1"])
                            break
                else:
                    continue

                # Aggregate values based on rrNo
                if rrNo in rrNo_values:
                    rrNo_values[location] += calorific_value
                else:
                    rrNo_values[location] = calorific_value
            
            
            # fetch data from AopTarget
            aopList = []
            fetchAopTarget = AopTarget.objects()
            if fetchAopTarget:
                for single_aop_target in fetchAopTarget:
                    aopList.append(single_aop_target.payload())

            net_qty_totals = {}
            actual_net_qty_totals = {}

            # Iterate over the retrieved data
            for single_gmr_data in fetchGmrData:
                mine_name = single_gmr_data.mine
                net_qty = single_gmr_data.net_qty
                actual_net_qty = single_gmr_data.actual_net_qty

                # net_qty_totals[mine_name] += float(net_qty)
                if mine_name in net_qty_totals:
                    net_qty_totals[mine_name] += float(net_qty)
                else:
                    net_qty_totals[mine_name] = float(net_qty)
                if actual_net_qty:
                    # actual_net_qty_totals[mine_name] += float(actual_net_qty)
                    if mine_name in actual_net_qty_totals:
                        actual_net_qty_totals[mine_name] += float(actual_net_qty)
                    else:
                        actual_net_qty_totals[mine_name] = float(actual_net_qty)

            for single_rail_data in fetchRailData:
                rail_mine_name = single_rail_data.source
                rail_net_qty = single_rail_data.total_secl_net_wt
                rail_actual_net_qty = single_rail_data.total_rly_net_wt

                if rail_mine_name in net_qty_totals:
                    net_qty_totals[rail_mine_name] += float(rail_net_qty)
                else:
                    net_qty_totals[rail_mine_name] = float(rail_net_qty)
                if rail_actual_net_qty:
                    # actual_net_qty_totals[mine_name] += float(actual_net_qty)
                    if rail_mine_name in actual_net_qty_totals:
                        actual_net_qty_totals[rail_mine_name] += float(rail_actual_net_qty)
                    else:
                        actual_net_qty_totals[rail_mine_name] = float(rail_actual_net_qty)

            clubbed_data = {
                mine: actual_net_qty_totals[mine] - net_qty_totals[mine]
                for mine in net_qty_totals
            }

            return rrNo_values, clubbed_data, aopList
    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


def get_financial_year(datestring):
    try:
        # date = datetime.datetime.strptime(datestring, "%Y-%m-%d").date()
        date = convert_to_utc_format(datestring, "%Y-%m-%d").date()
        # Initialize the current year
        year_of_date = date.year
        # Initialize the current financial year start date
        # financial_year_start_date = datetime.datetime.strptime(str(year_of_date) + "-04-01", "%Y-%m-%d").date()
        financial_year_start_date = convert_to_utc_format(str(year_of_date) + "-04-01", "%Y-%m-%d").date()
        if date < financial_year_start_date:
            return {"start_date": f"{financial_year_start_date.year}-04-01", "end_date": f"{financial_year_start_date.year+1}-03-31"}
        else:
            return {"start_date": f"{financial_year_start_date.year}-04-01", "end_date": f"{financial_year_start_date.year+1}-03-31"}
    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

# def transit_loss_gain_road_mode_month(specified_date):
#     try:
#         data = {}
#         result = {
#             "labels": [],
#             "datasets": [],
#             "weight_total": [],
#             "total": 0,
#             "page_size": 15,
#         }
        
#         financial_year = get_financial_year(datetime.date.today().strftime("%Y-%m-%d"))

#         # logs = (
#         #     Gmrdata.objects(created_at__gte=financial_year.get("start_date"), created_at__lte=specified_date)
#         # )
#         logs = (
#             Gmrdata.objects(GWEL_Tare_Time__gte=financial_year.get("start_date"), GWEL_Tare_Time__lte=specified_date)
#         )

#         if any(logs):
#             aggregated_data = defaultdict(
#                 lambda: defaultdict(
#                     lambda: {
#                         "net_qty": 0,
#                         "mine_name": "",
#                         "actual_net_qty": 0,
#                         "count": 0,
#                     }
#                 )
#             )

#             start_dates = {}

#             for log in logs:
#                 if log.GWEL_Tare_Time is not None:
#                     month = log.GWEL_Tare_Time.strftime("%Y-%m")
#                     payload = log.payload()
#                     result["labels"] = list(payload.keys())
#                     mine_name = payload.get("Mines_Name")
#                     do_no = payload.get("DO_No")

#                     if do_no not in start_dates:
#                         start_dates[do_no] = month
#                     elif month < start_dates[do_no]:
#                         start_dates[do_no] = month
#                     if payload.get("GWEL_Net_Wt(MT)") and payload.get("GWEL_Net_Wt(MT)") != "NaN":
#                         aggregated_data[month][do_no]["actual_net_qty"] += float(payload["GWEL_Net_Wt(MT)"])
#                     if payload.get("Challan_Net_Wt(MT)") and payload.get("Challan_Net_Wt(MT)") != "NaN":
#                         aggregated_data[month][do_no]["net_qty"] += float(payload.get("Challan_Net_Wt(MT)"))
#                     if payload.get("Mines_Name"):
#                         aggregated_data[month][do_no]["mine_name"] = payload["Mines_Name"]

#                     aggregated_data[month][do_no]["count"] += 1 
#             dataList = [
#                 {
#                     "month": month,
#                     "data": {
#                         do_no: {
#                             "final_net_qty": data["actual_net_qty"] - data["net_qty"],
#                             "mine_name": data["mine_name"],
#                             "month": month,
#                         }
#                         for do_no, data in aggregated_data[month].items()
#                     },
#                 }
#                 for month in aggregated_data
#             ]
#             console_logger.debug(dataList)
#             total_monthly_final_net_qty = {}
#             yearly_final_data = {}
#             for data in dataList:
#                 month = data["month"]
#                 total_monthly_final_net_qty[month] = sum(
#                     entry["final_net_qty"] for entry in data["data"].values()
#                 )

#             total_monthly_final_net = dict(sorted(total_monthly_final_net_qty.items()))

#             for key, single_count in total_monthly_final_net.items():
#                 year = datetime.datetime.strptime(key, "%Y-%m").year
#                 if year in yearly_final_data:
#                     yearly_final_data[year] += single_count
#                 else:
#                     yearly_final_data[year] = single_count

#             yearly_final_data_sort = dict(sorted(yearly_final_data.items()))
#         console_logger.debug(total_monthly_final_net)
#         return total_monthly_final_net

#     except Exception as e:
#         console_logger.debug("----- Gate Vehicle Count Error -----", e)
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#         console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
#         console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
#         return e


# @router.get("/test_graph", tags=["Vipin Extra"])
def transit_loss_gain_road_mode_month(date_object):
    try:
        get_date = datetime.datetime.strptime(date_object, '%Y-%m-%d').date()
        specified_date = get_date.year
        dictData = {}
        timezone = pytz.timezone('Asia/Kolkata')
        basePipeline = [
            {
                "$match": {
                        "GWEL_Tare_Time": {
                            "$gte": None,
                        },
                },
            },
            {
                '$project': {
                    'ts': None,
                    'actual_net_qty': {
                        '$toDouble': '$actual_net_qty'
                    }, 
                    'net_qty': {
                        '$toDouble': '$net_qty'
                    }, 
                    'label': {
                        '$cond': {
                            'if': {
                                '$ne': [
                                    '$vehicle_number', None
                                ]
                            }, 
                            'then': 'Road', 
                            'else': 'Rail'
                        }
                    }, 
                    '_id': 0
                }
            }, {
                '$group': {
                    '_id': {
                        'ts': '$ts', 
                        'label': '$label'
                    }, 
                    'actual_net_qty_sum': {
                        '$sum': '$actual_net_qty'
                    }, 
                    'net_qty_sum': {
                        '$sum': '$net_qty'
                    }
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'ts': '$_id.ts', 
                    'label': '$_id.label', 
                    'data': {
                        '$subtract': [
                            '$actual_net_qty_sum', '$net_qty_sum'
                        ]
                    }
                }
            }
        ]

        date = specified_date
        end_date = f'{date}-12-31 23:59:59'
        start_date = f'{date}-01-01 00:00:00'
        format_data = "%Y-%m-%d %H:%M:%S"

        endd_date = timezone.localize(datetime.datetime.strptime(end_date, format_data))
        startd_date = timezone.localize(datetime.datetime.strptime(start_date, format_data))

        basePipeline[0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
        basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date

        # basePipeline[1]["$project"]["ts"] = {"$month": "$GWEL_Tare_Time"}
        basePipeline[1]["$project"]["ts"] = {"$month": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}

        labels = [(startd_date + relativedelta(months=i)).strftime("%b %y")
                    for i in range(12)]
        
        output = Gmrdata.objects().aggregate(basePipeline)
        outputDict = {}
        for data in output:
            ts = data["ts"]
            label = data["label"]
            sum_value = data["data"]
            if ts not in outputDict:
                outputDict[ts] = {label: sum_value}
            else:
                if label not in outputDict[ts]:
                    outputDict[ts][label] = sum_value
                else:
                    outputDict[ts][label] += sum_value
        # console_logger.debug(outputDict)
        for index, label in enumerate(labels):
            # console_logger.debug(index)
            # console_logger.debug(label)
            if index in outputDict:
                for key, val in outputDict[index].items():
                    # console_logger.debug(key)
                    if key == "Road":
                        # console_logger.debug(label)
                        # result["data"]["datasets"][0]["data"][index-1] = val
                        dictData[f"{specified_date}-{index:02d}"] = val
                        # console_logger.debug(index)
                        # console_logger.debug(val)
                    elif key == "Rail":
                        # result["data"]["datasets"][1]["data"][index-1] = val
                        console_logger.debug(index)
                        console_logger.debug(val)
        return dictData
    
    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e
    
def transit_loss_gain_road_mode():
    try:
        data = {}
        result = {
            "labels": [],
            "datasets": [],
            "weight_total": [],
            "total": 0,
            "page_size": 15,
        }
        
        financial_year = get_financial_year(datetime.date.today().strftime("%Y-%m-%d"))

        # logs = (
        #     Gmrdata.objects(created_at__gte=financial_year.get("start_date"), created_at__lte=financial_year.get("end_date"))
        # )
        logs = (
            Gmrdata.objects(GWEL_Tare_Time__gte=financial_year.get("start_date"), GWEL_Tare_Time__lte=financial_year.get("end_date"))
        )

        if any(logs):
            aggregated_data = defaultdict(
                lambda: defaultdict(
                    lambda: {
                        "net_qty": 0,
                        "mine_name": "",
                        "actual_net_qty": 0,
                        "count": 0,
                    }
                )
            )

            start_dates = {}
            for log in logs:
                if log.GWEL_Tare_Time is not None:
                    month = log.GWEL_Tare_Time.strftime("%Y-%m")
                    payload = log.payload()
                    result["labels"] = list(payload.keys())
                    mine_name = payload.get("Mines_Name")
                    do_no = payload.get("DO_No")

                    if do_no not in start_dates:
                        start_dates[do_no] = month
                    elif month < start_dates[do_no]:
                        start_dates[do_no] = month

                    if payload.get("GWEL_Net_Wt(MT)") and payload.get("GWEL_Net_Wt(MT)") != "NaN":
                        aggregated_data[month][do_no]["actual_net_qty"] += float(payload["GWEL_Net_Wt(MT)"])
                    else:
                        aggregated_data[month][do_no]["actual_net_qty"] = 0
                    if payload.get("Challan_Net_Wt(MT)") and payload.get("Challan_Net_Wt(MT)") != "NaN":
                        aggregated_data[month][do_no]["net_qty"] += float(payload.get("Challan_Net_Wt(MT)"))
                    else:
                        aggregated_data[month][do_no]["net_qty"] = 0
                    if payload.get("Mines_Name"):
                        aggregated_data[month][do_no]["mine_name"] = payload["Mines_Name"]
                    else:
                        aggregated_data[month][do_no]["mine_name"] = "-"
                    aggregated_data[month][do_no]["count"] += 1 

            dataList = [
                {
                    "month": month,
                    "data": {
                        do_no: {
                            "final_net_qty": data["actual_net_qty"] - data["net_qty"],
                            "mine_name": data["mine_name"],
                            "month": month,
                        }
                        for do_no, data in aggregated_data[month].items()
                    },
                }
                for month in aggregated_data
            ]

            total_monthly_final_net_qty = {}
            yearly_final_data = {}
            for data in dataList:
                month = data["month"]
                total_monthly_final_net_qty[month] = sum(
                    entry["final_net_qty"] for entry in data["data"].values()
                )

            total_monthly_final_net = dict(sorted(total_monthly_final_net_qty.items()))

            for key, single_count in total_monthly_final_net.items():
                year = datetime.datetime.strptime(key, "%Y-%m").year
                if year in yearly_final_data:
                    # yearly_final_data[year] += single_count
                    yearly_final_data["road_mode"] += single_count
                else:
                    # yearly_final_data[year] = single_count
                    yearly_final_data["road_mode"] = single_count

            yearly_final_data_sort = dict(sorted(yearly_final_data.items()))

        return yearly_final_data_sort

    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


def transit_loss_gain_rail_mode():
    try:
        data = {}
        result = {
            "labels": [],
            "datasets": [],
            "weight_total": [],
            "total": 0,
            "page_size": 15,
        }
        
        financial_year = get_financial_year(datetime.date.today().strftime("%Y-%m-%d"))

        logs = (
            RailData.objects(created_at__gte=financial_year.get("start_date"), created_at__lte=financial_year.get("end_date"))
        )


        if any(logs):
            aggregated_data = defaultdict(
                lambda: defaultdict(
                    lambda: {
                        "net_qty": 0,
                        "mine_name": "",
                        "actual_net_qty": 0,
                        "count": 0,
                    }
                )
            )

            start_dates = {}
            for log in logs:
                if log.created_at is not None:
                    month = log.created_at.strftime("%Y-%m")
                    payload = log.payload()
                    result["labels"] = list(payload.keys())
                    mine_name = payload.get("source")
                    rr_no = payload.get("rr_no")

                    if rr_no not in start_dates:
                        start_dates[rr_no] = month
                    elif month < start_dates[rr_no]:
                        start_dates[rr_no] = month

                    if payload.get("total_rly_net_wt") and payload.get("total_rly_net_wt") != "NaN":
                        aggregated_data[month][rr_no]["actual_net_qty"] += float(payload["total_rly_net_wt"])
                    else:
                        aggregated_data[month][rr_no]["actual_net_qty"] = 0
                    if payload.get("total_secl_net_wt") and payload.get("total_secl_net_wt") != "NaN":
                        aggregated_data[month][rr_no]["net_qty"] += float(payload.get("total_secl_net_wt"))
                    else:
                        aggregated_data[month][rr_no]["net_qty"] = 0
                    if payload.get("Mines_Name"):
                        aggregated_data[month][rr_no]["mine_name"] = payload["Mines_Name"]
                    else:
                        aggregated_data[month][rr_no]["mine_name"] = "-"
                    aggregated_data[month][rr_no]["count"] += 1 

            dataList = [
                {
                    "month": month,
                    "data": {
                        rr_no: {
                            "final_net_qty": data["actual_net_qty"] - data["net_qty"],
                            "mine_name": data["mine_name"],
                            "month": month,
                        }
                        for rr_no, data in aggregated_data[month].items()
                    },
                }
                for month in aggregated_data
            ]

            total_monthly_final_net_qty = {}
            yearly_final_data = {}
            for data in dataList:
                month = data["month"]
                total_monthly_final_net_qty[month] = sum(
                    entry["final_net_qty"] for entry in data["data"].values()
                )

            total_monthly_final_net = dict(sorted(total_monthly_final_net_qty.items()))

            for key, single_count in total_monthly_final_net.items():
                year = datetime.datetime.strptime(key, "%Y-%m").year
                if year in yearly_final_data:
                    # yearly_final_data[year] += single_count
                    yearly_final_data["rail_mode"] += single_count
                else:
                    # yearly_final_data[year] = single_count
                    yearly_final_data["rail_mode"] = single_count

            yearly_final_data_sort = dict(sorted(yearly_final_data.items()))

        return yearly_final_data_sort

    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


def gmr_main_graph():
    try:
        net_qty_all_totals = {}
        actual_net_qty_all_totals = {}
        fetchGmrDataMain = Gmrdata.objects()

        # Iterate over the retrieved data
        for single_gmr_data in fetchGmrDataMain:
            mine_name = single_gmr_data.mine
            net_qty = single_gmr_data.net_qty
            actual_net_qty = single_gmr_data.actual_net_qty
            
            # Update net_qty totals dictionary
            if mine_name in actual_net_qty_all_totals:
                net_qty_all_totals[mine_name] += float(net_qty)
            else:
                net_qty_all_totals[mine_name] = float(net_qty)
            if actual_net_qty:
                # Update actual_net_qty totals dictionary
                if mine_name in actual_net_qty_all_totals:
                    actual_net_qty_all_totals[mine_name] += float(actual_net_qty)
                else:
                    actual_net_qty_all_totals[mine_name] = float(actual_net_qty)

        # Perform clubbing - subtract actual_net_qty from net_qty for each mine
        clubbed_data_final = {}
        
        for mine in net_qty_all_totals:
            clubbed_data_final[mine] = actual_net_qty_all_totals.get(mine, 0) - net_qty_all_totals[mine]

        return clubbed_data_final
    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


def rail_pdf(specified_date):
    try:
        # result = {"labels": [], "datasets": [], "total": 0, "page_size": 15}
        # if type and type == "display":

        if specified_date:
            data = {}

            # specified_change_date = convert_to_utc_format(specified_date, "%Y-%m-%d")
            specified_change_date = datetime.datetime.strptime(specified_date, "%Y-%m-%d")

            start_of_month = specified_change_date.replace(day=1)

            start_date = datetime.datetime.strftime(start_of_month, '%Y-%m-%d')
            end_date = datetime.datetime.strftime(specified_change_date, '%Y-%m-%d')

            logs = (RailData.objects().order_by("source", "rr_no", "-created_at"))

            coal_testing_train = CoalTestingTrain.objects(receive_date__gte=start_date, receive_date__lte=end_date).order_by("-ID")
            if any(logs):
                aggregated_data = defaultdict(
                    lambda: defaultdict(
                        lambda: {
                            "DO_Qty": 0,
                            "challan_lr_qty": 0,
                            "mine_name": "",
                            "balance_qty": 0,
                            "percent_of_supply": 0,
                            "actual_net_qty": 0,
                            "Gross_Calorific_Value_(Adb)": 0,
                            "count": 0,
                            "coal_count": 0,
                        }
                    )
                )

                aggregated_coal_data = defaultdict(
                    lambda: defaultdict(
                        lambda: {
                            "Gross_Calorific_Value_(Adb)": 0,
                            "coal_count": 0,
                        }
                    )
                )

                for single_log in coal_testing_train:
                    coal_date = single_log.receive_date.strftime("%Y-%m")
                    coal_payload = single_log.gradepayload()
                    mine = coal_payload["Mine"]
                    rr_no = coal_payload["rrNo"]
                    if coal_payload.get("Gross_Calorific_Value_(Adb)"):
                        aggregated_coal_data[coal_date][rr_no]["Gross_Calorific_Value_(Adb)"] += float(coal_payload["Gross_Calorific_Value_(Adb)"])
                        aggregated_coal_data[coal_date][rr_no]["coal_count"] += 1

                start_dates = {}
                grade = 0
                for log in logs:
                    if log.created_at!=None:
                        month = log.created_at.strftime("%Y-%m")
                        date = log.created_at.strftime("%Y-%m-%d")
                        payload = log.payload()
                        # result["labels"] = list(payload.keys())
                        mine_name = payload.get("source")
                        rr_no = payload.get("rr_no")
                        # if payload.get("Grade") is not None:
                        #     if '-' in payload.get("Grade"):
                        #         grade = payload.get("Grade").split("-")[0]
                        #     else:
                        #         grade = payload.get("Grade")
                        # If start_date is None or the current vehicle_in_time is earlier than start_date, update start_date
                        if rr_no not in start_dates:
                            start_dates[rr_no] = date
                        elif date < start_dates[rr_no]:
                            start_dates[rr_no] = date
                        if payload.get("rr_qty"):
                            # aggregated_data[date][do_no]["DO_Qty"] += float(
                            #     payload["DO_Qty"]
                            # )
                            aggregated_data[date][rr_no]["rr_qty"] = float(
                                payload["rr_qty"]
                            )
                        else:
                            aggregated_data[date][rr_no]["rr_qty"] = 0
                        if payload.get("total_secl_net_wt"):
                            aggregated_data[date][rr_no]["challan_lr_qty"] += float(
                                payload.get("total_secl_net_wt")
                            )
                        else:
                            aggregated_data[date][rr_no]["challan_lr_qty"] = 0
                        if payload.get("source"):
                            aggregated_data[date][rr_no]["source"] = payload[
                                "source"
                            ]
                        else:
                            aggregated_data[date][rr_no]["source"] = "-"
                        aggregated_data[date][rr_no]["count"] += 1 

                dataList = [
                    {
                        "date": date,
                        "data": {
                            rr_no: {
                                "rr_qty": data["rr_qty"],
                                "challan_lr_qty": data["challan_lr_qty"],
                                "mine_name": data["source"],
                                "date": date,
                            }
                            for rr_no, data in aggregated_data[date].items()
                        },
                    }
                    for date in aggregated_data
                ]
                coalDataList = [
                    {"date": coal_date, "data": {
                        rr_no: {
                            "average_Gross_Calorific_Value_(Adb)": data["Gross_Calorific_Value_(Adb)"] / data["coal_count"],
                        } for rr_no, data in aggregated_coal_data[coal_date].items()
                    }} for coal_date in aggregated_coal_data
                ]

                coal_grades = CoalGrades.objects()  # Fetch all coal grades from the database

                # Iterate through each month's data
                for month_data in coalDataList:
                    for key, mine_data in month_data["data"].items():
                        if mine_data["average_Gross_Calorific_Value_(Adb)"] is not None:
                            for single_coal_grades in coal_grades:
                                if single_coal_grades["end_value"] != "":
                                    if (int(single_coal_grades["start_value"]) <= int(float(mine_data.get("average_Gross_Calorific_Value_(Adb)"))) <= int(single_coal_grades["end_value"]) and single_coal_grades["start_value"] != "" and single_coal_grades["end_value"] != ""):
                                        mine_data["average_GCV_Grade"] = single_coal_grades["grade"]
                                    elif int(mine_data["average_Gross_Calorific_Value_(Adb)"]) > 7001:
                                        mine_data["average_GCV_Grade"] = "G-1"
                                        break
                
                final_data = []
                if specified_date:
                    filtered_data = [
                        entry for entry in dataList if entry["date"] == specified_date
                    ]
                    if filtered_data:
                        data = filtered_data[0]["data"]
                        # dictData["month"] = filtered_data[0]["month"]
                        for data_dom, values in data.items():
                            dictData = {}
                            dictData["rr_no"] = data_dom
                            dictData["mine_name"] = values["mine_name"]
                            dictData["rr_qty"] = round(values["rr_qty"], 2)
                            dictData["challan_lr_qty"] = round(values["challan_lr_qty"], 2)
                            dictData["date"] = values["date"]
                            dictData["cumulative_challan_lr_qty"] = 0
                            dictData["balance_qty"] = 0
                            dictData["percent_supply"] = 0
                            dictData["asking_rate"] = 0
                            # dictData['average_GCV_Grade'] = values["grade"]
                            if data_dom in start_dates:
                                dictData["start_date"] = start_dates[data_dom]
                                # a total of 45 days data is needed, so date + 44 days
                                endDataVariable = datetime.datetime.strptime(start_dates[data_dom], "%Y-%m-%d") + timedelta(days=44)
                                # dictData["balance_days"] = dictData["end_date"] - datetime.date.today()
                                balance_days = endDataVariable.date() - datetime.date.today()
                                dictData["end_date"] = endDataVariable.strftime("%Y-%m-%d")
                                dictData["balance_days"] = balance_days.days
                            else:
                                dictData["start_date"] = None
                                dictData["end_date"] = None
                                dictData["balance_days"] = None

                            # Look for data_dom match in coalDataList and add average_GCV_Grade
                            for coal_data in coalDataList:
                                # if coal_data['date'] == specified_date and data_dom in coal_data['data']:
                                if data_dom in coal_data['data']:
                                    dictData['average_GCV_Grade'] = coal_data['data'][data_dom]['average_GCV_Grade']
                                    break
                            else:
                                dictData['average_GCV_Grade'] = "-"
                
                            # append data
                            final_data.append(dictData)
                    
                    if final_data:
                        # Find the index of the month data in dataList
                        index_of_month = next((index for index, item in enumerate(dataList) if item['date'] == specified_date), None)

                        # If the month is not found, exit or handle the case
                        if index_of_month is None:
                            print("Month data not found.")
                            exit()

                        # Iterate over final_data
                        for entry in final_data:
                            rr_no = entry["rr_no"]
                            cumulative_lr_qty = 0
                            
                            # Iterate over dataList from the first month to the current month
                            for i in range(index_of_month + 1):
                                month_data = dataList[i]
                                data = month_data["data"].get(rr_no)
                                
                                # If data is found for the rr_no in the current month, update cumulative_lr_qty
                                if data:
                                    cumulative_lr_qty += data['challan_lr_qty']
                            
                            # Update cumulative_challan_lr_qty in final_data
                            entry['cumulative_challan_lr_qty'] = round(cumulative_lr_qty, 2)
                            if data["rr_qty"] != 0 and entry["cumulative_challan_lr_qty"] != 0:
                                entry["percent_supply"] = round((entry["cumulative_challan_lr_qty"] / data["rr_qty"]) * 100, 2)
                            else:
                                entry["percent_supply"] = 0

                            if entry["cumulative_challan_lr_qty"] != 0 and data["rr_qty"] != 0:
                                entry["balance_qty"] = round((data["rr_qty"] - entry["cumulative_challan_lr_qty"]), 2)
                            else:
                                entry["balance_qty"] = 0
                            
                            if entry["balance_qty"] and entry["balance_qty"] != 0:
                                if entry["balance_days"]:
                                    entry["asking_rate"] = round(entry["balance_qty"] / entry["balance_days"], 2)
                    return final_data
                
    except Exception as e:
        console_logger.debug(e)


@router.get("/pdf_minewise_road", tags=["PDF Report"])
def generate_gmr_report(
    response: Response,
    specified_date: Optional[str]=None,
    mine: Optional[str] = "All",
):
    try:
        # if specified_date:
        data = {}
        result = {
            "labels": [],
            "datasets": [],
            "weight_total": [],
            "total": 0,
            "page_size": 15,
        }

        if mine and mine != "All":
            data["mine__icontains"] = mine.upper()

        if specified_date:
            # specified_change_date = datetime.datetime.strptime(specified_date, "%Y-%m-%d")
            specified_change_date = convert_to_utc_format(specified_date, "%Y-%m-%d")
            to_ts = convert_to_utc_format(f'{specified_date} 23:59:59', "%Y-%m-%d %H:%M:%S")

        logs = (
            Gmrdata.objects(GWEL_Tare_Time__lte=to_ts, actual_tare_qty__ne=None, gate_approved=True, GWEL_Tare_Time__ne=None)
            # Gmrdata.objects()
            .order_by("-GWEL_Tare_Time")
        )
       
        # coal_testing = CoalTesting.objects(receive_date__gte=start_date, receive_date__lte=end_date).order_by("-ID")
        challan_lr_qty_full = defaultdict(float)
        if any(logs):
            aggregated_data = defaultdict(
                lambda: defaultdict(
                    lambda: {
                        "DO_Qty": 0,
                        "challan_lr_qty": 0,
                        "challan_lr_qty_full": 0,
                        "mine_name": "",
                        "balance_qty": 0,
                        "percent_of_supply": 0,
                        "actual_net_qty": 0,
                        "Gross_Calorific_Value_(Adb)": 0,
                        "count": 0,
                        "coal_count": 0,
                    }
                )
            )


            start_dates = {}
            grade = 0
            for log in logs:
                if log.GWEL_Tare_Time!=None:
                    month = log.GWEL_Tare_Time.strftime("%Y-%m")
                    date = log.GWEL_Tare_Time.strftime("%Y-%m-%d")
                    payload = log.payload()
                    result["labels"] = list(payload.keys())
                    mine_name = payload.get("Mines_Name")
                    do_no = payload.get("DO_No")
                    if payload.get("Grade") is not None:
                        if '-' in payload.get("Grade"):
                            grade = payload.get("Grade").split("-")[0]
                        else:
                            grade = payload.get("Grade")
                    # If start_date is None or the current vehicle_in_time is earlier than start_date, update start_date
                    if do_no not in start_dates:
                        start_dates[do_no] = date
                    elif date < start_dates[do_no]:
                        start_dates[do_no] = date
                    if payload.get("DO_Qty"):
                        aggregated_data[date][do_no]["DO_Qty"] = float(
                            payload["DO_Qty"]
                        )
                    else:
                        aggregated_data[date][do_no]["DO_Qty"] = 0

                    challan_net_wt = payload.get("Challan_Net_Wt(MT)")    
                
                    if challan_net_wt:
                        aggregated_data[date][do_no]["challan_lr_qty"] += float(challan_net_wt)

                    # if payload.get("Challan_Net_Wt(MT)"):
                    #     aggregated_data[date][do_no]["challan_lr_qty"] += float(
                    #         payload.get("Challan_Net_Wt(MT)")
                    #     )
                    # else:
                    #     aggregated_data[date][do_no]["challan_lr_qty"] = 0
                    if payload.get("Mines_Name"):
                        aggregated_data[date][do_no]["mine_name"] = payload[
                            "Mines_Name"
                        ]
                    else:
                        aggregated_data[date][do_no]["mine_name"] = "-"
                    aggregated_data[date][do_no]["count"] += 1 
            dataList = [
                {
                    "date": date,
                    "data": {
                        do_no: {
                            "DO_Qty": data["DO_Qty"],
                            "challan_lr_qty": data["challan_lr_qty"],
                            "mine_name": data["mine_name"],
                            "grade": grade,
                            "date": date,
                        }
                        for do_no, data in aggregated_data[date].items()
                    },
                }
                for date in aggregated_data
            ]
            final_data = []
            for entry in dataList:
                date = entry["date"]
                for data_dom, values in entry['data'].items():
                    dictData = {}
                    dictData["DO_No"] = data_dom
                    dictData["mine_name"] = values["mine_name"]
                    dictData["DO_Qty"] = values["DO_Qty"]
                    dictData["club_challan_lr_qty"] = values["challan_lr_qty"]
                    dictData["date"] = values["date"]
                    dictData["cumulative_challan_lr_qty"] = 0
                    dictData["balance_qty"] = 0
                    dictData["percent_supply"] = 0
                    dictData["asking_rate"] = 0
                    dictData['average_GCV_Grade'] = values["grade"]
                    
                    
                    if data_dom in start_dates:
                        # console_logger.debug(start_dates)
                        dictData["start_date"] = start_dates[data_dom]
                        dictData["end_date"] = datetime.datetime.strptime(start_dates[data_dom], "%Y-%m-%d") + timedelta(days=44)
                        balance_days = dictData["end_date"].date() - datetime.datetime.today().date()
                        dictData["balance_days"] = balance_days.days
                    else:
                        dictData["start_date"] = None
                        dictData["end_date"] = None
                        dictData["balance_days"] = None
                    
                    final_data.append(dictData)

            if final_data:
                startdate = f'{specified_date} 00:00:00'
                enddate = f'{specified_date} 23:59:59'
                # to_ts = datetime.datetime.strptime(enddate,"%Y-%m-%d %H:%M:%S")
                from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")
                to_ts = convert_to_utc_format(enddate, "%Y-%m-%d %H:%M:%S")
                
                pipeline = [
                    {
                        "$match": {
                            "GWEL_Tare_Time": {"$gte": from_ts, "$lte": to_ts},
                                "net_qty": {"$ne": None}
                            }
                    },
                    {
                    '$group': {
                        '_id': {
                            'date': {
                                '$dateToString': {
                                    'format': '%Y-%m-%d', 
                                    'date': '$GWEL_Tare_Time'
                                }
                            }, 
                            'do_no': '$arv_cum_do_number'
                        }, 
                        'total_net_qty': {
                            '$sum': {
                                '$toDouble': '$net_qty'
                            }
                        }
                    }
                }]

                # filtered_data = [
                #     entry for entry in dataList if entry["date"] == specified_date
                # ]
                
                filtered_data_new = Gmrdata.objects.aggregate(pipeline)
                # dictDaata = {}
                aggregated_totals = defaultdict(float)
                for single_data_entry in filtered_data_new:
                    do_no = single_data_entry['_id']['do_no']
                    total_net_qty = single_data_entry['total_net_qty']
                    aggregated_totals[do_no] += total_net_qty
                    
                # console_logger.debug(aggregated_totals)
                # Create a dictionary to store the latest entries based on DO_No
                data_by_do = {}
                finaldataMain = [single_data_list for single_data_list in final_data if single_data_list.get("balance_days") >= 0]
                for entry in finaldataMain:
                    do_no = entry['DO_No']
                    
                    # clubbing all challan_lr_qty to get cumulative_challan_lr_qty
                    if do_no not in data_by_do:
                        data_by_do[do_no] = entry
                        data_by_do[do_no]['cumulative_challan_lr_qty'] = round(entry['club_challan_lr_qty'], 2)
                    else:
                        data_by_do[do_no]['cumulative_challan_lr_qty'] += round(entry['club_challan_lr_qty'], 2)
                    
                    # data = filtered_data[0]["data"]
                    # console_logger.debug(data)
                    # Update challan_lr_qty if the DO_No matches
                    if do_no in aggregated_totals:
                        data_by_do[do_no]['challan_lr_qty'] = round(aggregated_totals[do_no], 2)
                    else:
                        data_by_do[do_no]['challan_lr_qty'] = 0

                    # Update calculated fields
                    if data_by_do[do_no]['DO_Qty'] != 0 and data_by_do[do_no]['cumulative_challan_lr_qty'] != 0:
                        data_by_do[do_no]['percent_supply'] = round((data_by_do[do_no]['cumulative_challan_lr_qty'] / data_by_do[do_no]['DO_Qty']) * 100, 2)
                    else:
                        data_by_do[do_no]['percent_supply'] = 0

                    if data_by_do[do_no]['cumulative_challan_lr_qty'] != 0 and data_by_do[do_no]['DO_Qty'] != 0:
                        data_by_do[do_no]['balance_qty'] = round(data_by_do[do_no]['DO_Qty'] - data_by_do[do_no]['cumulative_challan_lr_qty'], 2)
                    else:
                        data_by_do[do_no]['balance_qty'] = 0
                    
                    if data_by_do[do_no]['balance_days'] and data_by_do[do_no]['balance_qty'] != 0:
                        data_by_do[do_no]['asking_rate'] = round(data_by_do[do_no]['balance_qty'] / data_by_do[do_no]['balance_days'], 2)

                # Convert the data back to a list
                final_data = list(data_by_do.values())
                
                rrNo_values, clubbed_data, aopList = bar_graph_data(specified_date)
                clubbed_data_final = gmr_main_graph()
                total_monthly_final_net_qty = transit_loss_gain_road_mode_month(specified_date)
                yearly_final_data = transit_loss_gain_road_mode()
                yearly_rail_final_data = transit_loss_gain_rail_mode()

                # counter data
                dayWiseVehicleInCount = daywise_in_vehicle_count_datewise(specified_date)
                dayWiseGrnReceive = daywise_grn_receive_datewise(specified_date)
                dayWiseGwelReceive = daywise_gwel_receive_pdf_datewise(specified_date)
                dayWiseOutVehicelCount = daywise_out_vehicle_count_datewise(specified_date)

                # railway data

                fetchRailData = rail_pdf(specified_date)
                
                # console_logger.debug(fetchRailData)

                if specified_date:
                    month_data = specified_date
                    fetchData = generate_report(final_data, rrNo_values, month_data, clubbed_data, clubbed_data_final, dayWiseVehicleInCount, dayWiseGrnReceive, dayWiseGwelReceive, dayWiseOutVehicelCount, total_monthly_final_net_qty, yearly_final_data, aopList, fetchRailData, yearly_rail_final_data)
                    return fetchData
                else:
                    fetchData = generate_report(final_data, rrNo_values, "", clubbed_data, clubbed_data_final, dayWiseVehicleInCount, dayWiseGrnReceive, dayWiseGwelReceive, dayWiseOutVehicelCount, total_monthly_final_net_qty, yearly_final_data, aopList, fetchRailData, yearly_rail_final_data)
                    return fetchData
            
        else:
            return 400
    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug(
            "Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno)
        )
        return e


@router.post("/add/scheduler", tags=["PDF Report"])
def endpoint_to_add_scheduler(response: Response, payload: MisReportData):
    try:
        dataName = payload.dict()
        try:
            reportScheduler = ReportScheduler.objects.get(report_name=dataName.get("report_name"))
            reportScheduler.recipient_list = dataName.get("recipient_list")
            reportScheduler.cc_list = dataName.get("cc_list")
            reportScheduler.bcc_list = dataName.get("bcc_list")
            reportScheduler.filter = dataName.get("filter")
            reportScheduler.schedule = dataName.get("schedule")
            reportScheduler.shift_schedule = dataName.get("shift_schedule")
            # time_fetch_data = datetime.datetime.strptime(dataName.get("time"), "%H:%M") + timedelta(hours=5, minutes=30)
            # reportScheduler.time = time_fetch_data.strftime("%H:%M")
            reportScheduler.time = dataName.get("time")
            reportScheduler.save()

        except DoesNotExist as e:
            # time_fetch_data = datetime.datetime.strptime(dataName.get("time"), "%H:%M") + timedelta(hours=5, minutes=30)
            reportScheduler = ReportScheduler(report_name=dataName.get("report_name"), recipient_list=dataName.get("recipient_list"), cc_list=dataName.get("cc_list"), bcc_list=dataName.get("bcc_list"), filter = dataName.get("filter"), schedule = dataName.get("schedule"), shift_schedule = dataName.get("shift_schedule"), time=dataName.get("time"))
            reportScheduler.save()

        # hh, mm = reportScheduler.time.split(":")

        # converting into ist start
        time_format = "%H:%M"
        given_time = datetime.datetime.strptime(reportScheduler.time, time_format)
        # Time to subtract: 5 hours and 30 minutes
        time_to_subtract = datetime.timedelta(hours=5, minutes=30)
        # Subtract the time
        new_time = given_time - time_to_subtract
        new_time_str = new_time.strftime(time_format)
        hh, mm = new_time_str.split(":")
        # converting into ist finish
        console_logger.debug("filter added : %s",reportScheduler.filter)
        if len(reportScheduler) > 0:
            if reportScheduler.filter == "daily":
                backgroundTaskHandler.run_job(task_name=reportScheduler.report_name, func=send_report_generate, trigger="cron", **{"day": "*", "hour": hh, "minute": mm}, func_kwargs={"report_name":payload.report_name}, max_instances=1)
                # backgroundTaskHandler.run_job(task_name=reportScheduler.report_name, func=send_report_generate, trigger="cron", **{"day": "*", "second": 2})
            elif reportScheduler.filter == "weekly":
                # backgroundTaskHandler.run_job(task_name=reportScheduler.report_name, func=send_report_generate, trigger="cron", **{"week": reportScheduler.schedule}) # week (int|str) - ISO week (1-53)
                backgroundTaskHandler.run_job(task_name=reportScheduler.report_name, func=send_report_generate, trigger="cron", **{"day_of_week": reportScheduler.schedule, "hour": hh, "minute": mm}, func_kwargs={"report_name":payload.report_name}, max_instances=1)
            elif reportScheduler.filter == "monthly":
                # backgroundTaskHandler.run_job(task_name=reportScheduler.report_name, func=send_report_generate, trigger="cron", **{"month": reportScheduler.schedule}) # month (int|str) - month (1-12)
                backgroundTaskHandler.run_job(task_name=reportScheduler.report_name, func=send_report_generate, trigger="cron", **{"day": reportScheduler.schedule, "hour": hh, "minute": mm}, func_kwargs={"report_name":payload.report_name}, max_instances=1)
            elif reportScheduler.filter == "shift_schedule":
                shift_schedule = reportScheduler.shift_schedule
                for single_shift in shift_schedule:
                    # console_logger.debug(single_shift)
                    shift_time = datetime.datetime.strptime(single_shift.get("time"), time_format)
                    shift_time_ist = shift_time - time_to_subtract
                    shift_hh, shift_mm = shift_time_ist.strftime(time_format).split(":")
                    console_logger.debug(reportScheduler.report_name)
                    backgroundTaskHandler.run_job(
                        task_name=f"{reportScheduler.report_name}_{single_shift.get('shift_wise')}", 
                        func=send_shift_report_generate, 
                        trigger="cron", **{"day": "*", "hour": shift_hh, "minute": shift_mm}, 
                        func_kwargs={"report_name":f"{reportScheduler.report_name}", "shift_name": single_shift.get('shift_wise'), "shift_time": single_shift.get("time")},
                        max_instances=1)


        try:
            fetchEmailNotifications = emailNotifications.objects.get(notification_name=dataName.get("report_name"))
            fetchEmailNotifications.delete()
        except DoesNotExist as e:
            console_logger.debug("No report name found in emailnotifications db")
        return {"detail": "success"}
    except Exception as e:
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        return {"detail": "failed"}


@router.get("/fetch/singlescheduler", tags=["PDF Report"])
def endpoint_to_fetch_scheduler_id(response: Response, name: str):
    try:
        fetchScheduler = ReportScheduler.objects.get(report_name=name)
        return fetchScheduler.payload()
    except Exception as e:
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        return {"detail": "failed"}


@router.delete("/delete/scheduler", tags=["PDF Report"])
def endpoint_to_delete_scheduler(response: Response, id: str):
    try:
        fetchScheduler = ReportScheduler.objects.get(id=id)
        fetchScheduler.delete()
        return {"detail": "success"}
    except Exception as e:
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        return {"detail": "failed"}


@router.post("/smtp", tags=["Mail"])
async def add_smtp_settings(response: Response):
    try:
        headers = {}
        data = {}
        # url_data = f"http://192.168.1.57/api/v1/base/smtp/unprotected"
        url_data = f"http://{host}/api/v1/base/smtp/unprotected"
        
        response = requests.request("GET", url=url_data, headers=headers, data=data, proxies=proxies)
        data = json.loads(response.text)
        if response.status_code == 200:
            smtp_settings = SmtpSettings(**data)
            smtp_settings.save()
        return {"detail": "success"}
    except Exception as e:
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        return {"detail": "failed"} 


@router.get("/smtp", tags=["Mail"])
async def endpoint_to_get_smtp_settings(response: Response):
    try:
        fetchSmtpSettings = SmtpSettings.objects.get()
        return fetchSmtpSettings.payload()
    except Exception as e:
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        return {"detail": "failed"}
    

@router.post("/insert/aoptarget", tags=["PDF Report"])
def endpoint_to_insert_aoptarget(response: Response, payload: AopTargetData):
    try:
        dataName = payload.dict()
        try:
            aopTargetData = AopTarget.objects.get(source_name=dataName.get("source_name"))
            aopTargetData.aop_target = dataName.get("aop_target")
            aopTargetData.save()

        except DoesNotExist as e:
            aopTargetData = AopTarget(source_name=dataName.get("source_name"), aop_target=dataName.get("aop_target"))
            aopTargetData.save()
        return {"detail": "success"}
    except Exception as e:
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        return {"detail": "failed"}


@router.get("/fetch/coallocation", tags=["PDF Report"])
def endpoint_to_fetch_coal_location(response: Response):
    try:
        fetchCoalTestingLocation = CoalTesting.objects()
        fetchCoalTestingTrainLocation = CoalTestingTrain.objects()

        coalTestingData = [singlecoalTesting["location"] for singlecoalTesting in fetchCoalTestingLocation]
        coalTestingTrainData = [singlecoalTestingTrain["location"] for singlecoalTestingTrain in fetchCoalTestingTrainLocation]

        return list(set(coalTestingData)) + list(set(coalTestingTrainData))

    except Exception as e:
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        return {"detail": "failed"}


@router.get("/fetch/aoptarget", tags=["PDF Report"])
def endpoint_to_fetch_aoptarget(response: Response, target_name: str=None):
    try:
        dataList = []
        data = {"source_name": "", "aop_target": ""}
        if target_name:
            try:
                fetchAopTargetData = AopTarget.objects.get(source_name=target_name)
                # dataList.append(fetchAopTargetData.payload())
                # if fetchAopTargetData:
                dataList.append(fetchAopTargetData.reportpayload())
            except DoesNotExist as e:
                data["source_name"] = target_name
                dataList.append(data)
        else:
            fetchAopTargetData = AopTarget.objects()
            for singleAopTarget in fetchAopTargetData:
                dataList.append(singleAopTarget.reportpayload())
        return dataList
    except Exception as e:
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        return {"detail": "failed"}

@router.get("/fetch/minecoaljourneysource", tags=["PDF Report"])
def endpoint_to_fetch_aoptarget(response: Response, mine_name: str=None):
    try:
        mine_data = short_mine_collection.find_one({"mine_name":mine_name.upper()})
        return {
            "mine_name": mine_data.get("mine_name"),
            "short_code": mine_data.get("short_code"),
            "coal_journey": mine_data.get("coal_journey"),
        }

    except Exception as e:
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        return {"detail": "failed"}


@router.get("/fetch/similarminelocation", tags=["PDF Report"])
def endpoint_to_fetch_aoptarget(response: Response, mine_name: str):
    try:    
        fetchCoalTestingLocation = CoalTesting.objects.filter(location__contains = mine_name)
        fetchCoalTestingTrainLocation = CoalTestingTrain.objects.filter(location__contains = mine_name)

        coalTestingData = [singlecoalTesting["location"] for singlecoalTesting in fetchCoalTestingLocation]
        coalTestingTrainData = [singlecoalTestingTrain["location"] for singlecoalTestingTrain in fetchCoalTestingTrainLocation]

        return list(set(coalTestingData)) + list(set(coalTestingTrainData))
    except Exception as e:
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        return {"detail": "failed"}
    

@router.delete("/delete/aoptarget", tags=["PDF Report"])
def endpoint_to_delete_aoptarget(response: Response, id: str):
    try:
        fetchAopTarget = AopTarget.objects.get(id=id)
        fetchAopTarget.delete()
        return {"detail": "success"}
    except Exception as e:
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        return {"detail": "failed"}


@router.get("/fetch/scheduler", tags=["PDF Report"])
def endpoint_to_fetch_report_scheduler(response: Response):
    try:
        dataList = []
        fetchReportScheduler = ReportScheduler.objects()
        for single_report in fetchReportScheduler:
            dataList.append(single_report.payload())
        return dataList
    except Exception as e:
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        return {"detail": "failed"}


def fetch_email_data():
    try:
        headers = {}
        data = {}
        url_data = f"http://{ip}/api/v1/base/smtp/unprotected"
        response = requests.request("GET", url=url_data, headers=headers, data=data, proxies=proxies)
        data = json.loads(response.text)
        return response.status_code, data
    except Exception as e:
        console_logger.debug(e)

def check_existing_notification(notification_name):
    current_time = datetime.datetime.now()
    start_of_day = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = start_of_day + datetime.timedelta(days=1)
    console_logger.debug(start_of_day)
    console_logger.debug(end_of_day)
    console_logger.debug(emailNotifications.objects(
        Q(notification_name=notification_name) & 
        Q(created_at__gte=start_of_day) & 
        Q(created_at__lt=end_of_day)
    ).count() > 0)
    return emailNotifications.objects(
        Q(notification_name=notification_name) & 
        Q(created_at__gte=start_of_day) & 
        Q(created_at__lt=end_of_day)
    ).count() > 0 # getting either true or false

def send_shift_report_generate(**kwargs):
    # try:
    console_logger.debug(("scheduler report generate",kwargs))
    reportSchedule = ReportScheduler.objects.get(report_name=kwargs.get("report_name"))
    if reportSchedule.active == False:
        console_logger.debug("scheduler is off")
        return
    elif reportSchedule.active == True:
        if not check_existing_notification(f"coal_bunkering_schedule_{kwargs.get('shift_name')}"):
            emailNotifications(notification_name=f"coal_bunkering_schedule_{kwargs.get('shift_name')}").save()
            console_logger.debug("inside Coal Bunkering Schedule")
            fetchShiftScheduler = shiftScheduler.objects.get(report_name=kwargs.get("report_name"), shift_name=kwargs.get('shift_name'))
            if fetchShiftScheduler:
                # console_logger.debug(fetchShiftScheduler.start_shift_time)
                # console_logger.debug(fetchShiftScheduler.end_shift_time)

                fetchBunkerAnalysis = bunkerAnalysis.objects.filter(Q(created_at__gte=fetchShiftScheduler.start_shift_time) & Q(created_at__lte=fetchShiftScheduler.end_shift_time))
                html_per = ""
                if fetchBunkerAnalysis:
                    html_per += "<table border='1'><tr><th>ID</th><th>Units</th><th>Bunkering</th><th>Shift Name</th><th>MGCV</th><th>HGCV</th><th>Ratio</th><th>Date</th></tr>"
                    for single_bunker in fetchBunkerAnalysis:
                        # console_logger.debug(single_bunker.units)
                        html_per += "<tr>"
                        html_per +=f"<td>{single_bunker.ID}</td>"
                        if single_bunker.shift_name:
                            html_per += f"<td>{single_bunker.shift_name}</td>"
                        else:
                            html_per += "<td>-</td>"
                        html_per +=f"<td>{single_bunker.units}</td>"
                        html_per +=f"<td>{single_bunker.bunkering}</td>"
                        if single_bunker.mgcv:
                            html_per += f"<td>{single_bunker.mgcv}</td>"
                        else:
                            html_per += "<td>-</td>"
                        if single_bunker.hgcv:
                            html_per += f"<td>{single_bunker.hgcv}</td>"
                        else:
                            html_per += "<td>-</td>"
                        if single_bunker.ratio:
                            html_per += f"<td>{single_bunker.ratio}</td>"
                        else:
                            html_per += "<td>-</td>"
                        html_per += f"<td>{single_bunker.created_date.strftime('%d %b %Y')}</td>"
                        html_per += "</tr>"
                    html_per += "</table>"
                else:
                    html_per += "<b>No data found</b>"
                response_code, fetch_email = fetch_email_data()
                if response_code == 200:
                    subject = f"Bunker Analysis Report for Date: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                    body = f"""
                        <b>Bunker Analysis Report for Date: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}</b>
                        <br>
                        <br>
                        <!doctype html>
                        <html>
                        <head>
                            <meta charset="utf-8">
                            <title>Bunker Analysis Report</title>
                        </head>
                        <body>
                            {html_per}
                        </body>
                        </html>"""
                    checkEmailDevelopment = EmailDevelopmentCheck.objects()
                    if checkEmailDevelopment[0].development == "local":
                        console_logger.debug("inside 192")
                        send_email(fetch_email.get("Smtp_user"), subject, fetch_email.get("Smtp_password"), fetch_email.get("Smtp_host"), fetch_email.get("Smtp_port"), reportSchedule.recipient_list, body, "", reportSchedule.cc_list, reportSchedule.bcc_list)
                    elif checkEmailDevelopment[0].development == "prod":
                        console_logger.debug("outside 192")
                        send_data = {
                            "sender_email": fetch_email.get("Smtp_user"),
                            "subject": subject,
                            "password": fetch_email.get("Smtp_password"),
                            "smtp_host": fetch_email.get("Smtp_host"),
                            "smtp_port": fetch_email.get("Smtp_port"),
                            "receiver_email": reportSchedule.recipient_list,
                            "body": body,
                            "file_path": "",
                            "cc_list": reportSchedule.cc_list,
                            "bcc_list": reportSchedule.bcc_list
                        }
                        console_logger.debug(send_data)
                        generate_email(Response, email=send_data)

        else:
            return
    # except Exception as e:
    #     console_logger.debug(e)

def send_report_generate(**kwargs):
    try:
        console_logger.debug(("scheduler report generate",kwargs))
        reportSchedule = ReportScheduler.objects()
        # for singleReportschedule in reportSchedule:
        # if singleReportschedule["report_name"] == "daily_coal_logistic_report":
        
        if kwargs["report_name"] == "daily_coal_logistic_report":
            if reportSchedule[0].active == False:
                console_logger.debug("scheduler is off")
                return
            elif reportSchedule[0].active == True:
                if not check_existing_notification("daily_coal_logistic_report"):
                    emailNotifications(notification_name="daily_coal_logistic_report").save()
                    console_logger.debug("inside logistic report")
                    # date_data = datetime.datetime.today().strftime('%Y-%m-%d')
                    # start_date = "2024-07-29"
                    generateReportData = generate_gmr_report(Response, datetime.date.today().strftime("%Y-%m-%d"), "All")
                    # generateReportData = generate_gmr_report(Response, "2024-08-01", "All")
                    console_logger.debug(f"{os.path.join(os.getcwd())}/{generateReportData}")
                    response_code, fetch_email = fetch_email_data()
                    if response_code == 200:
                        console_logger.debug(reportSchedule[0].recipient_list)
                        subject = f"GMR Daily Coal Logistic Report {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                        body = f"Daily Coal Logistic Report for Date: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                        # send_email(smtpData.Smtp_user, subject, smtpData.Smtp_password, smtpData.Smtp_host, smtpData.Smtp_port, receiver_email, body, f"{os.path.join(os.getcwd())}{generateReportData}")
                        checkEmailDevelopment = EmailDevelopmentCheck.objects()
                        if checkEmailDevelopment[0].development == "local":
                            console_logger.debug("inside local")
                            send_email(fetch_email.get("Smtp_user"), subject, fetch_email.get("Smtp_password"), fetch_email.get("Smtp_host"), fetch_email.get("Smtp_port"), reportSchedule[0].recipient_list, body, f"{os.path.join(os.getcwd())}/{generateReportData}", reportSchedule[0].cc_list, reportSchedule[0].bcc_list)
                        elif checkEmailDevelopment[0].development == "prod":
                            console_logger.debug("inside prod")
                            send_data = {
                                "sender_email": fetch_email.get("Smtp_user"),
                                "subject": subject,
                                "password": fetch_email.get("Smtp_password"),
                                "smtp_host": fetch_email.get("Smtp_host"),
                                "smtp_port": fetch_email.get("Smtp_port"),
                                "receiver_email": reportSchedule[0].recipient_list,
                                "body": body,
                                "file_path": f"{os.path.join(os.getcwd())}/{generateReportData}",
                                "cc_list": reportSchedule[0].cc_list,
                                "bcc_list": reportSchedule[0].bcc_list
                            }
                            # console_logger.debug(send_data)
                            generate_email(Response, email=send_data)
                else:
                    return
        elif kwargs["report_name"] == "expiring_fitness_certificate":
            if reportSchedule[1].active == False:
                console_logger.debug("scheduler is off")
                return
            elif reportSchedule[1].active == True:
                if not check_existing_notification("expiring_fitness_certificate"):
                    emailNotifications(notification_name="expiring_fitness_certificate").save()
                    console_logger.debug("inside certificate expiry")
                    generateExpiryData = endpoint_to_fetch_going_to_expiry_vehicle(Response)
                    tabledata = ""
                    for single_data in generateExpiryData["datasets"]:
                        tabledata +="<tr>"
                        tabledata +=f"<td>{single_data['vehicle_number']}</td>"
                        tabledata +=f"<td>{single_data['vehicle_chassis_number']}</td>"
                        tabledata +=f"<td>{single_data['expiry_date']}</td>"
                        tabledata +=f"<td>{single_data['days_to_go']}</td>"
                        tabledata +="</tr>"
                    response_code, fetch_email = fetch_email_data()
                    if response_code == 200:
                        console_logger.debug(reportSchedule[1].recipient_list)
                        # for receiver_email in reportSchedule[1].recipient_list:
                        subject = f"Expiring Fitness Certificate for Date: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                        body = f"""
                        <b>Expiring Fitness Certificate for Date: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}</b>
                        <br>
                        <br>
                        <!doctype html>
                        <html>
                        <head>
                            <meta charset="utf-8">
                            <title>Expiry Certificate</title>
                        </head>
                        <body>
                            <table border='1'>
                                <tr>
                                    <th>Vehicle Number</th>
                                    <th>Vehicle Chassis Number</th>
                                    <th>Certificate Expiry</th>
                                    <th>Days To Go</th>
                                </tr>
                                {tabledata}
                            </table>
                        </body>
                        </html>"""
                        checkEmailDevelopment = EmailDevelopmentCheck.objects()
                        if checkEmailDevelopment[0].development == "local":
                            console_logger.debug("inside 192")
                            send_email(fetch_email.get("Smtp_user"), subject, fetch_email.get("Smtp_password"), fetch_email.get("Smtp_host"), fetch_email.get("Smtp_port"), reportSchedule[1].recipient_list, body, "", reportSchedule[1].cc_list, reportSchedule[1].bcc_list)
                        elif checkEmailDevelopment[0].development == "prod":
                            console_logger.debug("outside 192")
                            send_data = {
                                "sender_email": fetch_email.get("Smtp_user"),
                                "subject": subject,
                                "password": fetch_email.get("Smtp_password"),
                                "smtp_host": fetch_email.get("Smtp_host"),
                                "smtp_port": fetch_email.get("Smtp_port"),
                                "receiver_email": reportSchedule[1].recipient_list,
                                "body": body,
                                "file_path": "",
                                "cc_list": reportSchedule[1].cc_list,
                                "bcc_list": reportSchedule[1].bcc_list
                            }
                            # console_logger.debug(send_data)
                            generate_email(Response, email=send_data)
                else:
                    return

        elif kwargs["report_name"] == "gwel_coal_report":
            if reportSchedule[2].active == False:
                console_logger.debug("scheduler is off")
                return
            elif reportSchedule[2].active == True:
                if not check_existing_notification("gwel_coal_report"):
                    emailNotifications(notification_name="gwel_coal_report").save()
                    console_logger.debug("inside gwel coal report")
                    start_date = datetime.datetime.today().strftime('%Y-%m-%d')
                    end_date = datetime.datetime.today().strftime('%Y-%m-%d')
                    # start_date = "2024-07-29"
                    # end_date = "2024-07-29"
                    filter_type = "gwel"
                    generateGwelReportData = fetch_excel_data_rail(Response, f"{start_date}T00:00", f"{end_date}T23:59", filter_type)
                    # console_logger.debug(f"{os.path.join(os.getcwd())}/{generateGwelReportData}")
                    response_code, fetch_email = fetch_email_data()
                    if response_code == 200:
                        if generateGwelReportData.get("road") is None and generateGwelReportData.get("rail") is None:
                            console_logger.debug(reportSchedule[2].recipient_list)
                            subject = f"GWEL Received Coal Analysis {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                            body = f"""<b>No data found for GWEL Received Coal Analysis for Date: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}</b>"""
                            checkEmailDevelopment = EmailDevelopmentCheck.objects()
                            if checkEmailDevelopment[0].development == "local":
                                send_email(fetch_email.get("Smtp_user"), subject, fetch_email.get("Smtp_password"), fetch_email.get("Smtp_host"), fetch_email.get("Smtp_port"), reportSchedule[2].recipient_list, body, "", reportSchedule[2].cc_list, reportSchedule[2].bcc_list)
                            elif checkEmailDevelopment[0].development == "prod":
                                send_data = {
                                    "sender_email": fetch_email.get("Smtp_user"),
                                    "subject": subject,
                                    "password": fetch_email.get("Smtp_password"),
                                    "smtp_host": fetch_email.get("Smtp_host"),
                                    "smtp_port": fetch_email.get("Smtp_port"),
                                    "receiver_email": reportSchedule[2].recipient_list,
                                    "body": body,
                                    "file_path": "",
                                    "cc_list": reportSchedule[2].cc_list,
                                    "bcc_list": reportSchedule[2].bcc_list
                                }
                                # console_logger.debug(send_data)
                                generate_email(Response, email=send_data)
                        else:
                            console_logger.debug(reportSchedule[2].recipient_list)
                            subject = f"GWEL Received Coal Analysis {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                            body = f"GWEL Received Coal Analysis for Date: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                            # send_email(smtpData.Smtp_user, subject, smtpData.Smtp_password, smtpData.Smtp_host, smtpData.Smtp_port, receiver_email, body, f"{os.path.join(os.getcwd())}{generateReportData}")
                            checkEmailDevelopment = EmailDevelopmentCheck.objects()
                            if checkEmailDevelopment[0].development == "local":
                                send_email(fetch_email.get("Smtp_user"), subject, fetch_email.get("Smtp_password"), fetch_email.get("Smtp_host"), fetch_email.get("Smtp_port"), reportSchedule[2].recipient_list, body, generateGwelReportData, reportSchedule[2].cc_list, reportSchedule[2].bcc_list)
                            elif checkEmailDevelopment[0].development == "prod":
                                send_data = {
                                    "sender_email": fetch_email.get("Smtp_user"),
                                    "subject": subject,
                                    "password": fetch_email.get("Smtp_password"),
                                    "smtp_host": fetch_email.get("Smtp_host"),
                                    "smtp_port": fetch_email.get("Smtp_port"),
                                    "receiver_email": reportSchedule[2].recipient_list,
                                    "body": body,
                                    "file_path": f"{generateGwelReportData}",
                                    "cc_list": reportSchedule[2].cc_list,
                                    "bcc_list": reportSchedule[2].bcc_list
                                }
                                # console_logger.debug(send_data)
                                generate_email(Response, email=send_data)
                else:
                    return
        elif kwargs["report_name"] == "thirdparty_coal_report":
            if reportSchedule[3].active == False:
                console_logger.debug("scheduler is off")
                return
            elif reportSchedule[3].active == True:
                if not check_existing_notification("thirdparty_coal_report"):
                    emailNotifications(notification_name="thirdparty_coal_report").save()
                    console_logger.debug("inside Thirdparty Coal Report")
                    start_date = datetime.datetime.today().strftime('%Y-%m-%d')
                    end_date = datetime.datetime.today().strftime('%Y-%m-%d')
                    # start_date = "2024-07-29"
                    # end_date = "2024-07-29"
                    filter_type = "third_party"
                    generateThirdPartyReportData = fetch_excel_data_rail(Response, f"{start_date}T00:00", f"{end_date}T23:59", filter_type)
                    # console_logger.debug(f"{os.path.join(os.getcwd())}/{generateThirdPartyReportData}")
                    response_code, fetch_email = fetch_email_data()
                    if response_code == 200:
                        if generateThirdPartyReportData.get("road") is None and generateThirdPartyReportData.get("rail") is None:
                            console_logger.debug(reportSchedule[3].recipient_list)
                            subject = f"Third-Party Coal Analysis(Mahabal) {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                            body = f"""<b>No data found for Third-Party Coal Analysis(Mahabal) for Date: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}</b>"""
                            checkEmailDevelopment = EmailDevelopmentCheck.objects()
                            if checkEmailDevelopment[0].development == "local":
                                send_email(fetch_email.get("Smtp_user"), subject, fetch_email.get("Smtp_password"), fetch_email.get("Smtp_host"), fetch_email.get("Smtp_port"), reportSchedule[3].recipient_list, body, "", reportSchedule[3].cc_list, reportSchedule[3].bcc_list)
                            elif checkEmailDevelopment[0].development == "prod":
                                send_data = {
                                    "sender_email": fetch_email.get("Smtp_user"),
                                    "subject": subject,
                                    "password": fetch_email.get("Smtp_password"),
                                    "smtp_host": fetch_email.get("Smtp_host"),
                                    "smtp_port": fetch_email.get("Smtp_port"),
                                    "receiver_email": reportSchedule[3].recipient_list,
                                    "body": body,
                                    "file_path": "",
                                    "cc_list": reportSchedule[3].cc_list,
                                    "bcc_list": reportSchedule[3].bcc_list
                                }
                                # console_logger.debug(send_data)
                                generate_email(Response, email=send_data)
                        else:
                            console_logger.debug(reportSchedule[3].recipient_list)
                            subject = f"Third-Party Coal Analysis(Mahabal) {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                            body = f"Third-Party Coal Analysis(Mahabal) Report for Date: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                            # send_email(smtpData.Smtp_user, subject, smtpData.Smtp_password, smtpData.Smtp_host, smtpData.Smtp_port, receiver_email, body, f"{os.path.join(os.getcwd())}{generateReportData}")
                            checkEmailDevelopment = EmailDevelopmentCheck.objects()
                            if checkEmailDevelopment[0].development == "local":
                                send_email(fetch_email.get("Smtp_user"), subject, fetch_email.get("Smtp_password"), fetch_email.get("Smtp_host"), fetch_email.get("Smtp_port"), reportSchedule[3].recipient_list, body, generateThirdPartyReportData, reportSchedule[3].cc_list, reportSchedule[3].bcc_list)
                            elif checkEmailDevelopment[0].development == "prod":
                                send_data = {
                                    "sender_email": fetch_email.get("Smtp_user"),
                                    "subject": subject,
                                    "password": fetch_email.get("Smtp_password"),
                                    "smtp_host": fetch_email.get("Smtp_host"),
                                    "smtp_port": fetch_email.get("Smtp_port"),
                                    "receiver_email": reportSchedule[3].recipient_list,
                                    "body": body,
                                    "file_path": f"{generateThirdPartyReportData}",
                                    "cc_list": reportSchedule[3].cc_list,
                                    "bcc_list": reportSchedule[3].bcc_list
                                }
                                # console_logger.debug(send_data)
                                generate_email(Response, email=send_data)
                else:
                    return
        elif kwargs["report_name"] == "coal_logistics_table":
            if reportSchedule[4].active == False:
                console_logger.debug("scheduler is off")
                return
            elif reportSchedule[4].active == True:
                if not check_existing_notification("coal_logistics_table"):
                    emailNotifications(notification_name="coal_logistics_table").save()
                    console_logger.debug("inside Coal Logistics Table")
                    specified_date = datetime.datetime.today().strftime('%Y-%m-%d')
                    # specified_date = "2024-07-02"
                    roadData = fetch_data_road_logistics(Response, specified_date)
                    railData = fetch_data_rail_logistics(Response, specified_date)

                    # console_logger.debug(roadData)
                    # console_logger.debug(railData)
                    response_code, fetch_email = fetch_email_data()
                    if response_code == 200:
                        htmlData = ""
                        if roadData != 404 and roadData is not None:
                            htmlData += f"<b>Daily Road Coal Logistic Report for {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}</b><br><br>"
                            htmlData += roadData
                            htmlData += "<br><br>"
                        else:
                            htmlData += f"<b>No data found for Daily Road Coal Logistic Report for {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}</b><br><br>"
                        if railData != 404 and railData is not None:
                            htmlData += f"<b>Daily Rail Coal Logistic Report for {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}</b><br><br>"
                            htmlData += railData
                            htmlData += "<br><br>"
                        else:
                            htmlData += f"<b>No data found for Daily Rail Coal Logistic Report for {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}</b><br><br>"

                        console_logger.debug(reportSchedule[4].recipient_list)
                        subject = f"Daily Coal Receipt by Road & Rail for Date: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                        body = f"""
                        <h3>Daily Coal Receipt by Road & Rail for Date: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}</h3>
                        <br>
                        <br>
                        <br>
                        <!doctype html>
                        <html>
                        <head>
                            <meta charset="utf-8">
                            <title>Daily Coal Receipt by Road & Rail</title>
                        </head>
                        <body>
                            {htmlData}
                        </body>
                        </html>"""
                        checkEmailDevelopment = EmailDevelopmentCheck.objects()
                        if checkEmailDevelopment[0].development == "local":
                            send_email(fetch_email.get("Smtp_user"), subject, fetch_email.get("Smtp_password"), fetch_email.get("Smtp_host"), fetch_email.get("Smtp_port"), reportSchedule[4].recipient_list, body, "", reportSchedule[4].cc_list, reportSchedule[4].bcc_list)
                        elif checkEmailDevelopment[0].development == "prod":
                            send_data = {
                                "sender_email": fetch_email.get("Smtp_user"),
                                "subject": subject,
                                "password": fetch_email.get("Smtp_password"),
                                "smtp_host": fetch_email.get("Smtp_host"),
                                "smtp_port": fetch_email.get("Smtp_port"),
                                "receiver_email": reportSchedule[4].recipient_list,
                                "body": body,
                                "file_path": "",
                                "cc_list": reportSchedule[4].cc_list,
                                "bcc_list": reportSchedule[4].bcc_list
                            }
                            # console_logger.debug(send_data)
                            generate_email(Response, email=send_data)
                else:
                    return
        elif kwargs["report_name"] == "coal_bunkering_table":
            if reportSchedule[5].active == False:
                console_logger.debug("scheduler is off")
                return
            elif reportSchedule[5].active == True:
                if not check_existing_notification("coal_bunkering_table"):
                    emailNotifications(notification_name="coal_bunkering_table").save()
                    console_logger.debug("inside Coal Bunkering Table")
                    specified_date = datetime.datetime.today().strftime('%Y-%m-%d')
                    # specified_date = "2024-07-02"
                    bunkerData = coal_bunker_analysis(Response, specified_date)

                    # console_logger.debug(bunkerData)
                    response_code, fetch_email = fetch_email_data()
                    if response_code == 200:
                        htmlData = ""
                        if bunkerData != 404 and bunkerData is not None:
                            htmlData += f"<b>Daily Coal Bunkering Report for {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}</b><br><br>"
                            htmlData += bunkerData
                            htmlData += "<br><br>"
                        else:
                            htmlData += f"<b>No data found for Daily Coal Bunkering Report for {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}</b><br><br>"
                        
                        console_logger.debug(reportSchedule[5].recipient_list)
                        subject = f"Daily Coal Bunkering Report for Date: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                        body = f"""
                        <h3>Daily Coal Bunkering Report for Date: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}</h3>
                        <br>
                        <br>
                        <br>
                        <!doctype html>
                        <html>
                        <head>
                            <meta charset="utf-8">
                            <title>Daily Bunkering Report</title>
                        </head>
                        <body>
                            {htmlData}
                        </body>
                        </html>"""
                        checkEmailDevelopment = EmailDevelopmentCheck.objects()
                        if checkEmailDevelopment[0].development == "local":
                            send_email(fetch_email.get("Smtp_user"), subject, fetch_email.get("Smtp_password"), fetch_email.get("Smtp_host"), fetch_email.get("Smtp_port"), reportSchedule[5].recipient_list, body, "", reportSchedule[5].cc_list, reportSchedule[5].bcc_list)
                        elif checkEmailDevelopment[0].development == "prod":
                            send_data = {
                                "sender_email": fetch_email.get("Smtp_user"),
                                "subject": subject,
                                "password": fetch_email.get("Smtp_password"),
                                "smtp_host": fetch_email.get("Smtp_host"),
                                "smtp_port": fetch_email.get("Smtp_port"),
                                "receiver_email": reportSchedule[5].recipient_list,
                                "body": body,
                                "file_path": "",
                                "cc_list": reportSchedule[5].cc_list,
                                "bcc_list": reportSchedule[5].bcc_list
                            }
                            # console_logger.debug(send_data)
                            generate_email(Response, email=send_data)
                else:
                    return
        elif kwargs["report_name"] == "road_coal_journey_report":
            if reportSchedule[8].active == False:
                console_logger.debug("scheduler is off")
                return
            elif reportSchedule[8].active == True:
                if not check_existing_notification("road_coal_journey_report"):
                    emailNotifications(notification_name="road_coal_journey_report").save()
                    console_logger.debug("inside road coal journey report")
                    date_data = datetime.datetime.today().strftime('%Y-%m-%d')
                    # start_date = "2024-07-29"
                    headers = {
                        'accept': 'application/json',
                    }

                    params = {
                        'filter_data': ["Sr.No.", "Mines_Name", "Type_of_consumer", "PO_No", "Line_Item", "Delivery_Challan_No", "DO_No", "LOT", "vehicle_number", "Challan_Gross_Wt(MT)", "Challan_Tare_Wt(MT)", "Challan_Net_Wt(MT)", "GWEL_Gross_Wt(MT)", "GWEL_Tare_Wt(MT)", "GWEL_Net_Wt(MT)", "GWEL_Gross_Time", "GWEL_Tare_Time", "Transit_Loss", "Transporter_LR_No", "Transporter_LR_Date", "Total_net_amount", "Vehicle_in_time", "Vehicle_out_time", "TAT_difference", "PO_Date", "DO_Qty", "Weightment_Date", "Weightment_Time", "Vehicle_Chassis_No", "Fitness_Expiry", "Driver_Name", "Gate_Pass_No", "Total_net_amount", "Eway_bill_No" ],
                        'start_timestamp': f'{date_data}T00:00',
                        'end_timestamp': f'{date_data}T23:59',
                        'type': 'download',
                    }
                    response = requests.get(f'http://{ip}:7704/road_journey_table', params=params, headers=headers)
                    if response.status_code == 200:
                        finalData = json.loads(response.text)
                        console_logger.debug(f"{os.path.join(os.getcwd())}/{finalData.get('File_Path')}")
                        response_code, fetch_email = fetch_email_data()
                        if response_code == 200:
                            console_logger.debug(reportSchedule[8].recipient_list)
                            subject = f"GMR Road Coal Journey Report {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                            body = f"Daily GMR Road Coal Journey Report for Date: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                            checkEmailDevelopment = EmailDevelopmentCheck.objects()
                            if checkEmailDevelopment[0].development == "local":
                                console_logger.debug("inside local")
                                send_email(fetch_email.get("Smtp_user"), subject, fetch_email.get("Smtp_password"), fetch_email.get("Smtp_host"), fetch_email.get("Smtp_port"), reportSchedule[8].recipient_list, body, f"{os.path.join(os.getcwd())}/{finalData.get('File_Path')}", reportSchedule[8].cc_list, reportSchedule[8].bcc_list)
                            elif checkEmailDevelopment[0].development == "prod":
                                console_logger.debug("inside prod")
                                send_data = {
                                    "sender_email": fetch_email.get("Smtp_user"),
                                    "subject": subject,
                                    "password": fetch_email.get("Smtp_password"),
                                    "smtp_host": fetch_email.get("Smtp_host"),
                                    "smtp_port": fetch_email.get("Smtp_port"),
                                    "receiver_email": reportSchedule[8].recipient_list,
                                    "body": body,
                                    "file_path": f"{os.path.join(os.getcwd())}/{finalData.get('File_Path')}",
                                    "cc_list": reportSchedule[8].cc_list,
                                    "bcc_list": reportSchedule[8].bcc_list
                                }
                                # console_logger.debug(send_data)
                                generate_email(Response, email=send_data)
                else:
                    return
        return "success"
    except Exception as e:
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        return {"detail": "failed"}


@router.get("/mail_testing_data", tags=["test"])
def test_api(response: Response):
    try:
        # send_report_generate(**{'report_name': 'daily_coal_logistic_report'})
        # send_report_generate(**{'report_name': 'certificate_expiry_notifications'})
        # send_report_generate(**{'report_name': 'coal_bunkering_schedule'})
        send_report_generate(**{'report_name': 'road_coal_journey_report'})
        return {"detail": "success"}
    except Exception as e:
        console_logger.debug(e)


# @router.get("/coal_logistics_report", tags=["Road Map"])
# def coal_logistics_report(
#     response: Response,
#     specified_date: str,
#     search_text: Optional[str] = None,
#     currentPage: Optional[int] = None,
#     perPage: Optional[int] = None,
#     mine: Optional[str] = "All",
#     type: Optional[str] = "display"
# ):
#     try:
#         result = {"labels": [], "datasets": [], "total": 0, "page_size": 15}
#         if type and type == "display":

#             if specified_date:
#                 data = {}

#                 if mine and mine != "All":
#                     data["mine__icontains"] = mine.upper()

                
#                 page_no = 1
#                 page_len = result["page_size"]

#                 if currentPage:
#                     page_no = currentPage

#                 if perPage:
#                     page_len = perPage
#                     result["page_size"] = perPage

#                 specified_change_date = convert_to_utc_format(specified_date, "%Y-%m-%d")

#                 start_of_month = specified_change_date.replace(day=1)

#                 start_date = datetime.datetime.strftime(start_of_month, '%Y-%m-%d')
#                 end_date = datetime.datetime.strftime(specified_change_date, '%Y-%m-%d')

#                 if search_text:
#                     data = Q()
#                     if search_text.isdigit():
#                         data &= (Q(arv_cum_do_number__icontains=search_text))
#                     else:
#                         data &= (Q(mine__icontains=search_text))
        
#                     logs = (Gmrdata.objects(data).order_by("-created_at"))
#                 else:
#                     logs = (Gmrdata.objects().order_by("-created_at"))

#                 # coal_testing = CoalTesting.objects(receive_date__gte=start_date, receive_date__lte=end_date).order_by("-ID")
#                 if any(logs):
#                     aggregated_data = defaultdict(
#                         lambda: defaultdict(
#                             lambda: {
#                                 "DO_Qty": 0,
#                                 "challan_lr_qty": 0,
#                                 "mine_name": "",
#                                 "balance_qty": 0,
#                                 "percent_of_supply": 0,
#                                 "actual_net_qty": 0,
#                                 "Gross_Calorific_Value_(Adb)": 0,
#                                 "count": 0,
#                                 "coal_count": 0,
#                             }
#                         )
#                     )

#                     # aggregated_coal_data = defaultdict(
#                     #     lambda: defaultdict(
#                     #         lambda: {
#                     #             "Gross_Calorific_Value_(Adb)": 0,
#                     #             "coal_count": 0,
#                     #         }
#                     #     )
#                     # )

#                     # for single_log in coal_testing:
#                     #     coal_date = single_log.receive_date.strftime("%Y-%m")
#                     #     coal_payload = single_log.gradepayload()
#                     #     mine = coal_payload["Mine"]
#                     #     doNo = coal_payload["DO_No"]
#                     #     if coal_payload.get("Gross_Calorific_Value_(Adb)"):
#                     #         aggregated_coal_data[coal_date][doNo]["Gross_Calorific_Value_(Adb)"] += float(coal_payload["Gross_Calorific_Value_(Adb)"])
#                     #         aggregated_coal_data[coal_date][doNo]["coal_count"] += 1

#                     start_dates = {}
#                     grade = 0
#                     for log in logs:
#                         if log.vehicle_in_time!=None:
#                             month = log.vehicle_in_time.strftime("%Y-%m")
#                             date = log.vehicle_in_time.strftime("%Y-%m-%d")
#                             payload = log.payload()
#                             result["labels"] = list(payload.keys())
#                             mine_name = payload.get("Mines_Name")
#                             do_no = payload.get("DO_No")
#                             if payload.get("Grade") is not None:
#                                 if '-' in payload.get("Grade"):
#                                     grade = payload.get("Grade").split("-")[0]
#                                 else:
#                                     grade = payload.get("Grade")
#                             # If start_date is None or the current vehicle_in_time is earlier than start_date, update start_date
#                             if do_no not in start_dates:
#                                 start_dates[do_no] = date
#                             elif date < start_dates[do_no]:
#                                 start_dates[do_no] = date
#                             if payload.get("DO_Qty"):
#                                 aggregated_data[date][do_no]["DO_Qty"] = float(
#                                     payload["DO_Qty"]
#                                 )
#                             else:
#                                 aggregated_data[date][do_no]["DO_Qty"] = 0
#                             if payload.get("Challan_Net_Wt(MT)"):
#                                 aggregated_data[date][do_no]["challan_lr_qty"] += float(
#                                     payload.get("Challan_Net_Wt(MT)")
#                                 )
#                             else:
#                                 aggregated_data[date][do_no]["challan_lr_qty"] = 0
#                             if payload.get("Mines_Name"):
#                                 aggregated_data[date][do_no]["mine_name"] = payload[
#                                     "Mines_Name"
#                                 ]
#                             else:
#                                 aggregated_data[date][do_no]["mine_name"] = "-"
#                             aggregated_data[date][do_no]["count"] += 1 

#                     dataList = [
#                         {
#                             "date": date,
#                             "data": {
#                                 do_no: {
#                                     "DO_Qty": data["DO_Qty"],
#                                     "challan_lr_qty": data["challan_lr_qty"],
#                                     "mine_name": data["mine_name"],
#                                     "grade": grade,
#                                     "date": date,
#                                 }
#                                 for do_no, data in aggregated_data[date].items()
#                             },
#                         }
#                         for date in aggregated_data
#                     ]

#                     # coalDataList = [
#                     #     {"date": coal_date, "data": {
#                     #         doNo: {
#                     #             "average_Gross_Calorific_Value_(Adb)": data["Gross_Calorific_Value_(Adb)"] / data["coal_count"],
#                     #         } for doNo, data in aggregated_coal_data[coal_date].items()
#                     #     }} for coal_date in aggregated_coal_data
#                     # ]

#                     # coal_grades = CoalGrades.objects()  # Fetch all coal grades from the database

#                     # # Iterate through each month's data
#                     # for month_data in coalDataList:
#                     #     for key, mine_data in month_data["data"].items():
#                     #         if mine_data["average_Gross_Calorific_Value_(Adb)"] is not None:
#                     #             for single_coal_grades in coal_grades:
#                     #                 if single_coal_grades["end_value"] != "":
#                     #                     if (int(single_coal_grades["start_value"]) <= int(float(mine_data.get("average_Gross_Calorific_Value_(Adb)"))) <= int(single_coal_grades["end_value"]) and single_coal_grades["start_value"] != "" and single_coal_grades["end_value"] != ""):
#                     #                         mine_data["average_GCV_Grade"] = single_coal_grades["grade"]
#                     #                     elif int(mine_data["average_Gross_Calorific_Value_(Adb)"]) > 7001:
#                     #                         mine_data["average_GCV_Grade"] = "G-1"
#                     #                         break
                    
#                     final_data = []
#                     if specified_date:
#                         filtered_data = [
#                             entry for entry in dataList if entry["date"] == specified_date
#                         ]
#                         console_logger.debug(filtered_data)
#                         if filtered_data:
#                             data = filtered_data[0]["data"]
#                             # dictData["month"] = filtered_data[0]["month"]
#                             for data_dom, values in data.items():
#                                 console_logger.debug(data_dom)
#                                 console_logger.debug(values["challan_lr_qty"])
#                                 dictData = {}
#                                 dictData["DO_No"] = data_dom
#                                 dictData["mine_name"] = values["mine_name"]
#                                 dictData["DO_Qty"] = round(values["DO_Qty"], 2)
#                                 dictData["challan_lr_qty"] = round(values["challan_lr_qty"], 2)
#                                 dictData["date"] = values["date"]
#                                 dictData["cumulative_challan_lr_qty"] = 0
#                                 dictData["balance_qty"] = 0
#                                 dictData["percent_supply"] = 0
#                                 dictData["asking_rate"] = 0
#                                 dictData['average_GCV_Grade'] = values["grade"]
#                                 if data_dom in start_dates:
#                                     dictData["start_date"] = start_dates[data_dom]
#                                     # a total of 45 days data is needed, so date + 44 days
#                                     endDataVariable = datetime.datetime.strptime(start_dates[data_dom], "%Y-%m-%d") + timedelta(days=44)
#                                     # dictData["balance_days"] = dictData["end_date"] - datetime.date.today()
#                                     balance_days = endDataVariable.date() - datetime.date.today()
#                                     dictData["end_date"] = endDataVariable.strftime("%Y-%m-%d")
#                                     dictData["balance_days"] = balance_days.days
#                                 else:
#                                     dictData["start_date"] = None
#                                     dictData["end_date"] = None
#                                     dictData["balance_days"] = None

#                                 # Look for data_dom match in coalDataList and add average_GCV_Grade
#                                 # for coal_data in coalDataList:
#                                 #     # if coal_data['date'] == specified_date and data_dom in coal_data['data']:
#                                 #     if data_dom in coal_data['data']:
#                                 #         dictData['average_GCV_Grade'] = coal_data['data'][data_dom]['average_GCV_Grade']
#                                 #         break
#                                 # else:
#                                 #     dictData['average_GCV_Grade'] = "-"
                    
#                                 # append data
#                                 final_data.append(dictData)
                        
#                         if final_data:
#                             # Find the index of the month data in dataList
#                             index_of_month = next((index for index, item in enumerate(dataList) if item['date'] == specified_date), None)

#                             # If the month is not found, exit or handle the case
#                             if index_of_month is None:
#                                 print("Month data not found.")
#                                 exit()

#                             # Iterate over final_data
#                             for entry in final_data:
#                                 do_no = entry["DO_No"]
#                                 cumulative_lr_qty = 0
                                
#                                 # Iterate over dataList from the first month to the current month
#                                 for i in range(index_of_month + 1):
#                                     month_data = dataList[i]
#                                     data = month_data["data"].get(do_no)
                                    
#                                     # If data is found for the DO_No in the current month, update cumulative_lr_qty
#                                     if data:
#                                         cumulative_lr_qty += data['challan_lr_qty']
                                
#                                 # Update cumulative_challan_lr_qty in final_data
#                                 entry['cumulative_challan_lr_qty'] = round(cumulative_lr_qty, 2)
#                                 if data["DO_Qty"] != 0 and entry["cumulative_challan_lr_qty"] != 0:
#                                     entry["percent_supply"] = round((entry["cumulative_challan_lr_qty"] / data["DO_Qty"]) * 100, 2)
#                                 else:
#                                     entry["percent_supply"] = 0

#                                 if entry["cumulative_challan_lr_qty"] != 0 and data["DO_Qty"] != 0:
#                                     entry["balance_qty"] = round((data["DO_Qty"] - entry["cumulative_challan_lr_qty"]), 2)
#                                 else:
#                                     entry["balance_qty"] = 0
                                
#                                 if entry["balance_qty"] and entry["balance_qty"] != 0:
#                                     if entry["balance_days"]:
#                                         entry["asking_rate"] = round(entry["balance_qty"] / entry["balance_days"], 2)

#                     if final_data:
#                         start_index = (page_no - 1) * page_len
#                         end_index = start_index + page_len
#                         paginated_data = final_data[start_index:end_index]

#                         result["labels"] = list(final_data[0].keys())
#                         result["datasets"] = paginated_data
#                         result["total"] = len(final_data)

#                 return result
#             else:
#                 return 400
#         elif type and type == "download":
#             del type
#             file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
#             target_directory = f"static_server/gmr_ai/{file}"
#             os.umask(0)
#             os.makedirs(target_directory, exist_ok=True, mode=0o777)

#             specified_change_date = convert_to_utc_format(specified_date, "%Y-%m-%d")
#             start_of_month = specified_change_date.replace(day=1)
#             start_date = datetime.datetime.strftime(start_of_month, '%Y-%m-%d')
#             end_date = datetime.datetime.strftime(specified_change_date, '%Y-%m-%d')

#             if search_text:
#                 data = Q()
#                 if search_text.isdigit():
#                     data &= (Q(arv_cum_do_number__icontains=search_text))
#                 else:
#                     data &= (Q(mine__icontains=search_text))

#                 logs = (Gmrdata.objects(data).order_by("mine", "arv_cum_do_number", "-created_at"))
#             else:
#                 logs = (Gmrdata.objects().order_by("mine", "arv_cum_do_number", "-created_at"))

#             # coal_testing = CoalTesting.objects(receive_date__gte=start_date, receive_date__lte=end_date).order_by("-ID")
#             count = len(logs)
#             path = None
#             if any(logs):
#                 aggregated_data = defaultdict(
#                     lambda: defaultdict(
#                         lambda: {
#                             "DO_Qty": 0,
#                             "challan_lr_qty": 0,
#                             "mine_name": "",
#                             "balance_qty": 0,
#                             "percent_of_supply": 0,
#                             "actual_net_qty": 0,
#                             "Gross_Calorific_Value_(Adb)": 0,
#                             "count": 0,
#                             "coal_count": 0,
#                         }
#                     )
#                 )

#                 # aggregated_coal_data = defaultdict(
#                 #     lambda: defaultdict(
#                 #         lambda: {
#                 #             "Gross_Calorific_Value_(Adb)": 0,
#                 #             "coal_count": 0,
#                 #         }
#                 #     )
#                 # )

#                 # for single_log in coal_testing:
#                 #     coal_date = single_log.receive_date.strftime("%Y-%m")
#                 #     coal_payload = single_log.gradepayload()
#                 #     mine = coal_payload["Mine"]
#                 #     doNo = coal_payload["DO_No"]
#                 #     if coal_payload.get("Gross_Calorific_Value_(Adb)"):
#                 #         aggregated_coal_data[coal_date][doNo]["Gross_Calorific_Value_(Adb)"] += float(coal_payload["Gross_Calorific_Value_(Adb)"])
#                 #         aggregated_coal_data[coal_date][doNo]["coal_count"] += 1

#                 start_dates = {}
#                 for log in logs:
#                     if log.vehicle_in_time!=None:
#                         month = log.vehicle_in_time.strftime("%Y-%m")
#                         date = log.vehicle_in_time.strftime("%Y-%m-%d")
#                         payload = log.payload()
#                         result["labels"] = list(payload.keys())
#                         mine_name = payload.get("Mines_Name")
#                         do_no = payload.get("DO_No")
#                         if payload.get("Grade") is not None:
#                             if '-' in payload.get("Grade"):
#                                 grade = payload.get("Grade").split("-")[0]
#                             else:
#                                 grade = payload.get("Grade")
#                         # If start_date is None or the current vehicle_in_time is earlier than start_date, update start_date
#                         if do_no not in start_dates:
#                             start_dates[do_no] = date
#                         elif date < start_dates[do_no]:
#                             start_dates[do_no] = date
#                         if payload.get("DO_Qty"):
#                             aggregated_data[date][do_no]["DO_Qty"] = float(
#                                 payload["DO_Qty"]
#                             )
#                         else:
#                             aggregated_data[date][do_no]["DO_Qty"] = 0
#                         if payload.get("Challan_Net_Wt(MT)"):
#                             aggregated_data[date][do_no]["challan_lr_qty"] += float(
#                                 payload.get("Challan_Net_Wt(MT)")
#                             )
#                         else:
#                             aggregated_data[date][do_no]["challan_lr_qty"] = 0
#                         if payload.get("Mines_Name"):
#                             aggregated_data[date][do_no]["mine_name"] = payload[
#                                 "Mines_Name"
#                             ]
#                         else:
#                             aggregated_data[date][do_no]["mine_name"] = "-"
#                         aggregated_data[date][do_no]["count"] += 1 

#                 dataList = [
#                     {
#                         "date": date,
#                         "data": {
#                             do_no: {
#                                 "DO_Qty": data["DO_Qty"],
#                                 "challan_lr_qty": data["challan_lr_qty"],
#                                 "mine_name": data["mine_name"],
#                                 "grade": grade,
#                                 "date": date,
#                             }
#                             for do_no, data in aggregated_data[date].items()
#                         },
#                     }
#                     for date in aggregated_data
#                 ]

#                 # coalDataList = [
#                 #     {"date": coal_date, "data": {
#                 #         doNo: {
#                 #             "average_Gross_Calorific_Value_(Adb)": data["Gross_Calorific_Value_(Adb)"] / data["coal_count"],
#                 #         } for doNo, data in aggregated_coal_data[coal_date].items()
#                 #     }} for coal_date in aggregated_coal_data
#                 # ]

#                 # coal_grades = CoalGrades.objects()  # Fetch all coal grades from the database

#                 # # Iterate through each month's data
#                 # for month_data in coalDataList:
#                 #     for key, mine_data in month_data["data"].items():
#                 #         if mine_data["average_Gross_Calorific_Value_(Adb)"] is not None:
#                 #             for single_coal_grades in coal_grades:
#                 #                 if single_coal_grades["end_value"] != "":
#                 #                     if (int(single_coal_grades["start_value"]) <= int(float(mine_data.get("average_Gross_Calorific_Value_(Adb)"))) <= int(single_coal_grades["end_value"]) and single_coal_grades["start_value"] != "" and single_coal_grades["end_value"] != ""):
#                 #                         mine_data["average_GCV_Grade"] = single_coal_grades["grade"]
#                 #                     elif int(mine_data["average_Gross_Calorific_Value_(Adb)"]) > 7001:
#                 #                         mine_data["average_GCV_Grade"] = "G-1"
#                 #                         break
                
#                 final_data = []
#                 if specified_date:
#                     filtered_data = [
#                         entry for entry in dataList if entry["date"] == specified_date
#                     ]
#                     if filtered_data:
#                         data = filtered_data[0]["data"]
#                         # dictData["month"] = filtered_data[0]["month"]
#                         for data_dom, values in data.items():
#                             dictData = {}
#                             dictData["DO_No"] = data_dom
#                             dictData["mine_name"] = values["mine_name"]
#                             dictData["DO_Qty"] = round(values["DO_Qty"], 2)
#                             dictData["challan_lr_qty"] = round(values["challan_lr_qty"], 2)
#                             dictData["date"] = values["date"]
#                             dictData["cumulative_challan_lr_qty"] = 0
#                             dictData["balance_qty"] = 0
#                             dictData["percent_supply"] = 0
#                             dictData["asking_rate"] = 0
#                             dictData['average_GCV_Grade'] = values["grade"] 
#                             if data_dom in start_dates:
#                                 dictData["start_date"] = start_dates[data_dom]
#                                 # a total of 45 days data is needed, so date + 44 days
#                                 dictData["end_date"] = datetime.datetime.strptime(start_dates[data_dom], "%Y-%m-%d") + timedelta(days=44)
#                                 # dictData["balance_days"] = dictData["end_date"] - datetime.date.today()
#                                 balance_days = dictData["end_date"].date() - datetime.date.today()
#                                 dictData["balance_days"] = balance_days.days
#                             else:
#                                 dictData["start_date"] = None
#                                 dictData["end_date"] = None
#                                 dictData["balance_days"] = None

#                             # Look for data_dom match in coalDataList and add average_GCV_Grade
#                             # for coal_data in coalDataList:
#                             #     # if coal_data['date'] == specified_date and data_dom in coal_data['data']:
#                             #     if data_dom in coal_data['data']:
#                             #         dictData['average_GCV_Grade'] = coal_data['data'][data_dom]['average_GCV_Grade']
#                             #         break
#                             # else:
#                             #     dictData['average_GCV_Grade'] = "-"
                
#                             # append data
#                             final_data.append(dictData)
                            
#                     if final_data:
#                         path = os.path.join(
#                             "static_server",
#                             "gmr_ai",
#                             file,
#                             "Coal_Logistics_Report_{}.xlsx".format(
#                                 datetime.datetime.now().strftime("%Y-%m-%d:%H:%M:%S"),
#                             ),
#                         )

#                         filename = os.path.join(os.getcwd(), path)
#                         workbook = xlsxwriter.Workbook(filename)
#                         workbook.use_zip64()
#                         cell_format2 = workbook.add_format()
#                         cell_format2.set_bold()
#                         cell_format2.set_font_size(10)
#                         cell_format2.set_align("center")
#                         cell_format2.set_align("vjustify")

#                         worksheet = workbook.add_worksheet()
#                         worksheet.set_column("A:AZ", 20)
#                         worksheet.set_default_row(50)
#                         cell_format = workbook.add_format()
#                         cell_format.set_font_size(10)
#                         cell_format.set_align("center")
#                         cell_format.set_align("vcenter")

#                         # Find the index of the month data in dataList
#                         index_of_month = next((index for index, item in enumerate(dataList) if item['date'] == specified_date), None)

#                         # If the month is not found, exit or handle the case
#                         if index_of_month is None:
#                             print("Month data not found.")
#                             exit()

#                         # Iterate over final_data
#                         for entry in final_data:
#                             do_no = entry["DO_No"]
#                             cumulative_lr_qty = 0
                            
#                             # Iterate over dataList from the first month to the current month
#                             for i in range(index_of_month + 1):
#                                 month_data = dataList[i]
#                                 data = month_data["data"].get(do_no)
                                
#                                 # If data is found for the DO_No in the current month, update cumulative_lr_qty
#                                 if data:
#                                     cumulative_lr_qty += data['challan_lr_qty']
                            
#                             # Update cumulative_challan_lr_qty in final_data
#                             entry['cumulative_challan_lr_qty'] = cumulative_lr_qty
#                             if data["DO_Qty"] != 0 and entry["cumulative_challan_lr_qty"] != 0:
#                                 entry["percent_supply"] = round((entry["cumulative_challan_lr_qty"] / data["DO_Qty"]) * 100, 2)
#                             else:
#                                 entry["percent_supply"] = 0

#                             if entry["cumulative_challan_lr_qty"] != 0 and data["DO_Qty"] != 0:
#                                 entry["balance_qty"] = round((data["DO_Qty"] - entry["cumulative_challan_lr_qty"]), 2)
#                             else:
#                                 entry["balance_qty"] = 0
                            
#                             if entry["balance_qty"] and entry["balance_qty"] != 0:
#                                 if entry["balance_days"]:
#                                     entry["asking_rate"] = round(entry["balance_qty"] / entry["balance_days"], 2)

#                         result["datasets"] = final_data

#                         headers = ["Sr.No", "Mine Name", "DO_No", "Grade", "DO Qty", "Challan LR Qty", "Cumulative Challan Lr_Qty", "Balance Qty", "% of Supply", "Balance Days", "Asking Rate", "Do Start Date", "Do End Date"]
                        
#                         for index, header in enumerate(headers):
#                                     worksheet.write(0, index, header, cell_format2)
                        
#                         row = 1
#                         for single_data in result["datasets"]:
#                             worksheet.write(row, 0, count, cell_format)
#                             worksheet.write(row, 1, single_data["mine_name"])
#                             worksheet.write(row, 2, single_data["DO_No"])
#                             worksheet.write(row, 3, single_data["average_GCV_Grade"])
#                             worksheet.write(row, 4, single_data["DO_Qty"])
#                             worksheet.write(row, 5, single_data["challan_lr_qty"])
#                             worksheet.write(row, 6, single_data["cumulative_challan_lr_qty"])
#                             worksheet.write(row, 7, single_data["balance_qty"])
#                             worksheet.write(row, 8, single_data["percent_supply"])
#                             worksheet.write(row, 9, single_data["balance_days"])
#                             worksheet.write(row, 10, single_data["asking_rate"])
#                             worksheet.write(row, 11, single_data["start_date"])
#                             worksheet.write(row, 12, single_data["end_date"].strftime("%Y-%m-%d"))

#                             count -= 1
#                             row += 1
#                         workbook.close()

#                         return {
#                                 "Type": "daily_coal_report",
#                                 "Datatype": "Report",
#                                 "File_Path": path,
#                             }
#                     else:
#                         console_logger.error("No data found")
#                         return {
#                                     "Type": "daily_coal_report",
#                                     "Datatype": "Report",
#                                     "File_Path": path,
#                                 }

#     except Exception as e:
#         response.status_code = 400
#         console_logger.debug(e)
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#         console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
#         console_logger.debug(
#             "Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno)
#         )
#         return e


@router.get("/coal_logistics_report", tags=["Road Map"])
def coal_logistics_report(
    response: Response,
    specified_date: str,
    search_text: Optional[str] = None,
    currentPage: Optional[int] = None,
    perPage: Optional[int] = None,
    mine: Optional[str] = "All",
    type: Optional[str] = "display"
):
    try:
        result = {"labels": [], "datasets": [], "total": 0, "page_size": 15}
        if type and type == "display":

            # if specified_date:
            data = {}

            if mine and mine != "All":
                data["mine__icontains"] = mine.upper()

            
            page_no = 1
            page_len = result["page_size"]

            if currentPage:
                page_no = currentPage

            if perPage:
                page_len = perPage
                result["page_size"] = perPage

            if specified_date:
                specified_change_date = convert_to_utc_format(specified_date, "%Y-%m-%d")
                to_ts = convert_to_utc_format(f'{specified_date} 23:59:59', "%Y-%m-%d %H:%M:%S")

            if search_text:
                data = Q()
                if search_text.isdigit():
                    data &= (Q(arv_cum_do_number__icontains=search_text))
                else:
                    data &= (Q(mine__icontains=search_text))
    
                logs = (Gmrdata.objects(GWEL_Tare_Time__lte=to_ts, actual_tare_qty__ne=None, gate_approved=True, GWEL_Tare_Time__ne=None, **data).order_by("-GWEL_Tare_Time"))
            else:
                logs = Gmrdata.objects(GWEL_Tare_Time__lte=to_ts, actual_tare_qty__ne=None, gate_approved=True, GWEL_Tare_Time__ne=None).order_by("-GWEL_Tare_Time")

            if any(logs):
                aggregated_data = defaultdict(
                    lambda: defaultdict(
                        lambda: {
                            "DO_Qty": 0,
                            "challan_lr_qty": 0,
                            "challan_lr_qty_full": 0,
                            "mine_name": "",
                            "balance_qty": 0,
                            "percent_of_supply": 0,
                            "actual_net_qty": 0,
                            "Gross_Calorific_Value_(Adb)": 0,
                            "count": 0,
                            "coal_count": 0,
                        }
                    )
                )

                start_dates = {}
                grade = 0
                for log in logs:
                    if log.GWEL_Tare_Time!=None:
                        month = log.GWEL_Tare_Time.strftime("%Y-%m")
                        date = log.GWEL_Tare_Time.strftime("%Y-%m-%d")
                        payload = log.payload()
                        result["labels"] = list(payload.keys())
                        mine_name = payload.get("Mines_Name")
                        do_no = payload.get("DO_No")
                        if payload.get("Grade") is not None:
                            if '-' in payload.get("Grade"):
                                grade = payload.get("Grade").split("-")[0]
                            else:
                                grade = payload.get("Grade")
                        # If start_date is None or the current vehicle_in_time is earlier than start_date, update start_date
                        if do_no not in start_dates:
                            start_dates[do_no] = date
                        elif date < start_dates[do_no]:
                            start_dates[do_no] = date
                        if payload.get("DO_Qty"):
                            aggregated_data[date][do_no]["DO_Qty"] = float(
                                payload["DO_Qty"]
                            )
                        else:
                            aggregated_data[date][do_no]["DO_Qty"] = 0
                        
                        challan_net_wt = payload.get("Challan_Net_Wt(MT)")    
                
                        if challan_net_wt:
                            aggregated_data[date][do_no]["challan_lr_qty"] += float(challan_net_wt)

                        # if payload.get("Challan_Net_Wt(MT)"):
                        #     aggregated_data[date][do_no]["challan_lr_qty"] += float(
                        #         payload.get("Challan_Net_Wt(MT)")
                        #     )
                        # else:
                        #     aggregated_data[date][do_no]["challan_lr_qty"] = 0
                        if payload.get("Mines_Name"):
                            aggregated_data[date][do_no]["mine_name"] = payload[
                                "Mines_Name"
                            ]
                        else:
                            aggregated_data[date][do_no]["mine_name"] = "-"
                        aggregated_data[date][do_no]["count"] += 1 

                dataList = [
                    {
                        "date": date,
                        "data": {
                            do_no: {
                                "DO_Qty": data["DO_Qty"],
                                "challan_lr_qty": data["challan_lr_qty"],
                                "mine_name": data["mine_name"],
                                "grade": grade,
                                "date": date,
                            }
                            for do_no, data in aggregated_data[date].items()
                        },
                    }
                    for date in aggregated_data
                ]
                
                final_data = []
                for entry in dataList:
                    date = entry["date"]
                    # console_logger.debug(entry)
                    for data_dom, values in entry['data'].items():
                        # console_logger.debug(values["grade"])
                        dictData = {}
                        dictData["DO_No"] = data_dom
                        dictData["mine_name"] = values["mine_name"]
                        dictData["DO_Qty"] = values["DO_Qty"]
                        dictData["club_challan_lr_qty"] = values["challan_lr_qty"]
                        dictData['challan_lr_qty'] = 0
                        dictData["date"] = values["date"]
                        dictData["cumulative_challan_lr_qty"] = 0
                        dictData["balance_qty"] = 0
                        dictData["percent_supply"] = 0
                        dictData["asking_rate"] = 0
                        dictData['average_GCV_Grade'] = values["grade"]
                        
                        
                        if data_dom in start_dates:
                            # console_logger.debug(start_dates)
                            dictData["start_date"] = start_dates[data_dom]
                            dictData["end_date"] = datetime.datetime.strptime(start_dates[data_dom], "%Y-%m-%d") + timedelta(days=44)
                            balance_days = dictData["end_date"].date() - datetime.datetime.today().date()
                            dictData["balance_days"] = balance_days.days
                        else:
                            dictData["start_date"] = None
                            dictData["end_date"] = None
                            dictData["balance_days"] = None
                        
                        final_data.append(dictData)

                if final_data:
                    # filtered_data = [
                    #     entry for entry in dataList if entry["date"] == specified_date
                    # ]

                    startdate = f'{specified_date} 00:00:00'
                    enddate = f'{specified_date} 23:59:59'
                    # to_ts = datetime.datetime.strptime(enddate,"%Y-%m-%d %H:%M:%S")
                    from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")
                    to_ts = convert_to_utc_format(enddate, "%Y-%m-%d %H:%M:%S")
                    
                    pipeline = [
                        {
                            "$match": {
                                "GWEL_Tare_Time": {"$gte": from_ts, "$lte": to_ts},
                                    "net_qty": {"$ne": None}
                                }
                        },
                        {
                        '$group': {
                            '_id': {
                                'date': {
                                    '$dateToString': {
                                        'format': '%Y-%m-%d', 
                                        'date': '$GWEL_Tare_Time'
                                    }
                                }, 
                                'do_no': '$arv_cum_do_number'
                            }, 
                            'total_net_qty': {
                                '$sum': {
                                    '$toDouble': '$net_qty'
                                }
                            }
                        }
                    }]

                    filtered_data_new = Gmrdata.objects.aggregate(pipeline)
                    # dictDaata = {}
                    aggregated_totals = defaultdict(float)
                    for single_data_entry in filtered_data_new:
                        do_no = single_data_entry['_id']['do_no']
                        total_net_qty = single_data_entry['total_net_qty']
                        aggregated_totals[do_no] += total_net_qty

                    data_by_do = {}
                    finaldataMain = [single_data_list for single_data_list in final_data if single_data_list.get("balance_days") >= 0]
                    for entry in finaldataMain:
                        do_no = entry['DO_No']
                        
                        try:
                            club_challan_lr_qty = float(entry.get('club_challan_lr_qty', 0))
                        except ValueError:
                            club_challan_lr_qty = 0

                        if do_no not in data_by_do:
                            data_by_do[do_no] = entry.copy()  # Copy the entry to avoid modifying the original
                            data_by_do[do_no]['cumulative_challan_lr_qty'] = round(club_challan_lr_qty, 2)
                        else:
                            data_by_do[do_no]['cumulative_challan_lr_qty'] = round(
                                data_by_do[do_no].get('cumulative_challan_lr_qty', 0) + club_challan_lr_qty, 2
                            )
                        if filtered_data_new:
                            # data = filtered_data[0]["data"]
                            # Update challan_lr_qty if the DO_No matches
                            if do_no in aggregated_totals:
                                data_by_do[do_no]['challan_lr_qty'] = round(aggregated_totals[do_no], 2)
                            else:
                                data_by_do[do_no]['challan_lr_qty'] = 0
                        
                        # Update calculated fields
                        if data_by_do[do_no]['DO_Qty'] != 0 and data_by_do[do_no]['cumulative_challan_lr_qty'] != 0:
                            data_by_do[do_no]['percent_supply'] = round((data_by_do[do_no]['cumulative_challan_lr_qty'] / data_by_do[do_no]['DO_Qty']) * 100, 2)
                        else:
                            data_by_do[do_no]['percent_supply'] = 0

                        if data_by_do[do_no]['cumulative_challan_lr_qty'] != 0 and data_by_do[do_no]['DO_Qty'] != 0:
                            data_by_do[do_no]['balance_qty'] = round(data_by_do[do_no]['DO_Qty'] - data_by_do[do_no]['cumulative_challan_lr_qty'], 2)
                        else:
                            data_by_do[do_no]['balance_qty'] = 0
                        
                        if data_by_do[do_no]['balance_days'] and data_by_do[do_no]['balance_qty'] != 0:
                            data_by_do[do_no]['asking_rate'] = round(data_by_do[do_no]['balance_qty'] / data_by_do[do_no]['balance_days'], 2)

                        del entry['club_challan_lr_qty']
                    
                final_data = list(data_by_do.values())

                if final_data:
                    start_index = (page_no - 1) * page_len
                    end_index = start_index + page_len
                    paginated_data = final_data[start_index:end_index]

                    # result["labels"] = list(final_data[0].keys())
                    result["labels"] = ["DO_No", "mine_name", "DO_Qty", "date", "challan_lr_qty", "cumulative_challan_lr_qty","balance_qty", "percent_supply", "asking_rate", "average_GCV_Grade", "start_date", "end_date", "balance_days"]
                    result["datasets"] = paginated_data
                    result["total"] = len(final_data)

                return result
            # else:
            #     return 400
        elif type and type == "download":
            del type
            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)
            if specified_date:
                specified_change_date = convert_to_utc_format(specified_date, "%Y-%m-%d")
                to_ts = convert_to_utc_format(f'{specified_date} 23:59:59', "%Y-%m-%d %H:%M:%S")

            if search_text:
                data = Q()
                if search_text.isdigit():
                    data &= (Q(arv_cum_do_number__icontains=search_text))
                else:
                    data &= (Q(mine__icontains=search_text))
    
                logs = (Gmrdata.objects(GWEL_Tare_Time__lte=to_ts, actual_tare_qty__ne=None, gate_approved=True, GWEL_Tare_Time__ne=None, **data).order_by("-created_at"))
            else:
                console_logger.debug("inside else")
                logs = Gmrdata.objects(GWEL_Tare_Time__lte=to_ts, actual_tare_qty__ne=None, gate_approved=True, GWEL_Tare_Time__ne=None).order_by("-created_at")

            # coal_testing = CoalTesting.objects(receive_date__gte=start_date, receive_date__lte=end_date).order_by("-ID")
            count = len(logs)
            path = None
            if any(logs):
                aggregated_data = defaultdict(
                    lambda: defaultdict(
                        lambda: {
                            "DO_Qty": 0,
                            "challan_lr_qty": 0,
                            "challan_lr_qty_full": 0,
                            "mine_name": "",
                            "balance_qty": 0,
                            "percent_of_supply": 0,
                            "actual_net_qty": 0,
                            "Gross_Calorific_Value_(Adb)": 0,
                            "count": 0,
                            "coal_count": 0,
                        }
                    )
                )

                start_dates = {}
                grade = 0
                for log in logs:
                    if log.GWEL_Tare_Time!=None:
                        month = log.GWEL_Tare_Time.strftime("%Y-%m")
                        date = log.GWEL_Tare_Time.strftime("%Y-%m-%d")
                        payload = log.payload()
                        result["labels"] = list(payload.keys())
                        mine_name = payload.get("Mines_Name")
                        do_no = payload.get("DO_No")
                        if payload.get("Grade") is not None:
                            if '-' in payload.get("Grade"):
                                grade = payload.get("Grade").split("-")[0]
                            else:
                                grade = payload.get("Grade")
                        # If start_date is None or the current vehicle_in_time is earlier than start_date, update start_date
                        if do_no not in start_dates:
                            start_dates[do_no] = date
                        elif date < start_dates[do_no]:
                            start_dates[do_no] = date
                        if payload.get("DO_Qty"):
                            aggregated_data[date][do_no]["DO_Qty"] = float(
                                payload["DO_Qty"]
                            )
                        else:
                            aggregated_data[date][do_no]["DO_Qty"] = 0

                        challan_net_wt = payload.get("Challan_Net_Wt(MT)")    
                
                        if challan_net_wt:
                            aggregated_data[date][do_no]["challan_lr_qty"] += float(challan_net_wt)
                        # if payload.get("Challan_Net_Wt(MT)"):
                        #     aggregated_data[date][do_no]["challan_lr_qty"] += float(
                        #         payload.get("Challan_Net_Wt(MT)")
                        #     )
                        # else:
                        #     aggregated_data[date][do_no]["challan_lr_qty"] = 0
                        if payload.get("Mines_Name"):
                            aggregated_data[date][do_no]["mine_name"] = payload[
                                "Mines_Name"
                            ]
                        else:
                            aggregated_data[date][do_no]["mine_name"] = "-"
                        aggregated_data[date][do_no]["count"] += 1 

                dataList = [
                    {
                        "date": date,
                        "data": {
                            do_no: {
                                "DO_Qty": data["DO_Qty"],
                                "challan_lr_qty": data["challan_lr_qty"],
                                "mine_name": data["mine_name"],
                                "grade": grade,
                                "date": date,
                            }
                            for do_no, data in aggregated_data[date].items()
                        },
                    }
                    for date in aggregated_data
                ]
                
                final_data = []
                for entry in dataList:
                    date = entry["date"]
                    # console_logger.debug(entry)
                    for data_dom, values in entry['data'].items():
                        # console_logger.debug(values["grade"])
                        dictData = {}
                        dictData["DO_No"] = data_dom
                        dictData["mine_name"] = values["mine_name"]
                        dictData["DO_Qty"] = values["DO_Qty"]
                        dictData["club_challan_lr_qty"] = values["challan_lr_qty"]
                        dictData['challan_lr_qty'] = 0
                        dictData["date"] = values["date"]
                        dictData["cumulative_challan_lr_qty"] = 0
                        dictData["balance_qty"] = 0
                        dictData["percent_supply"] = 0
                        dictData["asking_rate"] = 0
                        dictData['average_GCV_Grade'] = values["grade"]
                        
                        
                        if data_dom in start_dates:
                            # console_logger.debug(start_dates)
                            dictData["start_date"] = start_dates[data_dom]
                            dictData["end_date"] = datetime.datetime.strptime(start_dates[data_dom], "%Y-%m-%d") + timedelta(days=44)
                            balance_days = dictData["end_date"].date() - datetime.datetime.today().date()
                            dictData["balance_days"] = balance_days.days
                        else:
                            dictData["start_date"] = None
                            dictData["end_date"] = None
                            dictData["balance_days"] = None
                        
                        final_data.append(dictData)  
                if final_data:
                    path = os.path.join(
                        "static_server",
                        "gmr_ai",
                        file,
                        "Coal_Logistics_Report_{}.xlsx".format(
                            datetime.datetime.now().strftime("%Y-%m-%d:%H:%M:%S"),
                        ),
                    )

                    filename = os.path.join(os.getcwd(), path)
                    workbook = xlsxwriter.Workbook(filename)
                    workbook.use_zip64()
                    cell_format2 = workbook.add_format()
                    cell_format2.set_bold()
                    cell_format2.set_font_size(10)
                    cell_format2.set_align("center")
                    cell_format2.set_align("vjustify")

                    worksheet = workbook.add_worksheet()
                    worksheet.set_column("A:AZ", 20)
                    worksheet.set_default_row(50)
                    cell_format = workbook.add_format()
                    cell_format.set_font_size(10)
                    cell_format.set_align("center")
                    cell_format.set_align("vcenter")

                    startdate = f'{specified_date} 00:00:00'
                    enddate = f'{specified_date} 23:59:59'
                    from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")
                    to_ts = convert_to_utc_format(enddate, "%Y-%m-%d %H:%M:%S")
                    
                    pipeline = [
                        {
                            "$match": {
                                "GWEL_Tare_Time": {"$gte": from_ts, "$lte": to_ts},
                                    "net_qty": {"$ne": None}
                                }
                        },
                        {
                        '$group': {
                            '_id': {
                                'date': {
                                    '$dateToString': {
                                        'format': '%Y-%m-%d', 
                                        'date': '$GWEL_Tare_Time'
                                    }
                                }, 
                                'do_no': '$arv_cum_do_number'
                            }, 
                            'total_net_qty': {
                                '$sum': {
                                    '$toDouble': '$net_qty'
                                }
                            }
                        }
                    }]

                    # filtered_data = [
                    #     entry for entry in dataList if entry["date"] == specified_date
                    # ]
                    
                    filtered_data_new = Gmrdata.objects.aggregate(pipeline)
                    # dictDaata = {}
                    aggregated_totals = defaultdict(float)
                    for single_data_entry in filtered_data_new:
                        console_logger.debug(single_data_entry)
                        do_no = single_data_entry['_id']['do_no']
                        total_net_qty = single_data_entry['total_net_qty']
                        aggregated_totals[do_no] += total_net_qty

                    # Create a dictionary to store the latest entries based on DO_No
                    data_by_do = {}
                    finaldataMain = [single_data_list for single_data_list in final_data if single_data_list.get("balance_days") >= 0]
                    # Iterate over final_data   
                    for entry in finaldataMain:
                        do_no = entry['DO_No']
                
                        # clubbing all challan_lr_qty to get cumulative_challan_lr_qty
                        if do_no not in data_by_do:
                            data_by_do[do_no] = entry
                            data_by_do[do_no]['cumulative_challan_lr_qty'] = round(entry['club_challan_lr_qty'], 2)
                        else:
                            data_by_do[do_no]['cumulative_challan_lr_qty'] += round(entry['club_challan_lr_qty'], 2)
                        
                        if filtered_data_new:
                            # data = filtered_data[0]["data"]
                            # Update challan_lr_qty if the DO_No matches
                            if do_no in aggregated_totals:
                                data_by_do[do_no]['challan_lr_qty'] = round(aggregated_totals[do_no], 2)
                            else:
                                data_by_do[do_no]['challan_lr_qty'] = 0

                        # Update calculated fields
                        if data_by_do[do_no]['DO_Qty'] != 0 and data_by_do[do_no]['cumulative_challan_lr_qty'] != 0:
                            data_by_do[do_no]['percent_supply'] = round((data_by_do[do_no]['cumulative_challan_lr_qty'] / data_by_do[do_no]['DO_Qty']) * 100, 2)
                        else:
                            data_by_do[do_no]['percent_supply'] = 0

                        if data_by_do[do_no]['cumulative_challan_lr_qty'] != 0 and data_by_do[do_no]['DO_Qty'] != 0:
                            data_by_do[do_no]['balance_qty'] = round(data_by_do[do_no]['DO_Qty'] - data_by_do[do_no]['cumulative_challan_lr_qty'], 2)
                        else:
                            data_by_do[do_no]['balance_qty'] = 0
                        
                        if data_by_do[do_no]['balance_days'] and data_by_do[do_no]['balance_qty'] != 0:
                            data_by_do[do_no]['asking_rate'] = round(data_by_do[do_no]['balance_qty'] / data_by_do[do_no]['balance_days'], 2)

                    final_data = list(data_by_do.values())
                    result["datasets"] = final_data

                    headers = ["Sr.No", "Mine Name", "DO_No", "Grade", "DO Qty", "Challan LR Qty", "Cumulative Challan Lr_Qty", "Balance Qty", "% of Supply", "Balance Days", "Asking Rate", "Do Start Date", "Do End Date"]
                    
                    for index, header in enumerate(headers):
                        worksheet.write(0, index, header, cell_format2)
                    
                    row = 1
                    for single_data in result["datasets"]:
                        worksheet.write(row, 0, count, cell_format)
                        worksheet.write(row, 1, single_data["mine_name"])
                        worksheet.write(row, 2, single_data["DO_No"])
                        worksheet.write(row, 3, single_data["average_GCV_Grade"])
                        worksheet.write(row, 4, single_data["DO_Qty"])
                        worksheet.write(row, 5, single_data["challan_lr_qty"])
                        worksheet.write(row, 6, single_data["cumulative_challan_lr_qty"])
                        worksheet.write(row, 7, single_data["balance_qty"])
                        worksheet.write(row, 8, single_data["percent_supply"])
                        worksheet.write(row, 9, single_data["balance_days"])
                        worksheet.write(row, 10, single_data["asking_rate"])
                        worksheet.write(row, 11, single_data["start_date"])
                        worksheet.write(row, 12, single_data["end_date"].strftime("%Y-%m-%d"))

                        count -= 1
                        row += 1
                    workbook.close()

                    return {
                            "Type": "daily_coal_report",
                            "Datatype": "Report",
                            "File_Path": path,
                        }
                else:
                    console_logger.error("No data found")
                    return {
                                "Type": "daily_coal_report",
                                "Datatype": "Report",
                                "File_Path": path,
                            }

    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug(
            "Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno)
        )
        return e




@router.get("/coal_logistics_report_train", tags=["Rail Map"])
def coal_logistics_report_train(
    response: Response,
    specified_date: str,
    search_text: Optional[str] = None,
    currentPage: Optional[int] = None,
    perPage: Optional[int] = None,
    mine: Optional[str] = "All",
    type: Optional[str] = "display"
):
    try:
        result = {"labels": [], "datasets": [], "total": 0, "page_size": 15}
        if type and type == "display":

            if specified_date:
                data = {}

                if mine and mine != "All":
                    data["mine__icontains"] = mine.upper()

                
                page_no = 1
                page_len = result["page_size"]

                if currentPage:
                    page_no = currentPage

                if perPage:
                    page_len = perPage
                    result["page_size"] = perPage

                # specified_change_date = convert_to_utc_format(specified_date, "%Y-%m-%d")
                specified_change_date = datetime.datetime.strptime(specified_date, "%Y-%m-%d")

                start_of_month = specified_change_date.replace(day=1)

                start_date = datetime.datetime.strftime(start_of_month, '%Y-%m-%d')
                end_date = datetime.datetime.strftime(specified_change_date, '%Y-%m-%d')

                if search_text:
                    data = Q()
                    if search_text.isdigit():
                        data &= (Q(arv_cum_do_number__icontains=search_text))
                    else:
                        data &= (Q(mine__icontains=search_text))
        
                    logs = (RailData.objects(data).order_by("source", "rr_no", "-created_at"))
                else:
                    logs = (RailData.objects().order_by("source", "rr_no", "-created_at"))
                coal_testing_train = CoalTestingTrain.objects(receive_date__gte=start_date, receive_date__lte=end_date).order_by("-ID")
                if any(logs):
                    aggregated_data = defaultdict(
                        lambda: defaultdict(
                            lambda: {
                                "DO_Qty": 0,
                                "challan_lr_qty": 0,
                                "mine_name": "",
                                "balance_qty": 0,
                                "percent_of_supply": 0,
                                "actual_net_qty": 0,
                                "Gross_Calorific_Value_(Adb)": 0,
                                "count": 0,
                                "coal_count": 0,
                            }
                        )
                    )

                    aggregated_coal_data = defaultdict(
                        lambda: defaultdict(
                            lambda: {
                                "Gross_Calorific_Value_(Adb)": 0,
                                "coal_count": 0,
                            }
                        )
                    )
                    for single_log in coal_testing_train:
                        coal_date = single_log.receive_date.strftime("%Y-%m")
                        coal_payload = single_log.gradepayload()
                        mine = coal_payload["Mine"]
                        rr_no = coal_payload["rrNo"]
                        if coal_payload.get("Gross_Calorific_Value_(Adb)"):
                            aggregated_coal_data[coal_date][rr_no]["Gross_Calorific_Value_(Adb)"] += float(coal_payload["Gross_Calorific_Value_(Adb)"])
                            aggregated_coal_data[coal_date][rr_no]["coal_count"] += 1

                    start_dates = {}
                    grade = 0
                    for log in logs:
                        if log.created_at!=None:
                            month = log.created_at.strftime("%Y-%m")
                            date = log.created_at.strftime("%Y-%m-%d")
                            payload = log.payload()
                            result["labels"] = list(payload.keys())
                            mine_name = payload.get("source")
                            rr_no = payload.get("rr_no")
                            # if payload.get("Grade") is not None:
                            #     if '-' in payload.get("Grade"):
                            #         grade = payload.get("Grade").split("-")[0]
                            #     else:
                            #         grade = payload.get("Grade")
                            # If start_date is None or the current vehicle_in_time is earlier than start_date, update start_date
                            if rr_no not in start_dates:
                                start_dates[rr_no] = date
                            elif date < start_dates[rr_no]:
                                start_dates[rr_no] = date
                            if payload.get("rr_qty"):
                                # aggregated_data[date][do_no]["DO_Qty"] += float(
                                #     payload["DO_Qty"]
                                # )
                                aggregated_data[date][rr_no]["rr_qty"] = float(
                                    payload["rr_qty"]
                                )
                            else:
                                aggregated_data[date][rr_no]["rr_qty"] = 0
                            if payload.get("total_secl_net_wt"):
                                aggregated_data[date][rr_no]["challan_lr_qty"] += float(
                                    payload.get("total_secl_net_wt")
                                )
                            else:
                                aggregated_data[date][rr_no]["challan_lr_qty"] = 0
                            if payload.get("source"):
                                aggregated_data[date][rr_no]["source"] = payload[
                                    "source"
                                ]
                            else:
                                aggregated_data[date][rr_no]["source"] = "-"
                            aggregated_data[date][rr_no]["count"] += 1 
                    dataList = [
                        {
                            "date": date,
                            "data": {
                                rr_no: {
                                    "rr_qty": data["rr_qty"] if data["rr_qty"] else 0,
                                    "challan_lr_qty": data["challan_lr_qty"],
                                    "mine_name": data["source"],
                                    "date": date,
                                }
                                for rr_no, data in aggregated_data[date].items()
                            },
                        }
                        for date in aggregated_data
                    ]

                    coalDataList = [
                        {"date": coal_date, "data": {
                            rr_no: {
                                "average_Gross_Calorific_Value_(Adb)": data["Gross_Calorific_Value_(Adb)"] / data["coal_count"],
                            } for rr_no, data in aggregated_coal_data[coal_date].items()
                        }} for coal_date in aggregated_coal_data
                    ]

                    coal_grades = CoalGrades.objects()  # Fetch all coal grades from the database

                    # Iterate through each month's data
                    for month_data in coalDataList:
                        for key, mine_data in month_data["data"].items():
                            if mine_data["average_Gross_Calorific_Value_(Adb)"] is not None:
                                for single_coal_grades in coal_grades:
                                    if single_coal_grades["end_value"] != "":
                                        if (int(single_coal_grades["start_value"]) <= int(float(mine_data.get("average_Gross_Calorific_Value_(Adb)"))) <= int(single_coal_grades["end_value"]) and single_coal_grades["start_value"] != "" and single_coal_grades["end_value"] != ""):
                                            mine_data["average_GCV_Grade"] = single_coal_grades["grade"]
                                        elif int(mine_data["average_Gross_Calorific_Value_(Adb)"]) > 7001:
                                            mine_data["average_GCV_Grade"] = "G-1"
                                            break
                    
                    final_data = []
                    if specified_date:
                        filtered_data = [
                            entry for entry in dataList if entry["date"] == specified_date
                        ]
                        if filtered_data:
                            data = filtered_data[0]["data"]
                            # dictData["month"] = filtered_data[0]["month"]
                            for data_dom, values in data.items():
                                dictData = {}
                                dictData["rr_no"] = data_dom
                                dictData["mine_name"] = values["mine_name"]
                                dictData["rr_qty"] = round(values["rr_qty"], 2)
                                dictData["challan_lr_qty"] = round(values["challan_lr_qty"], 2)
                                dictData["date"] = values["date"]
                                dictData["cumulative_challan_lr_qty"] = 0
                                dictData["balance_qty"] = 0
                                dictData["percent_supply"] = 0
                                dictData["asking_rate"] = 0
                                # dictData['average_GCV_Grade'] = values["grade"]
                                if data_dom in start_dates:
                                    dictData["start_date"] = start_dates[data_dom]
                                    # a total of 45 days data is needed, so date + 44 days
                                    endDataVariable = datetime.datetime.strptime(start_dates[data_dom], "%Y-%m-%d") + timedelta(days=44)
                                    # dictData["balance_days"] = dictData["end_date"] - datetime.date.today()
                                    balance_days = endDataVariable.date() - datetime.date.today()
                                    dictData["end_date"] = endDataVariable.strftime("%Y-%m-%d")
                                    dictData["balance_days"] = balance_days.days
                                else:
                                    dictData["start_date"] = None
                                    dictData["end_date"] = None
                                    dictData["balance_days"] = None

                                # Look for data_dom match in coalDataList and add average_GCV_Grade
                                for coal_data in coalDataList:
                                    # if coal_data['date'] == specified_date and data_dom in coal_data['data']:
                                    if data_dom in coal_data['data']:
                                        dictData['average_GCV_Grade'] = coal_data['data'][data_dom]['average_GCV_Grade']
                                        break
                                else:
                                    dictData['average_GCV_Grade'] = "-"
                    
                                # append data
                                final_data.append(dictData)
                        
                        if final_data:
                            # Find the index of the month data in dataList
                            index_of_month = next((index for index, item in enumerate(dataList) if item['date'] == specified_date), None)

                            # If the month is not found, exit or handle the case
                            if index_of_month is None:
                                print("Month data not found.")
                                exit()

                            # Iterate over final_data
                            for entry in final_data:
                                rr_no = entry["rr_no"]
                                cumulative_lr_qty = 0
                                
                                # Iterate over dataList from the first month to the current month
                                for i in range(index_of_month + 1):
                                    month_data = dataList[i]
                                    data = month_data["data"].get(rr_no)
                                    
                                    # If data is found for the rr_no in the current month, update cumulative_lr_qty
                                    if data:
                                        cumulative_lr_qty += data['challan_lr_qty']
                                
                                # Update cumulative_challan_lr_qty in final_data
                                entry['cumulative_challan_lr_qty'] = round(cumulative_lr_qty, 2)
                                if data["rr_qty"] != 0 and entry["cumulative_challan_lr_qty"] != 0:
                                    entry["percent_supply"] = round((entry["cumulative_challan_lr_qty"] / data["rr_qty"]) * 100, 2)
                                else:
                                    entry["percent_supply"] = 0

                                if entry["cumulative_challan_lr_qty"] != 0 and data["rr_qty"] != 0:
                                    entry["balance_qty"] = round((data["rr_qty"] - entry["cumulative_challan_lr_qty"]), 2)
                                else:
                                    entry["balance_qty"] = 0
                                
                                if entry["balance_qty"] and entry["balance_qty"] != 0:
                                    if entry["balance_days"]:
                                        entry["asking_rate"] = round(entry["balance_qty"] / entry["balance_days"], 2)

                    if final_data:
                        start_index = (page_no - 1) * page_len
                        end_index = start_index + page_len
                        paginated_data = final_data[start_index:end_index]

                        result["labels"] = list(final_data[0].keys())
                        result["datasets"] = paginated_data
                        result["total"] = len(final_data)

                return result
            else:
                return 400
        elif type and type == "download":
            del type
            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)

            specified_change_date = datetime.datetime.strptime(specified_date, "%Y-%m-%d")

            start_of_month = specified_change_date.replace(day=1)

            start_date = datetime.datetime.strftime(start_of_month, '%Y-%m-%d')
            end_date = datetime.datetime.strftime(specified_change_date, '%Y-%m-%d')

            if search_text:
                data = Q()
                if search_text.isdigit():
                    data &= (Q(arv_cum_do_number__icontains=search_text))
                else:
                    data &= (Q(mine__icontains=search_text))

                logs = (RailData.objects(data).order_by("source", "rr_no", "-created_at"))
            else:
                logs = (RailData.objects().order_by("source", "rr_no", "-created_at"))

            coal_testing_train = CoalTestingTrain.objects(receive_date__gte=start_date, receive_date__lte=end_date).order_by("-ID")
            count = len(logs)
            path = None
            if any(logs):
                aggregated_data = defaultdict(
                    lambda: defaultdict(
                        lambda: {
                            "DO_Qty": 0,
                            "challan_lr_qty": 0,
                            "mine_name": "",
                            "balance_qty": 0,
                            "percent_of_supply": 0,
                            "actual_net_qty": 0,
                            "Gross_Calorific_Value_(Adb)": 0,
                            "count": 0,
                            "coal_count": 0,
                        }
                    )
                )

                aggregated_coal_data = defaultdict(
                    lambda: defaultdict(
                        lambda: {
                            "Gross_Calorific_Value_(Adb)": 0,
                            "coal_count": 0,
                        }
                    )
                )

                for single_log in coal_testing_train:
                    coal_date = single_log.receive_date.strftime("%Y-%m")
                    coal_payload = single_log.gradepayload()
                    mine = coal_payload["Mine"]
                    rr_no = coal_payload["rrNo"]
                    if coal_payload.get("Gross_Calorific_Value_(Adb)"):
                        aggregated_coal_data[coal_date][rr_no]["Gross_Calorific_Value_(Adb)"] += float(coal_payload["Gross_Calorific_Value_(Adb)"])
                        aggregated_coal_data[coal_date][rr_no]["coal_count"] += 1

                start_dates = {}
                for log in logs:
                    if log.created_at!=None:
                        month = log.created_at.strftime("%Y-%m")
                        date = log.created_at.strftime("%Y-%m-%d")
                        payload = log.payload()
                        result["labels"] = list(payload.keys())
                        mine_name = payload.get("source")
                        rr_no = payload.get("rr_no")
                        # if payload.get("Grade") is not None:
                        #     if '-' in payload.get("Grade"):
                        #         grade = payload.get("Grade").split("-")[0]
                        #     else:
                        #         grade = payload.get("Grade")
                        # If start_date is None or the current vehicle_in_time is earlier than start_date, update start_date
                        if rr_no not in start_dates:
                            start_dates[rr_no] = date
                        elif date < start_dates[rr_no]:
                            start_dates[rr_no] = date
                        if payload.get("rr_qty"):
                            aggregated_data[date][rr_no]["rr_qty"] = float(
                                payload["rr_qty"]
                            )
                        if payload.get("total_secl_net_wt"):
                            aggregated_data[date][rr_no]["challan_lr_qty"] += float(
                                payload.get("total_secl_net_wt")
                            )
                        else:
                            aggregated_data[date][rr_no]["challan_lr_qty"] = 0
                        if payload.get("source"):
                            aggregated_data[date][rr_no]["source"] = payload[
                                "source"
                            ]
                        else:
                            aggregated_data[date][rr_no]["source"] = "-"
                        aggregated_data[date][rr_no]["count"] += 1

                dataList = [
                    {
                        "date": date,
                        "data": {
                            rr_no: {
                                "rr_qty": data["rr_qty"],
                                "challan_lr_qty": data["challan_lr_qty"],
                                "mine_name": data["source"],
                                "date": date,
                            }
                            for rr_no, data in aggregated_data[date].items()
                        },
                    }
                    for date in aggregated_data
                ]

                coalDataList = [
                    {"date": coal_date, "data": {
                        rr_no: {
                            "average_Gross_Calorific_Value_(Adb)": data["Gross_Calorific_Value_(Adb)"] / data["coal_count"],
                        } for rr_no, data in aggregated_coal_data[coal_date].items()
                    }} for coal_date in aggregated_coal_data
                ]

                coal_grades = CoalGrades.objects()  # Fetch all coal grades from the database

                # Iterate through each month's data
                for month_data in coalDataList:
                    for key, mine_data in month_data["data"].items():
                        if mine_data["average_Gross_Calorific_Value_(Adb)"] is not None:
                            for single_coal_grades in coal_grades:
                                if single_coal_grades["end_value"] != "":
                                    if (int(single_coal_grades["start_value"]) <= int(float(mine_data.get("average_Gross_Calorific_Value_(Adb)"))) <= int(single_coal_grades["end_value"]) and single_coal_grades["start_value"] != "" and single_coal_grades["end_value"] != ""):
                                        mine_data["average_GCV_Grade"] = single_coal_grades["grade"]
                                    elif int(mine_data["average_Gross_Calorific_Value_(Adb)"]) > 7001:
                                        mine_data["average_GCV_Grade"] = "G-1"
                                        break
                
                final_data = []
                if specified_date:
                    filtered_data = [
                        entry for entry in dataList if entry["date"] == specified_date
                    ]
                    if filtered_data:
                        data = filtered_data[0]["data"]
                        # dictData["month"] = filtered_data[0]["month"]
                        for data_dom, values in data.items():
                            dictData = {}
                            dictData["rr_no"] = data_dom
                            dictData["mine_name"] = values["mine_name"]
                            dictData["rr_qty"] = round(values["rr_qty"], 2)
                            dictData["challan_lr_qty"] = round(values["challan_lr_qty"], 2)
                            dictData["date"] = values["date"]
                            dictData["cumulative_challan_lr_qty"] = 0
                            dictData["balance_qty"] = 0
                            dictData["percent_supply"] = 0
                            dictData["asking_rate"] = 0
                            # dictData['average_GCV_Grade'] = values["grade"] 
                            if data_dom in start_dates:
                                dictData["start_date"] = start_dates[data_dom]
                                # a total of 45 days data is needed, so date + 44 days
                                dictData["end_date"] = datetime.datetime.strptime(start_dates[data_dom], "%Y-%m-%d") + timedelta(days=44)
                                # dictData["balance_days"] = dictData["end_date"] - datetime.date.today()
                                balance_days = dictData["end_date"].date() - datetime.date.today()
                                dictData["balance_days"] = balance_days.days
                            else:
                                dictData["start_date"] = None
                                dictData["end_date"] = None
                                dictData["balance_days"] = None

                            # Look for data_dom match in coalDataList and add average_GCV_Grade
                            for coal_data in coalDataList:
                                # if coal_data['date'] == specified_date and data_dom in coal_data['data']:
                                if data_dom in coal_data['data']:
                                    dictData['average_GCV_Grade'] = coal_data['data'][data_dom]['average_GCV_Grade']
                                    break
                            else:
                                dictData['average_GCV_Grade'] = "-"
                
                            # append data
                            final_data.append(dictData)
                            
                    if final_data:
                        path = os.path.join(
                            "static_server",
                            "gmr_ai",
                            file,
                            "Coal_Logistics_Rail_Report_{}.xlsx".format(
                                datetime.datetime.now().strftime("%Y-%m-%d:%H:%M:%S"),
                            ),
                        )

                        filename = os.path.join(os.getcwd(), path)
                        workbook = xlsxwriter.Workbook(filename)
                        workbook.use_zip64()
                        cell_format2 = workbook.add_format()
                        cell_format2.set_bold()
                        cell_format2.set_font_size(10)
                        cell_format2.set_align("center")
                        cell_format2.set_align("vjustify")

                        worksheet = workbook.add_worksheet()
                        worksheet.set_column("A:AZ", 20)
                        worksheet.set_default_row(50)
                        cell_format = workbook.add_format()
                        cell_format.set_font_size(10)
                        cell_format.set_align("center")
                        cell_format.set_align("vcenter")

                        # Find the index of the month data in dataList
                        index_of_month = next((index for index, item in enumerate(dataList) if item['date'] == specified_date), None)

                        # If the month is not found, exit or handle the case
                        if index_of_month is None:
                            print("Month data not found.")
                            exit()

                        # Iterate over final_data
                        for entry in final_data:
                            rr_no = entry["rr_no"]
                            cumulative_lr_qty = 0
                            
                            # Iterate over dataList from the first month to the current month
                            for i in range(index_of_month + 1):
                                month_data = dataList[i]
                                data = month_data["data"].get(rr_no)
                                
                                # If data is found for the DO_No in the current month, update cumulative_lr_qty
                                if data:
                                    cumulative_lr_qty += data['challan_lr_qty']
                            
                            # Update cumulative_challan_lr_qty in final_data
                            entry['cumulative_challan_lr_qty'] = cumulative_lr_qty
                            if data["rr_qty"] != 0 and entry["cumulative_challan_lr_qty"] != 0:
                                entry["percent_supply"] = round((entry["cumulative_challan_lr_qty"] / data["rr_qty"]) * 100, 2)
                            else:
                                entry["percent_supply"] = 0

                            if entry["cumulative_challan_lr_qty"] != 0 and data["rr_qty"] != 0:
                                entry["balance_qty"] = round((data["rr_qty"] - entry["cumulative_challan_lr_qty"]), 2)
                            else:
                                entry["balance_qty"] = 0
                            
                            if entry["balance_qty"] and entry["balance_qty"] != 0:
                                if entry["balance_days"]:
                                    entry["asking_rate"] = round(entry["balance_qty"] / entry["balance_days"], 2)

                        result["datasets"] = final_data

                        headers = [
                            "Sr.No", 
                            "Mine Name", 
                            "RR No", 
                            "Grade", 
                            "RR Qty", 
                            "Challan LR Qty", 
                            "Cumulative Challan Lr_Qty", 
                            "Balance Qty", 
                            "% of Supply", 
                            "Balance Days", 
                            "Asking Rate", 
                            "Do Start Date", 
                            "Do End Date"
                        ]
                        
                        for index, header in enumerate(headers):
                            worksheet.write(0, index, header, cell_format2)
                        
                        row = 1
                        for single_data in result["datasets"]:
                            worksheet.write(row, 0, count, cell_format)
                            worksheet.write(row, 1, single_data["mine_name"])
                            worksheet.write(row, 2, single_data["rr_no"])
                            worksheet.write(row, 3, single_data["average_GCV_Grade"])
                            worksheet.write(row, 4, single_data["rr_qty"])
                            worksheet.write(row, 5, single_data["challan_lr_qty"])
                            worksheet.write(row, 6, single_data["cumulative_challan_lr_qty"])
                            worksheet.write(row, 7, single_data["balance_qty"])
                            worksheet.write(row, 8, single_data["percent_supply"])
                            worksheet.write(row, 9, single_data["balance_days"])
                            worksheet.write(row, 10, single_data["asking_rate"])
                            worksheet.write(row, 11, single_data["start_date"])
                            worksheet.write(row, 12, single_data["end_date"].strftime("%Y-%m-%d"))

                            count -= 1
                            row += 1
                        workbook.close()

                        return {
                                "Type": "daily_rail_coal_report",
                                "Datatype": "Report",
                                "File_Path": path,
                            }
                    else:
                        console_logger.error("No data found")
                        return {
                                    "Type": "daily_rail_coal_report",
                                    "Datatype": "Report",
                                    "File_Path": path,
                                }

    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug(
            "Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno)
        )
        return e


@router.get("/mine_wise_average_gcv", tags=["Road Map"])
def mine_wise_average_gwel_gcv(
    response: Response,
    type: Optional[str] = "Daily",
    Month: Optional[str] = None, 
    Daily: Optional[str] = None, 
    Year: Optional[str] = None
):
    try:
        if type == "Daily":
            specified_date = datetime.datetime.strptime(Daily, "%Y-%m-%d")
            start_of_month = specified_date.replace(day=1)
            start_date = datetime.datetime.strftime(start_of_month, '%Y-%m-%d')
            end_date = datetime.datetime.strftime(specified_date, '%Y-%m-%d')
        elif type == "Week":
            specified_date = datetime.datetime.now().date()
            start_of_week = specified_date - datetime.timedelta(days=7)
            start_date = datetime.datetime.strftime(start_of_week, '%Y-%m-%d')
            end_date = datetime.datetime.strftime(specified_date, '%Y-%m-%d')
        elif type == "Month":
            date=Month
            datestructure = date.replace(" ", "").split("-")
            final_month = f"{datestructure[0]}-{str(datestructure[1]).zfill(2)}"
            start_month = f"{final_month}-01"
            startd_date = datetime.datetime.strptime(start_month, "%Y-%m-%d")
            endd_date = startd_date + datetime.timedelta(days=31)
            start_date = datetime.datetime.strftime(startd_date, '%Y-%m-%d')
            end_date = datetime.datetime.strftime(endd_date, '%Y-%m-%d')
        elif type == "Year":
            date = Year
            endd_date =f'{date}-12-31'
            startd_date = f'{date}-01-01'
            format_data = "%Y-%m-%d"
            end_date=datetime.datetime.strftime(datetime.datetime.strptime(endd_date,format_data), format_data)
            start_date=datetime.datetime.strftime(datetime.datetime.strptime(startd_date,format_data), format_data)

        # Query for CoalTesting objects
        fetchCoalTesting = CoalTesting.objects(
            receive_date__gte= datetime.datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"), receive_date__lte= datetime.datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M")
        )
        # Query for CoalTestingTrain objects
        fetchCoalTestingTrain = CoalTestingTrain.objects(
            receive_date__gte = datetime.datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"), receive_date__lte= datetime.datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M")
        )
        # Query for GMRData objects
        # fetchGmrData = Gmrdata.objects(created_at__gte=datetime.datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"), created_at__lte=datetime.datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"))
        
        rrNo_values = {}

        # Iterate through fetched CoalTesting objects
        for single_coal_testing in fetchCoalTesting:
            rrNo = single_coal_testing.rrNo
            location = single_coal_testing.location
            for param in single_coal_testing.parameters:
                if param["parameter_Name"] == "Gross_Calorific_Value_(Arb)":
                    if param["val1"] != None and param["val1"] != "":
                        calorific_value = float(param["val1"])
                        break
            else:
                continue

            # Aggregate values based on rrNo
            if rrNo in rrNo_values:
                rrNo_values[location] += calorific_value
            else:
                rrNo_values[location] = calorific_value

        # Iterate through fetched CoalTestingTrain objects
        for single_coal_testing_train in fetchCoalTestingTrain:
            rrNo = single_coal_testing_train.rrNo
            location = single_coal_testing_train.location
            for param in single_coal_testing_train.parameters:
                if param["parameter_Name"] == "Gross_Calorific_Value_(Arb)":
                    if param["val1"] != None:
                        calorific_value = float(param["val1"])
                        break
            else:
                continue

            # Aggregate values based on rrNo
            if rrNo in rrNo_values:
                rrNo_values[location] += calorific_value
            else:
                rrNo_values[location] = calorific_value
        
        # fetch data from AopTarget
        aopList = []
        fetchAopTarget = AopTarget.objects()
        if fetchAopTarget:
            for single_aop_target in fetchAopTarget:
                aopList.append(single_aop_target.payload())

        target_dict = {item['source_name']: int(item['aop_target']) for item in aopList}
        
        aligned_target_data = [target_dict.get(label.strip(), 0) for label in rrNo_values.keys()]

        result = {
            "data": {
                "labels": list(rrNo_values.keys()),
                "datasets": [
                    {"label": "Mine", "data": list(rrNo_values.values()), "order": 1, "type": "bar"},
                    {"label": "Target", "data": aligned_target_data, "order": 0, "type": "line"},
                ],
            }
        }

        return result
        
    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

@router.get("/source_wise_profit_loss", tags=["Road Map"])
def source_wise_transist_loss_gain(response: Response, type: Optional[str] = "Daily", Month: Optional[str] = None, Daily: Optional[str] = None, Year: Optional[str] = None):
    try:

        if type == "Daily":
            specified_date = datetime.datetime.strptime(Daily, "%Y-%m-%d")
            start_of_month = specified_date.replace(day=1)
            start_date = datetime.datetime.strftime(start_of_month, '%Y-%m-%d')
            end_date = datetime.datetime.strftime(specified_date, '%Y-%m-%d')
        elif type == "Week":
            specified_date = datetime.datetime.now().date()
            start_of_week = specified_date - datetime.timedelta(days=7)
            start_date = datetime.datetime.strftime(start_of_week, '%Y-%m-%d')
            end_date = datetime.datetime.strftime(specified_date, '%Y-%m-%d')
        elif type == "Month":
            date=Month
            datestructure = date.replace(" ", "").split("-")
            final_month = f"{datestructure[0]}-{str(datestructure[1]).zfill(2)}"
            start_month = f"{final_month}-01"
            startd_date = datetime.datetime.strptime(start_month, "%Y-%m-%d")
            endd_date = startd_date + datetime.timedelta(days=31)
            start_date = datetime.datetime.strftime(startd_date, '%Y-%m-%d')
            end_date = datetime.datetime.strftime(endd_date, '%Y-%m-%d')
        elif type == "Year":
            date=Year
            endd_date =f'{date}-12-31'
            startd_date = f'{date}-01-01'
            format_data = "%Y-%m-%d"
            end_date=datetime.datetime.strftime(datetime.datetime.strptime(endd_date,format_data), format_data)
            start_date=datetime.datetime.strftime(datetime.datetime.strptime(startd_date,format_data), format_data)

        net_qty_totals = {}
        actual_net_qty_totals = {}

        fetchGmrData = Gmrdata.objects(created_at__gte=datetime.datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"), created_at__lte=datetime.datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"))

        # Iterate over the retrieved data
        for single_gmr_data in fetchGmrData:
            mine_name = single_gmr_data.mine
            net_qty = single_gmr_data.net_qty
            actual_net_qty = single_gmr_data.actual_net_qty
            
            # Update net_qty totals dictionary
            if mine_name in net_qty_totals:
                net_qty_totals[mine_name] += float(net_qty)
            else:
                net_qty_totals[mine_name] = float(net_qty)
            if actual_net_qty:
                # Update actual_net_qty totals dictionary
                if mine_name in actual_net_qty_totals:
                    actual_net_qty_totals[mine_name] += float(actual_net_qty)
                else:
                    actual_net_qty_totals[mine_name] = float(actual_net_qty)

        # Perform clubbing - subtract actual_net_qty from net_qty for each mine
        clubbed_data = {}
        
        for mine in net_qty_totals:
            clubbed_data[mine] = actual_net_qty_totals.get(mine, 0) - net_qty_totals[mine]
        
        result = {
            "data": {
                "labels": list(clubbed_data.keys()),
                "datasets": [
                    {"label": "Mine", "data": list(clubbed_data.values()), "order": 1, "type": "bar"},
                ],
            }
        }

        return result

    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e
    

@router.get("/month_wise_transit_loss", tags=["Road Map"])
def month_wise_transist_loss_gain_mode(response: Response):
    try:
        result = {
            "labels": [],
            "datasets": [],
            "weight_total": [],
            "total": 0,
            "page_size": 15,
        }

        financial_year = get_financial_year(datetime.date.today().strftime("%Y-%m-%d"))

        logs = (
            Gmrdata.objects(vehicle_in_time__gte=financial_year.get("start_date"), vehicle_in_time__lte=financial_year.get("end_date"))
            .order_by("mine", "arv_cum_do_number", "-created_at")
        )
        if any(logs):
            aggregated_data = defaultdict(
                lambda: defaultdict(
                    lambda: {
                        "net_qty": 0,
                        "mine_name": "",
                        "actual_net_qty": 0,
                        "count": 0,
                    }
                )
            )

            start_dates = {}
            for log in logs:
                if log.vehicle_in_time!=None:
                    month = log.vehicle_in_time.strftime("%Y-%m")
                    payload = log.payload()
                    result["labels"] = list(payload.keys())
                    mine_name = payload.get("Mines_Name")
                    do_no = payload.get("DO_No")
                    # If start_date is None or the current GWEL_Tare_Time is earlier than start_date, update start_date
                    if do_no not in start_dates:
                        start_dates[do_no] = month
                    elif month < start_dates[do_no]:
                        start_dates[do_no] = month
                    if payload.get("GWEL_Net_Wt(MT)") and payload.get("GWEL_Net_Wt(MT)") != "NaN":
                        aggregated_data[month][do_no]["actual_net_qty"] += float(
                            payload["GWEL_Net_Wt(MT)"]
                        )
                    else:
                        aggregated_data[month][do_no]["actual_net_qty"] = 0
                    if payload.get("Challan_Net_Wt(MT)") and payload.get("Challan_Net_Wt(MT)") != "NaN":
                        aggregated_data[month][do_no]["net_qty"] += float(
                            payload.get("Challan_Net_Wt(MT)")
                        )
                    else:
                        aggregated_data[month][do_no]["net_qty"] = 0
                    if payload.get("Mines_Name"):
                        aggregated_data[month][do_no]["mine_name"] = payload[
                            "Mines_Name"
                        ]
                    else:
                        aggregated_data[month][do_no]["mine_name"] = "-"
                    aggregated_data[month][do_no]["count"] += 1 
            dataList = [
                {
                    "month": month,
                    "data": {
                        do_no: {
                            "final_net_qty": data["actual_net_qty"]-data["net_qty"],
                            "mine_name": data["mine_name"],
                            "month": month,
                        }
                        for do_no, data in aggregated_data[month].items()
                    },
                }
                for month in aggregated_data
            ]
            total_monthly_final_net_qty = {}
            yearly_final_data = {}
            for data in dataList:
                month = data["month"]

                total_monthly_final_net_qty[month] = 0

                for entry in data["data"].values():
                    total_monthly_final_net_qty[month] += entry["final_net_qty"]

            total_monthly_final_net = dict(sorted(total_monthly_final_net_qty.items()))
            # for key, single_count in total_monthly_final_net_qty.items():
            #     if datetime.datetime.strptime(key, "%Y-%m").year in yearly_final_data:
            #         yearly_final_data[datetime.datetime.strptime(key, "%Y-%m").year] += single_count
            #     else:
            #         yearly_final_data[datetime.datetime.strptime(key, "%Y-%m").year] = single_count
                    
            # yearly_final_data_sort = dict(sorted(yearly_final_data.items()))

        result = {
            "data": {
                "labels": list(total_monthly_final_net.keys()),
                "datasets": [
                    {"label": "month", "data": list(total_monthly_final_net.values())}
                ]
            }
        }

        return result
    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/overall_transit_loss", tags=["Road Map"])
def year_wise_transist_loss_gain_mode(response: Response):
    try:
        result = {
            "labels": [],
            "datasets": [],
            "weight_total": [],
            "total": 0,
            "page_size": 15,
        }

        financial_year = get_financial_year(datetime.date.today().strftime("%Y-%m-%d"))

        logs = (
            Gmrdata.objects(vehicle_in_time__gte=financial_year.get("start_date"), vehicle_in_time__lte=financial_year.get("end_date"))
            .order_by("mine", "arv_cum_do_number", "-created_at")
        )
        if any(logs):
            aggregated_data = defaultdict(
                lambda: defaultdict(
                    lambda: {
                        "net_qty": 0,
                        "mine_name": "",
                        "actual_net_qty": 0,
                        "count": 0,
                    }
                )
            )

            start_dates = {}
            for log in logs:
                if log.vehicle_in_time!=None:
                    month = log.vehicle_in_time.strftime("%Y-%m")
                    payload = log.payload()
                    result["labels"] = list(payload.keys())
                    mine_name = payload.get("Mines_Name")
                    do_no = payload.get("DO_No")
                    # If start_date is None or the current GWEL_Tare_Time is earlier than start_date, update start_date
                    if do_no not in start_dates:
                        start_dates[do_no] = month
                    elif month < start_dates[do_no]:
                        start_dates[do_no] = month
                    if payload.get("GWEL_Net_Wt(MT)") and payload.get("GWEL_Net_Wt(MT)") != "NaN":
                        aggregated_data[month][do_no]["actual_net_qty"] += float(
                            payload["GWEL_Net_Wt(MT)"]
                        )
                    else:
                        aggregated_data[month][do_no]["actual_net_qty"] = 0
                    if payload.get("Challan_Net_Wt(MT)") and payload.get("Challan_Net_Wt(MT)") != "NaN":
                        aggregated_data[month][do_no]["net_qty"] += float(
                            payload.get("Challan_Net_Wt(MT)")
                        )
                    else:
                        aggregated_data[month][do_no]["net_qty"] = 0
                    if payload.get("Mines_Name"):
                        aggregated_data[month][do_no]["mine_name"] = payload[
                            "Mines_Name"
                        ]
                    else:
                        aggregated_data[month][do_no]["mine_name"] = 0
                    aggregated_data[month][do_no]["count"] += 1 

            dataList = [
                {
                    "month": month,
                    "data": {
                        do_no: {
                            "final_net_qty": data["actual_net_qty"]-data["net_qty"],
                            "mine_name": data["mine_name"],
                            "month": month,
                        }
                        for do_no, data in aggregated_data[month].items()
                    },
                }
                for month in aggregated_data
            ]

            total_monthly_final_net_qty = {}
            yearly_final_data = {}
            for data in dataList:
                month = data["month"]

                total_monthly_final_net_qty[month] = 0

                for entry in data["data"].values():
                    total_monthly_final_net_qty[month] += entry["final_net_qty"]

            # total_monthly_final_net = dict(sorted(total_monthly_final_net_qty.items()))

            for key, single_count in total_monthly_final_net_qty.items():
                if datetime.datetime.strptime(key, "%Y-%m").year in yearly_final_data:
                    yearly_final_data[datetime.datetime.strptime(key, "%Y-%m").year] += single_count
                else:
                    yearly_final_data[datetime.datetime.strptime(key, "%Y-%m").year] = single_count
                    
            yearly_final_data_sort = dict(sorted(yearly_final_data.items()))

            result = {
                "data": {
                    "labels": ["Road Mode"],
                    "datasets": [
                        {"label": "Road Mode", "data": list(yearly_final_data_sort.values())}
                    ]
                }
            }

            return result
    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

@router.get("/transit_loss", tags=["Road Map"])
def endpoint_to_fetch_transit_loss(response: Response, type: Optional[str] = "Daily", 
                                   Daily: Optional[str] = None, 
                                   Month: Optional[str] = None,
                                   Year: Optional[str] = None,
                                   Overall: Optional[str] = None):
    try:
        data = {}
        timezone = pytz.timezone('Asia/Kolkata')
        current_time = datetime.datetime.now(timezone)
        UTC_OFFSET_TIMEDELTA = datetime.datetime.utcnow() - datetime.datetime.now()

        basePipeline = [
            {
                "$match": {
                        "GWEL_Tare_Time": {
                            "$gte": None,
                        },
                },
            },
            {
                '$project': {
                    'ts': None,
                    'actual_net_qty': {
                        '$toDouble': '$actual_net_qty'
                    }, 
                    'net_qty': {
                        '$toDouble': '$net_qty'
                    }, 
                    'label': {
                        '$cond': {
                            'if': {
                                '$ne': [
                                    '$vehicle_number', None
                                ]
                            }, 
                            'then': 'Road', 
                            'else': 'Rail'
                        }
                    }, 
                    '_id': 0
                }
            }, {
                '$group': {
                    '_id': {
                        'ts': '$ts', 
                        'label': '$label'
                    }, 
                    'actual_net_qty_sum': {
                        '$sum': '$actual_net_qty'
                    }, 
                    'net_qty_sum': {
                        '$sum': '$net_qty'
                    }
                }
            }, {
                '$project': {
                    '_id': 0, 
                    'ts': '$_id.ts', 
                    'label': '$_id.label', 
                    'data': {
                        '$subtract': [
                            '$actual_net_qty_sum', '$net_qty_sum'
                        ]
                    }
                }
            }
        ]

        if type == "Daily":
            date = Daily
            end_date = f'{date} 23:59:59'
            start_date = f'{date} 00:00:00'
            format_data = "%Y-%m-%d %H:%M:%S"
            endd_date = convert_to_utc_format(end_date.__str__(), format_data)
            startd_date = convert_to_utc_format(start_date.__str__(), format_data)

            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date

            basePipeline[1]["$project"]["ts"] = {"$hour": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}

            result = {
                "data": {
                    "labels": [str(i) for i in range(1, 25)],
                    "datasets": [
                        {"label": "Road", "data": [0 for i in range(1, 25)]},
                        {"label": "Rail", "data": [0 for i in range(1, 25)]},
                    ],
                }
            }

        elif type == "Week":
            start_date = (
                datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
                - datetime.timedelta(days=7)
            )
            end_date = datetime.datetime.utcnow().replace(hour=23, minute=59, second=59, microsecond=0)
            endd_date = end_date-datetime.timedelta(days=1)

            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = convert_to_utc_format(start_date.__str__(), "%Y-%m-%d %H:%M:%S")
            basePipeline[1]["$project"]["ts"] = {"$dayOfMonth": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}
            result = {
                "data": {
                    "labels": [
                        (
                         convert_to_utc_format(start_date.__str__(),"%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=i+1)
                        ).strftime("%d")
                        for i in range(1, 8)
                    ],
                    "datasets": [
                        {"label": "Road", "data": [0 for i in range(7)]},
                        {"label": "Rail", "data": [0 for i in range(7)]},
                    ],
                }
            }
        elif type == "Month":
            date = Month
            format_data = "%Y - %m-%d"
            start_date = f'{date}-01'
            startd_date = timezone.localize(datetime.datetime.strptime(start_date, format_data))

            end_date = startd_date + relativedelta(day=31)
            end_label = end_date.strftime("%d")

            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$lte"] = end_date
            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date
            # basePipeline[1]["$project"]["ts"] = {"$dayOfMonth": "$GWEL_Tare_Time"}
            basePipeline[1]["$project"]["ts"] = {"$dayOfMonth": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}
            result = {
                "data": {
                    "labels": [
                        (
                            startd_date + datetime.timedelta(days=i)
                        ).strftime("%d")
                        for i in range(int(end_label))
                    ],
                    "datasets": [
                        {"label": "Road", "data": [0 for i in range(int(end_label))]},
                        {"label": "Rail", "data": [0 for i in range(int(end_label))]},
                    ],
                }
            }

        elif type == "Year":
            date = Year
            end_date = f'{date}-12-31 23:59:59'
            start_date = f'{date}-01-01 00:00:00'
            format_data = "%Y-%m-%d %H:%M:%S"
            endd_date = timezone.localize(datetime.datetime.strptime(end_date, format_data))
            startd_date = timezone.localize(datetime.datetime.strptime(start_date, format_data))

            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date

            # basePipeline[1]["$project"]["ts"] = {"$month": "$GWEL_Tare_Time"}
            basePipeline[1]["$project"]["ts"] = {"$month": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}

            result = {
                "data": {
                    "labels": [
                        (
                            startd_date + relativedelta(months=i)
                        ).strftime("%b %y")
                        for i in range(12)
                    ],
                    "datasets": [
                        {"label": "Road", "data": [0 for i in range(12)]},
                        {"label": "Rail", "data": [0 for i in range(12)]},
                    ],
                }
            }
        elif type == "Overall":
            date = Overall
            end_date = f'{int(date) + 1}-03-31 23:59:59'
            start_date = f'{date}-04-01 00:00:00'
            format_data = "%Y-%m-%d %H:%M:%S"
            endd_date = timezone.localize(datetime.datetime.strptime(end_date, format_data))
            startd_date = timezone.localize(datetime.datetime.strptime(start_date, format_data))

            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date

            # basePipeline[1]["$project"]["ts"] = {"$year": "$GWEL_Tare_Time"}
            basePipeline[1]["$project"]["ts"] = {"$year": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}

            result = {
                "data": {
                    "labels": [f"{int(date)} - {int(date) + 1}"],
                    "datasets": [
                        {"label": "Road", "data": [0]},
                        {"label": "Rail", "data": [0]},
                    ],
                }
            }

        output = Gmrdata.objects().aggregate(basePipeline)
        outputDict = {}

        if type == "Overall":
            modified_labels = [f"{int(date)} - {int(date) + 1}"]
            for data in output:
                label = data["label"]
                total_loss = data["data"]
                outputDict[label] = total_loss

            for dataset in result["data"]["datasets"]:
                label = dataset["label"]
                if label in outputDict:
                    dataset["data"] = [outputDict[label]]
        else:
            for data in output:
                ts = data["ts"]
                label = data["label"]
                sum_value = data["data"]
                if ts not in outputDict:
                    outputDict[ts] = {label: sum_value}
                else:
                    if label not in outputDict[ts]:
                        outputDict[ts][label] = sum_value
                    else:
                        outputDict[ts][label] += sum_value

            for index, label in enumerate(result["data"]["labels"]):

                if type == "Daily":
                    modified_labels = [str(i) for i in range(1, 25)]

                elif type == "Week":
                    modified_labels = [
                        (
                            start_date + datetime.timedelta(days=i+1)
                        ).strftime("%d-%m-%Y,%a")
                        for i in range(7)
                    ]

                elif type == "Month":
                    modified_labels = [
                        (
                            startd_date + datetime.timedelta(days=i + 1)
                        ).strftime("%d-%b")
                        for i in range(-1, (int(end_label))-1)
                    ]

                elif type == "Year":
                    modified_labels = [
                        (
                            startd_date + relativedelta(months=i)
                        ).strftime("%b %y")
                        for i in range(12)
                    ]
                if type == "Year":
                    ts = index
                else:
                    ts = int(label)

                if ts in outputDict:
                    for key, val in outputDict[ts].items():
                        if type == "Year":
                            if key == "Road":
                                result["data"]["datasets"][0]["data"][index-1] = val
                            elif key == "Rail":
                                result["data"]["datasets"][1]["data"][index-1] = val
                        else:
                            if key == "Road":
                                result["data"]["datasets"][0]["data"][index] = val
                            elif key == "Rail":
                                result["data"]["datasets"][1]["data"][index] = val

        result["data"]["labels"] = copy.deepcopy(modified_labels)
        return result

    except Exception as e:
        console_logger.debug("----- Overall Transit Error -----", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.post("/add/sap/excel", tags=["Road Map"])
async def endpoint_to_add_sap_excel_data(response: Response, file: UploadFile = File(...)):
    try:
        if file is None:
            return {"error": "No file Uploaded!"}
        
        contents = await file.read()
        if not contents:
            return {"error": "Uploaded file is empty!"}

        if file.filename.endswith(".xlsx"):
            # file saving start
            date = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{date}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)
            file_extension = file.filename.split(".")[-1]
            file_name = f'sap_manual_{datetime.datetime.now().strftime("%Y-%m-%d:%H:%M")}.{file_extension}'
            full_path = os.path.join(os.getcwd(), target_directory, file_name)
            with open(full_path, "wb") as file_object:
                file_object.write(contents)
            # file saving end

            excel_data = pd.read_excel(BytesIO(contents))
            data_excel_fetch = json.loads(excel_data.to_json(orient="records"))
            for single_data in data_excel_fetch:
                try:
                    fetchSapRecords = SapRecords.objects.get(do_no=str(single_data["Source & DO No"]))
                except DoesNotExist as e:
                    add_data_excel = SapRecords(
                        slno=single_data["Slno"],
                        source=single_data["source"],
                        mine_name=single_data["Mines Name"],
                        sap_po=str(single_data["SAP PO"]),
                        line_item=str(single_data["Line Item"]),
                        do_no=str(single_data["Source & DO No"]),
                        do_qty=str(single_data["DO QTY"]),
                    )
                    add_data_excel.save()

                # take it here
                fetchGmrData = Gmrdata.objects(arv_cum_do_number = str(single_data["Source & DO No"]))
                for single_gmr_data in fetchGmrData:
                    single_gmr_data.po_no = str(single_data["SAP PO"])
                    single_gmr_data.line_item = str(single_data["Line Item"])
                    single_gmr_data.po_qty = str(single_data["DO QTY"])
                    single_gmr_data.save()

        return {"detail": "success"}
    except KeyError as e:
        raise HTTPException(status_code=404, detail="Key Error")
    except Exception as e:
        console_logger.debug("----- Sap Excel Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

def create_geofence(route_coordinates, tolerance_meters):
    try:
        # Constants
        meters_per_degree = 111_000  # Approximate value for the distance of 1 degree of latitude in meters
        tolerance_degrees = tolerance_meters / meters_per_degree

        # # Load the route coordinates from the JSON file
        # with open(route_file, 'r') as f:
        #     route_coordinates = json.load(f)

        # Convert the route coordinates into a LineString object
        geofenced_path = LineString(route_coordinates)

        # Create a buffer around the LineString to form a Polygon
        buffered_geofence = geofenced_path.buffer(tolerance_degrees)

        # Extract the exterior coordinates of the buffered polygon
        buffered_coordinates = list(buffered_geofence.exterior.coords)

        # Save the buffered coordinates to a new JSON file
        # output_file = route_file.replace('.json', '_buffered.json')
        # with open(output_file, 'w') as out_f:
            # json.dump(buffered_coordinates, out_f, indent=2)
        # with open(f'{Mine_Name}_geofence_coordinates.json', 'w') as json_file:
        #     json.dump(buffered_coordinates, json_file, indent=2)

        # print(f"Buffered geofence coordinates saved to '{Mine_Name}_geofence_coordinates.json'")
        return buffered_coordinates
    except Exception as e:
        console_logger.debug("----- Add edit lat long Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

def fetch_geofencing_data(name, latlong):
    try:
        tolerance_meters = 100  # Set the desired tolerance in meters

        # Replace 'YOUR_API_KEY' with your actual Google Maps API key
        gmaps = googlemaps.Client(key=os.getenv("GOOGLE_MAPS_KEY"))

        Mine_Name = name        #Mine Name

        # Define the starting and ending points using the provided coordinates
        # origin = "20.6853644, 79.3062271"            #Yekona Mines
        dataLatlong = latlong.split(',')
        origin = f"{dataLatlong[0]}, {dataLatlong[1]}"            #Yekona Mines
        destination = "20.2796104, 78.9765083"       #GMR Warora Energy Limited

        # Request directions
        directions_result = gmaps.directions(origin, destination)

        # Extract the polyline points from the directions result
        if directions_result:
            steps = directions_result[0]['legs'][0]['steps']
            polyline_points = [step['polyline']['points'] for step in steps]

            # Decode the polyline points to get the latitude and longitude
            route_coordinates = []
            for polyline_point in polyline_points:
                route_coordinates.extend(polyline.decode(polyline_point))

            fetch_geofence = create_geofence(route_coordinates, tolerance_meters)
            res_geofence = [list(ele) for ele in fetch_geofence]
            # updateSchedulerData = SelectedLocation.objects(
            #     name=Mine_Name,
            # ).update(geofence=res_geofence)
            # return {"details": "data updated"}
            console_logger.debug("data updated")
            return res_geofence
        else:
            console_logger.debug("No route found.")
            return {"detail": "No data found"}

        
    except Exception as e:
        console_logger.debug("----- Add edit lat long Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

@router.post("/addeditlatlong", tags=["Map Data"])
def endpoint_to_add_lat_long(response: Response, payload: LatLongPostIn, id: str = None):
    try:
        dataName = payload.dict()
        console_logger.debug(id)
        if id:
            # selectedLocationData = SelectedLocation.objects.get(id=id)
            # selectedLocationData.name = dataName.get("name")
            # selectedLocationData.latlong = dataName.get("latlong")
            # selectedLocationData.type = dataName.get("type")
            # selectedLocationData.save()
            updateSchedulerData = SelectedLocation.objects(
                id=ObjectId(id),
            ).update(name=dataName.get("name"), latlong=dataName.get("latlong"), type=dataName.get("type"), geofence=dataName.get("geofencing"))
        else:
            selectedLocationData = SelectedLocation(name=dataName.get("name"), latlong=dataName.get("latlong"), type=dataName.get("type"), geofence=dataName.get("geofencing"))
            selectedLocationData.save()


        # updateSchedulerData = SelectedLocation.objects(
            #     name=Mine_Name,
            # ).update(geofence=res_geofence)

        # fetch_geofencing_data(dataName.get("name"), dataName.get("latlong"))

        return {"detail": "success"}
    except Exception as e:
        console_logger.debug("----- Add edit lat long Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/fetch/geofence", tags=["Map Data"])
def endpoint_to_fetch_geofence(response: Response, name: str, latlang: str):
    try:
        getGeofencing = fetch_geofencing_data(name, latlang)
        return getGeofencing
    except Exception as e:
        console_logger.debug("----- Add edit lat long Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

    

@router.get("/getlatlong", tags=["Map Data"])
def endpoint_to_fetch_lat_long(response: Response):
    try:
        listData = []
        selectedData = SelectedLocation.objects()
        for single_data in selectedData:
            listData.append(single_data.payload())
        return listData
    except Exception as e:
        console_logger.debug("----- Get Lat Long Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e
    
# @router.post("/insert/geofence/json", tags=["Map Data"])
# async def endpoint_to_update_geofence_json(response: Response, name:str, json_file: Optional[UploadFile] = File(None)):
#     try:
#         console_logger.debug(json_file)
#         if json_file is None:
#             return {"error": "No file uploaded"}
        
#         contents = await json_file.read()

#         if not contents:
#             return {"error": "Uploaded file is empty"}
        
#         if not json_file.filename.endswith('.json'):
#             return {"error": "Uploaded file is not a JSON"}

#         try:
#             # Load JSON data from the file contents
#             fileJsonData = json.loads(contents)

#             # Check if the loaded data is a list of lists with numeric values
#             if not isinstance(fileJsonData, list):
#                 raise ValueError("JSON data is not a valid list")

#             for sublist in fileJsonData:
#                 if not isinstance(sublist, list):
#                     raise ValueError("JSON data does not contain lists of lists")
#                 for value in sublist:
#                     if not isinstance(value, (int, float)):
#                         raise ValueError("JSON data contains non-numeric values")

#         except (json.JSONDecodeError, ValueError) as e:
#             return {"error": str(e)}

#         console_logger.debug(fileJsonData)

#         updateSchedulerData = SelectedLocation.objects(
#                 name=name,
#         ).update(geofence=fileJsonData)

#         return {"detail": "success"}

#     except Exception as e:
#         console_logger.debug("----- Get Lat Long Error -----",e)
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#         console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
#         console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
#         return e

@router.delete("/deletelatlong", tags=["Map Data"])
def endpoint_to_delete_lat_long(response: Response, id: str):
    try:
        selectedLocationData = SelectedLocation.objects.get(id=id)
        selectedLocationData.delete()
        return {"detail": "success"}
    except DoesNotExist as e:
        return {"detail": "No data found"}
    except Exception as e:
        console_logger.debug("----- Delete Lat Long Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/getsaprecords", tags=["Sap Data"])
def endpoint_to_fetch_sap_records(response: Response, do_no: str):
    try:
        fetchSapRecords = SapRecords.objects.get(do_no=do_no)
        return fetchSapRecords.payload()
    except DoesNotExist as e:
        return {"detail": "No data found"}
    except Exception as e:
        console_logger.debug("----- Fetch Sap Records Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.delete("/deletesaprecord", tags=["Sap Data"])
def endpoint_to_fetch_sap_records(response: Response, do_no: str):
    try:
        fetchSapRecords = SapRecords.objects.get(do_no=do_no)
        fetchSapRecords.delete()
        return {"detail": "success"}
    except Exception as e:
        console_logger.debug("----- delete Sap Records Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


def list_report_name_pdf():
    try:
        checkPdfReportName = PdfReportName.objects()
        if checkPdfReportName:
            checkPdfReportName.delete()
        report_data = [
            {
                "report_id": 1,
                "name": "daily_coal_logistic_report",
            },
        ]
        for single_report_name in report_data:
            addreportName = PdfReportName(name=single_report_name["name"])
            addreportName.save()
        return {"detail": "success"}
    except Exception as e:
        console_logger.debug("----- List Report Name Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/addreportname", tags=["PDF Report"])
def endpoint_to_add_reportname():
    try:
        list_report_name_pdf()
        return {"detail": "success"}
    except Exception as e:
        console_logger.debug("----- Add Report Name Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

@router.post("/insert/reportname", tags=["PDF Report"])
def endpoint_to_insert_reportname(response: Response, name: str):
    try:
        if name:
            addreportName = PdfReportName(name=name)
            addreportName.save()

            addOnlyReportNameinReportScheduler = ReportScheduler(report_name=name)
            addOnlyReportNameinReportScheduler.save()
        return {"detail": "success"}
    except Exception as e:
        console_logger.debug("----- Add Report Name Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/fetch/reportname", tags=["PDF Report"])
def endpoint_to_fetch_report_name():
    try:
        listData = []
        fetchAllPdfReportName = PdfReportName.objects()
        for single_report_name in fetchAllPdfReportName:
            listData.append(single_report_name.payload())
        return listData
    except Exception as e:
        console_logger.debug("----- Fetch Report Name Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/coallabtestanalysis", tags=["Coal Testing"])
def endpoint_to_fetch_report_name(response: Response,
    type: Optional[str] = "Daily",
    Month: Optional[str] = None, 
    Daily: Optional[str] = None, 
    Year: Optional[str] = None
):
    try:
        if type == "Daily":
            specified_date = datetime.datetime.strptime(Daily, "%Y-%m-%d")
            start_of_month = specified_date.replace(day=1)
            start_date = datetime.datetime.strftime(start_of_month, '%Y-%m-%d')
            end_date = datetime.datetime.strftime(specified_date, '%Y-%m-%d')
        elif type == "Week":
            specified_date = datetime.datetime.now().date()
            start_of_week = specified_date - datetime.timedelta(days=7)
            start_date = datetime.datetime.strftime(start_of_week, '%Y-%m-%d')
            end_date = datetime.datetime.strftime(specified_date, '%Y-%m-%d')
        elif type == "Month":
            date=Month
            datestructure = date.replace(" ", "").split("-")
            final_month = f"{datestructure[0]}-{str(datestructure[1]).zfill(2)}"
            start_month = f"{final_month}-01"
            startd_date = datetime.datetime.strptime(start_month, "%Y-%m-%d")
            endd_date = startd_date + datetime.timedelta(days=31)
            start_date = datetime.datetime.strftime(startd_date, '%Y-%m-%d')
            end_date = datetime.datetime.strftime(endd_date, '%Y-%m-%d')
        elif type == "Year":
            date = Year
            endd_date =f'{date}-12-31'
            startd_date = f'{date}-01-01'
            format_data = "%Y-%m-%d"
            end_date=datetime.datetime.strftime(datetime.datetime.strptime(endd_date,format_data), format_data)
            start_date=datetime.datetime.strftime(datetime.datetime.strptime(startd_date,format_data), format_data)

        # Query for CoalTesting objects
        fetchCoalTesting = CoalTesting.objects(
            receive_date__gte= datetime.datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"), receive_date__lte= datetime.datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M")
        )
        # Query for CoalTestingTrain objects
        fetchCoalTestingTrain = CoalTestingTrain.objects(
            receive_date__gte = datetime.datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"), receive_date__lte= datetime.datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M")
        )
        # If no data is found from db, return empty result
        if not fetchCoalTesting and not fetchCoalTestingTrain:
            return {"data": {"labels": [], "datasets": []}}
        else:
            rrNo_values = {}
            calorific_value = 0
            third_party_calorific_value = 0
            # Iterate through fetched CoalTesting objects
            for single_coal_testing in fetchCoalTesting:
                rrNo = single_coal_testing.rrNo
                location = single_coal_testing.location
                for param in single_coal_testing.parameters:
                    if param["parameter_Name"] == "Gross_Calorific_Value_(Arb)":
                        if param["val1"] != None and param["val1"] != "":
                            calorific_value = float(param["val1"])
                            # break

                    if param["parameter_Name"] == "Third_Party_Gross_Calorific_Value_(Arb)":
                        if param["val1"] != None and param["val1"] != "":
                            third_party_calorific_value = float(param["val1"])


                # Aggregate values based on rrNo
                if rrNo in rrNo_values:
                    rrNo_values[location]["gwel"] += int(calorific_value)
                    rrNo_values[location]["third_party"] += int(third_party_calorific_value)
                else:
                    rrNo_values[location] = {
                        "gwel": int(calorific_value),
                        "third_party": int(third_party_calorific_value) if third_party_calorific_value else 0
                    }

            # Iterate through fetched CoalTestingTrain objects
            for single_coal_testing_train in fetchCoalTestingTrain:
                rrNo = single_coal_testing_train.rrNo
                location = single_coal_testing_train.location
                for param in single_coal_testing_train.parameters:
                    if param["parameter_Name"] == "Gross_Calorific_Value_(Arb)":
                        if param["val1"] != None and param["val1"] != "":
                            calorific_value = float(param["val1"])
                            # break
                    if param["parameter_Name"] == "Third_Party_Gross_Calorific_Value_(Arb)":
                        if param["val1"] != None and param["val1"] != "":
                            third_party_calorific_value = float(param["val1"])
                            

                # Aggregate values based on rrNo
                if rrNo in rrNo_values:
                    rrNo_values[location]["gwel"] += int(calorific_value)
                    rrNo_values[location]["third_party"] += int(third_party_calorific_value)
                else:
                    rrNo_values[location] = {
                        "gwel": int(calorific_value),
                        "third_party": int(third_party_calorific_value) if third_party_calorific_value else 0
                    }
            
            # fetch data from AopTarget
            aopList = []
            fetchAopTarget = AopTarget.objects()
            if fetchAopTarget:
                for single_aop_target in fetchAopTarget:
                    aopList.append(single_aop_target.payload())


            target_dict = {item['source_name']: int(item['aop_target']) for item in aopList}
            
            aligned_target_data = [target_dict.get(label.strip(), 0) for label in rrNo_values.keys()]

            result = {
                "data": {
                    "labels": list(rrNo_values.keys()),
                    "datasets": [
                        {"label": "GWEL", "data": [data['gwel'] for data in rrNo_values.values()], "order": 1, "type": "bar"},
                        {"label": "Third Party", "data": [data['third_party'] for data in rrNo_values.values()], "order": 1, "type": "bar"},
                        {"label": "Target", "data": aligned_target_data, "order": 0, "type": "line"},
                    ],
                }
            }

            return result
        
    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/report/inventory", tags=["Road Map"])
def endpoint_to_fetch_inventory(response: Response):
    try:
        results = {
            "title": "Inventory",
            "icon": "inventory",
            "data": ""
            }
        fetchGmrData = Gmrdata.objects()
        overall_gwel = 0
        overall_historian = 0
        for single_gmr_data in fetchGmrData:
            if single_gmr_data.payload()["GWEL_Net_Wt(MT)"] != None:
                overall_gwel += float(single_gmr_data.payload()["GWEL_Net_Wt(MT)"])
        
        fetchHistorianData = Historian.objects(tagid__in=[16, 3536])

        for single_historian in fetchHistorianData:
            overall_historian += float(single_historian.payload()["sum"])

        results["data"] = round(overall_gwel - overall_historian/1000, 2)

        return results

    except Exception as e:
        console_logger.debug("----- Fetch Report Name Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e
    

@router.get("/fetch/expiry/vehicle", tags=["Reports"])
def endpoint_to_fetch_going_to_expiry_vehicle(response: Response, page_no:int=None, page_size:int=None, search_text: Optional[str]=None, type: Optional[str] = "display"):
    try:
        if type and type == "display":
            if page_no and page_size:
                skip = page_size * (page_no - 1)
                limit = page_size
            query = {}
            if search_text:
                query["$or"] = [
                    {"vehicle_number": {"$regex": f"{search_text}", "$options": "i"}},
                    {"delivery_challan_number": {"$regex": f"{search_text}", "$options": "i"}},
                ]

            today = datetime.datetime.now()
            seven_days_ago = today + datetime.timedelta(days=7)
            
            result = {"labels": [], "datasets": [], "total": 0, "page_size": 15}
            pipeline = [
                    {
                        "$addFields": {
                            "certificate_expiry_date": {
                                "$dateFromString": {
                                    "dateString": "$certificate_expiry",
                                    "format": "%d-%m-%Y"
                                }
                            }
                        }
                    },
                    {
                        "$match": {
                            "$and": [
                                {"certificate_expiry_date": {"$lte": seven_days_ago}},
                                query
                            ]
                        }
                    },
                    {
                        "$addFields": {
                            "days_to_go": {
                                "$divide": [
                                    {
                                        "$subtract": ["$certificate_expiry_date", datetime.datetime.now() + datetime.timedelta(days=-1)]
                                    },
                                    1000 * 60 * 60 * 24
                                ]
                            }
                        }
                    },
                    {
                        "$sort": {"vehicle_number": 1, "created_at": -1}
                    },
                    {
                        "$group": {
                            "_id": "$vehicle_number",
                            "latest_record": {"$first": "$$ROOT"}
                        }
                    },
                    {
                        "$project": {  
                            "latest_record": -1
                        }
                    },
                    {
                        "$replaceRoot": {"newRoot": "$latest_record"}
                    },
                    {
                        "$sort": {"created_at": -1}
                    }
                ]
            count_pipeline = pipeline.copy()
            count_pipeline.append({"$count": "total_count"})
            count_result = list(Gmrdata.objects.aggregate(count_pipeline))
            total_count = count_result[0]["total_count"] if count_result else 0
            
            if page_no and page_size:
                pipeline.append({"$skip": skip})
                pipeline.append({"$limit": limit})
        
            results = list(Gmrdata.objects.aggregate(pipeline))
            results_sorted = sorted(results, key=lambda record: record["days_to_go"], reverse=False)
            result["labels"] = ["vehicle_number", "vehicle_chassis_number", "expiry_date", "fitness_file", "days_to_go"]
            finalData = []
            for record in results_sorted:
                dictData = {}
                # dictData["sr_no"] = count
                dictData["vehicle_number"] = record["vehicle_number"]
                dictData["vehicle_chassis_number"] = record["vehicle_chassis_number"]
                dictData["expiry_date"] = record["certificate_expiry"]
                dictData["fitness_file"] = record["fitness_file"]

                days_to_go = record["days_to_go"]
                total_seconds = int(days_to_go * 24 * 60 * 60)  # Convert days to seconds
                delta = timedelta(seconds=total_seconds)

                days = delta.days
                hours, remainder = divmod(delta.seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                if days < 0:
                    dictData["days_to_go"] = 0
                else:
                    dictData["days_to_go"] = f"{days}"
                # count -= 1

                finalData.append(dictData)

            result["datasets"] = finalData
            result["total"] = total_count
            return result
        elif type and type == "download":
            del type
            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)

            today = datetime.datetime.now()
            seven_days_ago = today + datetime.timedelta(days=7)
            
            result = {"labels": [], "datasets": [], "total": 0, "page_size": 15}

            pipeline = [
                    {
                        "$addFields": {
                            "certificate_expiry_date": {
                                "$dateFromString": {
                                    "dateString": "$certificate_expiry",
                                    "format": "%d-%m-%Y"
                                }
                            }
                        }
                    },
                    {
                        "$match": {
                            "$and": [
                                {"certificate_expiry_date": {"$lte": seven_days_ago}},
                            ]
                        }
                    },
                    {
                        "$addFields": {
                            "days_to_go": {
                                "$divide": [
                                    {
                                        "$subtract": ["$certificate_expiry_date", datetime.datetime.now() + datetime.timedelta(days=-1)]
                                    },
                                    1000 * 60 * 60 * 24
                                ]
                            }
                        }
                    },
                    {
                        "$sort": {"vehicle_number": 1, "created_at": -1}
                    },
                    {
                        "$group": {
                            "_id": "$vehicle_number",
                            "latest_record": {"$first": "$$ROOT"}
                        }
                    },
                    {
                        "$project": {  
                            "latest_record": -1
                        }
                    },
                    {
                        "$replaceRoot": {"newRoot": "$latest_record"}
                    },
                    {
                        "$sort": {"created_at": -1}
                    }
                ]
            count_pipeline = pipeline.copy()
            count_pipeline.append({"$count": "total_count"})
            count_result = list(Gmrdata.objects.aggregate(count_pipeline))
            total_count = count_result[0]["total_count"] if count_result else 0
            
            if page_no and page_size:
                pipeline.append({"$skip": skip})
                pipeline.append({"$limit": limit})
            
            results = list(Gmrdata.objects.aggregate(pipeline))
            # sorting data in ascending order i.e reverse=False
            # results_sorted = sorted(results, key=lambda record: record["latest_record"]["days_to_go"], reverse=False)
            results_sorted = sorted(results, key=lambda record: record["days_to_go"], reverse=False)
            count = len(results_sorted)
            path = os.path.join(
                "static_server",
                "gmr_ai",
                file,
                "Vehicle_expiry_{}.xlsx".format(
                    datetime.datetime.now().strftime("%Y-%m-%d:%H:%M:%S"),
                ),
            )
            filename = os.path.join(os.getcwd(), path)
            workbook = xlsxwriter.Workbook(filename)
            workbook.use_zip64()
            cell_format2 = workbook.add_format()
            cell_format2.set_bold()
            cell_format2.set_font_size(10)
            cell_format2.set_align("center")
            cell_format2.set_align("vjustify")

            worksheet = workbook.add_worksheet()
            worksheet.set_column("A:AZ", 20)
            worksheet.set_default_row(50)
            cell_format = workbook.add_format()
            cell_format.set_font_size(10)
            cell_format.set_align("center")
            cell_format.set_align("vcenter")
            headers = ["sr_no", "vehicle_number", "vehicle_chassis_number", "expiry_date", "days_to_go"]
            finalData = []

            for index, header in enumerate(headers):
                worksheet.write(0, index, header, cell_format2)

            row = 1
            for record in results_sorted:
                worksheet.write(row, 0, count, cell_format)
                worksheet.write(row, 1, record["vehicle_number"])
                worksheet.write(row, 2, record["vehicle_chassis_number"])
                worksheet.write(row, 3, record["certificate_expiry"])
                # worksheet.write(row, 4, record["fitness_file"])
                

                days_to_go = record["days_to_go"]
                total_seconds = int(days_to_go * 24 * 60 * 60)  # Convert days to seconds
                delta = timedelta(seconds=total_seconds)

                days = delta.days
                hours, remainder = divmod(delta.seconds, 3600)
                minutes, seconds = divmod(remainder, 60)
                if days < 0:
                    console_logger.debug("The number is negative.")
                    worksheet.write(row, 4, 0)
                else:
                    console_logger.debug("The number is positive.")
                    worksheet.write(row, 4, days)
                count -= 1
                row += 1
            workbook.close()

            return {
                "Type": "vehicle_fitness_expiry",
                "Datatype": "Report",
                "File_Path": path,
            }

    except Exception as e:
        console_logger.debug("----- Fetch Report Name Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

@router.get("/fetch/rail", tags=["Rail Map"])
def endpoint_to_fetch_railway_data(response: Response, currentPage: Optional[int] = None, perPage: Optional[int] = None, search_text: Optional[str] = None, start_timestamp: Optional[str] = None, end_timestamp: Optional[str] = None, type: Optional[str] = "display"):
    try:
        result = {        
                "labels": [],
                "datasets": [],
                "total" : 0,
                "page_size": 15
        }
        if type and type == "display":
            page_no = 1
            page_len = result["page_size"]

            if currentPage:
                page_no = currentPage

            if perPage:
                page_len = perPage
                result["page_size"] = perPage

            data = Q()

            # based on condition for timestamp playing with & and | 
            if start_timestamp:
                start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(created_at__gte = start_date)

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M","Asia/Kolkata",False)
                data &= Q(created_at__lte = end_date)

            if search_text:
                if search_text.isdigit():
                    data &= Q(rr_no__icontains=search_text) | Q(po_no__icontains=search_text)
                else:
                    data &= (Q(source__icontains=search_text))

            offset = (page_no - 1) * page_len
            # listData = []
            logs = (
                RailData.objects(data)
                .order_by("-created_at")
                .skip(offset)
                .limit(page_len)
            )   
            if any(logs):
                for log in logs:
                    result["labels"] = list(log.simplepayload().keys())
                    result["datasets"].append(log.simplepayload())
            result["total"]= len(RailData.objects(data))
            return result
        elif type and type == "download":
            del type

            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)

            # Constructing the base for query
            data = Q()

            if start_timestamp:
                start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(created_at__gte = start_date)

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M","Asia/Kolkata",False)
                data &= Q(created_at__lte = end_date)
            
            if search_text:
                if search_text.isdigit():
                    data &= Q(arv_cum_do_number__icontains = search_text) | Q(delivery_challan_number__icontains = search_text)
                else:
                    data &= Q(vehicle_number__icontains = search_text)

            usecase_data = RailData.objects(data).order_by("-created_at")
            count = len(usecase_data)
            path = None
            if usecase_data:
                try:
                    path = os.path.join(
                        "static_server",
                        "gmr_ai",
                        file,
                        "Rail_Report_{}.xlsx".format(
                            datetime.datetime.now().strftime("%Y-%m-%d:%H:%M:%S"),
                        ),
                    )
                    filename = os.path.join(os.getcwd(), path)
                    workbook = xlsxwriter.Workbook(filename)
                    workbook.use_zip64()
                    cell_format2 = workbook.add_format()
                    cell_format2.set_bold()
                    cell_format2.set_font_size(10)
                    cell_format2.set_align("center")
                    cell_format2.set_align("vjustify")

                    worksheet = workbook.add_worksheet()
                    worksheet.set_column("A:AZ", 20)
                    worksheet.set_default_row(50)
                    cell_format = workbook.add_format()
                    cell_format.set_font_size(10)
                    cell_format.set_align("center")
                    cell_format.set_align("vcenter")
                    headers = [
                        "Sr.No",
                        "RR No",
                        "RR Qty",
                        "Po No",
                        "Po Date",
                        "Line Item",
                        "Source",
                        "Placement Date",
                        "Completion Date",
                        "Drawn Date",
                        "Total ul wt",
                        "Boxes Supplied",
                        "Total Secl Gross Wt",
                        "Total Secl Tare Wt",
                        "Total Secl Net Wt",
                        "Total Secl Ol Wt",
                        "Boxes Loaded",
                        "Total Rly Gross Wt",
                        "Total Rly_Tare Wt",
                        "Total Rly Net Wt",
                        "Total Rly Ol Wt",
                        "Total Secl Chargable Wt",
                        "Total Rly Chargable Wt",
                        "Freight",
                        "Gst",
                        "Pola",
                        "Total Freight",
                        "Source Type",
                        "Created At"
                    ]
                   
                    for index, header in enumerate(headers):
                        worksheet.write(0, index, header, cell_format2)

                    for row, query in enumerate(usecase_data, start=1):
                        result = query.simplepayload()
                        worksheet.write(row, 0, count, cell_format)     
                        worksheet.write(row, 1, str(result["rr_no"]))                      
                        worksheet.write(row, 2, str(result["rr_qty"]))                      
                        worksheet.write(row, 3, str(result["po_no"]))                      
                        worksheet.write(row, 4, str(result["po_date"]))                      
                        worksheet.write(row, 5, str(result["line_item"]))                      
                        worksheet.write(row, 6, str(result["source"]))                      
                        worksheet.write(row, 7, str(result["placement_date"]))                      
                        worksheet.write(row, 8, str(result["completion_date"]))                      
                        worksheet.write(row, 9, str(result["drawn_date"]))                      
                        worksheet.write(row, 10, str(result["total_ul_wt"]))                      
                        worksheet.write(row, 11, str(result["boxes_supplied"]))                      
                        worksheet.write(row, 12, str(result["total_secl_gross_wt"]))                      
                        worksheet.write(row, 13, str(result["total_secl_tare_wt"]))                      
                        worksheet.write(row, 14, str(result["total_secl_net_wt"]))                      
                        worksheet.write(row, 15, str(result["total_secl_ol_wt"]))                      
                        worksheet.write(row, 16, str(result["boxes_loaded"]))                      
                        worksheet.write(row, 17, str(result["total_rly_gross_wt"]))                      
                        worksheet.write(row, 18, str(result["total_rly_tare_wt"]))                      
                        worksheet.write(row, 19, str(result["total_rly_net_wt"]))                      
                        worksheet.write(row, 20, str(result["total_rly_ol_wt"]))                      
                        worksheet.write(row, 21, str(result["total_secl_chargable_wt"]))                      
                        worksheet.write(row, 22, str(result["total_rly_chargable_wt"]))                      
                        worksheet.write(row, 23, str(result["freight"]))                      
                        worksheet.write(row, 24, str(result["gst"]))                      
                        worksheet.write(row, 25, str(result["pola"]))                      
                        worksheet.write(row, 26, str(result["total_freight"]))                      
                        worksheet.write(row, 27, str(result["source_type"]))                      
                        worksheet.write(row, 28, str(result["created_at"]))                   
                        
                        count-=1
                        
                    workbook.close()
                    console_logger.debug("Successfully {} report generated".format(service_id))
                    console_logger.debug("sent data {}".format(path))

                    return {
                            "Type": "gmr_rail_journey_download_event",
                            "Datatype": "Report",
                            "File_Path": path,
                            }
                except Exception as e:
                    console_logger.debug(e)
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
                    console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
            else:
                console_logger.error("No data found")
                return {
                        "Type": "gmr_rail_journey_download_event",
                        "Datatype": "Report",
                        "File_Path": path,
                        }
    except Exception as e:
        console_logger.debug("----- Fetch Report Name Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

@router.get("/fetch/singlerail", tags=["Rail Map"])
def endpoint_to_fetch_railway_data(response: Response, rrno: str):
    try:
        fetchRailData = RailData.objects.get(rr_no=rrno)
        return fetchRailData.payload()
    except DoesNotExist as e:
        raise HTTPException(status_code=404, detail="No data found")
    except Exception as e:
        console_logger.debug("----- Fetch Report Name Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e
    
@router.get("/fetch/allminenames", tags=["Rail Map"])
def endpoint_to_fetch_rail_mines(response: Response):
    try:
        # mine_names = short_mine_collection.find({},{"coal_journey":"rail"})
        mine_names = short_mine_collection.find({})
        dictData = {}
        railData = []
        roadData = []
        for single_data in mine_names:
            # console_logger.debug(single_data)
            if single_data.get("coal_journey") == "Rail":
                railData.append(single_data.get("mine_name"))
            if single_data.get("coal_journey") == "Road":
                roadData.append(single_data.get("mine_name"))
        
        dictData["road"] = roadData
        dictData["rail"] = railData

        return dictData
        # mine_data = [doc. for doc in mine_names]
        # console_logger.debug(mine_names)
    except Exception as e:
        console_logger.debug("----- Fetch Rail Mines Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.post("/update/rail", tags=["Railway"])
def endpoint_to_insert_rail_data(response: Response, payload: RailwayData):
    try:
        # Extract data from payload
        final_data = payload.dict()

        # Fetch existing RailData document
        fetchRailData = RailData.objects.get(rr_no=final_data.get("rr_no"))

        if fetchRailData:
            # Update top-level fields in the RailData document
            for key, value in final_data.items():
                if key != 'secl_rly_data' and hasattr(fetchRailData, key):
                    setattr(fetchRailData, key, value)

            # Update secl_rly_data
            for new_data in final_data.get('secl_rly_data', []):
                updated = False
                for secl_data in fetchRailData.secl_rly_data:
                    if secl_data.wagon_no == new_data['wagon_no']:
                        for key, value in new_data.items():
                            setattr(secl_data, key, value)
                        updated = True
                        break
                if not updated:
                    fetchRailData.secl_rly_data.append(SeclRailData(**new_data))

            fetchRailData.save()
        return {"detail": "success"}
    except Exception as e:
        console_logger.debug("----- Fetch Report Name Error -----", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.post("/insert/rail", tags=["Railway"])
def endpoint_to_insert_rail_data(response: Response, payload: RailwayData, rr_no: Optional[str] = None):
    try:
        # if rr_no:
            #update

        # Extract data from payload
        final_data = payload.dict()

        # Fetch existing RailData document
        # fetchRailData = RailData.objects.get(rr_no=final_data.get("rr_no"))
        

        # if fetchRailData:
        try:
            fetchRailData = RailData.objects.get(rr_no=rr_no)
            # Update top-level fields in the RailData document
            for key, value in final_data.items():
                if key != 'secl_rly_data' and hasattr(fetchRailData, key):
                    setattr(fetchRailData, key, value)

            # Update secl_rly_data
            for new_data in final_data.get('secl_rly_data', []):
                updated = False
                for secl_data in fetchRailData.secl_rly_data:
                    if secl_data.wagon_no == new_data['wagon_no']:
                        for key, value in new_data.items():
                            setattr(secl_data, key, value)
                        updated = True
                        break
                if not updated:
                    fetchRailData.secl_rly_data.append(SeclRailData(**new_data))

            fetchRailData.save()
            return {"detail": "success"}
        except DoesNotExist as e:
            final_data = payload.dict()
            secl_list_data = []
            for single_data in final_data.get("secl_rly_data"):
                secl_rly_dict_data = {
                    "indexing": single_data.get("indexing"),
                    "wagon_owner": single_data.get("wagon_owner"),
                    "wagon_type": single_data.get("wagon_type"),
                    "wagon_no": single_data.get("wagon_no"),
                    "secl_cc_wt": single_data.get("secl_cc_wt"),
                    "secl_gross_wt": single_data.get("secl_gross_wt"),
                    "secl_tare_wt": single_data.get("secl_tare_wt"),
                    "secl_net_wt": single_data.get("secl_net_wt"),
                    "secl_ol_wt": single_data.get("secl_ol_wt"),
                    "secl_ul_wt":single_data.get("secl_ul_wt"),
                    "secl_chargable_wt": single_data.get("secl_chargable_wt"),
                    "rly_cc_wt": single_data.get("rly_cc_wt"),
                    "rly_gross_wt": single_data.get("rly_gross_wt"),
                    "rly_tare_wt": single_data.get("rly_tare_wt"),
                    "rly_net_wt": single_data.get("rly_net_wt"),
                    "rly_permissible_cc_wt": single_data.get("rly_permissible_cc_wt"),
                    "rly_ol_wt": single_data.get("rly_ol_wt"),
                    "rly_norm_rate": single_data.get("rly_norm_rate"),
                    "rly_pun_rate": single_data.get("rly_pun_rate"),
                    "rly_chargable_wt": single_data.get("rly_chargable_wt"),
                    "rly_sliding_adjustment": single_data.get("rly_sliding_adjustment"),
                }
                secl_list_data.append(secl_rly_dict_data)
            rail_data = RailData(
                rr_no=final_data.get("rr_no"),
                rr_qty=final_data.get("rr_qty"),
                po_no=final_data.get("po_no"),
                po_date=final_data.get("po_date"),
                line_item=final_data.get("line_item"),
                source=final_data.get("source"),
                placement_date=final_data.get("placement_date"),
                completion_date=final_data.get("completion_date"),
                drawn_date=final_data.get("drawn_date"),
                total_ul_wt=final_data.get("total_ul_wt"),
                boxes_supplied=final_data.get("boxes_supplied"),
                total_secl_gross_wt=final_data.get("total_secl_gross_wt"),
                total_secl_tare_wt=final_data.get("total_secl_tare_wt"),
                total_secl_net_wt=final_data.get("total_secl_net_wt"),
                total_secl_ol_wt=final_data.get("total_secl_ol_wt"),
                boxes_loaded=final_data.get("boxes_loaded"),
                total_rly_gross_wt=final_data.get("total_rly_gross_wt"),
                total_rly_tare_wt=final_data.get("total_rly_tare_wt"),
                total_rly_net_wt=final_data.get("total_rly_net_wt"),
                total_rly_ol_wt=final_data.get("total_rly_ol_wt"),
                total_secl_chargable_wt=final_data.get("total_secl_chargable_wt"),
                total_rly_chargable_wt=final_data.get("total_rly_chargable_wt"),
                freight=final_data.get("freight"),
                gst=final_data.get("gst"),
                pola=final_data.get("pola"),
                total_freight=final_data.get("total_freight"),
                source_type=final_data.get("source_type"),
                secl_rly_data=secl_list_data,
            )     
            rail_data.save()
            return {"message": "Data inserted successfully"}

    except Exception as e:
        console_logger.debug("----- Fetch Report Name Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/load_bunker_data", tags=["Coal bunker"])
def save_bunker_data(start_date: Optional[str] = None, end_date: Optional[str] = None, shift_name: Optional[str] = None):
    success = False
    try:
        global consumption_headers, proxies
        entry = UsecaseParameters.objects.first()
        historian_ip = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Consumption IP') if entry else None

        # if not end_date:
        #     end_date = (datetime.datetime.now(IST).replace(minute=00,second=00,microsecond=00).strftime("%Y-%m-%dT%H:%M:%S"))
        # if not start_date:
        #     start_date = (datetime.datetime.now(IST).replace(minute=0,second=0,microsecond=0) - datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")

        headers_data = {
            'accept': 'application/json',
        }
        params = {
            'start_date': start_date,
            'end_date': end_date,
        }
        try:
            # response = requests.request("POST", url=consumption_url, headers=consumption_headers, data=payload, proxies=proxies)
            response = requests.get(f'http://{ip}/api/v1/host/bunker_extract_data', params=params, headers=headers_data)
            data = json.loads(response.text)
            for item in data["Data"]:
                tag_id = item["Data"]["TagID"]
                unit = "Unit1" if tag_id == 15274 else "Unit2"
                sum = str(int(float(item["Data"]["SUM"])) / 1000)
                created_date = item["Data"]["CreatedDate"]
                if bunkerAnalysis.objects.filter(tagid = tag_id, created_date=created_date):
                    console_logger.debug("data there bunkerAnalysis")
                    pass
                else:
                    console_logger.debug("adding data")
                    bunkerAnalysis(
                        tagid = tag_id,
                        units = unit,
                        bunkering = sum,
                        shift_name = shift_name,
                        created_date = created_date,
                        ID = bunkerAnalysis.objects.count() + 1).save()
                
            success = "completed"
            console_logger.debug("successful")
        except requests.exceptions.Timeout:
            console_logger.debug("Request Timed Out!")
        except requests.exceptions.ConnectionError:
            console_logger.debug("Connection Error")
    
    except Exception as e:
        success = False
        console_logger.debug("----- Bunker Consumption Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        success = e
        
    finally:
        console_logger.debug(f"success:{success}")
        SchedulerResponse("save consumption data", f"{success}")
        return {"message" : "Successful"}


@router.get("/coal_bunker_graph", tags=["Coal bunker"])
def coal_bunker_graph(response:Response, type: Optional[str] = "Daily",
                              Month: Optional[str] = None, 
                              Daily: Optional[str] = None, Year: Optional[str] = None):
    try:
        data={}
        UTC_OFFSET_TIMEDELTA = datetime.datetime.utcnow() - datetime.datetime.now()

        basePipeline = [
            {
                "$match": {
                    "created_date": {
                        "$gte": None,
                    },
                },
            },
            {
                "$project": {
                    "ts": {
                        # "$hour": {"date": "$created_date", "timezone": timezone},
                        "$hour": {"date": "$created_date"},
                    },
                    "tagid": "$tagid",
                    "bunkering": "$bunkering",
                    "_id": 0
                },
            },
            {
                "$group": {
                    "_id": {
                        "ts": "$ts",
                        "tagid": "$tagid"
                    },
                    "data": {
                        "$push": "$bunkering"
                    }
                }
            },
        ]
        
        if type == "Daily":

            date=Daily
            end_date =f'{date} 23:59:59'
            start_date = f'{date} 00:00:00'
            format_data = "%Y-%m-%d %H:%M:%S"
            endd_date=datetime.datetime.strptime(end_date,format_data)
            startd_date=datetime.datetime.strptime(start_date,format_data)

            basePipeline[0]["$match"]["created_date"]["$lte"] = (endd_date)
            basePipeline[0]["$match"]["created_date"]["$gte"] = (startd_date)
            

            result = {
                "data": {
                    "labels": [str(i) for i in range(1, 25)],
                    "datasets": [
                        {"label": "Unit 1", "data": [0 for i in range(1, 25)]},             
                        {"label": "Unit 2", "data": [0 for i in range(1, 25)]},             
                    ],
                }
            }

        elif type == "Week":
            basePipeline[0]["$match"]["created_date"]["$gte"] = (
                datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                + UTC_OFFSET_TIMEDELTA
                - datetime.timedelta(days=7)
            )
            basePipeline[1]["$project"]["ts"] = {"$dayOfMonth": "$created_date"}
            result = {
                "data": {
                    "labels": [
                        (
                            basePipeline[0]["$match"]["created_date"]["$gte"]
                            + datetime.timedelta(days=i + 1)
                        ).strftime("%d")
                        for i in range(1, 8)
                    ],
                    "datasets": [
                        {"label": "Unit 1", "data": [0 for i in range(1, 8)]},              
                        {"label": "Unit 2", "data": [0 for i in range(1, 8)]},              
                    ],
                }
            }

        elif type == "Month":

            date=Month
            format_data = "%Y - %m-%d"

            start_date = f'{date}-01'
            startd_date=datetime.datetime.strptime(start_date,format_data)
            
            end_date = startd_date + relativedelta( day=31)
            end_label = (end_date).strftime("%d")

            basePipeline[0]["$match"]["created_date"]["$lte"] = (end_date)
            basePipeline[0]["$match"]["created_date"]["$gte"] = (startd_date)
            basePipeline[1]["$project"]["ts"] = {"$dayOfMonth": "$created_date"}
            result = {
                "data": {
                    "labels": [
                        (
                            basePipeline[0]["$match"]["created_date"]["$gte"]
                            + datetime.timedelta(days=i + 1)
                        ).strftime("%d")
                        for i in range(-1, (int(end_label))-1)
                    ],
                    "datasets": [
                        {"label": "Unit 1", "data": [0 for i in range(-1, (int(end_label))-1)]},        
                        {"label": "Unit 2", "data": [0 for i in range(-1, (int(end_label))-1)]},        
                    ],
                }
            }

        elif type == "Year":

            date=Year
            end_date =f'{date}-12-31 23:59:59'
            start_date = f'{date}-01-01 00:00:00'
            format_data = "%Y-%m-%d %H:%M:%S"
            endd_date=datetime.datetime.strptime(end_date,format_data)
            startd_date=datetime.datetime.strptime(start_date,format_data)

            basePipeline[0]["$match"]["created_date"]["$lte"] = (
                endd_date
            )
            basePipeline[0]["$match"]["created_date"]["$gte"] = (
                startd_date          
            )

            basePipeline[1]["$project"]["ts"] = {"$month": "$created_date"}
            result = {
                "data": {
                    "labels": [
                        (
                            basePipeline[0]["$match"]["created_date"]["$gte"]
                            + relativedelta(months=i)
                        ).strftime("%m")
                        for i in range(0, 12)
                    ],
                    "datasets": [
                        {"label": "Unit 1", "data": [0 for i in range(0, 12)]},                     
                        {"label": "Unit 2", "data": [0 for i in range(0, 12)]},                     
                    ],
                }
            }
        # console_logger.debug(basePipeline)
        output = bunkerAnalysis.objects().aggregate(basePipeline)
        outputDict = {}

        for data in output:
            if "_id" in data:
                ts = data["_id"]["ts"]
                tag_id = data["_id"]["tagid"]

                data_list = data.get('data', [])
                sum_list = []
                for item in data_list:
                    try:
                        sum_value = float(item)
                        sum_list.append(sum_value)
                    except ValueError:
                        pass
                
                if ts not in outputDict:
                    outputDict[ts] = {tag_id: sum_list}
                else:
                    if tag_id not in outputDict[ts]:
                        outputDict[ts][tag_id] = sum_list
                    else:
                        outputDict[ts][tag_id].append(sum_list)

        modified_labels = [i for i in range(1, 25)]

        for index, label in enumerate(result["data"]["labels"]):
            if type == "Week":
                modified_labels = [
                    (
                        basePipeline[0]["$match"]["created_date"]["$gte"]
                        + datetime.timedelta(days=i + 1)
                    ).strftime("%d-%m-%Y,%a")
                    for i in range(1, 8)
                ]
            
            elif type == "Month":
                modified_labels = [
                    (
                        basePipeline[0]["$match"]["created_date"]["$gte"]
                        + datetime.timedelta(days=i + 1)
                    ).strftime("%d/%m")
                    for i in range(-1, (int(end_label))-1)
                ]

            elif type == "Year":
                modified_labels = [
                    (
                        basePipeline[0]["$match"]["created_date"]["$gte"]
                        + relativedelta(months=i)
                    ).strftime("%b %y")
                    for i in range(0, 12)
                ]

            if int(label) in outputDict:
                for key, val in outputDict[int(label)].items():
                    total_sum = sum(val)
                    if key == 15274:
                        result["data"]["datasets"][0]["data"][index] = total_sum
                    if key == 15275:
                        result["data"]["datasets"][1]["data"][index] = total_sum

        result["data"]["labels"] = copy.deepcopy(modified_labels)
        console_logger.debug(f"-------- Bunker Graph Response -------- {result}")
        return result
    
    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        return e


@router.get("/coal_bunker_table", tags=["Coal bunker"])
def coal_bunker_table(response:Response, specified_date: Optional[str] = None):
    try:
        DataExecutionsHandler = DataExecutions()
        response =  DataExecutionsHandler.bunker_coal_table_email(specified_date=specified_date)
        return response
    except Exception as e:
        success = False
        console_logger.debug("----- Coal Testing Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        success = e

@router.get("/coal_bunker_analysis", tags=["Coal bunker"])
def coal_bunker_analysis(response:Response, specified_date: Optional[str] = None):
    try:
        DataExecutionsHandler = DataExecutions()
        response =  DataExecutionsHandler.bunker_coal_analysis(specified_date=specified_date)
        return response
    except Exception as e:
        success = False
        console_logger.debug("----- Coal Testing Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        success = e

@router.get("/fetch/coalbunkerdata", tags=["Coal bunker"])
def fetch_coal_bunker_data(response: Response, currentPage: Optional[int] = None,perPage: Optional[int] = None, start_timestamp: Optional[str] = None, end_timestamp: Optional[str] = None, search_text: Optional[str] = None, date: Optional[str] = None, type: Optional[str] = "display"):
    try:
        DataExecutionsHandler = DataExecutions()
        response =  DataExecutionsHandler.bunker_coal_data(currentPage=currentPage, perPage=perPage, start_timestamp=start_timestamp, end_timestamp=end_timestamp, search_text=search_text, type=type, date=date)
        return response
    except Exception as e:
        success = False
        console_logger.debug("----- Coal Bunker Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        success = e

@router.post("/update/coalbunkerdata", tags=["Coal bunker"])
def update_coal_bunker_data(response: Response, Data: BunkerAnalysisData):
    try:
        payload = Data.dict()
        console_logger.debug(payload)
        DataExecutionsHandler = DataExecutions()
        response = DataExecutionsHandler.update_coalbunker_analysis_data(payload=payload)
        return response
    except Exception as e:
        success = False
        console_logger.debug("----- Coal Testing Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        success = e

@router.get("/load_coal_bunker_data", tags=["Coal bunker"])
def extract_bunker_data(response: Response, start_date: Optional[str] = None, end_date: Optional[str] = None):
    try:
        DataExecutionsHandler = DataExecutions()
        response =  DataExecutionsHandler.fetchcoalBunkerData(start_date=start_date, end_date=end_date)
        return {"detail": "success"}
    except Exception as e:
        success = False
        console_logger.debug("----- Coal Testing Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        success = e 


@router.get("/fetch/coalbunker", tags=["Coal bunker"])
def fetch_bunker_data(response: Response, currentPage: Optional[int] = None, perPage: Optional[int] = None, search_text: Optional[str] = None, start_timestamp: Optional[str] = None, end_timestamp: Optional[str] = None, month_date: Optional[str] = None, type: Optional[str] = "display"):
    try:
        DataExecutionsHandler = DataExecutions()
        response = DataExecutionsHandler.fetchcoalBunkerDbData(currentPage=currentPage, perPage=perPage, search_text=search_text, start_timestamp=start_timestamp, end_timestamp=end_timestamp, month_date=month_date, type=type)
        return response
    except Exception as e:
        success = False
        console_logger.debug("----- Coal Testing Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        success = e 


@router.get("/fetch/testing/bunker", tags=["Vipin Data"])
def fetch_bunker_data(response: Response):
    try:
        fetchBunkerData = BunkerData.objects()
        bunker_generate_report(fetchBunkerdata=fetchBunkerData)
        return {"detail": "success"}
    except Exception as e:
        success = False
        console_logger.debug("----- Coal Testing Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        success = e 


def update_dictionary(data):
    # Check if key 2 is empty and key 3 contains a space
    if data[2] == '' and ' ' in data[3]:
        # Split the value at key 3 by the first space
        parts = data[3].split(' ', 1)
        data[2] = parts[0]
        data[3] = parts[1]
    return data


@router.post("/pdf_railway_data_upload", tags=["Extra"])
async def extract_data_railway(response: Response, pdf_upload: Optional[UploadFile] = File(None)):
    try:
        if pdf_upload is None:
            return {"error": "No file uploaded"}
        contents = await pdf_upload.read()

        # Check if the file is empty
        if not contents:
            return {"error": "Uploaded file is empty"}
        
        # Verify file format (PDF)
        if not pdf_upload.filename.endswith('.pdf'):
            return {"error": "Uploaded file is not a PDF"}

        file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
        target_directory = f"static_server/gmr_ai/{file}"
        os.umask(0)
        os.makedirs(target_directory, exist_ok=True, mode=0o777)

        file_extension = pdf_upload.filename.split(".")[-1]
        file_name = f'pdf_{datetime.datetime.now().strftime("%Y-%m-%d:%H:%M")}.{file_extension}'
        full_path = os.path.join(os.getcwd(), target_directory, file_name)
        with open(full_path, "wb") as file_object:
            file_object.write(contents)

        outdata = outbond(full_path)
        upper_data = extract_pdf_data(full_path)

        rr_no = upper_data.get('RR_NO')
        if rr_no:
            sap_record = SapRecords.objects(do_no=rr_no).first()
            if sap_record:
                upper_data.update({
                    "LINE_ITEM": sap_record.line_item,
                    "RR_Qty": sap_record.do_qty,
                    "SOURCE": sap_record.source,
                    "PO_NO": sap_record.sap_po,
                    "RR_Qty": sap_record.do_qty
                })
            else:
                upper_data.update({
                    "LINE_ITEM": None,
                    "RR_Qty":None,
                    "SOURCE":None,
                    "PO_NO":None,
                    "RR_Qty":None, 
                })

        key_mappings = {
            0: "sr_no",
            1: "wagon_owner",
            2: "wagon_type",
            3: "wagon_no",
            4: "rly_cc_wt",
            5: "rly_tare_wt",
            6: "no_of_art",
            7: "cmdt_code",
            8: "rly_gross_wt",
            9: "rly_sliding_adjustment",
            10: "dip_wt",
            11: "actl_wt",
            12: "rly_permissible_cc_wt",
            13: "rly_ol_wt",
            14: "rly_norm_rate",
            15: "rly_pun_rate",
            16: "rly_chargable_wt",
        }
        console_logger.debug(upper_data)
        # Extract the relevant records starting from the specified entry
        start_index = None
        for i, record in enumerate(outdata):
            # finding and printing starting where value start from 1
            if record.get(0) == "1":
                start_index = i
                break

        if start_index is None:
            raise ValueError("Starting record not found in the data")

        # Process and transform the records
        transformed_data = []
        for record in outdata[start_index:]:
            updated_record_data = update_dictionary(record)
            transformed_record = {key_mappings[k]: v for k, v in updated_record_data.items()}
            transformed_data.append(transformed_record)

        header_data = {
            "header_data": upper_data,
            "table_data": transformed_data,
        }

        return header_data
    except Exception as e:
        success = False
        console_logger.debug("----- Coal Testing Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        success = e


@router.get("/rail/rake/count", tags=["Rail Map"])
def daywise_rake_scanned_count(response:Response):
    try:

        DataExecutionsHandler = DataExecutions()
        response = DataExecutionsHandler.rakeScannedOutData()
        return response

    except Exception as e:
        console_logger.debug("----- Vehicle Scanned Count Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/fetch/rail/excel", tags=["excel test"])
def fetch_excel_data_rail(response:Response, start_date: str, end_date: str, filter_type: str):
    try:
        DataExecutionsHandler = DataExecutions()
        response = DataExecutionsHandler.download_coal_test_excel(start_date=start_date, end_date=end_date, filter_type=filter_type)
        return response
    except Exception as e:
        console_logger.debug("----- Vehicle Scanned Count Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

@router.get("/fetch/road/coal", tags=["excel test"])
def fetch_data_road_logistics(response:Response, specified_date: str):
    try:
        DataExecutionsHandler = DataExecutions()
        response = DataExecutionsHandler.download_road_coal_logistics(specified_date=specified_date)
        return response
    except Exception as e:
        console_logger.debug("----- Vehicle Scanned Count Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/fetch/rail/coal", tags=["excel test"])
def fetch_data_rail_logistics(response:Response, specified_date: str):
    try:
        DataExecutionsHandler = DataExecutions()
        response = DataExecutionsHandler.download_rail_coal_logistics(specified_date=specified_date)
        return response
    except Exception as e:
        console_logger.debug("----- Vehicle Scanned Count Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/bunker/report", tags=["bunker"])
def fetch_pdf_report_bunker(response:Response, sample_id: str):
    try:
        DataExecutionsHandler = DataExecutions()
        response = DataExecutionsHandler.display_pdf_report_bunker_addons(sample_id=sample_id)
        return response
    except Exception as e:
        console_logger.debug("----- Vehicle Scanned Count Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/update/scheduler/status", tags=["scheduler"])
def update_pdf_report_bunker(response:Response, scheduler_name: str, active: bool):
    try:
        DataExecutionsHandler = DataExecutions()
        response = DataExecutionsHandler.update_schheduler_status(scheduler_name=scheduler_name, active=active)
        return response
    except Exception as e:
        console_logger.debug("----- Update Scheduler Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/fetch/scheduler/status", tags=["scheduler"])
def display_pdf_report_bunker(response:Response):
    try:
        DataExecutionsHandler = DataExecutions()
        response = DataExecutionsHandler.fetch_scheduler_status()
        return response
    except Exception as e:
        console_logger.debug("----- Fetch Scheduler Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

    

# @router.get("/fetch/road/excel", tags=["excel test"])
# def fetch_excel_data_road(response:Response, start_date: str, end_date: str, filter_type: str):
#     try:
#         DataExecutionsHandler = DataExecutions()
#         response = DataExecutionsHandler.coal_test_road_excel(start_date=start_date, end_date=end_date, filter_type=filter_type)
#         return response
#     except Exception as e:
#         console_logger.debug("----- Vehicle Scanned Count Error -----",e)
#         response.status_code = 400
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#         console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
#         console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
#         return e


@router.post("/send_email", tags=["Generate_Email"])
# def generate_email(response: Response, email:EmailRequest):
def generate_email(response: Response, email:dict):
    try:
        url = f"http://{ip}/api/v1/host/send-email/"

        headers = {'Content-Type': 'application/json'}

        # payload = json.dumps(email.dict())
        payload = json.dumps(email)
        response = requests.request("POST", url, headers=headers, data=payload)
        console_logger.debug(response.status_code)
        console_logger.debug(response.text)
        response.status_code = response.status_code
        return response.json()
    
    except Exception as e:
        console_logger.debug("----- Email Generation Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.post("/truck_tare_email_alert", tags=["Generate_Email"])
def generate_truck_tare_email_alert(response: Response, data: TruckEmailTrigger):
    try:
        console_logger.debug((data.dict()))
        payload = data.dict()
        console_logger.debug(payload['details'][0]['vehicle_number'])
        reportSchedule = ReportScheduler.objects()
        if reportSchedule[6].active == False:
            console_logger.debug("scheduler is off")
            return {"detail": "scheduler is off"}
        elif reportSchedule[6].active == True:
            console_logger.debug("inside Truck Tare Difference Alert")
            response_code, fetch_email = fetch_email_data()
            if response_code == 200:
                console_logger.debug(reportSchedule[6].recipient_list)
                subject = f"Truck Tare Difference Alert Report {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                table_data = "<tr>"
                table_data += f"<td>{payload['details'][0]['vehicle_number']}</td>"
                table_data += f"<td>{payload['details'][0]['current_gwel_tare_time']}</td>"
                table_data += f"<td>{payload['details'][0]['current_gwel_tare_wt']}</td>"
                table_data += f"<td>{payload['details'][0]['min_GWEL_Tare_Wt']}</td>"
                table_data += f"<td>{payload['details'][0]['max_GWEL_Tare_Wt']}</td>"
                table_data += f"<td>{payload['details'][0]['difference']}</td>"
                table_data += "<tr>"

                body = f"""
                        <b>Truck Tare Difference Alert Report for Date: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S').strftime('%d %B %Y %H:%M:%S')}</b>
                        <br>
                        <br>
                        <!doctype html>
                        <html>
                        <head>
                            <meta charset="utf-8">
                            <title>Truck Tare Difference Alert</title>
                        </head>
                        <body>
                            <table border='1'>
                                <tr>
                                    <th>Vehicle Number</th>
                                    <th>Current GWEL Tare Time</th>
                                    <th>Current GWEL Tare Wt(MT)</th>
                                    <th>Min GWEL Tare Wt(MT)</th>
                                    <th>Max GWEL Tare Wt(MT)</th>
                                    <th>Difference(MT)</th>
                                </tr>
                                {table_data}
                            </table>
                        </body>
                        </html>"""
                checkEmailDevelopment = EmailDevelopmentCheck.objects()
                if checkEmailDevelopment[0].development == "local":
                    console_logger.debug("inside local")
                    send_email(fetch_email.get("Smtp_user"), subject, fetch_email.get("Smtp_password"), fetch_email.get("Smtp_host"), fetch_email.get("Smtp_port"), reportSchedule[6].recipient_list, body, "", reportSchedule[6].cc_list, reportSchedule[6].bcc_list)
                elif checkEmailDevelopment[0].development == "prod":
                    console_logger.debug("inside prod")
                    send_data = {
                        "sender_email": fetch_email.get("Smtp_user"),
                        "subject": subject,
                        "password": fetch_email.get("Smtp_password"),
                        "smtp_host": fetch_email.get("Smtp_host"),
                        "smtp_port": fetch_email.get("Smtp_port"),
                        "receiver_email": reportSchedule[6].recipient_list,
                        "body": body,
                        "file_path": "",
                        "cc_list": reportSchedule[6].cc_list,
                        "bcc_list": reportSchedule[6].bcc_list
                    }
                    console_logger.debug(send_data)
                    generate_email(Response, email=send_data)
        return {"detail": "success"}
    except Exception as e:
        console_logger.debug("----- Email Generation Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/road/mine_wise_vehicle_count", tags=["Road Map"])
def minewise_day_vehicle_scanned_count(response:Response, specified_time: str):
    try:
        # current_time = datetime.datetime.now(IST)
        # today = current_time.date()
        # startdate = f'{today} 00:00:00'
        # # from_ts = datetime.datetime.strptime(startdate,"%Y-%m-%d %H:%M:%S")
        # from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")
        timezone = pytz.timezone('Asia/Kolkata')
        current_time = datetime.datetime.now(timezone)
        UTC_OFFSET_TIMEDELTA = datetime.datetime.utcnow() - datetime.datetime.now()

        basePipeline = [
            {
                '$match': {
                    'created_at': {
                        '$gte': None,
                        '$lte': None,
                    }
                }
            }, {
                '$group': {
                    '_id': '$mine', 
                    'vehicle_count': {
                        '$sum': 1
                    }
                }
            }
        ]

        date = specified_time
        end_date = f'{date} 23:59:59'
        start_date = f'{date} 00:00:00'
        format_data = "%Y-%m-%d %H:%M:%S"
        endd_date = convert_to_utc_format(end_date.__str__(), format_data)
        startd_date = convert_to_utc_format(start_date.__str__(), format_data)

        basePipeline[0]["$match"]["created_at"]["$lte"] = endd_date
        basePipeline[0]["$match"]["created_at"]["$gte"] = startd_date

        output = Gmrdata.objects().aggregate(basePipeline)
        listdata = []
        for data in output:
            outputDict = {}
            console_logger.debug(data)
            outputDict['mine_name'] = data.get("_id")
            outputDict['vehicle_count'] = data.get("vehicle_count")
            listdata.append(outputDict)

        console_logger.debug(listdata)

        return listdata

    except Exception as e:
        console_logger.debug("----- Vehicle Scanned Count Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.post("/coalstocktracker", tags=["Coal Tracker"])
def endpoint_to_fetch_coal_stock_tracker(response: Response, specified_date: str):
    try:
        console_logger.debug(specified_date)

        specified_change_date = datetime.datetime.strftime(datetime.datetime.strptime(specified_date, "%Y-%m-%d"), "%d-%m-%Y")

        url = "https://gateway.grid-india.in/POSOCO/reports/1.0/WebAccessAPI/GetUtilityExternalSharedData?apikey=fdcfa9a0-3e10-45cc-ac8b-a0b076b0b21f"

        payload = json.dumps({
        "Date": specified_change_date,
        "SchdRevNo": -1,
        "UserName": "usr_GMR_WARORA",
        "UtilAcronymList": []
        })
        headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Basic dXNyX0dNUl9XQVJPUkE6V2Jlc0ludGVAMDIwNzIwMzg='
        }

        response = requests.request("POST", url, headers=headers, data=payload)

        if response.status_code == 200:
            console_logger.debug(response.text)

    except Exception as e:
        console_logger.debug("----- Vehicle Scanned Count Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.post("/geofence_email_alert", tags=["Generate_Email"])
def generate_truck_tare_email_alert(response: Response, data: geofenceEmailTrigger):
    try:
        console_logger.debug((data.dict()))
        payload = data.dict()
        reportSchedule = ReportScheduler.objects()
        if reportSchedule[7].active == False:
            console_logger.debug("scheduler is off")
            return {"detail": "scheduler is off"}
        elif reportSchedule[7].active == True:
            console_logger.debug("inside Truck Tare Difference Alert")
            response_code, fetch_email = fetch_email_data()
            if response_code == 200:
                console_logger.debug(reportSchedule[7].recipient_list)
                subject = f"Geofence Alert {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                title_data = ""
                if payload.get("geo_fence") == "outside":
                    title_data = f"<b>Vehicle Number {payload.get('vehicle_number')} is outside the geofenced area.</b>"
                elif payload.get("geo_fence") == "inside":
                    title_data = f"<b>Vehicle Number {payload.get('vehicle_number')} is inside the geofenced area.</b>"
                body = f"""
                        <b>Geofence Alert: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S').strftime('%d %B %Y %H:%M:%S')}</b>
                        <br>
                        <br>
                        <!doctype html>
                        <html>
                        <head>
                            <meta charset="utf-8">
                            <title>Geofence Alert</title>
                        </head>
                        <body>
                            {title_data}
                            <br>
                            <br>
                            <table border='1'>
                                <tr>
                                    <th>Vehicle Number</th>
                                    <th>Current Lat Long</th>
                                    <th>Map Location</th>
                                    <th>Source Location</th>
                                </tr>
                                <tr>
                                    <td>{payload.get('vehicle_number')}</td>
                                    <td>{payload.get('lat_long')}</td>
                                    <td><a href='https://maps.google.com/?q={payload.get('lat_long')}'>Location</a></td>
                                    <td>{payload.get('mine_name')}</td>
                                </tr>
                            </table>
                        </body>
                        </html>"""
                checkEmailDevelopment = EmailDevelopmentCheck.objects()
                if checkEmailDevelopment[7].development == "local":
                    console_logger.debug("inside local")
                    send_email(fetch_email.get("Smtp_user"), subject, fetch_email.get("Smtp_password"), fetch_email.get("Smtp_host"), fetch_email.get("Smtp_port"), reportSchedule[7].recipient_list, body, "", reportSchedule[7].cc_list, reportSchedule[7].bcc_list)
                elif checkEmailDevelopment[7].development == "prod":
                    console_logger.debug("inside prod")
                    send_data = {
                        "sender_email": fetch_email.get("Smtp_user"),
                        "subject": subject,
                        "password": fetch_email.get("Smtp_password"),
                        "smtp_host": fetch_email.get("Smtp_host"),
                        "smtp_port": fetch_email.get("Smtp_port"),
                        "receiver_email": reportSchedule[7].recipient_list,
                        "body": body,
                        "file_path": "",
                        "cc_list": reportSchedule[7].cc_list,
                        "bcc_list": reportSchedule[7].bcc_list
                    }
                    console_logger.debug(send_data)
                    generate_email(Response, email=send_data)
        return {"detail": "success"}
    except Exception as e:
        console_logger.debug("----- Email Generation Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.post("/insert/shift/schedule", tags=["scheduler"])
def endpoint_to_insert_shift_schedule(response: Response, data: ShiftMainData, report_name: str):
    try:
        console_logger.debug(data)
        inputData = data.dict()
        console_logger.debug(inputData)
        fetchAllScheduler = shiftScheduler.objects(report_name=report_name)
        if fetchAllScheduler:
            fetchAllScheduler.delete()

        for single_data in inputData.get("data"):
            shiftScheduler(shift_name = single_data.get('shift_name'), start_shift_time = single_data.get("start_shift_time"), end_shift_time = single_data.get("end_shift_time"), report_name=report_name).save()
        return {"details": "success"}
    except Exception as e:
        console_logger.debug("----- Email Generation Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/fetch/shift/schedule", tags=["scheduler"])
def endpoint_to_fetch_shift_schedule(response: Response):
    try:
        listData = []
        try:
            fetchShiftSchedule = shiftScheduler.objects()
            for single_schedule in fetchShiftSchedule:
                listData.append(single_schedule.payload())
            return listData
        except DoesNotExist as e:
            return {"details": "No data found"}
    except Exception as e:
        console_logger.debug("----- Email Generation Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/fetch/single/shiftschedule", tags=["scheduler"])
def endpoint_to_fetch_single_shift_schedule(response: Response, report_name: str):
    try:
        fetchSingleShiftSchedule = shiftScheduler.objects(report_name=report_name)
        listData = []
        if fetchSingleShiftSchedule:
            for singleShiftSchedule in fetchSingleShiftSchedule:
                listData.append(singleShiftSchedule.payload())
        return listData
    except Exception as e:
        console_logger.debug("----- Email Generation Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/emailtriggerplatform", tags=["email"])
def end_point_to_update_email_trigger(response: Response, development: str):
    try:
        try:
            checkEmailDevelopment = EmailDevelopmentCheck.objects()
            if checkEmailDevelopment:
                checkEmailDevelopment.delete()

                EmailDevelopmentCheck(development=development).save()

        except DoesNotExist as e:
            EmailDevelopmentCheck(development=development).save()
        
        return {"detail": "success"}
    except Exception as e:
        console_logger.debug("----- Email Trigger Platform Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/fetchemailtriggerplatform", tags=["email"])
def end_point_to_fetch_email_trigger(response: Response):
    try:
        checkEmailDevelopment = EmailDevelopmentCheck.objects()
        console_logger.debug(checkEmailDevelopment[0].development) 
        return {"detail": checkEmailDevelopment[0].development}
    except Exception as e:
        console_logger.debug("----- Email Trigger Platform Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e



def bunker_scheduler(**kwargs):
    try:

        time_format = "%H:%M"
        # Time to subtract: 5 hours and 30 minutes
        time_to_subtract = datetime.timedelta(hours=5, minutes=30)

        # fetchShiftSchedule = shiftScheduler.objects()
        # for single_shift in fetchShiftSchedule:
        #     console_logger.debug(single_shift.shift_name)
        #     console_logger.debug(single_shift.start_shift_time)
        #     console_logger.debug(single_shift.end_shift_time)
        # start_shift_time = datetime.datetime.strptime(kwargs["start_time"], time_format)
        # start_shift_time_ist = start_shift_time - time_to_subtract
        # start_shift_hh, start_shift_mm = start_shift_time_ist.strftime(time_format).split(":")
        # start_shift_hh, start_shift_mm = kwargs["start_time"].strftime(time_format).split(":")
        console_logger.debug(kwargs["start_time"])
        console_logger.debug(type(kwargs["start_time"]))
        start_shift_hh, start_shift_mm = kwargs["start_time"].split(":")
        # end_shift_time = datetime.datetime.strptime(kwargs["end_time"], time_format)
        # end_shift_time_ist = end_shift_time - time_to_subtract
        # end_shift_hh, end_shift_mm = end_shift_time_ist.strftime(time_format).split(":")
        # end_shift_hh, end_shift_mm = kwargs["end_time"].strftime(time_format).split(":")
        console_logger.debug(kwargs["end_time"])
        console_logger.debug(type(kwargs["end_time"]))
        end_shift_hh, end_shift_mm = kwargs["end_time"].split(":")

        console_logger.debug(kwargs["shift_name"])

        start_date = datetime.datetime.now().strftime("%Y-%m-%d")
        start_ddate = f"{start_date}T{start_shift_hh}:{start_shift_mm}:00"
        end_ddate = f"{start_date}T{end_shift_hh}:{end_shift_mm}:00"

        console_logger.debug(start_ddate)
        console_logger.debug(end_ddate)

        # backgroundTaskHandler.run_job(
        #     task_name=f"{single_shift.shift_name}", 
        #     func=send_report_generate, 
        #     trigger="cron", **{"day": "*", "hour": shift_hh, "minute": shift_mm}, 
        #     func_kwargs={"report_name":f"{single_shift.shift_name}, start_time"}, 
        #     func_args=[fetchShiftSchedule.shift_name],
        #     max_instances=1)
        save_bunker_data(start_ddate, end_ddate, kwargs["shift_name"])
    except Exception as e:
        console_logger.debug("----- Email Generation Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e





#  x------------------------------    Scheduler To Tigger Coal API's    ------------------------------------x


params = UsecaseParameters.objects.first()


if not params:
    testing_hr, testing_min = "00", "00"
    consumption_hr, consumption_min = "00", "00"

if params:
    testing_scheduler = None
    try:
        gmr_dict = params.Parameters.get('gmr_api', {})
        roi_dict = gmr_dict.get('roi1', {})
        testing_dict = roi_dict.get('Coal Testing Scheduler', {})
        if testing_dict:
            testing_scheduler = testing_dict.get("time")
    except AttributeError:
        console_logger.error("Error accessing nested dictionary for testing_scheduler.")
        testing_scheduler = None

    if testing_scheduler is not None:
        console_logger.debug(f"---- Coal Testing Schedular ----         {testing_scheduler}")
        testing_hr, testing_min = testing_scheduler.split(":")

    # consumption_scheduler = None
    # try:
    #     gmr_dict = params.Parameters.get('gmr_api', {})
    #     roi_dict = gmr_dict.get('roi1', {})
    #     consumption_dict = roi_dict.get('Coal Consumption Scheduler', {})
    #     if consumption_dict:
    #         consumption_scheduler = consumption_dict.get("time")
    # except AttributeError:
    #     console_logger.error("Error accessing nested dictionary for consumption_scheduler.")
    #     consumption_scheduler = None

    # if consumption_scheduler is not None:
    #     console_logger.debug(f"---- Coal Consumption Schedular ----     {consumption_scheduler}")
    #     consumption_hr, consumption_min = consumption_scheduler.split(":")


console_logger.debug(f"---- Coal Testing Hr ----          {testing_hr}")
console_logger.debug(f"---- Coal Testing Min ----         {testing_min}")


backgroundTaskHandler.run_job(task_name="save consumption data",
                                func=extract_historian_data,
                                trigger="cron",
                                **{"day": "*", "hour": "*", "minute": 0})

backgroundTaskHandler.run_job(task_name="save testing data",
                                func=coal_test,
                                trigger="cron",
                                **{"day": "*", "hour": testing_hr, "minute": testing_min})

# Time format for parsing and formatting time
time_format = "%H:%M"
# Time to subtract: 5 hours and 30 minutes
time_to_subtract = datetime.timedelta(hours=5, minutes=30)
fetchShiftSchedule = shiftScheduler.objects(report_name="bunker_db_schedule")
for single_shift in fetchShiftSchedule:
    console_logger.debug(single_shift.shift_name)
    console_logger.debug(single_shift.start_shift_time)
    console_logger.debug(single_shift.end_shift_time)
    # Parse end_shift_time
    end_shift_time = datetime.datetime.strptime(single_shift.end_shift_time, time_format)
    # Adjust for timezone by subtracting the specified duration
    end_shift_time_ist = end_shift_time - time_to_subtract
    # Convert the adjusted time back to hours and minutes
    end_shift_hh, end_shift_mm = end_shift_time_ist.strftime(time_format).split(":")
    # Schedule the background task
    backgroundTaskHandler.run_job(
        task_name=single_shift.shift_name,
        func=bunker_scheduler,
        trigger="cron",
        **{"day": "*", "hour": end_shift_hh, "minute": end_shift_mm}, 
        func_kwargs={
            "shift_name": single_shift.shift_name, 
            "start_time": single_shift.start_shift_time, 
            "end_time": single_shift.end_shift_time
        }
    )                              




if __name__ == "__main__":
    usecase_handler_object.handler.run(ip=server_ip, port=server_port)
    usecase_handler_object.handler.send_status(True)
    pre_processing()
    import uvicorn
    uvicorn.run("main:router",reload=True, host="0.0.0.0",port=7704)
    # sched.add_job(scheduled_job, "interval", seconds=10)
    # sched.start()