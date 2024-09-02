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
from helpers.coal_consumption_report import generate_report_consumption
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
import PyPDF3
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

from PyPDF2 import PdfReader

from pdfminer.high_level import extract_text
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from io import StringIO

from requests.auth import HTTPBasicAuth


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
    

# @router.post("/pdf_data_upload", tags=["Extra"])
# async def extract_data_from_mahabal_pdf(response: Response, pdf_upload: Optional[UploadFile] = File(None)):
#     try:
#         if pdf_upload is None:
#             return {"error": "No file uploaded"}
#         contents = await pdf_upload.read()

#         # Check if the file is empty
#         if not contents:
#             return {"error": "Uploaded file is empty"}
        
#         # Verify file format (PDF)
#         if not pdf_upload.filename.endswith('.pdf'):
#             return {"error": "Uploaded file is not a PDF"}

#         file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
#         target_directory = f"static_server/gmr_ai/{file}"
#         os.umask(0)
#         os.makedirs(target_directory, exist_ok=True, mode=0o777)

#         file_extension = pdf_upload.filename.split(".")[-1]
#         file_name = f'pdf_{datetime.datetime.now().strftime("%Y-%m-%d:%H:%M")}.{file_extension}'
#         full_path = os.path.join(os.getcwd(), target_directory, file_name)
#         with open(full_path, "wb") as file_object:
#             file_object.write(contents)
        
#         pdfReader = PyPDF2.PdfReader(full_path)
#         totalPages = len(pdfReader.pages)
        
#         listData = []
#         id = None
        
#         list_data = []
#         for page in range(1,totalPages+1):
#             rrLot = mahabal_rr_lot(full_path, page)
#             ulrData = mahabal_ulr(full_path, page)
#             parameterData = mahabal_parameter(full_path, page)

#             console_logger.debug(rrLot)
#             console_logger.debug(ulrData)
#             console_logger.debug(parameterData)

#             api_data = {
#                 "Total_Moisture_%": None,
#                 "Inherent_Moisture_(Adb)_%": None,
#                 "Ash_(Adb)_%": None,
#                 "Volatile_Matter_(Adb)_%": None,
#                 "Gross_Calorific_Value_(Adb)_Kcal/Kg": None,
#                 "Ash_(Arb)_%": None,
#                 "Volatile_Matter_(Arb)_%": None,
#                 "Fixed_Carbon_(Arb)_%": None,
#                 "Gross_Calorific_Value_(Arb)_Kcal/Kg": None,
#                 "DO_No": None,
#                 "Lot_No": None, 
#                 "RR_No": None,
#             }
#             pdf_data = {
#                 "Third_Party_Total_Moisture_%": None,
#                 "Third_Party_Total_Moisture(adb)_%": None,
#                 "Third_Party_Inherent_Moisture_(Adb)_%": None,
#                 "Third_Party_Inherent_Moisture_(Arb)_%": None,
#                 "Third_Party_Ash_(Adb)_%": None,
#                 "Third_Party_Volatile_Matter_(Adb)_%": None,
#                 "Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg": None,
#                 "Third_Party_Ash_(Arb)_%": None,
#                 "Third_Party_Volatile_Matter_(Arb)_%": None,
#                 "Third_Party_Fixed_Carbon_(Arb)_%": None,
#                 "Third_Party_Fixed_Carbon_(Adb)_%": None,
#                 "Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg": None,
#                 "Third_Party_Report_No": None,
#             }

        
#             # rail data
#             if rrLot != None and parameterData != None:
#                 if rrLot.get("rake") != None and rrLot.get("rr") != None:
#                     try:
#                         # coalTrainData = CoalTestingTrain.objects.get(rake_no=f"{int(rrLot.get('rake'))}", rrNo=rrLot.get("rr"))
#                         querysetTrain = CoalTestingTrain.objects.filter(rake_no=f"{int(rrLot.get('rake'))}", rrNo=rrLot.get("rr"))
#                         if querysetTrain.count() == 0:
#                             console_logger.debug("no data available")
#                             continue
#                         if querysetTrain.count() == 1:
#                             coalTrainData = querysetTrain.get()
#                         else:
#                             coalTrainData = querysetTrain.first() 
#                         id = str(coalTrainData.id)
#                         if coalTrainData.rrNo:
#                             api_data["RR_No"] = coalTrainData.rrNo
#                         if coalTrainData.rake_no:
#                             api_data["Lot_No"] = coalTrainData.rake_no
#                         for single_data in coalTrainData.parameters:
#                             if single_data.get("parameter_Name") == "Total_Moisture":
#                                 api_data["Total_Moisture_%"] = single_data.get("val1")
#                             elif single_data.get("parameter_Name") == "Inherent_Moisture_(Adb)":
#                                 api_data["Inherent_Moisture_(Adb)_%"] = single_data.get("val1")
#                             elif single_data.get("parameter_Name") == "Ash_(Adb)":
#                                 api_data["Ash_(Adb)_%"] = single_data.get("val1")
#                             elif single_data.get("parameter_Name") == "Volatile_Matter_(Adb)":
#                                 api_data["Volatile_Matter_(Adb)_%"] = single_data.get("val1")
#                             elif single_data.get("parameter_Name") == "Gross_Calorific_Value_(Adb)":
#                                 api_data["Gross_Calorific_Value_(Adb)_Kcal/Kg"] = single_data.get(
#                                     "val1"
#                                 )
#                             elif single_data.get("parameter_Name") == "Ash_(Arb)":
#                                 api_data["Ash_(Arb)_%"] = single_data.get("val1")
#                             elif single_data.get("parameter_Name") == "Volatile_Matter_(Arb)":
#                                 api_data["Volatile_Matter_(Arb)_%"] = single_data.get("val1")
#                             elif single_data.get("parameter_Name") == "Fixed_Carbon_(Arb)":
#                                 api_data["Fixed_Carbon_(Arb)_%"] = single_data.get("val1")
#                             elif single_data.get("parameter_Name") == "Gross_Calorific_Value_(Arb)":
#                                 api_data["Gross_Calorific_Value_(Arb)_Kcal/Kg"] = single_data.get(
#                                     "val1"
#                                 )
#                     except DoesNotExist as e:
#                         pass
                    
#                     if ulrData.get("report_no"):
#                         pdf_data["Third_Party_Report_No"]= ulrData.get("report_no")
                    
#                     for key, value in parameterData.items():
#                         if value != '-':
#                             if key == 'total_moisture_adb':
#                                 pdf_data["Third_Party_Total_Moisture(adb)_%"] = value
#                             elif key == 'total_moisture_arb':
#                                 pdf_data["Third_Party_Total_Moisture_%"] = value
#                             elif key == 'moisture_inherent_adb':
#                                 pdf_data["Third_Party_Inherent_Moisture_(Adb)_%"] = value
#                             elif key == 'moisture_inherent_arb':
#                                 pdf_data["Third_Party_Inherent_Moisture_(Arb)_%"] = value
#                             elif key == "ash_adb":
#                                 pdf_data["Third_Party_Ash_(Adb)_%"] = value
#                             elif key == "ash_arb":
#                                 pdf_data["Third_Party_Ash_(Arb)_%"] = value
#                             elif key == "volatile_adb":
#                                 pdf_data["Third_Party_Volatile_Matter_(Adb)_%"] = value
#                             elif key == "volatile_arb":
#                                 pdf_data["Third_Party_Volatile_Matter_(Arb)_%"] = value
#                             elif key == "fixed_carbon_adb":
#                                 pdf_data["Third_Party_Fixed_Carbon_(Adb)_%"] = value
#                             elif key == "fixed_carbon_arb":
#                                 pdf_data["Third_Party_Fixed_Carbon_(Arb)_%"] = value
#                             elif key == "gross_calorific_adb":
#                                 pdf_data["Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"] = value
#                             elif key == "gross_calorific_arb":
#                                 pdf_data["Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"] = value
#                     # dataDict = {"id": id, "api_data": api_data, "pdf_data": pdf_data}
#                     list_data.append({"id": id, "api_data": api_data, "pdf_data": pdf_data})
#                     # return list_data 
#                 # road data
#                 elif rrLot.get("lot") != None and rrLot.get("do") != None:
#                     try:
#                         # queryset = CoalTesting.objects.get(rake_no=f'LOT-{rrLot.get("lot")}', rrNo=rrLot.get("do"))
#                         queryset = CoalTesting.objects.filter(rake_no=f'LOT-{rrLot.get("lot")}', rrNo=rrLot.get("do"))
#                         if queryset.count() == 0:
#                             console_logger.debug("no data available")
#                             continue
#                         if queryset.count() == 1:
#                             coalRoadData = queryset.get()
#                         else:
#                             coalRoadData = queryset.first() 
#                         id = str(coalRoadData.id)
#                         if coalRoadData.rrNo:
#                             api_data["DO_No"] = coalRoadData.rrNo
#                         if coalRoadData.rake_no:
#                             api_data["Lot_No"] = coalRoadData.rake_no
#                         for single_data in coalRoadData.parameters:
#                             if single_data.get("parameter_Name") == "Total_Moisture":
#                                 api_data["Total_Moisture_%"] = single_data.get("val1")
#                             elif single_data.get("parameter_Name") == "Inherent_Moisture_(Adb)":
#                                 api_data["Inherent_Moisture_(Adb)_%"] = single_data.get("val1")
#                             elif single_data.get("parameter_Name") == "Ash_(Adb)":
#                                 api_data["Ash_(Adb)_%"] = single_data.get("val1")
#                             elif single_data.get("parameter_Name") == "Volatile_Matter_(Adb)":
#                                 api_data["Volatile_Matter_(Adb)_%"] = single_data.get("val1")
#                             elif single_data.get("parameter_Name") == "Gross_Calorific_Value_(Adb)":
#                                 api_data["Gross_Calorific_Value_(Adb)_Kcal/Kg"] = single_data.get(
#                                     "val1"
#                                 )
#                             elif single_data.get("parameter_Name") == "Ash_(Arb)":
#                                 api_data["Ash_(Arb)_%"] = single_data.get("val1")
#                             elif single_data.get("parameter_Name") == "Volatile_Matter_(Arb)":
#                                 api_data["Volatile_Matter_(Arb)_%"] = single_data.get("val1")
#                             elif single_data.get("parameter_Name") == "Fixed_Carbon_(Arb)":
#                                 api_data["Fixed_Carbon_(Arb)_%"] = single_data.get("val1")
#                             elif single_data.get("parameter_Name") == "Gross_Calorific_Value_(Arb)":
#                                 api_data["Gross_Calorific_Value_(Arb)_Kcal/Kg"] = single_data.get(
#                                     "val1"
#                                 )
#                     except DoesNotExist as e:
#                         pass

#                     if ulrData.get("report_no"):
#                         pdf_data["Third_Party_Report_No"]= ulrData.get("report_no")
                    
#                     for key, value in parameterData.items():
#                         if value != '-':
#                             if key == 'total_moisture_adb':
#                                 pdf_data["Third_Party_Total_Moisture(adb)_%"] = value
#                             elif key == 'total_moisture_arb':
#                                 pdf_data["Third_Party_Total_Moisture_%"] = value
#                             elif key == 'moisture_inherent_adb':
#                                 pdf_data["Third_Party_Inherent_Moisture_(Adb)_%"] = value
#                             elif key == 'moisture_inherent_arb':
#                                 pdf_data["Third_Party_Inherent_Moisture_(Arb)_%"] = value
#                             elif key == "ash_adb":
#                                 pdf_data["Third_Party_Ash_(Adb)_%"] = value
#                             elif key == "ash_arb":
#                                 pdf_data["Third_Party_Ash_(Arb)_%"] = value
#                             elif key == "volatile_adb":
#                                 pdf_data["Third_Party_Volatile_Matter_(Adb)_%"] = value
#                             elif key == "volatile_arb":
#                                 pdf_data["Third_Party_Volatile_Matter_(Arb)_%"] = value
#                             elif key == "fixed_carbon_adb":
#                                 pdf_data["Third_Party_Fixed_Carbon_(Adb)_%"] = value
#                             elif key == "fixed_carbon_arb":
#                                 pdf_data["Third_Party_Fixed_Carbon_(Arb)_%"] = value
#                             elif key == "gross_calorific_adb":
#                                 pdf_data["Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"] = value
#                             elif key == "gross_calorific_arb":
#                                 pdf_data["Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"] = value
#                     # dataDict = {"id": id, "api_data": api_data, "pdf_data": pdf_data}
#                     list_data.append({"id": id, "api_data": api_data, "pdf_data": pdf_data})
#             else:
#                 console_logger.debug("data not found")  
#         return list_data      
#     except DoesNotExist as e:
#         console_logger.debug("No matching object found.")
#         return HTTPException(status_code="404", detail="No matching object found in db")
#     except MultipleObjectsReturned:
#         pass
#     #     console_logger.debug("multiple entry found for single rrno/dono")
#     #     return HTTPException(status_code="400", detail="multiple entry found for single rrno/dono")
#     except Exception as e:
#         console_logger.debug("----- Excel error -----", e)
#         response.status_code = 400
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#         console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
#         console_logger.debug(
#             "Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno)
#         )
#         return e


def extract_text_by_page(pdf_path):
    with open(pdf_path, "rb") as fh:
        for page in PDFPage.get_pages(fh, caching=True, check_extractable=True):
            resource_manager = PDFResourceManager()
            fake_file_handle = StringIO()
            converter = TextConverter(
                resource_manager, fake_file_handle, laparams=LAParams()
            )
            page_interpreter = PDFPageInterpreter(resource_manager, converter)
            page_interpreter.process_page(page)
            text = fake_file_handle.getvalue()
            yield text
            converter.close()
            fake_file_handle.close()


def extract_report_no(text):
    pattern = re.compile(r"Report\s+No\.?\s*:?\s*((?:ULR\s+No\.?\s*)?(\S+))")
    match = pattern.search(text)
    if match:
        full_match = match.group(1)
        potential_report_no = match.group(2)
        if "ULR No" in full_match:
            return potential_report_no
        elif not potential_report_no.startswith("ULR"):
            return potential_report_no
    return None


def extract_and_standardize_date(text):
    # Patterns to match various date formats
    patterns = [
        r"Date\s*:?\s*(\d{1,2}[\s.]\d{1,2}[\s.]\d{4})",  # Matches "15 06 2024" and "05.12.2010"
        r"Date\s*:?\s*(\d{2}\.\d{2}\.\d{4})",  # Matches "15.06.2024"
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            date_str = match.group(1)
            # Standardize the date format
            parts = re.split(r"[\s.]", date_str)
            return f"{parts[0].zfill(2)}.{parts[1].zfill(2)}.{parts[2]}"

    return None


def extract_data_from_text(text):
    # print(text)
    extracted_data = {}
    patterns = {
        # "date": re.compile(r"Date\.?\s*:?\s*(\d{2}\.\d{2}\.\d{4})"),
        "lot": re.compile(r"Lot\-\s*(\d+)"),
        "do": re.compile(r"D[O0]\s+(\d+)"),
        "rake": re.compile(r"Rake\s+(?:No\.?\s+)?(\d+)", re.IGNORECASE),
        "rr": re.compile(r"RR\s*.*?(\d+)", re.DOTALL),
    }

    result_section_pattern = r"Discipline:.*?(?=END\s+OF\s+REPORT)"
    result_section = re.search(result_section_pattern, text, re.DOTALL)

    extracted_data["report_no"] = extract_report_no(text)
    extracted_data["date"] = extract_and_standardize_date(text)

    for key, pattern in patterns.items():
        match = pattern.search(text)
        if match:
            extracted_data[key] = match.group(1)
            if result_section:
                results_text = result_section.group(0)

                exclude_pattern = r"IS\s+\d{4}\s*\(Part\s+[IVXLCDM|]+\)\s*:\s*\d{4}"
                cleaned_text = re.sub(exclude_pattern, "", results_text)

                values_pattern = r"\b\d+\.\d+|\b\d+\b"
                values = re.findall(values_pattern, cleaned_text)

                filtered_values = [
                    value
                    for value in values
                    if value not in ["1", "2", "3", "4", "5", "6"]
                ]

                new_filter = filtered_values[:10]
                extracted_data["total_moisture_adb"] = "-"
                extracted_data["moisture_inherent_adb"] = new_filter[0]
                extracted_data["ash_adb"] = new_filter[1]
                extracted_data["volatile_adb"] = new_filter[2]
                extracted_data["fixed_carbon_adb"] = new_filter[3]
                extracted_data["gross_calorific_adb"] = new_filter[4]
                extracted_data["total_moisture_arb"] = new_filter[5]
                extracted_data["moisture_inherent_arb"] = "-"
                extracted_data["ash_arb"] = new_filter[6]
                extracted_data["volatile_arb"] = new_filter[7]
                extracted_data["fixed_carbon_arb"] = new_filter[8]
                extracted_data["gross_calorific_arb"] = new_filter[9]

    return extracted_data


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
        
        extracted_data_per_page = []
        for page_number, text in enumerate(extract_text_by_page(full_path), start=1):
            extracted_data = extract_data_from_text(text)
            if extracted_data:
                extracted_data["page"] = page_number
                extracted_data_per_page.append(extracted_data)
        listData = []
        for data in extracted_data_per_page:
            dictData = {}
            # dictData[f"page_{data['page']}"] = {}
            print(f"Page {data['page']}:")
            for key, value in data.items():
                if key != "page":
                    dictData[key] = value
            listData.append(dictData)

        # console_logger.debug(listData)
        mainListData = []
        for single_list in listData:
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
            if "rake" in single_list:
                try:
                    # coalTrainData = CoalTestingTrain.objects.get(rake_no=f"{int(rrLot.get('rake'))}", rrNo=rrLot.get("rr"))
                    querysetTrain = CoalTestingTrain.objects.filter(rake_no=f"{int(single_list.get('rake'))}", rrNo=single_list.get('rr'))
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
                        if single_data.get("parameter_Name") == "Inherent_Moisture_(Adb)":
                            api_data["Inherent_Moisture_(Adb)_%"] = single_data.get("val1")
                        if single_data.get("parameter_Name") == "Ash_(Adb)":
                            api_data["Ash_(Adb)_%"] = single_data.get("val1")
                        if single_data.get("parameter_Name") == "Volatile_Matter_(Adb)":
                            api_data["Volatile_Matter_(Adb)_%"] = single_data.get("val1")
                        if single_data.get("parameter_Name") == "Gross_Calorific_Value_(Adb)":
                            api_data["Gross_Calorific_Value_(Adb)_Kcal/Kg"] = single_data.get(
                                "val1"
                            )
                        if single_data.get("parameter_Name") == "Ash_(Arb)":
                            api_data["Ash_(Arb)_%"] = single_data.get("val1")
                        if single_data.get("parameter_Name") == "Volatile_Matter_(Arb)":
                            api_data["Volatile_Matter_(Arb)_%"] = single_data.get("val1")
                        if single_data.get("parameter_Name") == "Fixed_Carbon_(Arb)":
                            api_data["Fixed_Carbon_(Arb)_%"] = single_data.get("val1")
                        if single_data.get("parameter_Name") == "Gross_Calorific_Value_(Arb)":
                            api_data["Gross_Calorific_Value_(Arb)_Kcal/Kg"] = single_data.get(
                                "val1"
                            )
                except DoesNotExist as e:
                    continue
                
                if single_list.get("report_no"):
                    pdf_data["Third_Party_Report_No"]= single_list.get("report_no")
                
                # for key, value in parameterData.items():
                #     if value != '-':
                if 'total_moisture_adb' in single_list:
                    pdf_data["Third_Party_Total_Moisture(adb)_%"] = single_list["total_moisture_adb"]
                if 'total_moisture_arb' in single_list:
                    pdf_data["Third_Party_Total_Moisture_%"] = single_list["total_moisture_arb"]
                if 'moisture_inherent_adb' in single_list:
                    pdf_data["Third_Party_Inherent_Moisture_(Adb)_%"] = single_list["moisture_inherent_adb"]
                if 'moisture_inherent_arb' in single_list:
                    pdf_data["Third_Party_Inherent_Moisture_(Arb)_%"] = single_list["moisture_inherent_arb"]
                if "ash_adb" in single_list:
                    pdf_data["Third_Party_Ash_(Adb)_%"] = single_list["ash_adb"]
                if "ash_arb" in single_list:
                    pdf_data["Third_Party_Ash_(Arb)_%"] = single_list["ash_arb"]
                if "volatile_adb" in single_list:
                    pdf_data["Third_Party_Volatile_Matter_(Adb)_%"] = single_list["volatile_adb"]
                if "volatile_arb" in single_list:
                    pdf_data["Third_Party_Volatile_Matter_(Arb)_%"] = single_list["volatile_arb"]
                if "fixed_carbon_adb" in single_list:
                    pdf_data["Third_Party_Fixed_Carbon_(Adb)_%"] = single_list["fixed_carbon_adb"]
                if "fixed_carbon_arb" in single_list:
                    pdf_data["Third_Party_Fixed_Carbon_(Arb)_%"] = single_list["fixed_carbon_arb"]
                if "gross_calorific_adb" in single_list:
                    pdf_data["Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"] = single_list["gross_calorific_adb"]
                if "gross_calorific_arb" in single_list:
                    pdf_data["Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"] = single_list["gross_calorific_arb"]
                # dataDict = {"id": id, "api_data": api_data, "pdf_data": pdf_data}
                mainListData.append({"id": id, "api_data": api_data, "pdf_data": pdf_data})
            #     # return list_data 
                # console_logger.debug(mainListData)
            # road data
            elif "lot" in single_list:
                try:
                    # queryset = CoalTesting.objects.get(rake_no=f'LOT-{rrLot.get("lot")}', rrNo=rrLot.get("do"))
                    queryset = CoalTesting.objects.filter(rake_no=f'LOT-{single_list.get("lot")}', rrNo=single_list.get("do"))
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
                        if single_data.get("parameter_Name") == "Inherent_Moisture_(Adb)":
                            api_data["Inherent_Moisture_(Adb)_%"] = single_data.get("val1")
                        if single_data.get("parameter_Name") == "Ash_(Adb)":
                            api_data["Ash_(Adb)_%"] = single_data.get("val1")
                        if single_data.get("parameter_Name") == "Volatile_Matter_(Adb)":
                            api_data["Volatile_Matter_(Adb)_%"] = single_data.get("val1")
                        if single_data.get("parameter_Name") == "Gross_Calorific_Value_(Adb)":
                            api_data["Gross_Calorific_Value_(Adb)_Kcal/Kg"] = single_data.get(
                                "val1"
                            )
                        if single_data.get("parameter_Name") == "Ash_(Arb)":
                            api_data["Ash_(Arb)_%"] = single_data.get("val1")
                        if single_data.get("parameter_Name") == "Volatile_Matter_(Arb)":
                            api_data["Volatile_Matter_(Arb)_%"] = single_data.get("val1")
                        if single_data.get("parameter_Name") == "Fixed_Carbon_(Arb)":
                            api_data["Fixed_Carbon_(Arb)_%"] = single_data.get("val1")
                        if single_data.get("parameter_Name") == "Gross_Calorific_Value_(Arb)":
                            api_data["Gross_Calorific_Value_(Arb)_Kcal/Kg"] = single_data.get(
                                "val1"
                            )
                except DoesNotExist as e:
                    console_logger.debug("data not there")
                    continue

                if "report_no" in single_list:
                    pdf_data["Third_Party_Report_No"]= single_list.get("report_no")
                
                if 'total_moisture_adb' in single_list:
                    pdf_data["Third_Party_Total_Moisture(adb)_%"] = single_list["total_moisture_adb"]
                if 'total_moisture_arb' in single_list:
                    pdf_data["Third_Party_Total_Moisture_%"] = single_list["total_moisture_arb"]
                if 'moisture_inherent_adb' in single_list:
                    pdf_data["Third_Party_Inherent_Moisture_(Adb)_%"] = single_list["moisture_inherent_adb"]
                if 'moisture_inherent_arb' in single_list:
                    pdf_data["Third_Party_Inherent_Moisture_(Arb)_%"] = single_list["moisture_inherent_arb"]
                if "ash_adb" in single_list:
                    pdf_data["Third_Party_Ash_(Adb)_%"] = single_list["ash_adb"]
                if "ash_arb" in single_list:
                    pdf_data["Third_Party_Ash_(Arb)_%"] = single_list["ash_arb"]
                if "volatile_adb" in single_list:
                    pdf_data["Third_Party_Volatile_Matter_(Adb)_%"] = single_list["volatile_adb"]
                if "volatile_arb" in single_list:
                    pdf_data["Third_Party_Volatile_Matter_(Arb)_%"] = single_list["volatile_arb"]
                if "fixed_carbon_adb" in single_list:
                    pdf_data["Third_Party_Fixed_Carbon_(Adb)_%"] = single_list["fixed_carbon_adb"]
                if "fixed_carbon_arb" in single_list:
                    pdf_data["Third_Party_Fixed_Carbon_(Arb)_%"] = single_list["fixed_carbon_arb"]
                if "gross_calorific_adb" in single_list:
                    pdf_data["Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"] = single_list["gross_calorific_adb"]
                if "gross_calorific_arb" in single_list:
                    pdf_data["Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"] = single_list["gross_calorific_arb"]
                # dataDict = {"id": id, "api_data": api_data, "pdf_data": pdf_data}
                mainListData.append({"id": id, "api_data": api_data, "pdf_data": pdf_data})
                # console_logger.debug(mainListData)

        return mainListData      
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

# @router.get("/load_historian_data", tags=["Coal Consumption"])                                    # coal consumption
# def extract_historian_data(start_date: Optional[str] = None, end_date: Optional[str] = None):
#     success = False
#     try:
#         global consumption_headers, proxies
#         entry = UsecaseParameters.objects.first()
#         historian_ip = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Consumption IP') if entry else None
#         historian_timer = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Consumption Duration') if entry else None

#         console_logger.debug(f"---- Coal Consumption IP ----        {historian_ip}")

#         if not end_date:
#             end_date = (datetime.datetime.now(IST).replace(minute=00,second=00,microsecond=00).strftime("%Y-%m-%dT%H:%M:%S"))
#         if not start_date:
#             start_date = (datetime.datetime.now(IST).replace(minute=0,second=0,microsecond=0) - datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")

#         console_logger.debug(f" --- Consumption Start Date --- {start_date}")
#         console_logger.debug(f" --- Consumption End Date --- {end_date}")

#         payload = json.dumps({
#                     "StartTime": start_date,
#                     "EndTime": end_date, 
#                     "RetrievalType": "Aggregate", 
#                     "RetrievalMode": "History", 
#                     "TagID": ["2","3538","16","3536"],
#                     "RetrieveBy": "ID"
#                     })
        
#         consumption_url = f"http://{historian_ip}/api/REST/HistoryData/LoadTagData"
#         # consumption_url = "http://10.100.12.28:8093/api/REST/HistoryData/LoadTagData"
#         try:
#             response = requests.request("POST", url=consumption_url, headers=consumption_headers, data=payload, proxies=proxies)
#             data = json.loads(response.text)

#             for item in data["Data"]:
#                 tag_id = item["Data"]["TagID"]
#                 sum = item["Data"]["SUM"]
#                 avg = item["Data"]["AVG"]
#                 created_date = item["Data"]["CreatedDate"]

#                 if tag_id in [16, 3538]:
#                     sum_value = str(round(int(float(sum)) / 1000 , 2))
#                 elif tag_id in [2, 3536]:
#                     sum_value = avg

#                 if not Historian.objects.filter(tagid=tag_id, created_date=created_date):
#                     Historian(
#                         tagid = tag_id,
#                         sum = sum_value,
#                         created_date = created_date,
#                         ID=Historian.objects.count() + 1
#                     ).save()
#                 else:
#                     console_logger.debug("data already exists in historian")
                
#             success = "completed"
#         except requests.exceptions.Timeout:
#             console_logger.debug("Request timed out!")
#         except requests.exceptions.ConnectionError:
#             console_logger.debug("Connection error")
    
#     except Exception as e:
#         success = False
#         console_logger.debug("----- Coal Testing Error -----",e)
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#         console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
#         console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
#         success = e
        
#     finally:
#         console_logger.debug(f"success:{success}")
#         SchedulerResponse("save consumption data", f"{success}")
#         return {"message" : "Successful"} 


@router.get("/load_historian_data", tags=["Coal Consumption"])                                    # coal consumption
def extract_historian_data(start_date: Optional[str] = None, end_date: Optional[str] = None):
    success = False
    try:
        headers_data = {
            'accept': 'application/json',
        }
        params = {
            'start_date': start_date,
            'end_date': end_date,
        }
        try:
            response = requests.get(f'http://{ip}/api/v1/host/historian_extract_data', params=params, headers=headers_data)
            data = json.loads(response.text)
            if response.status_code == 200:
                for item in data.get("Data", []):
                    item_data = item.get("Data")
                    if item_data:
                        tag_id = item_data.get("TagID")
                        avg = item_data.get("AVG")
                        created_date = item_data.get("CreatedDate")

                    if tag_id in [16, 3538, 2, 3536] and avg is not None:
                        sum_value = avg

                    if int(float(sum_value)) > 0:
                        Historian.objects(
                            tagid=tag_id, 
                            created_date=created_date
                        ).update_one(
                            set__sum=sum_value,
                            set__created_at=datetime.datetime.utcnow(),
                            upsert=True
                        )
                    
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


@router.get("/load_historian", tags=["Historian"])
def sync_historian_data(start_date: Optional[str] = None, end_date: Optional[str] = None):
    success = False
    try:
        headers_data = {
            'accept': 'application/json',
        }

        start_dt = datetime.datetime.fromisoformat(start_date)
        end_dt = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)
        
        intervals = []
        current_dt = start_dt
        while current_dt < end_dt:
            next_dt = current_dt + timedelta(hours=1)
            if next_dt > end_dt:
                next_dt = end_dt
            intervals.append({"StartTime": current_dt.isoformat(), "EndTime": next_dt.isoformat()})
            current_dt = next_dt

        for interval in intervals:
            params = {
                'start_date': interval["StartTime"],
                'end_date': interval["EndTime"],
            }

            try:
                response = requests.get(f'http://{ip}/api/v1/host/historian_extract_data', params=params, headers=headers_data)
                data = json.loads(response.text)

                for item in data.get("Data", []):
                    item_data = item.get("Data")
                    if item_data:
                        tag_id = item_data.get("TagID")
                        avg = item_data.get("AVG")
                        created_date = item_data.get("CreatedDate")

                        if tag_id in [16, 3538, 2, 3536] and avg is not None:
                            sum_value = avg

                            if int(float(sum_value)) > 0:
                                Historian.objects(
                                    tagid=tag_id, 
                                    created_date=created_date
                                ).update_one(
                                    set__sum=sum_value,
                                    set__created_at=datetime.datetime.utcnow(),
                                    upsert=True
                                )

                success = "completed"
            except requests.exceptions.Timeout:
                console_logger.debug("Request timed out!")
            except requests.exceptions.ConnectionError:
                console_logger.debug("Connection error")

    except Exception as e:
        success = False
        console_logger.debug("----- Coal Testing Error -----", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        success = e

    finally:
        console_logger.debug(f"success: {success}")
        SchedulerResponse("save consumption data", f"{success}")
        return {"message": "Successful"}

    

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
        console_logger.debug("hitted data")
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
        # coal_testing_url = f"http://{ip}/api/v1/host/coal_extract_data?start_date=2024-07-09&end_date=2024-08-09"
        # coal_testing_url = f"http://{testing_ip}/limsapi/api/SampleDetails/GetSampleRecord/GetSampleRecord?Fromdate={start_date}&todate={end_date}"
        # coal_testing_url = f"http://172.21.96.145/limsapi/api/SampleDetails/GetSampleRecord/GetSampleRecord?Fromdate={start_date}&todate={end_date}"
        try:
            response = requests.request("GET", url=coal_testing_url, headers=headers, data=payload, proxies=proxies)
            testing_data = response.json()
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
                        # param_info = {
                        #     "parameter_Name": param.get("parameter_Name")
                        #     .title()
                        #     .replace(" ", "_"),
                        #     "unit_Val": param.get("unit_Val").title().replace(" ",""),
                        #     "test_Method": param.get("test_Method"),
                        #     "val1": param.get("val1"),
                        # }

                        # if param.get("parameter_Name").title() == "Gross Calorific Value (Adb)":
                        #     if param.get("val1"):
                        #         fetchCoalGrades = CoalGrades.objects()
                        #         for single_coal_grades in fetchCoalGrades:
                        #             if (
                        #                 single_coal_grades["start_value"]
                        #                 <= param.get("val1")
                        #                 <= single_coal_grades["end_value"]
                        #                 and single_coal_grades["start_value"] != ""
                        #                 and single_coal_grades["end_value"] != ""
                        #             ):
                        #                 param_info["grade"] = single_coal_grades["grade"]
                        #             elif param.get("val1") > "7001":
                        #                 param_info["grade"] = "G-1"
                        #                 break

                        if param.get("parameter_Name").title() == "Gross Calorific Value (Adb)":
                            param_info = {
                                "parameter_Name": param.get("parameter_Name")
                                .title()
                                .replace(" ", "_"),
                                "unit_Val": param.get("unit_Val").title().replace(" ",""),
                                "test_Method": param.get("test_Method"),
                                "val1": "0",
                            }
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
                                        param_info["grade"] = "G-1"
                                        break
                        else:
                            param_info = {
                                "parameter_Name": param.get("parameter_Name")
                                .title()
                                .replace(" ", "_"),
                                "unit_Val": param.get("unit_Val").title().replace(" ",""),
                                "test_Method": param.get("test_Method"),
                                "val1": param.get("val1"),
                            }

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

                    # for secl_param in entry["sample_Parameters"]:
                    #     param_info = {
                    #         "parameter_Name": secl_param.get("parameter_Name")
                    #         .title()
                    #         .replace(" ", "_"),
                    #         "unit_Val": secl_param.get("unit_Val").title().replace(" ",""),
                    #         "test_Method": secl_param.get("test_Method"),
                    #         "val1": secl_param.get("val1"),
                    #     }
                    #     secl_data["parameters"].append(param_info)
                    # secl_extracted_data.append(secl_data)

                    for secl_param in entry["sample_Parameters"]:
                        if secl_param.get("parameter_Name").title() == "Gross Calorific Value (Adb)":
                            param_info = {
                                "parameter_Name": secl_param.get("parameter_Name")
                                .title()
                                .replace(" ", "_"),
                                "unit_Val": secl_param.get("unit_Val").title().replace(" ",""),
                                "test_Method": secl_param.get("test_Method"),
                                "val1": "0",
                            }
                        else:
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
                    if "Rake" in secl_entry.get("rake_No").strip():
                        rake_no = secl_entry.get("rake_No").strip()
                    else:
                        no_data = '{:02d}'.format(int(secl_entry.get("rake_No").strip()))
                        # rake_no = f"Rake-{str(no_data)}"
                        rake_no = f"{str(no_data)}"
                    try:
                        coalTestRailData = CoalTestingTrain.objects.get(rrNo=secl_entry.get("rrNo").strip(), rake_no=rake_no)
                    except DoesNotExist as e:
                        CoalTestingTrain(
                            location=re.sub(r'\t', '', re.sub(' +', ' ', secl_entry.get("sample_Desc").strip())),
                            rrNo=secl_entry.get("rrNo").strip(),
                            rR_Qty=secl_entry.get("rR_Qty").strip(),
                            # rake_no=secl_entry.get("rake_No").strip(),
                            rake_no=rake_no,
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

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__lte = end_date)

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
                                if values.get('average_GCV_Grade'):
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
                # Perform pagination here using list slicing
                start_index = (page_no - 1) * page_len
                end_index = start_index + page_len
                paginated_data = final_data[start_index:end_index]

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

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__lte = end_date)

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
                                    dictData["Difference_Gross_Calorific_Value_Grade_(Adb)"] = str(abs(int(dictData['GWEL_Gross_Calorific_Value_Grade_(Adb)'].replace('G-', ''))) - int(dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"].replace('G-', '')))
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
                                    dictData["Difference_Gross_Calorific_Value_Grade_(Adb)"] = str(abs(int(dictData['GWEL_Gross_Calorific_Value_Grade_(Adb)'].replace('G-', ''))) - int(dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"].replace('G-', '')))
                           
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
def wcl_addon_data(response: Response, paydata: WCLtestMain):
    try:
        multyData = paydata.dict()
        for dataLoad in multyData["data"]:
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
                            console_logger.debug()
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


@router.post("/coal_test_wcl_train_addon", tags=["Coal Testing"])
def wcl_addon_data(response: Response, paydata: WCLtestMain):
    try:
        multyData = paydata.dict()
        for dataLoad in multyData["data"]:
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
                        worksheet.write(row, 0, count, cell_format)
                        if filter_type == "gwel":
                            worksheet.write(row, 1, str(result["Mine"]), cell_format)
                            worksheet.write(row, 2, str(result["Lot_No"]), cell_format)
                            worksheet.write(row, 3, str(result["DO_No"]), cell_format)
                            worksheet.write(row, 4, str(result["DO_Qty"]), cell_format)
                            worksheet.write(row, 5, str(result["Supplier"]), cell_format)
                            worksheet.write(row, 6, str(result["Date"]), cell_format)
                            worksheet.write(row, 7, str(result["Time"]), cell_format)
                            worksheet.write(row, 8, str(result["GWEL_Total_Moisture_%"]), cell_format)
                            worksheet.write(row, 9, str(result["GWEL_Inherent_Moisture_(Adb)_%"]), cell_format)
                            worksheet.write(row, 10, str(result["GWEL_Ash_(Adb)_%"]), cell_format)
                            worksheet.write(row, 11, str(result["GWEL_Volatile_Matter_(Adb)_%"]), cell_format)
                            worksheet.write(row, 12, str(result["GWEL_Gross_Calorific_Value_(Adb)_Kcal/Kg"]), cell_format)
                            worksheet.write(row, 13, str(result["GWEL_Ash_(Arb)_%"]), cell_format)
                            worksheet.write(row, 14, str(result["GWEL_Volatile_Matter_(Arb)_%"]), cell_format)
                            worksheet.write(row, 15, str(result["GWEL_Fixed_Carbon_(Arb)_%"]), cell_format)
                            worksheet.write(row, 16, str(result["GWEL_Gross_Calorific_Value_(Arb)_Kcal/Kg"]), cell_format)
                            if result.get("GWEL_Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= int(result.get("GWEL_Gross_Calorific_Value_(Adb)_Kcal/Kg"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 17, str(single_coal_grades["grade"]), cell_format)
                                    elif int(result.get("GWEL_Gross_Calorific_Value_(Adb)_Kcal/Kg")) > 7001:
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
                                        <= float(result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 18, str(single_coal_grades["grade"]), cell_format)
                                    elif float(result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg")) > 7001:
                                        worksheet.write(row, 18, "G-1", cell_format)
                        elif filter_type == "all":
                            worksheet.write(row, 1, str(result["Mine"]), cell_format)
                            worksheet.write(row, 2, str(result["Lot_No"]), cell_format)
                            worksheet.write(row, 3, str(result["DO_No"]), cell_format)
                            worksheet.write(row, 4, str(result["DO_Qty"]), cell_format)
                            worksheet.write(row, 5, str(result["Supplier"]), cell_format)
                            worksheet.write(row, 6, str(result["Date"]), cell_format)
                            worksheet.write(row, 7, str(result["Time"]), cell_format)
                            worksheet.write(row, 8, str(result["GWEL_Total_Moisture_%"]), cell_format)
                            worksheet.write(row, 9, str(result["GWEL_Inherent_Moisture_(Adb)_%"]), cell_format)
                            worksheet.write(row, 10, str(result["GWEL_Ash_(Adb)_%"]), cell_format)
                            worksheet.write(row, 11, str(result["GWEL_Volatile_Matter_(Adb)_%"]), cell_format)
                            worksheet.write(row, 12, str(result["GWEL_Gross_Calorific_Value_(Adb)_Kcal/Kg"]), cell_format)
                            worksheet.write(row, 13, str(result["GWEL_Ash_(Arb)_%"]), cell_format)
                            worksheet.write(row, 14, str(result["GWEL_Volatile_Matter_(Arb)_%"]), cell_format)
                            worksheet.write(row, 15, str(result["GWEL_Fixed_Carbon_(Arb)_%"]), cell_format)
                            worksheet.write(row, 16, str(result["GWEL_Gross_Calorific_Value_(Arb)_Kcal/Kg"]), cell_format)
                            if result.get("GWEL_Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= float(result.get("GWEL_Gross_Calorific_Value_(Adb)_Kcal/Kg"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 17, str(single_coal_grades["grade"]), cell_format)
                                    elif float(result.get("GWEL_Gross_Calorific_Value_(Adb)_Kcal/Kg")) > 7001:
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
                                        <= float(result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 27, str(single_coal_grades["grade"]), cell_format)
                                    elif float(result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg")) > 7001:
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
            

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__lte = end_date)


            
            logs = (
                CoalTestingTrain.objects(data)
                .order_by("-ID")
                .skip(offset)
                .limit(page_len)                  
            )        

            if any(logs):
                for log in logs:
                    # result["labels"] = list(log.payload().keys())
                    result["labels"] = [
                    "Sr.No", 
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
                            worksheet.write(row, 8, str(result["GWEL_Total_Moisture_%"]), cell_format)
                            worksheet.write(row, 9, str(result["GWEL_Inherent_Moisture_(Adb)_%"]), cell_format)
                            worksheet.write(row, 10, str(result["GWEL_Ash_(Adb)_%"]), cell_format)
                            worksheet.write(row, 11, str(result["GWEL_Volatile_Matter_(Adb)_%"]), cell_format)
                            worksheet.write(row, 12, str(result["GWEL_Gross_Calorific_Value_(Adb)_Kcal/Kg"]), cell_format)
                            worksheet.write(row, 13, str(result["GWEL_Ash_(Arb)_%"]), cell_format)
                            worksheet.write(row, 14, str(result["GWEL_Volatile_Matter_(Arb)_%"]), cell_format)
                            worksheet.write(row, 15, str(result["GWEL_Fixed_Carbon_(Arb)_%"]), cell_format)
                            worksheet.write(row, 16, str(result["GWEL_Gross_Calorific_Value_(Arb)_Kcal/Kg"]), cell_format)
                            if result.get("GWEL_Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= float(result.get("GWEL_Gross_Calorific_Value_(Adb)_Kcal/Kg"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 17, str(single_coal_grades["grade"]), cell_format)
                                    elif float(result.get("GWEL_Gross_Calorific_Value_(Adb)_Kcal/Kg")) > 7001:
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
                            worksheet.write(row, 8, str(result["GWEL_Total_Moisture_%"]), cell_format)
                            worksheet.write(row, 9, str(result["GWEL_Inherent_Moisture_(Adb)_%"]), cell_format)
                            worksheet.write(row, 10, str(result["GWEL_Ash_(Adb)_%"]), cell_format)
                            worksheet.write(row, 11, str(result["GWEL_Volatile_Matter_(Adb)_%"]), cell_format)
                            worksheet.write(row, 12, str(result["GWEL_Gross_Calorific_Value_(Adb)_Kcal/Kg"]), cell_format)
                            worksheet.write(row, 13, str(result["GWEL_Ash_(Arb)_%"]), cell_format)
                            worksheet.write(row, 14, str(result["GWEL_Volatile_Matter_(Arb)_%"]), cell_format)
                            worksheet.write(row, 15, str(result["GWEL_Fixed_Carbon_(Arb)_%"]), cell_format)
                            worksheet.write(row, 16, str(result["GWEL_Gross_Calorific_Value_(Arb)_Kcal/Kg"]), cell_format)
                            if result.get("GWEL_Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= float(result.get("GWEL_Gross_Calorific_Value_(Adb)_Kcal/Kg"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 17, str(single_coal_grades["grade"]), cell_format)
                                    elif float(result.get("GWEL_Gross_Calorific_Value_(Adb)_Kcal/Kg")) > 7001:
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
def gmr_table(response:Response, filter_data: Optional[List[str]] = Query([]), 
              currentPage: Optional[int] = None, perPage: Optional[int] = None, 
              date: Optional[str] = None, search_text: Optional[str] = None,
              start_timestamp: Optional[str] = None, end_timestamp: Optional[str] = None, 
              type: Optional[str] = "display", 
              consumer_type: Optional[str] = "All"):
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

            if date:
                end =f'{date} 23:59:59'
                start = f'{date} 00:00:00'

                start_date = convert_to_utc_format(start, "%Y-%m-%d %H:%M:%S")
                end_date = convert_to_utc_format(end, "%Y-%m-%d %H:%M:%S")

                data &= Q(created_at__gte = start_date)
                data &= Q(created_at__lte = end_date)
                

            if start_timestamp:
                start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
                data &= Q(GWEL_Tare_Time__gte = start_date)

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M","Asia/Kolkata",False)
                data &= Q(GWEL_Tare_Time__lte = end_date)

            if search_text:
                if search_text.isdigit():
                    data &= Q(arv_cum_do_number__icontains=search_text) | Q(delivery_challan_number__icontains=search_text)
                else:
                    data &= (Q(vehicle_number__icontains=search_text))
            
            if consumer_type and consumer_type != "All":
                data &= Q(type_consumer__icontains=consumer_type)

            offset = (page_no - 1) * page_len
            
            logs = (
                Gmrdata.objects(data)
                .order_by("-vehicle_in_time")
                .skip(offset)
                .limit(page_len)
            )        
            if any(logs):
                for log in logs:
                    result["labels"] = list(log.payload().keys())
                    result["datasets"].append(log.payload())

            result["total"]= len(Gmrdata.objects(data))
            # console_logger.debug(f"-------- Road Journey Table Response -------- {result}")
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
                data &= Q(created_at__gte = start_date)

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M","Asia/Kolkata",False)
                data &= Q(created_at__lte = end_date)
            
            if search_text:
                if search_text.isdigit():
                    data &= Q(arv_cum_do_number__icontains = search_text) | Q(delivery_challan_number__icontains = search_text)
                else:
                    data &= Q(vehicle_number__icontains = search_text)

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
        entry_exists = Gmrrequest.objects(delivery_challan_number=challan_no,vehicle_number__exists=True,expiry_validation=True).order_by("-created_at").first()
        
        if not record:    
            raise HTTPException(status_code=404, detail="Record not found")
        record.dc_request = True
        record.save()

        if not entry_exists:
            dc_data = Gmrrequest(delivery_challan_number = challan_no,
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
    search_type: Optional[str] = "All"
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

        if search_type == "All":
            payload_method = "tare_payload"

        elif search_type == "fitness":
            data &= Q(request="Fitness_Expiry_Request")
            payload_method = "payload"

        elif search_type == "tare":
            data &= Q(request="Tare_Diff_Request")
            payload_method = "tare_payload"
            
        else:
            data &= Q(request="DC_Expiry_Request")
            payload_method = "payload"

        offset = (page_no - 1) * page_len

        logs = (
            Gmrrequest.objects(data)
            .order_by("-created_at")
            .skip(offset)
            .limit(page_len)
        )

        if logs:
            for log in logs:
                payload = getattr(log, payload_method)()
                result["labels"] = list(payload.keys())
                result["datasets"].append(payload)

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
    search_type: Optional[str] = "All"
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

            if search_type == "All":
                payload_method = "history_tare_payload"

            elif search_type == "fitness":
                data &= Q(request="Fitness_Expiry_Request")
                payload_method = "history_payload"

            elif search_type == "tare":
                data &= Q(request="Tare_Diff_Request")
                payload_method = "history_tare_payload"

            else:
                data &= Q(request="DC_Expiry_Request")
                payload_method = "history_payload"

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
                    payload = getattr(log, payload_method)()
                    result["labels"] = list(payload.keys())
                    result["datasets"].append(payload)

            result["total"] = Gmrrequest.objects(data).count()
            return result

        elif type == "download":
            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)

            data = Q(approved_at__ne=None)

            if search_type == "All":
                data &= Q(request__in=["Fitness_Expiry_Request", "Tare_Diff_Request", "DC_Expiry_Request"])
                payload_method = "history_tare_payload"
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
                    "Challan Tare Wt(MT)",
                    "GWEL Tare Wt(MT)",
                    "Total Net Amount",
                    "Remark",
                    "Request Time",
                    "Approval Time",
                    "TAT"
                ]

            elif search_type == "fitness":
                data &= Q(request="Fitness_Expiry_Request")
                payload_method = "history_payload"
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
            elif search_type == "tare":
                data &= Q(request="Tare_Diff_Request")
                payload_method = "history_tare_payload"
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
                    "Challan Tare Wt(MT)",
                    "GWEL Tare Wt(MT)",
                    "Total Net Amount",
                    "Remark",
                    "Request Time",
                    "Approval Time",
                    "TAT"
                ]
            else:
                data &= Q(request="DC_Expiry_Request")
                payload_method = "history_payload"
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
                    cell_format2.set_align("vcenter")

                    worksheet = workbook.add_worksheet()
                    worksheet.set_column("A:AZ", 20)
                    worksheet.set_default_row(50)
                    cell_format = workbook.add_format()
                    cell_format.set_font_size(10)
                    cell_format.set_align("center")
                    cell_format.set_align("vcenter")

                    for index, header in enumerate(headers):
                        worksheet.write(0, index, header, cell_format2)

                    for row, query in enumerate(usecase_data, start=1):
                        result = getattr(query, payload_method)()
                        worksheet.write(row, 0, row, cell_format)
                        worksheet.write(row, 1, str(result.get("Request_type", "")), cell_format)
                        worksheet.write(row, 2, str(result.get("Mine", "")), cell_format)
                        worksheet.write(row, 3, str(result.get("Vehicle_Number", "")), cell_format)
                        worksheet.write(row, 4, str(result.get("Delivery_Challan_No", "")), cell_format)
                        worksheet.write(row, 5, str(result.get("DO_No", "")), cell_format)
                        worksheet.write(row, 6, str(result.get("Vehicle_Chassis_No", "")), cell_format)
                        worksheet.write(row, 7, str(result.get("Fitness_Expiry", "")), cell_format)
                        worksheet.write(row, 8, str(result.get("DC_Date", "")), cell_format)
                        worksheet.write(row, 9, str(result.get("Challan_Net_Wt(MT)", "")), cell_format)
                        if search_text == "All":
                            worksheet.write(row, 10, str(result.get("Challan_Tare_Wt(MT)", "")), cell_format)
                            worksheet.write(row, 11, str(result.get("GWEL_Tare_Wt(MT)", "")), cell_format)
                            worksheet.write(row, 12, str(result.get("Total_net_amount", "")), cell_format)
                            worksheet.write(row, 13, str(result.get("Remark", "")), cell_format)
                            worksheet.write(row, 14, str(result.get("Request_Time", "")), cell_format)
                            worksheet.write(row, 15, str(result.get("Approval_Time", "")), cell_format)
                            worksheet.write(row, 16, str(result.get("TAT", "")), cell_format)
                        if search_type == "tare" or search_type == "All":
                            worksheet.write(row, 10, str(result.get("Challan_Tare_Wt(MT)", "")), cell_format)
                            worksheet.write(row, 11, str(result.get("GWEL_Tare_Wt(MT)", "")), cell_format)
                            worksheet.write(row, 12, str(result.get("Total_net_amount", "")), cell_format)
                            worksheet.write(row, 13, str(result.get("Remark", "")), cell_format)
                            worksheet.write(row, 14, str(result.get("Request_Time", "")), cell_format)
                            worksheet.write(row, 15, str(result.get("Approval_Time", "")), cell_format)
                            worksheet.write(row, 16, str(result.get("TAT", "")), cell_format)
                        else:
                            worksheet.write(row, 10, str(result.get("Total_net_amount", "")), cell_format)
                            worksheet.write(row, 11, str(result.get("Remark", "")), cell_format)
                            worksheet.write(row, 12, str(result.get("Request_Time", "")), cell_format)
                            worksheet.write(row, 13, str(result.get("Approval_Time", "")), cell_format)
                            worksheet.write(row, 14, str(result.get("TAT", "")), cell_format)
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


@router.put("/road/update_expiry_date", tags=["Road Map Request"])
async def update_fc_expiry_date(vehicle_number: str, remark: Optional[str] = None):
    try:
        record = Gmrdata.objects(vehicle_number=vehicle_number).order_by("-created_at").first()
        request_record = Gmrrequest.objects(vehicle_number=vehicle_number, expiry_validation=True).order_by("-created_at").first()

        if remark is None:
            remark = "Fitness Extended For 7 days"

        if request_record:
            request_record.expiry_validation = False
            request_record.approved_at = datetime.utcnow()
            request_record.remark = remark
            request_record.save()

        if not record:
            raise HTTPException(status_code=404, detail="Record not found")
        else:
            record.certificate_expiry = (datetime.datetime.now().date() + timedelta(days=7)).strftime("%d-%m-%Y")
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
        
        record.dc_request_status = True
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

        record.dc_request_status = False
        record.dc_request = False
        record.save()
            
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
        entry_exists = Gmrrequest.objects(delivery_challan_number=challan_no,vehicle_number__exists=True,expiry_validation=True).order_by("-created_at").first()
        
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
                                    actual_tare_qty = data.Actual_Tare_Qty,
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
                            actual_tare_qty = data.Actual_Tare_Qty,
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
            remark = "Tare Req Approved"

        if request_record:
            request_record.expiry_validation = False
            request_record.approved_at = datetime.datetime.utcnow()
            request_record.remark = remark
            request_record.save()

        if not record:
            raise HTTPException(status_code=404, detail="Record not found")

        record.tare_request_status = True
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
            remark = "Tare Req Declined"

        if request_record:
            request_record.expiry_validation = False
            request_record.approved_at =  datetime.datetime.utcnow()
            request_record.remark = remark
            request_record.save()

        if not record:
            raise HTTPException(status_code=404, detail="Record not found")
            
        record.tare_request_status = False
        record.tare_request = False
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
                date: Optional[str] = None,
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

            if date:
                end =f'{date} 23:59:59'
                start = f'{date} 00:00:00'
                
                data["created_at__gte"] = convert_to_utc_format(start, "%Y-%m-%d %H:%M:%S")
                data["created_at__lte"] = convert_to_utc_format(end, "%Y-%m-%d %H:%M:%S")

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
                        "Sr.No",
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

        vehicle_count = Gmrdata.objects(created_at__gte=from_ts, created_at__ne=None).count()
        # vehicle_count = Gmrdata.objects(GWEL_Tare_Time__gte=from_ts, GWEL_Tare_Time__ne=None).count()

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


@router.get("/road/unit1_coal_generation", tags=["Road Map"])
def daywise_unit1_generation(response: Response):
    try:
        current_time = datetime.datetime.now(IST)
        today = current_time.date()
        startdate = datetime.datetime.strptime(f'{today} 00:00:00',"%Y-%m-%d %H:%M:%S")

        pipeline = [
            {
                "$match": {
                    "created_date": {"$gte": startdate},
                    "tagid": 2,
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
                    },
                    "count": {"$sum":1}
                }
            }
        ]

        result = Historian.objects.aggregate(pipeline)

        total_sum = 0
        count = 1
        for doc in result:
            count = doc["count"]
            total_sum = doc["total_sum"]
        
        result = total_sum / count

        return {
            "title": "Today's Unit 1 Average Generation(MW)",
            "icon" : "energy",
            "data": round(result, 2),
            "last_updated": today
        }

    except Exception as e:
        console_logger.debug(f"----- Unit 1 Generation Error -----{e}")
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return {"error": str(e)}


@router.get("/road/unit2_coal_generation", tags=["Road Map"])
def daywise_unit1_generation(response: Response):
    try:
        current_time = datetime.datetime.now(IST)
        today = current_time.date()
        startdate = datetime.datetime.strptime(f'{today} 00:00:00',"%Y-%m-%d %H:%M:%S")

        pipeline = [
            {
                "$match": {
                    "created_date": {"$gte": startdate},
                    "tagid": 3536,
                    "sum": {"$ne": None}
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_sum": {
                        "$sum": {
                            "$toDouble": "$sum"
                        }
                    },
                    "count": {"$sum": 1}
                }
            }
        ]

        result = Historian.objects.aggregate(pipeline)
        
        total_sum = 0
        count = 1
        for doc in result:
            count = doc["count"]
            total_sum = doc["total_sum"]
        
        result = total_sum / count

        return {
            "title": "Today's Unit 2 Average Generation(MW)",
            "icon" : "energy",
            "data": round(result, 2),
            "last_updated": today
        }

    except Exception as e:
        console_logger.debug(f"----- Unit 2 Generation Error -----{e}")
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return {"error": str(e)}


@router.get("/road/unit1_coal_consumption", tags=["Road Map"])
def daywise_unit1_consumption(response: Response):
    try:
        current_time = datetime.datetime.now(IST)
        today = current_time.date()
        startdate = datetime.datetime.strptime(f'{today} 00:00:00',"%Y-%m-%d %H:%M:%S")

        pipeline = [
            {
                "$match": {
                    "created_date": {"$gte": startdate},
                    "tagid": 16,
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
                    },
                    "count": {"$sum":1}
                }
            }
        ]

        result = Historian.objects.aggregate(pipeline)

        total_sum = 0
        count = 1
        for doc in result:
            count = doc["count"]
            total_sum = doc["total_sum"]
        
        result = total_sum / count

        return {
            "title": "Today's Unit 1 Coal Consumption(MT)",
            "icon" : "coal",
            "data": round(result, 2),
            "last_updated": today
        }

    except Exception as e:
        console_logger.debug(f"----- Unit 1 Consumption Error ----- {e}")
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return {"error": str(e)}


@router.get("/road/unit2_coal_consumption", tags=["Road Map"])
def daywise_unit2_consumption(response: Response):
    try:
        current_time = datetime.datetime.now(IST)
        today = current_time.date()
        startdate = datetime.datetime.strptime(f'{today} 00:00:00',"%Y-%m-%d %H:%M:%S")

        pipeline = [
            {
                "$match": {
                    "created_date": {"$gte": startdate},
                    "tagid": 3538,
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
                    },
                    "count": {"$sum":1}
                }
            }
        ]

        result = Historian.objects.aggregate(pipeline)

        total_sum = 0
        count=1
        for doc in result:
            count = doc["count"]
            total_sum = doc["total_sum"]
        
        result = total_sum / count

        return {
            "title": "Today's Unit 2 Coal Consumption(MT)",
            "icon" : "coal",
            "data": round(result, 2),
            "last_updated": today
        }

    except Exception as e:
        console_logger.debug(f"----- Unit 2 Consumption Error ----- {e}")
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
                                    "$toDouble": "$net_qty"
                                }
                            }
                        }
                    }]
        # console_logger.debug(pipeline)
        result = Gmrdata.objects.aggregate(pipeline)

        total_coal = 0
        for doc in result:
            total_coal = doc["total_net_qty"]

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
                    "GWEL_Tare_Time": {"$gte": from_ts, "$lte": to_ts},
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
            start_of_month = specified_date.replace(day=1)
            start_of_month = datetime.datetime.strftime(start_of_month, '%Y-%m-%d')
            end_of_month = datetime.datetime.strftime(specified_date, '%Y-%m-%d')

            fetchCoalTesting = CoalTesting.objects(
                receive_date__gte= datetime.datetime.strptime(start_of_month, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"), receive_date__lte= datetime.datetime.strptime(end_of_month, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M")
            )
            
            fetchCoalTestingTrain = CoalTestingTrain.objects(
                receive_date__gte = datetime.datetime.strptime(start_of_month, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"), receive_date__lte= datetime.datetime.strptime(end_of_month, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M")
            )

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

                if rrNo in rrNo_values:
                    rrNo_values[location] += calorific_value
                else:
                    rrNo_values[location] = calorific_value

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

                if rrNo in rrNo_values:
                    rrNo_values[location] += calorific_value
                else:
                    rrNo_values[location] = calorific_value
            
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

        for single_gmr_data in fetchGmrDataMain:
            mine_name = single_gmr_data.mine
            net_qty = single_gmr_data.net_qty
            actual_net_qty = single_gmr_data.actual_net_qty
        
            if mine_name in actual_net_qty_all_totals:
                net_qty_all_totals[mine_name] += float(net_qty)
            else:
                net_qty_all_totals[mine_name] = float(net_qty)
            if actual_net_qty:
                if mine_name in actual_net_qty_all_totals:
                    actual_net_qty_all_totals[mine_name] += float(actual_net_qty)
                else:
                    actual_net_qty_all_totals[mine_name] = float(actual_net_qty)

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
        if specified_date:
            data = {}

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
                        if rr_no not in start_dates:
                            start_dates[rr_no] = date
                        elif date < start_dates[rr_no]:
                            start_dates[rr_no] = date
                        if payload.get("rr_qty"):
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

                coal_grades = CoalGrades.objects()

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
            to_ts = convert_to_utc_format(f'{specified_date} 23:59:59', "%Y-%m-%d %H:%M:%S")

        logs = (
            Gmrdata.objects(GWEL_Tare_Time__lte=to_ts, actual_tare_qty__ne=None, gate_approved=True, GWEL_Tare_Time__ne=None)
            # Gmrdata.objects()
            .order_by("-GWEL_Tare_Time")
        )
        sap_records = SapRecords.objects.all()
        
        if any(logs) or any(sap_records):
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
                        "start_date": "",
                        "end_date": "",
                        "source_type": "",
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
                    # if do_no not in start_dates:
                    #     start_dates[do_no] = date
                    # elif date < start_dates[do_no]:
                    #     start_dates[do_no] = date
                    # console_logger.debug(payload.get("start_date"))
                    if payload.get("slno"):
                        aggregated_data[date][do_no]["slno"] = datetime.datetime.strptime(payload.get("slno"), '%Y%m').strftime('%B %Y')
                    else:
                        aggregated_data[date][do_no]["slno"] = "-"
                    if payload.get("start_date"):
                        aggregated_data[date][do_no]["start_date"] = payload.get("start_date")
                    else:
                        aggregated_data[date][do_no]["start_date"] = "0"
                    if payload.get("end_date"):
                        aggregated_data[date][do_no]["end_date"] = payload.get("end_date")
                    else:
                        aggregated_data[date][do_no]["end_date"] = "0"

                    if payload.get("Type_of_consumer"):
                        aggregated_data[date][do_no]["source_type"] = payload.get("Type_of_consumer")

                    if payload.get("DO_Qty"):
                        aggregated_data[date][do_no]["DO_Qty"] = float(
                            payload["DO_Qty"]
                        )
                    else:
                        aggregated_data[date][do_no]["DO_Qty"] = 0

                    challan_net_wt = payload.get("Challan_Net_Wt(MT)")    
                
                    if challan_net_wt:
                        aggregated_data[date][do_no]["challan_lr_qty"] += float(challan_net_wt)

                    if payload.get("Mines_Name"):
                        aggregated_data[date][do_no]["mine_name"] = payload[
                            "Mines_Name"
                        ]
                    else:
                        aggregated_data[date][do_no]["mine_name"] = "-"
                    aggregated_data[date][do_no]["count"] += 1 
            
            for record in sap_records:
                do_no = record.do_no
                if do_no not in aggregated_data[specified_date]:
                    aggregated_data[specified_date][do_no]["DO_Qty"] = float(record.do_qty) if record.do_qty else 0
                    aggregated_data[specified_date][do_no]["mine_name"] = record.mine_name if record.mine_name else "-"
                    aggregated_data[specified_date][do_no]["start_date"] = record.start_date if record.start_date else "0"
                    aggregated_data[specified_date][do_no]["end_date"] = record.end_date if record.end_date else "0"
                    aggregated_data[specified_date][do_no]["source_type"] = record.consumer_type if record.consumer_type else "Unknown"
                    try:
                        aggregated_data[specified_date][do_no]["slno"] = datetime.datetime.strptime(record.slno, "%Y%m").strftime("%B %Y") if record.slno else "-"
                    except ValueError as e:
                        aggregated_data[specified_date][do_no]["slno"] = record.slno if record.slno else "-"
                    aggregated_data[specified_date][do_no]["count"] = 1

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
                            "start_date": data["start_date"],
                            "end_date": data["end_date"],
                            "source_type": data["source_type"],
                            "slno": data["slno"],
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
                    dictData["start_date"] = values["start_date"]
                    dictData["end_date"] = values["end_date"]
                    dictData["source_type"] = values["source_type"]
                    dictData["slno"] = values["slno"]
                    dictData["cumulative_challan_lr_qty"] = 0
                    dictData["balance_qty"] = 0
                    dictData["percent_supply"] = 0
                    dictData["asking_rate"] = 0
                    dictData['average_GCV_Grade'] = values["grade"]
                    
                    if dictData["start_date"] != "0" and dictData["end_date"] != "0":
                        # balance_days = datetime.datetime.strptime(dictData["end_date"], "%Y-%m-%d").date() - datetime.datetime.strptime(dictData["start_date"], "%Y-%m-%d").date()
                        balance_days = datetime.datetime.strptime(dictData["end_date"], "%Y-%m-%d").date() - datetime.datetime.today().date()
                        dictData["balance_days"] = balance_days.days
                    else:
                        dictData["balance_days"] = 0

                    # if data_dom in start_dates:
                    #     dictData["start_date"] = start_dates[data_dom]
                    #     dictData["end_date"] = datetime.datetime.strptime(start_dates[data_dom], "%Y-%m-%d") + timedelta(days=44)
                    #     balance_days = dictData["end_date"].date() - datetime.datetime.today().date()
                    #     dictData["balance_days"] = balance_days.days
                    # else:
                    #     dictData["start_date"] = None
                    #     dictData["end_date"] = None
                    #     dictData["balance_days"] = None
                    
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
                aggregated_totals = defaultdict(float)
                for single_data_entry in filtered_data_new:
                    do_no = single_data_entry['_id']['do_no']
                    total_net_qty = single_data_entry['total_net_qty']
                    aggregated_totals[do_no] += total_net_qty
                    
                data_by_do = {}
                finaldataMain = [single_data_list for single_data_list in final_data if single_data_list.get("balance_days") >= 0]
                for entry in finaldataMain:
                    do_no = entry['DO_No']
                    
                    if do_no not in data_by_do:
                        data_by_do[do_no] = entry
                        data_by_do[do_no]['cumulative_challan_lr_qty'] = round(entry['club_challan_lr_qty'], 2)
                    else:
                        data_by_do[do_no]['cumulative_challan_lr_qty'] += round(entry['club_challan_lr_qty'], 2)

                    if do_no in aggregated_totals:
                        data_by_do[do_no]['challan_lr_qty'] = round(aggregated_totals[do_no], 2)
                    else:
                        data_by_do[do_no]['challan_lr_qty'] = 0

                    if data_by_do[do_no]['DO_Qty'] != 0 and data_by_do[do_no]['cumulative_challan_lr_qty'] != 0:
                        data_by_do[do_no]['percent_supply'] = round((data_by_do[do_no]['cumulative_challan_lr_qty'] / data_by_do[do_no]['DO_Qty']) * 100, 2)
                    else:
                        data_by_do[do_no]['percent_supply'] = 0

                    # if data_by_do[do_no]['cumulative_challan_lr_qty'] != 0 and data_by_do[do_no]['DO_Qty'] != 0:
                    data_by_do[do_no]['balance_qty'] = round(data_by_do[do_no]['DO_Qty'] - data_by_do[do_no]['cumulative_challan_lr_qty'], 2)
                    # else:
                    #     data_by_do[do_no]['balance_qty'] = 0
                    
                    if data_by_do[do_no]['balance_days'] and data_by_do[do_no]['balance_qty'] != 0:
                        data_by_do[do_no]['asking_rate'] = round(data_by_do[do_no]['balance_qty'] / data_by_do[do_no]['balance_days'], 2)

                # final_data = list(data_by_do.values())

                sort_final_data = list(data_by_do.values())
                # Sort the data by 'balance_days', placing entries with 'balance_days' of 0 at the end
                final_data = sorted(sort_final_data, key=lambda x: (x['balance_days'] == 0, x['balance_days']))
                
                rrNo_values, clubbed_data, aopList = bar_graph_data(specified_date)
                clubbed_data_final = gmr_main_graph()
                total_monthly_final_net_qty = transit_loss_gain_road_mode_month(specified_date)
                yearly_final_data = transit_loss_gain_road_mode()
                yearly_rail_final_data = transit_loss_gain_rail_mode()

                dayWiseVehicleInCount = daywise_in_vehicle_count_datewise(specified_date)
                dayWiseGrnReceive = daywise_grn_receive_datewise(specified_date)
                dayWiseGwelReceive = daywise_gwel_receive_pdf_datewise(specified_date)
                dayWiseOutVehicelCount = daywise_out_vehicle_count_datewise(specified_date)

                fetchRailData = rail_pdf(specified_date)

                fetchRakeQuota = end_point_to_fetch_rake_quota_test(response, month_date=datetime.datetime.today().strftime('%Y-%m'), type="display")

                # console_logger.debug(fetchRakeQuota)

                seclLinkagegraph = endpoint_to_fetch_secl_linkage_matrialization(response, str(datetime.datetime.strptime(specified_date, "%Y-%m-%d").strftime("%Y")))
                wclLinkagegraph = endpoint_to_fetch_wcl_linkage_matrialization(response, str(datetime.datetime.strptime(specified_date, "%Y-%m-%d").strftime("%Y")))

                # console_logger.debug(seclLinkagegraph)
                # console_logger.debug(wclLinkagegraph)

                if specified_date:
                    month_data = specified_date
                    fetchData = generate_report(final_data, rrNo_values, month_data, clubbed_data, clubbed_data_final, dayWiseVehicleInCount, dayWiseGrnReceive, dayWiseGwelReceive, dayWiseOutVehicelCount, total_monthly_final_net_qty, yearly_final_data, aopList, fetchRailData, yearly_rail_final_data, fetchRakeQuota.get('datasets'), seclLinkagegraph, wclLinkagegraph)
                    return fetchData
                else:
                    fetchData = generate_report(final_data, rrNo_values, "", clubbed_data, clubbed_data_final, dayWiseVehicleInCount, dayWiseGrnReceive, dayWiseGwelReceive, dayWiseOutVehicelCount, total_monthly_final_net_qty, yearly_final_data, aopList, fetchRailData, yearly_rail_final_data, fetchRakeQuota.get('datasets'), seclLinkagegraph, wclLinkagegraph)
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

        if reportScheduler.time != "":
            time_format = "%H:%M"
            given_time = datetime.datetime.strptime(reportScheduler.time, time_format)

            time_to_subtract = datetime.timedelta(hours=5, minutes=30)

            new_time = given_time - time_to_subtract
            new_time_str = new_time.strftime(time_format)
            hh, mm = new_time_str.split(":")

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
                    shift_time = datetime.datetime.strptime(single_shift.get("time"), time_format)
                    shift_time_ist = shift_time - time_to_subtract
                    shift_hh, shift_mm = shift_time_ist.strftime(time_format).split(":")
                    backgroundTaskHandler.run_job(
                        task_name=f"{reportScheduler.report_name}_{single_shift.get('shift_wise')}", 
                        func=send_shift_report_generate, 
                        trigger="cron", **{"day": "*", "hour": shift_hh, "minute": shift_mm}, 
                        func_kwargs={"report_name":f"{reportScheduler.report_name}", "shift_name": single_shift.get('shift_wise'), "shift_time": single_shift.get("time")},
                        max_instances=1)
        try:
            fetchEmailNotifications = emailNotifications.objects(notification_name=dataName.get("report_name"))
            for singleEmailData in fetchEmailNotifications:
                singleEmailData.delete()
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
        if kwargs["report_name"] == "daily_coal_logistic_report":
            if reportSchedule[0].active == False:
                console_logger.debug("scheduler is off")
                return
            elif reportSchedule[0].active == True:
                if not check_existing_notification("daily_coal_logistic_report"):
                    emailNotifications(notification_name="daily_coal_logistic_report").save()
                    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
                    generateReportData = generate_gmr_report(Response, yesterday.strftime("%Y-%m-%d"), "All")
                    # generateReportData = generate_gmr_report(Response, "2024-08-01", "All")
                    
                    response_code, fetch_email = fetch_email_data()
                    if response_code == 200:
                        console_logger.debug(reportSchedule[0].recipient_list)
                        subject = f"GMR Daily Coal Logistic Report {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                        body = f"Daily Coal Logistic Report for Date: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                        # send_email(smtpData.Smtp_user, subject, smtpData.Smtp_password, smtpData.Smtp_host, smtpData.Smtp_port, receiver_email, body, f"{os.path.join(os.getcwd())}{generateReportData}")
                        checkEmailDevelopment = EmailDevelopmentCheck.objects()
                        if checkEmailDevelopment[0].development == "local":
                            send_email(fetch_email.get("Smtp_user"), subject, fetch_email.get("Smtp_password"), fetch_email.get("Smtp_host"), fetch_email.get("Smtp_port"), reportSchedule[0].recipient_list, body, f"{os.path.join(os.getcwd())}/{generateReportData}", reportSchedule[0].cc_list, reportSchedule[0].bcc_list)
                        elif checkEmailDevelopment[0].development == "prod":
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
                            generate_email(Response, email=send_data)
                else:
                    return
        elif kwargs["report_name"] == "expiring_fitness_certificate":
            if reportSchedule[1].active == False:
                return
            elif reportSchedule[1].active == True:
                if not check_existing_notification("expiring_fitness_certificate"):
                    emailNotifications(notification_name="expiring_fitness_certificate").save()
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
                            send_email(fetch_email.get("Smtp_user"), subject, fetch_email.get("Smtp_password"), fetch_email.get("Smtp_host"), fetch_email.get("Smtp_port"), reportSchedule[1].recipient_list, body, "", reportSchedule[1].cc_list, reportSchedule[1].bcc_list)
                        elif checkEmailDevelopment[0].development == "prod":
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
        elif kwargs["report_name"] == "daily_coal_consumption_report":
            if reportSchedule[9].active == False:
                console_logger.debug("scheduler is off")
                return
            elif reportSchedule[9].active == True:
                if not check_existing_notification("daily_coal_consumption_report"):
                    emailNotifications(notification_name="daily_coal_consumption_report").save()
                    generateReportData = endpoint_to_generate_coal_consumption_report(Response, datetime.date.today().strftime("%Y-%m-%d"))
                    response_code, fetch_email = fetch_email_data()
                    if response_code == 200:
                        console_logger.debug(reportSchedule[9].recipient_list)
                        subject = f"GMR Daily Specific Coal Consumption Report {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                        body = f"Daily Specific Coal Consumption Report for Date: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                        # send_email(smtpData.Smtp_user, subject, smtpData.Smtp_password, smtpData.Smtp_host, smtpData.Smtp_port, receiver_email, body, f"{os.path.join(os.getcwd())}{generateReportData}")
                        checkEmailDevelopment = EmailDevelopmentCheck.objects()
                        if checkEmailDevelopment[0].development == "local":
                            send_email(fetch_email.get("Smtp_user"), subject, fetch_email.get("Smtp_password"), fetch_email.get("Smtp_host"), fetch_email.get("Smtp_port"), reportSchedule[9].recipient_list, body, f"{os.path.join(os.getcwd())}/{generateReportData}", reportSchedule[9].cc_list, reportSchedule[9].bcc_list)
                        elif checkEmailDevelopment[0].development == "prod":
                            send_data = {
                                "sender_email": fetch_email.get("Smtp_user"),
                                "subject": subject,
                                "password": fetch_email.get("Smtp_password"),
                                "smtp_host": fetch_email.get("Smtp_host"),
                                "smtp_port": fetch_email.get("Smtp_port"),
                                "receiver_email": reportSchedule[9].recipient_list,
                                "body": body,
                                "file_path": f"{os.path.join(os.getcwd())}/{generateReportData}",
                                "cc_list": reportSchedule[9].cc_list,
                                "bcc_list": reportSchedule[9].bcc_list
                            }
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




@router.get("/consumer_type", tags=["Road Map"])
def endpoint_to_fetch_consumer_type(response: Response):
    try:
        listData = []
        checkConsumerType = Gmrdata.objects.only("type_consumer")
        for singleConsumerType in checkConsumerType:
            # console_logger.debug(singleConsumerType.payload())
            listData.append(singleConsumerType.payload()["Type_of_consumer"])
        return list(set(listData))
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

@router.get("/coal_logistics_report", tags=["Road Map"])
def coal_logistics_report_test(
    response: Response,
    specified_date: str,
    search_text: Optional[str] = None,
    currentPage: Optional[int] = None,
    perPage: Optional[int] = None,
    mine: Optional[str] = "All",
    consumer_type: Optional[str] = "All",
    type: Optional[str] = "display"
):
    try:
        result = {"labels": [], "datasets": [], "total": 0, "page_size": 15}
        if type and type == "display":

            data = Q()
            sap_data = Q()

            if mine and mine != "All":
                # data["mine__icontains"] = mine.upper()
                data &= Q(mine__icontains=mine.upper())
                sap_data &= Q(mine_name__icontains=mine.upper())

            if consumer_type and consumer_type != "All":
                # data["type_consumer__icontains"] = consumer_type
                data &= Q(type_consumer__icontains=consumer_type)
                # sap_data &= Q(consumer_type__icontains=consumer_type)
                sap_data &= Q(consumer_type__iexact=consumer_type)

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
                if search_text.isdigit():
                    data &= Q(arv_cum_do_number__icontains=search_text)
                    # sap_data &= Q(do_no__icontains=search_text)
                else:
                    data &= Q(mine__icontains=search_text)
                    # sap_data &= Q(mine_name__icontains=search_text)
                logs = (
                    Gmrdata.objects(GWEL_Tare_Time__lte=to_ts, actual_tare_qty__ne=None, gate_approved=True, GWEL_Tare_Time__ne=None)
                    .filter(data)
                    .order_by("-GWEL_Tare_Time")
                )
                # console_logger.debug(sap_data)
                # sap_records = SapRecords.objects.filter(sap_data)
                if not logs:  # If no data found in gmrData, search in sapRecords
                    if search_text.isdigit():
                        sap_data &= Q(do_no__icontains=search_text)
                    else:
                        sap_data &= Q(mine_name__icontains=search_text)
                    sap_records = SapRecords.objects.filter(sap_data) 
                else:
                    sap_records = []
            else:
                logs = (
                    Gmrdata.objects(GWEL_Tare_Time__lte=to_ts, actual_tare_qty__ne=None, gate_approved=True, GWEL_Tare_Time__ne=None)
                    .filter(data)
                    .order_by("-GWEL_Tare_Time")
                )
                sap_records = SapRecords.objects.filter(sap_data)

            # sap_records = SapRecords.objects.all()

            if any(logs) or any(sap_records):
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
                            "start_date": "",
                            "end_date": "",
                            "source_type": "",
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
                        # if do_no not in start_dates:
                        #     start_dates[do_no] = date
                        # elif date < start_dates[do_no]:
                        #     start_dates[do_no] = date
                        if payload.get("slno"):
                            aggregated_data[date][do_no]["slno"] = datetime.datetime.strptime(payload.get("slno"), '%Y%m').strftime('%B %Y')
                        else:
                            aggregated_data[date][do_no]["slno"] = "-"
                        if payload.get("start_date"):
                            aggregated_data[date][do_no]["start_date"] = payload.get("start_date")
                        else:
                            aggregated_data[date][do_no]["start_date"] = "0"
                        if payload.get("end_date"):
                            aggregated_data[date][do_no]["end_date"] = payload.get("end_date")
                        else:
                            aggregated_data[date][do_no]["end_date"] = "0"

                        if payload.get("Type_of_consumer"):
                            aggregated_data[date][do_no]["source_type"] = payload.get("Type_of_consumer")
                        
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

                for record in sap_records:
                    do_no = record.do_no
                    if do_no not in aggregated_data[specified_date]:
                        aggregated_data[specified_date][do_no]["DO_Qty"] = float(record.do_qty) if record.do_qty else 0
                        aggregated_data[specified_date][do_no]["mine_name"] = record.mine_name if record.mine_name else "-"
                        aggregated_data[specified_date][do_no]["start_date"] = record.start_date if record.start_date else "0"
                        aggregated_data[specified_date][do_no]["end_date"] = record.end_date if record.end_date else "0"
                        aggregated_data[specified_date][do_no]["source_type"] = record.consumer_type if record.consumer_type else "Unknown"
                        try:
                            aggregated_data[specified_date][do_no]["slno"] = datetime.datetime.strptime(record.slno, "%Y%m").strftime("%B %Y") if record.slno else "-"
                        except ValueError as e:
                            aggregated_data[specified_date][do_no]["slno"] = record.slno if record.slno else "-"
                        aggregated_data[specified_date][do_no]["count"] = 1
                
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
                                "start_date": data["start_date"],
                                "end_date": data["end_date"],
                                "source_type": data["source_type"],
                                "slno": data["slno"],
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
                        dictData['challan_lr_/_qty'] = 0
                        dictData["date"] = values["date"]
                        dictData["start_date"] = values["start_date"]
                        dictData["end_date"] = values["end_date"]
                        dictData["source_type"] = values["source_type"]
                        dictData["month"] = values["slno"]
                        dictData["cumulative_challan_lr_qty"] = 0
                        dictData["balance_qty"] = 0
                        dictData["percent_supply"] = 0
                        dictData["asking_rate"] = 0
                        dictData['average_GCV_Grade'] = values["grade"]
                        
                        
                        if dictData["start_date"] != "0" and dictData["end_date"] != "0":
                            # balance_days = datetime.datetime.strptime(dictData["end_date"], "%Y-%m-%d").date() - datetime.datetime.strptime(dictData["start_date"], "%Y-%m-%d").date()
                            balance_days = datetime.datetime.strptime(dictData["end_date"], "%Y-%m-%d").date() - datetime.datetime.today().date()
                            dictData["balance_days"] = balance_days.days
                        else:
                            dictData["balance_days"] = 0

                        # if data_dom in start_dates:
                        #     dictData["start_date"] = start_dates[data_dom]
                        #     dictData["end_date"] = datetime.datetime.strptime(start_dates[data_dom], "%Y-%m-%d") + timedelta(days=44)
                        #     balance_days = dictData["end_date"].date() - datetime.datetime.today().date()
                        #     dictData["balance_days"] = balance_days.days
                        # else:
                        #     dictData["start_date"] = None
                        #     dictData["end_date"] = None
                        #     dictData["balance_days"] = None
                        
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
                            data_by_do[do_no]['cumulative_challan_lr_/_qty'] = round(club_challan_lr_qty, 2)
                        else:
                            data_by_do[do_no]['cumulative_challan_lr_/_qty'] = round(
                                data_by_do[do_no].get('cumulative_challan_lr_/_qty', 0) + club_challan_lr_qty, 2
                            )
                        if filtered_data_new:
                            # data = filtered_data[0]["data"]
                            # Update challan_lr_qty if the DO_No matches
                            if do_no in aggregated_totals:
                                data_by_do[do_no]['challan_lr_qty'] = round(aggregated_totals[do_no], 2)
                            else:
                                data_by_do[do_no]['challan_lr_qty'] = 0
                        
                        # Update calculated fields
                        if data_by_do[do_no]['DO_Qty'] != 0 and data_by_do[do_no]['cumulative_challan_lr_/_qty'] != 0:
                            data_by_do[do_no]['percent_supply'] = round((data_by_do[do_no]['cumulative_challan_lr_/_qty'] / data_by_do[do_no]['DO_Qty']) * 100, 2)
                        else:
                            data_by_do[do_no]['percent_supply'] = 0

                        # if data_by_do[do_no]['cumulative_challan_lr_qty'] != 0 and data_by_do[do_no]['DO_Qty'] != 0:
                        data_by_do[do_no]['balance_qty'] = round(data_by_do[do_no]['DO_Qty'] - data_by_do[do_no]['cumulative_challan_lr_/_qty'], 2)
                        # else:
                        #     data_by_do[do_no]['balance_qty'] = 0
                        
                        if data_by_do[do_no]['balance_days'] and data_by_do[do_no]['balance_qty'] != 0:
                            data_by_do[do_no]['asking_rate'] = round(data_by_do[do_no]['balance_qty'] / data_by_do[do_no]['balance_days'], 2)

                        del entry['club_challan_lr_qty']
                    
                sort_final_data = list(data_by_do.values())
                # Sort the data by 'balance_days', placing entries with 'balance_days' of 0 at the end
                final_data = sorted(sort_final_data, key=lambda x: (x['balance_days'] == 0, x['balance_days']))
                # console_logger.debug(final_data)

                if final_data:
                    start_index = (page_no - 1) * page_len
                    end_index = start_index + page_len
                    paginated_data = final_data[start_index:end_index]

                    # result["labels"] = list(final_data[0].keys())
                    result["labels"] = ["month", "DO_No", "mine_name", "DO_Qty", "date", "challan_lr_/_qty", "cumulative_challan_lr_/_qty","balance_qty", "percent_supply", "asking_rate", "average_GCV_Grade", "start_date", "end_date", "balance_days"]
                    result["datasets"] = paginated_data
                    result["total"] = len(final_data)

                return result
            else:
                return result
        elif type and type == "download":
            del type
            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)
            if specified_date:
                specified_change_date = convert_to_utc_format(specified_date, "%Y-%m-%d")
                to_ts = convert_to_utc_format(f'{specified_date} 23:59:59', "%Y-%m-%d %H:%M:%S")
            data = Q()
            sap_data = Q()
            if search_text:
                if search_text.isdigit():
                    data &= Q(arv_cum_do_number__icontains=search_text)
                    # sap_data &= Q(do_no__icontains=search_text)
                else:
                    data &= Q(mine__icontains=search_text)
                    # sap_data &= Q(mine_name__icontains=search_text)
                logs = (
                    Gmrdata.objects(GWEL_Tare_Time__lte=to_ts, actual_tare_qty__ne=None, gate_approved=True, GWEL_Tare_Time__ne=None)
                    .filter(data)
                    .order_by("-GWEL_Tare_Time")
                )
                # console_logger.debug(sap_data)
                # sap_records = SapRecords.objects.filter(sap_data)
                if not logs:  # If no data found in gmrData, search in sapRecords
                    if search_text.isdigit():
                        sap_data &= Q(do_no__icontains=search_text)
                    else:
                        sap_data &= Q(mine_name__icontains=search_text)
                    sap_records = SapRecords.objects.filter(sap_data) 
                else:
                    sap_records = []
            else:
                logs = (
                    Gmrdata.objects(GWEL_Tare_Time__lte=to_ts, actual_tare_qty__ne=None, gate_approved=True, GWEL_Tare_Time__ne=None)
                    .filter(data)
                    .order_by("-GWEL_Tare_Time")
                )
                sap_records = SapRecords.objects.filter(sap_data)

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
                            "start_date": "",
                            "end_date": "",
                            "source_type": "",
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
                        # if do_no not in start_dates:
                        #     start_dates[do_no] = date
                        # elif date < start_dates[do_no]:
                        #     start_dates[do_no] = date
                        
                        if payload.get("slno"):
                            aggregated_data[date][do_no]["slno"] = datetime.datetime.strptime(payload.get("slno"), '%Y%m').strftime('%B %Y')
                        else:
                            aggregated_data[date][do_no]["slno"] = "-"

                        if payload.get("start_date"):
                            aggregated_data[date][do_no]["start_date"] = payload.get("start_date")
                        else:
                            aggregated_data[date][do_no]["start_date"] = "0"
                        if payload.get("end_date"):
                            aggregated_data[date][do_no]["end_date"] = payload.get("end_date")
                        else:
                            aggregated_data[date][do_no]["end_date"] = "0"
                        if payload.get("Type_of_consumer"):
                            aggregated_data[date][do_no]["source_type"] = payload.get("Type_of_consumer")
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
                
                for record in sap_records:
                    do_no = record.do_no
                    if do_no not in aggregated_data[specified_date]:
                        aggregated_data[specified_date][do_no]["DO_Qty"] = float(record.do_qty) if record.do_qty else 0
                        aggregated_data[specified_date][do_no]["mine_name"] = record.mine_name if record.mine_name else "-"
                        aggregated_data[specified_date][do_no]["start_date"] = record.start_date if record.start_date else "0"
                        aggregated_data[specified_date][do_no]["end_date"] = record.end_date if record.end_date else "0"
                        aggregated_data[specified_date][do_no]["source_type"] = record.consumer_type if record.consumer_type else "Unknown"
                        try:
                            aggregated_data[specified_date][do_no]["slno"] = datetime.datetime.strptime(record.slno, "%Y%m").strftime("%B %Y") if record.slno else "-"
                        except ValueError as e:
                            aggregated_data[specified_date][do_no]["slno"] = record.slno if record.slno else "-"
                        aggregated_data[specified_date][do_no]["count"] = 1

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
                                "start_date": data["start_date"],
                                "end_date": data["end_date"],
                                "source_type": data["source_type"],
                                "slno": data["slno"],
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
                        dictData['challan_lr_qty'] = 0
                        dictData["date"] = values["date"]
                        dictData["start_date"] = values["start_date"]
                        dictData["end_date"] = values["end_date"]
                        dictData["source_type"] = values["source_type"]
                        dictData["month"] = values["slno"]
                        dictData["cumulative_challan_lr_qty"] = 0
                        dictData["balance_qty"] = 0
                        dictData["percent_supply"] = 0
                        dictData["asking_rate"] = 0
                        dictData['average_GCV_Grade'] = values["grade"]
                        
                        
                        if dictData["start_date"] != "0" and dictData["end_date"] != "0":
                            # balance_days = datetime.datetime.strptime(dictData["end_date"], "%Y-%m-%d").date() - datetime.datetime.strptime(dictData["start_date"], "%Y-%m-%d").date()
                            balance_days = datetime.datetime.strptime(dictData["end_date"], "%Y-%m-%d").date() - datetime.datetime.today().date()
                            dictData["balance_days"] = balance_days.days
                        else:
                            dictData["balance_days"] = 0

                        # if data_dom in start_dates:
                        #     dictData["start_date"] = start_dates[data_dom]
                        #     dictData["end_date"] = datetime.datetime.strptime(start_dates[data_dom], "%Y-%m-%d") + timedelta(days=44)
                        #     balance_days = dictData["end_date"].date() - datetime.datetime.today().date()
                        #     dictData["balance_days"] = balance_days.days
                        # else:
                        #     dictData["start_date"] = None
                        #     dictData["end_date"] = None
                        #     dictData["balance_days"] = None
                        
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

                        # if data_by_do[do_no]['cumulative_challan_lr_qty'] != 0 and data_by_do[do_no]['DO_Qty'] != 0:
                        data_by_do[do_no]['balance_qty'] = round(data_by_do[do_no]['DO_Qty'] - data_by_do[do_no]['cumulative_challan_lr_qty'], 2)
                        # else:
                        #     data_by_do[do_no]['balance_qty'] = 0
                        
                        if data_by_do[do_no]['balance_days'] and data_by_do[do_no]['balance_qty'] != 0:
                            data_by_do[do_no]['asking_rate'] = round(data_by_do[do_no]['balance_qty'] / data_by_do[do_no]['balance_days'], 2)

                    # final_data = list(data_by_do.values())
                    sort_final_data = list(data_by_do.values())
                    # Sort the data by 'balance_days', placing entries with 'balance_days' of 0 at the end
                    final_data = sorted(sort_final_data, key=lambda x: (x['balance_days'] == 0, x['balance_days']))
                    result["datasets"] = final_data

                    headers = ["Month", "Mine Name", "DO_No", "Grade", "DO Qty", "Challan Lr / Qty", "Cumulative Challan Lr / Qty", "Balance Qty", "% of Supply", "Balance Days", "Asking Rate", "Do Start Date", "Do End Date"]
                    
                    for index, header in enumerate(headers):
                        worksheet.write(0, index, header, cell_format2)
                    
                    row = 1
                    for single_data in result["datasets"]:
                        # worksheet.write(row, 0, count, cell_format)
                        worksheet.write(row, 0, single_data["month"])
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
                        worksheet.write(row, 12, single_data["end_date"])

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
                                result["data"]["datasets"][0]["data"][index-1] = round(val, 2)
                            elif key == "Rail":
                                result["data"]["datasets"][1]["data"][index-1] = round(val, 2)
                        else:
                            if key == "Road":
                                result["data"]["datasets"][0]["data"][index] = round(val, 2)
                            elif key == "Rail":
                                result["data"]["datasets"][1]["data"][index] = round(val, 2)

        result["data"]["labels"] = copy.deepcopy(modified_labels)
        return result

    except Exception as e:
        console_logger.debug("----- Overall Transit Error -----", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


# @router.post("/add/sap/excel", tags=["Road Map"])
# async def endpoint_to_add_sap_excel_data(response: Response, file: UploadFile = File(...)):
#     try:
#         if file is None:
#             return {"error": "No file Uploaded!"}
        
#         contents = await file.read()

#         if not contents:
#             return {"error": "Uploaded file is empty!"}

#         if file.filename.endswith(".xlsx"):
#             # file saving start
#             date = str(datetime.datetime.now().strftime("%d-%m-%Y"))
#             target_directory = f"static_server/gmr_ai/{date}"
#             os.umask(0)
#             os.makedirs(target_directory, exist_ok=True, mode=0o777)
#             file_extension = file.filename.split(".")[-1]
#             file_name = f'sap_manual_{datetime.datetime.now().strftime("%Y-%m-%d:%H:%M")}.{file_extension}'
#             full_path = os.path.join(os.getcwd(), target_directory, file_name)
#             with open(full_path, "wb") as file_object:
#                 file_object.write(contents)
#             # file saving end

#             excel_data = pd.read_excel(BytesIO(contents))
#             data_excel_fetch = json.loads(excel_data.to_json(orient="records"))
#             for single_data in data_excel_fetch:
#                 try:
#                     fetchSapRecords = SapRecords.objects.get(do_no=str(single_data["Source & DO No"]))
#                 except DoesNotExist as e:
#                     add_data_excel = SapRecords(
#                         slno=single_data["Slno"],
#                         source=single_data["source"],
#                         mine_name=single_data["Mines Name"],
#                         sap_po=str(single_data["SAP PO"]),
#                         line_item=str(single_data["Line Item"]),
#                         do_no=str(single_data["Source & DO No"]),
#                         do_qty=str(single_data["DO QTY"]),
#                     )
#                     add_data_excel.save()

#                 # take it here
#                 fetchGmrData = Gmrdata.objects(arv_cum_do_number = str(single_data["Source & DO No"]))
#                 for single_gmr_data in fetchGmrData:
#                     single_gmr_data.po_no = str(single_data["SAP PO"])
#                     single_gmr_data.line_item = str(single_data["Line Item"])
#                     single_gmr_data.po_qty = str(single_data["DO QTY"])
#                     single_gmr_data.save()

#         return {"detail": "success"}
#     except KeyError as e:
#         raise HTTPException(status_code=404, detail="Key Error")
#     except Exception as e:
#         console_logger.debug("----- Sap Excel Error -----",e)
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#         console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
#         console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
#         return e

@router.post("/add/sap/excel", tags=["Coal Testing"])
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
            file_name = f'coallab_sap_manual_{datetime.datetime.now().strftime("%Y-%m-%d:%H:%M")}.{file_extension}'
            full_path = os.path.join(os.getcwd(), target_directory, file_name)
            with open(full_path, "wb") as file_object:
                file_object.write(contents)
            # file saving end

            excel_data = pd.read_excel(BytesIO(contents))
            data_excel_fetch = json.loads(excel_data.to_json(orient="records"))
            for single_data in data_excel_fetch:
                console_logger.debug(single_data)
                try:
                    fetchSapRecords = SapRecords.objects.get(do_no=str(single_data["DO No"]))
                except DoesNotExist as e:
                    add_data_excel = SapRecords(
                        # slno=single_data["Slno"] if single_data["Slno"] else None,
                        # source=single_data["source"],
                        # mine_name=single_data["Mines Name"],
                        sap_po=str(single_data["SAP PO"]) if single_data["SAP PO"] else None,
                        po_date=str(single_data["SAP PO Date"]) if single_data["SAP PO Date"] else None,
                        line_item=str(single_data["Line Item"]) if single_data["Line Item"] else None,
                        do_no=str(single_data["DO No"]) if single_data["DO No"] else None,
                        # do_qty=str(single_data["DO QTY"]),
                        # rake_no=single_data["DO/RR Qty"],
                        # start_date=single_data["DO Start Date"],
                        # end_date=single_data["DO End Date"],
                        # grade=single_data["Grade"]
                    )
                    add_data_excel.save()

                # # take it here
                fetchGmrData = Gmrdata.objects(arv_cum_do_number = str(single_data["DO No"]))
                for single_gmr_data in fetchGmrData:
                    single_gmr_data.po_no = str(single_data["SAP PO"]) if single_data["SAP PO"] else None
                    single_gmr_data.line_item = str(single_data["Line Item"]) if single_data["Line Item"] else None
                    single_gmr_data.po_date = str(single_data["SAP PO Date"]) if single_data["SAP PO Date"] else None
                    # single_gmr_data.slno = str(single_data["Slno"]) if single_data["Slno"] else None
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

@router.post("/add/railsap/excel", tags=["Coal Testing"])
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
            file_name = f'rail_sap_manual_{datetime.datetime.now().strftime("%Y-%m-%d:%H:%M")}.{file_extension}'
            full_path = os.path.join(os.getcwd(), target_directory, file_name)
            with open(full_path, "wb") as file_object:
                file_object.write(contents)
            # file saving end

            excel_data = pd.read_excel(BytesIO(contents))
            data_excel_fetch = json.loads(excel_data.to_json(orient="records"))
            for single_data in data_excel_fetch:
                console_logger.debug(single_data)
                try:
                    fetchRailSapRecords = sapRecordsRail.objects.get(rr_no=str(single_data["RR No"]))
                except DoesNotExist as e:
                    add_raildata_excel = sapRecordsRail(
                        sap_po=str(single_data["SAP PO"]) if single_data["SAP PO"] else None,
                        do_date=str(single_data["SAP PO Date"]) if single_data["SAP PO Date"] else None,
                        line_item=str(single_data["Line Item"]) if single_data["Line Item"] else None,
                        rr_no=str(single_data["RR No"]) if single_data["RR No"] else None,
                    )
                    add_raildata_excel.save()

                # take it here
                fetchRailGmrData = RailData.objects(rr_no = str(single_data["RR No"]))
                for single_rail_gmr_data in fetchRailGmrData:
                    single_rail_gmr_data.po_no = str(single_data["SAP PO"]) if single_data["SAP PO"] else None
                    single_rail_gmr_data.line_item = str(single_data["Line Item"]) if single_data["Line Item"] else None
                    single_rail_gmr_data.po_date = str(single_data["SAP PO Date"]) if single_data["SAP PO Date"] else None
                    single_rail_gmr_data.save()

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
        if id:
            updateSchedulerData = SelectedLocation.objects(
                id=ObjectId(id),
            ).update(name=dataName.get("name"), latlong=dataName.get("latlong"), type=dataName.get("type"), geofence=dataName.get("geofencing"))
        else:
            selectedLocationData = SelectedLocation(name=dataName.get("name"), latlong=dataName.get("latlong"), type=dataName.get("type"), geofence=dataName.get("geofencing"))
            selectedLocationData.save()
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
def endpoint_to_fetch_railway_data(response: Response, currentPage: Optional[int] = None, perPage: Optional[int] = None, search_text: Optional[str] = None, start_timestamp: Optional[str] = None, end_timestamp: Optional[str] = None, month_date: Optional[str] = None, type: Optional[str] = "display"):
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
                data &= Q(placement_date__gte = start_date)

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M","Asia/Kolkata",False)
                data &= Q(placement_date__lte = end_date)

            if search_text:
                if search_text.isdigit():
                    data &= Q(rr_no__icontains=search_text) | Q(po_no__icontains=search_text)
                else:
                    data &= (Q(source__icontains=search_text))

            if month_date:
                start_date = f'{month_date}-01'
                startd_date=datetime.datetime.strptime(f"{start_date}T00:00","%Y-%m-%dT%H:%M")
                end_date = (datetime.datetime.strptime(start_date, "%Y-%m-%d") + relativedelta(day=31)).strftime("%Y-%m-%d")
                # console_logger.debug(startd_date.strftime("%Y-%m-%dT%H:%M"))
                # console_logger.debug(f"{end_date}T23:59")
                data &= Q(placement_date__gte = startd_date.strftime("%Y-%m-%dT%H:%M"))
                data &= Q(placement_date__lte = f"{end_date}T23:59")

            offset = (page_no - 1) * page_len
            logs = (
                RailData.objects(data)
                .order_by("-placement_date")
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
        console_logger.debug("----- Fetch Railway Data Error -----",e)
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
            if single_data.get("coal_journey") == "Rail":
                railData.append(single_data.get("mine_name"))
            if single_data.get("coal_journey") == "Road":
                roadData.append(single_data.get("mine_name"))
        
        dictData["road"] = roadData
        dictData["rail"] = railData

        return dictData
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


# @router.post("/insert/rail", tags=["Railway"])
# def endpoint_to_insert_rail_data(response: Response, payload: RailwayData, rr_no: Optional[str] = None):
#     try:
#         # Extract data from payload
#         final_data = payload.dict()

#         try:
#             fetchRailData = RailData.objects.get(rr_no=rr_no)
#             # Update top-level fields in the RailData document
#             for key, value in final_data.items():
#                 if key != 'secl_rly_data' and hasattr(fetchRailData, key):
#                     setattr(fetchRailData, key, value)

#             # Update secl_rly_data
#             for new_data in final_data.get('secl_rly_data', []):
#                 updated = False
#                 for secl_data in fetchRailData.secl_rly_data:
#                     if secl_data.wagon_no == new_data['wagon_no']:
#                         for key, value in new_data.items():
#                             setattr(secl_data, key, value)
#                         updated = True
#                         break
#                 if not updated:
#                     fetchRailData.secl_rly_data.append(SeclRailData(**new_data))

#             fetchRailData.save()
#             return {"detail": "success"}
#         except DoesNotExist as e:
#             final_data = payload.dict()
#             secl_list_data = []
#             for single_data in final_data.get("secl_rly_data"):
#                 secl_rly_dict_data = {
#                     "indexing": single_data.get("indexing"),
#                     "wagon_owner": single_data.get("wagon_owner"),
#                     "wagon_type": single_data.get("wagon_type"),
#                     "wagon_no": single_data.get("wagon_no"),
#                     "secl_cc_wt": single_data.get("secl_cc_wt"),
#                     "secl_gross_wt": single_data.get("secl_gross_wt"),
#                     "secl_tare_wt": single_data.get("secl_tare_wt"),
#                     "secl_net_wt": single_data.get("secl_net_wt"),
#                     "secl_ol_wt": single_data.get("secl_ol_wt"),
#                     "secl_ul_wt":single_data.get("secl_ul_wt"),
#                     "secl_chargable_wt": single_data.get("secl_chargable_wt"),
#                     "rly_cc_wt": single_data.get("rly_cc_wt"),
#                     "rly_gross_wt": single_data.get("rly_gross_wt"),
#                     "rly_tare_wt": single_data.get("rly_tare_wt"),
#                     "rly_net_wt": single_data.get("rly_net_wt"),
#                     "rly_permissible_cc_wt": single_data.get("rly_permissible_cc_wt"),
#                     "rly_ol_wt": single_data.get("rly_ol_wt"),
#                     "rly_norm_rate": single_data.get("rly_norm_rate"),
#                     "rly_pun_rate": single_data.get("rly_pun_rate"),
#                     "rly_chargable_wt": single_data.get("rly_chargable_wt"),
#                     "rly_sliding_adjustment": single_data.get("rly_sliding_adjustment"),
#                 }
#                 secl_list_data.append(secl_rly_dict_data)
#             rail_data = RailData(
#                 rr_no=final_data.get("rr_no"),
#                 rr_qty=final_data.get("rr_qty"),
#                 po_no=final_data.get("po_no"),
#                 po_date=final_data.get("po_date"),
#                 line_item=final_data.get("line_item"),
#                 source=final_data.get("source"),
#                 placement_date=final_data.get("placement_date"),
#                 completion_date=final_data.get("completion_date"),
#                 drawn_date=final_data.get("drawn_date"),
#                 total_ul_wt=final_data.get("total_ul_wt"),
#                 boxes_supplied=final_data.get("boxes_supplied"),
#                 total_secl_gross_wt=final_data.get("total_secl_gross_wt"),
#                 total_secl_tare_wt=final_data.get("total_secl_tare_wt"),
#                 total_secl_net_wt=final_data.get("total_secl_net_wt"),
#                 total_secl_ol_wt=final_data.get("total_secl_ol_wt"),
#                 boxes_loaded=final_data.get("boxes_loaded"),
#                 total_rly_gross_wt=final_data.get("total_rly_gross_wt"),
#                 total_rly_tare_wt=final_data.get("total_rly_tare_wt"),
#                 total_rly_net_wt=final_data.get("total_rly_net_wt"),
#                 total_rly_ol_wt=final_data.get("total_rly_ol_wt"),
#                 total_secl_chargable_wt=final_data.get("total_secl_chargable_wt"),
#                 total_rly_chargable_wt=final_data.get("total_rly_chargable_wt"),
#                 freight=final_data.get("freight"),
#                 gst=final_data.get("gst"),
#                 pola=final_data.get("pola"),
#                 total_freight=final_data.get("total_freight"),
#                 source_type=final_data.get("source_type"),
#                 secl_rly_data=secl_list_data,
#             )     
#             rail_data.save()
#             return {"message": "Data inserted successfully"}

#     except Exception as e:
#         console_logger.debug("----- Fetch Report Name Error -----",e)
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#         console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
#         console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
#         return e


# Helper function to determine the rake_no
# def calculate_rake_no(month, placement_date):
#     # try:
#     # Convert strings to date objects
#     # month_start_date = datetime.strptime(month, '%b %d, %Y')  # Assuming month is in this format
#     month_start_date = datetime.datetime.strptime(month, '%Y-%m-%d')  # Assuming month is in this format
#     placement_date_obj = datetime.datetime.strptime(placement_date, '%Y-%m-%d')

#     # Calculate the 3rd date of the next month
#     next_month_start_date = month_start_date + timedelta(days=30)
#     next_month_3rd = next_month_start_date.replace(day=3)
#     console_logger.debug(month_start_date.replace(day=4))
#     console_logger.debug(placement_date_obj)
#     console_logger.debug(next_month_3rd)
#     # Check if placement_date falls between 4th of the current month and 3rd of the next month
#     if month_start_date.replace(day=4) <= placement_date_obj <= next_month_3rd:
#         return "1"
#     else:
#         return "rev1"
    # except ValueError:
    #     return "rev1"  # Default to rev1 if date parsing fails

def calculate_rake_no(month, placement_date, existing_rake_nos):
    try:
        month_start_date = datetime.datetime.strptime(month, '%Y-%m-%d')
        placement_date_obj = datetime.datetime.strptime(placement_date, '%Y-%m-%d')
        next_month_start_date = month_start_date + datetime.timedelta(days=32)
        console_logger.debug(next_month_start_date)
        next_month_3rd = next_month_start_date.replace(day=3)
        console_logger.debug(month_start_date.replace(day=4))
        console_logger.debug(placement_date_obj)
        console_logger.debug(next_month_3rd)
        console_logger.debug(existing_rake_nos)
        if month_start_date.replace(day=4) <= placement_date_obj <= next_month_3rd:
            rake_no_base = "1"
        else:
            rake_no_base = "rev1"
        filtered_rake_nos = [rake for rake in existing_rake_nos if rake is not None]
        console_logger.debug(rake_no_base)
        if "rev" in rake_no_base:
            console_logger.debug("rev is present")
            # Filter out rake numbers that start with "rev"
            rev_list = [x for x in filtered_rake_nos if x.startswith("rev")]
            if rev_list:
                # Extract the numeric part and find the maximum value
                max_rev_number = max(int(x.split("rev")[1]) for x in rev_list)
                # Increment the maximum value
                rake_no_base = f"rev{max_rev_number + 1}"
            else:
                rake_no_base = "rev1"
        else:
            console_logger.debug("rev is absent")
            number_list = [int(x) for x in filtered_rake_nos if x.isdigit()]
            if number_list:
                max_number = max(number_list)
                rake_no_base = str(max_number + 1)
            else:
                rake_no_base = "1" 
        return rake_no_base
    except Exception as e:
        console_logger.debug("----- Calculate Rake No Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.post("/insert/rail", tags=["Railway"])
def endpoint_to_insert_rail_data(response: Response, payload: RailwayData, rr_no: Optional[str] = None):
    try:
        # Extract data from payload
        final_data = payload.dict()
        console_logger.debug(rr_no)
        # console_logger.debug(final_data)
        try:
            fetchRailData = RailData.objects.get(rr_no=rr_no)
            try:
                fetchSaprecordsRail = sapRecordsRail.objects.get(rr_no=rr_no)
            except DoesNotExist as e:
                # Handle the case where sapRecordsRail object doesn't exist
                fetchSaprecordsRail = None
            
            if fetchSaprecordsRail:
                fetchRailData.month = datetime.datetime.strptime(fetchSaprecordsRail.month, '%b %d, %Y').strftime('%Y-%m-%d')
                fetchRailData.rr_date = fetchSaprecordsRail.rr_date
                fetchRailData.siding = fetchSaprecordsRail.siding
                fetchRailData.mine = fetchSaprecordsRail.mine
                fetchRailData.grade = fetchSaprecordsRail.grade
                fetchRailData.rr_qty = fetchSaprecordsRail.rr_qty
                fetchRailData.po_amount = fetchSaprecordsRail.po_amount
            # Update top-level fields in the RailData document
            for key, value in final_data.items():
                if key != 'secl_rly_data' and hasattr(fetchRailData, key):
                    setattr(fetchRailData, key, value)
            
            # if fetchRailData.placement_date:
            #     console_logger.debug(fetchRailData.placement_date)
            #     console_logger.debug(datetime.datetime.strptime(fetchSaprecordsRail.month, '%b %d, %Y').strftime('%Y-%m-%d'))
            #     console_logger.debug(datetime.datetime.strptime(fetchRailData.placement_date, '%Y-%m-%dT%H:%M').strftime('%Y-%m-%d'))
            #     # Set rake_no based on month and placement_date
            #     fetchRailData.rake_no = calculate_rake_no(datetime.datetime.strptime(fetchSaprecordsRail.month, '%b %d, %Y').strftime('%Y-%m-%d'), fetchRailData.placement_date.strftime('%Y-%m-%d'))

            console_logger.debug(final_data)
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
            listAveryData = []
            for new_data in final_data.get('secl_rly_data', []):
                dictAveryData = {}
                console_logger.debug(new_data)
                dictAveryData["indexing"] = new_data.get("indexing")
                dictAveryData["wagon_owner"] = new_data.get("wagon_owner")
                dictAveryData["wagon_type"] = new_data.get("wagon_type")
                dictAveryData["wagon_no"] = new_data.get("wagon_no")
                listAveryData.append(AveryRailData(**dictAveryData))
        
            fetchRailData.avery_rly_data = listAveryData
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

            avery_list_data = []
            for single_data in final_data.get("secl_rly_data"):
                avery_rly_dict_data = {
                    "indexing": single_data.get("indexing"),
                    "wagon_owner": single_data.get("wagon_owner"),
                    "wagon_type": single_data.get("wagon_type"),
                    "wagon_no": single_data.get("wagon_no"),
                }
                avery_list_data.append(avery_rly_dict_data)

            try:
                fetchSaprecordsRail = sapRecordsRail.objects.get(rr_no=final_data.get("rr_no"))
            except DoesNotExist as e:
                # Handle the case where sapRecordsRail object doesn't exist
                fetchSaprecordsRail = None
            console_logger.debug(fetchSaprecordsRail)
            rail_data = RailData(
                rr_no=final_data.get("rr_no"),
                # rr_qty=final_data.get("rr_qty"),
                rr_qty=fetchSaprecordsRail.rr_qty if fetchSaprecordsRail and fetchSaprecordsRail.rr_qty else "",
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
                avery_rly_data=avery_list_data,
                month=datetime.datetime.strptime(fetchSaprecordsRail.month, '%b %d, %Y').strftime('%Y-%m-%d') if fetchSaprecordsRail and fetchSaprecordsRail.month else "",
                rr_date=fetchSaprecordsRail.rr_date if fetchSaprecordsRail and fetchSaprecordsRail.rr_date else "",
                siding=fetchSaprecordsRail.siding if fetchSaprecordsRail and fetchSaprecordsRail.siding else "",
                mine=fetchSaprecordsRail.mine if fetchSaprecordsRail and fetchSaprecordsRail.mine else "",
                grade=fetchSaprecordsRail.grade if fetchSaprecordsRail and fetchSaprecordsRail.grade else "",
                # rr_qty=fetchSaprecordsRail.get("rr_qty") if fetchSaprecordsRail.get("rr_qty") else "",
                po_amount=fetchSaprecordsRail.po_amount if fetchSaprecordsRail and fetchSaprecordsRail.po_amount else "",
            ) 
            existing_rake_nos = [data.rake_no for data in RailData.objects()]
            console_logger.debug(existing_rake_nos)
            console_logger.debug(final_data.get("placement_date"))
            if final_data.get("placement_date") and fetchSaprecordsRail and fetchSaprecordsRail.month:
                console_logger.debug(final_data.get("placement_date"))
                console_logger.debug(datetime.datetime.strptime(fetchSaprecordsRail.month, '%b %d, %Y').strftime('%Y-%m-%d'))
                placement_date_obj = datetime.datetime.strptime(final_data.get("placement_date"), '%Y-%m-%dT%H:%M')
                console_logger.debug(existing_rake_nos)
                # Set rake_no based on month and placement_date
                rail_data.rake_no = calculate_rake_no(datetime.datetime.strptime(fetchSaprecordsRail.month, '%b %d, %Y').strftime('%Y-%m-%d'), placement_date_obj.strftime('%Y-%m-%d'), existing_rake_nos) 
            #Set rake_no based on month and placement_date
            # rail_data.rake_no = calculate_rake_no(fetchSaprecordsRail.month, final_data.get("placement_date")) 
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
        headers_data = {
            'accept': 'application/json',
        }

        params = {
            'start_date': start_date,
            'end_date': end_date,
        }
        
        try:
            response = requests.get(f'http://{ip}/api/v1/host/bunker_extract_data', params=params, headers=headers_data)
            data = json.loads(response.text)
            console_logger.debug(data)
            for item in data["Data"]:
                if item["Data"] is not None:
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
                else:
                    success = "No data found"
                    console_logger.debug("No data found")
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
        # console_logger.debug(f"-------- Bunker Graph Response -------- {result}")
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
        response = DataExecutionsHandler.bunker_coal_analysis(specified_date=specified_date)
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
        response = DataExecutionsHandler.bunker_coal_data(currentPage=currentPage, perPage=perPage, start_timestamp=start_timestamp, end_timestamp=end_timestamp, search_text=search_text, type=type, date=date)
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
        response = DataExecutionsHandler.fetchcoalBunkerData(start_date=start_date, end_date=end_date)
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


@router.get("/fetch/testing/bunker", tags=["extra"])
def fetch_bunker_data_test(response: Response):
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
def generate_email(response: Response, email:dict):
    try:
        url = f"http://{ip}/api/v1/host/send-email/"

        headers = {'Content-Type': 'application/json'}

        payload = json.dumps(email)
        response = requests.request("POST", url, headers=headers, data=payload)
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
        payload = data.dict()
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
                    # console_logger.debug(send_data)
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
        timezone = pytz.timezone('Asia/Kolkata')
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
            outputDict['mine_name'] = data.get("_id")
            outputDict['vehicle_count'] = data.get("vehicle_count")
            listdata.append(outputDict)


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
                if checkEmailDevelopment[0].development == "local":
                    console_logger.debug("inside local")
                    send_email(fetch_email.get("Smtp_user"), subject, fetch_email.get("Smtp_password"), fetch_email.get("Smtp_host"), fetch_email.get("Smtp_port"), reportSchedule[7].recipient_list, body, "", reportSchedule[7].cc_list, reportSchedule[7].bcc_list)
                elif checkEmailDevelopment[0].development == "prod":
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
                    # console_logger.debug(send_data)
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


# @router.post("/insert/shift/schedule", tags=["scheduler"])
# def endpoint_to_insert_shift_schedule(response: Response, data: ShiftMainData, report_name: str):
#     try:
#         inputData = data.dict()
#         fetchAllScheduler = shiftScheduler.objects(report_name=report_name)
#         if fetchAllScheduler:
#             fetchAllScheduler.delete()
        
#         if fetchAllScheduler.time != "":
#             time_format = "%H:%M"
#             given_time = datetime.datetime.strptime(fetchAllScheduler.time, time_format)

#             time_to_subtract = datetime.timedelta(hours=5, minutes=30)

#             new_time = given_time - time_to_subtract
#             new_time_str = new_time.strftime(time_format)
#             hh, mm = new_time_str.split(":")

#         for single_data in inputData.get("data"):
#             if single_data.get("filter") == "shift_schedule":
#                 shiftScheduler(shift_name = single_data.get('shift_name'), start_shift_time = single_data.get("start_shift_time"), end_shift_time = single_data.get("end_shift_time"), scheduling=single_data.get("scheduling"), report_name=report_name).save()
#                 console_logger.debug(single_data.get('shift_name'))
#                 console_logger.debug(single_data.get("start_shift_time"))
#                 console_logger.debug(single_data.get("end_shift_time"),)
#                 # Parse end_shift_time
#                 end_shift_time = datetime.datetime.strptime(single_data.get("end_shift_time"), time_format)
#                 # Adjust for timezone by subtracting the specified duration
#                 end_shift_time_ist = end_shift_time - time_to_subtract
#                 # Convert the adjusted time back to hours and minutes
#                 end_shift_hh, end_shift_mm = end_shift_time_ist.strftime(time_format).split(":")
#                 # Schedule the background task
#                 backgroundTaskHandler.run_job(
#                     task_name=single_data.get('shift_name'),
#                     func=bunker_scheduler,
#                     trigger="cron",
#                     **{"day": "*", "hour": end_shift_hh, "minute": end_shift_mm}, 
#                     func_kwargs={
#                         "shift_name": single_data.get('shift_name'), 
#                         "start_time": single_data.get("start_shift_time"), 
#                         "end_time": single_data.get("end_shift_time")
#                     }
#                 ) 
#             elif single_data.get("filter") == "daily":

#                 backgroundTaskHandler.run_job(task_name=report_name, func=send_report_generate, trigger="cron", **{"day": "*", "hour": hh, "minute": mm}, func_kwargs={"report_name":payload.report_name}, max_instances=1)
#                 # backgroundTaskHandler.run_job(task_name=reportScheduler.report_name, func=send_report_generate, trigger="cron", **{"day": "*", "second": 2})
#             elif single_data.get("filter") == "weekly":
#                 # backgroundTaskHandler.run_job(task_name=reportScheduler.report_name, func=send_report_generate, trigger="cron", **{"week": reportScheduler.schedule}) # week (int|str) - ISO week (1-53)
#                 backgroundTaskHandler.run_job(task_name=report_name, func=send_report_generate, trigger="cron", **{"day_of_week": reportScheduler.schedule, "hour": hh, "minute": mm}, func_kwargs={"report_name":payload.report_name}, max_instances=1)
#             elif single_data.get("filter") == "monthly":
#                 # backgroundTaskHandler.run_job(task_name=reportScheduler.report_name, func=send_report_generate, trigger="cron", **{"month": reportScheduler.schedule}) # month (int|str) - month (1-12)
#                 backgroundTaskHandler.run_job(task_name=report_name, func=send_report_generate, trigger="cron", **{"day": reportScheduler.schedule, "hour": hh, "minute": mm}, func_kwargs={"report_name":payload.report_name}, max_instances=1)
        
#         return {"details": "success"}
#     except Exception as e:
#         console_logger.debug("----- Email Generation Error -----",e)
#         response.status_code = 400
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#         console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
#         console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
#         return e
    
@router.post("/insert/shift/schedule", tags=["scheduler"])
def endpoint_to_insert_shift_schedule(response: Response, data: ShiftMainData, report_name: str):
    try:
        dataName = data.dict()
        for single_data in dataName.get("data"):
            try:
                shiftSchedulerData = shiftScheduler.objects.get(report_name=report_name, shift_name=single_data.get("shift_name"))
                shiftSchedulerData.start_shift_time = single_data.get("start_shift_time")
                shiftSchedulerData.end_shift_time = single_data.get("end_shift_time")
                shiftSchedulerData.filter = single_data.get("filter")
                shiftSchedulerData.schedule = single_data.get("schedule")
                shiftSchedulerData.time = single_data.get("time")
                shiftSchedulerData.duration = single_data.get("duration")
                shiftSchedulerData.save()

            except DoesNotExist as e:
                shiftSchedulerData = shiftScheduler(report_name=report_name,shift_name=single_data.get("shift_name"), start_shift_time=single_data.get("start_shift_time"), end_shift_time=single_data.get("end_shift_time"), filter = single_data.get("filter"), schedule = single_data.get("schedule"), time=single_data.get("time"), duration=single_data.get("duration"))
                shiftSchedulerData.save()
        
            time_format = "%H:%M"

            time_to_subtract = datetime.timedelta(hours=5, minutes=30)
            console_logger.debug(single_data.get("filter"))
            if single_data.get("filter") == "daily" or single_data.get("filter") == "weekly" or single_data.get("filter") == "monthly":
                if single_data.get("duration") != "":
                    splitDuration = single_data.get("duration").split(":")
                    hours = int(splitDuration[1])
                    minutes = int(splitDuration[2])
                    trigger_time = datetime.datetime.strptime(shiftSchedulerData.time, "%H:%M")
                    duration_timedelta = timedelta(hours=hours, minutes=minutes)
                    calculation_time = trigger_time - duration_timedelta
                    given_time = datetime.datetime.strptime(calculation_time.strftime("%H:%M"), time_format)
                    new_time = given_time - time_to_subtract
                    new_time_str = new_time.strftime(time_format)
                    hh, mm = new_time_str.split(":")
                else:
                    given_time = datetime.datetime.strptime(shiftSchedulerData.time, time_format)
                    new_time = given_time - time_to_subtract
                    new_time_str = new_time.strftime(time_format)
                    hh, mm = new_time_str.split(":")

            if single_data.get("filter") == "shift_schedule":
                console_logger.debug(single_data.get('shift_name'))
                console_logger.debug(single_data.get("start_shift_time"))
                console_logger.debug(single_data.get("end_shift_time"),)
                # Parse end_shift_time
                end_shift_time = datetime.datetime.strptime(single_data.get("end_shift_time"), time_format)
                # Adjust for timezone by subtracting the specified duration
                end_shift_time_ist = end_shift_time - time_to_subtract
                # Convert the adjusted time back to hours and minutes
                end_shift_hh, end_shift_mm = end_shift_time_ist.strftime(time_format).split(":")
                # Schedule the background task
                backgroundTaskHandler.run_job(
                    task_name=single_data.get('shift_name'),
                    func=bunker_scheduler,
                    trigger="cron",
                    **{"day": "*", "hour": end_shift_hh, "minute": end_shift_mm}, 
                    func_kwargs={
                        "shift_name": single_data.get('shift_name'), 
                        "start_time": single_data.get("start_shift_time"), 
                        "end_time": single_data.get("end_shift_time")
                    }
                ) 
            elif single_data.get("filter") == "daily":
                console_logger.debug(hh)
                console_logger.debug(mm)
                backgroundTaskHandler.run_job(task_name=report_name, func=shiftSchedulerfunc, trigger="cron", **{"day": "*", "hour": hh, "minute": mm}, func_kwargs={"report_name":report_name, "duration": single_data.get("duration")}, max_instances=1)
                # backgroundTaskHandler.run_job(task_name=reportScheduler.report_name, func=send_report_generate, trigger="cron", **{"day": "*", "second": 2})
            elif single_data.get("filter") == "weekly":
                backgroundTaskHandler.run_job(task_name=report_name, func=shiftSchedulerfunc, trigger="cron", **{"day_of_week": shiftSchedulerData.schedule, "hour": hh, "minute": mm}, func_kwargs={"report_name":report_name, "duration": single_data.get("duration")}, max_instances=1)
            elif single_data.get("filter") == "monthly":
                backgroundTaskHandler.run_job(task_name=report_name, func=shiftSchedulerfunc, trigger="cron", **{"day": shiftSchedulerData.schedule, "hour": hh, "minute": mm}, func_kwargs={"report_name":report_name, "duration": single_data.get("duration")}, max_instances=1)
        
        return {"details": "success"}
    except Exception as e:
        console_logger.debug("----- Email Generation Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

def shiftSchedulerfunc(**kwargs):
    try:
        console_logger.debug(("scheduler report generate",kwargs))
        if kwargs["report_name"] == "save_coalextract_data":
            durationSplit = kwargs["duration"].split(":")
            end_date = datetime.date.today().strftime("%Y-%m-%d")
            start_date = (datetime.date.today() - timedelta(int(f"{durationSplit[0]}"))).strftime("%Y-%m-%d")
            coal_test(start_date=start_date, end_date=end_date)
        elif kwargs["report_name"] == "update_coalgcv_data":
            endpoint_to_fetch_coal_quality_gcv()
        elif kwargs["report_name"] == "extract_bunker_data":
            todays_date = datetime.date.today().strftime("%Y-%m-%d")
            DataExecutionsHandler = DataExecutions()
            DataExecutionsHandler.fetchcoalBunkerData(start_date=todays_date, end_date=todays_date)
    
        return "success"

    except Exception as e:
        console_logger.debug("----- Shift Scheduler Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

@router.get("/sync/limbs/bombcalorimeter", tags=["Coal Testing"])
def endpoint_to_sync_limbs_bombcalorimter(response: Response, start_date: Optional[str] = None, end_date: Optional[str] = None):
    try:

        coal_test(start_date=start_date, end_date=end_date)
        endpoint_to_fetch_coal_quality_gcv()
        return {"detail": "success"}
    except Exception as e:
        console_logger.debug("----- Email Generation Error -----",e)
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
        start_shift_hh, start_shift_mm = kwargs["start_time"].split(":")
        end_shift_hh, end_shift_mm = kwargs["end_time"].split(":")
        start_date = datetime.datetime.now().strftime("%Y-%m-%d")
        start_ddate = f"{start_date}T{start_shift_hh}:{start_shift_mm}:00"
        end_ddate = f"{start_date}T{end_shift_hh}:{end_shift_mm}:00"
        console_logger.debug(kwargs["shift_name"])
        save_bunker_data(start_ddate, end_ddate, kwargs["shift_name"])
    except Exception as e:
        console_logger.debug("----- Email Generation Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

@router.get("/coal/qualitygcv", tags=["Coal Testing"])
def endpoint_to_fetch_coal_quality_gcv():
    try:
        DataExecutionsHandler = DataExecutions()
        response = DataExecutionsHandler.fetch_coal_quality_gcv()
        return response
    except Exception as e:
        console_logger.debug("----- Coal Quality GCV Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.post("/insert/scheduler/shifts", tags=["Coal Testing"])
def endpoint_to_insert_shifts_scheduler(response: Response, shift_scheduler: str):
    try:
        DataExecutionsHandler = DataExecutions()
        response = DataExecutionsHandler.insertShiftScheduler(shift_scheduler=shift_scheduler)
        return response
    except Exception as e:
        console_logger.debug("----- Insert Shift Scheduler Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

@router.get("/fetch/scheduler/shifts", tags=["Coal Testing"])
def endpoint_to_fetch_shifts_scheduler(response: Response):
    try:
        DataExecutionsHandler = DataExecutions()
        response = DataExecutionsHandler.fetchShiftScheduler()
        return response
    except Exception as e:
        console_logger.debug("----- Fetch Shift Scheduler Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

def process_today_data(specified_date):
    try:
        today = specified_date
        today_start = f'{today} 00:00:00'
        today_end = f'{today} 23:59:59'
        today_start = datetime.datetime.strptime(today_start, "%Y-%m-%d %H:%M:%S")
        today_end = datetime.datetime.strptime(today_end, "%Y-%m-%d %H:%M:%S")
        pipeline = [
            {
                "$match": {
                    "created_date": {
                        "$gte": today_start,
                        "$lt": today_end
                    },
                    "tagid": { "$in": [2, 16, 3536, 3538] }
                }
            },
            {
                "$project": {
                    "hour": { "$hour": "$created_date" },
                    "tagid": 1,
                    "sum": { "$toDouble": "$sum" }
                }
            },
            {
                "$group": {
                    "_id": {
                        "hour": "$hour",
                        "tagid": "$tagid"
                    },
                    "total_sum": { "$sum": "$sum" }
                }
            },
            {
                "$sort": { "_id.hour": 1 }
            }
        ]

        results = list(Historian.objects.aggregate(pipeline))

        unit_data = {
            "Unit 1": {
                "label": list(range(24)),
                "generation_tag": [0] * 24,
                "consumption_tag": [0] * 24,
                "specific_coal": [0] * 24,
                "total_generation_sum": 0,
                "total_consumption_sum": 0,
                "total_specific_coal": 0
            },
            "Unit 2": {
                "label": list(range(24)),
                "generation_tag": [0] * 24,
                "consumption_tag": [0] * 24,
                "specific_coal": [0] * 24,
                "total_generation_sum": 0,
                "total_consumption_sum": 0,
                "total_specific_coal": 0
            }
        }

        for result in results:
            hour = result["_id"]["hour"]
            tag_id = result["_id"]["tagid"]
            sum_value = result["total_sum"]

            if tag_id == 2:
                unit_data["Unit 1"]["generation_tag"][hour] = sum_value
                unit_data["Unit 1"]["total_generation_sum"] += sum_value
            elif tag_id == 16:
                unit_data["Unit 1"]["consumption_tag"][hour] = sum_value
                unit_data["Unit 1"]["total_consumption_sum"] += sum_value
            elif tag_id == 3536:
                unit_data["Unit 2"]["generation_tag"][hour] = sum_value
                unit_data["Unit 2"]["total_generation_sum"] += sum_value
            elif tag_id == 3538:
                unit_data["Unit 2"]["consumption_tag"][hour] = sum_value
                unit_data["Unit 2"]["total_consumption_sum"] += sum_value
        
        for unit in ["Unit 1", "Unit 2"]:
            for hour in range(24):
                generation = unit_data[unit]["generation_tag"][hour]
                consumption = unit_data[unit]["consumption_tag"][hour]
                if generation > 0:
                    unit_data[unit]["specific_coal"][hour] = round(consumption / generation, 2)
                    unit_data[unit]["total_specific_coal"] += round(consumption / generation, 2)
                else:
                    unit_data[unit]["specific_coal"][hour] = 0
                    unit_data[unit]["total_specific_coal"] += 0

        return unit_data
    except Exception as e:
        console_logger.debug("----- Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

@router.get("/coal_consumption_pdf_report", tags=["PDF Report"])
def endpoint_to_generate_coal_consumption_report(response: Response, specified_date: Optional[str]=None):
    try:
        fetchtableData = process_today_data(specified_date)
        fetchData = generate_report_consumption(specified_date, fetchtableData)
        return fetchData
    except Exception as e:
        console_logger.debug("----- Coal Consumption PDF Report Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

    
@router.post("/tarealert", tags=["Email Alert"])
def endpoint_to_send_tare_email_alert(response: Response, data: RequestData):
    try:
        payload = data.dict()
        reportSchedule = ReportScheduler.objects()
        if reportSchedule[10].active == False:
            console_logger.debug("scheduler is off")
            return {"detail": "scheduler is off"}
        elif reportSchedule[10].active == True:
            console_logger.debug("inside Tare Email Alert")
            response_code, fetch_email = fetch_email_data()
            if response_code == 200:
                console_logger.debug(reportSchedule[7].recipient_list)
                subject = f"Tare Alert {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                title_data = "<b>Tare Weight is not in between +/-500kg</b>"
                body = f"""
                        <b>Tare Alert: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S').strftime('%d %B %Y %H:%M:%S')}</b>
                        <br>
                        <br>
                        <!doctype html>
                        <html>
                        <head>
                            <meta charset="utf-8">
                            <title>Tare Alert</title>
                        </head>
                        <body>
                            {title_data}
                            <br>
                            <br>
                            <table border='1'>
                                <tr>
                                    <th>Mine</th>
                                    <th>Vehicle No</th>
                                    <th>Delivery Challan No</th>
                                    <th>DO No</th>
                                    <th>Vehicle Chassis No</th>
                                    <th>Fitness Expiry</th>
                                    <th>DC Date</th>
                                    <th>Challan Net Wt(MT)</th>
                                    <th>Challan Tare Wt(MT)</th>
                                    <th>GWEL Tare Wt(MT)</th>
                                    <th>Total Net Amount</th>
                                </tr>
                                <tr>
                                    <td>{payload.get('Mine_Name')}</td>
                                    <td>{payload.get('Vehicle_Truck_Registration_No')}</td>
                                    <td>{payload.get('Delivery_Challan_Number')}</td>
                                    <td>{payload.get('ARV_Cum_DO_Number')}</td>
                                    <td>{payload.get('Chassis_No')}</td>
                                    <td>{payload.get('Certificate_will_expire_on')}</td>
                                    <td>{payload.get('Delivery_Challan_Date')}</td>
                                    <td>{payload.get('Net_Qty')}</td>
                                    <td>{payload.get('Tare_Qty')}</td>
                                    <td>{payload.get('Actual_Tare_Qty')}</td>
                                    <td>{payload.get('Total_Net_Amount_of_Figures')}</td>
                                </tr>
                            </table>
                        </body>
                        </html>"""
                checkEmailDevelopment = EmailDevelopmentCheck.objects()
                if checkEmailDevelopment[0].development == "local":
                    console_logger.debug("inside local")
                    send_email(fetch_email.get("Smtp_user"), subject, fetch_email.get("Smtp_password"), fetch_email.get("Smtp_host"), fetch_email.get("Smtp_port"), reportSchedule[10].recipient_list, body, "", reportSchedule[10].cc_list, reportSchedule[10].bcc_list)
                elif checkEmailDevelopment[0].development == "prod":
                    console_logger.debug("inside prod")
                    send_data = {
                        "sender_email": fetch_email.get("Smtp_user"),
                        "subject": subject,
                        "password": fetch_email.get("Smtp_password"),
                        "smtp_host": fetch_email.get("Smtp_host"),
                        "smtp_port": fetch_email.get("Smtp_port"),
                        "receiver_email": reportSchedule[10].recipient_list,
                        "body": body,
                        "file_path": "",
                        "cc_list": reportSchedule[10].cc_list,
                        "bcc_list": reportSchedule[10].bcc_list
                    }
                    # console_logger.debug(send_data)
                    generate_email(Response, email=send_data)
        return {"detail": "success"}
    except Exception as e:
        console_logger.debug("----- Coal Consumption PDF Report Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


# Helper function to handle regex extraction with error handling
def extract_with_regex(pattern, text, group_index=1):
    try:
        match = re.search(pattern, text, re.MULTILINE)
        if match:
            return match.group(group_index).strip()
        return None
    except Exception as e:
        console_logger.debug("----- Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

def extract_with_regex_scheme(text):
    try:
        pattern = r"([\w\s\(\)\-]+)\s*Scheme Name\s*:"
        match = re.search(pattern, text, re.DOTALL)
        if match:
            text=' '.join(match.group(1).split()).strip()
            return text
        return None
    except Exception as e:
        console_logger.debug("----- Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


# Function to extract specific fields using regex
def extract_fields(pdf_path):
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
        fields = {}
        # Adjusted regex patterns for more accurate extraction
        fields["do_no"] = extract_with_regex(r'(\d+)\s*Sales Order Number', text)
        if fields["do_no"]:        
            fields["do_date"] = extract_with_regex(r"([\w\s,]+)(?=\s*Sales Order Date)", text)
            fields["start_date"] = extract_with_regex(r"([\w\s,]+)(?=\s*Sales Order Valid From)", text)
            fields["end_date"] = extract_with_regex(r"([\w\s,]+)(?=\s*Sales Order Valid To)", text)
            fields["slno"] = extract_with_regex(r"([\w\s,]+)(?=\s*Month)", text)
            if fields["slno"]:
                fields["slno"]
            else:
                fields["slno"] = extract_with_regex(r'Month\s*:\s*(\d+)', text)
            # fields["Scheme Name"] = extract_with_regex(r"(.*?)\s*Scheme Name\s*:", text) 
            fields["consumer_type"] = extract_with_regex_scheme(text) 

            fields["grade"] = extract_with_regex(r"(\w+)\s+Grade Desc\s*:", text)
            fields["Size"] = extract_with_regex(r"([\-\d\s\w]+)\s+Size\s*:", text)        
            # Improved pattern for Plant extraction
            fields["mine"] = extract_with_regex(r'Line Item Plant Material Material Description HSN Code Unit of Measure Quantity\s*\n10\s+([^\d]+)', text)
            
            # Extract Line Item and Quantity more generally
            fields["line_item"] = extract_with_regex(r'Line Item\s+Plant\s+Material.*\n(\d+)', text)
            fields["do_qty"] = extract_with_regex(r'\b(\d{1,3}(?:,\d{3})*)\b\s*$', text)        
            # New field: Total Net Amount
            fields["po_amount"] = extract_with_regex(r"Total\s+([\d,]+\.\d{2})", text)   
        else:        
            fields["do_no"] = extract_with_regex(r'Sales Order Number\s+:\s+(\d+)', text)
            fields["do_date"] = extract_with_regex(r'Sales Order Date\s+:\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})', text)
            fields["start_date"] = extract_with_regex(r'Sales Order Valid From\s+:\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})', text)
            fields["end_date"] = extract_with_regex(r'Sales Order Valid To\s+:\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})', text)
            fields["slno"] = extract_with_regex(r"([\w\s,]+)(?=\s*Month)", text)
            if fields["slno"]:
                fields["slno"]
            else:
                fields["slno"] = extract_with_regex(r'Month\s*:\s*(\d+)', text)
            fields["consumer_type"] = extract_with_regex(r"Scheme Name\s*:\s*(.*\s*.*)", text) 
            fields["consumer_type"] = re.sub(r'\n', '', fields["consumer_type"])
            fields["grade"] = extract_with_regex(r"(?i)Grade Desc\s*:\s*(.*)" , text)
            fields["Size"] = extract_with_regex(r"Size\s*:\s*(\S+)\s*", text)+" MM"        
            # Improved pattern for Plant extraction
            fields["mine"] = extract_with_regex(r'Line Item Plant Material Material Description HSN Code Unit of Measure Quantity\s*\n10\s+([^\d]+)', text)
            # Extract Line Item and Quantity more generally
            fields["line_item"] = extract_with_regex(r'Line Item\s+Plant\s+Material.*\n(\d+)', text)
            fields["do_qty"] = extract_with_regex(r'\b(\d{1,3}(?:,\d{3})*)\b\s*$', text)        
            # New field: Total Net Amount
            fields["po_amount"] = extract_with_regex(r"Total\s+([\d,]+\.\d{2})", text)
        return fields
    except Exception as e:
        console_logger.debug("----- Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.post("/road/sapupload", tags=["Coal Testing"])
async def endpoint_to_upload_sap_data(response: Response, pdf_upload: Optional[UploadFile] = File(None)):
    try:
        if pdf_upload is None:
            return {"error": "No file uploaded"}
        contents = await pdf_upload.read()

        # Check if the file is empty
        if not contents:
            return {"error": "Uploaded file is empty"}

        # Verify file format (PDF)
        if not pdf_upload.filename.endswith(('.pdf','.PDF')):
            return {"error": "Uploaded file is not a PDF"}
        
        file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
        target_directory = f"static_server/gmr_ai/{file}"
        os.umask(0)
        os.makedirs(target_directory, exist_ok=True, mode=0o777)

        file_extension = pdf_upload.filename.split(".")[-1]
        file_name = f'sap_upload_{datetime.datetime.now().strftime("%Y-%m-%d:%H:%M")}.{file_extension}'
        full_path = os.path.join(os.getcwd(), target_directory, file_name)
        with open(full_path, "wb") as file_object:
            file_object.write(contents)

        fetchPdfData = extract_fields(full_path)

        if fetchPdfData:
            try:
                checkSaprecords = SapRecords.objects.get(do_no=fetchPdfData.get("do_no"))
                checkSaprecords.do_date = datetime.datetime.strptime(fetchPdfData.get("do_date"), '%b %d, %Y').strftime('%Y-%m-%d')
                checkSaprecords.start_date = datetime.datetime.strptime(fetchPdfData.get("start_date"), '%b %d, %Y').strftime('%Y-%m-%d')
                checkSaprecords.end_date = datetime.datetime.strptime(fetchPdfData.get("end_date"), '%b %d, %Y').strftime('%Y-%m-%d')
                checkSaprecords.slno = fetchPdfData.get("slno")
                checkSaprecords.consumer_type = fetchPdfData.get("consumer_type")
                checkSaprecords.grade = f'{fetchPdfData.get("grade")} {fetchPdfData.get("size")}'
                checkSaprecords.mine_name = fetchPdfData.get("mine")
                # checkSaprecords.line_item = fetchPdfData.get("line_item")
                checkSaprecords.do_qty = fetchPdfData.get("do_qty").replace(",", "")
                checkSaprecords.po_amount = fetchPdfData.get("po_amount")
                checkSaprecords.save()

            except DoesNotExist as e:
                # insertSapRecords = SapRecords(do_no=fetchPdfData.get("do_no"), do_date=fetchPdfData.get("do_date"), start_date=datetime.datetime.strptime(fetchPdfData.get("start_date"), '%b %d, %Y').strftime('%Y-%m-%d'), end_date=datetime.datetime.strptime(fetchPdfData.get("end_date"), '%b %d, %Y').strftime('%Y-%m-%d'), slno=fetchPdfData.get("slno"), consumer_type=fetchPdfData.get("consumer_type"), grade=f'{fetchPdfData.get("grade")} {fetchPdfData.get("size")}', mine_name=fetchPdfData.get("mine"), line_item=fetchPdfData.get("line_item"), do_qty=fetchPdfData.get("do_qty").replace(",", ""), po_amount=fetchPdfData.get("po_amount"))
                insertSapRecords = SapRecords(do_no=fetchPdfData.get("do_no"), do_date=fetchPdfData.get("do_date"), start_date=datetime.datetime.strptime(fetchPdfData.get("start_date"), '%b %d, %Y').strftime('%Y-%m-%d'), end_date=datetime.datetime.strptime(fetchPdfData.get("end_date"), '%b %d, %Y').strftime('%Y-%m-%d'), slno=fetchPdfData.get("slno"), consumer_type=fetchPdfData.get("consumer_type"), grade=f'{fetchPdfData.get("grade")} {fetchPdfData.get("size")}', mine_name=fetchPdfData.get("mine"), do_qty=fetchPdfData.get("do_qty").replace(",", ""), po_amount=fetchPdfData.get("po_amount"))
                insertSapRecords.save()

            
            try:
                checkGmrData = Gmrdata.objects(arv_cum_do_number=fetchPdfData.get("do_no"))
                for singleCheckGmrData in checkGmrData:
                    singleCheckGmrData.do_date = datetime.datetime.strptime(fetchPdfData.get("do_date"), '%b %d, %Y').strftime('%Y-%m-%d')
                    singleCheckGmrData.start_date = datetime.datetime.strptime(fetchPdfData.get("start_date"), '%b %d, %Y').strftime('%Y-%m-%d')
                    singleCheckGmrData.end_date = datetime.datetime.strptime(fetchPdfData.get("end_date"), '%b %d, %Y').strftime('%Y-%m-%d')
                    singleCheckGmrData.slno = fetchPdfData.get("slno")
                    singleCheckGmrData.type_consumer = fetchPdfData.get("consumer_type")
                    singleCheckGmrData.grade = f'{fetchPdfData.get("grade")} {fetchPdfData.get("size")}'
                    singleCheckGmrData.mine = fetchPdfData.get("mine")
                    # singleCheckGmrData.line_item = fetchPdfData.get("line_item")
                    singleCheckGmrData.po_qty = fetchPdfData.get("do_qty").replace(",", "")
                    singleCheckGmrData.po_amount = fetchPdfData.get("po_amount")
                    singleCheckGmrData.save()
            except DoesNotExist as e:
                pass

        return {"detail": "success"}
    except Exception as e:
        console_logger.debug("----- Road Sap Upload Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


# @router.get("/fetch/rake/quota", tags=["Rail Map"])
# def end_point_to_fetch_rake_quota_test(response: Response, 
#                 currentPage: Optional[int] = None,
#                 perPage: Optional[int] = None,
#                 # search_text: Optional[str] = None,
#                 month_date: Optional[str] = None,
#                 start_timestamp: Optional[str] = None,
#                 end_timestamp: Optional[str] = None,
#                 type: Optional[str] = "display"):
#     try:
#         data = Q()
#         result = {        
#                 "labels": [],
#                 "datasets": [],
#                 "total": 0,
#                 "page_size": 15
#         }

#         if type and type == "display":

#             page_no = 1
#             page_len = result["page_size"]

#             if currentPage:
#                 page_no = currentPage

#             if perPage:
#                 page_len = perPage
#                 result["page_size"] = perPage

#             if month_date:
#                 month_check = datetime.datetime.strptime(month_date, "%Y-%m").strftime("%m-%Y")
#                 data &= Q(month__iexact = month_check)

#             if start_timestamp:
#                 start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M", "Asia/Kolkata", False)
#                 data &= Q(created_at__gte=start_date)

#             if end_timestamp:
#                 end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M", "Asia/Kolkata", False)
#                 data &= Q(created_at__lte=end_date)

#             offset = (page_no - 1) * page_len
#             logs = (
#                 rakeQuota.objects(data)
#                 .order_by("year", "month")
#                 .skip(offset)
#                 .limit(page_len)
#             )
#             listData = []
#             if logs:
#                 for log in logs:
#                     # dictData = {"SrNo": 0, "month": "", 'valid_upto': "", "rake_alloted": "", "source_type": "","rakes_loaded_till_date": 0, "rakes_loaded_on_date": 0, "previous_month_rake": 0, "rakes_received_on_date": 0, "total_rakes_received_for_month": 0, "balance_rakes_to_receive": 0, "no_of_rakes_in_transist": 0, "rakes_previous_month_quota_received": 0}
#                     dictData = {"month": "", "source_type": "", "rakes_previous_month_quota_received": 0, "rake_planned_for_the_month": "","rakes_loaded_till_date": 0, "rakes_loaded_on_date": 0, "previous_month_rake": 0, "rakes_received_on_date": 0, "total_rakes_received_for_month": 0, "balance_rakes_to_receive": 0, "no_of_rakes_in_transist": 0, }
#                     rake_year = log.year
#                     rake_month = log.month
#                     # Convert rake_month to match RailData drawn_date format
#                     month_year = f"{rake_year}-{rake_month[:2].upper()}"
#                     # date_obj = datetime.datetime.strptime(month_year, "%Y-%b")
#                     date_obj = datetime.datetime.strptime(month_year, "%Y-%m")

#                     # Get the previous month
#                     prev_date_obj = date_obj - datetime.timedelta(days=1)
#                     # prev_month_year = prev_date_obj.strftime("%Y-%b")
#                     prev_month_year = prev_date_obj.strftime("%Y-%m")

#                     # Format the date object to the desired format
#                     formatted_date = date_obj.strftime("%Y-%m")
#                     # Query RailData based on drawn_date month-year match
#                     # rail_logs = RailData.objects.filter(drawn_date__icontains=formatted_date)
#                     rail_logs = RailData.objects.filter(placement_date__icontains=formatted_date)
#                     dictData["month"] = datetime.datetime.strptime(log.month, "%m-%Y").strftime("%b-%Y")
#                     dictData["valid_upto"] = log.valid_upto
#                     dictData["rake_planned_for_the_month"] = log.rake_alloted

#                     # Calculate balance rakes to receive
#                     balance_rakes_to_receive = int(log.rake_alloted) - int(dictData["total_rakes_received_for_month"])
#                     dictData["balance_rakes_to_receive"] = balance_rakes_to_receive

#                     prev_date_obj = datetime.datetime.strptime(log.month, "%m-%Y")

#                     last_month = date_obj.month-1
#                     last_year = date_obj.year

#                     if last_month == 0:
#                         last_month = 12
#                         last_year -= 1

#                     last_month_date_obj = datetime.datetime(last_year, last_month, 1)

#                     # Convert back to the "%b-%Y" format
#                     last_month_str = last_month_date_obj.strftime("%m-%Y")
#                     # last_month = datetime.datetime.strptime(last_month_str, "%b-%Y")
#                     last_month = datetime.datetime.strptime(last_month_str, "%m-%Y")

#                     # Query for the previous month's data
#                     prev_month_log = rakeQuota.objects.filter(
#                         month=f'{last_month.strftime("%b").upper()}-{last_month.strftime("%Y")}',
#                         year=last_month.strftime("%Y")
#                     ).first()
#                     # If there is a previous month log, add its rake_alloted to the current month's rake_alloted
#                     if prev_month_log:
#                         # dictData["previous_month_rake"] += prev_month_log.rake_alloted
#                         # dictData["previous_month_rake"] = int(log.rake_alloted) + int(prev_month_log.rake_alloted)
#                         dictData["previous_month_rake"] = int(prev_month_log.rake_alloted)

#                     if rail_logs:
#                         for rail_log in rail_logs:
#                             # source_type = rail_log.source_type
#                             # console_logger.debug(rail_log.source_type)
#                             if rail_log.source_type != "":
#                                 dictData["source_type"] = rail_log.source_type
#                             # Count the rakes loaded till the drawn_date for the specific rr_no
#                             dictData["rakes_loaded_till_date"] = RailData.objects.filter(
#                                 # rr_no=rail_log.rr_no,
#                                 drawn_date__lte=f"{formatted_date}-30T23:59"
#                             ).count()
#                             # Get today's date in UTC
#                             today_utc = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0)
#                             end_of_day_utc = today_utc + timedelta(hours=23, minutes=59, seconds=59)

#                             # Query to filter data based on the current date
#                             dictData["rakes_loaded_on_date"] = RailData.objects.filter(
#                                 # rr_no=rail_log.rr_no,
#                                 drawn_date__gte=today_utc,
#                                 drawn_date__lte=end_of_day_utc
#                             ).count()
#                             dictData["no_of_rakes_in_transist"] = dictData["rakes_loaded_till_date"] - dictData["total_rakes_received_for_month"]

#                             # dictData["balance_rakes_to_receive"] = log.rake_alloted - dictData["total_rakes_received_for_month"]
#                             # dictData["rakes_loaded_on_date"] = RailData.objects.filter(
#                             #     rr_no=rail_log.rr_no,
#                             #     drawn_date__gte=f"{datetime.datetime.today().strftime('%Y-%m-%d')}T00:00",
#                             #     drawn_date__lte=f"{datetime.datetime.today().strftime('%Y-%m-%d')}T23:59"
#                             # ).count()
#                     listData.append(dictData)
#                 # After building the listData, add the balance_rakes_to_receive to the next available month
#                 for i, current_month_data in enumerate(listData):
#                     for j in range(i + 1, len(listData)):
#                         next_month_data = listData[j]
#                         # console_logger.debug(next_month_data["month"])
#                         # console_logger.debug(current_month_data["month"])
#                         # Compare months to find the next available month
#                         if next_month_data["month"] > current_month_data["month"]:
#                             next_month_data["rakes_previous_month_quota_received"] = current_month_data["balance_rakes_to_receive"]
#                             break  # Stop after updating the first available next month

#             # Append to the result dataset
#                 result["labels"] = list(dictData.keys())
#                 result["datasets"] = listData
#                 result["total"] = len(rakeQuota.objects(data))
#             return result

#         elif type == "download":
#             file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
#             target_directory = f"static_server/gmr_ai/{file}"
#             os.umask(0)
#             os.makedirs(target_directory, exist_ok=True, mode=0o777)

#             headers = ["month", "year", 'valid_upto', "rake_alloted", "source_type", "rakes_planned_for_month","rakes_loaded_till_date", "rakes_loaded_on_date", "previous_month_rake", "rakes_received_on_date", "total_rakes_received_for_month", "balance_rakes_to_receive", "no_of_rakes_in_transist"]

#             if month_date:
#                 month_check = datetime.datetime.strptime(month_date, "%Y-%m").strftime("%b-%Y").upper()
#                 data &= Q(month__iexact = month_check)
            
#             if start_timestamp:
#                 start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M", "Asia/Kolkata", False)
#                 data &= Q(created_at__gte=start_date)

#             if end_timestamp:
#                 end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M", "Asia/Kolkata", False)
#                 data &= Q(created_at__lte=end_date)

#             usecase_data = rakeQuota.objects(data).order_by("-created_at")
#             count = len(usecase_data)
#             path = None

#             if usecase_data:
#                 try: 
#                     path = os.path.join(
#                         "static_server",
#                         "gmr_ai",
#                         file,
#                         "Rake_Quota_{}.xlsx".format(
#                             datetime.datetime.now().strftime("%Y-%m-%d:%H:%M:%S"),
#                         ),
#                     )
#                     filename = os.path.join(os.getcwd(), path)
#                     workbook = xlsxwriter.Workbook(filename)
#                     workbook.use_zip64()
#                     cell_format2 = workbook.add_format()
#                     cell_format2.set_bold()
#                     cell_format2.set_font_size(10)
#                     cell_format2.set_align("center")
#                     cell_format2.set_align("vcenter")

#                     worksheet = workbook.add_worksheet()
#                     worksheet.set_column("A:AZ", 20)
#                     worksheet.set_default_row(50)
#                     cell_format = workbook.add_format()
#                     cell_format.set_font_size(10)
#                     cell_format.set_align("center")
#                     cell_format.set_align("vcenter")

#                     for index, header in enumerate(headers):
#                         worksheet.write(0, index, header, cell_format2)
                    
#                     for row, query in enumerate(usecase_data, start=1):
#                         rake_month = query.month
#                         rake_year = query.year
#                         month_year = f"{rake_year}-{rake_month[:3].upper()}"
#                         date_obj = datetime.datetime.strptime(month_year, "%Y-%b")
#                         formatted_date = date_obj.strftime("%Y-%m")
#                         rail_logs = RailData.objects.filter(drawn_date__icontains=formatted_date)
#                         # worksheet.write(row, 0, row, cell_format)
#                         worksheet.write(row, 0, str(query.month), cell_format)
#                         worksheet.write(row, 1, str(query.year), cell_format)
#                         worksheet.write(row, 2, str(query.valid_upto), cell_format)
#                         worksheet.write(row, 3, str(query.rake_alloted), cell_format)
#                         prev_date_obj = datetime.datetime.strptime(query.month, "%b-%Y")

#                         last_month = date_obj.month-1
#                         last_year = date_obj.year

#                         if last_month == 0:
#                             last_month = 12
#                             last_year -= 1

#                         last_month_date_obj = datetime.datetime(last_year, last_month, 1)

#                         # Convert back to the "%b-%Y" format
#                         last_month_str = last_month_date_obj.strftime("%b-%Y")
#                         last_month = datetime.datetime.strptime(last_month_str, "%b-%Y")

#                         prev_month_log = rakeQuota.objects.filter(
#                             month=f'{last_month.strftime("%b").upper()}-{last_month.strftime("%Y")}',
#                             year=last_month.strftime("%Y")
#                         ).first()
#                         # If there is a previous month log, add its rake_alloted to the current month's rake_alloted
#                         if prev_month_log:
#                             previous_month_rake = int(query.rake_alloted) + int(prev_month_log.rake_alloted)
#                             if previous_month_rake:
#                                 worksheet.write(row, 7, str(previous_month_rake), cell_format)
#                         else:
#                             worksheet.write(row, 8, 0, cell_format)
#                         total_rakes_received_for_month = 0
#                         if rail_logs:
#                             for rail_log in rail_logs:
#                                 source_type = rail_log.source_type
#                                 if source_type != "":
#                                     worksheet.write(row, 4, str(source_type), cell_format)
#                                 else:
#                                     worksheet.write(row, 4, "-", cell_format)
#                                 # Count the rakes loaded till the drawn_date for the specific rr_no
#                                 rakes_loaded_till_date = RailData.objects.filter(
#                                     # rr_no=rail_log.rr_no,
#                                     drawn_date__lte=f"{formatted_date}-30T23:59"
#                                 ).count()
#                                 if rakes_loaded_till_date:
#                                     worksheet.write(row, 5, str(rakes_loaded_till_date), cell_format)
#                                 else:
#                                     worksheet.write(row, 5, 0, cell_format)

#                                 rakes_loaded_on_date = RailData.objects.filter(
#                                     # rr_no=rail_log.rr_no,
#                                     drawn_date__gte=f"{datetime.datetime.today().strftime('%Y-%m-%d')}T00:00",
#                                     drawn_date__lte=f"{datetime.datetime.today().strftime('%Y-%m-%d')}T23:59"
#                                 ).count()
#                                 if rakes_loaded_on_date != 0:
#                                     worksheet.write(row, 6, str(rakes_loaded_on_date), cell_format)
#                                 else:
#                                     worksheet.write(row, 6, 0, cell_format)

#                                 worksheet.write(row, 8, 0, cell_format)
#                                 worksheet.write(row, 9, 0, cell_format)
#                                 worksheet.write(row, 10, 0, cell_format)
#                                 worksheet.write(row, 11, 0, cell_format)
#                                 # worksheet.write(row, 12, 0, cell_format)
#                                 worksheet.write(row, 12, rakes_loaded_till_date - total_rakes_received_for_month, cell_format)
#                         else:
#                             worksheet.write(row, 5, "-", cell_format)
#                             worksheet.write(row, 6, 0, cell_format)
#                             worksheet.write(row, 7, 0, cell_format)
#                             worksheet.write(row, 9, 0, cell_format)
#                             worksheet.write(row, 10, 0, cell_format)
#                             worksheet.write(row, 11, 0, cell_format)
#                             worksheet.write(row, 12, 0, cell_format)
#                             # worksheet.write(row, 13, 0, cell_format)
                                    
#                         count -= 1
                    
#                     workbook.close()
#                     console_logger.debug("Successfully {} report generated".format(service_id))
#                     console_logger.debug("sent data {}".format(path))

#                     return {
#                         "Type": "Rake_quota_download_event",
#                         "Datatype": "Report",
#                         "File_Path": path,
#                     }

#                 except Exception as e:
#                     console_logger.debug(e)
#                     exc_type, exc_obj, exc_tb = sys.exc_info()
#                     fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#                     console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
#                     console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
                
#             else:
#                 console_logger.error("No data found")
#                 return {
#                     "Type": "Rake_quota_download_event",
#                     "Datatype": "Report",
#                     "File_Path": path,
#                 }

#     except Exception as e:
#         console_logger.debug("----- Fetch Rake Quota Error -----",e)
#         response.status_code = 400
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#         console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
#         console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
#         return e


@router.get("/fetch/rake/quota", tags=["Rail Map"])
def end_point_to_fetch_rake_quota_test(response: Response, 
                currentPage: Optional[int] = None,
                perPage: Optional[int] = None,
                # search_text: Optional[str] = None,
                month_date: Optional[str] = None,
                start_timestamp: Optional[str] = None,
                end_timestamp: Optional[str] = None,
                type: Optional[str] = "display"):
    try:
        data = Q()
        result = {        
                "labels": [],
                "datasets": [],
                "total": 0,
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

            # if month_date:
            #     month_check = datetime.datetime.strptime(month_date, "%Y-%m").strftime("%m-%Y")
            #     data &= Q(month__iexact = month_check)

            if month_date:
                # Convert the month_date to match the placement_date format
                # month_start = datetime.datetime.strptime(month_date, "%Y-%m")
                # month_end = (month_start + datetime.timedelta(days=31)).replace(day=1) - datetime.timedelta(seconds=1)

                # Convert month_date to a datetime object representing the 4th of the current month
                month_start = datetime.datetime.strptime(month_date, "%Y-%m").replace(day=4)

                # Calculate the 3rd of the next month
                month_end = (month_start + timedelta(days=31)).replace(day=3)

                # Ensure the end date is at the end of the day
                month_end = month_end.replace(hour=23, minute=59, second=59)

                console_logger.debug(month_start.strftime("%Y-%m-%dT%H:%M"))
                console_logger.debug(month_end.strftime("%Y-%m-%dT%H:%M"))
                # Filter RailData based on placement_date within the month range
                rail_logs = RailData.objects(
                    placement_date__gte=month_start.strftime("%Y-%m-%dT%H:%M"),
                    placement_date__lte=month_end.strftime("%Y-%m-%dT%H:%M"),
                )
                month_check = []
                # If any records found, extract the relevant month-year from placement_date
                if rail_logs:
                    # for log in rail_logs:
                    #     console_logger.debug(log.month)
                    placement_dates = [datetime.datetime.strptime(log.month, "%Y-%m-%d").strftime("%m-%Y") for log in rail_logs if log.month is not None]
                    # console_logger.debug(placement_dates)
                    # Since all placement_dates should be within the same month, take the first one
                    # month_check = placement_dates
                    # console_logger.debug(month_check)
                    # data &= Q(month__in=month_check)
                    month_check.extend(list(set(placement_dates)))
                current_month = month_start.strftime("%m-%Y")
                month_check.append(current_month)
                data &= Q(month__in=month_check)
                # else:
                #     month_check = datetime.datetime.strptime(month_date, "%Y-%m").strftime("%m-%Y")
                #     data &= Q(month__iexact = month_check)
            # console_logger.debug(data)
            if start_timestamp:
                start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M", "Asia/Kolkata", False)
                data &= Q(created_at__gte=start_date)

            if end_timestamp:
                end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M", "Asia/Kolkata", False)
                data &= Q(created_at__lte=end_date)

            offset = (page_no - 1) * page_len
            logs = (
                rakeQuota.objects(data)
                .order_by("year", "-month")
                .skip(offset)
                .limit(page_len)
            )
            listData = []
            if logs:
                for log in logs:
                    # dictData = {"SrNo": 0, "month": "", 'valid_upto': "", "rake_alloted": "", "source_type": "","rakes_loaded_till_date": 0, "rakes_loaded_on_date": 0, "previous_month_rake": 0, "rakes_received_on_date": 0, "total_rakes_received_for_month": 0, "balance_rakes_to_receive": 0, "no_of_rakes_in_transist": 0, "rakes_previous_month_quota_received": 0}
                    # dictData = {"month": "", "source_type": "", "rakes_previous_month_quota_received": 0, "rake_planned_for_the_month": "","rakes_loaded_till_date": 0, "rakes_loaded_on_date": 0, "previous_month_rake": 0, "rakes_received_on_date": 0, "total_rakes_received_for_month": 0, "balance_rakes_to_receive": 0, "no_of_rakes_in_transist": 0, }
                    dictData = {"month": "", "source_type": "", "rakes_previous_month_quota_received": 0, "rake_planned_for_the_month": "","rakes_loaded_till_date": 0, "rakes_loaded_on_date": 0, "rakes_received_on_date": 0, "total_rakes_received_for_month": 0, "balance_rakes_to_receive": 0, "no_of_rakes_in_transist": 0, "expected_rakes_date": 0, "expected_rakes_value": 0}
                    rake_year = log.year
                    rake_month = log.month
                    # Convert rake_month to match RailData drawn_date format
                    month_year = f"{rake_year}-{rake_month[:2].upper()}"
                    # date_obj = datetime.datetime.strptime(month_year, "%Y-%b")
                    date_obj = datetime.datetime.strptime(month_year, "%Y-%m")

                    # Get the previous month
                    prev_date_obj = date_obj - datetime.timedelta(days=1)
                    # prev_month_year = prev_date_obj.strftime("%Y-%b")
                    prev_month_year = prev_date_obj.strftime("%Y-%m")

                    # Format the date object to the desired format
                    formatted_date = date_obj.strftime("%Y-%m")
                    # Query RailData based on drawn_date month-year match
                    # rail_logs = RailData.objects.filter(drawn_date__icontains=formatted_date)
                    rail_logs = RailData.objects.filter(placement_date__icontains=formatted_date)
                    dictData["month"] = datetime.datetime.strptime(log.month, "%m-%Y").strftime("%b-%Y")
                    dictData["valid_upto"] = log.valid_upto
                    dictData["rake_planned_for_the_month"] = log.rake_alloted
                    if log.expected_rakes:
                        dictData["expected_rakes_date"] = list(log.expected_rakes.keys())[0]
                        dictData["expected_rakes_value"] = list(log.expected_rakes.values())[0]
                    # Calculate balance rakes to receive
                    balance_rakes_to_receive = int(log.rake_alloted) - int(dictData["total_rakes_received_for_month"])
                    dictData["balance_rakes_to_receive"] = balance_rakes_to_receive

                    prev_date_obj = datetime.datetime.strptime(log.month, "%m-%Y")

                    last_month = date_obj.month-1
                    last_year = date_obj.year

                    if last_month == 0:
                        last_month = 12
                        last_year -= 1

                    last_month_date_obj = datetime.datetime(last_year, last_month, 1)

                    # Convert back to the "%b-%Y" format
                    last_month_str = last_month_date_obj.strftime("%m-%Y")
                    # last_month = datetime.datetime.strptime(last_month_str, "%b-%Y")
                    last_month = datetime.datetime.strptime(last_month_str, "%m-%Y")

                    # Query for the previous month's data
                    # prev_month_log = rakeQuota.objects.filter(
                    #     month=f'{last_month.strftime("%b").upper()}-{last_month.strftime("%Y")}',
                    #     year=last_month.strftime("%Y")
                    # ).first()
                    # # If there is a previous month log, add its rake_alloted to the current month's rake_alloted
                    # if prev_month_log:
                    #     # dictData["previous_month_rake"] += prev_month_log.rake_alloted
                    #     # dictData["previous_month_rake"] = int(log.rake_alloted) + int(prev_month_log.rake_alloted)
                    #     dictData["previous_month_rake"] = int(prev_month_log.rake_alloted)

                    if rail_logs:
                        for rail_log in rail_logs:
                            # source_type = rail_log.source_type
                            # console_logger.debug(rail_log.source_type)
                            if rail_log.source_type != "":
                                dictData["source_type"] = rail_log.source_type
                            # Count the rakes loaded till the drawn_date for the specific rr_no
                            dictData["rakes_loaded_till_date"] = RailData.objects.filter(
                                # rr_no=rail_log.rr_no,
                                drawn_date__lte=f"{formatted_date}-30T23:59"
                            ).count()
                            # Get today's date in UTC
                            today_utc = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0)
                            end_of_day_utc = today_utc + timedelta(hours=23, minutes=59, seconds=59)

                            # Query to filter data based on the current date
                            dictData["rakes_loaded_on_date"] = RailData.objects.filter(
                                # rr_no=rail_log.rr_no,
                                drawn_date__gte=today_utc,
                                drawn_date__lte=end_of_day_utc
                            ).count()
                            dictData["no_of_rakes_in_transist"] = dictData["rakes_loaded_till_date"] - dictData["total_rakes_received_for_month"]

                            # dictData["balance_rakes_to_receive"] = log.rake_alloted - dictData["total_rakes_received_for_month"]
                            # dictData["rakes_loaded_on_date"] = RailData.objects.filter(
                            #     rr_no=rail_log.rr_no,
                            #     drawn_date__gte=f"{datetime.datetime.today().strftime('%Y-%m-%d')}T00:00",
                            #     drawn_date__lte=f"{datetime.datetime.today().strftime('%Y-%m-%d')}T23:59"
                            # ).count()
                    listData.append(dictData)
                # After building the listData, add the balance_rakes_to_receive to the next available month
                for i, current_month_data in enumerate(listData):
                    for j in range(i + 1, len(listData)):
                        next_month_data = listData[j]
                        # console_logger.debug(next_month_data["month"])
                        # console_logger.debug(current_month_data["month"])
                        # Compare months to find the next available month
                        if next_month_data["month"] > current_month_data["month"]:
                            next_month_data["rakes_previous_month_quota_received"] = current_month_data["balance_rakes_to_receive"]
                            break  # Stop after updating the first available next month

            # Append to the result dataset
                result["labels"] = list(dictData.keys())
                result["datasets"] = listData
                result["total"] = len(rakeQuota.objects(data))
            return result

        elif type == "download":
            file = datetime.datetime.now().strftime("%d-%m-%Y")
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)

            headers = ["month", "source_type", "rakes_previous_month_quota_received", "rake_planned_for_the_month",
                    "rakes_loaded_till_date", "rakes_loaded_on_date", "rakes_received_on_date", "total_rakes_received_for_month",
                    "balance_rakes_to_receive", "no_of_rakes_in_transist", "expected_rakes_date", "expected_rakes_value"]
            # month_date = "2024-08"
            if month_date:
                # Convert the month_date to match the placement_date format
                # month_start = datetime.datetime.strptime(month_date, "%Y-%m")
                # month_end = (month_start + datetime.timedelta(days=31)).replace(day=1) - datetime.timedelta(seconds=1)

                # Convert month_date to a datetime object representing the 4th of the current month
                month_start = datetime.datetime.strptime(month_date, "%Y-%m").replace(day=4)

                # Calculate the 3rd of the next month
                month_end = (month_start + timedelta(days=31)).replace(day=3)

                # Ensure the end date is at the end of the day
                month_end = month_end.replace(hour=23, minute=59, second=59)

                # console_logger.debug(month_start.strftime("%Y-%m-%dT%H:%M"))
                # console_logger.debug(month_end.strftime("%Y-%m-%dT%H:%M"))
                # Filter RailData based on placement_date within the month range
                rail_logs = RailData.objects(
                    placement_date__gte=month_start.strftime("%Y-%m-%dT%H:%M"),
                    placement_date__lte=month_end.strftime("%Y-%m-%dT%H:%M"),
                )
                month_check = []
                # If any records found, extract the relevant month-year from placement_date
                if rail_logs:
                    # for log in rail_logs:
                    #     console_logger.debug(log.month)
                    placement_dates = [datetime.datetime.strptime(log.month, "%Y-%m-%d").strftime("%m-%Y") for log in rail_logs if log.month is not None]
                    # console_logger.debug(placement_dates)
                    # Since all placement_dates should be within the same month, take the first one
                    # month_check = placement_dates
                    # console_logger.debug(month_check)
                    # data &= Q(month__in=month_check)
                    month_check.extend(list(set(placement_dates)))
                current_month = month_start.strftime("%m-%Y")
                month_check.append(current_month)
                data &= Q(month__in=month_check)

            # if start_timestamp:
            #     start_date = convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M", "Asia/Kolkata", False)
            #     data &= Q(created_at__gte=start_date)

            # if end_timestamp:
            #     end_date = convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M", "Asia/Kolkata", False)
            #     data &= Q(created_at__lte=end_date)

            usecase_data = rakeQuota.objects.filter(data).order_by("year", "-month")
            count = len(usecase_data)
            path = None

            if usecase_data:
                try:
                    path = os.path.join(
                        "static_server",
                        "gmr_ai",
                        file,
                        f"Rake_Quota_{datetime.datetime.now().strftime('%Y-%m-%d:%H:%M:%S')}.xlsx",
                    )
                    filename = os.path.join(os.getcwd(), path)
                    workbook = xlsxwriter.Workbook(filename)
                    workbook.use_zip64()
                    cell_format2 = workbook.add_format({'bold': True, 'font_size': 10, 'align': 'center', 'valign': 'vcenter'})
                    cell_format = workbook.add_format({'font_size': 10, 'align': 'center', 'valign': 'vcenter'})

                    worksheet = workbook.add_worksheet()
                    worksheet.set_column("A:AZ", 20)
                    worksheet.set_default_row(50)

                    # Write headers
                    for index, header in enumerate(headers):
                        worksheet.write(0, index, header, cell_format2)

                    listData = []
                    for row, query in enumerate(usecase_data, start=1):
                        dictData = {
                            "month": "", "source_type": "", "rakes_previous_month_quota_received": 0, 
                            "rake_planned_for_the_month": "", "rakes_loaded_till_date": 0, "rakes_loaded_on_date": 0, 
                            "rakes_received_on_date": 0, "total_rakes_received_for_month": 0, 
                            "balance_rakes_to_receive": 0, "no_of_rakes_in_transist": 0, "expected_rakes_date": 0, "expected_rakes_value": 0
                        }
                        
                        month_year = f"{query.year}-{query.month[:2].upper()}"
                        date_obj = datetime.datetime.strptime(month_year, "%Y-%m")
                        formatted_date = date_obj.strftime("%Y-%m")
                        rail_logs = RailData.objects.filter(drawn_date__icontains=formatted_date)

                        # dictData["month"] = datetime.datetime.strptime(f"{query.month}-{query.year}", "%b-%Y").strftime("%b-%Y")
                        dictData["month"] = month_year
                        dictData["rake_planned_for_the_month"] = query.rake_alloted
                        if query.expected_rakes:
                            dictData["expected_rakes_date"] = list(query.expected_rakes.keys())[0]
                            dictData["expected_rakes_value"] = list(query.expected_rakes.values())[0]
                        if rail_logs:
                            for rail_log in rail_logs:
                                if rail_log.source_type != "":
                                    dictData["source_type"] = rail_log.source_type
                                rakes_loaded_till_date = RailData.objects.filter(
                                    drawn_date__lte=f"{formatted_date}-30T23:59"
                                ).count()
                                dictData["rakes_loaded_till_date"] = rakes_loaded_till_date
                                rakes_loaded_on_date = RailData.objects.filter(
                                    drawn_date__gte=f"{datetime.datetime.today().strftime('%Y-%m-%d')}T00:00",
                                    drawn_date__lte=f"{datetime.datetime.today().strftime('%Y-%m-%d')}T23:59"
                                ).count()
                                dictData["rakes_loaded_on_date"] = rakes_loaded_on_date
                                dictData["no_of_rakes_in_transist"] = rakes_loaded_till_date - dictData["total_rakes_received_for_month"]

                        balance_rakes_to_receive = int(query.rake_alloted) - dictData["total_rakes_received_for_month"]
                        dictData["balance_rakes_to_receive"] = balance_rakes_to_receive

                        prev_month_log = rakeQuota.objects.filter(
                            month=(date_obj.replace(day=1) - datetime.timedelta(days=1)).strftime("%b-%Y").upper()
                        ).first()

                        if prev_month_log:
                            dictData["rakes_previous_month_quota_received"] = int(prev_month_log.rake_alloted)

                        # console_logger.debug(dictData)

                        # Write data to worksheet
                        worksheet.write(row, 0, dictData["month"], cell_format)
                        worksheet.write(row, 1, dictData["source_type"], cell_format)
                        worksheet.write(row, 2, dictData["rakes_previous_month_quota_received"], cell_format)
                        worksheet.write(row, 3, dictData["rake_planned_for_the_month"], cell_format)
                        worksheet.write(row, 4, dictData["rakes_loaded_till_date"], cell_format)
                        worksheet.write(row, 5, dictData["rakes_loaded_on_date"], cell_format)
                        worksheet.write(row, 6, dictData["rakes_received_on_date"], cell_format)
                        worksheet.write(row, 7, dictData["total_rakes_received_for_month"], cell_format)
                        worksheet.write(row, 8, dictData["balance_rakes_to_receive"], cell_format)
                        worksheet.write(row, 9, dictData["no_of_rakes_in_transist"], cell_format)
                        worksheet.write(row, 10, dictData["expected_rakes_date"], cell_format)
                        worksheet.write(row, 11, dictData["expected_rakes_value"], cell_format)

                        listData.append(dictData)

                    workbook.close()
                    console_logger.debug(f"Successfully {service_id} report generated")
                    console_logger.debug(f"Sent data {path}")

                    return {
                        "Type": "Rake_quota_download_event",
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
                    "Type": "Rake_quota_download_event",
                    "Datatype": "Report",
                    "File_Path": path,
                }

    except Exception as e:
        console_logger.debug("----- Fetch Rake Quota Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.post("/insert/rake/quota", tags=["Rail Map"])
def endpoint_to_insert_rake_quota(response:Response, data:rakeQuotaManual):
    try:
        payload = data.dict()
        insertRakeQuota = rakeQuota(
            month=payload.get("month"),
            year=payload.get("year"),
            valid_upto=payload.get("valid_upto"),
            coal_field=payload.get("coal_field"),
            rake_alloted=payload.get("rake_alloted"),
            rake_received=payload.get("rake_received"),
            due=payload.get("due"),
            grade=payload.get("grade"),
            ID=rakeQuota.objects.count() + 1)
        insertRakeQuota.save()
        return {"details": "success"}
    except Exception as e:
        console_logger.debug("----- Fetch Rake Quota Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

def extract_quota_rail(file):
    try:
        pdfFileObj = open(file, 'rb')
        pdfReader = PyPDF3.PdfFileReader(pdfFileObj)

        pageObj = pdfReader.getPage(0)
        mytext = pageObj.extractText()

        result={}
        date_match = re.search(r'([A-Z]{3}-\d{4})', mytext,re.DOTALL)
        result["date"]= date_match.group(1) if date_match else None

        no_rake_match = re.search(r'(\d+Rakes@\d+\s*BOXN)', mytext)
        result['no_rake'] = no_rake_match.group(1).replace('\n', '') if no_rake_match else None

        rakes_start_index = no_rake_match.start()
        result['coal_field'] = mytext[rakes_start_index-5:rakes_start_index].strip()


        grade_match = re.search(r"(\d{4}-\d{4}-\s*\w+)",mytext)
        result['coal_grade'] = grade_match.group(1).replace('\n', '') if grade_match else None

        valid_match = re.search(r"This Programme is Valid Upto:\s*(\d{2}-\d{2}-\d{4})",mytext)
        result['valid'] = valid_match.group(1) if valid_match else None

        return result
    except Exception as e:
        console_logger.debug("----- Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.post("/rail/rakequotaupload", tags=["Rail Map"])
async def endpoint_to_upload_rake_data(response: Response, pdf_upload: Optional[UploadFile] = File(None)):
    try:
        if pdf_upload is None:
            return {"error": "No file uploaded"}
        contents = await pdf_upload.read()

        if not contents:
            return {"error": "Uploaded file is empty"}

        if not pdf_upload.filename.endswith(('.pdf','.PDF')):
            return {"error": "Uploaded file is not a PDF"}
        
        file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
        target_directory = f"static_server/gmr_ai/{file}"
        os.umask(0)
        os.makedirs(target_directory, exist_ok=True, mode=0o777)

        file_extension = pdf_upload.filename.split(".")[-1]
        file_name = f'rake_quota_upload_{datetime.datetime.now().strftime("%Y-%m-%d:%H:%M")}.{file_extension}'
        full_path = os.path.join(os.getcwd(), target_directory, file_name)
        with open(full_path, "wb") as file_object:
            file_object.write(contents)
        fetchPdfData = extract_quota_rail(full_path)
        if fetchPdfData:
            try:
                checkRakeRecords = rakeQuota.objects.get(month=fetchPdfData.get("date"))
                # checkRakeRecords.month = fetchPdfData.get("date")
                checkRakeRecords.month = datetime.datetime.strptime(fetchPdfData.get("date"), "%b-%Y").strftime("%m-%Y")
                checkRakeRecords.year = fetchPdfData.get("date").split("-")[1]
                checkRakeRecords.valid_upto = fetchPdfData.get("valid")
                checkRakeRecords.coal_field = fetchPdfData.get("coal_field")
                checkRakeRecords.rake_alloted = fetchPdfData.get("no_rake").split("Rakes")[0]
                checkRakeRecords.grade = fetchPdfData.get("coal_grade")
                checkRakeRecords.save()
            except DoesNotExist as e:
                insertRakeRecords = rakeQuota(
                    # month=fetchPdfData.get("date"),
                    month=datetime.datetime.strptime(fetchPdfData.get("date"), "%b-%Y").strftime("%m-%Y"),
                    year=fetchPdfData.get("date").split("-")[1],
                    valid_upto=fetchPdfData.get("valid"),
                    coal_field=fetchPdfData.get("coal_field"),
                    rake_alloted=fetchPdfData.get("no_rake").split("Rakes")[0],
                    grade=fetchPdfData.get("coal_grade"),
                    ID=rakeQuota.objects.count() + 1)
                insertRakeRecords.save()
        return {"details": "success"}
    except Exception as e:
        console_logger.debug("----- Rake Quota Upload Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

# Helper function to handle regex extraction with error handling
def extract_with_regex_rcr(pattern, text, group_index=1):
    try:
        match = re.search(pattern, text, re.MULTILINE)
        if match:
            return match.group(group_index).strip()
        return None  # or return a default value, e.g., "Not Found"
    except Exception as e:
        console_logger.debug("----- Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


def extract_with_regex_scheme_rcr(text):
    try:
        pattern = r"([A-Za-z\s\(\)\-]+)\s*Scheme Name\s*:"
        match = re.search(pattern, text, re.DOTALL)
        if match:
            text = " ".join(match.group(1).split()).strip()
            return text
        return None
    except Exception as e:
        console_logger.debug("----- Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


def extract_fields_rcr_data(pdf_path):
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text()

        # print(text)
        fields = {}
        # Adjusted regex patterns for more accurate extraction
        sales_order_no = extract_with_regex_rcr(r"(\d+)\s*Sales Order Number", text)

        if len(sales_order_no) > 4:
            fields["rr_no"] = sales_order_no
            fields["rr_date"] = extract_with_regex_rcr(
                r"([\w\s,]+)(?=\s*Sales Order Date)", text
            )
            fields["start_date"] = extract_with_regex_rcr(
                r"([\w\s,]+)(?=\s*Sales Order Valid From)", text
            )
            fields["end_date"] = extract_with_regex_rcr(
                r"([\w\s,]+)(?=\s*Sales Order Valid To)", text
            )
            month = extract_with_regex_rcr(r"([\w\s,]+)(?=\s*Month)", text)
            if len(month) > 6:
                fields["month"] = month[-6:]

            fields["consumer_type"] = extract_with_regex_scheme_rcr(text)

            fields["grade"] = extract_with_regex_rcr(r"(\w+)\s+Grade Desc\s*:", text)
            fields["size"] = extract_with_regex_rcr(r"([\-\d\s\w]+)\s+Size\s*:", text)
            # Improved pattern for Plant extraction
            fields["mine"] = extract_with_regex_rcr(
                r"Line Item Mine Material Material Description HSN Code Unit of Measure Quantity\s*\n10\s+([^\d]+)",
                text,
            )

            # Extract Line Item and Quantity more generally
            fields["line_item"] = extract_with_regex_rcr(
                r"Line Item\s+Mine\s+Material.*\n(\d+)", text
            )
            fields["rr_qty"] = extract_with_regex_rcr(r"\b(\d{1,3}(?:,\d{3})*)\b\s*$", text)
            # New field: Total Net Amount
            fields["po_amount"] = extract_with_regex_rcr(r"TOTAL\s*:\s*([\d,]+\.\d{2})", text)
        else:
            fields["rr_no"] = extract_with_regex_rcr(r"Sales Order Number\s+:\s+(\d+)", text)
            fields["rr_date"] = extract_with_regex_rcr(
                r"Sales Order Date\s+:\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", text
            )
            fields["start_date"] = extract_with_regex_rcr(
                r"Sales Order Valid From\s+:\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", text
            )
            fields["end_date"] = extract_with_regex_rcr(
                r"Sales Order Valid To\s+:\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", text
            )
            fields["month"] = extract_with_regex_rcr(r"Month\s*:\s*(\d+)", text)
            fields["consumer_type"] = extract_with_regex_rcr(r"Scheme Name\s*:\s*(.*)", text)
            fields["grade"] = extract_with_regex_rcr(r"(?i)Grade Desc\s*:\s*(.*)", text)
            fields["size"] = extract_with_regex_rcr(r"Size\s*:\s*(\S+)\s*", text) + " MM"
            # Improved pattern for Plant extraction
            fields["mine"] = extract_with_regex_rcr(
                r"Line Item Mine Material Material Description HSN Code Unit of Measure Quantity\s*\n10\s+([^\d]+)",
                text,
            )
            # Extract Line Item and Quantity more generally
            fields["line_item"] = extract_with_regex_rcr(
                r"Line Item\s+Mine\s+Material.*\n(\d+)", text
            )
            fields["rr_qty"] = extract_with_regex_rcr(r"\b(\d{1,3}(?:,\d{3})*)\b\s*$", text)
            # New field: Total Net Amount
            fields["po_amount"] = extract_with_regex_rcr(r"TOTAL\s*:\s*([\d,]+\.\d{2})", text)
        return fields
    except Exception as e:
        console_logger.debug("----- Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.post("/rail/saprecordsrcr", tags=["Rail Map"])
async def endpoint_to_upload_sap_data(response: Response, pdf_upload: Optional[UploadFile] = File(None)):
    try:
        if pdf_upload is None:
            return {"error": "No file uploaded"}
        contents = await pdf_upload.read()

        # Check if the file is empty
        if not contents:
            return {"error": "Uploaded file is empty"}

        if not pdf_upload.filename.endswith(('.pdf','.PDF')):
            return {"error": "Uploaded file is not a PDF"}
        
        file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
        target_directory = f"static_server/gmr_ai/{file}"
        os.umask(0)
        os.makedirs(target_directory, exist_ok=True, mode=0o777)

        file_extension = pdf_upload.filename.split(".")[-1]
        file_name = f'sap_rcr_upload_{datetime.datetime.now().strftime("%Y-%m-%d:%H:%M")}.{file_extension}'
        full_path = os.path.join(os.getcwd(), target_directory, file_name)
        with open(full_path, "wb") as file_object:
            file_object.write(contents)

        fetchRcrData = extract_fields_rcr_data(full_path)

        if fetchRcrData:
            try:
                checkRcrRecords = sapRecordsRCR.objects.get(rr_no=fetchRcrData.get("rr_no"))
                # checkRcrRecords.month = fetchRcrData.get("date")
                checkRcrRecords.rr_date = datetime.datetime.strptime(fetchRcrData.get("rr_date"), "%b %d, %Y").strftime("%Y-%m-%d")
                checkRcrRecords.start_date = datetime.datetime.strptime(fetchRcrData.get("start_date"), '%b %d, %Y').strftime('%Y-%m-%d')
                checkRcrRecords.end_date = datetime.datetime.strptime(fetchRcrData.get("end_date"), '%b %d, %Y').strftime('%Y-%m-%d')
                checkRcrRecords.month = fetchRcrData.get("month")
                checkRcrRecords.consumer_type = fetchRcrData.get("consumer_type")
                checkRcrRecords.grade = f'{fetchRcrData.get("grade")} {fetchRcrData.get("size")}'
                checkRcrRecords.mine = fetchRcrData.get("mine")
                checkRcrRecords.line_item = fetchRcrData.get("line_item")
                checkRcrRecords.rr_qty = fetchRcrData.get("rr_qty")
                checkRcrRecords.po_amount = fetchRcrData.get("po_amount")
                checkRcrRecords.save()
            except DoesNotExist as e:
                insertRcrRecords = sapRecordsRCR(
                    rr_no=fetchRcrData.get("rr_no"),
                    rr_date=datetime.datetime.strptime(fetchRcrData.get("rr_date"), '%b %d, %Y').strftime('%Y-%m-%d'),
                    start_date=datetime.datetime.strptime(fetchRcrData.get("start_date"), '%b %d, %Y').strftime('%Y-%m-%d'),
                    end_date=datetime.datetime.strptime(fetchRcrData.get("end_date"), '%b %d, %Y').strftime('%Y-%m-%d'),
                    month=fetchRcrData.get("month"),
                    consumer_type=fetchRcrData.get("consumer_type"),
                    grade=f'{fetchRcrData.get("grade")} {fetchRcrData.get("size")}',
                    mine=fetchRcrData.get("mine"),
                    line_item=fetchRcrData.get("line_item"),
                    rr_qty=fetchRcrData.get("rr_qty"),
                    po_amount=fetchRcrData.get("po_amount"),
                    # id=sapRecordsRCR.objects.count() + 1
                    )
                insertRcrRecords.save()
        return {"details": "success"}
        
    except Exception as e:
        console_logger.debug("----- RCR Sap Upload Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

def extract_with_regex_rail(pattern, text, group_index=1):
    try:
        match = re.search(pattern, text,re.MULTILINE)
        if match:
            return match.group(group_index).strip()
        return None  # or return a default value, e.g., "Not Found"
    except Exception as e:
        console_logger.debug("----- Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

def extract_fields_rail(pdf_path):
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
        fields = {}    
        # Adjusted regex patterns for more accurate extraction
        fields["sale_order_date"] = extract_with_regex(r'\s*(\w+\s+\d{1,2},\s+\d{4})\s*Sale Order Date', text)
        fields["rr_no"] = extract_with_regex(r'\s*:\s*(\d+)\s*RR_NO\s*:', text)
        fields["rr_date"] = extract_with_regex(r'\s*:\s*(\w+\s+\d{1,2},\s+\d{4})\s*RR_DATE', text)
        fields["siding"] = extract_with_regex(r'\s*:\s*\d+(.*?)Siding', text)
        fields["mine"] = extract_with_regex( r"TaxTermina\s*l Tax[\s\S]*?([A-Z\s]+\(\s*\d{4}\s*\))\d{10}", text).replace('\n', ' ')
        fields["grade_size"] = extract_with_regex(r'([A-Z\d/-]+\s*MM)', text)
        fields["billed_quantity"] = extract_with_regex(r'(\d{4}\.\d{3})', text)
        fields["total_amount"] = extract_with_regex(r'(\d+\.\d+)\s+Total Amount:', text)
        return fields
    except Exception as e:
        console_logger.debug("----- Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.post("/rail/saprecords", tags=["Rail Map"])
async def endpoint_to_upload_rail_data(response: Response, pdf_upload: Optional[UploadFile] = File(None)):
    try:
        if pdf_upload is None:
            return {"error": "No file uploaded"}
        contents = await pdf_upload.read()

        if not contents:
            return {"error": "Uploaded file is empty"}

        if not pdf_upload.filename.endswith(('.pdf','.PDF')):
            return {"error": "Uploaded file is not a PDF"}
        
        file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
        target_directory = f"static_server/gmr_ai/{file}"
        os.umask(0)
        os.makedirs(target_directory, exist_ok=True, mode=0o777)

        file_extension = pdf_upload.filename.split(".")[-1]
        file_name = f'sap_rail_upload_{datetime.datetime.now().strftime("%Y-%m-%d:%H:%M")}.{file_extension}'
        full_path = os.path.join(os.getcwd(), target_directory, file_name)
        with open(full_path, "wb") as file_object:
            file_object.write(contents)

        fetchRailData = extract_fields_rail(full_path)

        if fetchRailData:
            try:
                checkRailSapRecords = sapRecordsRail.objects.get(rr_no=fetchRailData.get("rr_no"))
                checkRailSapRecords.month = datetime.datetime.strptime(fetchRailData.get("sale_order_date"), "%b %d, %Y").strftime("%Y-%m-%d")
                checkRailSapRecords.rr_date = datetime.datetime.strptime(fetchRailData.get("rr_date"), '%b %d, %Y').strftime('%Y-%m-%d')
                checkRailSapRecords.siding = fetchRailData.get("siding")
                checkRailSapRecords.mine = fetchRailData.get("mine")
                checkRailSapRecords.grade = fetchRailData.get("grade_size")
                checkRailSapRecords.rr_qty = fetchRailData.get("billed_quantity")
                checkRailSapRecords.po_amount = fetchRailData.get("total_amount")
                checkRailSapRecords.save()
            except DoesNotExist as e:
                insertRailSapRecords = sapRecordsRail(
                    rr_no=fetchRailData.get("rr_no"),
                    month=fetchRailData.get("sale_order_date"),
                    rr_date=datetime.datetime.strptime(fetchRailData.get("rr_date"), '%b %d, %Y').strftime('%Y-%m-%d'),
                    siding=fetchRailData.get("siding"),
                    mine=fetchRailData.get("mine"),
                    grade=fetchRailData.get("grade"),
                    rr_qty=fetchRailData.get("billed_quantity"),
                    po_amount=fetchRailData.get("total_amount"),
                    )
                insertRailSapRecords.save()
        
            try:
                checkRailData = RailData.objects(rr_no=fetchRailData.get("rr_no"))
                for singleCheckRailData in checkRailData:
                    singleCheckRailData.month = datetime.datetime.strptime(fetchRailData.get("sale_order_date"), '%b %d, %Y').strftime('%Y-%m-%d')
                    singleCheckRailData.rr_date = datetime.datetime.strptime(fetchRailData.get("rr_date"), '%b %d, %Y').strftime('%Y-%m-%d')
                    singleCheckRailData.siding = fetchRailData.get("siding")
                    singleCheckRailData.mine = fetchRailData.get("mine")
                    singleCheckRailData.grade = fetchRailData.get("grade_size")
                    singleCheckRailData.rr_qty = fetchRailData.get("billed_quantity")
                    singleCheckRailData.po_amount = fetchRailData.get("total_amount")
                    singleCheckRailData.save()
            except DoesNotExist as e:
                pass

        return {"details": "success"}      
    except Exception as e:
        console_logger.debug("----- Rake Quota Upload Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/fetch/saprcr", tags=["Rail Map"])
def endpoint_to_fetch_saprcr_data(response: Response, currentPage: Optional[int] = None, perPage: Optional[int] = None, search_text: Optional[str] = None, start_timestamp: Optional[str] = None, end_timestamp: Optional[str] = None, type: Optional[str] = "display"):
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
                    data &= Q(rr_no__icontains=search_text)
                else:
                    data &= (Q(mine__icontains=search_text))

            # if month_date:
            #     start_date = f'{month_date}-01'
            #     console_logger.debug(start_date)
            #     startd_date=datetime.datetime.strptime(f"{start_date}T00:00","%Y-%m-%dT%H:%M")
            #     end_date = (datetime.datetime.strptime(start_date, "%Y-%m-%d") + relativedelta(day=31)).strftime("%Y-%m-%d")
            #     # endd_date= f"{end_date}T23:59"
            #     console_logger.debug(startd_date.strftime("%Y-%m-%dT%H:%M"))
            #     console_logger.debug(f"{end_date}T23:59")
            #     data &= Q(placement_date__gte = startd_date.strftime("%Y-%m-%dT%H:%M"))
            #     data &= Q(placement_date__lte = f"{end_date}T23:59")

            offset = (page_no - 1) * page_len
            # listData = []
            logs = (
                sapRecordsRCR.objects(data)
                .order_by("-created_at")
                .skip(offset)
                .limit(page_len)
            )   
            if any(logs):
                for log in logs:
                    result["labels"] = list(log.payload().keys())
                    result["datasets"].append(log.payload())
                result["total"]= len(sapRecordsRCR.objects(data))
                return result
            else:
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
                    data &= Q(rr_no__icontains=search_text)
                else:
                    data &= (Q(mine__icontains=search_text))

            usecase_data = sapRecordsRCR.objects(data).order_by("-created_at")
            count = len(usecase_data)
            path = None
            if usecase_data:
                try:
                    path = os.path.join(
                        "static_server",
                        "gmr_ai",
                        file,
                        "Rail_rcr_Report_{}.xlsx".format(
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
                        "Sr no",
                        "RR No",
                        "RR Date",
                        "Start Date",
                        "End Date",
                        "Month",
                        "Consumer Type",
                        "Grade",
                        "Mine",
                        "Line Item",
                        "RR Qty",
                        "PO Amount",
                        "Created At",
                    ]
                   
                    for index, header in enumerate(headers):
                        worksheet.write(0, index, header, cell_format2)

                    for row, query in enumerate(usecase_data, start=1):
                        result = query.payload()
                        worksheet.write(row, 0, count, cell_format)     
                        worksheet.write(row, 1, str(result["rr_no"]))                      
                        worksheet.write(row, 2, str(result["rr_date"]))                      
                        worksheet.write(row, 3, str(result["start_date"]))                      
                        worksheet.write(row, 4, str(result["end_date"]))                      
                        worksheet.write(row, 5, str(result["month"]))                      
                        worksheet.write(row, 6, str(result["consumer_type"]))                      
                        worksheet.write(row, 7, str(result["grade"]))                      
                        worksheet.write(row, 8, str(result["mine"]))                      
                        worksheet.write(row, 9, str(result["line_item"]))                      
                        worksheet.write(row, 10, str(result["rr_qty"]))                      
                        worksheet.write(row, 11, str(result["po_amount"]))                      
                        worksheet.write(row, 12, str(result["created_at"]))                   
                        
                        count-=1
                        
                    workbook.close()
                    console_logger.debug("Successfully {} report generated".format(service_id))
                    console_logger.debug("sent data {}".format(path))

                    return {
                            "Type": "gmr_rcr_rail_download_event",
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
                        "Type": "gmr_rcr_rail_download_event",
                        "Datatype": "Report",
                        "File_Path": path,
                        }
    except Exception as e:
        console_logger.debug("----- Fetch RCR Report Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

    
# @router.get("/update/gmrdata", tags=["Coal Testing"])
# async def endpoint_to_update_gmrdata_using_saprecords(response: Response, do_no: str):
#     try:
#         try:
#             sapData = SapRecords.objects.get(do_no=do_no)
#             if sapData:
#                 Gmrdata.objects(
#                     arv_cum_do_number=do_no,
#                 ).update(
#                     do_date=sapData.do_date, 
#                     start_date=sapData.start_date, 
#                     end_date=sapData.end_date, 
#                     slno=sapData.slno,
#                     type_consumer= sapData.consumer_type,
#                     grade= sapData.grade,
#                     mine= sapData.mine_name,
#                     po_qty= sapData.do_qty,
#                     po_amount= sapData.po_amount)
#         except DoesNotExist as e:
#             raise HTTPException(status_code=404, detail="No data found")
#         return {"details": "success"}
#     except Exception as e:
#         console_logger.debug("----- Road Sap Upload Error -----",e)
#         response.status_code = 400
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#         console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
#         console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
#         return e


# @router.get("/check/saprecords", tags=["Coal Testing"])
# async def endpoint_to_dono_using_saprecords(response: Response, do_no: str):
#     try:
#         try:
#             sapData = SapRecords.objects.get(do_no=do_no)
#             if sapData:
#                 return sapData.payload()
#         except DoesNotExist as e:
#             raise HTTPException(status_code=404, detail="No data found")
#     except Exception as e:
#         console_logger.debug("----- Road Sap Upload Error -----",e)
#         response.status_code = 400
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
#         console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
#         console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
#         return e


@router.get("/fetch/secllinkage", tags=["Rail Map"])
def endpoint_to_fetch_secl_linkage_matrialization(response: Response, year_data: str):
    try:
        # railData_pipeline = [
        #     {
        #         "$match": {
        #             "placement_date": {"$regex": f"^{month}"}
        #         }
        #     },
        #     {
        #         "$group": {
        #             "_id": month,
        #             # "total_rr_qty": {"$sum": {"$toDouble": "$rr_qty"}},
        #             "total_rly_tare_wt": {"$sum": {"$toDouble": "$total_rly_tare_wt"}}
        #         }
        #     }
        # ]
        # # Convert the month to the format used in sapRecordsRail
        # # month_name = datetime.datetime.strptime(month, "%Y-%m").strftime("%b")  # Converts "2024-08" to "Aug"
        # # month_regex = f"^{month_name} "

        # month_name = datetime.datetime.strptime(month, "%Y-%m").strftime("%b")  # Converts "2024-08" to "Aug"
        # year = datetime.datetime.strptime(month, "%Y-%m").strftime("%Y")  # Extracts the year as "2024"

        # # Construct the regular expression for the month and year
        # month_regex = f"^{month_name} \\d+, {year}$"  # Matches format like "Aug 1, 2024"

        # # Aggregation for sapRecordsRail
        # sapRecordsRail_pipeline = [
        #     {
        #         "$match": {
        #             "month": {"$regex": f"^{month_regex}"}
        #         }
        #     },
        #     {
        #         "$group": {
        #             "_id": None,
        #             "total_rr_qty": {"$sum": {"$toDouble": "$rr_qty"}}
        #         }
        #     }
        # ]
        # # Run aggregations
        # # railData_result = list(db.railData.aggregate(railData_pipeline))
        # # sapRecordsRail_result = list(db.sapRecordsRail.aggregate(sapRecordsRail_pipeline))

        # railData_result_cursor = RailData.objects().aggregate(railData_pipeline)
        # sapRecordsRail_result_cursor = sapRecordsRail.objects().aggregate(sapRecordsRail_pipeline)
        
        # railData_result = list(railData_result_cursor)
        # sapRecordsRail_result = list(sapRecordsRail_result_cursor)

        # console_logger.debug(railData_result)
        # console_logger.debug(sapRecordsRail_result)

        # # for singledata in railData_result:
        # #     console_logger.debug(singledata)

        # # Combine the results
        # total_rr_qty = sapRecordsRail_result[0]["total_rr_qty"] if sapRecordsRail_result else 0
        # total_rly_tare_wt = railData_result[0]["total_rly_tare_wt"] if railData_result else 0

        # # # Calculate percentage
        # percentage = (total_rly_tare_wt / total_rr_qty) * 100 if total_rr_qty != 0 else 0
        # chart_data = {
        #     "labels": [month],
        #     "datasets": [
        #         {
        #             "label": "Total RR Qty",
        #             "data": [total_rr_qty],
        #             "backgroundColor": "rgba(54, 162, 235, 0.2)",
        #             "borderColor": "rgba(54, 162, 235, 1)",
        #             "borderWidth": 1,
        #             "yAxisID": "y"
        #         },
        #         # {
        #         #     "label": "Total Rly Tare Wt",
        #         #     "data": [total_rly_tare_wt],
        #         #     "backgroundColor": "rgba(75, 192, 192, 0.2)",
        #         #     "borderColor": "rgba(75, 192, 192, 1)",
        #         #     "borderWidth": 1,
        #         #     "yAxisID": "y"
        #         # },
        #         {
        #             "label": "Percentage",
        #             "data": [percentage],
        #             "backgroundColor": "rgba(255, 206, 86, 0.2)",
        #             "borderColor": "rgba(255, 206, 86, 1)",
        #             "borderWidth": 1,
        #             "type": "line",
        #             "yAxisID": "y1"
        #         }
        #     ]
        # }
        # return chart_data

        # railData_pipeline = [
        #     {
        #         '$project': {
        #             'month': {
        #                 '$dateToString': {
        #                     'format': '%Y-%m', 
        #                     'date': {
        #                         '$dateFromString': {
        #                             'dateString': '$placement_date', 
        #                             'format': '%Y-%m-%dT%H:%M'
        #                         }
        #                     }
        #                 }
        #             }, 
        #             'total_rly_tare_wt': {
        #                 '$toDouble': '$total_rly_tare_wt'
        #             }
        #         }
        #     }, {
        #         '$group': {
        #             '_id': '$month', 
        #             'total_rly_tare_wt': {
        #                 '$sum': '$total_rly_tare_wt'
        #             }
        #         }
        #     }, {
        #         '$sort': {
        #             '_id': 1
        #         }
        #     }
        # ]

        railData_pipeline = [
            {
                '$match': {
                    'placement_date': {
                        '$ne': None
                    }, 
                    'placement_date': {
                        '$ne': ''
                    }
                }
            }, {
                '$project': {
                    'year': {
                        '$substr': [
                            '$placement_date', 0, 4
                        ]
                    }, 
                    'month': {
                        '$dateToString': {
                            'format': '%Y-%m', 
                            'date': {
                                '$dateFromString': {
                                    'dateString': '$placement_date', 
                                    'format': '%Y-%m-%dT%H:%M'
                                }
                            }
                        }
                    }, 
                    'total_rly_tare_wt': {
                        '$toDouble': '$total_rly_tare_wt'
                    }
                }
            }, {
                '$match': {
                    'year': year_data
                }
            }, {
                '$group': {
                    '_id': '$month', 
                    'total_rly_tare_wt': {
                        '$sum': '$total_rly_tare_wt'
                    }
                }
            }, {
                '$sort': {
                    '_id': 1
                }
            }
        ]

        # sapRecordsRail_pipeline = [
        #     {
        #         "$project": {
        #             "month": {
        #                 "$dateToString": {"format": "%Y-%m", "date": {"$dateFromString": {"dateString": "$month"}}}
        #             },
        #             "total_rr_qty": {"$toDouble": "$rr_qty"}
        #         }
        #     },
        #     {
        #         "$group": {
        #             "_id": "$month",
        #             "total_rr_qty": {"$sum": "$total_rr_qty"}
        #         }
        #     },
        #     {
        #         "$sort": {"_id": 1}
        #     }
        # ]

        sapRecordsRail_pipeline = [
            {
                "$project": {
                    "year": {
                        "$substr": ["$month", 7, 4]
                    },
                    "month": {
                        "$dateToString": {
                            "format": "%Y-%m",
                            "date": {
                                "$dateFromString": {
                                    "dateString": "$month"
                                }
                            }
                        }
                    },
                    "total_rr_qty": {
                        "$toDouble": "$rr_qty"
                    }
                }
            },
            {
                "$match": {
                    "year": year_data
                }
            },
            {
                "$group": {
                    "_id": "$month",
                    "total_rr_qty": {
                        "$sum": "$total_rr_qty"
                    }
                }
            },
            {
                "$sort": {
                    "_id": 1
                }
            }
        ]

        # Run aggregations
        railData_result_cursor = RailData.objects().aggregate(railData_pipeline)
        sapRecordsRail_result_cursor = sapRecordsRail.objects().aggregate(sapRecordsRail_pipeline)

        if railData_result_cursor and sapRecordsRail_result_cursor:
            # Convert cursors to lists
            railData_result = list(railData_result_cursor)
            sapRecordsRail_result = list(sapRecordsRail_result_cursor)

            # Convert results to dictionaries by month
            railData_dict = {item["_id"]: item["total_rly_tare_wt"] for item in railData_result}
            sapRecordsRail_dict = {item["_id"]: item["total_rr_qty"] for item in sapRecordsRail_result}

            # Prepare Chart.js data
            months = sorted(set(railData_dict.keys()).union(sapRecordsRail_dict.keys()))
            total_rr_qty_data = [sapRecordsRail_dict.get(month, 0) for month in months]
            total_rly_tare_wt_data = [railData_dict.get(month, 0) for month in months]
            percentages = [
                (total_rly_tare_wt / rr_qty * 100 if rr_qty != 0 else 0)
                for rr_qty, total_rly_tare_wt in zip(total_rr_qty_data, total_rly_tare_wt_data)
            ]

            chart_data = {
                # "labels": total_rr_qty_data,
                "labels": months,
                "datasets": [
                    # {
                    #     "label": "Total RR Qty",
                    #     "data": total_rr_qty_data,
                    #     "backgroundColor": "rgba(54, 162, 235, 0.2)",
                    #     "borderColor": "rgba(54, 162, 235, 1)",
                    #     "borderWidth": 1,
                    #     "yAxisID": "y"
                    # },
                    # {
                    #     "label": "Total Rly Tare Wt",
                    #     "data": total_rly_tare_wt_data,
                    #     "backgroundColor": "rgba(75, 192, 192, 0.2)",
                    #     "borderColor": "rgba(75, 192, 192, 1)",
                    #     "borderWidth": 1,
                    #     "yAxisID": "y"
                    # },
                    {
                        "label": "Percentage",
                        "data": percentages,
                        "borderWidth": 1,
                        # "yAxisID": "y1"
                    }
                ]
            }

            # Return chart data
            return chart_data

    except Exception as e:
        console_logger.debug("----- Secl Linkage Matrialization Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/fetch/wcllinkage", tags=["Road Map"])
def endpoint_to_fetch_wcl_linkage_matrialization(response: Response, year_data: str):
    try:
        # sapRecordsPipeline = [
        #     {
        #         '$project': {
        #             'month': {
        #                 '$dateToString': {
        #                     'format': '%Y%m', 
        #                     'date': {
        #                         '$dateFromString': {
        #                             'dateString': {
        #                                 '$concat': [
        #                                     {
        #                                         '$substr': [
        #                                             '$slno', 0, 4
        #                                         ]
        #                                     }, '-', {
        #                                         '$substr': [
        #                                             '$slno', 4, 2
        #                                         ]
        #                                     }, '-01'
        #                                 ]
        #                             }, 
        #                             'format': '%Y-%m-%d'
        #                         }
        #                     }
        #                 }
        #             }, 
        #             'do_qty': {
        #                 '$toDouble': '$do_qty'
        #             }, 
        #             'do_no': 1
        #         }
        #     }, {
        #         '$group': {
        #             '_id': '$month', 
        #             'total_do_qty': {
        #                 '$sum': '$do_qty'
        #             }, 
        #             'do_nos': {
        #                 '$addToSet': '$do_no'
        #             }
        #         }
        #     }, {
        #         '$sort': {
        #             '_id': 1
        #         }
        #     }
        # ]

        sapRecordsPipeline = [
            {
                '$project': {
                    'month': {
                        '$dateToString': {
                            'format': '%Y%m',
                            'date': {
                                '$dateFromString': {
                                    'dateString': {
                                        '$concat': [
                                            {'$substr': ['$slno', 0, 4]}, '-',  
                                            {'$substr': ['$slno', 4, 2]}, '-01' 
                                        ]
                                    },
                                    'format': '%Y-%m-%d'
                                }
                            }
                        }
                    },
                    'year': {
                        '$substr': ['$slno', 0, 4]  
                    },
                    'do_qty': {
                        '$toDouble': '$do_qty'
                    },
                    'do_no': 1
                }
            },
            {
                '$match': {
                    'year': year_data 
                }
            },
            {
                '$group': {
                    '_id': '$month',
                    'total_do_qty': {
                        '$sum': '$do_qty'
                    },
                    'do_nos': {
                        '$addToSet': '$do_no'
                    }
                }
            },
            {
                '$sort': {
                    '_id': 1
                }
            }
        ]

        roadDataSap_result_cursor = SapRecords.objects().aggregate(sapRecordsPipeline)

        sapData_result = list(roadDataSap_result_cursor)

        # console_logger.debug(sapData_result)
        listData = []
        # Loop through each month and build the pipeline
        for month_data in sapData_result:
            month = month_data['_id']
            do_nos = month_data['do_nos']

            pipelineData = [
                {
                    '$match': {
                        'arv_cum_do_number': {
                            '$in': do_nos,
                        }
                    }
                }, {
                    '$addFields': {
                        'net_qty': {
                            '$cond': {
                                'if': {
                                    '$isNumber': '$net_qty'
                                }, 
                                'then': '$net_qty', 
                                'else': {
                                    '$toDouble': '$net_qty'
                                }
                            }
                        }
                    }
                }, {
                    '$match': {
                        'net_qty': {
                            '$ne': None
                        }
                    }
                }, {
                    '$group': {
                        '_id': month, 
                        'total_net_qty': {
                            '$sum': '$net_qty'
                        }
                    }
                }
            ]

            # console_logger.debug(pipelineData)

            gmrdata_result_cursor = Gmrdata.objects().aggregate(pipelineData)

            gmrData_result = list(gmrdata_result_cursor)

            # console_logger.debug(gmrData_result)

            listData.append(gmrData_result)

        # Flatten the list of lists
        flat_gmrData_result = [item for sublist in listData for item in sublist]

        # console_logger.debug(flat_gmrData_result)

        # Create a dictionary for quick lookup
        gmr_data_dict = {item['_id']: item['total_net_qty'] for item in flat_gmrData_result}

        # Calculate percentages
        chart_data = {
            'labels': [],
            'datasets': [{
                'label': 'Percentage',
                'data': [],
                # 'backgroundColor': 'rgba(75, 192, 192, 0.2)',
                # 'borderColor': 'rgba(75, 192, 192, 1)',
                'borderWidth': 1
            }]
        }

        for month_data in sapData_result:
            month = month_data['_id']
            total_do_qty = month_data['total_do_qty']
            total_net_qty = gmr_data_dict.get(month, 0)
            
            percentage = (total_net_qty / total_do_qty) * 100 if total_do_qty > 0 else 0
            
            # chart_data['labels'].append(total_do_qty)
            chart_data['labels'].append(month)
            chart_data['datasets'][0]['data'].append(percentage)
        return chart_data 
    except Exception as e:
        console_logger.debug("----- Wcl Linkage Matrialization Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e

@router.post("/update/rakequota", tags=["Rail Map"])
def endpoint_to_update_rakequota_data(response: Response, data:rakeQuotaUpdate):
    try:
        payload = data.dict()
        console_logger.debug(payload)
        try:
            fetchrakeQuota = rakeQuota.objects(month=datetime.datetime.strptime(payload.get("month"), "%b-%Y").strftime("%m-%Y"))
            if fetchrakeQuota:
                fetchrakeQuota.update(rake_alloted=payload.get("rakes_planned_for_month"), expected_rakes=payload.get("expected_rakes"))
        except DoesNotExist as e:
            return {"detail": "No data found"}
        
        return {"details": "success"}
    except Exception as e:
        console_logger.debug("-----Update RakeQuota Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


# Function to make the request
def make_request(endpoint_name, url, avery_id, avery_pass, proxies):
    try:
        response = requests.get(url, auth=HTTPBasicAuth(avery_id, avery_pass), proxies=proxies)
        
        # Check if the request was successful
        if response.status_code == 200:
            # print(f"{endpoint_name} Response Data:", response.json())
            return response.json()
        else:
            console_logger.debug(f"{endpoint_name} Failed to retrieve data. Status code: {response.status_code} - Message: {response.text}")
    except Exception as e:
        console_logger.debug(f"{endpoint_name} An error occurred:", str(e))


@router.get("/update/averydata", tags=["Rail Map"])
def endpoint_to_update_averydata(response:Response, start_date: str, end_date: str):
    try:
        emailData = EmailDevelopmentCheck.objects()
        
        # avery_id = "sanjaysingh@awtx-itw.com"
        # avery_pass = "Sanjay@321"

        avery_id = emailData[0].avery_id
        avery_pass = emailData[0].avery_pass

        console_logger.debug(emailData[0].avery_id)
        console_logger.debug(emailData[0].avery_pass)
        console_logger.debug(emailData[0].wagontrippler1)
        console_logger.debug(emailData[0].wagontrippler2)
        console_logger.debug(emailData[0].port)

        # Define the proxies dictionary to bypass proxy for the target IP
        proxies = {
            "http": None,
            "https": None
        }
        
        # urls = {
        #     "WagonTrippler_1": f"http://172.21.92.15:8081/API/values/getdata?FromDate={start_date}&ToDate={end_date}",          #available data from 2024/08/13
        #     "WagonTrippler_2": f"http://172.21.92.24:8081/API/values/getdata?FromDate={start_date}&ToDate={end_date}"           #available data from 2024/08/23
        # }

        urls = {
            "WagonTrippler_1": f"http://{emailData[0].wagontrippler1}:{emailData[0].port}/API/values/getdata?FromDate={start_date}&ToDate={end_date}",          #available data from 2024/08/13
            "WagonTrippler_2": f"http://{emailData[0].wagontrippler2}:{emailData[0].port}/API/values/getdata?FromDate={start_date}&ToDate={end_date}"           #available data from 2024/08/23
        }

        listData = []
        for name, url in urls.items():
            fetchAveryData = make_request(name, url, avery_id, avery_pass, proxies)
            # console_logger.debug(fetchAveryData)
            if fetchAveryData:
                for singleAveryData in fetchAveryData:
                    if "A" in singleAveryData.get("rakeId"):
                        # console_logger.debug(singleAveryData.get("rakeId").split("A")[1])
                        rr_number = singleAveryData.get("rakeId").split("A")[1]
                    elif "B" in singleAveryData.get("rakeId"):
                        # console_logger.debug(singleAveryData.get("rakeId").split("B")[1])
                        rr_number = singleAveryData.get("rakeId").split("B")[1]
                    fetchRailData = RailData.objects.get(rr_no=rr_number)

                    console_logger.debug(fetchRailData.rr_no)
                    if fetchRailData:
                        # console_logger.debug(fetchRailData.avery_rly_data)
                        for single_rail_data in fetchRailData.avery_rly_data:
                            if singleAveryData.get("wagonId")[-5:] == single_rail_data.wagon_no[-5:]:
                                console_logger.debug("matched")
                                # Update fields
                                single_rail_data.ser_no = singleAveryData.get("serNo")
                                single_rail_data.rake_no = singleAveryData.get("rakeNo")
                                single_rail_data.rake_id = singleAveryData.get("rakeId")
                                single_rail_data.wagon_no_avery = singleAveryData.get("wagonNo")
                                single_rail_data.wagon_id = singleAveryData.get("wagonId")
                                single_rail_data.wagon_type = singleAveryData.get("wagonType")
                                single_rail_data.wagon_cc = singleAveryData.get("wagonCC")
                                single_rail_data.mode = singleAveryData.get("mode")
                                single_rail_data.tip_startdate = singleAveryData.get("tipStartDate")
                                single_rail_data.tip_starttime = singleAveryData.get("tipStartTime")
                                single_rail_data.tip_enddate = singleAveryData.get("tipEndDate")
                                single_rail_data.tip_endtime = singleAveryData.get("tipEndTime")
                                single_rail_data.tipple_time = singleAveryData.get("tippleTime")
                                single_rail_data.status = singleAveryData.get("status")
                                single_rail_data.wagon_gross_time = str(singleAveryData.get("wagonGrossWt"))
                                single_rail_data.wagon_tare_wt = str(singleAveryData.get("wagonTareWt"))
                                single_rail_data.wagon_net_wt = str(singleAveryData.get("wagonNetWt"))
                                single_rail_data.time_in_tipp = singleAveryData.get("timeIn_tipp")
                                single_rail_data.po_number = singleAveryData.get("ponumber")
                                single_rail_data.coal_grade = singleAveryData.get("coalgrade")
                                
                                # # Save the changes
                                fetchRailData.save()

                    # old code        
                    # if fetchRailData:
                    #     # console_logger.debug(fetchRailData.avery_rly_data)
                    #     for single_rail_data in fetchRailData.secl_rly_data:
                    #         # console_logger.debug(singleAveryData.get("wagonId")[-5:])
                    #         # console_logger.debug(single_rail_data.wagon_no)
                    #         if singleAveryData.get("wagonId")[-5:] == single_rail_data.wagon_no[-5:]:
                    #             dictData = {
                    #                 "ser_no" : singleAveryData.get("serNo"),
                    #                 "rake_no" : singleAveryData.get("rakeNo"),
                    #                 "rake_id" : singleAveryData.get("rakeId"),
                    #                 # "rake_no": single_rail_data.wagon_no,
                    #                 "wagon_no" : singleAveryData.get("wagonNo"),
                    #                 "wagon_id" : single_rail_data.wagon_no,
                    #                 "wagon_type" : singleAveryData.get("wagonType"),
                    #                 "wagon_cc" : singleAveryData.get("wagonCC"),
                    #                 "mode" : singleAveryData.get("mode"),
                    #                 "tip_startdate" : singleAveryData.get("tipStartDate"),
                    #                 "tip_starttime" : singleAveryData.get("tipStartTime"),
                    #                 "tip_enddate" : singleAveryData.get("tipEndDate"),
                    #                 "tip_endtime" : singleAveryData.get("tipEndTime"),
                    #                 "tipple_time" : singleAveryData.get("tippleTime"),
                    #                 "status" : singleAveryData.get("status"),
                    #                 "wagon_gross_time" : str(singleAveryData.get("wagonGrossWt")),
                    #                 "wagon_tare_wt" : str(singleAveryData.get("wagonTareWt")),
                    #                 "wagon_net_wt" : str(singleAveryData.get("wagonNetWt")),
                    #                 "time_in_tipp" : singleAveryData.get("timeIn_tipp"),
                    #                 "po_number" : singleAveryData.get("ponumber"),
                    #                 "coal_grade" : singleAveryData.get("coalgrade"),
                    #             }
                    #             listData.append(AveryRailData(**dictData))
                    # fetchRailData.avery_rly_data = listData
                    # fetchRailData.save()

        return {"detail": "success"}
    except Exception as e:
        console_logger.debug("----- Road Sap Upload Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e



@router.get("/update/averydata/test", tags=["Rail Map"])
def endpoint_to_update_averydata(response:Response):
    try:
        fetchAveryData = [{'serNo': '3', 'rakeNo': '2', 'rakeId': 'B162014638', 'wagonNo': '3', 'wagonId': 'SECR    12254', 'wagonType': 'Single', 'wagonCC': '80', 'mode': 'MD MD', 'tipStartDate': '08/25/2024 00:00:00', 'tipStartTime': '00:21:55', 'tipEndDate': '08/25/2024 00:00:00', 'tipEndTime': '00:37:54', 'tippleTime': '16', 'status': None, 'wagonGrossWt': 88.55, 'wagonTareWt': 20.7, 'wagonNetWt': 67.85, 'timeIn_tipp': None, 'ponumber': '4500099650', 'coalgrade': 'G-11'}, {'serNo': '3', 'rakeNo': '2', 'rakeId': 'B162014638', 'wagonNo': '4', 'wagonId': 'WC      10219', 'wagonType': 'Single', 'wagonCC': '80', 'mode': 'MD MD', 'tipStartDate': '08/25/2024 00:00:00', 'tipStartTime': '00:40:53', 'tipEndDate': '08/25/2024 00:00:00', 'tipEndTime': '02:16:50', 'tippleTime': '96', 'status': None, 'wagonGrossWt': 89.5, 'wagonTareWt': 20.95, 'wagonNetWt': 68.55, 'timeIn_tipp': None, 'ponumber': '4500099650', 'coalgrade': 'G-11'}, {'serNo': '3', 'rakeNo': '2', 'rakeId': 'B162014638', 'wagonNo': '5', 'wagonId': 'NR       81575', 'wagonType': 'Single', 'wagonCC': '80', 'mode': 'MD MD', 'tipStartDate': '08/25/2024 00:00:00', 'tipStartTime': '02:19:39', 'tipEndDate': '08/25/2024 00:00:00', 'tipEndTime': '02:23:07', 'tippleTime': '4', 'status': None, 'wagonGrossWt': 87.1, 'wagonTareWt': 22.55, 'wagonNetWt': 64.55, 'timeIn_tipp': None, 'ponumber': '4500099650', 'coalgrade': 'G-11'}, {'serNo': '3', 'rakeNo': '2', 'rakeId': 'B162014638', 'wagonNo': '6', 'wagonId': 'SECR   812070', 'wagonType': 'Single', 'wagonCC': '80', 'mode': 'MD MD', 'tipStartDate': '08/25/2024 00:00:00', 'tipStartTime': '02:25:23', 'tipEndDate': '08/25/2024 00:00:00', 'tipEndTime': '02:46:04', 'tippleTime': '21', 'status': None, 'wagonGrossWt': 86.4, 'wagonTareWt': 24.0, 'wagonNetWt': 62.4, 'timeIn_tipp': None, 'ponumber': '4500099650', 'coalgrade': 'G-11'}, {'serNo': '3', 'rakeNo': '2', 'rakeId': 'B162014638', 'wagonNo': '7', 'wagonId': 'SECR   21836', 'wagonType': 'Single', 'wagonCC': '80', 'mode': 'MD MD', 'tipStartDate': '08/25/2024 00:00:00', 'tipStartTime': '02:48:25', 'tipEndDate': '08/25/2024 00:00:00', 'tipEndTime': '02:59:44', 'tippleTime': '11', 'status': None, 'wagonGrossWt': 85.15, 'wagonTareWt': 21.4, 'wagonNetWt': 63.75, 'timeIn_tipp': None, 'ponumber': '4500099650', 'coalgrade': 'G-11'}, {'serNo': '3', 'rakeNo': '2', 'rakeId': 'B162014638', 'wagonNo': '8', 'wagonId': 'SECR   10714', 'wagonType': 'Single', 'wagonCC': '80', 'mode': 'MD MD', 'tipStartDate': '08/25/2024 00:00:00', 'tipStartTime': '03:01:47', 'tipEndDate': '08/25/2024 00:00:00', 'tipEndTime': '03:58:32', 'tippleTime': '57', 'status': None, 'wagonGrossWt': 93.8, 'wagonTareWt': 21.95, 'wagonNetWt': 71.85, 'timeIn_tipp': None, 'ponumber': '4500099650', 'coalgrade': 'G-11'}, {'serNo': '3', 'rakeNo': '2', 'rakeId': 'B162014638', 'wagonNo': '9', 'wagonId': 'SECR   10752', 'wagonType': 'Single', 'wagonCC': '80', 'mode': 'MD MD', 'tipStartDate': '08/25/2024 00:00:00', 'tipStartTime': '04:00:49', 'tipEndDate': '08/25/2024 00:00:00', 'tipEndTime': '04:05:42', 'tippleTime': '5', 'status': None, 'wagonGrossWt': 83.05, 'wagonTareWt': 22.0, 'wagonNetWt': 61.05, 'timeIn_tipp': None, 'ponumber': '4500099650', 'coalgrade': 'G-11'}]
        listData = []
        if fetchAveryData:
            for singleAveryData in fetchAveryData:
                if "A" in singleAveryData.get("rakeId"):
                    # console_logger.debug(singleAveryData.get("rakeId").split("A")[1])
                    rr_number = singleAveryData.get("rakeId").split("A")[1]
                elif "B" in singleAveryData.get("rakeId"):
                    # console_logger.debug(singleAveryData.get("rakeId").split("B")[1])
                    rr_number = singleAveryData.get("rakeId").split("B")[1]
                fetchRailData = RailData.objects.get(rr_no=rr_number)

                console_logger.debug(fetchRailData.rr_no)
                if fetchRailData:
                    # console_logger.debug(fetchRailData.avery_rly_data)
                    for single_rail_data in fetchRailData.avery_rly_data:
                        console_logger.debug(singleAveryData.get("wagonId")[-5:])
                        console_logger.debug(single_rail_data.wagon_no[-5:])
                        if singleAveryData.get("wagonId")[-5:] == single_rail_data.wagon_no[-5:]:
                            console_logger.debug("matched")
                            # Update fields
                            single_rail_data.ser_no = singleAveryData.get("serNo")
                            single_rail_data.rake_no = singleAveryData.get("rakeNo")
                            single_rail_data.rake_id = singleAveryData.get("rakeId")
                            single_rail_data.wagon_no_avery = singleAveryData.get("wagonNo")
                            single_rail_data.wagon_id = singleAveryData.get("wagonId")
                            single_rail_data.wagon_type = singleAveryData.get("wagonType")
                            single_rail_data.wagon_cc = singleAveryData.get("wagonCC")
                            single_rail_data.mode = singleAveryData.get("mode")
                            single_rail_data.tip_startdate = singleAveryData.get("tipStartDate")
                            single_rail_data.tip_starttime = singleAveryData.get("tipStartTime")
                            single_rail_data.tip_enddate = singleAveryData.get("tipEndDate")
                            single_rail_data.tip_endtime = singleAveryData.get("tipEndTime")
                            single_rail_data.tipple_time = singleAveryData.get("tippleTime")
                            single_rail_data.status = singleAveryData.get("status")
                            single_rail_data.wagon_gross_time = str(singleAveryData.get("wagonGrossWt"))
                            single_rail_data.wagon_tare_wt = str(singleAveryData.get("wagonTareWt"))
                            single_rail_data.wagon_net_wt = str(singleAveryData.get("wagonNetWt"))
                            single_rail_data.time_in_tipp = singleAveryData.get("timeIn_tipp")
                            single_rail_data.po_number = singleAveryData.get("ponumber")
                            single_rail_data.coal_grade = singleAveryData.get("coalgrade")
                            
                            # # Save the changes
                            fetchRailData.save()

        return {"detail": "success"}
    except Exception as e:
        console_logger.debug("----- Road Sap Upload Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e       


@router.post("/update/useraverydata", tags=["Rail Map"])
def endpoint_to_update_avery_user_data(response: Response, data: mainAveryData, rr_no: str):
    try:
        payload = data.dict()
        fetchRailData = RailData.objects.get(rr_no=rr_no)
        if payload.get("data"):
            avery_user_data_instances = [AveryRailData(**item) for item in payload.get("data")]
            fetchRailData.avery_rly_data = avery_user_data_instances
        else:
            fetchRailData.avery_rly_data.clear()

        try:
            fetchRailData.save()
        except ValidationError as e:
            console_logger.error(f"Validation error while saving RailData: {e}")
            return {"details": "error", "message": str(e)}
        return {"details": "success"}
    except Exception as e:
        console_logger.debug("----- Road Sap Upload Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e       

@router.get("/fetch/avery/singlerail", tags=["Rail Map"])
def endpoint_to_fetch_avery_railway_data(response: Response, rrno: str):
    try:
        fetchRailData = RailData.objects.get(rr_no=rrno)
        return fetchRailData.averyPayload()
    except DoesNotExist as e:
        raise HTTPException(status_code=404, detail="No data found")
    except Exception as e:
        console_logger.debug("----- Fetch Railway Data Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/update/saprecordsroad", tags=["Road Map"])
def endpoint_to_update_averydata(response:Response):
    try:
        fetchsapRecords = SapRecords.objects()
        for single_saprecords in fetchsapRecords:
            if single_saprecords:
                gmrDatafetch = Gmrdata.objects(
                        arv_cum_do_number=single_saprecords.do_no,
                    )
                if gmrDatafetch:
                    gmrDatafetch.update(
                            do_date=single_saprecords.do_date, 
                            start_date=single_saprecords.start_date, 
                            end_date=single_saprecords.end_date, 
                            slno=single_saprecords.slno,
                            type_consumer= single_saprecords.consumer_type,
                            grade= single_saprecords.grade,
                            mine= single_saprecords.mine_name,
                            po_qty= single_saprecords.do_qty,
                            po_amount= single_saprecords.po_amount)
                
        return {"detail": "success"}
    except Exception as e:
        console_logger.debug("----- Road Saprecords Upload Error -----",e)
        response.status_code = 400
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
        console_logger.debug(f"---- Coal Testing Schedular ----  {testing_scheduler}")
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

# Time format for parsing and formatting time
time_format = "%H:%M"
# Time to subtract: 5 hours and 30 minutes
time_to_subtract = datetime.timedelta(hours=5, minutes=30)



backgroundTaskHandler.run_job(task_name="save consumption data",
                                func=extract_historian_data,
                                trigger="cron",
                                **{"day": "*", "hour": "*", "minute": 0})


# shift_time = "22:00"
# Adata = datetime.datetime.strptime(shift_time, time_format)
# shift_time_ist = Adata - time_to_subtract

# coal_shift_hh, coal_shift_mm = shift_time_ist.strftime(time_format).split(":")
# console_logger.debug(coal_shift_hh)
# console_logger.debug(coal_shift_mm)
# backgroundTaskHandler.run_job(task_name="save testing data",
#                                 func=coal_test,
#                                 trigger="cron",
#                                 **{"day": "*", "hour": coal_shift_hh, "minute": coal_shift_mm})

# gcv_shift_time = "22:30"
# Bdata = datetime.datetime.strptime(gcv_shift_time, time_format)
# gcv_shift_time_ist = Bdata - time_to_subtract

# gcv_shift_hh, gcv_shift_mm = gcv_shift_time_ist.strftime(time_format).split(":")
# console_logger.debug(gcv_shift_hh)
# console_logger.debug(gcv_shift_mm)
# backgroundTaskHandler.run_job(task_name="update coal gcv data",
#                                 func=endpoint_to_fetch_coal_quality_gcv,
#                                 trigger="cron",
#                                 **{"day": "*", "hour": gcv_shift_hh, "minute": gcv_shift_mm})

                                


# backgroundTaskHandler.run_job(task_name="save testing data", func=coal_test, trigger="cron", **{"day": "*", "second": 2})
                                


# fetchShiftSchedule = shiftScheduler.objects(report_name="bunker_db_schedule")
# for single_shift in fetchShiftSchedule:
#     console_logger.debug(single_shift.shift_name)
#     console_logger.debug(single_shift.start_shift_time)
#     console_logger.debug(single_shift.end_shift_time)
#     # Parse end_shift_time
#     end_shift_time = datetime.datetime.strptime(single_shift.end_shift_time, time_format)
#     # Adjust for timezone by subtracting the specified duration
#     end_shift_time_ist = end_shift_time - time_to_subtract
#     # Convert the adjusted time back to hours and minutes
#     end_shift_hh, end_shift_mm = end_shift_time_ist.strftime(time_format).split(":")
#     # Schedule the background task
#     backgroundTaskHandler.run_job(
#         task_name=single_shift.shift_name,
#         func=bunker_scheduler,
#         trigger="cron",
#         **{"day": "*", "hour": end_shift_hh, "minute": end_shift_mm}, 
#         func_kwargs={
#             "shift_name": single_shift.shift_name, 
#             "start_time": single_shift.start_shift_time, 
#             "end_time": single_shift.end_shift_time
#         }
#     )                              




if __name__ == "__main__":
    usecase_handler_object.handler.run(ip=server_ip, port=server_port)
    usecase_handler_object.handler.send_status(True)
    pre_processing()
    import uvicorn
    uvicorn.run("main:router",reload=True, host="0.0.0.0",port=7704)
    # sched.add_job(scheduled_job, "interval", seconds=10)
    # sched.start()