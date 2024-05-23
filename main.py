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
from service import host, db_port, username, password
from helpers.mail import send_email, send_test_email
import cryptocode
from mongoengine import MultipleObjectsReturned

# mahabal starts
import tabula
import math
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import re
from collections import OrderedDict
# mahabal end


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
              "description": "Road Map for Truck Journey"}]

router = FastAPI(title="GMR API's", description="Contains GMR Testing, Consumption and Roadmap apis",
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


# ---------------------------------- Mahabal data start ----------------------------------------

def mahabal_rr_lot(pdf_path):
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
            pages="all",
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


def mahabal_ulr(pdf_path):
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
            pages="all",
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


def mahabal_parameter(pdf_path):
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
            pages="all",
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


# @router.post("/pdf_data_upload", tags=[""])
# async def ectract_data_from_mahabal_pdf(response: Response, pdf_upload: Optional[UploadFile] = File(None)):
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
#         console_logger.debug(full_path)
#         with open(full_path, "wb") as file_object:
#             file_object.write(contents)


#         console_logger.debug(mahabal_rr_lot(full_path))
#         console_logger.debug(mahabal_ulr(full_path))
#         console_logger.debug(mahabal_parameter(full_path))

#         rrLot = mahabal_rr_lot(full_path)
#         ulrData = mahabal_ulr(full_path)
#         parameterData = mahabal_parameter(full_path)

#         console_logger.debug(rrLot)
#         console_logger.debug(ulrData)
#         console_logger.debug(parameterData)

#         if rrLot != None and parameterData != None:
#             if rrLot.get("rake") != None and rrLot.get("rr") != None:
#                 console_logger.debug("train data")
#                 console_logger.debug(rrLot.get("rake"))
#                 console_logger.debug(rrLot.get("rr"))
#                 try:
#                     coalTrainData = CoalTestingTrain.objects.get(rake_no=rrLot.get("rake"), rrNo=rrLot.get("rr"))
#                     console_logger.debug(coalTrainData)
#                 except DoesNotExist as e:
#                     console_logger.debug("No matching object found.")
#                     response.status_code = 404
#                     return e
#                 if ulrData.get("report_no"):
#                     coalTrainData.third_party_report_no = ulrData.get("report_no")
#                 if coalTrainData:
#                     console_logger.debug("data there")
#                     console_logger.debug(parameterData)
#                     listData = []
#                     console_logger.debug(coalTrainData.rrNo)
#                     # existing_parameter_names = set(singleData.get("parameter_Name") for singleData in coalTrainData.parameters)
#                     existing_parameter_names = [singleData.get("parameter_Name") for singleData in coalTrainData.parameters]
#                     console_logger.debug(existing_parameter_names)
#                     for key, value in parameterData.items():
#                         if value != '-':
#                             if key == 'total_moisture_adb' and "Third_Party_Total_Moisture_(Adb)" not in existing_parameter_names:
#                             # if key == 'total_moisture_adb':
#                                 # if "Third_Party_Total_Moisture_(Adb)" not in singleData:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Total_Moisture_(Adb)",
#                                     # "parameter_Name": "Third_Party_Total_Moisture_%",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalTrainData.parameters.append(addData)
#                             # elif key == 'total_moisture_arb' and "Third_Party_Total_Moisture_(Arb)" not in existing_parameter_names:
#                             elif key == 'total_moisture_arb' and "Third_Party_Total_Moisture" not in existing_parameter_names:
#                             # elif key == 'total_moisture_arb':
#                                 # if "Third_Party_Total_Moisture_(Arb)" not in singleData:
#                                 addData = {
#                                     # "parameter_Name": "Third_Party_Total_Moisture_(Arb)",
#                                     "parameter_Name": "Third_Party_Total_Moisture",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalTrainData.parameters.append(addData)
#                             elif key == 'moisture_inherent_adb' and "Third_Party_Inherent_Moisture_(Adb)" not in existing_parameter_names:
#                             # elif key == 'moisture_inherent_adb':
#                                 # if "Third_Party_Inherent_Moisture_(Adb)" not in singleData:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Inherent_Moisture_(Adb)",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalTrainData.parameters.append(addData)
#                             elif key == 'moisture_inherent_arb' and "Third_Party_Inherent_Moisture_(Arb)" not in existing_parameter_names:
#                             # elif key == 'moisture_inherent_arb':
#                                 # if "Third_Party_Inherent_Moisture_(Arb)" not in singleData:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Inherent_Moisture_(Arb)",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalTrainData.parameters.append(addData)
#                             elif key == "ash_adb" and "Third_Party_Ash_(Adb)" not in existing_parameter_names:
#                             # elif key == "ash_adb":
#                                 # if "Third_Party_Ash_(Adb)" not in singleData:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Ash_(Adb)",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalTrainData.parameters.append(addData)
#                             elif key == "ash_arb" and "Third_Party_Ash_(Arb)" not in existing_parameter_names:
#                             # elif key == "ash_arb":
#                                 # if "Third_Party_Ash_(Arb)" not in singleData:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Ash_(Arb)",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalTrainData.parameters.append(addData)
#                             elif key == "volatile_adb" and "Third_Party_Volatile_Matter_(Adb)" not in existing_parameter_names:
#                             # elif key == "volatile_adb":
#                                 # if "Third_Party_Volatile_Matter_(Adb)" not in singleData:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Volatile_Matter_(Adb)",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalTrainData.parameters.append(addData)
#                             elif key == "volatile_arb" and "Third_Party_Volatile_Matter_(Arb)" not in existing_parameter_names:
#                             # elif key == "volatile_arb":
#                                 # if "Third_Party_Volatile_Matter_(Arb)" not in singleData:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Volatile_Matter_(Arb)",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalTrainData.parameters.append(addData)
#                             elif key == "fixed_carbon_adb" and "Third_Party_Fixed_Carbon_(Adb)" not in existing_parameter_names:
#                             # elif key == "fixed_carbon_adb":
#                                 # if "Third_Party_Fixed_Carbon_(Adb)" not in singleData:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Fixed_Carbon_(Adb)",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalTrainData.parameters.append(addData)
#                             elif key == "fixed_carbon_arb" and "Third_Party_Fixed_Carbon_(Arb)" not in existing_parameter_names:
#                             # elif key == "fixed_carbon_arb":
#                                 # if "Third_Party_Fixed_Carbon_(Arb)" not in singleData:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Fixed_Carbon_(Arb)",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalTrainData.parameters.append(addData)
#                             elif key == "gross_calorific_adb" and "Third_Party_Gross_Calorific_Value_(Adb)" not in existing_parameter_names:
#                             # elif key == "gross_calorific_adb":
#                                 # if "Third_Party_Gross_Calorific_Value_(Adb)" not in singleData:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Gross_Calorific_Value_(Adb)",
#                                     "unit_Val": "Kcal/Kg",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalTrainData.parameters.append(addData)
#                             # elif key == "gross_calorific_arb":
#                             elif key == "gross_calorific_arb" and "Third_Party_Gross_Calorific_Value_(Arb)" not in existing_parameter_names:
#                                 # if "Third_Party_Gross_Calorific_Value_(Arb)" not in singleData:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Gross_Calorific_Value_(Arb)",
#                                     "unit_Val": "Kcal/Kg",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalTrainData.parameters.append(addData)
#                     console_logger.debug(coalTrainData)
#                     coalTrainData.save()
#                     return {"detail": "success"}

#             elif rrLot.get("lot") != None and rrLot.get("do") != None:
#                 try:
#                     coalRoadData = CoalTesting.objects.get(rake_no=rrLot.get("lot"), rrNo=rrLot.get("do"))
#                     console_logger.debug(coalRoadData)
#                 except DoesNotExist as e:
#                     console_logger.debug("No matching object found.")
#                     response.status_code = 404
#                     return e
#                     # return HTTPException(status_code="404", detail="No matching object found in db")
#                 console_logger.debug(coalRoadData)
#                 if ulrData.get("report_no"):
#                     coalRoadData.third_party_report_no = ulrData.get("report_no")
#                 addData = {}
#                 if coalRoadData:
#                     console_logger.debug("data there")
#                     console_logger.debug(parameterData)
#                     listData = []
#                     existing_parameter_names = [singleData.get("parameter_Name") for singleData in coalRoadData.parameters]
#                     console_logger.debug(existing_parameter_names)
#                     for key, value in parameterData.items():
#                         if value != '-':
#                             if key == 'total_moisture_adb' and "Third_Party_Total_Moisture_(Adb)" not in existing_parameter_names:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Total_Moisture_(Adb)",
#                                     # "parameter_Name": "Third_Party_Total_Moisture_%",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalRoadData.parameters.append(addData)
#                             elif key == 'total_moisture_arb' and "Third_Party_Total_Moisture" not in existing_parameter_names:
#                                 addData = {
#                                     # "parameter_Name": "Third_Party_Total_Moisture_(Arb)",
#                                     "parameter_Name": "Third_Party_Total_Moisture",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalRoadData.parameters.append(addData)
#                             elif key == 'moisture_inherent_adb' and "Third_Party_Inherent_Moisture_(Adb)" not in existing_parameter_names:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Inherent_Moisture_(Adb)",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalRoadData.parameters.append(addData)
#                             elif key == 'moisture_inherent_arb' and "Third_Party_Inherent_Moisture_(Arb)" not in existing_parameter_names:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Inherent_Moisture_(Arb)",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalRoadData.parameters.append(addData)
#                             elif key == "ash_adb" and "Third_Party_Ash_(Adb)" not in existing_parameter_names:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Ash_(Adb)",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalRoadData.parameters.append(addData)
#                             elif key == "ash_arb" and "Third_Party_Ash_(Arb)" not in existing_parameter_names:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Ash_(Arb)",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalRoadData.parameters.append(addData)
#                             elif key == "volatile_adb" and "Third_Party_Volatile_Matter_(Adb)" not in existing_parameter_names:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Volatile_Matter_(Adb)",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalRoadData.parameters.append(addData)
#                             elif key == "volatile_arb" and "Third_Party_Volatile_Matter_(Arb)" not in existing_parameter_names:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Volatile_Matter_(Arb)",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalRoadData.parameters.append(addData)
#                             elif key == "fixed_carbon_adb" and "Third_Party_Fixed_Carbon_(Adb)" not in existing_parameter_names:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Fixed_Carbon_(Adb)",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalRoadData.parameters.append(addData)
#                             elif key == "fixed_carbon_arb" and "Third_Party_Fixed_Carbon_(Arb)" not in existing_parameter_names:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Fixed_Carbon_(Arb)",
#                                     "unit_Val": "%",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalRoadData.parameters.append(addData)
#                             elif key == "gross_calorific_adb" and "Third_Party_Gross_Calorific_Value_(Adb)" not in existing_parameter_names:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Gross_Calorific_Value_(Adb)",
#                                     "unit_Val": "Kcal/Kg",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalRoadData.parameters.append(addData)
#                             elif key == "gross_calorific_arb" and "Third_Party_Gross_Calorific_Value_(Arb)" not in existing_parameter_names:
#                                 addData = {
#                                     "parameter_Name": "Third_Party_Gross_Calorific_Value_(Arb)",
#                                     "unit_Val": "Kcal/Kg",
#                                     "test_Method": "",
#                                     "val1": value,
#                                 }
#                                 coalRoadData.parameters.append(addData)
#                     coalRoadData.save()
#                     return {"detail": "success"}
#                 else:
#                     console_logger.debug("None data")
            
#     except DoesNotExist as e:
#         console_logger.debug("No matching object found.")
#         return HTTPException(status_code="404", detail="No matching object found in db")
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
    

@router.post("/pdf_data_upload", tags=[""])
async def ectract_data_from_mahabal_pdf(response: Response, pdf_upload: Optional[UploadFile] = File(None)):
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
        # console_logger.debug(full_path)
        with open(full_path, "wb") as file_object:
            file_object.write(contents)


        # console_logger.debug(mahabal_rr_lot(full_path))
        # console_logger.debug(mahabal_ulr(full_path))
        # console_logger.debug(mahabal_parameter(full_path))

        rrLot = mahabal_rr_lot(full_path)
        ulrData = mahabal_ulr(full_path)
        parameterData = mahabal_parameter(full_path)
        listData = []
        id = None
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
                    coalTrainData = CoalTestingTrain.objects.get(rake_no=f"{int(rrLot.get('rake'))}", rrNo=rrLot.get("rr"))
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
                dataDict = {"id": id, "api_data": api_data, "pdf_data": pdf_data}
                return dataDict 
            # road data
            elif rrLot.get("lot") != None and rrLot.get("do") != None:
                try:
                    coalRoadData = CoalTesting.objects.get(rake_no=f'LOT-{rrLot.get("lot")}', rrNo=rrLot.get("do"))
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
                dataDict = {"id": id, "api_data": api_data, "pdf_data": pdf_data}
                return dataDict     
        else:
            console_logger.debug("data not found")   
    except DoesNotExist as e:
        console_logger.debug("No matching object found.")
        return HTTPException(status_code="404", detail="No matching object found in db")
    except MultipleObjectsReturned:
        console_logger.debug("multiple entry found for single rrno/dono")
        return HTTPException(status_code="400", detail="multiple entry found for single rrno/dono")
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


# ---------------------------------- Mahabal data end ----------------------------------------


consumption_headers = {
'ClientToken': 'Administrator',
'Content-Type': 'application/json'}


@router.get("/load_historian_data", tags=["Coal Consumption"])                                    # coal consumption
def extract_historian_data(start_date: Optional[str] = None, end_date: Optional[str] = None):
    
    # entry = UsecaseParameters.objects.filter(Parameters__gmr_api__exists=True).first()
    entry = UsecaseParameters.objects.first()
    historian_ip = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Consumption IP') if entry else None
    historian_timer = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Consumption Duration') if entry else None

    console_logger.debug(f"---- Coal Consumption IP ----        {historian_ip}")
    console_logger.debug(f"---- Coal Consumption Duration ----  {historian_timer}")

    current_time = datetime.datetime.now(IST)
    current_date = current_time.date()

    if not end_date:
        end_date = current_date.__str__()                                                    # end_date will always be the current date
    if not start_date:
        no_of_day = historian_timer.split(":")[0]
        start_date = (current_date-timedelta(int(no_of_day))).__str__()

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
    
    response = requests.request("POST", url=consumption_url, headers=consumption_headers, data=payload)
    data = json.loads(response.text)
    console_logger.debug(data)

    for item in data["Data"]:
        tag_id = item["Data"]["TagID"]
        sum = item["Data"]["SUM"]
        created_date = item["Data"]["CreatedDate"] 

        Historian(
            tagid = tag_id,
            sum = sum,
            created_date = created_date,
            ID = Historian.objects.count() + 1).save()

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
                        "$hour": {"date": "$created_date", "timezone": timezone},
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
            console_logger.debug(date)
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

        modified_labels = [i for i in range(len(result["data"]["labels"]))]

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
                        "$hour": {"date": "$created_date", "timezone": timezone},
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
            console_logger.debug(date)
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

        modified_labels = [i for i in range(len(result["data"]["labels"]))]

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


@router.get("/extract_coal_test", tags=["Coal Testing"])
def coal_test(start_date: Optional[str] = None, end_date: Optional[str] = None):
    # entry = UsecaseParameters.objects.filter(Parameters__gmr_api__exists=True).first()
    entry = UsecaseParameters.objects.first()
    testing_ip = (
        entry.Parameters.get("gmr_api", {}).get("roi1", {}).get("Coal Testing IP")
        if entry
        else None
    )
    testing_timer = (
        entry.Parameters.get("gmr_api", {}).get("roi1", {}).get("Coal Testing Duration")
        if entry
        else None
    )

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

    coal_testing_url = f"http://{testing_ip}/limsapi/api/SampleDetails/GetSampleRecord/GetSampleRecord?Fromdate={start_date}&todate={end_date}"
    # coal_testing_url = f"http://172.21.96.145/limsapi/api/SampleDetails/GetSampleRecord/GetSampleRecord?Fromdate={start_date}&todate={end_date}"

    response = requests.request("GET", url=coal_testing_url, headers=headers, data=payload)
    
    testing_data = json.loads(response.text)

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
        CoalTesting(
            location=entry.get("sample_Desc").upper().strip(),
            rrNo=entry.get("rrNo").strip(),
            rR_Qty=entry.get("rR_Qty").strip(),
            rake_no=entry.get("rake_No").upper().strip(),
            supplier=entry.get("supplier").strip(),
            receive_date=entry.get("receive_date"),
            parameters=entry.get("parameters"),
            ID=CoalTesting.objects.count() + 1,
        ).save()

    for secl_entry in secl_extracted_data:
        CoalTestingTrain(
            location=secl_entry.get("sample_Desc").upper().strip(),
            rrNo=secl_entry.get("rrNo").strip(),
            rR_Qty=secl_entry.get("rR_Qty").strip(),
            rake_no=secl_entry.get("rake_No").strip(),
            supplier=secl_entry.get("supplier").strip(),
            receive_date=secl_entry.get("receive_date"),
            parameters=secl_entry.get("parameters"),
            ID=CoalTestingTrain.objects.count() + 1,
        ).save()

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
                            dictData['DO_Qty'] = str(values['average_DO_Qty'])
                            dictData['Gross_Calorific_Value_(Adb)'] = str(values['average_Gross_Calorific_Value_(Adb)'])
                            if values["average_Third_Party_Gross_Calorific_Value_(Adb)"] != "":    
                                dictData['Gross_Calorific_Value_Grade_(Adb)'] = values['average_GCV_Grade']
                                dictData["Third_Party_Gross_Calorific_Value_(Adb)"] = str(values["average_Third_Party_Gross_Calorific_Value_(Adb)"])
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
                        return result
                else:
                    console_logger.debug("inside else")
                    filtered_data = [entry for entry in dataList]
                    for single_data in filtered_data:
                        for mine, values in single_data['data'].items():
                            dictData = {}
                            dictData['Mine'] = mine
                            dictData['DO_Qty'] = str(values['average_DO_Qty'])
                            dictData['Gross_Calorific_Value_(Adb)'] = str(values['average_Gross_Calorific_Value_(Adb)'])
                            if values["average_Third_Party_Gross_Calorific_Value_(Adb)"] != "":
                                dictData['Gross_Calorific_Value_Grade_(Adb)'] = values['average_GCV_Grade']
                                dictData["Third_Party_Gross_Calorific_Value_(Adb)"] = str(values["average_Third_Party_Gross_Calorific_Value_(Adb)"])
                                if values.get("average_Third_Party_GCV_Grade"):
                                    dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"] = str(values["average_Third_Party_GCV_Grade"])
                                dictData["Difference_Gross_Calorific_Value_(Adb)"] = str(abs(int(float(dictData["Gross_Calorific_Value_(Adb)"])) - int(float(dictData["Third_Party_Gross_Calorific_Value_(Adb)"]))))
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
                        "Gross_Calorific_Value_(Adb)",
                        "Gross_Calorific_Value_Grade",
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
                                dictData['DO_Qty'] = str(values['average_DO_Qty'])
                                dictData['Gross_Calorific_Value_(Adb)'] = str(values['average_Gross_Calorific_Value_(Adb)'])
                                if values["average_Third_Party_Gross_Calorific_Value_(Adb)"] != "":    
                                    dictData['Gross_Calorific_Value_Grade_(Adb)'] = values['average_GCV_Grade']
                                    dictData["Third_Party_Gross_Calorific_Value_(Adb)"] = str(values["average_Third_Party_Gross_Calorific_Value_(Adb)"])
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
                                dictData['DO_Qty'] = str(values['average_DO_Qty'])
                                dictData['Gross_Calorific_Value_(Adb)'] = str(values['average_Gross_Calorific_Value_(Adb)'])
                                if values["average_Third_Party_Gross_Calorific_Value_(Adb)"] != "":
                                    dictData['Gross_Calorific_Value_Grade_(Adb)'] = values['average_GCV_Grade']
                                    dictData["Third_Party_Gross_Calorific_Value_(Adb)"] = str(values["average_Third_Party_Gross_Calorific_Value_(Adb)"])
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
                            console_logger.debug(values)
                            dictData = {}
                            dictData['Mine'] = mine
                            dictData['RR_Qty'] = str(values['average_RR_Qty'])
                            dictData['Gross_Calorific_Value_(Adb)'] = str(values['average_Gross_Calorific_Value_(Adb)'])

                            if values["average_Third_Party_Gross_Calorific_Value_(Adb)"] != "":
                                # dictData['Gross_Calorific_Value_Grade_(Adb)'] = values['average_Third_Party_GCV_Grade']
                                dictData['Gross_Calorific_Value_Grade_(Adb)'] = values['average_GCV_Grade']
                                dictData["Third_Party_Gross_Calorific_Value_(Adb)"] = str(values["average_Third_Party_Gross_Calorific_Value_(Adb)"])
                                if values.get("average_Third_Party_GCV_Grade"):
                                    dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"] = str(values["average_Third_Party_GCV_Grade"])
                                dictData["Difference_Gross_Calorific_Value_(Adb)"] = str(abs(int(float(dictData["Gross_Calorific_Value_(Adb)"])) - int(float(dictData["Third_Party_Gross_Calorific_Value_(Adb)"]))))
                                if values.get("average_Third_Party_GCV_Grade"):
                                    dictData["Difference_Gross_Calorific_Value_Grade_(Adb)"] = str(abs(int(dictData['Gross_Calorific_Value_Grade_(Adb)'].replace('G-', ''))) - int(dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"].replace('G-', '')))
                            elif values["average_Third_Party_Gross_Calorific_Value_(Adb)"]:
                                dictData['Gross_Calorific_Value_Grade_(Adb)'] = values['average_GCV_Grade']
                                dictData["Third_Party_Gross_Calorific_Value_(Adb)"] = str(values["average_Third_Party_Gross_Calorific_Value_(Adb)"])
                                if values.get("average_Third_Party_GCV_Grade"):
                                    dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"] = str(values["average_Third_Party_GCV_Grade"])
                                if dictData["Third_Party_Gross_Calorific_Value_(Adb)"] != "":
                                    dictData["Difference_Gross_Calorific_Value_(Adb)"] = str(abs(int(float(dictData["Gross_Calorific_Value_(Adb)"])) - int(float(dictData["Third_Party_Gross_Calorific_Value_(Adb)"]))))
                                if values.get("average_Third_Party_GCV_Grade"):
                                    dictData["Difference_Gross_Calorific_Value_Grade_(Adb)"] = str(abs(int(dictData['Gross_Calorific_Value_Grade_(Adb)'].replace('G-', ''))) - int(dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"].replace('G-', '')))
                            final_data.append(dictData)
                    else:
                        console_logger.debug("No data available for the given month:", month_date)
                        return result
                else:
                    console_logger.debug("inside else")
                    filtered_data = [entry for entry in dataList]
                    for single_data in filtered_data:
                        for mine, values in single_data['data'].items():
                            dictData = {}
                            dictData['Mine'] = mine
                            dictData['RR_Qty'] = str(values['average_RR_Qty'])
                            dictData['Gross_Calorific_Value_(Adb)'] = str(values['average_Gross_Calorific_Value_(Adb)'])
                            if values["average_Third_Party_Gross_Calorific_Value_(Adb)"] != "":
                                # dictData['Gross_Calorific_Value_Grade_(Adb)'] = values['average_Third_Party_GCV_Grade']
                                dictData['Gross_Calorific_Value_Grade_(Adb)'] = values['average_GCV_Grade']
                                dictData["Third_Party_Gross_Calorific_Value_(Adb)"] = str(values["average_Third_Party_Gross_Calorific_Value_(Adb)"])
                                if values.get("average_Third_Party_GCV_Grade"):
                                    dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"] = str(values["average_Third_Party_GCV_Grade"])
                                dictData["Difference_Gross_Calorific_Value_(Adb)"] = str(abs(int(float(dictData["Gross_Calorific_Value_(Adb)"])) - int(float(dictData["Third_Party_Gross_Calorific_Value_(Adb)"]))))
                                if values.get("average_Third_Party_GCV_Grade"):
                                    dictData["Difference_Gross_Calorific_Value_Grade_(Adb)"] = str(abs(int(dictData['Gross_Calorific_Value_Grade_(Adb)'].replace('G-', ''))) - int(dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"].replace('G-', '')))
                            elif values["average_Third_Party_Gross_Calorific_Value_(Adb)"] == "":
                                dictData['Gross_Calorific_Value_Grade_(Adb)'] = values['average_GCV_Grade']
                                dictData["Third_Party_Gross_Calorific_Value_(Adb)"] = str(values["average_Third_Party_Gross_Calorific_Value_(Adb)"])
                                if values.get("average_Third_Party_GCV_Grade"):
                                    dictData["Third_Party_Gross_Calorific_Value_(Adb)_grade"] = str(values["average_Third_Party_GCV_Grade"])
                                if dictData["Third_Party_Gross_Calorific_Value_(Adb)"] != "":
                                    dictData["Difference_Gross_Calorific_Value_(Adb)"] = str(abs(int(float(dictData["Gross_Calorific_Value_(Adb)"])) - int(float(dictData["Third_Party_Gross_Calorific_Value_(Adb)"]))))
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
                        "Gross_Calorific_Value_(Adb)",
                        "Gross_Calorific_Value_Grade",
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
def wcl_addon_data(response: Response, data: WCLtest):
    try:
        dataLoad = data.dict()
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
def wcl_addon_data(response: Response, data: WCLtest):
    try:
        dataLoad = data.dict()
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

            # console_logger.debug(data)

            logs = (
                CoalTesting.objects(data)
                .order_by("-ID")
                .skip(offset)
                .limit(page_len)                  
            )        

            if any(logs):
                for log in logs:
                    # result["labels"] = list(log.payload().keys())
                    result["labels"] = ["Sr.No","Mine","Lot_No","DO_No","DO_Qty", "Supplier", "Date", "Time","Id", "Total_Moisture_%", 
                                        "Inherent_Moisture_(Adb)_%", "Ash_(Adb)_%", "Volatile_Matter_(Adb)_%", "Gross_Calorific_Value_(Adb)_Kcal/Kg", 
                                        "Ash_(Arb)_%", "Volatile_Matter_(Arb)_%", "Fixed_Carbon_(Arb)_%", "Gross_Calorific_Value_(Arb)_Kcal/Kg",
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

                    headers = ["Sr.No",
                               "Mine",
                               "Lot_No",
                               "DO_No",
                               "DO_Qty", 
                               "Supplier", 
                               "Total_Moisture_%", 
                               "Inherent_Moisture_(Adb)_%", 
                               "Ash_(Adb)_%", 
                               "Volatile_Matter_(Adb)_%", 
                               "Gross_Calorific_Value_(Adb)_Kcal/Kg", 
                               "Ash_(Arb)_%", 
                               "Volatile_Matter_(Arb)_%", 
                               "Fixed_Carbon_(Arb)_%", 
                               "Gross_Calorific_Value_(Arb)_Kcal/Kg", 
                               "Third_Party_Report_No", 
                               "Third_Party_Total_Moisture_%", 
                               "Third_Party_Inherent_Moisture_(Adb)_%", 
                               "Third_Party_Ash_(Adb)_%", 
                               "Third_Party_Volatile_Matter_(Adb)_%", 
                               "Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg",  
                               "Third_Party_Ash_(Arb)_%", 
                               "Third_Party_Volatile_Matter_(Arb)_%", 
                               "Third_Party_Fixed_Carbon_(Arb)_%", 
                               "Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg", 
                               "Date", 
                               "Time"]

                    for index, header in enumerate(headers):
                        worksheet.write(0, index, header, cell_format2)

                    for row, query in enumerate(usecase_data,start=1):
                        result = query.payload()
                        worksheet.write(row, 0, count, cell_format)
                        worksheet.write(row, 1, str(result["Mine"]), cell_format)
                        worksheet.write(row, 2, str(result["Lot_No"]), cell_format)
                        worksheet.write(row, 3, str(result["DO_No"]), cell_format)
                        worksheet.write(row, 4, str(result["DO_Qty"]), cell_format)
                        worksheet.write(row, 5, str(result["Supplier"]), cell_format)
                        worksheet.write(row, 6, str(result["Total_Moisture_%"]), cell_format)
                        worksheet.write(row, 7, str(result["Inherent_Moisture_(Adb)_%"]), cell_format)
                        worksheet.write(row, 8, str(result["Ash_(Adb)_%"]), cell_format)
                        worksheet.write(row, 9, str(result["Volatile_Matter_(Adb)_%"]), cell_format)
                        worksheet.write(row, 10, str(result["Gross_Calorific_Value_(Adb)_Kcal/Kg"]), cell_format)
                        worksheet.write(row, 11, str(result["Ash_(Arb)_%"]), cell_format)
                        worksheet.write(row, 12, str(result["Volatile_Matter_(Arb)_%"]), cell_format)
                        worksheet.write(row, 13, str(result["Fixed_Carbon_(Arb)_%"]), cell_format)
                        worksheet.write(row, 14, str(result["Gross_Calorific_Value_(Arb)_Kcal/Kg"]), cell_format)
                        if result.get("Third_Party_Report_No"):
                            worksheet.write(row, 15, str(result["Third_Party_Report_No"]), cell_format)
                        if result.get("Third_Party_Total_Moisture_%"):
                            worksheet.write(row, 16, str(result["Third_Party_Total_Moisture_%"]), cell_format)
                        if result.get("Third_Party_Inherent_Moisture_(Adb)_%"):
                            worksheet.write(row, 17, str(result["Third_Party_Inherent_Moisture_(Adb)_%"]), cell_format)
                        if result.get("Third_Party_Ash_(Adb)_%"):
                            worksheet.write(row, 18, str(result["Third_Party_Ash_(Adb)_%"]), cell_format)
                        if result.get("Third_Party_Volatile_Matter_(Adb)_%"):
                            worksheet.write(row, 19, str(result["Third_Party_Volatile_Matter_(Adb)_%"]), cell_format)
                        if result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                            worksheet.write(row, 20, str(result["Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"]), cell_format)
                        if result.get("Third_Party_Ash_(Arb)_%"):
                            worksheet.write(row, 21, str(result["Third_Party_Ash_(Arb)_%"]), cell_format)
                        if result.get("Third_Party_Volatile_Matter_(Arb)_%"):
                            worksheet.write(row, 22, str(result["Third_Party_Volatile_Matter_(Arb)_%"]), cell_format)
                        if result.get("Third_Party_Fixed_Carbon_(Arb)_%"):
                            worksheet.write(row, 23, str(result["Third_Party_Fixed_Carbon_(Arb)_%"]), cell_format)
                        if result.get("Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"):
                            worksheet.write(row, 24, str(result["Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"]), cell_format)
                        worksheet.write(row, 25, str(result["Date"]), cell_format)
                        worksheet.write(row, 26, str(result["Time"]), cell_format)
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
                    "Total_Moisture_%", 
                    "Inherent_Moisture_(Adb)_%", 
                    "Ash_(Adb)_%", 
                    "Volatile_Matter_(Adb)_%", 
                    "Gross_Calorific_Value_(Adb)_Kcal/Kg", 
                    "Ash_(Arb)_%", 
                    "Volatile_Matter_(Arb)_%", 
                    "Fixed_Carbon_(Arb)_%", 
                    "Gross_Calorific_Value_(Arb)_Kcal/Kg", 
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


                    headers = ["Sr.No",
                               "Mine",
                               "Lot_No", 
                               "RR_No", 
                               "RR_Qty", 
                               "Supplier", 
                               "Total_Moisture_%", 
                               "Inherent_Moisture_(Adb)_%", 
                               "Ash_(Adb)_%", 
                               "Volatile_Matter_(Adb)_%", 
                               "Gross_Calorific_Value_(Adb)_Kcal/Kg", 
                               "Ash_(Arb)_%", 
                               "Volatile_Matter_(Arb)_%", 
                               "Fixed_Carbon_(Arb)_%", 
                               "Gross_Calorific_Value_(Arb)_Kcal/Kg", 
                               "Third_Party_Report_No", 
                               "Third_Party_Total_Moisture_%", 
                               "Third_Party_Inherent_Moisture_(Adb)_%", 
                               "Third_Party_Ash_(Adb)_%", "Third_Party_Volatile_Matter_(Adb)_%", 
                               "Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg",
                               "Third_Party_Ash_(Arb)_%", 
                               "Third_Party_Volatile_Matter_(Arb)_%", 
                               "Third_Party_Fixed_Carbon_(Arb)_%", 
                               "Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg", 
                               "Date", 
                               "Time"]

                    for index, header in enumerate(headers):
                        worksheet.write(0, index, header, cell_format2)

                    for row, query in enumerate(usecase_data,start=1):
                        result = query.payload()
                        worksheet.write(row, 0, count, cell_format)
                        worksheet.write(row, 1, str(result["Mine"]), cell_format)
                        worksheet.write(row, 2, str(result["Lot_No"]), cell_format)
                        worksheet.write(row, 3, str(result["RR_No"]), cell_format)
                        worksheet.write(row, 4, str(result["RR_Qty"]), cell_format)
                        worksheet.write(row, 5, str(result["Supplier"]), cell_format)
                        worksheet.write(row, 6, str(result["Total_Moisture_%"]), cell_format)
                        worksheet.write(row, 7, str(result["Inherent_Moisture_(Adb)_%"]), cell_format)
                        worksheet.write(row, 8, str(result["Ash_(Adb)_%"]), cell_format)
                        worksheet.write(row, 9, str(result["Volatile_Matter_(Adb)_%"]), cell_format)
                        worksheet.write(row, 10, str(result["Gross_Calorific_Value_(Adb)_Kcal/Kg"]), cell_format)
                        worksheet.write(row, 11, str(result["Ash_(Arb)_%"]), cell_format)
                        worksheet.write(row, 12, str(result["Volatile_Matter_(Arb)_%"]), cell_format)
                        worksheet.write(row, 13, str(result["Fixed_Carbon_(Arb)_%"]), cell_format)
                        worksheet.write(row, 14, str(result["Gross_Calorific_Value_(Arb)_Kcal/Kg"]), cell_format)
                        if result.get("Third_Party_Report_No"):
                            worksheet.write(row, 15, str(result["Third_Party_Report_No"]), cell_format)
                        if result.get("Third_Party_Total_Moisture_%"):
                            worksheet.write(row, 16, str(result["Third_Party_Total_Moisture_%"]), cell_format)
                        if result.get("Third_Party_Inherent_Moisture_(Adb)_%"):
                            worksheet.write(row, 17, str(result["Third_Party_Inherent_Moisture_(Adb)_%"]), cell_format)
                        if result.get("Third_Party_Ash_(Adb)_%"):
                            worksheet.write(row, 18, str(result["Third_Party_Ash_(Adb)_%"]), cell_format)
                        if result.get("Third_Party_Volatile_Matter_(Adb)_%"):
                            worksheet.write(row, 19, str(result["Third_Party_Volatile_Matter_(Adb)_%"]), cell_format)
                        if result.get("Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"):
                            worksheet.write(row, 20, str(result["Third_Party_Gross_Calorific_Value_(Adb)_Kcal/Kg"]), cell_format)
                        if result.get("Third_Party_Ash_(Arb)_%"):
                            worksheet.write(row, 21, str(result["Third_Party_Ash_(Arb)_%"]), cell_format)
                        if result.get("Third_Party_Volatile_Matter_(Arb)_%"):
                            worksheet.write(row, 22, str(result["Third_Party_Volatile_Matter_(Arb)_%"]), cell_format)
                        if result.get("Third_Party_Fixed_Carbon_(Arb)_%"):
                            worksheet.write(row, 23, str(result["Third_Party_Fixed_Carbon_(Arb)_%"]), cell_format)
                        if result.get("Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"):
                            worksheet.write(row, 24, str(result["Third_Party_Gross_Calorific_Value_(Arb)_Kcal/Kg"]), cell_format)
                        worksheet.write(row, 25, str(result["Date"]), cell_format)
                        worksheet.write(row, 26, str(result["Time"]), cell_format)
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
def gmr_table(response:Response,currentPage: Optional[int] = None, perPage: Optional[int] = None,
                    search_text: Optional[str] = None,
                    start_timestamp: Optional[str] = None,
                    end_timestamp: Optional[str] = None,
                    type: Optional[str] = "display"):
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

                    headers = [
                        "Sr.No.",
                        "PO No",
                        "PO Date",
                        "PO Qty",
                        "Delivery Challan No.",
                        "DO No",
                        "Mines Name",
                        "Grade",
                        "Type of consumer",
                        "DC Date",
                        "Vehicle No.",
                        "Vehicle Chassis No.",
                        "Fitness Expiry",
                        "Weightment Date",
                        "Weightment Time",
                        "Gross Wt. as per challan (MT)",
                        "Tare Wt. as per challan (MT)",
                        "Net Wt. as per challan (MT)",
                        "Gross Wt. as per actual (MT)",
                        "Tare Wt. as per actual (MT)",
                        "Net Wt. as per actual (MT)",
                        "Wastage",
                        "Transporter LR No.",
                        "Transporter LR Date",
                        "E-way bill No.",
                        "Vehicle Out Time",
                        "Total Net Amount",
                        "Driver Name",
                        "Gate Pass No",
                        "Gate Verified Time",
                        "Vehicle In Time",
                        "Actual Gross Wt Time",
                        "Actual Tare Wt Time",
                        "Transit Loss",
                        "LOT"
                    ]

                    for index, header in enumerate(headers):
                        worksheet.write(0, index, header, cell_format2)
                    for row, query in enumerate(usecase_data,start=1):
                        result = query.payload()
                        worksheet.write(row, 0, count, cell_format)
                        worksheet.write(row, 1, str(result["PO_No"]), cell_format)
                        worksheet.write(row, 2, str(result["PO_Date"]), cell_format)
                        worksheet.write(row, 3, str(result["PO_Qty"]), cell_format)
                        worksheet.write(row, 4, str(result["Delivery_Challan_No"]), cell_format)
                        worksheet.write(row, 5, str(result["DO_No"]), cell_format)
                        worksheet.write(row, 6, str(result["Mines_Name"]), cell_format)
                        worksheet.write(row, 7, str(result["Grade"]), cell_format)
                        worksheet.write(row, 8, str(result["Type_of_consumer"]), cell_format)
                        worksheet.write(row, 9, str(result["DC_Date"]), cell_format)
                        worksheet.write(row, 10, str(result["vehicle_number"]), cell_format)
                        worksheet.write(row, 11, str(result["Vehicle_Chassis_No"]), cell_format)
                        worksheet.write(row, 12, str(result["Fitness_Expiry"]), cell_format)
                        worksheet.write(row, 13, str(result["Weightment_Date"]), cell_format)
                        worksheet.write(row, 14, str(result["Weightment_Time"]), cell_format)
                        worksheet.write(row, 15, str(result["Challan_Gross_Wt(MT)"]), cell_format)
                        worksheet.write(row, 16, str(result["Challan_Tare_Wt(MT)"]), cell_format)
                        worksheet.write(row, 17, str(result["Challan_Net_Wt(MT)"]), cell_format)
                        worksheet.write(row, 18, str(result["GWEL_Gross_Wt(MT)"]), cell_format)
                        worksheet.write(row, 19, str(result["GWEL_Tare_Wt(MT)"]), cell_format)
                        worksheet.write(row, 20, str(result["GWEL_Net_Wt(MT)"]), cell_format)
                        worksheet.write(row, 21, str(result["Wastage"]), cell_format)
                        worksheet.write(row, 22, str(result["Transporter_LR_No"]), cell_format)
                        worksheet.write(row, 23, str(result["Transporter_LR_Date"]), cell_format)
                        worksheet.write(row, 24, str(result["Eway_bill_No"]), cell_format)
                        worksheet.write(row, 25, str(result["Vehicle_out_time"]), cell_format)
                        worksheet.write(row, 26, str(result["Total_net_amount"]), cell_format)
                        worksheet.write(row, 27, str(result["Driver_Name"]), cell_format)
                        worksheet.write(row, 28, str(result["Gate_Pass_No"]), cell_format)
                        worksheet.write(row, 29, str(result["Gate_verified_time"]), cell_format)
                        worksheet.write(row, 30, str(result["Vehicle_in_time"]), cell_format)
                        worksheet.write(row, 31, str(result["GWEL_Gross_Time"]), cell_format)
                        worksheet.write(row, 32, str(result["GWEL_Tare_Time"]), cell_format)
                        worksheet.write(row, 33, str(result["Transit_Loss"]), cell_format)
                        worksheet.write(row, 34, str(result["LOT"]), cell_format)
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


@router.get("/road/minewise_road_graph", tags=["Road Map"])
def minewise_road_analysis(response:Response,type: Optional[str] = "Daily",
                            Month: Optional[str] = None, Daily: Optional[str] = None, 
                            Year: Optional[str] = None):
    try:
        data={}
        UTC_OFFSET_TIMEDELTA = datetime.datetime.utcnow() - datetime.datetime.now()

        basePipeline = [
            {
                "$match": {
                    "created_at": {
                        "$gte": None,
                    },
                },
            },
            {
                "$project": {
                    "ts": {
                        "$hour": {"date": "$created_at", "timezone": timezone},
                    },
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

            date=Daily
            end_date =f'{date} 23:59:59'
            start_date = f'{date} 00:00:00'
            format_data = "%Y-%m-%d %H:%M:%S"
            endd_date=datetime.datetime.strptime(end_date,format_data)
            startd_date=datetime.datetime.strptime(start_date,format_data)

            basePipeline[0]["$match"]["created_at"]["$lte"] = (endd_date)
            basePipeline[0]["$match"]["created_at"]["$gte"] = (startd_date)
            

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
            basePipeline[0]["$match"]["created_at"]["$gte"] = (
                datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                + UTC_OFFSET_TIMEDELTA
                - datetime.timedelta(days=7)
            )
            basePipeline[1]["$project"]["ts"] = {"$dayOfMonth": "$created_at"}
            result = {
                "data": {
                    "labels": [
                        (
                            basePipeline[0]["$match"]["created_at"]["$gte"]
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

            date=Month
            format_data = "%Y - %m-%d"

            start_date = f'{date}-01'
            startd_date=datetime.datetime.strptime(start_date,format_data)
            
            end_date = startd_date + relativedelta( day=31)
            end_label = (end_date).strftime("%d")

            basePipeline[0]["$match"]["created_at"]["$lte"] = (end_date)
            basePipeline[0]["$match"]["created_at"]["$gte"] = (startd_date)
            basePipeline[1]["$project"]["ts"] = {"$dayOfMonth": "$created_at"}
            result = {
                "data": {
                    "labels": [
                        (
                            basePipeline[0]["$match"]["created_at"]["$gte"]
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

            date=Year
            end_date =f'{date}-12-31 23:59:59'
            start_date = f'{date}-01-01 00:00:00'
            format_data = "%Y-%m-%d %H:%M:%S"
            endd_date=datetime.datetime.strptime(end_date,format_data)
            startd_date=datetime.datetime.strptime(start_date,format_data)

            basePipeline[0]["$match"]["created_at"]["$lte"] = (
                endd_date
            )
            basePipeline[0]["$match"]["created_at"]["$gte"] = (
                startd_date          
            )

            basePipeline[1]["$project"]["ts"] = {"$month": "$created_at"}
            result = {
                "data": {
                    "labels": [
                        (
                            basePipeline[0]["$match"]["created_at"]["$gte"]
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
                        basePipeline[0]["$match"]["created_at"]["$gte"]
                        + datetime.timedelta(days=i + 1)
                    ).strftime("%d-%m-%Y,%a")
                    for i in range(1, 8)
                ]
            
            elif type == "Month":
                modified_labels = [
                    (
                        basePipeline[0]["$match"]["created_at"]["$gte"]
                        + datetime.timedelta(days=i + 1)
                    ).strftime("%d/%m")
                    for i in range(-1, (int(end_label))-1)
                ]

            elif type == "Year":
                modified_labels = [
                    (
                        basePipeline[0]["$match"]["created_at"]["$gte"]
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



@router.get("/road/vehicle_in_count", tags=["Road Map"])
def daywise_in_vehicle_count(response:Response):
    try:
        current_time = datetime.datetime.now(IST)
        today = current_time.date()
        startdate = f'{today} 00:00:00'
        # from_ts = datetime.datetime.strptime(startdate,"%Y-%m-%d %H:%M:%S")
        from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")

        vehicle_count = Gmrdata.objects(created_at__gte=from_ts, vehicle_in_time__ne=None).count()

        return {"title": "Vehicle in count",
                "data": vehicle_count,
                "last_updated": today}

    except Exception as e:
        console_logger.debug("----- Road Vehicle Count Error -----",e)
        response.status_code = 400
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


@router.get("/road/vehicle_out_count", tags=["Road Map"])
def daywise_out_vehicle_count(response:Response):
    try:
        current_time = datetime.datetime.now(IST)
        today = current_time.date()
        startdate = f'{today} 00:00:00'
        # from_ts = datetime.datetime.strptime(startdate,"%Y-%m-%d %H:%M:%S")
        from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")

        vehicle_count = Gmrdata.objects(GWEL_Tare_Time__gte=from_ts, vehicle_out_time__ne=None).count()

        return {"title": "Vehicle out count",
                "data": vehicle_count,
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
                            "created_at": {"$gte": from_ts},
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
        
        result = Gmrdata.objects.aggregate(pipeline)

        total_coal = 0
        for doc in result:
            total_coal = doc["total_net_qty"]

        return {"title": "Total GRN Coal(MT)",
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
                            "created_at": {"$gte": from_ts},
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
                    }]
        
        result = Gmrdata.objects.aggregate(pipeline)

        total_coal = 0
        for doc in result:
            total_coal = doc["total_actual_net_qty"]

        return {"title": "Total GWEL Coal(MT)",
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


        data = {"data": {}, "Total": {"vehicle_count": 0, "Challan_Net_Wt(MT)": 0, "GWEL_Net_Wt(MT)": 0}}

        mine_pipeline = [
                    {
                        "$match": {
                            "created_at": {"$gte": from_ts},
                        }
                    },
                    {
                        "$group": {
                            "_id": "$mine",
                            "vehicle_count": {
                                "$sum": 1
                            },
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
                    }
                ]

        if type == "display":

                if start_timestamp:
                    end_date =f'{start_timestamp} 23:59:59'
                    start_date = f'{start_timestamp} 00:00:00'
                    format_data = "%Y-%m-%d %H:%M:%S"

                    startd_date = convert_to_utc_format(start_date, format_data)
                    endd_date = convert_to_utc_format(end_date, format_data)

                    mine_pipeline[0]["$match"]["created_at"]["$lte"] = (endd_date)
                    mine_pipeline[0]["$match"]["created_at"]["$gte"] = (startd_date)

                mine_data = list(Gmrdata.objects.aggregate(mine_pipeline))

                for mine in mine_data:
                    mine_name = mine["_id"]
                    vehicle_count = mine["vehicle_count"]
                    net_qty = mine["net_qty"]
                    actual_net_qty = mine["actual_net_qty"]

                    data["data"][mine_name] = {
                        "vehicle_count": vehicle_count,
                        "Challan_Net_Wt(MT)": round(net_qty,2),
                        "GWEL_Net_Wt(MT)": round(actual_net_qty,2)}
                    
                    data["Total"]["vehicle_count"] += vehicle_count
                    data["Total"]["Challan_Net_Wt(MT)"] += round(net_qty,2)
                    data["Total"]["GWEL_Net_Wt(MT)"] += round(actual_net_qty,2)

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
                end_date =f'{end_timestamp} 23:59:59'
                start_date = f'{start_timestamp} 00:00:00'
                format_data = "%Y-%m-%d %H:%M:%S"
                
                startd_date = convert_to_utc_format(start_date, format_data)
                endd_date = convert_to_utc_format(end_date, format_data)

                mine_pipeline[0]["$match"]["created_at"]["$lte"] = (endd_date)
                mine_pipeline[0]["$match"]["created_at"]["$gte"] = (startd_date)

            mine_data = list(Gmrdata.objects.aggregate(mine_pipeline))

            for mine in mine_data:
                mine_name = mine["_id"]
                vehicle_count = mine["vehicle_count"]
                net_qty = mine["net_qty"]
                actual_net_qty = mine["actual_net_qty"]

                data["data"][mine_name] = {
                    "vehicle_count": vehicle_count,
                    "Challan_Net_Wt(MT)": net_qty,
                    "GWEL_Net_Wt(MT)": actual_net_qty}
                
                data["Total"]["vehicle_count"] += vehicle_count
                data["Total"]["Challan_Net_Wt(MT)"] += net_qty
                data["Total"]["GWEL_Net_Wt(MT)"] += actual_net_qty

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

        vehicle_count = Gmrdata.objects(vehicle_in_time__gte=from_ts, vehicle_in_time__lte=to_ts, vehicle_in_time__ne=None).count()

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
        enddate = f'{date} 00:00:00'
        # from_ts = datetime.datetime.strptime(startdate,"%Y-%m-%d %H:%M:%S")
        # to_ts = datetime.datetime.strptime(enddate,"%Y-%m-%d %H:%M:%S")
        from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")
        to_ts = convert_to_utc_format(enddate, "%Y-%m-%d %H:%M:%S")

        pipeline = [
                    {
                        "$match": {
                            "vehicle_in_time": {"$gte": from_ts},
                            "vehicle_in_time": {"$lte": to_ts},
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
        
        result = Gmrdata.objects.aggregate(pipeline)

        total_coal = 0
        for doc in result:
            total_coal = doc["total_net_qty"]

        console_logger.debug({"title": "Total GRN Coal(MT)",
                "data": round(total_coal,2)})

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
        # from_ts = datetime.datetime.strptime(startdate,"%Y-%m-%d %H:%M:%S")
        # to_ts = datetime.datetime.strptime(enddate,"%Y-%m-%d %H:%M:%S")
        from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")
        to_ts = convert_to_utc_format(enddate, "%Y-%m-%d %H:%M:%S")
        pipeline = [
                    {
                        "$match": {
                            "vehicle_in_time": {"$gte": from_ts},
                            "vehicle_in_time": {"$lte": to_ts},
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
                    }]
        
        result = Gmrdata.objects.aggregate(pipeline)

        total_coal = 0
        for doc in result:
            total_coal = doc["total_actual_net_qty"]

        return {"title": "Total GWEL Coal(MT)",
                "data": round(total_coal,2)}

    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


def daywise_out_vehicle_count_datewise(date):
    try:
        startdate = f'{date} 00:00:00'
        enddate = f'{date} 23:59:59'
        # from_ts = datetime.datetime.strptime(startdate,"%Y-%m-%d %H:%M:%S")
        # to_ts = datetime.datetime.strptime(enddate,"%Y-%m-%d %H:%M:%S")
        from_ts = convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")
        to_ts = convert_to_utc_format(enddate, "%Y-%m-%d %H:%M:%S")

        # vehicle_count = Gmrdata.objects(created_at__gte=from_ts, created_at__lte=to_ts, vehicle_out_time__ne=None).count()
        vehicle_count = Gmrdata.objects(GWEL_Tare_Time__gte=from_ts, GWEL_Tare_Time__lte=to_ts, vehicle_out_time__ne=None).count()

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

            # specified_date = datetime.datetime.strptime(specified_date, "%Y-%m-%d")
            specified_date = convert_to_utc_format(specified_date, "%Y-%m-%d")
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
            fetchGmrData = Gmrdata.objects(created_at__gte=datetime.datetime.strptime(start_of_month, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"), created_at__lte=datetime.datetime.strptime(end_of_month, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"))
            
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
              
            # console_logger.debug(aopList)

            net_qty_totals = {}
            actual_net_qty_totals = {}

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
        console_logger.debug(date)
        # Initialize the current year
        year_of_date = date.year
        # Initialize the current financial year start date
        financial_year_start_date = datetime.datetime.strptime(str(year_of_date) + "-04-01", "%Y-%m-%d").date()
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

        logs = (
            Gmrdata.objects(vehicle_in_time__gte=financial_year.get("start_date"), vehicle_in_time__lte=financial_year.get("end_date"))
            .order_by("mine", "arv_cum_do_number", "-created_at")
        )

        # logs = (
        #     Gmrdata.objects()
        #     .order_by("mine", "arv_cum_do_number", "-created_at")
        # )
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
                    if payload.get("GWEL_Net_Wt(MT)"):
                        aggregated_data[month][do_no]["actual_net_qty"] += float(
                            payload["GWEL_Net_Wt(MT)"]
                        )
                    if payload.get("Challan_Net_Wt(MT)"):
                        aggregated_data[month][do_no]["net_qty"] += float(
                            payload.get("Challan_Net_Wt(MT)")
                        )
                    if payload.get("Mines_Name"):
                        aggregated_data[month][do_no]["mine_name"] = payload[
                            "Mines_Name"
                        ]
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

            for key, single_count in total_monthly_final_net_qty.items():
                if datetime.datetime.strptime(key, "%Y-%m").year in yearly_final_data:
                    yearly_final_data[datetime.datetime.strptime(key, "%Y-%m").year] += single_count
                else:
                    yearly_final_data[datetime.datetime.strptime(key, "%Y-%m").year] = single_count
                    
            yearly_final_data_sort = dict(sorted(yearly_final_data.items()))

        return total_monthly_final_net, yearly_final_data_sort

    except Exception as e:
        console_logger.debug("----- Gate Vehicle Count Error -----",e)
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

@router.get("/pdf_minewise_road", tags=["Road Map"])
def generate_gmr_report(
    response: Response,
    specified_date: str,
    mine: Optional[str] = "All",
):
    try:
        if specified_date:
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

            # specified_change_date = datetime.datetime.strptime(specified_date, "%Y-%m-%d")
            specified_change_date = convert_to_utc_format(specified_date, "%Y-%m-%d")

            start_of_month = specified_change_date.replace(day=1)

            start_date = datetime.datetime.strftime(start_of_month, '%Y-%m-%d')
            end_date = datetime.datetime.strftime(specified_change_date, '%Y-%m-%d')

            logs = (
                Gmrdata.objects()
                .order_by("mine", "arv_cum_do_number", "-created_at")
            )
            coal_testing = CoalTesting.objects(receive_date__gte=start_date, receive_date__lte=end_date).order_by("-ID")
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

                for single_log in coal_testing:
                    coal_date = single_log.receive_date.strftime("%Y-%m")
                    coal_payload = single_log.gradepayload()
                    mine = coal_payload["Mine"]
                    doNo = coal_payload["DO_No"]
                    if coal_payload.get("Gross_Calorific_Value_(Adb)"):
                        aggregated_coal_data[coal_date][doNo]["Gross_Calorific_Value_(Adb)"] += float(coal_payload["Gross_Calorific_Value_(Adb)"])
                        aggregated_coal_data[coal_date][doNo]["coal_count"] += 1

                start_dates = {}
                for log in logs:
                    if log.vehicle_in_time!=None:
                        month = log.vehicle_in_time.strftime("%Y-%m")
                        date = log.vehicle_in_time.strftime("%Y-%m-%d")
                        payload = log.payload()
                        result["labels"] = list(payload.keys())
                        mine_name = payload.get("Mines_Name")
                        do_no = payload.get("DO_No")
                        # If start_date is None or the current vehicle_in_time is earlier than start_date, update start_date
                        if do_no not in start_dates:
                            start_dates[do_no] = date
                        elif date < start_dates[do_no]:
                            start_dates[do_no] = date
                        if payload.get("PO_Qty"):
                            aggregated_data[date][do_no]["DO_Qty"] += float(
                                payload["PO_Qty"]
                            )
                        if payload.get("Challan_Net_Wt(MT)"):
                            aggregated_data[date][do_no]["challan_lr_qty"] += float(
                                payload.get("Challan_Net_Wt(MT)")
                            )
                        if payload.get("Mines_Name"):
                            aggregated_data[date][do_no]["mine_name"] = payload[
                                "Mines_Name"
                            ]
                        aggregated_data[date][do_no]["count"] += 1 

                dataList = [
                    {
                        "date": date,
                        "data": {
                            do_no: {
                                "DO_Qty": data["DO_Qty"],
                                "challan_lr_qty": data["challan_lr_qty"],
                                "mine_name": data["mine_name"],
                                "date": date,
                            }
                            for do_no, data in aggregated_data[date].items()
                        },
                    }
                    for date in aggregated_data
                ]

                coalDataList = [
                    {"date": coal_date, "data": {
                        doNo: {
                            "average_Gross_Calorific_Value_(Adb)": data["Gross_Calorific_Value_(Adb)"] / data["coal_count"],
                        } for doNo, data in aggregated_coal_data[coal_date].items()
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
                            dictData["DO_No"] = data_dom
                            dictData["mine_name"] = values["mine_name"]
                            dictData["DO_Qty"] = values["DO_Qty"]
                            dictData["challan_lr_qty"] = values["challan_lr_qty"]
                            dictData["date"] = values["date"]
                            dictData["cumulative_challan_lr_qty"] = 0
                            dictData["balance_qty"] = 0
                            dictData["percent_supply"] = 0
                            dictData["asking_rate"] = 0
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
                        # Find the index of the month data in dataList
                        index_of_month = next((index for index, item in enumerate(dataList) if item['date'] == specified_date), None)

                        # If the month is not found, exit or handle the case
                        if index_of_month is None:
                            print("Month data not found.")
                            exit()

                        # Iterate over final_data
                        for entry in final_data:
                            do_no = entry["DO_No"]
                            cumulative_lr_qty = 0
                            
                            # Iterate over dataList from the first month to the current month
                            for i in range(index_of_month + 1):
                                month_data = dataList[i]
                                data = month_data["data"].get(do_no)
                                
                                # If data is found for the DO_No in the current month, update cumulative_lr_qty
                                if data:
                                    cumulative_lr_qty += data['challan_lr_qty']
                            
                            # Update cumulative_challan_lr_qty in final_data
                            entry['cumulative_challan_lr_qty'] = cumulative_lr_qty
                            if data["DO_Qty"] != 0 and entry["cumulative_challan_lr_qty"] != 0:
                                entry["percent_supply"] = (entry["cumulative_challan_lr_qty"] / data["DO_Qty"]) * 100
                            else:
                                entry["percent_supply"] = 0

                            if entry["cumulative_challan_lr_qty"] != 0 and data["DO_Qty"] != 0:
                                entry["balance_qty"] = (data["DO_Qty"] - entry["cumulative_challan_lr_qty"])
                            else:
                                entry["balance_qty"] = 0
                            
                            if entry["balance_qty"] and entry["balance_qty"] != 0:
                                if entry["balance_days"]:
                                    entry["asking_rate"] = entry["balance_qty"] / entry["balance_days"]

                
                rrNo_values, clubbed_data, aopList = bar_graph_data(specified_date)
                clubbed_data_final = gmr_main_graph()
                total_monthly_final_net_qty, yearly_final_data = transit_loss_gain_road_mode()

                # counter data
                dayWiseVehicleInCount = daywise_in_vehicle_count_datewise(specified_date)
                dayWiseGrnReceive = daywise_grn_receive_datewise(specified_date)
                dayWiseGwelReceive = daywise_gwel_receive_pdf_datewise(specified_date)
                dayWiseOutVehicelCount = daywise_out_vehicle_count_datewise(specified_date)
                
                if specified_date:
                    month_data = specified_date
                    fetchData = generate_report(final_data, rrNo_values, month_data, clubbed_data, clubbed_data_final, dayWiseVehicleInCount, dayWiseGrnReceive, dayWiseGwelReceive, dayWiseOutVehicelCount, total_monthly_final_net_qty, yearly_final_data, aopList)
                    return fetchData
                else:
                    fetchData = generate_report(final_data, rrNo_values, "", clubbed_data, clubbed_data_final, dayWiseVehicleInCount, dayWiseGrnReceive, dayWiseGwelReceive, dayWiseOutVehicelCount, total_monthly_final_net_qty, yearly_final_data, aopList)
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

@router.post("/add/scheduler", tags=[""])
def endpoint_to_add_scheduler(response: Response, payload: MisReportData):
    try:
        dataName = payload.dict()
        try:
            reportScheduler = ReportScheduler.objects.get(report_name=dataName.get("report_name"))
            reportScheduler.recipient_list = dataName.get("recipient_list")
            reportScheduler.filter = dataName.get("filter")
            reportScheduler.schedule = dataName.get("schedule")
            # time_fetch_data = datetime.datetime.strptime(dataName.get("time"), "%H:%M") + timedelta(hours=5, minutes=30)
            # reportScheduler.time = time_fetch_data.strftime("%H:%M")
            reportScheduler.time = dataName.get("time")
            reportScheduler.save()

        except DoesNotExist as e:
            # time_fetch_data = datetime.datetime.strptime(dataName.get("time"), "%H:%M") + timedelta(hours=5, minutes=30)
            reportScheduler = ReportScheduler(report_name=dataName.get("report_name"), recipient_list=dataName.get("recipient_list"), filter = dataName.get("filter"), schedule = dataName.get("schedule"), time=dataName.get("time"))
            reportScheduler.save()

        hh, mm = reportScheduler.time.split(":")
        if len(reportScheduler) > 0:
            if reportScheduler.filter == "daily":
                backgroundTaskHandler.run_job(task_name=reportScheduler.report_name, func=send_report_generate, trigger="cron", **{"day": "*", "hour": hh, "minute": mm})
                # backgroundTaskHandler.run_job(task_name=reportScheduler.report_name, func=send_report_generate, trigger="cron", **{"day": "*", "second": 2})
            elif reportScheduler.filter == "weekly":
                # backgroundTaskHandler.run_job(task_name=reportScheduler.report_name, func=send_report_generate, trigger="cron", **{"week": reportScheduler.schedule}) # week (int|str) - ISO week (1-53)
                backgroundTaskHandler.run_job(task_name=reportScheduler.report_name, func=send_report_generate, trigger="cron", **{"day_of_week": reportScheduler.schedule, "hour": hh, "minute": mm})
            elif reportScheduler.filter == "monthly":
                # backgroundTaskHandler.run_job(task_name=reportScheduler.report_name, func=send_report_generate, trigger="cron", **{"month": reportScheduler.schedule}) # month (int|str) - month (1-12)
                backgroundTaskHandler.run_job(task_name=reportScheduler.report_name, func=send_report_generate, trigger="cron", **{"day": reportScheduler.schedule, "hour": hh, "minute": mm})

        return {"detail": "success"}
    except Exception as e:
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        return {"detail": "failed"}


@router.post("/smtp", tags=[""])
async def add_smtp_settings(response: Response, payload: SmtpSettingsPostIn):
    """
    Smtp Settings
    """
    if SmtpSettings.objects.first() is not None:
        # console_logger.debug(SmtpSettings.objects.first())
        raise HTTPException(status_code=409)
    encrypt_pass = cryptocode.encrypt(payload.dict()["Smtp_password"], "8tFXLF46fRUkRFqJrfMjIbYAYeEJKyqB")
    # console_logger.debug(payload.dict())

    mail = send_test_email(payload)

    # console_logger.debug(mail)
    # sending test email
    if mail == "success":
        payload = payload.dict(exclude_none=True, exclude_defaults=True)
        payload["Smtp_password"] = encrypt_pass
        smtp_settings = SmtpSettings(**payload)
        smtp_settings.save()

        raise HTTPException(status_code=200, detail="success")
    else:
        console_logger.debug("Failed to Send")
        raise HTTPException(status_code=400, detail="Invalid Smtp Settings")



@router.get("/smtp", tags=[""])
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
    

@router.post("/insert/aoptarget", tags=[""])
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

@router.get("/fetch/coallocation", tags=[""])
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


@router.get("/fetch/aoptarget", tags=[""])
def endpoint_to_fetch_aoptarget(response: Response):
    try:
        dataList = []
        fetchAopTargetData = AopTarget.objects()
        for singleAopTarget in fetchAopTargetData:
            dataList.append(singleAopTarget.payload())
        return dataList
    except Exception as e:
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        return {"detail": "failed"}
    

@router.get("/fetch/scheduler", tags=[""])
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
    
def send_report_generate():
    try:
        console_logger.debug("scheduler report generate")
        reportSchedule = ReportScheduler.objects()
        for singleReportschedule in reportSchedule:
            if singleReportschedule["report_name"] == "daily_coal_logistic_report":
                generateReportData = generate_gmr_report(Response, datetime.date.today().strftime("%Y-%m-%d"), "All")
                console_logger.debug(f"{os.path.join(os.getcwd())}/{generateReportData}")
                smtpData = SmtpSettings.objects.first()
                for receiver_email in singleReportschedule.recipient_list:
                    subject = f"GMR Daily Coal Logistic Report {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                    body = f"Daily Coal Logistic Report for Date: {datetime.datetime.strptime(datetime.datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d').strftime('%d %B %Y')}"
                    send_email(smtpData.Smtp_user, subject, smtpData.Smtp_password, smtpData.Smtp_host, smtpData.Smtp_port, receiver_email, body, f"{os.path.join(os.getcwd())}{generateReportData}")
        return "success"
    except Exception as e:
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug("Error on line {}".format(sys.exc_info()[-1].tb_lineno))
        return {"detail": "failed"}


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

    consumption_scheduler = None
    try:
        gmr_dict = params.Parameters.get('gmr_api', {})
        roi_dict = gmr_dict.get('roi1', {})
        consumption_dict = roi_dict.get('Coal Consumption Scheduler', {})
        if consumption_dict:
            consumption_scheduler = consumption_dict.get("time")
    except AttributeError:
        console_logger.error("Error accessing nested dictionary for consumption_scheduler.")
        consumption_scheduler = None

    if consumption_scheduler is not None:
        console_logger.debug(f"---- Coal Consumption Schedular ----     {consumption_scheduler}")
        consumption_hr, consumption_min = consumption_scheduler.split(":")


console_logger.debug(f"---- Coal Testing Hr ----          {testing_hr}")
console_logger.debug(f"---- Coal Testing Min ----         {testing_min}")
console_logger.debug(f"---- Coal Consumption Hr ----      {consumption_hr}")
console_logger.debug(f"---- Coal Consumption Min ----     {consumption_min}")


backgroundTaskHandler.run_job(task_name="save consumption data",
                                func=extract_historian_data,
                                trigger="cron",
                                **{"day": "*", "hour": consumption_hr, "minute": consumption_min})


backgroundTaskHandler.run_job(task_name="save testing data",
                                func=coal_test,
                                trigger="cron",
                                **{"day": "*", "hour": testing_hr, "minute": testing_min})

# console_logger.debug(datetime.datetime.utcnow())


if __name__ == "__main__":
    usecase_handler_object.handler.run(ip=server_ip, port=server_port)
    usecase_handler_object.handler.send_status(True)
    pre_processing()
    import uvicorn
    uvicorn.run("main:router",reload=True, host="0.0.0.0",port=7704)
    # sched.add_job(scheduled_job, "interval", seconds=10)
    # sched.start()
