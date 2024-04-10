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
from fastapi import FastAPI
import json
from fastapi import Response
from lxml import etree
import xml.etree.ElementTree as ET
import datetime
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
from helpers.serializer import *
from mongoengine.queryset.base import Q
from datetime import timedelta
import requests
from helpers.scheduler import backgroundTaskHandler
from dateutil.relativedelta import relativedelta
import copy
from helpers.read_timezone import read_timezone_from_file
from helpers.serializer import *
import xlsxwriter
from typing import Optional


### database setup
host = os.environ.get("HOST", "192.168.1.57")
db_port = int(os.environ.get("DB_PORT", 30000))
username = os.environ.get("USERNAME", "gmr_api")
password = os.environ.get("PASSWORD", "Q1hTpYkpYNRzsUVs")

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


usecase_handler_object.handler = rdx.SocketHandler(
    service_id=service_id,
    parent_ids=parent_ids,
)


tags_meta = [{"name":"Coal Consumption",
              "description": "Coal Consumption Data"},
              {"name":"Coal Testing",
              "description": "Coal Testing And Sampling"},
              {"name":"Road Map Table",
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


consumption_headers = {
'ClientToken': 'Administrator',
'Content-Type': 'application/json'}


@router.get("/load_historian_data", tags=["Coal Consumption"])                                    # coal consumption
def extract_historian_data():
    
    # entry = UsecaseParameters.objects.filter(Parameters__gmr_api__exists=True).first()
    entry = UsecaseParameters.objects.first()
    historian_ip = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Consumption IP') if entry else None
    historian_timer = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Consumption Duration') if entry else None

    console_logger.debug(f"---- Coal Consumption IP ----        {historian_ip}")
    console_logger.debug(f"---- Coal Consumption Duration ----  {historian_timer}")

    no_of_day = historian_timer.split(":")[0]
    end_date = datetime.date.today().__str__()                                                    # end_date will always be the current date
    start_date = (datetime.date.today()-timedelta(int(no_of_day))).__str__()

    console_logger.debug(f" --- Consumption Start Date --- {start_date}")
    console_logger.debug(f" --- Consumption End Date --- {end_date}")

    payload = json.dumps({
                "StartTime": start_date,
                "EndTime": end_date, 
                "RetrievalType": "Aggregate", 
                "RetrievalMode": "History", 
                "TagID": ["2","3538"],
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



@router.get("/coal_comsumption_graph", tags=["Coal Consumption"])
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
                        {"label": "Unit 1", "data": [0 for i in range(1, 25)]},             # unit 1 = tagid_2
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
                        {"label": "Unit 1", "data": [0 for i in range(1, 8)]},              # unit 1 = tagid_2
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
                        {"label": "Unit 1", "data": [0 for i in range(-1, (int(end_label))-1)]},        # unit 1 = tagid_2
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
                        {"label": "Unit 1", "data": [0 for i in range(0, 12)]},                     # unit 1 = tagid_2
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
                    if key == 2:
                        result["data"]["datasets"][0]["data"][index] = total_sum
                    else:
                        result["data"]["datasets"][1]["data"][index] = total_sum

        result["data"]["labels"] = copy.deepcopy(modified_labels)
        console_logger.debug(f"-------- Coal Consumption Response -------- {result}")
        return result
    
    except Exception as e:
        response.status_code = 400
        console_logger.debug(e)
        return e


#  x------------------------------    Coal Quality Testing Api's    ------------------------------------x


@router.get("/coal_test", tags=["Coal Testing"])
def coal_test():
    # entry = UsecaseParameters.objects.filter(Parameters__gmr_api__exists=True).first()
    entry = UsecaseParameters.objects.first()
    testing_ip = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Testing IP') if entry else None
    testing_timer = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Testing Duration') if entry else None
    
    console_logger.debug(f"---- Coal Testing IP ----            {testing_ip}")
    console_logger.debug(f"---- Coal Testing Duration ----      {testing_timer}")

    no_of_day = testing_timer.split(":")[0]
    payload={}
    headers = {}
    end_date = datetime.date.today()                                      #  end_date will always be the current date
    start_date = (end_date-timedelta(int(no_of_day))).__str__()
    console_logger.debug(f" --- Test Start Date --- {start_date}")
    console_logger.debug(f" --- Test End Date --- {end_date}")

    coal_testing_url = f"http://{testing_ip}/limsapi/api/SampleDetails/GetSampleRecord/GetSampleRecord?Fromdate={start_date}&todate={end_date}"
    # coal_testing_url = f"http://172.21.96.145/limsapi/api/SampleDetails/GetSampleRecord/GetSampleRecord?Fromdate={start_date}&todate={end_date}"

    response = requests.request("GET", url = coal_testing_url,headers=headers, data=payload)
    testing_data = json.loads(response.text)

    wcl_extracted_data = []
    secl_extracted_data = []

    for entry in testing_data["responseData"]:
        if entry["supplier"] == "WCL" and entry["rrNo"] != "" and entry["rrNo"] != "NA":

            data = {
                "sample_Desc": entry["sample_Desc"],
                "rrNo": entry["rrNo"],
                "rR_Qty": entry["rR_Qty"],
                "rake_No": entry["rake_No"],
                "supplier": entry["supplier"],
                "receive_date": entry["sample_Received_Date"],
                "parameters": [] 
            }

            for param in entry["sample_Parameters"]:
                param_info = {
                    "parameter_Name": param.get('parameter_Name'),
                    "unit_Val": param["unit_Val"],
                    "test_Method": param["test_Method"],
                    "val1": param["val1"]
                }
                data["parameters"].append(param_info)
            wcl_extracted_data.append(data)

        
        if entry["supplier"] == "SECL" and entry["rrNo"] != "" and entry["rrNo"] != "NA":

            secl_data = {
                "sample_Desc": entry["sample_Desc"],
                "rrNo": entry["rrNo"],
                "rR_Qty": entry["rR_Qty"],
                "rake_No": entry["rake_No"],
                "supplier": entry["supplier"],
                "receive_date": entry["sample_Received_Date"],
                "parameters": [] 
            }

            for secl_param in entry["sample_Parameters"]:
                param_info = {
                    "parameter_Name": secl_param.get('parameter_Name'),
                    "unit_Val": secl_param["unit_Val"],
                    "test_Method": secl_param["test_Method"],
                    "val1": secl_param["val1"]
                }
                secl_data["parameters"].append(param_info)
            secl_extracted_data.append(secl_data)

    for entry in wcl_extracted_data:
        CoalTesting(
            location = entry["sample_Desc"].upper(),
            rrNo = entry["rrNo"],
            rR_Qty = entry["rR_Qty"],
            rake_no = entry["rake_No"],
            supplier = entry["supplier"],
            receive_date = entry["receive_date"],
            parameters = entry["parameters"],
            ID = CoalTesting.objects.count() + 1
        ).save()

    for secl_entry in secl_extracted_data:
        CoalTestingTrain(
            location = secl_entry["sample_Desc"].upper(),
            rrNo = secl_entry["rrNo"],
            rR_Qty = secl_entry["rR_Qty"],
            rake_no = secl_entry["rake_No"],
            supplier = secl_entry["supplier"],
            receive_date = secl_entry["receive_date"],
            parameters = secl_entry["parameters"],
            ID = CoalTestingTrain.objects.count() + 1
        ).save()
    
    return {"message" : "Successful"}



@router.get("/coal_test_table", tags=["Coal Testing"])
def coal_wcl_test_table(response:Response,currentPage: Optional[int] = None, perPage: Optional[int] = None,
                    search_text: Optional[str] = None,
                    start_timestamp: Optional[str] = None,
                    end_timestamp: Optional[str] = None,
                    type: Optional[str] = "display"):
    try:
        data={}
        result = {        
                "labels": [],
                "datasets": [],
                "total" : 0,
                "page_size": 15
        }

        UTC_OFFSET_TIMEDELTA = datetime.datetime.utcnow() - datetime.datetime.now()
        
        if type and type == "display":

            page_no = 1
            page_len = result["page_size"]

            if currentPage:
                page_no = currentPage

            if perPage:
                page_len = perPage
                result["page_size"] = perPage

            if not start_timestamp:
                from_date = (
                    (datetime.datetime.utcnow()-datetime.timedelta(days=30)).replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    + UTC_OFFSET_TIMEDELTA
                )
            
            else:
                from_date = (
                    datetime.datetime.strptime(
                        start_timestamp, "%Y-%m-%dT%H:%M"
                    )
                    + UTC_OFFSET_TIMEDELTA
                )
                del start_timestamp

            if not end_timestamp:
                to_date = datetime.datetime.utcnow()

            else:
                to_date = (
                    datetime.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M")
                    + UTC_OFFSET_TIMEDELTA
                )
                del end_timestamp

            if from_date:
                data["created_at__gte"] = from_date
            if to_date:
                data["created_at__lte"] = to_date
            
            if search_text:
                if search_text.isdigit():
                    data["rrNo__icontains"] = search_text
                else:
                    data["location__icontains"] = search_text

            offset = (page_no - 1) * page_len
            
            logs = (
                CoalTesting.objects(**data)
                .order_by("-ID")
                .skip(offset)
                .limit(page_len)                  
            )        

            if any(logs):
                for log in logs:
                    result["labels"] = list(log.payload().keys())
                    result["datasets"].append(log.payload())

            result["total"] = (len(CoalTesting.objects(**data)))
            console_logger.debug(f"-------- Coal Testing Response -------- {result}")
            return result

        elif type and type == "download":
            del type

            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.makedirs(target_directory, exist_ok=True, mode=0o777)

            if not start_timestamp:
                from_date = (
                    datetime.datetime.utcnow().replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    + UTC_OFFSET_TIMEDELTA
                )
            
            else:
                from_date = (
                    datetime.datetime.strptime(
                    start_timestamp, "%Y-%m-%dT%H:%M"
                    )
                    + UTC_OFFSET_TIMEDELTA
                )
                del start_timestamp

            if not end_timestamp:
                to_date = datetime.datetime.utcnow()
            else:
                to_date = (
                    datetime.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M")
                    + UTC_OFFSET_TIMEDELTA
                )
                del end_timestamp

            console_logger.info(from_date)
            console_logger.info(to_date)
            
            if from_date:
                data["created_at__gte"] = from_date
            if to_date:
                data["created_at__lte"] = to_date

            console_logger.debug(data)

            if search_text:
                if search_text.isdigit():
                    data["rrNo__icontains"] = search_text
                    del search_text
                else:
                    data["location__icontains"] = search_text
                    del search_text

            usecase_data = CoalTesting.objects(**data).order_by("-created_at")
            count = len(usecase_data)
            path = None
            if usecase_data:
                try:
                    path = os.path.join(
                        "static_server",
                        "gmr_ai",
                        file,
                        "WCL_Report_{}.xlsx".format(
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
                        "RR_No",
                        "RR_Qty",
                        "Rake_No",
                        "Supplier",
                        "Total Moisture %",
                        "Inherent Moisture (ADB) %",
                        "ASH (ADB) %",
                        "Volatile Matter (ADB) %",
                        "Gross calorific value (ADB) Kcal/kg",
                        "ASH (ARB) %",
                        "Volatile Matter (ARB) %",
                        "Fixed Carbon (ARB) %",
                        "Gross Calorific Value (ARB) Kcal/Kg",
                        "Date",
                        "Time",
                    ]

                    for index, header in enumerate(headers):
                        worksheet.write(0, index, header, cell_format2)

                    for row, query in enumerate(usecase_data,start=1):
                        result = query.payload()
                        worksheet.write(row, 0, count, cell_format)
                        worksheet.write(row, 1, str(result["Mine"]), cell_format)
                        worksheet.write(row, 2, str(result["RR_No"]), cell_format)
                        worksheet.write(row, 3, str(result["RR_Qty"]), cell_format)
                        worksheet.write(row, 4, str(result["Rake_No"]), cell_format)
                        worksheet.write(row, 5, str(result["Supplier"]), cell_format)
                        worksheet.write(row, 6, str(result["Total Moisture %"]), cell_format)
                        worksheet.write(row, 7, str(result["Inherent Moisture (ADB) %"]), cell_format)
                        worksheet.write(row, 8, str(result["ASH (ADB) %"]), cell_format)
                        worksheet.write(row, 9, str(result["Volatile Matter (ADB) %"]), cell_format)
                        worksheet.write(row, 10, str(result["Gross calorific value (ADB) Kcal/kg"]), cell_format)
                        worksheet.write(row, 11, str(result["ASH (ARB) %"]), cell_format)
                        worksheet.write(row, 12, str(result["Volatile Matter (ARB) %"]), cell_format)
                        worksheet.write(row, 13, str(result["Fixed Carbon (ARB) %"]), cell_format)
                        worksheet.write(row, 14, str(result["Gross Calorific Value (ARB) Kcal/Kg"]), cell_format)
                        worksheet.write(row, 15, str(result["Date"]), cell_format)
                        worksheet.write(row, 16, str(result["Time"]), cell_format)
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
        return e



@router.get("/coal_train_test_table", tags=["Coal Testing"])
def coal_secl_test_table(response:Response,currentPage: Optional[int] = None, perPage: Optional[int] = None,
                    search_text: Optional[str] = None,
                    start_timestamp: Optional[str] = None,
                    end_timestamp: Optional[str] = None,
                    type: Optional[str] = "display"):
    try:
        data={}
        result = {        
                "labels": [],
                "datasets": [],
                "total" : 0,
                "page_size": 15
        }

        UTC_OFFSET_TIMEDELTA = datetime.datetime.utcnow() - datetime.datetime.now()
        
        if type and type == "display":

            page_no = 1
            page_len = result["page_size"]

            if currentPage:
                page_no = currentPage

            if perPage:
                page_len = perPage
                result["page_size"] = perPage

            if not start_timestamp:
                from_date = (
                    (datetime.datetime.utcnow()-datetime.timedelta(days=30)).replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    + UTC_OFFSET_TIMEDELTA
                )
            
            else:
                from_date = (
                    datetime.datetime.strptime(
                        start_timestamp, "%Y-%m-%dT%H:%M"
                    )
                    + UTC_OFFSET_TIMEDELTA
                )
                del start_timestamp

            if not end_timestamp:
                to_date = datetime.datetime.utcnow()

            else:
                to_date = (
                    datetime.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M")
                    + UTC_OFFSET_TIMEDELTA
                )
                del end_timestamp

            if from_date:
                data["created_at__gte"] = from_date
            if to_date:
                data["created_at__lte"] = to_date
            
            if search_text:
                if search_text.isdigit():
                    data["rrNo__icontains"] = search_text
                else:
                    data["location__icontains"] = search_text

            offset = (page_no - 1) * page_len
            
            logs = (
                CoalTestingTrain.objects(**data)
                .order_by("-ID")
                .skip(offset)
                .limit(page_len)                  
            )        

            if any(logs):
                for log in logs:
                    result["labels"] = list(log.payload().keys())
                    result["datasets"].append(log.payload())

            result["total"] = (len(CoalTestingTrain.objects(**data)))
            console_logger.debug(f"-------- Coal Testing Response -------- {result}")
            return result

        elif type and type == "download":
            del type

            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.makedirs(target_directory, exist_ok=True, mode=0o777)

            if not start_timestamp:
                from_date = (
                    datetime.datetime.utcnow().replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    + UTC_OFFSET_TIMEDELTA
                )
            
            else:
                from_date = (
                    datetime.datetime.strptime(
                    start_timestamp, "%Y-%m-%dT%H:%M"
                    )
                    + UTC_OFFSET_TIMEDELTA
                )
                del start_timestamp

            if not end_timestamp:
                to_date = datetime.datetime.utcnow()
            else:
                to_date = (
                    datetime.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M")
                    + UTC_OFFSET_TIMEDELTA
                )
                del end_timestamp

            console_logger.info(from_date)
            console_logger.info(to_date)
            
            if from_date:
                data["created_at__gte"] = from_date
            if to_date:
                data["created_at__lte"] = to_date

            console_logger.debug(data)

            if search_text:
                if search_text.isdigit():
                    data["rrNo__icontains"] = search_text
                    del search_text
                else:
                    data["location__icontains"] = search_text
                    del search_text

            usecase_data = CoalTestingTrain.objects(**data).order_by("-created_at")
            count = len(usecase_data)
            path = None
            if usecase_data:
                try:
                    path = os.path.join(
                        "static_server",
                        "gmr_ai",
                        file,
                        "SECL_Report_{}.xlsx".format(
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
                        "RR_No",
                        "RR_Qty",
                        "Rake_No",
                        "Supplier",
                        "Total Moisture %",
                        "Inherent Moisture (ADB) %",
                        "ASH (ADB) %",
                        "Volatile Matter (ADB) %",
                        "Gross calorific value (ADB) Kcal/kg",
                        "ASH (ARB) %",
                        "Volatile Matter (ARB) %",
                        "Fixed Carbon (ARB) %",
                        "Gross Calorific Value (ARB) Kcal/Kg",
                        "Date",
                        "Time",
                    ]

                    for index, header in enumerate(headers):
                        worksheet.write(0, index, header, cell_format2)

                    for row, query in enumerate(usecase_data,start=1):
                        result = query.payload()
                        worksheet.write(row, 0, count, cell_format)
                        worksheet.write(row, 1, str(result["Mine"]), cell_format)
                        worksheet.write(row, 2, str(result["RR_No"]), cell_format)
                        worksheet.write(row, 3, str(result["RR_Qty"]), cell_format)
                        worksheet.write(row, 4, str(result["Rake_No"]), cell_format)
                        worksheet.write(row, 5, str(result["Supplier"]), cell_format)
                        worksheet.write(row, 6, str(result["Total Moisture %"]), cell_format)
                        worksheet.write(row, 7, str(result["Inherent Moisture (ADB) %"]), cell_format)
                        worksheet.write(row, 8, str(result["ASH (ADB) %"]), cell_format)
                        worksheet.write(row, 9, str(result["Volatile Matter (ADB) %"]), cell_format)
                        worksheet.write(row, 10, str(result["Gross calorific value (ADB) Kcal/kg"]), cell_format)
                        worksheet.write(row, 11, str(result["ASH (ARB) %"]), cell_format)
                        worksheet.write(row, 12, str(result["Volatile Matter (ARB) %"]), cell_format)
                        worksheet.write(row, 13, str(result["Fixed Carbon (ARB) %"]), cell_format)
                        worksheet.write(row, 14, str(result["Gross Calorific Value (ARB) Kcal/Kg"]), cell_format)
                        worksheet.write(row, 15, str(result["Date"]), cell_format)
                        worksheet.write(row, 16, str(result["Time"]), cell_format)
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
        return e


#  x------------------------------   Road Trip Coal API    ------------------------------------x


@router.get("/road_journey_table", tags=["Road Map Table"])
def gmr_table(response:Response,currentPage: Optional[int] = None, perPage: Optional[int] = None,
                    search_text: Optional[str] = None,
                    start_timestamp: Optional[str] = None,
                    end_timestamp: Optional[str] = None,
                    type: Optional[str] = "display"):
    try:
        data={}
        result = {        
                "labels": [],
                "datasets": [],
                "total" : 0,
                "page_size": 15
        }

        UTC_OFFSET_TIMEDELTA = datetime.datetime.utcnow() - datetime.datetime.now()
        
        if type and type == "display":

            page_no = 1
            page_len = result["page_size"]

            if currentPage:
                page_no = currentPage

            if perPage:
                page_len = perPage
                result["page_size"] = perPage

            if not start_timestamp:
                from_date = (
                    (datetime.datetime.utcnow()-datetime.timedelta(days=30)).replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    + UTC_OFFSET_TIMEDELTA
                )
                console_logger.debug(from_date)
            
            else:
                from_date = (
                    datetime.datetime.strptime(
                        start_timestamp, "%Y-%m-%dT%H:%M"
                    )
                    + UTC_OFFSET_TIMEDELTA
                )
                del start_timestamp

            if not end_timestamp:
                to_date = datetime.datetime.utcnow()

            else:
                to_date = (
                    datetime.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M")
                    + UTC_OFFSET_TIMEDELTA
                )
                del end_timestamp

            if from_date:
                data["created_at__gte"] = from_date
            if to_date:
                data["created_at__lte"] = to_date
            
            if search_text:
                if search_text.isdigit():
                    data["arv_cum_do_number__icontains"] = search_text
                else:
                    data["vehicle_number__icontains"] = search_text

            offset = (page_no - 1) * page_len
            
            logs = (
                Gmrdata.objects(**data)
                .order_by("-ID")
                .skip(offset)
                .limit(page_len)
            )        
            if any(logs):
                for log in logs:
                    result["labels"] = list(log.payload().keys())
                    result["datasets"].append(log.payload())

            result["total"]= len(Gmrdata.objects(**data))
            console_logger.debug(f"-------- Road Journey Response -------- {result}")
            return result

        elif type and type == "download":
            del type

            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.makedirs(target_directory, exist_ok=True, mode=0o777)

            if not start_timestamp:
                from_date = (
                    datetime.datetime.utcnow().replace(
                        hour=0, minute=0, second=0, microsecond=0
                    )
                    + UTC_OFFSET_TIMEDELTA
                )
            
            else:
                from_date = (
                    datetime.datetime.strptime(
                        start_timestamp, "%Y-%m-%dT%H:%M"
                    )
                    + UTC_OFFSET_TIMEDELTA
                )
                del start_timestamp

            if not end_timestamp:
                to_date = datetime.datetime.utcnow()
            else:
                to_date = (
                    datetime.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M")
                    + UTC_OFFSET_TIMEDELTA
                )
                del end_timestamp

            console_logger.info(from_date)
            console_logger.info(to_date)

            if from_date:
                data["created_at__gte"] = from_date
            if to_date:
                data["created_at__lte"] = to_date

            console_logger.debug(data)

            if search_text:
                if search_text.isdigit():
                    data["arv_cum_do_number__icontains"] = search_text
                    del search_text
                else:
                    data["vehicle_number__icontains"] = search_text
                    del search_text

            usecase_data = Gmrdata.objects(**data).order_by("-created_at")
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
                        "Arv Cum DO No.",
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
                        "Out date",
                        "Out time",
                        "Total Net Amount",
                        "Driver Name",
                        "Gate Pass No",
                        "Gate Verified Time",
                        "Vehicle In Time",
                        "Actual Gross Wt Time",
                        "Actual Tare Wt Time"
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
                        worksheet.write(row, 5, str(result["Arv_Cum_DO_No"]), cell_format)
                        worksheet.write(row, 6, str(result["Mines_Name"]), cell_format)
                        worksheet.write(row, 7, str(result["Grade"]), cell_format)
                        worksheet.write(row, 8, str(result["Type_of_consumer"]), cell_format)
                        worksheet.write(row, 9, str(result["DC_Date"]), cell_format)
                        worksheet.write(row, 10, str(result["vehicle_number"]), cell_format)
                        worksheet.write(row, 11, str(result["Vehicle_Chassis_No"]), cell_format)
                        worksheet.write(row, 12, str(result["Fitness_Expiry"]), cell_format)
                        worksheet.write(row, 13, str(result["Weightment_Date"]), cell_format)
                        worksheet.write(row, 14, str(result["Weightment_Time"]), cell_format)
                        worksheet.write(row, 15, str(result["Gross_challan_Wt(MT)"]), cell_format)
                        worksheet.write(row, 16, str(result["Tare_challan_Wt(MT)"]), cell_format)
                        worksheet.write(row, 17, str(result["Net_challan_Wt(MT)"]), cell_format)
                        worksheet.write(row, 18, str(result["Gross_actual_Wt(MT)"]), cell_format)
                        worksheet.write(row, 19, str(result["Tare_actual_Wt(MT)"]), cell_format)
                        worksheet.write(row, 20, str(result["Net_actual_Wt(MT)"]), cell_format)
                        worksheet.write(row, 21, str(result["Wastage"]), cell_format)
                        worksheet.write(row, 22, str(result["Transporter_LR_No"]), cell_format)
                        worksheet.write(row, 23, str(result["Transporter_LR_Date"]), cell_format)
                        worksheet.write(row, 24, str(result["Eway_bill_No"]), cell_format)
                        worksheet.write(row, 25, str(result["Out_date"]), cell_format)
                        worksheet.write(row, 26, str(result["Out_time"]), cell_format)
                        worksheet.write(row, 27, str(result["Total_net_amount"]), cell_format)
                        worksheet.write(row, 28, str(result["Driver_Name"]), cell_format)
                        worksheet.write(row, 29, str(result["Gate_Pass_No"]), cell_format)
                        worksheet.write(row, 30, str(result["Gate_verified_time"]), cell_format)
                        worksheet.write(row, 31, str(result["Vehicle_in_time"]), cell_format)
                        worksheet.write(row, 32, str(result["Actual_gross_wt_time"]), cell_format)
                        worksheet.write(row, 33, str(result["Actual_tare_wt_time"]), cell_format)
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



if __name__ == "__main__":
    usecase_handler_object.handler.run(ip=server_ip, port=server_port)
    usecase_handler_object.handler.send_status(True)
    pre_processing()
    import uvicorn
    uvicorn.run("main:router",reload=False, host="0.0.0.0",port=7704)
    # sched.add_job(scheduled_job, "interval", seconds=10)
    # sched.start()


