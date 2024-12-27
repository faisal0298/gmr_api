import requests
import json
import os, sys
from database.models import *
from helpers.logger import console_logger
from helpers.usecase_handler import load_params, pre_processing
from fastapi import FastAPI, BackgroundTasks
from lxml import etree
import xml.etree.ElementTree as ET
import datetime
import requests
from datetime import timedelta, date
import xlsxwriter
import copy
from dateutil.relativedelta import relativedelta
from helpers.read_timezone import read_timezone_from_file
from typing import Optional
from mongoengine.queryset.visitor import Q
from collections import defaultdict
import pandas as pd
import pytz
from service import host, db_port, username, password, ip
from mongoengine import MultipleObjectsReturned
from io import BytesIO
from pymongo import MongoClient
from dateutil import tz
from helpers.bunker_report_handler import bunker_single_generate_report
from bson.objectid import ObjectId

import smbclient
import re

from dotenv import load_dotenv, dotenv_values
from bson.objectid import ObjectId
load_dotenv() 


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


IST = pytz.timezone('Asia/Kolkata')

class DataExecutions:
    def __init__(self) -> None:
        pass
    
    def convert_to_utc_format(self, date_time, format, timezone= "Asia/Kolkata", start = True):
        to_zone = tz.gettz(timezone)
        _datetime = datetime.datetime.strptime(date_time, format)

        if not start:
            _datetime =_datetime.replace(hour=23,minute=59)
        return _datetime.replace(tzinfo=to_zone).astimezone(datetime.timezone.utc).replace(tzinfo=None)

    # def fetchcoalBunkerData(self, start_date, end_date):
    #     try:
    #         global consumption_headers, proxies
    #         # entry = UsecaseParameters.objects.filter(Parameters__gmr_api__exists=True).first()
    #         entry = UsecaseParameters.objects.first()
    #         testing_ip = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Testing IP') if entry else None
    #         testing_timer = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Testing Duration') if entry else None

    #         console_logger.debug(f"---- Coal Consumption IP ----        {testing_ip}")
    #         console_logger.debug(f"---- Coal Consumption Duration ----  {testing_timer}")

    #         current_time = datetime.datetime.now(IST)
    #         current_date = current_time.date()

    #         if not end_date:
    #             end_date = current_date.__str__()                                                    # end_date will always be the current date
    #         if not start_date:
    #             no_of_day = testing_ip.split(":")[0]
    #             start_date = (current_date-timedelta(int(no_of_day))).__str__()

    #         console_logger.debug(f"{start_date}")
    #         console_logger.debug(f"{end_date}")

    #         startd_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').strftime('%m/%d/%y')
    #         endd_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').strftime('%m/%d/%y')
    #         console_logger.debug(ip)
    #         url = f"http://{ip}/api/v1/host/bunker_data?start_date={startd_date}&end_date={endd_date}"
    #         # url = f"http://{testing_ip}/limsapi/api/SampleDetails/GetSampleRecord/GetSampleRecord?Fromdate={startd_date}&todate={endd_date}"
     
    #         try:
    #             payload = {}
    #             headers = {}

    #             response = requests.request("GET", url, headers=headers, data=payload)
    #             console_logger.debug(response.status_code)
    #             data = response.json()
    #             if response.status_code == 200:
    #                 sample_list_data = []
    #                 for single_data in data["responseData"]:
    #                     try:
    #                         fetchBunkerData = BunkerData.objects.get(sample_details_id=single_data.get("sample_Details_Id"))
    #                     except DoesNotExist as e:
    #                         if single_data.get("sample_Desc") == "Bunker U#01" or single_data.get("sample_Desc") == "Bunker U#02":
    #                             # console_logger.debug(single_data["sample_Desc"])
    #                             sample_list_data = []
    #                             for final_single_data in single_data.get("sample_Parameters", []):
    #                                 bunker_sample_para = {
    #                                     "sample_details_id": final_single_data.get("sample_Details_Id"),
    #                                     "parameters_id": final_single_data.get("parameters_Id"),
    #                                     "parameter_name": final_single_data.get("parameter_Name"),
    #                                     "unit_val": final_single_data.get("unit_Val"),
    #                                     "test_method": final_single_data.get("test_Method"),
    #                                     "val1": final_single_data.get("val1"),
    #                                     "parameter_type": final_single_data.get("parameter_Type"),
    #                                 }
    #                                 sample_list_data.append(bunker_sample_para)

    #                             bunkerData = BunkerData(
    #                                 sample_details_id=single_data.get("sample_Details_Id"),
    #                                 work_order_id=single_data.get("work_Order_Id"),
    #                                 test_report_no=single_data.get("test_Report_No"),
    #                                 ulr_no = single_data.get("ulrNo"),
    #                                 test_report_date=single_data.get("test_Report_Date"),
    #                                 sample_id_no=single_data.get("sample_Id_No"),
    #                                 sample_desc=single_data.get("sample_Desc"),
    #                                 # rake_no=single_data.get("rake_No"),
    #                                 # rrNo=single_data.get("rrNo"),
    #                                 rR_Qty=single_data.get("rR_Qty"),
    #                                 supplier=single_data.get("supplier"),
    #                                 received_condition=single_data.get("received_Condition"),
    #                                 from_sample_condition_date=single_data.get("from_Sample_Collection_Date"),
    #                                 to_sample_condition_date=single_data.get("to_Sample_Collection_Date"),
    #                                 sample_received_date=single_data.get("sample_Received_Date"),
    #                                 sample_date=single_data.get("sample_Date"),
    #                                 analysis_date=single_data.get("analysis_Date"),
    #                                 sample_qty=single_data.get("sample_Qty"),
    #                                 # method_reference=single_data.get("method_Reference"),
    #                                 humidity=single_data.get("humidity"),
    #                                 test_temp=single_data.get("test_Temp"),
    #                                 sample_parameters=sample_list_data,
    #                             )
    #                             bunkerData.save()
    #             return "success"
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


    def get_val_from_sample_params(self, params, param_type):
        try:
            for param in params:
                if param['parameter_Type'] == param_type:
                    return float(param['val1'])
            return None
        except Exception as e:
            success = False
            console_logger.debug("----- Coal Testing Error -----",e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
            console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
            success = e

    def convert_to_float(self, value):
        try:
            # Sanitize the string by replacing invalid patterns like multiple dots
            sanitized_value = value.replace('..', '.')
            
            # Attempt to convert to float
            return float(sanitized_value)
        except (ValueError, AttributeError):
            # Handle invalid conversion (return None or log the error)
            return None

    def fetchcoalBunkerData(self, start_date, end_date):
        try:
            global consumption_headers, proxies
            # entry = UsecaseParameters.objects.filter(Parameters__gmr_api__exists=True).first()
            entry = UsecaseParameters.objects.first()
            testing_ip = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Testing IP') if entry else None
            testing_timer = entry.Parameters.get('gmr_api', {}).get('roi1', {}).get('Coal Testing Duration') if entry else None

            console_logger.debug(f"---- Coal Consumption IP ----        {testing_ip}")
            console_logger.debug(f"---- Coal Consumption Duration ----  {testing_timer}")

            current_time = datetime.datetime.now(IST)
            current_date = current_time.date()

            if not end_date:
                end_date = current_date.__str__()                                                    # end_date will always be the current date
            if not start_date:
                no_of_day = testing_ip.split(":")[0]
                start_date = (current_date-timedelta(int(no_of_day))).__str__()

            console_logger.debug(f"{start_date}")
            console_logger.debug(f"{end_date}")

            startd_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').strftime('%m/%d/%y')
            endd_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').strftime('%m/%d/%y')
            console_logger.debug(ip)
            url = f"http://{ip}/api/v1/host/bunker_data?start_date={startd_date}&end_date={endd_date}"
            # url = f"http://{testing_ip}/limsapi/api/SampleDetails/GetSampleRecord/GetSampleRecord?Fromdate={startd_date}&todate={endd_date}"
     
            try:
                payload = {}
                headers = {}

                response = requests.request("GET", url, headers=headers, data=payload)
                # console_logger.debug(response.status_code)
                data = response.json()
                # console_logger.debug(len(data["responseData"]))
                # console_logger.debug(data["responseData"])
                if response.status_code == 200:
                    sample_list_data = []
                    for single_data in data["responseData"]:
                        try:
                            # if single_data.get("sample_Desc") in ["Bunker U#01", "Bunker U#02", "BUNKER U#01", "BUNKER U#02"]:
                            if "bunker" in single_data.get("sample_Desc").strip().lower():
                                sample_date = single_data.get("sample_Date", "").strip()
                                
                                # Check if the sample_Date is not empty and has valid length (> 0)
                                if len(sample_date) > 0:
                                    # Further processing of sample_Date
                                    if len(sample_date) == 19:  # If date contains time
                                        new_date = sample_date.split(' ')[0]
                                    else:
                                        if '.' in sample_date:  # If the date uses '.' as a delimiter
                                            new_date = datetime.datetime.strptime(sample_date, "%d.%m.%Y").strftime("%d/%m/%Y")
                                        elif '/' in sample_date:  # If the date uses '/' as a delimiter
                                            new_date = datetime.datetime.strptime(sample_date, "%d/%m/%Y").strftime("%d/%m/%Y")
                                        else:
                                            new_date = sample_date
                                    
                                    # Fetch data or insert new record into the database
                                    fetchBunkerData = BunkerQualityAnalysis.objects.get(sample_date=datetime.datetime.strptime(new_date, "%d/%m/%Y"), unit_no=int(single_data["sample_Desc"].split("#")[1]))
                                    fetchBunkerData.ulr = single_data.get("ulrNo") if single_data.get("ulrNo") else fetchBunkerData.ulr
                                    fetchBunkerData.certificate_no = single_data.get("sample_Id_No") if single_data.get("sample_Id_No") else fetchBunkerData.certificate_no
                                    fetchBunkerData.test_report_date = single_data.get("test_Report_Date") if single_data.get("test_Report_Date") else fetchBunkerData.test_report_date
                                    fetchBunkerData.analysis_date = single_data.get("analysis_Date") if single_data.get("analysis_Date") else fetchBunkerData.analysis_date
                                    fetchBunkerData.bunkered_qty = float(single_data.get("sample_Qty")) if single_data.get("sample_Qty") else fetchBunkerData.bunkered_qty
                                    # fetchBunkerData.sample_name = single_data.get("sample_Desc").split(' ')[0] if single_data.get("sample_Desc") else fetchBunkerData.sample_name
                                    fetchBunkerData.sample_name = "Bunker"
                                    fetchBunkerData.lab_temp = self.convert_to_float(single_data.get("test_Temp")) if single_data.get("test_Temp") else fetchBunkerData.lab_temp
                                    fetchBunkerData.lab_rh = float(single_data.get("humidity")) if single_data.get("humidity") else fetchBunkerData.lab_rh
                                    # Continue processing other fields...
                                    fetchBunkerData.save()
                                else:
                                    console_logger.debug("sample_Date is missing or empty, skipping DB operation.")
                        except DoesNotExist as e:
                            # Handle DoesNotExist and proceed similarly
                            # if single_data.get("sample_Desc") in ["Bunker U#01", "Bunker U#02", "BUNKER U#01", "BUNKER U#02"]:
                            if "bunker" in single_data.get("sample_Desc").strip().lower():
                                sample_date = single_data.get("sample_Date", "").strip()
                                
                                if len(sample_date) > 0:  # Check if sample_Date exists and is not empty
                                    if len(sample_date) == 19:
                                        new_date = sample_date.split(' ')[0]
                                    else:
                                        if '.' in sample_date:  # Handle dot delimiter
                                            new_date = datetime.datetime.strptime(sample_date, "%d.%m.%Y").strftime("%d/%m/%Y")
                                        elif '/' in sample_date:  # Handle slash delimiter
                                            new_date = datetime.datetime.strptime(sample_date, "%d/%m/%Y").strftime("%d/%m/%Y")
                                        else:
                                            new_date = sample_date
                                    
                                    insertBunkerQualityAnalysis = BunkerQualityAnalysis(
                                        slno=BunkerQualityAnalysis.objects.count() + 1,
                                        sample_date=datetime.datetime.strptime(new_date, "%d/%m/%Y") if single_data.get("sample_Date") else None,
                                        ulr=single_data.get("ulrNo") if single_data.get("ulrNo") else None,
                                        certificate_no=single_data.get("sample_Id_No") if single_data.get("sample_Id_No") else None,
                                        test_report_date=single_data.get("test_Report_Date") if single_data.get("test_Report_Date") else None,
                                        unit_no=int(single_data.get("sample_Desc").split("#")[1]) if single_data.get("sample_Desc") else None,
                                        analysis_date=single_data.get("analysis_Date") if single_data.get("analysis_Date") else None,
                                        bunkered_qty=single_data.get("sample_Qty") if single_data.get("sample_Qty") else None,
                                        # sample_name=single_data.get("sample_Desc").split(' ')[0] if single_data.get("sample_Desc") else None,
                                        sample_name="Bunker",
                                        lab_temp=self.convert_to_float(single_data.get("test_Temp")) if single_data.get("test_Temp") else None,
                                        lab_rh=single_data.get("humidity") if single_data.get("humidity") else None,
                                        adb_im=self.get_val_from_sample_params(single_data['sample_Parameters'], "AirDryBasis_IM"),
                                        adb_ash=self.get_val_from_sample_params(single_data['sample_Parameters'], "AirDryBasis_Ash"),
                                        adb_vm=self.get_val_from_sample_params(single_data['sample_Parameters'], "AirDryBasis_VM"),
                                        adb_gcv=self.get_val_from_sample_params(single_data['sample_Parameters'], "AirDryBasis_GCV"),
                                        arb_tm=self.get_val_from_sample_params(single_data['sample_Parameters'], "ReceivedBasis_TM"),
                                        arb_vm=self.get_val_from_sample_params(single_data['sample_Parameters'], "ReceivedBasis_VM"),
                                        arb_ash=self.get_val_from_sample_params(single_data['sample_Parameters'], "ReceivedBasis_ASH"),
                                        arb_fc=self.get_val_from_sample_params(single_data['sample_Parameters'], "ReceivedBasis_FC"),
                                        arb_gcv=self.get_val_from_sample_params(single_data['sample_Parameters'], "ReceivedBasis_GCV"),
                                    )
                                    insertBunkerQualityAnalysis.save()
                                else:
                                    console_logger.debug("sample_Date is missing or empty, skipping DB operation.")

                return "success"
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


    # def fetchcoalBunkerDbData(self, currentPage, perPage, search_text, start_timestamp, end_timestamp, month_date, type):
    #     try:
    #         result = {        
    #             "labels": [],
    #             "datasets": [],
    #             "total" : 0,
    #             "page_size": 15
    #         }
    #         if type and type == "display":
    #             data = Q()
    #             page_no = 1
    #             page_len = result["page_size"]

    #             if currentPage:
    #                 page_no = currentPage

    #             if perPage:
    #                 page_len = perPage
    #                 result["page_size"] = perPage

    #             offset = (page_no - 1) * page_len

    #             # based on condition for timestamp playing with & and | 
    #             if start_timestamp:
    #                 start_date = self.convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
    #                 data &= Q(created_at__gte = start_date)

    #             if end_timestamp:
    #                 end_date = self.convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M","Asia/Kolkata",False)
    #                 data &= Q(created_at__lte = end_date)

    #             if month_date:
    #                 start_date = f'{month_date}-01'
    #                 startd_date=datetime.datetime.strptime(start_date,"%Y-%m-%d")
    #                 end_date = startd_date + relativedelta(day=31)
    #                 data &= Q(created_at__gte = startd_date)
    #                 data &= Q(created_at__lte = end_date)

    #             # if search_text:
    #             #     if search_text.isdigit():
    #             #         # data &= Q(rrNo__icontains=search_text) | Q(rake_no__icontains=search_text) | Q(work_order_id__icontains=search_text) | Q(sample_details_id__icontains=search_text)
    #             #         data &= Q(sample_details_id__icontains=search_text) | Q(rrNo__icontains=search_text)
    #             #     else:
    #             #         data &= Q(sample_desc__icontains=search_text)

    #             if search_text:
    #                 if search_text.isdigit():
    #                     data &= Q(sample_details_id__icontains=search_text) | Q(rrNo__icontains=search_text)
    #                 else:
    #                     data &= Q(sample_desc__icontains=search_text) | Q(ulr_no__icontains=search_text)

    #             # console_logger.debug(data)

    #             # listData = []
    #             # logs = (
    #             #     BunkerData.objects(data)
    #             #     .order_by("-created_at")
    #             #     .skip(offset)
    #             #     .limit(page_len)
    #             # )
    #             logs = (
    #                 BunkerQualityAnalysis.objects(data)
    #                 .order_by("-created_at")
    #                 .skip(offset)
    #                 .limit(page_len)
    #             )   
    #             if any(logs):
    #                 for log in logs:
    #                     console_logger.debug(log)
    #                     # result["labels"] = list(log.simplepayload().keys())
    #                     result["labels"] = [
    #                         "sample_details_id",
    #                         "work_order_id",
    #                         "test_report_no",
    #                         "ulr_no",
    #                         "test_report_date",
    #                         "sample_id_no",
    #                         "sample_desc",
    #                         "rR_Qty",
    #                         "supplier",
    #                         "received_condition",
    #                         "from_sample_condition_date",
    #                         "to_sample_condition_date",
    #                         "sample_received_date",
    #                         "sample_date",
    #                         "analysis_date",
    #                         "sample_qty",
    #                         "humidity",
    #                         "test_temp",
    #                         "created_at",
    #                         "Inherent_Moisture_(Adb)",
    #                         "Ash_(Adb)",
    #                         "Volatile_Matter_(Adb)",
    #                         "Gross_Calorific_Value_(Adb)",
    #                         "Total_Moisture_(Arb)",
    #                         "Volatile_Matter_(Arb)",
    #                         "Ash_(Arb)",
    #                         "Fixed_Carbon_(Arb)",
    #                         "Gross_Calorific_Value_(Arb)",
    #                     ]
    #                     updated_payload = log.simplepayload()
    #                     for single_under_log in log["sample_parameters"]:
    #                         # console_logger.debug(single_under_log.parameter_type)
    #                         # console_logger.debug(single_under_log.val1)
    #                         if single_under_log.parameter_type == "AirDryBasis_IM":
    #                             updated_payload.update({"Inherent_Moisture_(Adb)": single_under_log.val1})
    #                         if single_under_log.parameter_type == "AirDryBasis_Ash":
    #                             updated_payload.update({"Ash_(Adb)": single_under_log.val1})
    #                         if single_under_log.parameter_type == "AirDryBasis_VM":
    #                             updated_payload.update({"Volatile_Matter_(Adb)": single_under_log.val1})
    #                         if single_under_log.parameter_type == "AirDryBasis_GCV":
    #                             updated_payload.update({"Gross_Calorific_Value_(Adb)": single_under_log.val1})
    #                         if single_under_log.parameter_type == "ReceivedBasis_TM":
    #                             updated_payload.update({"Total_Moisture_(Arb)": single_under_log.val1})
    #                         if single_under_log.parameter_type == "ReceivedBasis_VM":
    #                             updated_payload.update({"Volatile_Matter_(Arb)": single_under_log.val1})
    #                         if single_under_log.parameter_type == "ReceivedBasis_ASH":
    #                             updated_payload.update({"Ash_(Arb)": single_under_log.val1})
    #                         if single_under_log.parameter_type == "ReceivedBasis_FC":
    #                             updated_payload.update({"Fixed_Carbon_(Arb)": single_under_log.val1})
    #                         if single_under_log.parameter_type == "ReceivedBasis_GCV":
    #                             updated_payload.update({"Gross_Calorific_Value_(Arb)": single_under_log.val1})
    #                     result["datasets"].append(updated_payload)
    #             result["total"]= len(BunkerData.objects(data))
    #             return result
    #         elif type and type == "download":
    #             del type

    #             file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
    #             target_directory = f"static_server/gmr_ai/{file}"
    #             os.umask(0)
    #             os.makedirs(target_directory, exist_ok=True, mode=0o777)

    #             # Constructing the base for query
    #             data = Q()

    #             if start_timestamp:
    #                 start_date = self.convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
    #                 data &= Q(created_at__gte = start_date)

    #             if end_timestamp:
    #                 end_date = self.convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M","Asia/Kolkata",False)
    #                 data &= Q(created_at__lte = end_date)
                
    #             if search_text:
    #                 if search_text.isdigit():
    #                     data &= Q(arv_cum_do_number__icontains = search_text) | Q(delivery_challan_number__icontains = search_text)
    #                 else:
    #                     data &= Q(vehicle_number__icontains = search_text)

    #             usecase_data = BunkerData.objects(data).order_by("-created_at")
    #             count = len(usecase_data)
    #             path = None
    #             if usecase_data:
    #                 try:
    #                     path = os.path.join(
    #                         "static_server",
    #                         "gmr_ai",
    #                         file,
    #                         "Bunker_Report_{}.xlsx".format(
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
    #                     cell_format2.set_align("vjustify")

    #                     worksheet = workbook.add_worksheet()
    #                     worksheet.set_column("A:AZ", 20)
    #                     worksheet.set_default_row(50)
    #                     cell_format = workbook.add_format()
    #                     cell_format.set_font_size(10)
    #                     cell_format.set_align("center")
    #                     cell_format.set_align("vcenter")

    #                     headers = [
    #                         "Sr.No",
    #                         "Sample Details Id",
    #                         "Work Order Id",
    #                         "Test Report No",
    #                         "ULR No",
    #                         "Test Report Date",
    #                         "Sample ID No",
    #                         "Sample Desc",
    #                         "RR Qty",
    #                         "Supplier",
    #                         "Received Condition",
    #                         "From Sample Condition Date",
    #                         "To Sample Condition Date",
    #                         "Sample Received Date",
    #                         "Sample Date",
    #                         "Analysis Date",
    #                         "Sample Qty",
    #                         "Humidity",
    #                         "Test Temp",
    #                         "Inherent Moisture (Adb)",
    #                         "Ash (Adb)",
    #                         "Volatile Matter (Adb)",
    #                         "Gross Calorific Value (Adb)",
    #                         "Total Moisture (Arb)",
    #                         "Volatile Matter (Arb)",
    #                         "Ash (Arb)",
    #                         "Fixed Carbon (Arb)",
    #                         "Gross Calorific Value (Arb)",
    #                         "Created At"
    #                     ]
                    
    #                     for index, header in enumerate(headers):
    #                         worksheet.write(0, index, header, cell_format2)

    #                     for row, query in enumerate(usecase_data, start=1):
    #                         result = query.payload()
    #                         worksheet.write(row, 0, count, cell_format)     
    #                         worksheet.write(row, 1, str(result["sample_details_id"]))                      
    #                         worksheet.write(row, 2, str(result["work_order_id"]))                      
    #                         worksheet.write(row, 3, str(result["test_report_no"]))                      
    #                         worksheet.write(row, 4, str(result["ulr_no"]))                      
    #                         worksheet.write(row, 5, str(result["test_report_date"]))                      
    #                         worksheet.write(row, 6, str(result["sample_id_no"]))                      
    #                         worksheet.write(row, 7, str(result["sample_desc"]))                  
    #                         worksheet.write(row, 8, str(result["rR_Qty"]))                      
    #                         worksheet.write(row, 9, str(result["supplier"]))                      
    #                         worksheet.write(row, 10, str(result["received_condition"]))                      
    #                         worksheet.write(row, 11, str(result["from_sample_condition_date"]))                      
    #                         worksheet.write(row, 12, str(result["to_sample_condition_date"]))                      
    #                         worksheet.write(row, 13, str(result["sample_received_date"]))                      
    #                         worksheet.write(row, 14, str(result["sample_date"]))                      
    #                         worksheet.write(row, 15, str(result["analysis_date"]))                      
    #                         worksheet.write(row, 16, str(result["sample_qty"]))                     
    #                         worksheet.write(row, 17, str(result["humidity"]))                      
    #                         worksheet.write(row, 18, str(result["test_temp"]))
    #                         for single_under_log in result["sample_parameters"]:
    #                             console_logger.debug(single_under_log)
    #                             if single_under_log.get("parameter_type") == "AirDryBasis_IM":
    #                                 worksheet.write(row, 19, single_under_log.get("val1"))
    #                             if single_under_log.get("parameter_type") == "AirDryBasis_Ash":
    #                                 worksheet.write(row, 20, single_under_log.get("val1"))
    #                             if single_under_log.get("parameter_type") == "AirDryBasis_VM":
    #                                 worksheet.write(row, 21, single_under_log.get("val1"))
    #                             if single_under_log.get("parameter_type") == "AirDryBasis_GCV":
    #                                 worksheet.write(row, 22, single_under_log.get("val1"))
    #                             if single_under_log.get("parameter_type") == "ReceivedBasis_TM":
    #                                 worksheet.write(row, 23, single_under_log.get("val1"))
    #                             if single_under_log.get("parameter_type") == "ReceivedBasis_VM":
    #                                 worksheet.write(row, 24, single_under_log.get("val1"))
    #                             if single_under_log.get("parameter_type") == "ReceivedBasis_ASH":
    #                                 worksheet.write(row, 25, single_under_log.get("val1"))
    #                             if single_under_log.get("parameter_type") == "ReceivedBasis_FC":
    #                                 worksheet.write(row, 26, single_under_log.get("val1"))
    #                             if single_under_log.get("parameter_type") == "ReceivedBasis_GCV":
    #                                 worksheet.write(row, 27, single_under_log.get("val1"))  
    #                         worksheet.write(row, 28, str(result["created_at"]))               
                            
    #                         count-=1
                            
    #                     workbook.close()

    #                     return {
    #                             "Type": "gmr_coal_bunker_download_event",
    #                             "Datatype": "Report",
    #                             "File_Path": path,
    #                             }
    #                 except Exception as e:
    #                     console_logger.debug(e)
    #                     exc_type, exc_obj, exc_tb = sys.exc_info()
    #                     fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #                     console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
    #                     console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
    #             else:
    #                 console_logger.error("No data found")
    #                 return {
    #                         "Type": "gmr_coal_bunker_download_event",
    #                         "Datatype": "Report",
    #                         "File_Path": path,
    #                         }
    #     except Exception as e:
    #         success = False
    #         console_logger.debug("----- Coal Bunker Error -----",e)
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
    #         console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
    #         success = e

    def fetchcoalBunkerDbData(self, currentPage, perPage, search_text, start_timestamp, end_timestamp, month_date, type):
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

                # based on condition for timestamp playing with & and | 
                if start_timestamp:
                    start_date = self.convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
                    data &= Q(created_at__gte = start_date)

                if end_timestamp:
                    end_date = self.convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M","Asia/Kolkata",False)
                    data &= Q(created_at__lte = end_date)

                if month_date:
                    start_date = f'{month_date}-01'
                    startd_date=datetime.datetime.strptime(start_date,"%Y-%m-%d")
                    end_date = startd_date + relativedelta(day=31)
                    data &= Q(created_at__gte = startd_date)
                    data &= Q(created_at__lte = end_date)

                # if search_text:
                #     if search_text.isdigit():
                #         # data &= Q(rrNo__icontains=search_text) | Q(rake_no__icontains=search_text) | Q(work_order_id__icontains=search_text) | Q(sample_details_id__icontains=search_text)
                #         data &= Q(sample_details_id__icontains=search_text) | Q(rrNo__icontains=search_text)
                #     else:
                #         data &= Q(sample_desc__icontains=search_text)

                if search_text:
                    if search_text.isdigit():
                        data &= Q(sample_details_id__icontains=search_text) | Q(rrNo__icontains=search_text)
                    else:
                        data &= Q(sample_desc__icontains=search_text) | Q(ulr_no__icontains=search_text)

                # console_logger.debug(data)

                # listData = []
                # logs = (
                #     BunkerData.objects(data)
                #     .order_by("-created_at")
                #     .skip(offset)
                #     .limit(page_len)
                # )
                logs = (
                    BunkerQualityAnalysis.objects(data)
                    .order_by("-created_at")
                    .skip(offset)
                    .limit(page_len)
                )   
                if any(logs):
                    for log in logs:
                        console_logger.debug(log)
                        # result["labels"] = list(log.payload().keys())
                        result["labels"] = [
                            "srno",
                            "ULR",
                            "certificate_no",
                            "unit_no",
                            "sample_date",
                            "analysis_date",
                            "bunkered_qty",
                            "sample_name",
                            "humidity",
                            "test_temp",
                            "Inherent_Moisture_(Adb)",
                            "Ash_(Adb)",
                            "Volatile_Matter_(Adb)",
                            "Gross_Calorific_Value_(Adb)",
                            "Total_Moisture_(Arb)",
                            "Volatile_Matter_(Arb)",
                            "Ash_(Arb)",
                            "Fixed_Carbon_(Arb)",
                            "Gross_Calorific_Value_(Arb)",
                            "created_at",
                        ]
                        updated_payload = log.payload()
                        updated_payload["srno"] = updated_payload.pop("slno")
                        updated_payload["ULR"] = updated_payload.pop("ulr")
                        if "adb_im" in log:
                            updated_payload["Inherent_Moisture_(Adb)"] = updated_payload.pop("adb_im")
                        if "adb_ash" in log:
                            updated_payload["Ash_(Adb)"] = updated_payload.pop("adb_ash")
                        if "adb_vm" in log:
                            updated_payload["Volatile_Matter_(Adb)"] = updated_payload.pop("adb_vm")
                        if "adb_gcv" in log:
                            updated_payload["Gross_Calorific_Value_(Adb)"] = updated_payload.pop("adb_gcv")
                        if "arb_tm" in log:
                            updated_payload["Total_Moisture_(Arb)"] = updated_payload.pop("arb_tm")
                        if "arb_vm" in log:
                            updated_payload["Volatile_Matter_(Arb)"] = updated_payload.pop("arb_vm")
                        if "arb_ash" in log:
                            updated_payload["Ash_(Arb)"] = updated_payload.pop("arb_ash")
                        if "arb_fc" in log:
                            updated_payload["Fixed_Carbon_(Arb)"] = updated_payload.pop("arb_fc")
                        if "arb_gcv" in log:
                            updated_payload["Gross_Calorific_Value_(Arb)"] = updated_payload.pop("arb_gcv")
                        if "lab_rh" in log:
                            updated_payload["humidity"] = updated_payload.pop("lab_rh")
                        if "lab_temp" in log:
                            updated_payload["test_temp"] = updated_payload.pop("lab_temp")
                        result["datasets"].append(updated_payload)
                result["total"]= len(BunkerQualityAnalysis.objects(data))
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
                    start_date = self.convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
                    data &= Q(created_at__gte = start_date)

                if end_timestamp:
                    end_date = self.convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M","Asia/Kolkata",False)
                    data &= Q(created_at__lte = end_date)
                
                if search_text:
                    if search_text.isdigit():
                        data &= Q(arv_cum_do_number__icontains = search_text) | Q(delivery_challan_number__icontains = search_text)
                    else:
                        data &= Q(vehicle_number__icontains = search_text)

                usecase_data = BunkerQualityAnalysis.objects(data).order_by("-created_at")
                count = len(usecase_data)
                path = None
                logo_path = f"{os.path.join(os.getcwd(), 'static_server/receipt/report_logo.png')}"
                if usecase_data:
                    try:
                        path = os.path.join(
                            "static_server",
                            "gmr_ai",
                            file,
                            "Bunker_Report_{}.xlsx".format(
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
                        cell_format2.set_text_wrap(True)
                        cell_format2.set_border(1)

                        header_format = workbook.add_format({'bold': True, 'font_size': 40, 'align': 'center'})
                        date_format = workbook.add_format({'align': 'center', 'font_size': 12, "bold": True})
                        report_name_format = workbook.add_format({'align': 'center', 'font_size': 15, "bold": True})

                        header_format.set_align("vcenter")
                        date_format.set_align("vcenter")
                        report_name_format.set_align("vcenter")
                        header_format.set_border(1)
                        date_format.set_border(1)
                        report_name_format.set_border(1)

                        worksheet = workbook.add_worksheet()
                        worksheet.set_column("A:AZ", 20)
                        worksheet.set_default_row(50)
                        cell_format = workbook.add_format()
                        cell_format.set_font_size(10)
                        cell_format.set_align("center")
                        cell_format.set_align("vcenter")
                        cell_format.set_text_wrap(True)
                        cell_format.set_border(1)


                        worksheet.insert_image('A1', logo_path, {'x_scale': 0.3, 'y_scale': 0.3})
                    
                        # Merge cells for the main header and place it in the center
                        main_header = "GMR Warora Energy Limited"  # Set your main header text here
                        worksheet.merge_range("A1:T1", main_header, header_format)  # Merge cells A1 to H1 for the header
                        
                        # Write the current date on the left side (A2)
                        worksheet.write("A2", f"Date: {datetime.datetime.now().strftime('%d-%m-%Y')}", date_format)
                        worksheet.merge_range("C2:T2", f"Bunker Quality Analysis", report_name_format)

                        headers = [
                            "Sr.No",
                            "ULR",
                            "Certificate No",
                            "Unit No",
                            "Sample Date",
                            "Analysis Date",
                            "Bunkered Qty",
                            "Sample Name",
                            "Humidity",
                            "Test Temp",
                            "Inherent Moisture (Adb)",
                            "Ash (Adb)",
                            "Volatile Matter (Adb)",
                            "Gross Calorific Value (Adb)",
                            "Total Moisture (Arb)",
                            "Volatile Matter (Arb)",
                            "Ash (Arb)",
                            "Fixed Carbon (Arb)",
                            "Gross Calorific Value (Arb)",
                            "Created At",
                        ]
                    
                        for index, header in enumerate(headers):
                            worksheet.write(2, index, header, cell_format2)

                        for row, query in enumerate(usecase_data, start=3):
                            result = query.payload()
                            worksheet.write(row, 0, count, cell_format)     
                            worksheet.write(row, 1, str(result["ulr"]), cell_format)                      
                            worksheet.write(row, 2, str(result["certificate_no"]), cell_format)                      
                            worksheet.write(row, 3, str(result["unit_no"]), cell_format)                      
                            worksheet.write(row, 4, str(result["sample_date"]), cell_format)                      
                            worksheet.write(row, 5, str(result["analysis_date"]), cell_format)                      
                            worksheet.write(row, 6, str(result["bunkered_qty"]), cell_format)                      
                            worksheet.write(row, 7, str(result["sample_name"]), cell_format)                  
                            worksheet.write(row, 8, str(result["lab_rh"]), cell_format)                      
                            worksheet.write(row, 9, str(result["lab_temp"]), cell_format)                      
                            worksheet.write(row, 10, str(result["adb_im"]), cell_format)                      
                            worksheet.write(row, 11, str(result["adb_ash"]), cell_format)                      
                            worksheet.write(row, 12, str(result["adb_vm"]), cell_format)                      
                            worksheet.write(row, 13, str(result["adb_gcv"]), cell_format)                      
                            worksheet.write(row, 14, str(result["arb_tm"]), cell_format)                      
                            worksheet.write(row, 15, str(result["arb_vm"]), cell_format)                      
                            worksheet.write(row, 16, str(result["arb_ash"]), cell_format)                     
                            worksheet.write(row, 17, str(result["arb_fc"]), cell_format)                      
                            worksheet.write(row, 18, str(result["arb_gcv"]), cell_format)
                            worksheet.write(row, 19, str(result["created_at"]), cell_format)               
                            
                            count-=1
                            
                        workbook.close()

                        return {
                                "Type": "gmr_coal_bunker_download_event",
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
                            "Type": "gmr_coal_bunker_download_event",
                            "Datatype": "Report",
                            "File_Path": path,
                            }
        except Exception as e:
            success = False
            console_logger.debug("----- Coal Bunker Error -----",e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
            console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
            success = e

    def rakeScannedOutData(self):
        try:
            current_time = datetime.datetime.now(IST)
            today = current_time.date()
            startdate = f'{today} 00:00:00'
            console_logger.debug(type(startdate))
            # from_ts = datetime.datetime.strptime(startdate,"%Y-%m-%d %H:%M:%S")
            from_ts = self.convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")

            total_count = BunkerData.objects(created_at__gte=from_ts, created_at__ne=None).count()
            # vehicle_count = Gmrdata.objects(GWEL_Tare_Time__gte=from_ts, GWEL_Tare_Time__ne=None).count()

            return {"title": "Today's Rake Count",
                    "icon" : "vehicle",
                    "data": total_count,
                    "last_updated": today}
        except Exception as e:
            success = False
            console_logger.debug("----- Coal Bunker Error -----",e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
            console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
            success = e


    def coal_test_road_excel(self, start_date, end_date, filter_type):
        try:
            # console_logger.debug(filter_type)
            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)
            
            data = Q()
            if filter_type == "gwel":
                if start_date:
                    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%dT%H:%M")
                    data &= Q(plant_analysis_date__gte = start_date)

                if end_date:
                    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M")
                    data &= Q(plant_analysis_date__lte = end_date)
                
                usecase_data = RecieptCoalQualityAnalysis.objects(data, mode="Road").order_by("-plant_analysis_date")
            else:
                if start_date:
                    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%dT%H:%M")
                    data &= Q(thirdparty_created_date__gte = start_date)

                if end_date:
                    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M")
                    data &= Q(thirdparty_created_date__lte = end_date)
                
                # usecase_data = CoalTesting.objects(data).order_by("-third_party_upload_date")
                usecase_data = RecieptCoalQualityAnalysis.objects(data, mode="Road").order_by("-third_party_upload_date")
            count = len(usecase_data)
            path = None
            logo_path = f"{os.path.join(os.getcwd(), 'static_server/receipt/report_logo.png')}"
            if usecase_data:
                try:
                    path = os.path.join(
                        "static_server",
                        "gmr_ai",
                        file,
                        f"Roadwise_Coal_Lab_Test_Report_{filter_type}_{datetime.datetime.now().strftime('%Y-%m-%d:%H:%M:%S')}.xlsx"
                    )
                    filename = os.path.join(os.getcwd(), path)
                    workbook = xlsxwriter.Workbook(filename)
                    workbook.use_zip64()
                    cell_format2 = workbook.add_format()
                    cell_format2.set_bold()
                    cell_format2.set_font_size(10)
                    cell_format2.set_align("center")
                    cell_format2.set_align("vcenter")
                    cell_format2.set_text_wrap(True)
                    cell_format2.set_border(1)


                    header_format = workbook.add_format({'bold': True, 'font_size': 40, 'align': 'center'})
                    date_format = workbook.add_format({'align': 'center', 'font_size': 12, "bold": True})
                    report_name_format = workbook.add_format({'align': 'center', 'font_size': 15, "bold": True})

                    header_format.set_align("vcenter")
                    date_format.set_align("vcenter")
                    report_name_format.set_align("vcenter")

                    worksheet = workbook.add_worksheet()
                    worksheet.set_column("A:AZ", 20)
                    worksheet.set_default_row(50)
                    cell_format = workbook.add_format()
                    cell_format.set_font_size(10)
                    cell_format.set_align("center")
                    cell_format.set_align("vcenter")
                    cell_format.set_text_wrap(True)
                    cell_format.set_border(1)


                    worksheet.insert_image('A1', logo_path, {'x_scale': 0.3, 'y_scale': 0.3})

                    # Merge cells for the main header and place it in the center
                    main_header = "GMR Warora Energy Limited"  # Set your main header text here
                    worksheet.merge_range("A1:Y1", main_header, header_format)  # Merge cells A1 to H1 for the header
                    console_logger.debug(end_date)
                    # Write the current date on the left side (A2)
                    worksheet.write("A2", f"Date: {end_date.strftime('%d-%m-%Y')}", date_format)

                    if filter_type == "gwel":
                        # worksheet.merge_range("C2:Y2", f"Report Name: GWEL Receipt Quality Analysis (Road)", report_name_format)
                        worksheet.merge_range("C2:Y2", f"GWEL Receipt Quality Analysis (Road)", report_name_format)
                        headers = [
                                "Sr.No.",
                                "Plant Certificate ID",
                                "Sample No",
                                "Do No",
                                "Plant Sample Date",
                                "Plant Preparation Date",
                                "Plant Analysis Date",
                                "Sample Qty",
                                "Mine",
                                "Mine Grade",
                                "Mode",
                                "GWEL LAB TEMP",
                                "GWEL LAB RH",
                                "GWEL ARB TM",
                                "GWEL ARB VM",
                                "GWEL ARB ASH",
                                "GWEL ARB FC",
                                "GWEL ARB GCV",
                                "GWEL ADB IM",
                                "GWEL ADB VM",
                                "GWEL ADB ASH",
                                "GWEL ADB FC",
                                "GWEL ADB GCV",
                                "GWEL ULR ID",
                                "GWEL GRADE"
                                ]
                    elif filter_type == "third_party":
                        # worksheet.merge_range("C2:Y2", f"Report Name: ThirdParty Receipt Quality Analysis (Road)", report_name_format)
                        worksheet.merge_range("C2:Y2", f"ThirdParty Receipt Quality Analysis (Road)", report_name_format)
                        headers = [
                                    "Sr.No.",
                                    "Plant Certificate ID",
                                    "Sample No",
                                    "Do No",
                                    "Plant Sample Date",
                                    "Plant Preparation Date",
                                    "Plant Analysis Date",
                                    "Sample Qty",
                                    "Mine",
                                    "Mine Grade",
                                    "Mode",
                                    "THIRDPARTY Report Date",
                                    "THIRDPARTY Reference No",
                                    "THIRDPARTY Sample Date",
                                    "THIRDPARTY ARB TM",
                                    "THIRDPARTY ARB VM",
                                    "THIRDPARTY ARB ASH",
                                    "THIRDPARTY ARB FC",
                                    "THIRDPARTY ARB GCV",
                                    "THIRDPARTY ADB IM",
                                    "THIRDPARTY ADB VM",
                                    "THIRDPARTY ADB ASH",
                                    "THIRDPARTY ADB FC",
                                    "THIRDPARTY ADB GCV",
                                    "THIRDPARTY GRADE"
                                    ]
                    elif filter_type == "all":
                        # worksheet.merge_range("C2:Y2", f"Report Name: Receipt Quality Analysis (Road)", report_name_format)
                        worksheet.merge_range("C2:Y2", f"Receipt Quality Analysis (Road)", report_name_format)
                        headers = [
                                    "Sr.No.",
                                    "Plant Certificate Id",
                                    "Sample No",
                                    "Do No",
                                    "Plant Sample Date",
                                    "Plant Preparation Date",
                                    "Plant Analysis Date",
                                    "Sample Qty",
                                    "Mine",
                                    "Mine Grade",
                                    "Mode",
                                    "GWEL Lab Temp",
                                    "GWEL Lab RH",
                                    "GWEL ARB TM",
                                    "GWEL ARB VM",
                                    "GWEL ARB ASH",
                                    "GWEL ARB FC",
                                    "GWEL ARB GCV",
                                    "GWEL ADB IM",
                                    "GWEL ADB VM",
                                    "GWEL ADB ASH",
                                    "GWEL ADB FC",
                                    "GWEL ADB GCV",
                                    "GWEL ULR ID",
                                    "GWEL GRADE",
                                    "THIRDPARTY REPORT DATE",
                                    "THIRDPARTY REFERENCE NO",
                                    "THIRDPARTY SAMPLE DATE",
                                    "THIRDPARTY ARB TM",
                                    "THIRDPARTY ARB VM",
                                    "THIRDPARTY ARB ASH",
                                    "THIRDPARTY ARB FC",
                                    "THIRDPARTY ARB GCV",
                                    "THIRDPARTY ADB IM",
                                    "THIRDPARTY ADB VM",
                                    "THIRDPARTY ADB Ash",
                                    "THIRDPARTY ADB FC",
                                    "THIRDPARTY ADB GCV",
                                    "THIRRDPARTY GRADE",
                                    ]
                    else:
                        # worksheet.merge_range("C2:Y2", f"Report Name: Receipt Quality Analysis (Road)", report_name_format)
                        worksheet.merge_range("C2:Y2", f"Receipt Quality Analysis (Road)", report_name_format)
                        headers = [
                                "Sr.No.",
                                "Plant Certificate Id",
                                "Sample No",
                                "Do No",
                                "Plant Sample Date",
                                "Plant Preparation Date",
                                "Plant Analysis Date",
                                "Sample Qty",
                                "Mine",
                                "Mine Grade",
                                "Mode"
                            ]

                    for index, header in enumerate(headers):
                        worksheet.write(2, index, header, cell_format2)

                    fetchCoalGrades = CoalGrades.objects()

                    for row, query in enumerate(usecase_data, start=3):
                        result = query.payload()
                        worksheet.write(row, 0, count, cell_format)
                        if filter_type == "gwel":
                            worksheet.write(row, 1, str(result["plant_certificate_id"]), cell_format)
                            worksheet.write(row, 2, str(result["sample_no"]), cell_format)
                            worksheet.write(row, 3, str(result["do_no"]), cell_format)
                            worksheet.write(row, 4, str(result["GWEL_sample_date"]), cell_format)
                            worksheet.write(row, 5, str(result["GWEL_preparation_date"]), cell_format)
                            worksheet.write(row, 6, str(result["GWEL_analysis_date"]), cell_format)
                            worksheet.write(row, 7, str(result["sample_qty"]), cell_format)
                            worksheet.write(row, 8, str(result["mine"]), cell_format)
                            worksheet.write(row, 9, str(result["mine_grade"]), cell_format)
                            worksheet.write(row, 10, str(result["mode"]), cell_format)
                            worksheet.write(row, 11, str(result["GWEL_LAB_TEMP"]), cell_format)
                            worksheet.write(row, 12, str(result["GWEL_LAB_RH"]), cell_format)
                            worksheet.write(row, 13, str(result["GWEL_ARB_TM"]), cell_format)
                            worksheet.write(row, 14, str(result["GWEL_ARB_VM"]), cell_format)
                            worksheet.write(row, 15, str(result["GWEL_ARB_ASH"]), cell_format)
                            worksheet.write(row, 16, str(result["GWEL_ARB_FC"]), cell_format)
                            worksheet.write(row, 17, str(result["GWEL_ARB_GCV"]), cell_format)
                            worksheet.write(row, 18, str(result["GWEL_ADB_IM"]), cell_format)
                            worksheet.write(row, 19, str(result["GWEL_ADB_VM"]), cell_format)
                            worksheet.write(row, 20, str(result["GWEL_ADB_ASH"]), cell_format)
                            worksheet.write(row, 21, str(result["GWEL_ADB_FC"]), cell_format)
                            worksheet.write(row, 22, str(result["GWEL_ADB_GCV"]), cell_format)
                            worksheet.write(row, 23, str(result["GWEL_ULR_ID"]), cell_format)
                            if result.get("GWEL_ADB_GCV"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= int(result.get("GWEL_ADB_GCV"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 24, str(single_coal_grades["grade"]), cell_format)
                                    elif int(result.get("GWEL_ADB_GCV")) > 7001:
                                        worksheet.write(row, 24, "G-1", cell_format)

                        elif filter_type == "third_party":
                            worksheet.write(row, 1, str(result["plant_certificate_id"]), cell_format)
                            worksheet.write(row, 2, str(result["sample_no"]), cell_format)
                            worksheet.write(row, 3, str(result["do_no"]), cell_format)
                            worksheet.write(row, 4, str(result["GWEL_sample_date"]), cell_format)
                            worksheet.write(row, 5, str(result["GWEL_preparation_date"]), cell_format)
                            worksheet.write(row, 6, str(result["GWEL_analysis_date"]), cell_format)
                            worksheet.write(row, 7, str(result["sample_qty"]), cell_format)
                            worksheet.write(row, 8, str(result["mine"]), cell_format)
                            worksheet.write(row, 9, str(result["mine_grade"]), cell_format)
                            worksheet.write(row, 10, str(result["mode"]), cell_format)
                            if result.get("THIRDPARTY_REPORT_DATE"):
                                worksheet.write(row, 11, str(result["THIRDPARTY_REPORT_DATE"]), cell_format)
                            if result.get("THIRDPARTY_REFERENCE_NO"):
                                worksheet.write(row, 12, str(result["THIRDPARTY_REFERENCE_NO"]), cell_format)
                            if result.get("THIRDPARTY_SAMPLE_DATE"):
                                worksheet.write(row, 13, str(result["THIRDPARTY_SAMPLE_DATE"]), cell_format)
                            if result.get("THIRDPARTY_ARB_TM"):
                                worksheet.write(row, 14, str(result["THIRDPARTY_ARB_TM"]), cell_format)
                            if result.get("THIRDPARTY_ARB_VM"):
                                worksheet.write(row, 15, str(result["THIRDPARTY_ARB_VM"]), cell_format)
                            if result.get("THIRDPARTY_ARB_ASH"):
                                worksheet.write(row, 16, str(result["THIRDPARTY_ARB_ASH"]), cell_format)
                            if result.get("THIRDPARTY_ARB_FC"):
                                worksheet.write(row, 17, str(result["THIRDPARTY_ARB_FC"]), cell_format)
                            if result.get("THIRDPARTY_ARB_GCV"):
                                worksheet.write(row, 18, str(result["THIRDPARTY_ARB_GCV"]), cell_format)
                            if result.get("THIRDPARTY_ADB_IM"):
                                worksheet.write(row, 19, str(result["THIRDPARTY_ADB_IM"]), cell_format)
                            if result.get("THIRDPARTY_ADB_VM"):
                                worksheet.write(row, 20, str(result["THIRDPARTY_ADB_VM"]), cell_format)
                            if result.get("THIRDPARTY_ADB_ASH"):
                                worksheet.write(row, 21, str(result["THIRDPARTY_ADB_ASH"]), cell_format)
                            if result.get("THIRDPARTY_ADB_FC"):
                                worksheet.write(row, 22, str(result["THIRDPARTY_ADB_FC"]), cell_format)
                            if result.get("THIRDPARTY_ADB_GCV"):
                                worksheet.write(row, 23, str(result["THIRDPARTY_ADB_GCV"]), cell_format)
                            if result.get("THIRDPARTY_ADB_GCV"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= float(result.get("THIRDPARTY_ADB_GCV"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 24, str(single_coal_grades["grade"]), cell_format)
                                    elif float(result.get("THIRDPARTY_ADB_GCV")) > 7001:
                                        worksheet.write(row, 24, "G-1", cell_format)
                        elif filter_type == "all":
                            worksheet.write(row, 1, str(result["plant_certificate_id"]), cell_format)
                            worksheet.write(row, 2, str(result["sample_no"]), cell_format)
                            worksheet.write(row, 3, str(result["do_no"]), cell_format)
                            worksheet.write(row, 4, str(result["GWEL_sample_date"]), cell_format)
                            worksheet.write(row, 5, str(result["GWEL_preparation_date"]), cell_format)
                            worksheet.write(row, 6, str(result["GWEL_analysis_date"]), cell_format)
                            worksheet.write(row, 7, str(result["sample_qty"]), cell_format)
                            worksheet.write(row, 8, str(result["mine"]), cell_format)
                            worksheet.write(row, 9, str(result["mine_grade"]), cell_format)
                            worksheet.write(row, 10, str(result["mode"]), cell_format)
                            worksheet.write(row, 11, str(result["GWEL_LAB_TEMP"]), cell_format)
                            worksheet.write(row, 12, str(result["GWEL_LAB_RH"]), cell_format)
                            worksheet.write(row, 13, str(result["GWEL_ARB_TM"]), cell_format)
                            worksheet.write(row, 14, str(result["GWEL_ARB_VM"]), cell_format)
                            worksheet.write(row, 15, str(result["GWEL_ARB_ASH"]), cell_format)
                            worksheet.write(row, 16, str(result["GWEL_ARB_FC"]), cell_format)
                            worksheet.write(row, 17, str(result["GWEL_ARB_GCV"]), cell_format)
                            worksheet.write(row, 18, str(result["GWEL_ADB_IM"]), cell_format)
                            worksheet.write(row, 19, str(result["GWEL_ADB_VM"]), cell_format)
                            worksheet.write(row, 20, str(result["GWEL_ADB_ASH"]), cell_format)
                            worksheet.write(row, 21, str(result["GWEL_ADB_FC"]), cell_format)
                            worksheet.write(row, 22, str(result["GWEL_ADB_GCV"]), cell_format)
                            worksheet.write(row, 23, str(result["GWEL_ULR_ID"]), cell_format)
                            if result.get("GWEL_ADB_GCV"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= float(result.get("GWEL_ADB_GCV"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 24, str(single_coal_grades["grade"]), cell_format)
                                    elif float(result.get("GWEL_ADB_GCV")) > 7001:
                                        worksheet.write(row, 24, "G-1", cell_format)
                            if result.get("THIRDPARTY_REPORT_DATE"):
                                worksheet.write(row, 25, str(result["THIRDPARTY_REPORT_DATE"]), cell_format)
                            if result.get("THIRDPARTY_REFERENCE_NO"):
                                worksheet.write(row, 26, str(result["THIRDPARTY_REFERENCE_NO"]), cell_format)
                            if result.get("THIRDPARTY_SAMPLE_DATE"):
                                worksheet.write(row, 27, str(result["THIRDPARTY_SAMPLE_DATE"]), cell_format)
                            if result.get("THIRDPARTY_ARB_TM"):
                                worksheet.write(row, 28, str(result["THIRDPARTY_ARB_TM"]), cell_format)
                            if result.get("THIRDPARTY_ARB_VM"):
                                worksheet.write(row, 29, str(result["THIRDPARTY_ARB_VM"]), cell_format)
                            if result.get("THIRDPARTY_ARB_ASH"):
                                worksheet.write(row, 30, str(result["THIRDPARTY_ARB_ASH"]), cell_format)
                            if result.get("THIRDPARTY_ARB_FC"):
                                worksheet.write(row, 31, str(result["THIRDPARTY_ARB_FC"]), cell_format)
                            if result.get("THIRDPARTY_ARB_GCV"):
                                worksheet.write(row, 32, str(result["THIRDPARTY_ARB_GCV"]), cell_format)
                            if result.get("THIRDPARTY_ADB_IM"):
                                worksheet.write(row, 33, str(result["THIRDPARTY_ADB_IM"]), cell_format)
                            if result.get("THIRDPARTY_ADB_VM"):
                                worksheet.write(row, 34, str(result["THIRDPARTY_ADB_VM"]), cell_format)
                            if result.get("THIRDPARTY_ADB_ASH"):
                                worksheet.write(row, 35, str(result["THIRDPARTY_ADB_ASH"]), cell_format)
                            if result.get("THIRDPARTY_ADB_FC"):
                                worksheet.write(row, 36, str(result["THIRDPARTY_ADB_FC"]), cell_format)
                            if result.get("THIRDPARTY_ADB_GCV"):
                                worksheet.write(row, 37, str(result["THIRDPARTY_ADB_GCV"]), cell_format)
                            if result.get("THIRDPARTY_ADB_GCV"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= float(result.get("THIRDPARTY_ADB_GCV"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 38, str(single_coal_grades["grade"]), cell_format)
                                    elif float(result.get("THIRDPARTY_ADB_GCV")) > 7001:
                                        worksheet.write(row, 38, "G-1", cell_format)
                        else:
                            worksheet.write(row, 1, str(result["plant_certificate_id"]), cell_format)
                            worksheet.write(row, 2, str(result["sample_no"]), cell_format)
                            worksheet.write(row, 3, str(result["do_no"]), cell_format)
                            worksheet.write(row, 4, str(result["GWEL_sample_date"]), cell_format)
                            worksheet.write(row, 5, str(result["GWEL_preparation_date"]), cell_format)
                            worksheet.write(row, 6, str(result["GWEL_analysis_date"]), cell_format)
                            worksheet.write(row, 7, str(result["sample_qty"]), cell_format)
                            worksheet.write(row, 8, str(result["mine"]), cell_format)
                            worksheet.write(row, 9, str(result["mine_grade"]), cell_format)
                            worksheet.write(row, 10, str(result["mode"]), cell_format)
                        count -= 1
                    workbook.close()

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
            # response.status_code = 400
            console_logger.debug(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
            console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
            return e
    

    def coal_rail_excel(self, start_date, end_date, filter_type):
        try:
            # console_logger.debug(filter_type)
            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)

            data = Q()
            # console_logger.debug(start_date)
            # console_logger.debug(end_date)
            # if start_date:
            #     start_date = datetime.datetime.strptime(start_date, "%Y-%m-%dT%H:%M")
            #     data &= Q(created_at__gte = start_date)

            # if end_date:
            #     end_date = datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M")
            #     data &= Q(created_at__lte = end_date)

            usecase_data = RecieptCoalQualityAnalysis.objects(data, mode="Rail").order_by("-created_at")
            
            if filter_type == "gwel":
                if start_date:
                    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%dT%H:%M")
                    data &= Q(plant_analysis_date__gte = start_date)

                if end_date:
                    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M")
                    data &= Q(plant_analysis_date__lte = end_date)
                
                usecase_data = RecieptCoalQualityAnalysis.objects(data, mode="Rail").order_by("-plant_analysis_date")
            else:
                if start_date:
                    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%dT%H:%M")
                    data &= Q(thirdparty_created_date__gte = start_date)

                if end_date:
                    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M")
                    data &= Q(thirdparty_created_date__lte = end_date)
                
                # usecase_data = CoalTesting.objects(data).order_by("-third_party_upload_date")
                usecase_data = RecieptCoalQualityAnalysis.objects(data, mode="Rail").order_by("-third_party_upload_date")
            
            count = len(usecase_data)
            path = None
            logo_path = f"{os.path.join(os.getcwd(), 'static_server/receipt/report_logo.png')}"
            if usecase_data:
                try:
                    path = os.path.join(
                        "static_server",
                        "gmr_ai",
                        file,
                        f"Railwise_Coal_Lab_Test_Report_{filter_type}_{datetime.datetime.now().strftime('%Y-%m-%d:%H:%M:%S')}.xlsx"
                    )
                    filename = os.path.join(os.getcwd(), path)
                    workbook = xlsxwriter.Workbook(filename)
                    workbook.use_zip64()
                    cell_format2 = workbook.add_format()
                    cell_format2.set_bold()
                    cell_format2.set_font_size(10)
                    cell_format2.set_align("center")
                    cell_format2.set_align("vcenter")
                    cell_format2.set_text_wrap(True)
                    cell_format2.set_border(1)
                    

                    header_format = workbook.add_format({'bold': True, 'font_size': 40, 'align': 'center'})
                    date_format = workbook.add_format({'align': 'center', 'font_size': 12, "bold": True})
                    report_name_format = workbook.add_format({'align': 'center', 'font_size': 15, "bold": True})

                    header_format.set_align("vcenter")
                    date_format.set_align("vcenter")
                    report_name_format.set_align("vcenter")

                    worksheet = workbook.add_worksheet()
                    worksheet.set_column("A:AZ", 20)
                    worksheet.set_default_row(50)
                    cell_format = workbook.add_format()
                    cell_format.set_font_size(10)
                    cell_format.set_align("center")
                    cell_format.set_align("vcenter")
                    cell_format.set_text_wrap(True)
                    cell_format.set_border(1)

                    worksheet.insert_image('A1', logo_path, {'x_scale': 0.3, 'y_scale': 0.3})
                    
                    # Merge cells for the main header and place it in the center
                    main_header = "GMR Warora Energy Limited"  # Set your main header text here
                    worksheet.merge_range("A1:Y1", main_header, header_format)  # Merge cells A1 to H1 for the header
                    # Write the current date on the left side (A2)
                    worksheet.write("A2", f"Date: {end_date.strftime('%d-%m-%Y')}", date_format)

                    if filter_type == "gwel":
                        # worksheet.merge_range("C2:H2", f"Report Name: GWEL Receipt Quality Analysis (Rail)", report_name_format)
                        worksheet.merge_range("C2:Y2", f"GWEL Receipt Quality Analysis (Rail)", report_name_format)
                        headers =[
                            "Sr.No.",
                            "Plant Certificate ID",
                            "Sample No",
                            "Do No",
                            "Plant Sample Date",
                            "Plant Preparation Date",
                            "Plant Analysis Date",
                            "Sample Qty",
                            "Mine",
                            "Mine Grade",
                            "Mode",
                            "GWEL LAB TEMP",
                            "GWEL LAB RH",
                            "GWEL ARB TM",
                            "GWEL ARB VM",
                            "GWEL ARB ASH",
                            "GWEL ARB FC",
                            "GWEL ARB GCV",
                            "GWEL ADB IM",
                            "GWEL ADB VM",
                            "GWEL ADB ASH",
                            "GWEL ADB FC",
                            "GWEL ADB GCV",
                            "GWEL ULR ID",
                            "GWEL GRADE"
                        ]
                    elif filter_type == "third_party":
                        # worksheet.merge_range("C2:H2", f"Report Name: ThirdParty Receipt Quality Analysis (Rail)", report_name_format)
                        worksheet.merge_range("C2:Y2", f"ThirdParty Receipt Quality Analysis (Rail)", report_name_format)
                        headers = [
                            "Sr.No.",
                            "Plant Certificate ID",
                            "Sample No",
                            "Do No",
                            "Plant Sample Date",
                            "Plant Preparation Date",
                            "Plant Analysis Date",
                            "Sample Qty",
                            "Mine",
                            "Mine Grade",
                            "Mode",
                            "THIRDPARTY Report Date",
                            "THIRDPARTY Reference No",
                            "THIRDPARTY Sample Date",
                            "THIRDPARTY ARB TM",
                            "THIRDPARTY ARB VM",
                            "THIRDPARTY ARB ASH",
                            "THIRDPARTY ARB FC",
                            "THIRDPARTY ARB GCV",
                            "THIRDPARTY ADB IM",
                            "THIRDPARTY ADB VM",
                            "THIRDPARTY ADB ASH",
                            "THIRDPARTY ADB FC",
                            "THIRDPARTY ADB GCV",
                            "THIRDPARTY GRADE"
                        ]
                    elif filter_type == "all":
                        # worksheet.merge_range("C2:H2", f"Report Name: Receipt Quality Analysis (Rail)", report_name_format)
                        worksheet.merge_range("C2:Y2", f"Receipt Quality Analysis (Rail)", report_name_format)
                        headers = [
                            "Sr.No.",
                            "Plant Certificate Id",
                            "Sample No",
                            "Do No",
                            "Plant Sample Date",
                            "Plant Preparation Date",
                            "Plant Analysis Date",
                            "Sample Qty",
                            "Mine",
                            "Mine Grade",
                            "Mode",
                            "GWEL Lab Temp",
                            "GWEL Lab RH",
                            "GWEL ARB TM",
                            "GWEL ARB VM",
                            "GWEL ARB ASH",
                            "GWEL ARB FC",
                            "GWEL ARB GCV",
                            "GWEL ADB IM",
                            "GWEL ADB VM",
                            "GWEL ADB ASH",
                            "GWEL ADB FC",
                            "GWEL ADB GCV",
                            "GWEL ULR ID",
                            "GWEL GRADE",
                            "THIRDPARTY REPORT DATE",
                            "THIRDPARTY REFERENCE NO",
                            "THIRDPARTY SAMPLE DATE",
                            "THIRDPARTY ARB TM",
                            "THIRDPARTY ARB VM",
                            "THIRDPARTY ARB ASH",
                            "THIRDPARTY ARB FC",
                            "THIRDPARTY ARB GCV",
                            "THIRDPARTY ADB IM",
                            "THIRDPARTY ADB VM",
                            "THIRDPARTY ADB Ash",
                            "THIRDPARTY ADB FC",
                            "THIRDPARTY ADB GCV",
                            "THIRRDPARTY GRADE",
                        ]
                    else:
                        # worksheet.merge_range("C2:H2", f"Report Name: Receipt Quality Analysis (Rail)", report_name_format)
                        worksheet.merge_range("C2:Y2", f"Receipt Quality Analysis (Rail)", report_name_format)
                        headers = [
                            "Sr.No.",
                            "Plant Certificate Id",
                            "Sample No",
                            "Do No",
                            "Plant Sample Date",
                            "Plant Preparation Date",
                            "Plant Analysis Date",
                            "Sample Qty",
                            "Mine",
                            "Mine Grade",
                            "Mode"]

                    for index, header in enumerate(headers):
                        worksheet.write(2, index, header, cell_format2)
                    fetchCoalGrades = CoalGrades.objects()
                    for row, query in enumerate(usecase_data,start=3):
                        result = query.payload()
                        worksheet.write(row, 0, count, cell_format)
                        if filter_type == "gwel":
                            worksheet.write(row, 1, str(result["plant_certificate_id"]), cell_format)
                            worksheet.write(row, 2, str(result["sample_no"]), cell_format)
                            worksheet.write(row, 3, str(result["do_no"]), cell_format)
                            worksheet.write(row, 4, str(result["GWEL_sample_date"]), cell_format)
                            worksheet.write(row, 5, str(result["GWEL_preparation_date"]), cell_format)
                            worksheet.write(row, 6, str(result["GWEL_analysis_date"]), cell_format)
                            worksheet.write(row, 7, str(result["sample_qty"]), cell_format)
                            worksheet.write(row, 8, str(result["mine"]), cell_format)
                            worksheet.write(row, 9, str(result["mine_grade"]), cell_format)
                            worksheet.write(row, 10, str(result["mode"]), cell_format)
                            worksheet.write(row, 11, str(result["GWEL_LAB_TEMP"]), cell_format)
                            worksheet.write(row, 12, str(result["GWEL_LAB_RH"]), cell_format)
                            worksheet.write(row, 13, str(result["GWEL_ARB_TM"]), cell_format)
                            worksheet.write(row, 14, str(result["GWEL_ARB_VM"]), cell_format)
                            worksheet.write(row, 15, str(result["GWEL_ARB_ASH"]), cell_format)
                            worksheet.write(row, 16, str(result["GWEL_ARB_FC"]), cell_format)
                            worksheet.write(row, 17, str(result["GWEL_ARB_GCV"]), cell_format)
                            worksheet.write(row, 18, str(result["GWEL_ADB_IM"]), cell_format)
                            worksheet.write(row, 19, str(result["GWEL_ADB_VM"]), cell_format)
                            worksheet.write(row, 20, str(result["GWEL_ADB_ASH"]), cell_format)
                            worksheet.write(row, 21, str(result["GWEL_ADB_FC"]), cell_format)
                            worksheet.write(row, 22, str(result["GWEL_ADB_GCV"]), cell_format)
                            worksheet.write(row, 23, str(result["GWEL_ULR_ID"]), cell_format)
                            if result.get("GWEL_ADB_GCV"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= int(result.get("GWEL_ADB_GCV"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        console_logger.debug("inside if")
                                        worksheet.write(row, 24, str(single_coal_grades["grade"]), cell_format)
                                    elif int(result.get("GWEL_ADB_GCV")) > 7001:
                                        worksheet.write(row, 24, "G-1", cell_format)
                                        console_logger.debug("inside else")

                        elif filter_type == "third_party":
                            worksheet.write(row, 1, str(result["plant_certificate_id"]), cell_format)
                            worksheet.write(row, 2, str(result["sample_no"]), cell_format)
                            worksheet.write(row, 3, str(result["do_no"]), cell_format)
                            worksheet.write(row, 4, str(result["GWEL_sample_date"]), cell_format)
                            worksheet.write(row, 5, str(result["GWEL_preparation_date"]), cell_format)
                            worksheet.write(row, 6, str(result["GWEL_analysis_date"]), cell_format)
                            worksheet.write(row, 7, str(result["sample_qty"]), cell_format)
                            worksheet.write(row, 8, str(result["mine"]), cell_format)
                            worksheet.write(row, 9, str(result["mine_grade"]), cell_format)
                            worksheet.write(row, 10, str(result["mode"]), cell_format)
                            if result.get("THIRDPARTY_REPORT_DATE"):
                                worksheet.write(row, 11, str(result["THIRDPARTY_REPORT_DATE"]), cell_format)
                            if result.get("THIRDPARTY_REFERENCE_NO"):
                                worksheet.write(row, 12, str(result["THIRDPARTY_REFERENCE_NO"]), cell_format)
                            if result.get("THIRDPARTY_SAMPLE_DATE"):
                                worksheet.write(row, 13, str(result["THIRDPARTY_SAMPLE_DATE"]), cell_format)
                            if result.get("THIRDPARTY_ARB_TM"):
                                worksheet.write(row, 14, str(result["THIRDPARTY_ARB_TM"]), cell_format)
                            if result.get("THIRDPARTY_ARB_VM"):
                                worksheet.write(row, 15, str(result["THIRDPARTY_ARB_VM"]), cell_format)
                            if result.get("THIRDPARTY_ARB_ASH"):
                                worksheet.write(row, 16, str(result["THIRDPARTY_ARB_ASH"]), cell_format)
                            if result.get("THIRDPARTY_ARB_FC"):
                                worksheet.write(row, 17, str(result["THIRDPARTY_ARB_FC"]), cell_format)
                            if result.get("THIRDPARTY_ARB_GCV"):
                                worksheet.write(row, 18, str(result["THIRDPARTY_ARB_GCV"]), cell_format)
                            if result.get("THIRDPARTY_ADB_IM"):
                                worksheet.write(row, 19, str(result["THIRDPARTY_ADB_IM"]), cell_format)
                            if result.get("THIRDPARTY_ADB_VM"):
                                worksheet.write(row, 20, str(result["THIRDPARTY_ADB_VM"]), cell_format)
                            if result.get("THIRDPARTY_ADB_ASH"):
                                worksheet.write(row, 21, str(result["THIRDPARTY_ADB_ASH"]), cell_format)
                            if result.get("THIRDPARTY_ADB_FC"):
                                worksheet.write(row, 22, str(result["THIRDPARTY_ADB_FC"]), cell_format)
                            if result.get("THIRDPARTY_ADB_GCV"):
                                worksheet.write(row, 23, str(result["THIRDPARTY_ADB_GCV"]), cell_format)
                            if result.get("THIRDPARTY_ADB_GCV"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= float(result.get("THIRDPARTY_ADB_GCV"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 24, str(single_coal_grades["grade"]), cell_format)
                                    elif float(result.get("THIRDPARTY_ADB_GCV")) > 7001:
                                        worksheet.write(row, 24, "G-1", cell_format)
                        elif filter_type == "all":
                            worksheet.write(row, 1, str(result["plant_certificate_id"]), cell_format)
                            worksheet.write(row, 2, str(result["sample_no"]), cell_format)
                            worksheet.write(row, 3, str(result["do_no"]), cell_format)
                            worksheet.write(row, 4, str(result["GWEL_sample_date"]), cell_format)
                            worksheet.write(row, 5, str(result["GWEL_preparation_date"]), cell_format)
                            worksheet.write(row, 6, str(result["GWEL_analysis_date"]), cell_format)
                            worksheet.write(row, 7, str(result["sample_qty"]), cell_format)
                            worksheet.write(row, 8, str(result["mine"]), cell_format)
                            worksheet.write(row, 9, str(result["mine_grade"]), cell_format)
                            worksheet.write(row, 10, str(result["mode"]), cell_format)
                            worksheet.write(row, 11, str(result["GWEL_LAB_TEMP"]), cell_format)
                            worksheet.write(row, 12, str(result["GWEL_LAB_RH"]), cell_format)
                            worksheet.write(row, 13, str(result["GWEL_ARB_TM"]), cell_format)
                            worksheet.write(row, 14, str(result["GWEL_ARB_VM"]), cell_format)
                            worksheet.write(row, 15, str(result["GWEL_ARB_ASH"]), cell_format)
                            worksheet.write(row, 16, str(result["GWEL_ARB_FC"]), cell_format)
                            worksheet.write(row, 17, str(result["GWEL_ARB_GCV"]), cell_format)
                            worksheet.write(row, 18, str(result["GWEL_ADB_IM"]), cell_format)
                            worksheet.write(row, 19, str(result["GWEL_ADB_VM"]), cell_format)
                            worksheet.write(row, 20, str(result["GWEL_ADB_ASH"]), cell_format)
                            worksheet.write(row, 21, str(result["GWEL_ADB_FC"]), cell_format)
                            worksheet.write(row, 22, str(result["GWEL_ADB_GCV"]), cell_format)
                            worksheet.write(row, 23, str(result["GWEL_ULR_ID"]), cell_format)
                            if result.get("GWEL_ADB_GCV"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= float(result.get("GWEL_ADB_GCV"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 24, str(single_coal_grades["grade"]), cell_format)
                                    elif float(result.get("GWEL_ADB_GCV")) > 7001:
                                        worksheet.write(row, 24, "G-1", cell_format)
                            if result.get("THIRDPARTY_REPORT_DATE"):
                                worksheet.write(row, 25, str(result["THIRDPARTY_REPORT_DATE"]), cell_format)
                            if result.get("THIRDPARTY_REFERENCE_NO"):
                                worksheet.write(row, 26, str(result["THIRDPARTY_REFERENCE_NO"]), cell_format)
                            if result.get("THIRDPARTY_SAMPLE_DATE"):
                                worksheet.write(row, 27, str(result["THIRDPARTY_SAMPLE_DATE"]), cell_format)
                            if result.get("THIRDPARTY_ARB_TM"):
                                worksheet.write(row, 28, str(result["THIRDPARTY_ARB_TM"]), cell_format)
                            if result.get("THIRDPARTY_ARB_VM"):
                                worksheet.write(row, 29, str(result["THIRDPARTY_ARB_VM"]), cell_format)
                            if result.get("THIRDPARTY_ARB_ASH"):
                                worksheet.write(row, 30, str(result["THIRDPARTY_ARB_ASH"]), cell_format)
                            if result.get("THIRDPARTY_ARB_FC"):
                                worksheet.write(row, 31, str(result["THIRDPARTY_ARB_FC"]), cell_format)
                            if result.get("THIRDPARTY_ARB_GCV"):
                                worksheet.write(row, 32, str(result["THIRDPARTY_ARB_GCV"]), cell_format)
                            if result.get("THIRDPARTY_ADB_IM"):
                                worksheet.write(row, 33, str(result["THIRDPARTY_ADB_IM"]), cell_format)
                            if result.get("THIRDPARTY_ADB_VM"):
                                worksheet.write(row, 34, str(result["THIRDPARTY_ADB_VM"]), cell_format)
                            if result.get("THIRDPARTY_ADB_ASH"):
                                worksheet.write(row, 35, str(result["THIRDPARTY_ADB_ASH"]), cell_format)
                            if result.get("THIRDPARTY_ADB_FC"):
                                worksheet.write(row, 36, str(result["THIRDPARTY_ADB_FC"]), cell_format)
                            if result.get("THIRDPARTY_ADB_GCV"):
                                worksheet.write(row, 37, str(result["THIRDPARTY_ADB_GCV"]), cell_format)
                            if result.get("THIRDPARTY_ADB_GCV"):
                                for single_coal_grades in fetchCoalGrades:
                                    if (
                                        int(single_coal_grades["start_value"])
                                        <= float(result.get("THIRDPARTY_ADB_GCV"))
                                        <= int(single_coal_grades["end_value"])
                                        and single_coal_grades["start_value"] != ""
                                        and single_coal_grades["end_value"] != ""
                                    ):
                                        worksheet.write(row, 38, str(single_coal_grades["grade"]), cell_format)
                                    elif float(result.get("THIRDPARTY_ADB_GCV")) > 7001:
                                        worksheet.write(row, 38, "G-1", cell_format)
                        else:
                            worksheet.write(row, 1, str(result["plant_certificate_id"]), cell_format)
                            worksheet.write(row, 2, str(result["sample_no"]), cell_format)
                            worksheet.write(row, 3, str(result["do_no"]), cell_format)
                            worksheet.write(row, 4, str(result["GWEL_sample_date"]), cell_format)
                            worksheet.write(row, 5, str(result["GWEL_preparation_date"]), cell_format)
                            worksheet.write(row, 6, str(result["GWEL_analysis_date"]), cell_format)
                            worksheet.write(row, 7, str(result["sample_qty"]), cell_format)
                            worksheet.write(row, 8, str(result["mine"]), cell_format)
                            worksheet.write(row, 9, str(result["mine_grade"]), cell_format)
                            worksheet.write(row, 10, str(result["mode"]), cell_format)
                        count -= 1
                    workbook.close()

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
            # response.status_code = 400
            console_logger.debug(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
            console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
            return e
        
    def download_coal_test_excel(self, start_date, end_date, filter_type):
        try:
            console_logger.debug(start_date)
            console_logger.debug(end_date)
            console_logger.debug(filter_type)
            fetchDataRail = self.coal_rail_excel(start_date, end_date, filter_type)
            fetchDataRoad = self.coal_test_road_excel(start_date, end_date, filter_type)

            console_logger.debug(fetchDataRail)
            console_logger.debug(fetchDataRoad)

            dataDict = {
                "road": fetchDataRoad.get("File_Path"),
                "rail": fetchDataRail.get("File_Path"),
            }

            return dataDict

        except Exception as e:
            # response.status_code = 400
            console_logger.debug(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
            console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
            return e

    # def download_road_coal_logistics(self, specified_date: str, mine: Optional[str] = "All"):
    #     try:
    #         data = {}
    #         result = {
    #             "labels": [],
    #             "datasets": [],
    #             "weight_total": [],
    #             "total": 0,
    #             "page_size": 15,
    #         }

    #         if mine and mine != "All":
    #             data["mine__icontains"] = mine.upper()

    #         if specified_date:
    #             to_ts = self.convert_to_utc_format(f'{specified_date} 23:59:59', "%Y-%m-%d %H:%M:%S")

    #         logs = (
    #             Gmrdata.objects(GWEL_Tare_Time__lte=to_ts, actual_tare_qty__ne=None, gate_approved=True, GWEL_Tare_Time__ne=None)
    #             # Gmrdata.objects()
    #             .order_by("-GWEL_Tare_Time")
    #         )

    #         sap_records = SapRecords.objects.all()
        
    #         if any(logs):
    #             aggregated_data = defaultdict(
    #                 lambda: defaultdict(
    #                     lambda: {
    #                         "DO_Qty": 0,
    #                         "challan_lr_qty": 0,
    #                         "challan_lr_qty_full": 0,
    #                         "mine_name": "",
    #                         "balance_qty": 0,
    #                         "percent_of_supply": 0,
    #                         "actual_net_qty": 0,
    #                         "Gross_Calorific_Value_(Adb)": 0,
    #                         "count": 0,
    #                         "coal_count": 0,
    #                         "start_date": "",
    #                         "end_date": "",
    #                         "source_type": "",
    #                     }
    #                 )
    #             )


    #             start_dates = {}
    #             grade = 0
    #             for log in logs:
    #                 if log.GWEL_Tare_Time!=None:
    #                     month = log.GWEL_Tare_Time.strftime("%Y-%m")
    #                     date = log.GWEL_Tare_Time.strftime("%Y-%m-%d")
    #                     payload = log.payload()
    #                     result["labels"] = list(payload.keys())
    #                     mine_name = payload.get("Mines_Name")
    #                     do_no = payload.get("DO_No")
    #                     if payload.get("Grade") is not None:
    #                         if '-' in payload.get("Grade"):
    #                             grade = payload.get("Grade").split("-")[0]
    #                         else:
    #                             grade = payload.get("Grade")
    #                     # If start_date is None or the current vehicle_in_time is earlier than start_date, update start_date
    #                     # if do_no not in start_dates:
    #                     #     start_dates[do_no] = date
    #                     # elif date < start_dates[do_no]:
    #                     #     start_dates[do_no] = date
    #                     if payload.get("slno"):
    #                         aggregated_data[date][do_no]["slno"] = datetime.datetime.strptime(payload.get("slno"), '%Y%m').strftime('%B %Y')
    #                     else:
    #                         aggregated_data[date][do_no]["slno"] = "-"
    #                     if payload.get("start_date"):
    #                         aggregated_data[date][do_no]["start_date"] = payload.get("start_date")
    #                     else:
    #                         aggregated_data[date][do_no]["start_date"] = "0"
    #                     if payload.get("end_date"):
    #                         aggregated_data[date][do_no]["end_date"] = payload.get("end_date")
    #                     else:
    #                         aggregated_data[date][do_no]["end_date"] = "0"

    #                     if payload.get("Type_of_consumer"):
    #                         aggregated_data[date][do_no]["source_type"] = payload.get("Type_of_consumer")

    #                     if payload.get("DO_Qty"):
    #                         aggregated_data[date][do_no]["DO_Qty"] = float(
    #                             payload["DO_Qty"]
    #                         )
    #                     else:
    #                         aggregated_data[date][do_no]["DO_Qty"] = 0

    #                     challan_net_wt = payload.get("Challan_Net_Wt(MT)")    
                    
    #                     if challan_net_wt:
    #                         aggregated_data[date][do_no]["challan_lr_qty"] += float(challan_net_wt)

    #                     if payload.get("Mines_Name"):
    #                         aggregated_data[date][do_no]["mine_name"] = payload[
    #                             "Mines_Name"
    #                         ]
    #                     else:
    #                         aggregated_data[date][do_no]["mine_name"] = "-"
    #                     aggregated_data[date][do_no]["count"] += 1 
                
    #             for record in sap_records:
    #                 do_no = record.do_no
    #                 if do_no not in aggregated_data[specified_date]:
    #                     aggregated_data[specified_date][do_no]["DO_Qty"] = float(record.do_qty) if record.do_qty else 0
    #                     aggregated_data[specified_date][do_no]["mine_name"] = record.mine_name if record.mine_name else "-"
    #                     aggregated_data[specified_date][do_no]["start_date"] = record.start_date if record.start_date else "0"
    #                     aggregated_data[specified_date][do_no]["end_date"] = record.end_date if record.end_date else "0"
    #                     aggregated_data[specified_date][do_no]["source_type"] = record.consumer_type if record.consumer_type else "Unknown"
    #                     try:
    #                         aggregated_data[specified_date][do_no]["slno"] = datetime.datetime.strptime(record.slno, "%Y%m").strftime("%B %Y") if record.slno else "-"
    #                     except ValueError as e:
    #                         aggregated_data[specified_date][do_no]["slno"] = record.slno if record.slno else "-"
    #                     aggregated_data[specified_date][do_no]["count"] = 1

    #             dataList = [
    #                 {
    #                     "date": date,
    #                     "data": {
    #                         do_no: {
    #                             "DO_Qty": data["DO_Qty"],
    #                             "challan_lr_qty": data["challan_lr_qty"],
    #                             "mine_name": data["mine_name"],
    #                             "grade": grade,
    #                             "date": date,
    #                             "start_date": data["start_date"],
    #                             "end_date": data["end_date"],
    #                             "source_type": data["source_type"],
    #                             "slno": data["slno"],
    #                         }
    #                         for do_no, data in aggregated_data[date].items()
    #                     },
    #                 }
    #                 for date in aggregated_data
    #             ]
    #             final_data = []
    #             for entry in dataList:
    #                 date = entry["date"]
    #                 for data_dom, values in entry['data'].items():
    #                     dictData = {}
    #                     dictData["DO_No"] = data_dom
    #                     dictData["mine_name"] = values["mine_name"]
    #                     dictData["DO_Qty"] = values["DO_Qty"]
    #                     dictData["club_challan_lr_qty"] = values["challan_lr_qty"]
    #                     dictData["date"] = values["date"]
    #                     dictData["start_date"] = values["start_date"]
    #                     dictData["end_date"] = values["end_date"]
    #                     dictData["source_type"] = values["source_type"]
    #                     dictData["slno"] = values["slno"]
    #                     dictData["cumulative_challan_lr_qty"] = 0
    #                     dictData["balance_qty"] = 0
    #                     dictData["percent_supply"] = 0
    #                     dictData["asking_rate"] = 0
    #                     dictData['average_GCV_Grade'] = values["grade"]
                        
    #                     if dictData["start_date"] != "0" and dictData["end_date"] != "0":
    #                         today_date = datetime.datetime.today().date()
    #                         # balance_days = datetime.datetime.strptime(dictData["end_date"], "%Y-%m-%d").date() - datetime.datetime.strptime(dictData["start_date"], "%Y-%m-%d").date()
    #                         tomorrow_date = datetime.datetime.strptime(dictData["end_date"], "%Y-%m-%d").date() + datetime.timedelta(days=1)
    #                         # balance_days = datetime.datetime.strptime(dictData["end_date"], "%Y-%m-%d").date() - datetime.datetime.today().date()
    #                         balance_days = tomorrow_date - datetime.datetime.today().date()
    #                         dictData["balance_days"] = balance_days.days
    #                     else:
    #                         dictData["balance_days"] = 0

    #                     # if data_dom in start_dates:
    #                     #     dictData["start_date"] = start_dates[data_dom]
    #                     #     dictData["end_date"] = datetime.datetime.strptime(start_dates[data_dom], "%Y-%m-%d") + timedelta(days=44)
    #                     #     balance_days = dictData["end_date"].date() - datetime.datetime.today().date()
    #                     #     dictData["balance_days"] = balance_days.days
    #                     # else:
    #                     #     dictData["start_date"] = None
    #                     #     dictData["end_date"] = None
    #                     #     dictData["balance_days"] = None
                        
    #                     final_data.append(dictData)

    #             if final_data:
    #                 startdate = f'{specified_date} 00:00:00'
    #                 enddate = f'{specified_date} 23:59:59'
    #                 # to_ts = datetime.datetime.strptime(enddate,"%Y-%m-%d %H:%M:%S")
    #                 from_ts = self.convert_to_utc_format(startdate, "%Y-%m-%d %H:%M:%S")
    #                 to_ts = self.convert_to_utc_format(enddate, "%Y-%m-%d %H:%M:%S")
                    
    #                 pipeline = [
    #                     {
    #                         "$match": {
    #                             "GWEL_Tare_Time": {"$gte": from_ts, "$lte": to_ts},
    #                                 "net_qty": {"$ne": None}
    #                             }
    #                     },
    #                     {
    #                     '$group': {
    #                         '_id': {
    #                             'date': {
    #                                 '$dateToString': {
    #                                     'format': '%Y-%m-%d', 
    #                                     'date': '$GWEL_Tare_Time'
    #                                 }
    #                             }, 
    #                             'do_no': '$arv_cum_do_number'
    #                         }, 
    #                         'total_net_qty': {
    #                             '$sum': {
    #                                 '$toDouble': '$net_qty'
    #                             }
    #                         }
    #                     }
    #                 }]

    #                 # filtered_data = [
    #                 #     entry for entry in dataList if entry["date"] == specified_date
    #                 # ]
                    
    #                 filtered_data_new = Gmrdata.objects.aggregate(pipeline)
    #                 aggregated_totals = defaultdict(float)
    #                 for single_data_entry in filtered_data_new:
    #                     do_no = single_data_entry['_id']['do_no']
    #                     total_net_qty = single_data_entry['total_net_qty']
    #                     aggregated_totals[do_no] += total_net_qty
                        
    #                 data_by_do = {}
    #                 finaldataMain = [single_data_list for single_data_list in final_data if single_data_list.get("balance_days") >= 0]
    #                 for entry in finaldataMain:
    #                     do_no = entry['DO_No']
                        
    #                     if do_no not in data_by_do:
    #                         data_by_do[do_no] = entry
    #                         data_by_do[do_no]['cumulative_challan_lr_qty'] = round(entry['club_challan_lr_qty'], 2)
    #                     else:
    #                         data_by_do[do_no]['cumulative_challan_lr_qty'] += round(entry['club_challan_lr_qty'], 2)

    #                     if do_no in aggregated_totals:
    #                         data_by_do[do_no]['challan_lr_qty'] = round(aggregated_totals[do_no], 2)
    #                     else:
    #                         data_by_do[do_no]['challan_lr_qty'] = 0

    #                     if data_by_do[do_no]['DO_Qty'] != 0 and data_by_do[do_no]['cumulative_challan_lr_qty'] != 0:
    #                         data_by_do[do_no]['percent_supply'] = round((data_by_do[do_no]['cumulative_challan_lr_qty'] / data_by_do[do_no]['DO_Qty']) * 100, 2)
    #                     else:
    #                         data_by_do[do_no]['percent_supply'] = 0

    #                     # if data_by_do[do_no]['cumulative_challan_lr_qty'] != 0 and data_by_do[do_no]['DO_Qty'] != 0:
    #                     data_by_do[do_no]['balance_qty'] = round(data_by_do[do_no]['DO_Qty'] - data_by_do[do_no]['cumulative_challan_lr_qty'], 2)
    #                     # else:
    #                     #     data_by_do[do_no]['balance_qty'] = 0
                        
    #                     if data_by_do[do_no]['balance_days'] and data_by_do[do_no]['balance_qty'] != 0:
    #                         data_by_do[do_no]['asking_rate'] = round(data_by_do[do_no]['balance_qty'] / data_by_do[do_no]['balance_days'], 2)

    #                 # final_data = list(data_by_do.values())
    #                 sort_final_data = list(data_by_do.values())
    #                 # Sort the data by 'balance_days', placing entries with 'balance_days' of 0 at the end
    #                 final_data = sorted(sort_final_data, key=lambda x: (x['balance_days'] == 0, x['balance_days']))
    #                 if final_data:
                        
    #                     grouped_data = defaultdict(list)
    #                     for single_data in final_data:
    #                         source_type = single_data.get("source_type").strip()
    #                         grouped_data[source_type].append(single_data)

    #                     final_total_do_qty = 0
    #                     final_total_challan_lr_qty = 0
    #                     final_total_cc_lr_qty = 0
    #                     final_total_balance_qty = 0

    #                     per_data = ""
    #                     per_data += "<table border='1'>"
    #                     for source_type, entries in grouped_data.items():
    #                         # per_data += f"<span style='font-size: 10px; font-weight: 600'>{source_type}</span>"
    #                         per_data += f"<tr><td colspan='13' style='text-align: center'><b>{source_type}</b></span></td></tr>"
    #                         # per_data += "<table class='logistic_report_data' style='width: 100%; text-align: center; border-spacing: 0px; border: 1px solid lightgray;'>"
    #                         per_data += (
    #                             "<thead>"
    #                         )
    #                         per_data += "<tr>"
    #                         per_data += "<th>Month</th>"
    #                         per_data += "<th>Mine Name</th>"
    #                         per_data += "<th>DO No</th>"
    #                         per_data += "<th>Grade</th>"
    #                         per_data += "<th>DO Qty</th>"
    #                         per_data += "<th>Challan LR / Qty</th>"
    #                         per_data += "<th>C.C. LR / Qty</th>"
    #                         per_data += "<th>Balance Qty</th>"
    #                         per_data += "<th>% of Supply</th>"
    #                         per_data += "<th>Balance Days</th>"
    #                         per_data += "<th>Asking Rate</th>"
    #                         per_data += "<th>Do Start date</th>"
    #                         per_data += "<th>Do End date</th></tr></thead><tbody>"
    #                         total_do_qty = 0
    #                         total_challan_lr_qty = 0
    #                         total_cc_lr_qty = 0
    #                         total_balance_qty = 0

    #                         for entry in entries:
    #                             per_data += f"<tr>"
    #                             per_data += f"<td> {entry.get('slno')}</span></td>"
    #                             per_data += f"<td> {entry.get('mine_name')}</span></td>"
    #                             per_data += f"<td> {entry.get('DO_No')}</span></td>"
    #                             per_data += f"<td> {entry.get('average_GCV_Grade')}</span></td>"
    #                             per_data += f"<td> {round(entry.get('DO_Qty'), 2)}</span></td>"
    #                             total_do_qty += round(entry.get('DO_Qty'), 2)
    #                             per_data += f"<td> {round(entry.get('challan_lr_qty'), 2)}</span></td>"
    #                             total_challan_lr_qty += round(entry.get('challan_lr_qty'), 2)
    #                             per_data += f"<td> {round(entry.get('cumulative_challan_lr_qty'), 2)}</span></td>"
    #                             total_cc_lr_qty += round(entry.get('cumulative_challan_lr_qty'), 2)
    #                             per_data += f"<td> {round(entry.get('balance_qty'), 2)}</span></td>"
    #                             total_balance_qty += round(entry.get('balance_qty'), 2)
    #                             per_data += f"<td> {round(entry.get('percent_supply'), 2)}%</span></td>"
    #                             per_data += f"<td> {entry.get('balance_days')}</span></td>"
    #                             per_data += f"<td> {round(entry.get('asking_rate'))}</span></td>"
    #                             if entry.get("start_date") != "0":
    #                                 per_data += f"<td> {datetime.datetime.strptime(entry.get('start_date'),'%Y-%m-%d').strftime('%d %B %y')}</span></td>"
    #                             else:
    #                                 per_data += f"<td>0</span></td>"
    #                             if entry.get("end_date") != "0":
    #                                 per_data += f"<td> {datetime.datetime.strptime(entry.get('end_date'),'%Y-%m-%d').strftime('%d %B %y')}</span></td>"
    #                             else:    
    #                                 per_data += f"<td>0</span></td>"
    #                             per_data += "</tr>"
    #                         per_data += "<tr>"
    #                         per_data += "<td colspan='4'><strong>Total</strong></td>"
    #                         per_data += f"<td><strong>{round(total_do_qty, 2)}</strong></td>"
    #                         per_data += f"<td><strong>{round(total_challan_lr_qty, 2)}</strong></td>"
    #                         per_data += f"<td><strong>{round(total_cc_lr_qty, 2)}</strong></td>"
    #                         per_data += f"<td><strong>{round(total_balance_qty, 2)}</strong></td>"
    #                         if total_cc_lr_qty != 0 and total_do_qty != 0:
    #                             per_data += f"<td><strong>{round(total_cc_lr_qty/total_do_qty, 2)}%</strong></td>"
    #                         else:
    #                             per_data += f"<td><strong>0%</strong></td>"
    #                         per_data += f"<td colspan='4'><strong></strong></td>"
    #                         per_data += "</tr>"
    #                         final_total_do_qty += total_do_qty
    #                         final_total_challan_lr_qty += total_challan_lr_qty
    #                         final_total_cc_lr_qty += total_cc_lr_qty
    #                         final_total_balance_qty += total_balance_qty
    #                     per_data += "<tr>"
    #                     per_data += "<td colspan='4'><strong>Grand Total</strong></td>"
    #                     per_data += f"<td><strong>{round(final_total_do_qty, 2)}</strong></td>"
    #                     per_data += f"<td><strong>{round(final_total_challan_lr_qty, 2)}</strong></td>"
    #                     per_data += f"<td><strong>{round(final_total_cc_lr_qty, 2)}</strong></td>"
    #                     per_data += f"<td><strong>{round(final_total_balance_qty, 2)}</strong></td>"
    #                     per_data += f"<td><strong>{round(final_total_cc_lr_qty/final_total_do_qty, 2)}%</strong></td>"
    #                     per_data += f"<td colspan='4'><strong></strong></td>"
    #                     per_data += "</tr>"
    #                     per_data += "</tbody></table>"
    #                     return per_data
    #                 else:
    #                     return 404

    #     except Exception as e:
    #         # response.status_code = 400
    #         console_logger.debug(e)
    #         exc_type, exc_obj, exc_tb = sys.exc_info()
    #         fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #         console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
    #         console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
    #         return e

    def download_road_coal_logistics(self, specified_date: str):
        try:
            if specified_date:
                from_ts = self.convert_to_utc_format(f'{specified_date} 00:00:00', "%Y-%m-%d %H:%M:%S")
                to_ts = self.convert_to_utc_format(f'{specified_date} 23:59:59', "%Y-%m-%d %H:%M:%S")
            created_at_date = datetime.datetime(2024, 9, 23, 19, 50, 51, 572000)
            basePipeline = [
                {
                    '$match': {
                        'created_at': {
                            '$gt': created_at_date,
                        }
                    }
                },
                {
                    '$match': {
                        'GWEL_Tare_Time': {
                            '$ne': None, 
                            # '$gte': from_ts, 
                            '$lte': to_ts,
                        }
                    }
                }, {
                    '$group': {
                        '_id': '$arv_cum_do_number', 
                        'challan_lr_qty': {
                            '$sum': {
                                '$toDouble': '$net_qty'
                            }
                        }, 
                        'Grade': {
                            '$first': '$grade'
                        }, 
                        'slno': {
                            '$first': '$slno'
                        }, 
                        'start_date': {
                            '$first': '$start_date'
                        }, 
                        'end_date': {
                            '$first': '$end_date'
                        }, 
                        'type_consumer': {
                            '$first': '$type_consumer'
                        }, 
                        'do_qty': {
                            '$first': '$po_qty'
                        }, 
                        'mine_name': {
                            '$first': '$mine'
                        }, 
                        'grn_status': {
                            '$first': '$grn_status'
                        },
                        'date': {
                            '$last': '$GWEL_Tare_Time'
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'gmrdata', 
                        'localField': '_id', 
                        'foreignField': 'arv_cum_do_number', 
                        'as': 'cumulative_data'
                    }
                }, 
                {
                    '$addFields': {
                        'cumulative_challan_lr_qty': {
                            '$sum': {
                                '$map': {
                                    'input': '$cumulative_data', 
                                    'as': 'item', 
                                    'in': {
                                        '$convert': {
                                            'input': '$$item.net_qty', 
                                            'to': 'double', 
                                            'onError': 0, 
                                            'onNull': 0
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            ]

            challanlrqtybasePipeline = [
                {
                    '$match': {
                        'created_at': {
                            '$gt': created_at_date,
                        }
                    }
                },
                {
                    '$match': {
                        'GWEL_Tare_Time': {
                            '$ne': None, 
                            '$gte': from_ts, 
                            '$lte': to_ts,
                        }
                    }
                }, {
                    '$group': {
                        '_id': '$arv_cum_do_number', 
                        'challan_lr_qty': {
                            '$sum': {
                                '$toDouble': '$net_qty'
                            }
                        }, 
                        'Grade': {
                            '$first': '$grade'
                        }, 
                        'slno': {
                            '$first': '$slno'
                        }, 
                        'start_date': {
                            '$first': '$start_date'
                        }, 
                        'end_date': {
                            '$first': '$end_date'
                        }, 
                        'type_consumer': {
                            '$first': '$type_consumer'
                        }, 
                        'do_qty': {
                            '$first': '$po_qty'
                        }, 
                        'mine_name': {
                            '$first': '$mine'
                        }, 
                        'grn_status': {
                            '$first': '$grn_status'
                        },
                        'date': {
                            '$last': '$GWEL_Tare_Time'
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'gmrdata', 
                        'localField': '_id', 
                        'foreignField': 'arv_cum_do_number', 
                        'as': 'cumulative_data'
                    }
                }, 
                {
                    '$addFields': {
                        'cumulative_challan_lr_qty': {
                            '$sum': {
                                '$map': {
                                    'input': '$cumulative_data', 
                                    'as': 'item', 
                                    'in': {
                                        '$convert': {
                                            'input': '$$item.net_qty', 
                                            'to': 'double', 
                                            'onError': 0, 
                                            'onNull': 0
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            ]
            basePipelineHistoric = [
                {
                    '$match': {
                        'GWEL_Tare_Time': {
                            '$ne': None, 
                            # '$gte': from_ts, 
                            '$lte': to_ts,
                        }
                    }
                }, {
                    '$group': {
                        '_id': '$arv_cum_do_number', 
                        'challan_lr_qty': {
                            '$sum': {
                                '$toDouble': '$net_qty'
                            }
                        }, 
                        'Grade': {
                            '$first': '$grade'
                        }, 
                        'slno': {
                            '$first': '$slno'
                        }, 
                        'start_date': {
                            '$first': '$start_date'
                        }, 
                        'end_date': {
                            '$first': '$end_date'
                        }, 
                        'type_consumer': {
                            '$first': '$type_consumer'
                        }, 
                        'do_qty': {
                            '$first': '$po_qty'
                        }, 
                        'mine_name': {
                            '$first': '$mine'
                        }, 
                        'grn_status': {
                            '$first': '$grn_status'
                        },
                        'date': {
                            '$last': '$GWEL_Tare_Time'
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'gmrdata', 
                        'localField': '_id', 
                        'foreignField': 'arv_cum_do_number', 
                        'as': 'cumulative_data'
                    }
                }, 
                {
                    '$addFields': {
                        'cumulative_challan_lr_qty': {
                            '$sum': {
                                '$map': {
                                    'input': '$cumulative_data', 
                                    'as': 'item', 
                                    'in': {
                                        '$convert': {
                                            'input': '$$item.net_qty', 
                                            'to': 'double', 
                                            'onError': 0, 
                                            'onNull': 0
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            ]

            basepipelineHistoricChallanLrQty = [
                {
                    '$match': {
                        'GWEL_Tare_Time': {
                            '$ne': None, 
                            '$gte': from_ts, 
                            '$lte': to_ts,
                        }
                    }
                }, {
                    '$group': {
                        '_id': '$arv_cum_do_number', 
                        'challan_lr_qty': {
                            '$sum': {
                                '$toDouble': '$net_qty'
                            }
                        }, 
                        'Grade': {
                            '$first': '$grade'
                        }, 
                        'slno': {
                            '$first': '$slno'
                        }, 
                        'start_date': {
                            '$first': '$start_date'
                        }, 
                        'end_date': {
                            '$first': '$end_date'
                        }, 
                        'type_consumer': {
                            '$first': '$type_consumer'
                        }, 
                        'do_qty': {
                            '$first': '$po_qty'
                        }, 
                        'mine_name': {
                            '$first': '$mine'
                        }, 
                        'grn_status': {
                            '$first': '$grn_status'
                        },
                        'date': {
                            '$last': '$GWEL_Tare_Time'
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'gmrdata', 
                        'localField': '_id', 
                        'foreignField': 'arv_cum_do_number', 
                        'as': 'cumulative_data'
                    }
                }, 
                {
                    '$addFields': {
                        'cumulative_challan_lr_qty': {
                            '$sum': {
                                '$map': {
                                    'input': '$cumulative_data', 
                                    'as': 'item', 
                                    'in': {
                                        '$convert': {
                                            'input': '$$item.net_qty', 
                                            'to': 'double', 
                                            'onError': 0, 
                                            'onNull': 0
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            ]

            # roadrcr start
            basePipelineRcrRoad = [
                {
                    '$match': {
                        'tar_wt_date': {
                            '$ne': None, 
                            # '$gte': from_ts, 
                            '$lte': to_ts,
                        }
                    }
                }, {
                    '$group': {
                        '_id': '$do_number', 
                        'challan_lr_qty': {
                            '$sum': {
                                '$toDouble': '$dc_net_wt'
                            }
                        }, 
                        'Grade': {
                            '$first': '$grade'
                        }, 
                        'slno': {
                            '$first': '$slno'
                        }, 
                        'start_date': {
                            '$first': '$start_date'
                        }, 
                        'end_date': {
                            '$first': '$end_date'
                        }, 
                        'type_consumer': {
                            '$first': '$type_consumer'
                        }, 
                        'do_qty': {
                            '$first': '$po_qty'
                        }, 
                        'mine_name': {
                            '$first': '$mine'
                        }, 
                        'date': {
                            '$last': '$tar_wt_date'
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'RcrRoadData', 
                        'localField': '_id', 
                        'foreignField': 'do_number', 
                        'as': 'cumulative_data'
                    }
                }, 
                # {
                #     '$addFields': {
                #         'cumulative_challan_lr_qty': {
                #             '$sum': {
                #                 '$map': {
                #                     'input': '$cumulative_data', 
                #                     'as': 'item', 
                #                     'in': {
                #                         '$toDouble': '$$item.dc_net_wt'
                #                     }
                #                 }
                #             }
                #         }
                #     }
                # }
                {
                    '$addFields': {
                        'cumulative_challan_lr_qty': {
                            '$sum': {
                                '$map': {
                                    'input': '$cumulative_data', 
                                    'as': 'item', 
                                    'in': {
                                        '$convert': {
                                            'input': '$$item.net_qty', 
                                            'to': 'double', 
                                            'onError': 0, 
                                            'onNull': 0
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            ]


            basepipelineRcrRoadChallanLrQty = [
                {
                    '$match': {
                        'tar_wt_date': {
                            '$ne': None, 
                            '$gte': from_ts, 
                            '$lte': to_ts,
                        }
                    }
                }, {
                    '$group': {
                        '_id': '$do_number', 
                        'challan_lr_qty': {
                            '$sum': {
                                '$toDouble': '$dc_net_wt'
                            }
                        }, 
                        'Grade': {
                            '$first': '$grade'
                        }, 
                        'slno': {
                            '$first': '$slno'
                        }, 
                        'start_date': {
                            '$first': '$start_date'
                        }, 
                        'end_date': {
                            '$first': '$end_date'
                        }, 
                        'type_consumer': {
                            '$first': '$type_consumer'
                        }, 
                        'do_qty': {
                            '$first': '$po_qty'
                        }, 
                        'mine_name': {
                            '$first': '$mine'
                        }, 
                        # 'grn_status': {
                        #     '$first': '$grn_status'
                        # },
                        'date': {
                            '$last': '$tar_wt_date'
                        }
                    }
                }, {
                    '$lookup': {
                        'from': 'RcrRoadData', 
                        'localField': '_id', 
                        'foreignField': 'do_number', 
                        'as': 'cumulative_data'
                    }
                }, 
                {
                    '$addFields': {
                        'cumulative_challan_lr_qty': {
                            '$sum': {
                                '$map': {
                                    'input': '$cumulative_data', 
                                    'as': 'item', 
                                    'in': {
                                        '$convert': {
                                            'input': '$$item.net_qty', 
                                            'to': 'double', 
                                            'onError': 0, 
                                            'onNull': 0
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            ]
            # roadrcr end 

            saprecordsPipeline = [
                {
                    '$match': {
                        '$expr': {
                            '$and': [
                                {
                                    '$lte': [
                                        { '$dateFromString': { 'dateString': '$start_date' } }, 
                                        datetime.datetime.strptime(specified_date, "%Y-%m-%d")
                                    ]
                                }, 
                                {
                                    '$gte': [
                                        { '$dateFromString': { 'dateString': '$end_date' } }, 
                                        datetime.datetime.strptime(specified_date, "%Y-%m-%d")
                                    ]
                                }
                            ]
                        }
                    }
                }, 
                {
                    '$group': {
                        '_id': '$do_no',  # Grouping by do_no
                        'mine_name': { '$first': '$mine_name' },  # Getting the first mine_name in the group
                        'do_qty': { '$sum': { '$toDouble': '$do_qty' } },  # Summing up the do_qty as double
                        'start_date': { '$first': '$start_date' },  # Getting the first start_date
                        'end_date': { '$first': '$end_date' },  # Getting the first end_date
                        'source_type': { '$first': '$consumer_type' },  # Getting the first source_type
                        'slno': { '$first': '$slno' },  # Getting the first slno
                        'Grade': {'$first': '$grade'}
                    }
                }, 
                {
                    '$project': {
                        '_id': 1, 
                        'mine_name': 1, 
                        'do_qty': 1, 
                        'start_date': 1, 
                        'end_date': 1, 
                        'source_type': 1, 
                        'slno': 1,
                        'Grade': 1,
                    }
                }
            ]


            saprecordsRcrRoadPipeline = [
                {
                    '$match': {
                        '$expr': {
                            '$and': [
                                {
                                    '$lte': [
                                        { '$dateFromString': { 'dateString': '$start_date' } }, 
                                        datetime.datetime.strptime(specified_date, "%Y-%m-%d")
                                    ]
                                }, 
                                {
                                    '$gte': [
                                        { '$dateFromString': { 'dateString': '$end_date' } }, 
                                        datetime.datetime.strptime(specified_date, "%Y-%m-%d")
                                    ]
                                }
                            ]
                        }
                    }
                }, 
                {
                    '$group': {
                        '_id': '$do_no',  # Grouping by do_no
                        'mine_name': { '$first': '$mine_name' },  # Getting the first mine_name in the group
                        'do_qty': { '$sum': { '$toDouble': '$do_qty' } },  # Summing up the do_qty as double
                        'start_date': { '$first': '$start_date' },  # Getting the first start_date
                        'end_date': { '$first': '$end_date' },  # Getting the first end_date
                        'source_type': { '$first': '$consumer_type' },  # Getting the first source_type
                        'slno': { '$first': '$slno' },  # Getting the first slno
                        'Grade': {'$first': '$grade'}
                    }
                }, 
                {
                    '$project': {
                        '_id': 1, 
                        'mine_name': 1, 
                        'do_qty': 1, 
                        'start_date': 1, 
                        'end_date': 1, 
                        'source_type': 1, 
                        'slno': 1,
                        'Grade': 1,
                    }
                }
            ]


            fetchGmrData = Gmrdata.objects.aggregate(basePipeline)
            fetchGmrDatachallanltqty = Gmrdata.objects.aggregate(challanlrqtybasePipeline)
            fetchGmrHistoricData = gmrdataHistoric.objects.aggregate(basePipelineHistoric)
            fetchGmrHistoricDataChallanLrQty = gmrdataHistoric.objects.aggregate(basepipelineHistoricChallanLrQty)
            
            fetchRcrRoadData = RcrRoadData.objects.aggregate(basePipelineRcrRoad)
            fetchRcrRoadchallanlrqty = RcrRoadData.objects.aggregate(basepipelineRcrRoadChallanLrQty)
            
            fetchSapRecordsData = SapRecords.objects.aggregate(saprecordsPipeline)

            fetchSapRecordsRcrData = SapRecordsRcrRoad.objects.aggregate(saprecordsRcrRoadPipeline)
            listData= []
            for singleData in fetchGmrData:
                dictData = {}
                dictData["DO_No"] = singleData.get("_id")
                dictData["mine_name"] = singleData.get("mine_name")
                if singleData.get("do_qty"):
                    dictData["DO_Qty"] = int(singleData.get("do_qty"))
                else:
                    dictData["DO_Qty"] = 0
                dictData["cumulative_challan_lr_qty"] = round(singleData.get("cumulative_challan_lr_qty"), 2)
                dictData["grn_status"] = singleData.get("grn_status")
                dictData["date"] = singleData.get("date").strftime("%Y-%m-%d")
                dictData["start_date"] = singleData.get("start_date")
                dictData["end_date"] = singleData.get("end_date")
                dictData["source_type"] = singleData.get("type_consumer")
                dictData["slno"] = datetime.datetime.strptime(singleData.get("slno"), "%Y%m").strftime("%B %Y") if singleData.get("slno") else "-"
                if singleData.get("Grade") is not None:
                    if '-' in singleData.get("Grade"):
                        dictData["average_GCV_Grade"] = singleData.get("Grade").split("-")[0]
                    elif " " in singleData.get("Grade"):
                        dictData["average_GCV_Grade"] = singleData.get("Grade").split(" ")[0]
                    else:
                        dictData["average_GCV_Grade"] = singleData.get("Grade")
                if singleData.get("start_date") is not None and singleData.get("end_date") is not None:
                    tomorrow_date = datetime.datetime.strptime(singleData.get("end_date"), "%Y-%m-%d").date() + datetime.timedelta(days=1)
                    balance_days = tomorrow_date - datetime.datetime.strptime(specified_date, "%Y-%m-%d").date()
                    dictData["balance_days"] = balance_days.days
                else:
                    dictData["balance_days"] = 0
                if singleData.get("do_qty") is not None:
                    single_do_qty = singleData.get("do_qty")
                else:
                    single_do_qty = 0
                if single_do_qty != 0:
                    dictData['percent_supply'] = round((singleData.get('cumulative_challan_lr_qty') / int(single_do_qty)) * 100, 2)
                else:
                    dictData['percent_supply'] = 0
                    
                dictData["balance_qty"] = round(int(single_do_qty) - singleData.get("cumulative_challan_lr_qty"), 2)
                    
                if dictData['balance_days'] and dictData['balance_qty'] != 0:
                    dictData['asking_rate'] = round(dictData['balance_qty'] / dictData['balance_days'], 2)
                else:
                    dictData["asking_rate"] = 0
                listData.append(dictData)
            
            for singleDataHistoric in fetchGmrHistoricData:
                dictDataHIstoric = {}
                dictDataHIstoric["DO_No"] = singleDataHistoric.get("_id")
                dictDataHIstoric["mine_name"] = singleDataHistoric.get("mine_name")
                if singleDataHistoric.get("do_qty"):
                    dictDataHIstoric["DO_Qty"] = int(float(singleDataHistoric.get("do_qty")))
                else:
                    dictDataHIstoric["DO_Qty"] = 0
                dictDataHIstoric["cumulative_challan_lr_qty"] = round(singleDataHistoric.get("cumulative_challan_lr_qty"), 2)
                dictDataHIstoric["grn_status"] = singleDataHistoric.get("grn_status")
                dictDataHIstoric["date"] = singleDataHistoric.get("date").strftime("%Y-%m-%d")
                dictDataHIstoric["start_date"] = singleDataHistoric.get("start_date")
                dictDataHIstoric["end_date"] = singleDataHistoric.get("end_date")
                dictDataHIstoric["source_type"] = singleDataHistoric.get("type_consumer")
                dictDataHIstoric["slno"] = datetime.datetime.strptime(singleDataHistoric.get("slno"), "%Y%m").strftime("%B %Y") if singleDataHistoric.get("slno") else "-"
                if singleDataHistoric.get("Grade") is not None:
                    if '-' in singleDataHistoric.get("Grade"):
                        dictDataHIstoric["average_GCV_Grade"] = singleDataHistoric.get("Grade").split("-")[0]
                    elif " " in singleDataHistoric.get("Grade"):
                        dictDataHIstoric["average_GCV_Grade"] = singleDataHistoric.get("Grade").split(" ")[0]
                    else:
                        dictDataHIstoric["average_GCV_Grade"] = singleDataHistoric.get("Grade")
                if singleDataHistoric.get("start_date") is not None and singleDataHistoric.get("end_date") is not None:
                    tomorrow_date = datetime.datetime.strptime(singleDataHistoric.get("end_date"), "%Y-%m-%d").date() + datetime.timedelta(days=1)
                    balance_days = tomorrow_date - datetime.datetime.strptime(specified_date, "%Y-%m-%d").date()
                    dictDataHIstoric["balance_days"] = balance_days.days
                else:
                    dictDataHIstoric["balance_days"] = 0
                
                if singleDataHistoric.get("do_qty") is not None:
                    historic_do_qty = singleDataHistoric.get("do_qty")
                else:
                    historic_do_qty = 0
                if historic_do_qty != 0:
                    dictDataHIstoric['percent_supply'] = round((singleDataHistoric.get('cumulative_challan_lr_qty') / int(float(historic_do_qty))) * 100, 2)
                else:
                    dictDataHIstoric['percent_supply'] = 0
                
                    
                dictDataHIstoric["balance_qty"] = round(int(float(historic_do_qty)) - singleDataHistoric.get("cumulative_challan_lr_qty"), 2)
                    
                if dictDataHIstoric['balance_days'] and dictDataHIstoric['balance_qty'] != 0:
                    dictDataHIstoric['asking_rate'] = round(dictDataHIstoric['balance_qty'] / dictDataHIstoric['balance_days'], 2)
                else:
                    dictDataHIstoric["asking_rate"] = 0
                
                do_no_exists = any(item['DO_No'] == dictDataHIstoric["DO_No"] for item in listData)

                if not do_no_exists:
                    console_logger.debug("DO_No does not exist in final_data.")
                    listData.append(dictDataHIstoric)


            for singleDataRcrData in fetchRcrRoadData:
                dictDataRcr = {}
                dictDataRcr["DO_No"] = singleDataRcrData.get("_id")
                dictDataRcr["mine_name"] = singleDataRcrData.get("mine_name")
                if singleDataRcrData.get("do_qty"):
                    dictDataRcr["DO_Qty"] = int(singleDataRcrData.get("do_qty"))
                else:
                    dictDataRcr["DO_Qty"] = 0
                dictDataRcr["cumulative_challan_lr_qty"] = round(singleDataRcrData.get("cumulative_challan_lr_qty"), 2)
                dictDataRcr["grn_status"] = singleDataRcrData.get("grn_status")
                dictDataRcr["date"] = singleDataRcrData.get("date").strftime("%Y-%m-%d")
                dictDataRcr["start_date"] = singleDataRcrData.get("start_date")
                dictDataRcr["end_date"] = singleDataRcrData.get("end_date")
                dictDataRcr["source_type"] = singleDataRcrData.get("type_consumer")
                dictDataRcr["slno"] = datetime.datetime.strptime(singleDataRcrData.get("slno"), "%Y%m").strftime("%B %Y") if singleDataRcrData.get("slno") else "-"
                if singleDataRcrData.get("Grade") is not None:
                    if '-' in singleDataRcrData.get("Grade"):
                        dictDataRcr["average_GCV_Grade"] = singleDataRcrData.get("Grade").split("-")[0]
                    elif " " in singleDataRcrData.get("Grade"):
                        dictDataRcr["average_GCV_Grade"] = singleDataRcrData.get("Grade").split(" ")[0]
                    else:
                        dictDataRcr["average_GCV_Grade"] = singleDataRcrData.get("Grade")
                if singleDataRcrData.get("start_date") is not None and singleDataRcrData.get("end_date") is not None:
                    tomorrow_date = datetime.datetime.strptime(singleDataRcrData.get("end_date"), "%Y-%m-%d").date() + datetime.timedelta(days=1)
                    balance_days = tomorrow_date - datetime.datetime.strptime(specified_date, "%Y-%m-%d").date()
                    dictDataRcr["balance_days"] = balance_days.days
                else:
                    dictDataRcr["balance_days"] = 0
                if singleDataRcrData.get("do_qty") is not None:
                    single_do_qty = singleDataRcrData.get("do_qty")
                else:
                    single_do_qty = 0
                if single_do_qty != 0:
                    dictDataRcr['percent_supply'] = round((singleDataRcrData.get('cumulative_challan_lr_qty') / int(single_do_qty)) * 100, 2)
                else:
                    dictDataRcr['percent_supply'] = 0
                    
                dictDataRcr["balance_qty"] = round(int(single_do_qty) - singleDataRcrData.get("cumulative_challan_lr_qty"), 2)
                    
                if dictDataRcr['balance_days'] and dictDataRcr['balance_qty'] != 0:
                    dictDataRcr['asking_rate'] = round(dictDataRcr['balance_qty'] / dictDataRcr['balance_days'], 2)
                else:
                    dictDataRcr["asking_rate"] = 0
                listData.append(dictDataRcr)
            

            for saprecordsSingle in fetchSapRecordsData:
                sapdict = {}
                sapdict["DO_No"] = saprecordsSingle.get("_id")
                sapdict["mine_name"] = saprecordsSingle.get("mine_name")
                sapdict["DO_Qty"] = int(saprecordsSingle.get("do_qty"))
                sapdict["start_date"] = saprecordsSingle.get("start_date")
                sapdict["end_date"] = saprecordsSingle.get("end_date")
                sapdict["source_type"] = saprecordsSingle.get("source_type")
                sapdict["challan_lr_qty"] = 0
                sapdict["cumulative_challan_lr_qty"] = 0
                sapdict["slno"] = datetime.datetime.strptime(saprecordsSingle.get("slno"), "%Y%m").strftime("%B %Y") if saprecordsSingle.get("slno") else "-"
                if saprecordsSingle.get("Grade") is not None:
                    if '-' in saprecordsSingle.get("Grade"):
                        sapdict["average_GCV_Grade"] = saprecordsSingle.get("Grade").split("-")[0]
                    elif " " in saprecordsSingle.get("Grade"):
                        sapdict["average_GCV_Grade"] = saprecordsSingle.get("Grade").split(" ")[0]
                    else:
                        sapdict["average_GCV_Grade"] = saprecordsSingle.get("Grade")
                if saprecordsSingle.get("start_date") is not None and saprecordsSingle.get("end_date") is not None:
                    tomorrow_date = datetime.datetime.strptime(saprecordsSingle.get("end_date"), "%Y-%m-%d").date() + datetime.timedelta(days=1)
                    balance_days = tomorrow_date - datetime.datetime.strptime(specified_date, "%Y-%m-%d").date()
                    sapdict["balance_days"] = balance_days.days
                else:
                    sapdict["balance_days"] = 0
                if saprecordsSingle.get("do_qty") is not None:
                    do_qty_val = saprecordsSingle.get("do_qty")
                else:
                    do_qty_val = 0
                if do_qty_val != 0:
                    sapdict['percent_supply'] = round((sapdict["cumulative_challan_lr_qty"] / int(do_qty_val)) * 100, 2)
                else:
                    sapdict['percent_supply'] = 0
                sapdict["balance_qty"] = round(int(do_qty_val) - sapdict["cumulative_challan_lr_qty"], 2)
                if sapdict['balance_days'] and sapdict['balance_qty'] != 0:
                    sapdict['asking_rate'] = round(sapdict['balance_qty'] / sapdict['balance_days'], 2)
                else:
                    sapdict["asking_rate"] = 0

                sap_do_no_exists = any(item['DO_No'] == sapdict.get("DO_No") for item in listData)
                
                if not sap_do_no_exists:
                    console_logger.debug("DO_No does not exist in final_data for sap_records")
                    listData.append(sapdict)
            

            for saprecordsRcrSingle in fetchSapRecordsRcrData:
                sapdictRcr = {}
                sapdictRcr["DO_No"] = saprecordsRcrSingle.get("_id")
                sapdictRcr["mine_name"] = saprecordsRcrSingle.get("mine_name")
                sapdictRcr["DO_Qty"] = int(saprecordsRcrSingle.get("do_qty"))
                sapdictRcr["start_date"] = saprecordsRcrSingle.get("start_date")
                sapdictRcr["end_date"] = saprecordsRcrSingle.get("end_date")
                sapdictRcr["source_type"] = saprecordsRcrSingle.get("source_type")
                sapdictRcr["challan_lr_qty"] = 0
                sapdictRcr["cumulative_challan_lr_qty"] = 0
                sapdictRcr["slno"] = datetime.datetime.strptime(saprecordsRcrSingle.get("slno"), "%Y%m").strftime("%B %Y") if saprecordsRcrSingle.get("slno") else "-"
                if saprecordsRcrSingle.get("Grade") is not None:
                    if '-' in saprecordsRcrSingle.get("Grade"):
                        sapdictRcr["average_GCV_Grade"] = saprecordsRcrSingle.get("Grade").split("-")[0]
                    elif " " in saprecordsRcrSingle.get("Grade"):
                        sapdictRcr["average_GCV_Grade"] = saprecordsRcrSingle.get("Grade").split(" ")[0]
                    else:
                        sapdictRcr["average_GCV_Grade"] = saprecordsRcrSingle.get("Grade")
                if saprecordsRcrSingle.get("start_date") is not None and saprecordsRcrSingle.get("end_date") is not None:
                    tomorrow_date = datetime.datetime.strptime(saprecordsRcrSingle.get("end_date"), "%Y-%m-%d").date() + datetime.timedelta(days=1)
                    balance_days = tomorrow_date - datetime.datetime.strptime(specified_date, "%Y-%m-%d").date()
                    sapdictRcr["balance_days"] = balance_days.days
                else:
                    sapdictRcr["balance_days"] = 0
                if saprecordsRcrSingle.get("do_qty") is not None:
                    do_qty_val = saprecordsRcrSingle.get("do_qty")
                else:
                    do_qty_val = 0
                if do_qty_val != 0:
                    sapdictRcr['percent_supply'] = round((sapdictRcr["cumulative_challan_lr_qty"] / int(do_qty_val)) * 100, 2)
                else:
                    sapdictRcr['percent_supply'] = 0
                sapdictRcr["balance_qty"] = round(int(do_qty_val) - sapdictRcr["cumulative_challan_lr_qty"], 2)
                if sapdictRcr['balance_days'] and sapdictRcr['balance_qty'] != 0:
                    sapdictRcr['asking_rate'] = round(sapdictRcr['balance_qty'] / sapdictRcr['balance_days'], 2)
                else:
                    sapdictRcr["asking_rate"] = 0

                sap_do_no_exists = any(item['DO_No'] == sapdictRcr.get("DO_No") for item in listData)
                
                if not sap_do_no_exists:
                    console_logger.debug("DO_No does not exist in final_data for sap_records")
                    listData.append(sapdictRcr)


            for singlelrqtyData in fetchGmrDatachallanltqty:
                dictDatalrQty = {}
                dictDatalrQty["DO_No"] = singlelrqtyData.get("_id")
                dictDatalrQty["mine_name"] = singlelrqtyData.get("mine_name")
                if singlelrqtyData.get("do_qty"):
                    dictDatalrQty["DO_Qty"] = int(float(singlelrqtyData.get("do_qty")))
                else:
                    dictDatalrQty["DO_Qty"] = 0
                dictDatalrQty["challan_lr_qty"] = round(singlelrqtyData.get("challan_lr_qty"), 2)
                
                # Check if there is an item with the same DO_No in listData
                do_no_exists_data = next((item for item in listData if item["DO_No"] == dictDatalrQty["DO_No"]), None)
                
                # If it exists, update the "challan_lr_qty" in listData
                if do_no_exists_data:
                    do_no_exists_data["challan_lr_qty"] = dictDatalrQty["challan_lr_qty"]

            for singlehistoriclrqty in fetchGmrHistoricDataChallanLrQty:
                dictDatahistoriclrQty = {}
                dictDatahistoriclrQty["DO_No"] = singlehistoriclrqty.get("_id")
                dictDatahistoriclrQty["mine_name"] = singlehistoriclrqty.get("mine_name")
                if singlehistoriclrqty.get("do_qty"):
                    dictDatahistoriclrQty["DO_Qty"] = int(float(singlehistoriclrqty.get("do_qty")))
                else:
                    dictDatahistoriclrQty["DO_Qty"] = 0
                dictDatahistoriclrQty["challan_lr_qty"] = round(singlehistoriclrqty.get("challan_lr_qty"), 2)
                
                # Check if there is an item with the same DO_No in listData
                do_no_exists_historic = next((item for item in listData if item["DO_No"] == dictDatahistoriclrQty["DO_No"]), None)
                
                # If it exists, update the "challan_lr_qty" in listData
                if do_no_exists_historic:
                    do_no_exists_historic["challan_lr_qty"] = dictDatahistoriclrQty["challan_lr_qty"]


            
            for singlercrroadlrqty in fetchRcrRoadchallanlrqty:
                dictDataRcrRoadlrQty = {}
                dictDataRcrRoadlrQty["DO_No"] = singlercrroadlrqty.get("_id")
                dictDataRcrRoadlrQty["mine_name"] = singlercrroadlrqty.get("mine_name")
                if singlercrroadlrqty.get("do_qty"):
                    dictDataRcrRoadlrQty["DO_Qty"] = int(float(singlercrroadlrqty.get("do_qty")))
                else:
                    dictDataRcrRoadlrQty["DO_Qty"] = 0
                dictDataRcrRoadlrQty["challan_lr_qty"] = round(singlercrroadlrqty.get("challan_lr_qty"), 2)
                
                # Check if there is an item with the same DO_No in listData
                do_no_exists_rcr_road = next((item for item in listData if item["DO_No"] == dictDataRcrRoadlrQty["DO_No"]), None)
                
                # If it exists, update the "challan_lr_qty" in listData
                if do_no_exists_rcr_road:
                    do_no_exists_historic["challan_lr_qty"] = dictDataRcrRoadlrQty["challan_lr_qty"]

            final_data_check = [
                d for d in listData
                if d['end_date'] is not None and 
                (datetime.datetime.strptime(d['end_date'], '%Y-%m-%d') + datetime.timedelta(days=2)) > datetime.datetime.strptime(specified_date, "%Y-%m-%d")
            ]

            # console_logger.debug(final_data_check)

            filtered_data = []
            for single_data_percent in final_data_check:
                percent_supply = single_data_percent.get('percent_supply')

                # Check for percent_supply greater than or equal to 100.0
                if percent_supply >= 100.0:
                    # Query Gmrdata with the DO_No
                    fetchGmrData = Gmrdata.objects(arv_cum_do_number=single_data_percent.get("DO_No")).first()
                    
                    # Check if data exists and GWEL_Tare_Time is not None
                    if fetchGmrData is not None and fetchGmrData.GWEL_Tare_Time:
                        # Compare GWEL_Tare_Time + 2 days with today's date
                        if (fetchGmrData.GWEL_Tare_Time + datetime.timedelta(days=2)) < datetime.datetime.now():
                            # console_logger.debug("Data removed due to GWEL_Tare_Time being older than today's date.")
                            continue  # Skip entry
                    
                # If not removed, append to filtered_data
                filtered_data.append(single_data_percent)
                
            # Sort the data by 'balance_days', placing entries with 'balance_days' of 0 at the end
            final_data_check = sorted(filtered_data, key=lambda x: (x['balance_days'] == 0, x['balance_days']))

            final_data = final_data_check
            if final_data:
                grouped_data = defaultdict(list)
                for single_data in final_data:
                    source_type = single_data.get("source_type").strip()
                    grouped_data[source_type].append(single_data)

                final_total_do_qty = 0
                final_total_challan_lr_qty = 0
                final_total_cc_lr_qty = 0
                final_total_balance_qty = 0

                per_data = ""
                per_data += "<table border='1'>"
                for source_type, entries in grouped_data.items():
                    # per_data += f"<span style='font-size: 10px; font-weight: 600'>{source_type}</span>"
                    per_data += f"<tr><td colspan='13' style='text-align: center'><b>{source_type}</b></span></td></tr>"
                    # per_data += "<table class='logistic_report_data' style='width: 100%; text-align: center; border-spacing: 0px; border: 1px solid lightgray;'>"
                    per_data += (
                        "<thead>"
                    )
                    per_data += "<tr>"
                    per_data += "<th>Month</th>"
                    per_data += "<th>Mine Name</th>"
                    per_data += "<th>DO No</th>"
                    per_data += "<th>Grade</th>"
                    per_data += "<th>DO Qty</th>"
                    per_data += "<th>Challan LR / Qty</th>"
                    per_data += "<th>C.C. LR / Qty</th>"
                    per_data += "<th>Balance Qty</th>"
                    per_data += "<th>% of Supply</th>"
                    per_data += "<th>Balance Days</th>"
                    per_data += "<th>Asking Rate</th>"
                    per_data += "<th>Do Start date</th>"
                    per_data += "<th>Do End date</th></tr></thead><tbody>"
                    total_do_qty = 0
                    total_challan_lr_qty = 0
                    total_cc_lr_qty = 0
                    total_balance_qty = 0

                    for entry in entries:
                        per_data += f"<tr>"
                        per_data += f"<td> {entry.get('slno')}</span></td>"
                        per_data += f"<td> {entry.get('mine_name')}</span></td>"
                        per_data += f"<td> {entry.get('DO_No')}</span></td>"
                        per_data += f"<td> {entry.get('average_GCV_Grade')}</span></td>"
                        per_data += f"<td> {round(entry.get('DO_Qty'), 2)}</span></td>"
                        total_do_qty += round(entry.get('DO_Qty'), 2)
                        if entry.get("challan_lr_qty"):
                            per_data += f"<td>{round(entry.get('challan_lr_qty'), 2)}</span></td>"
                            total_challan_lr_qty += round(entry.get('challan_lr_qty'), 2)
                        else:
                            per_data += f"<td>0</span></td>"
                        per_data += f"<td> {round(entry.get('cumulative_challan_lr_qty'), 2)}</span></td>"
                        total_cc_lr_qty += round(entry.get('cumulative_challan_lr_qty'), 2)
                        per_data += f"<td> {round(entry.get('balance_qty'), 2)}</span></td>"
                        total_balance_qty += round(entry.get('balance_qty'), 2)
                        per_data += f"<td> {round(entry.get('percent_supply'), 2)}%</span></td>"
                        per_data += f"<td> {entry.get('balance_days')}</span></td>"
                        per_data += f"<td> {round(entry.get('asking_rate'))}</span></td>"
                        if entry.get("start_date") != "0":
                            per_data += f"<td> {datetime.datetime.strptime(entry.get('start_date'),'%Y-%m-%d').strftime('%d %B %y')}</span></td>"
                        else:
                            per_data += f"<td>0</span></td>"
                        if entry.get("end_date") != "0":
                            per_data += f"<td> {datetime.datetime.strptime(entry.get('end_date'),'%Y-%m-%d').strftime('%d %B %y')}</span></td>"
                        else:    
                            per_data += f"<td>0</span></td>"
                        per_data += "</tr>"
                    per_data += "<tr>"
                    per_data += "<td colspan='4'><strong>Total</strong></td>"
                    per_data += f"<td><strong>{round(total_do_qty, 2)}</strong></td>"
                    per_data += f"<td><strong>{round(total_challan_lr_qty, 2)}</strong></td>"
                    per_data += f"<td><strong>{round(total_cc_lr_qty, 2)}</strong></td>"
                    per_data += f"<td><strong>{round(total_balance_qty, 2)}</strong></td>"
                    if total_cc_lr_qty != 0 and total_do_qty != 0:
                        per_data += f"<td><strong>{round(total_cc_lr_qty/total_do_qty, 2)}%</strong></td>"
                    else:
                        per_data += f"<td><strong>0%</strong></td>"
                    per_data += f"<td colspan='4'><strong></strong></td>"
                    per_data += "</tr>"
                    final_total_do_qty += total_do_qty
                    final_total_challan_lr_qty += total_challan_lr_qty
                    final_total_cc_lr_qty += total_cc_lr_qty
                    final_total_balance_qty += total_balance_qty
                per_data += "<tr>"
                per_data += "<td colspan='4'><strong>Grand Total</strong></td>"
                per_data += f"<td><strong>{round(final_total_do_qty, 2)}</strong></td>"
                per_data += f"<td><strong>{round(final_total_challan_lr_qty, 2)}</strong></td>"
                per_data += f"<td><strong>{round(final_total_cc_lr_qty, 2)}</strong></td>"
                per_data += f"<td><strong>{round(final_total_balance_qty, 2)}</strong></td>"
                per_data += f"<td><strong>{round(final_total_cc_lr_qty/final_total_do_qty, 2)}%</strong></td>"
                per_data += f"<td colspan='4'><strong></strong></td>"
                per_data += "</tr>"
                per_data += "</tbody></table>"
                return per_data
            else:
                return 404

        except Exception as e:
            # response.status_code = 400
            console_logger.debug(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
            console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
            return e
        

    
    def download_rail_coal_logistics(self, specified_date: str, mine: Optional[str] = "All"):
        try:
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

                    if final_data:
                        per_data = ""
                        per_data += "<table border='1'>"
                        per_data += "<thead>"
                        per_data += "<tr>"
                        per_data += "<th>Mine Name</th>"
                        per_data += "<th>RR No</th>"
                        per_data += "<th>Grade</th>"
                        per_data += "<th>RR Qty</th>"
                        per_data += "<th>Challan LR / Qty</th>"
                        per_data += "<th>C.C. LR / Qty</th>"
                        per_data += "<th>Balance Qty</th>"
                        per_data += "<th>% of Supply</th>"
                        per_data += "<th>Balance Days</th>"
                        per_data += "<th>Asking Rate</th>"
                        per_data += "<th>Do Start date</th>"
                        per_data += "<th>Do End date</th>"
                        per_data += "</tr>"
                        per_data += "</thead>"
                        per_data += "<tbody>"
                        for single_final_data in final_data:
                            per_data += "<tr>"
                            per_data += f"<td>{single_final_data.get('mine_name')}</td>"
                            per_data += f"<td>{single_final_data.get('rr_no')}</td>"
                            per_data += f"<td>{single_final_data.get('average_GCV_Grade')}</td>"
                            per_data += f"<td>{single_final_data.get('rr_qty')}</td>"
                            per_data += f"<td>{round(single_final_data.get('challan_lr_qty'), 2)}</td>"
                            per_data += f"<td>{round(single_final_data.get('cumulative_challan_lr_qty'), 2)}</td>"
                            per_data += f"<td>{round(single_final_data.get('balance_qty'), 2)}</td>"
                            per_data += f"<td>{round(single_final_data.get('percent_supply'), 2)}</td>"
                            per_data += f"<td>{single_final_data.get('balance_days')}</td>"
                            # per_data += f"<td>{single_final_data.get('date')}</td>"
                            per_data += f"<td>{round(single_final_data.get('asking_rate'), 2)}</td>"
                            per_data += f"<td>{single_final_data.get('start_date')}</td>"
                            per_data += f"<td>{single_final_data.get('end_date')}</td>"
                            per_data += "</tr>"
                        per_data += "</tbody>"
                        per_data += "</table>"

                        return per_data
                    else:
                        return 404


        except Exception as e:
            console_logger.debug(e)


    def display_pdf_report_bunker_addons(self, certificate_no):
        try:
            fetchBunkerSingleData = BunkerQualityAnalysis.objects.get(certificate_no=certificate_no)
            # fetchBunkerSingleData = BunkerData.objects.get(sample_details_id=sample_id)
            fetch_pdf_bunker = bunker_single_generate_report(fetchBunkerSingleData=fetchBunkerSingleData)
            return fetch_pdf_bunker
        except Exception as e:
            console_logger.debug(e)


    def update_schheduler_status(self, scheduler_name, active):
        try:
            updateSchedulerData = ReportScheduler.objects(
                report_name=scheduler_name,
            ).update(active=active)
            return {"detail": "success"}
        except Exception as e:
            console_logger.debug(e)

    
    def fetch_scheduler_status(self):
        try:
            listData = []
            fetchSchedulerData = ReportScheduler.objects()
            for single_data in fetchSchedulerData:
                listData.append(single_data.status_payload())
            return listData
        except Exception as e:
            console_logger.debug(e)

    def bunker_coal_analysis(self, specified_date):
        try:
            data={}
            UTC_OFFSET_TIMEDELTA = datetime.datetime.utcnow() - datetime.datetime.now()

            basePipeline = [
                {
                    '$match': {
                        'created_date': {
                            '$gte': None, 
                            '$lte': None,
                        }
                    }
                }, {
                    '$project': {
                        'ts': {
                            '$dayOfMonth': {
                                'date': '$created_date', 
                                'timezone': 'Asia/Kolkata'
                            }
                        }, 
                        'tagid': '$tagid', 
                        'sum': '$sum', 
                        '_id': 0
                    }
                }, {
                    '$group': {
                        '_id': {
                            'ts': '$ts', 
                            'tagid': '$tagid'
                        }, 
                        'data': {
                            '$push': '$sum'
                        }
                    }
                }
            ]
            date=specified_date
            end_date =f'{date} 23:59:59'
            start_date = f'{date} 00:00:00'
            format_data = "%Y-%m-%d %H:%M:%S"
            endd_date=datetime.datetime.strptime(end_date,format_data)
            startd_date=datetime.datetime.strptime(start_date,format_data)

            basePipeline[0]["$match"]["created_date"]["$lte"] = (endd_date)
            basePipeline[0]["$match"]["created_date"]["$gte"] = (startd_date)

            console_logger.debug(basePipeline)
            output = Historian.objects().aggregate(basePipeline)
            outputDict = {}
            for data in output:
                inputData = {}
                console_logger.debug(data)
                tagid = data.get("_id").get("tagid")
                value = data.get("data")[0]

                if tagid in {15274, 15275}:
                    console_logger.debug(f"Processing tagid: {tagid}, value: {value}")
                    
                    unit = "Unit1" if tagid == 15274 else "Unit2"
                    inputData["units"] = unit
                    inputData["tagid"] = str(tagid)
                    inputData["date"] = str(specified_date)
                    inputData["bunkering"] = str(int(float(value)) / 1000)
                    
                    bunkerAnalysis(**inputData).save()
                else:
                    console_logger.warning(f"Unexpected tagid: {tagid}")

            # console_logger.debug(inputData)

            # console_logger.debug(outputDict)
            return {"detail": "success"}
        except Exception as e:
            console_logger.debug(e)

    
    def bunker_coal_data(self, currentPage, perPage, start_timestamp, end_timestamp, search_text, type, date):
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

                data = Q()

                if date:
                    end_date =f'{date}T23:59:59'

                    start_date = f'{date}T00:00:00'
                    # endd_date=self.convert_to_utc_format(end_date,"%Y-%m-%dT%H:%M:%S")
                    # startd_date=self.convert_to_utc_format(start_date,"%Y-%m-%dT%H:%M:%S")
                    endd_date=datetime.datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%S") 
                    startd_date=datetime.datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S") 
                    date_query = Q(start_date__gte=startd_date) & Q(start_date__lte=endd_date)
                    data &= date_query

                if start_timestamp:
                    # start_date = self.convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
                    start_date = datetime.datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M")
                    data &= Q(start_date__gte = start_date)

                if end_timestamp:
                    # end_date = self.convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M","Asia/Kolkata",False)
                    end_date = datetime.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M")
                    data &= Q(start_date__lte = end_date)

                if search_text:
                    if search_text.isdigit():
                        data &= Q(tag_id__icontains=search_text)
                    else:
                        data &= (Q(units__icontains=search_text))

                offset = (page_no - 1) * page_len

                logs = (
                    bunkerAnalysis.objects(data)
                    .order_by("-created_date")
                    .skip(offset)
                    .limit(page_len)
                )
                if any(logs):
                    for log in logs:
                        payload = log.payload()
                        # result["labels"] = list(payload.keys())
                        result["labels"] = ["Sr.No", "shift_name", "unit", "bunkering", "mgcv", "hgcv", "ratio", "start_date", "Date"]
                        result["datasets"].append(log.payload())
                result["total"] = (len(bunkerAnalysis.objects(data)))
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
                    # start_date = self.convert_to_utc_format(start_timestamp, "%Y-%m-%dT%H:%M")
                    start_date = datetime.datetime.strptime(start_timestamp, "%Y-%m-%dT%H:%M")
                    data &= Q(start_date__gte = start_date)

                if end_timestamp:
                    # end_date = self.convert_to_utc_format(end_timestamp, "%Y-%m-%dT%H:%M","Asia/Kolkata",False)
                    end_date = datetime.datetime.strptime(end_timestamp, "%Y-%m-%dT%H:%M")
                    data &= Q(start_date__lte = end_date)
                
                if search_text:
                    if search_text.isdigit():
                        data &= Q(tag_id__icontains=search_text)
                    else:
                        data &= (Q(units__icontains=search_text))
                
                usecase_data = bunkerAnalysis.objects(data).order_by("-created_date")
                count = len(usecase_data)
                path = None
                if usecase_data:
                    try:
                        path = os.path.join(
                            "static_server",
                            "gmr_ai",
                            file,
                            "Bunker_analysis_{}.xlsx".format(
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
                                "Units",
                                "Tag Id", 
                                "Bunkering", 
                                "MGCV", 
                                "HGCV", 
                                "Ratio",
                                "Start Date",
                                "End Date",
                                # "Created At"
                                ]
                        for index, header in enumerate(headers):
                            worksheet.write(0, index, header, cell_format2)
                        for row, query in enumerate(usecase_data,start=1):
                            result = query.payload()
                            worksheet.write(row, 0, count, cell_format)     
                            worksheet.write(row, 1, str(result["units"]), cell_format)                     
                            worksheet.write(row, 2, str(result["tagid"]), cell_format)                     
                            worksheet.write(row, 3, str(result["bunkering"]), cell_format)                     
                            worksheet.write(row, 4, str(result["mgcv"]), cell_format)                     
                            worksheet.write(row, 5, str(result["hgcv"]), cell_format)                     
                            worksheet.write(row, 6, str(result["ratio"]), cell_format)                     
                            worksheet.write(row, 7, str(result["start_date"]), cell_format)                     
                            worksheet.write(row, 8, str(result["date"]), cell_format)                     
                            # worksheet.write(row, 9, str(result["created_at"]), cell_format)                     
                            count-=1
                        workbook.close()
                        console_logger.debug("sent data {}".format(path))

                        return {
                                "Type": "bunker_analysis_download_event",
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
                            "Type": "bunker_analysis_download_event",
                            "Datatype": "Report",
                            "File_Path": path,
                            }

        except Exception as e:
            console_logger.debug(e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
            console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
            return e


    def bunker_coal_table_email(self, specified_date):
        try:
            data={}
            UTC_OFFSET_TIMEDELTA = datetime.datetime.utcnow() - datetime.datetime.now()

            basePipeline = [
                {
                    '$match': {
                        'created_date': {
                            '$gte': None, 
                            '$lte': None,
                        }
                    }
                }, {
                    '$project': {
                        'ts': {
                            '$dayOfMonth': {
                                'date': '$created_date', 
                                'timezone': 'Asia/Kolkata'
                            }
                        }, 
                        'tagid': '$tagid', 
                        'sum': '$sum', 
                        '_id': 0
                    }
                }, {
                    '$group': {
                        '_id': {
                            'ts': '$ts', 
                            'tagid': '$tagid'
                        }, 
                        'data': {
                            '$push': '$sum'
                        }
                    }
                }
            ]
            date=specified_date
            end_date =f'{date} 23:59:59'
            start_date = f'{date} 00:00:00'
            format_data = "%Y-%m-%d %H:%M:%S"
            endd_date=datetime.datetime.strptime(end_date,format_data)
            startd_date=datetime.datetime.strptime(start_date,format_data)

            basePipeline[0]["$match"]["created_date"]["$lte"] = (endd_date)
            basePipeline[0]["$match"]["created_date"]["$gte"] = (startd_date)

            console_logger.debug(basePipeline)
            output = Historian.objects().aggregate(basePipeline)
            outputDict = {}

            for data in output:
                console_logger.debug(data)
                tagid = data.get("_id").get("tagid")
                value = data.get("data")[0]
                if tagid == 15274:
                    # outputDict[tagid] = value
                    outputDict["Unit1"] = value
                if tagid == 15275:
                    # outputDict[tagid] = value
                    outputDict["Unit2"] = value
                console_logger.debug(outputDict)
            if outputDict:
                outputDict2 = dict(sorted(outputDict.items(),key= lambda x:x[1]))
                html_data = "<table border='1'>"
                html_data += "<thead>"
                html_data += "<th>UNIT</th>"
                html_data += "<th>BUNKERING (MT)</th>"
                html_data += "<th>MGCV (MT)</th>"
                html_data += "<th>HGCV (MT)</th>"
                html_data += "<th>RATIO (MGCV/HGCV)</th>"
                html_data += "</thead><tbody>"
                for key, value in outputDict2.items():
                    html_data += f"<tr>"
                    html_data += f"<td>{key}</td>"
                    html_data += f"<td>{value}</td>"
                    html_data += f"<td>-</td>"
                    html_data += f"<td>-</td>"
                    html_data += f"<td>-</td>"
                    html_data += f"</tr>"
                html_data += "</tbody></table>"
                console_logger.debug(html_data)
                return html_data
            else:
                return 404
        except Exception as e:
            console_logger.debug(e)

    def update_coalbunker_analysis_data(self, payload):
        try:
            updateBunkerAnaylsisData = bunkerAnalysis.objects(
                id=ObjectId(payload.get("id")),
            ).update(mgcv=payload.get("mgcv"), hgcv=payload.get("hgcv"), ratio=payload.get("ratio"))
            return {"detail": "success"}
        except Exception as e:
            console_logger.debug(e)
    
    def insertShiftScheduler(self, shift_scheduler):
        try:
            try:
                SchedulerShifts.objects.get(scheduler_name = shift_scheduler)
            except DoesNotExist as e:
                SchedulerShifts(scheduler_name = shift_scheduler).save()
            return {"detail": "success"}
        except Exception as e:
            console_logger.debug(e)
    
    def fetchShiftScheduler(self):
        try:
            dataList = []
            fetchSchedulerShifts = SchedulerShifts.objects()
            if fetchSchedulerShifts:
                for single_fetchSchdulerShifts in fetchSchedulerShifts:
                    dataList.append(single_fetchSchdulerShifts.payload())
            return dataList
        except Exception as e:
            console_logger.debug(e)

    def fetch_coal_quality_gcv(self):
        try:
            console_logger.debug("coal_quality_gcv hitted")
            headers = {
                'accept': 'application/json',
            }
            response = requests.get(f'http://{ip}/api/v1/host/fetch_coal_gcv_quality', headers=headers)
            # console_logger.debug(response.status_code)
            if response.status_code == 200:
                fetchDetail = response.json()
                if fetchDetail:
                    for single_excel_data in fetchDetail:
                        console_logger.debug(f"Sample name: {single_excel_data[1]['Unnamed: 4']}")
                        console_logger.debug(f"Result (Ho): {single_excel_data[1]['Unnamed: 8']}")
                        if "/" in str(single_excel_data[1]['Unnamed: 4']) and "," not in str(single_excel_data[1]['Unnamed: 4']):
                            splitDataname = re.sub("\s\s+", " ", single_excel_data[1]['Unnamed: 4']).split("/")
                            doNo = splitDataname[0]
                            pattern = r'\b(LT|R|LOT-|LOT|LT-|R-)\s?\d+\b'
                            secondData = splitDataname[1].split(" ")
                            location = secondData[0]
                            match = re.search(pattern, splitDataname[1])
                            rakeNo = match.group()
                            console_logger.debug(doNo)
                            console_logger.debug(rakeNo)
                            if "LT" in rakeNo or "LOT" in rakeNo:
                                console_logger.debug("inside road")
                                splitData = rakeNo[-2:]
                                checkRoadTesting = CoalTesting.objects(rrNo=doNo, rake_no=f"LOT-{splitData.strip()}")
                                if checkRoadTesting:
                                    for single_road_data in checkRoadTesting:
                                        for oneData in single_road_data.parameters:
                                            if oneData.get("parameter_Name") == "Gross_Calorific_Value_(Adb)":
                                                oneData["val1"] = str(single_excel_data[1]['Unnamed: 8'])
                                        single_road_data.save()
                            elif "R" in rakeNo:
                                console_logger.debug("inside rail")
                                splitData = rakeNo[-2:]
                                console_logger.debug(int(splitData.strip()))
                                checkRailTesting = CoalTestingTrain.objects(rrNo=doNo, rake_no=f"{splitData.strip()}")
                                if checkRailTesting:
                                    for single_rail_data in checkRailTesting:
                                        for oneData in single_rail_data.parameters:
                                            if oneData.get("parameter_Name") == "Gross_Calorific_Value_(Adb)":
                                                oneData["val1"] = str(single_excel_data[1]['Unnamed: 8'])
                                        single_rail_data.save()
                    return {"detail": "success"}
                else:
                    return {"detail":"no data found"}
        except Exception as e:
            console_logger.debug(e)

    def get_financial_year_dates_from_todays(self, input_date):
        try:
            if input_date.month >= 4:
                start_date = date(input_date.year, 4, 1)
            else:
                start_date = date(input_date.year - 1, 4, 1)
            
            end_date = input_date
            
            return start_date, end_date
        except Exception as e:
            console_logger.debug(e)
    
    def get_financial_year_final(self, input_date):
        try:
            if input_date.month >= 4:
                start_year = input_date.year
                end_year = input_date.year + 1
            # If the month is before April, the financial year started the previous year
            # and ends in the current year.
            else:
                start_year = input_date.year - 1
                end_year = input_date.year
            
            return f"{start_year}-{str(end_year)[-2:]}"
        except Exception as e:
            console_logger.debug(e)
    
    def findExcelColumnTitleFromColumnNumber(self, n):
        try:
            arr = []
            while n > 0:
                remainder = n % 26
                if remainder == 0:
                    arr.append('Z')
                    n = (n // 26) - 1
                else:
                    arr.append(chr((remainder - 1) + ord('A')))
                    n = n // 26
            return ''.join(reversed(arr))
        except Exception as e:
            console_logger.debug(e)
    
    def findSingleRailDataThroughRRNo(self, rr_no, response):
        try:
            try:
                console_logger.debug("inside saprecords rail")
                fetchRailData = None
                fetchSapRecordsRail = None

                try:
                    fetchRailData = RailData.objects.get(rr_no=rr_no)
                except DoesNotExist as e:
                    pass

                try:
                    fetchSapRecordsRail = sapRecordsRail.objects.get(rr_no=rr_no)
                except DoesNotExist as e:
                    pass

                payload = {}

                if fetchRailData:
                    payload.update(fetchRailData.averyPayload())

                if fetchSapRecordsRail:
                    anotherPay = fetchSapRecordsRail.anotherPayload()
                    payload.update({
                        "invoice_date": anotherPay.get("invoice_date"),
                        "invoice_no": anotherPay.get("invoice_no"),
                        "sale_date": anotherPay.get("sale_date"),
                        "sizing_charges": anotherPay.get("sizing_charges"),
                        "evac_facility_charge": anotherPay.get("evac_facility_charge"),
                        "royality_charges": anotherPay.get("royality_charges"),
                        "nmet_charges": anotherPay.get("nmet_charges"),
                        "dmf": anotherPay.get("dmf"),
                        "adho_sanrachna_vikas": anotherPay.get("adho_sanrachna_vikas"),
                        "pariyavaran_upkar": anotherPay.get("pariyavaran_upkar"),
                        "assessable_value": anotherPay.get("assessable_value"),
                        "igst": anotherPay.get("igst"),
                        "gst_comp_cess": anotherPay.get("gst_comp_cess"),
                        "gross_bill_value": anotherPay.get("gross_bill_value"),
                        "less_underloading_charges": anotherPay.get("less_underloading_charges"),
                        "net_value": anotherPay.get("net_value"),
                        "total_amount": anotherPay.get("total_amount"),
                    })

                try:
                    fetchGrnData = Grn.objects.get(do_no=rr_no)
                    payload["is_grn_booked"] = True
                except DoesNotExist as e:
                    payload["is_grn_booked"] = False

                if payload:
                    return payload
                else:
                    raise DoesNotExist("No data found in both RailData and sapRecordsRail.")
            except Exception as e:
                console_logger.error(f"Error while fetching data: {e}")
                raise

                # response.status_code = 404
                # return {"details": "no data found"}
        except Exception as e:
            console_logger.debug("----- Error -----",e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
            console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
            return e
    

DataExecutionsHandler = DataExecutions()