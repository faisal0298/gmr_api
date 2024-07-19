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
from datetime import timedelta
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
            
            console_logger.debug(url)
            try:
                payload = {}
                headers = {}

                response = requests.request("GET", url, headers=headers, data=payload)
                console_logger.debug(response.status_code)
                data = response.json()
                if response.status_code == 200:
                    console_logger.debug("inside here")
                    sample_list_data = []
                    for single_data in data["responseData"]:
                        try:
                            fetchBunkerData = BunkerData.objects.get(sample_details_id=single_data.get("sample_Details_Id"))
                        except DoesNotExist as e:
                            if single_data.get("sample_Desc") == "Bunker U#02":
                                console_logger.debug(single_data["sample_Desc"])
                                sample_list_data = []
                                for final_single_data in single_data.get("sample_Parameters", []):
                                    bunker_sample_para = {
                                        "sample_details_id": final_single_data.get("sample_Details_Id"),
                                        "parameters_id": final_single_data.get("parameters_Id"),
                                        "parameter_name": final_single_data.get("parameter_Name"),
                                        "unit_val": final_single_data.get("unit_Val"),
                                        "test_method": final_single_data.get("test_Method"),
                                        "val1": final_single_data.get("val1"),
                                        "parameter_type": final_single_data.get("parameter_Type"),
                                    }
                                    sample_list_data.append(bunker_sample_para)

                                bunkerData = BunkerData(
                                    sample_details_id=single_data.get("sample_Details_Id"),
                                    work_order_id=single_data.get("work_Order_Id"),
                                    test_report_no=single_data.get("test_Report_No"),
                                    ulr_no = single_data.get("ulrNo"),
                                    test_report_date=single_data.get("test_Report_Date"),
                                    sample_id_no=single_data.get("sample_Id_No"),
                                    sample_desc=single_data.get("sample_Desc"),
                                    # rake_no=single_data.get("rake_No"),
                                    # rrNo=single_data.get("rrNo"),
                                    rR_Qty=single_data.get("rR_Qty"),
                                    supplier=single_data.get("supplier"),
                                    received_condition=single_data.get("received_Condition"),
                                    from_sample_condition_date=single_data.get("from_Sample_Collection_Date"),
                                    to_sample_condition_date=single_data.get("to_Sample_Collection_Date"),
                                    sample_received_date=single_data.get("sample_Received_Date"),
                                    sample_date=single_data.get("sample_Date"),
                                    analysis_date=single_data.get("analysis_Date"),
                                    sample_qty=single_data.get("sample_Qty"),
                                    # method_reference=single_data.get("method_Reference"),
                                    humidity=single_data.get("humidity"),
                                    test_temp=single_data.get("test_Temp"),
                                    sample_parameters=sample_list_data,
                                )
                                bunkerData.save()
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
                logs = (
                    BunkerData.objects(data)
                    .order_by("-ID")
                    .skip(offset)
                    .limit(page_len)
                )   
                if any(logs):
                    for log in logs:
                        # result["labels"] = list(log.simplepayload().keys())
                        result["labels"] = [
                            "sample_details_id",
                            "work_order_id",
                            "test_report_no",
                            "ulr_no",
                            "test_report_date",
                            "sample_id_no",
                            "sample_desc",
                            "rR_Qty",
                            "supplier",
                            "received_condition",
                            "from_sample_condition_date",
                            "to_sample_condition_date",
                            "sample_received_date",
                            "sample_date",
                            "analysis_date",
                            "sample_qty",
                            "humidity",
                            "test_temp",
                            "created_at",
                            "Inherent_Moisture_(Adb)",
                            "Ash_(Adb)",
                            "Volatile_Matter_(Adb)",
                            "Gross_Calorific_Value_(Adb)",
                            "Total_Moisture_(Arb)",
                            "Volatile_Matter_(Arb)",
                            "Ash_(Arb)",
                            "Fixed_Carbon_(Arb)",
                            "Gross_Calorific_Value_(Arb)",
                        ]
                        updated_payload = log.simplepayload()
                        for single_under_log in log["sample_parameters"]:
                            # console_logger.debug(single_under_log.parameter_type)
                            # console_logger.debug(single_under_log.val1)
                            if single_under_log.parameter_type == "AirDryBasis_IM":
                                updated_payload.update({"Inherent_Moisture_(Adb)": single_under_log.val1})
                            if single_under_log.parameter_type == "AirDryBasis_Ash":
                                updated_payload.update({"Ash_(Adb)": single_under_log.val1})
                            if single_under_log.parameter_type == "AirDryBasis_VM":
                                updated_payload.update({"Volatile_Matter_(Adb)": single_under_log.val1})
                            if single_under_log.parameter_type == "AirDryBasis_GCV":
                                updated_payload.update({"Gross_Calorific_Value_(Adb)": single_under_log.val1})
                            if single_under_log.parameter_type == "ReceivedBasis_TM":
                                updated_payload.update({"Total_Moisture_(Arb)": single_under_log.val1})
                            if single_under_log.parameter_type == "ReceivedBasis_VM":
                                updated_payload.update({"Volatile_Matter_(Arb)": single_under_log.val1})
                            if single_under_log.parameter_type == "ReceivedBasis_ASH":
                                updated_payload.update({"Ash_(Arb)": single_under_log.val1})
                            if single_under_log.parameter_type == "ReceivedBasis_FC":
                                updated_payload.update({"Fixed_Carbon_(Arb)": single_under_log.val1})
                            if single_under_log.parameter_type == "ReceivedBasis_GCV":
                                updated_payload.update({"Gross_Calorific_Value_(Arb)": single_under_log.val1})
                        result["datasets"].append(updated_payload)
                result["total"]= len(BunkerData.objects(data))
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

                usecase_data = BunkerData.objects(data).order_by("-created_at")
                count = len(usecase_data)
                path = None
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
                            "Sample Details Id",
                            "Work Order Id",
                            "Test Report No",
                            "ULR No",
                            "Test Report Date",
                            "Sample ID No",
                            "Sample Desc",
                            "RR Qty",
                            "Supplier",
                            "Received Condition",
                            "From Sample Condition Date",
                            "To Sample Condition Date",
                            "Sample Received Date",
                            "Sample Date",
                            "Analysis Date",
                            "Sample Qty",
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
                            "Created At"
                        ]
                    
                        for index, header in enumerate(headers):
                            worksheet.write(0, index, header, cell_format2)

                        for row, query in enumerate(usecase_data, start=1):
                            result = query.payload()
                            worksheet.write(row, 0, count, cell_format)     
                            worksheet.write(row, 1, str(result["sample_details_id"]))                      
                            worksheet.write(row, 2, str(result["work_order_id"]))                      
                            worksheet.write(row, 3, str(result["test_report_no"]))                      
                            worksheet.write(row, 4, str(result["ulr_no"]))                      
                            worksheet.write(row, 5, str(result["test_report_date"]))                      
                            worksheet.write(row, 6, str(result["sample_id_no"]))                      
                            worksheet.write(row, 7, str(result["sample_desc"]))                  
                            worksheet.write(row, 8, str(result["rR_Qty"]))                      
                            worksheet.write(row, 9, str(result["supplier"]))                      
                            worksheet.write(row, 10, str(result["received_condition"]))                      
                            worksheet.write(row, 11, str(result["from_sample_condition_date"]))                      
                            worksheet.write(row, 12, str(result["to_sample_condition_date"]))                      
                            worksheet.write(row, 13, str(result["sample_received_date"]))                      
                            worksheet.write(row, 14, str(result["sample_date"]))                      
                            worksheet.write(row, 15, str(result["analysis_date"]))                      
                            worksheet.write(row, 16, str(result["sample_qty"]))                     
                            worksheet.write(row, 17, str(result["humidity"]))                      
                            worksheet.write(row, 18, str(result["test_temp"]))
                            for single_under_log in result["sample_parameters"]:
                                console_logger.debug(single_under_log)
                                if single_under_log.get("parameter_type") == "AirDryBasis_IM":
                                    worksheet.write(row, 19, single_under_log.get("val1"))
                                if single_under_log.get("parameter_type") == "AirDryBasis_Ash":
                                    worksheet.write(row, 20, single_under_log.get("val1"))
                                if single_under_log.get("parameter_type") == "AirDryBasis_VM":
                                    worksheet.write(row, 21, single_under_log.get("val1"))
                                if single_under_log.get("parameter_type") == "AirDryBasis_GCV":
                                    worksheet.write(row, 22, single_under_log.get("val1"))
                                if single_under_log.get("parameter_type") == "ReceivedBasis_TM":
                                    worksheet.write(row, 23, single_under_log.get("val1"))
                                if single_under_log.get("parameter_type") == "ReceivedBasis_VM":
                                    worksheet.write(row, 24, single_under_log.get("val1"))
                                if single_under_log.get("parameter_type") == "ReceivedBasis_ASH":
                                    worksheet.write(row, 25, single_under_log.get("val1"))
                                if single_under_log.get("parameter_type") == "ReceivedBasis_FC":
                                    worksheet.write(row, 26, single_under_log.get("val1"))
                                if single_under_log.get("parameter_type") == "ReceivedBasis_GCV":
                                    worksheet.write(row, 27, single_under_log.get("val1"))  
                            worksheet.write(row, 28, str(result["created_at"]))               
                            
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
            console_logger.debug(filter_type)
            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)
            
            data = Q()

            if start_date:
                start_date = self.convert_to_utc_format(start_date, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__gte = start_date)

            if end_date:
                end_date = self.convert_to_utc_format(end_date, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__lte = end_date)

            usecase_data = CoalTesting.objects(data).order_by("-receive_date")
            count = len(usecase_data)
            path = None
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
                        console_logger.debug(result)
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
            console_logger.debug(filter_type)
            file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
            target_directory = f"static_server/gmr_ai/{file}"
            os.umask(0)
            os.makedirs(target_directory, exist_ok=True, mode=0o777)

            data = Q()

            if start_date:
                start_date = self.convert_to_utc_format(start_date, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__gte = start_date)

            if end_date:
                end_date = self.convert_to_utc_format(end_date, "%Y-%m-%dT%H:%M")
                data &= Q(receive_date__lte = end_date)

            usecase_data = CoalTestingTrain.objects(data).order_by("-receive_date")
            count = len(usecase_data)
            path = None
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
        
    def download_road_coal_logistics(self, specified_date: str, mine: Optional[str] = "All"):
        try:
            if specified_date:
                data = {}
                # result = {
                #     "labels": [],
                #     "datasets": [],
                #     "weight_total": [],
                #     "total": 0,
                #     "page_size": 15,
                # }

                if mine and mine != "All":
                    data["mine__icontains"] = mine.upper()

                # specified_change_date = datetime.datetime.strptime(specified_date, "%Y-%m-%d")
                specified_change_date = self.convert_to_utc_format(specified_date, "%Y-%m-%d")

                start_of_month = specified_change_date.replace(day=1)

                start_date = datetime.datetime.strftime(start_of_month, '%Y-%m-%d')
                end_date = datetime.datetime.strftime(specified_change_date, '%Y-%m-%d')

                logs = (
                    Gmrdata.objects()
                    .order_by("mine", "arv_cum_do_number", "-created_at")
                )
                # coal_testing = CoalTesting.objects(receive_date__gte=start_date, receive_date__lte=end_date).order_by("-ID")
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


                    start_dates = {}
                    grade = 0
                    for log in logs:
                        if log.vehicle_in_time!=None:
                            month = log.vehicle_in_time.strftime("%Y-%m")
                            date = log.vehicle_in_time.strftime("%Y-%m-%d")
                            payload = log.payload()
                            # result["labels"] = list(payload.keys())
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
                                aggregated_data[date][do_no]["DO_Qty"] = float(payload["DO_Qty"])
                            # if payload.get("DO_Qty"):
                            #     aggregated_data[date][do_no]["DO_Qty"] += float(
                            #         payload["DO_Qty"]
                            #     )
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
                                    "grade": grade,
                                    "date": date,
                                }
                                for do_no, data in aggregated_data[date].items()
                            },
                        }
                        for date in aggregated_data
                    ]
                    final_data = []
                    if specified_date:
                        filtered_data = [
                            entry for entry in dataList if entry["date"] == specified_date
                        ]
                        if filtered_data:
                            data = filtered_data[0]["data"]
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
                                dictData['average_GCV_Grade'] = values["grade"] 
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
                    
                                # append data
                                final_data.append(dictData)
                        # console_logger.debug(final_data)

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
                    

                    console_logger.debug(final_data)
                    if final_data:
                        per_data = ""
                        per_data += "<table border='1'>"
                        per_data += "<thead>"
                        per_data += "<tr>"
                        per_data += "<th>Mine Name</th>"
                        per_data += "<th>DO No</th>"
                        per_data += "<th>Grade</th>"
                        per_data += "<th>DO Qty</th>"
                        per_data += "<th>Challan LR Qty</th>"
                        per_data += "<th>C.C. LR Qty</th>"
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
                            console_logger.debug(single_final_data)
                            per_data += "<tr>"
                            per_data += f"<td>{single_final_data.get('mine_name')}</td>"
                            per_data += f"<td>{single_final_data.get('DO_No')}</td>"
                            per_data += f"<td>{single_final_data.get('average_GCV_Grade')}</td>"
                            per_data += f"<td>{single_final_data.get('DO_Qty')}</td>"
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

                        console_logger.debug(per_data)
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
                            if payload.get("source"):
                                aggregated_data[date][rr_no]["source"] = payload[
                                    "source"
                                ]
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
                        # return final_data
                    
                    console_logger.debug(final_data)
                    if final_data:
                        per_data = ""
                        per_data += "<table border='1'>"
                        per_data += "<thead>"
                        per_data += "<tr>"
                        per_data += "<th>Mine Name</th>"
                        per_data += "<th>RR No</th>"
                        per_data += "<th>Grade</th>"
                        per_data += "<th>RR Qty</th>"
                        per_data += "<th>Challan LR Qty</th>"
                        per_data += "<th>C.C. LR Qty</th>"
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
                            console_logger.debug(single_final_data)
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

                        console_logger.debug(per_data)
                        return per_data
                    else:
                        return 404


        except Exception as e:
            console_logger.debug(e)


    def display_pdf_report_bunker_addons(self, sample_id):
        try:
            fetchBunkerSingleData = BunkerData.objects.get(sample_details_id=sample_id)
            fetch_pdf_bunker = bunker_single_generate_report(fetchBunkerSingleData=fetchBunkerSingleData)
            return fetch_pdf_bunker
        except Exception as e:
            console_logger.debug(e)

DataExecutionsHandler = DataExecutions()