from fastapi import Response
from typing import Optional
from helpers.logger import console_logger
import os, sys
import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from database.models import *
import copy
from functools import lru_cache


def convert_to_utc_format(date_time, format, timezone= "Asia/Kolkata",start = True):
    to_zone = tz.gettz(timezone)
    _datetime = datetime.datetime.strptime(date_time, format)

    if not start:
        _datetime =_datetime.replace(hour=23,minute=59)
    return _datetime.replace(tzinfo=to_zone).astimezone(datetime.timezone.utc).replace(tzinfo=None)


UTC_OFFSET_TIMEDELTA = datetime.datetime.utcnow() - datetime.datetime.now()


def global_coal_analysis(                                  # contains graph, table & infocard
    type: Optional[str] = "Daily",
    Month: Optional[str] = None,
    Daily: Optional[str] = None,
    Year: Optional[str] = None
):
    try:
        basePipeline = [
            {
                "$match": {"created_date": {"$gte": None}},
            },
            {"$sort": {"created_date": -1}},
            {
                "$group": {
                    "_id": {
                        "ts": None,
                        "tagid": "$tagid"
                    },
                    "latest_sum": {"$first": "$sum"}
                }
            },
            {
                "$project": {
                    "ts": "$_id.ts",
                    "tagid": "$_id.tagid",
                    "sum": "$latest_sum",
                    "_id": 0
                }
            },
            {
                "$group": {
                    "_id": {"ts": "$ts", "tagid": "$tagid"},
                    "data": {"$push": "$sum"}
                }
            }
        ]

        if type == "Daily":
            date = Daily
            start_date = datetime.datetime.strptime(f"{date} 00:00:00", "%Y-%m-%d %H:%M:%S")
            end_date = datetime.datetime.strptime(f"{date} 23:59:59", "%Y-%m-%d %H:%M:%S")
            basePipeline[0]["$match"]["created_date"].update({"$gte": start_date, "$lte": end_date})
            basePipeline[2]["$group"]["_id"]["ts"] = {"$hour": "$created_date"}
            result = {
                "data": {
                    "labels": [str(i) for i in range(24)],
                    "generation_dataset": [
                        {"label": "Unit 1", "data": [0 for _ in range(24)]},  # tagid_2     (generation)
                        {"label": "Unit 2", "data": [0 for _ in range(24)]}   # tagid_3536  (generation)
                    ],
                    "consumption_dataset": [
                        {"label": "Unit 1", "data": [0 for _ in range(24)]},  # tagid_16    (consumption)  
                        {"label": "Unit 2", "data": [0 for _ in range(24)]}   # tagid_3538  (consumption)
                    ]
                }
            }

        elif type == "Week":
            basePipeline[0]["$match"]["created_date"]["$gte"] = (
                datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                + UTC_OFFSET_TIMEDELTA
                - datetime.timedelta(days=7)
            )
            basePipeline[2]["$group"]["_id"]["ts"] = {"$dayOfMonth": "$created_date"}
            result = {
                "data": {
                    "labels": [
                        (
                            basePipeline[0]["$match"]["created_date"]["$gte"]
                            + datetime.timedelta(days=i + 1)
                        ).strftime("%d")
                        for i in range(1, 8)
                    ],
                    "generation_dataset": [
                        {"label": "Unit 1", "data": [0 for i in range(1, 8)]},              # unit 1 = tagid_2    (generation)
                        {"label": "Unit 2", "data": [0 for i in range(1, 8)]},              # unit 2 = tagid_3536 (generation)
                    ],
                    "consumption_dataset": [
                        {"label": "Unit 1", "data": [0 for i in range(1, 8)]},              # unit 1 = tagid_16   (consumption)
                        {"label": "Unit 2", "data": [0 for i in range(1, 8)]},              # unit 2 = tagid_3538 (consumption)
                    ],
                }
            }

        elif type == "Month":

            date=Month
            format_data = "%Y-%m-%d"

            start_date = f'{date}-01'
            startd_date=datetime.datetime.strptime(start_date,format_data)
            end_date = startd_date + relativedelta( day=31)
            end_label = (end_date).strftime("%d")

            basePipeline[0]["$match"]["created_date"]["$lte"] = (end_date)
            basePipeline[0]["$match"]["created_date"]["$gte"] = (startd_date)
            basePipeline[2]["$group"]["_id"]["ts"] = {"$dayOfMonth": "$created_date"}
            result = {
                "data": {
                    "labels": [
                        (
                            basePipeline[0]["$match"]["created_date"]["$gte"]
                            + datetime.timedelta(days=i + 1)
                        ).strftime("%d")
                        for i in range(-1, (int(end_label))-1)
                    ],
                    "generation_dataset": [
                        {"label": "Unit 1", "data": [0 for i in range(-1, (int(end_label))-1)]},        # unit 1 = tagid_2    (generation)
                        {"label": "Unit 2", "data": [0 for i in range(-1, (int(end_label))-1)]},        # unit 2 = tagid_3536 (generation)
                    ],
                    "consumption_dataset": [
                        {"label": "Unit 1", "data": [0 for i in range(-1, (int(end_label))-1)]},        # unit 1 = tagid_16   (consumption)
                        {"label": "Unit 2", "data": [0 for i in range(-1, (int(end_label))-1)]},        # unit 2 = tagid_3538 (consumption)
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

            basePipeline[0]["$match"]["created_date"]["$lte"] = (endd_date)
            basePipeline[0]["$match"]["created_date"]["$gte"] = (startd_date)

            basePipeline[2]["$group"]["_id"]["ts"] = {"$month": "$created_date"}
            result = {
                "data": {
                    "labels": [
                        (
                            basePipeline[0]["$match"]["created_date"]["$gte"]
                            + relativedelta(months=i)
                        ).strftime("%m")
                        for i in range(0, 12)
                    ],
                    "generation_dataset": [
                        {"label": "Unit 1", "data": [0 for i in range(0, 12)]},                     # unit 1 = tagid_2    (generation)
                        {"label": "Unit 2", "data": [0 for i in range(0, 12)]},                     # unit 2 = tagid_3536 (generation)
                    ],
                    "consumption_dataset": [
                        {"label": "Unit 1", "data": [0 for i in range(0, 12)]},                     # unit 1 = tagid_16   (consumption)
                        {"label": "Unit 2", "data": [0 for i in range(0, 12)]},                     # unit 2 = tagid_3538 (consumption)
                    ],
                }
            }

        output = Historian.objects().aggregate(basePipeline)
        outputDict = {}
        for data in output:
            ts = data["_id"]["ts"]
            tag_id = data["_id"]["tagid"]
            sum_list = [float(item) for item in data.get('data', []) if item]
            if sum_list:
                outputDict.setdefault(ts, {}).setdefault(tag_id, []).extend(sum_list)

        modified_labels = [i for i in range(0, 24)]
        total_consumption_unit1 = 0
        total_consumption_unit2 = 0
        total_generation_unit1 = 0
        total_generation_unit2 = 0
        non_zero_gen_count_unit1 = 0
        non_zero_gen_count_unit2 = 0
        non_zero_con_count_unit1 = 0
        non_zero_con_count_unit2 = 0

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

            if int(label.split("-")[0]) in outputDict:
                for tag, values in outputDict[int(label.split("-")[0])].items():
                    avg_val = sum(values) / len(values) if values else 0

                    if tag == 2:  # Unit 1 generation
                        result["data"]["generation_dataset"][0]["data"][index] = round(avg_val, 2)
                        total_generation_unit1 += avg_val
                        if avg_val > 0:
                            non_zero_gen_count_unit1 += 1
                    elif tag == 3536:  # Unit 2 generation
                        result["data"]["generation_dataset"][1]["data"][index] = round(avg_val, 2)
                        total_generation_unit2 += avg_val
                        if avg_val > 0:
                            non_zero_gen_count_unit2 += 1

                    if tag == 16:  # Unit 1 consumption
                        result["data"]["consumption_dataset"][0]["data"][index] = round(avg_val, 2)
                        total_consumption_unit1 += avg_val
                        if avg_val > 0:
                            non_zero_con_count_unit1 += 1
                    elif tag == 3538:  # Unit 2 consumption
                        result["data"]["consumption_dataset"][1]["data"][index] = round(avg_val, 2)
                        total_consumption_unit2 += avg_val
                        if avg_val > 0:
                            non_zero_con_count_unit2 += 1

        result["data"]["specific_coal_consumption"] = {
            "Unit 1": [0 for _ in result["data"]["labels"]],
            "Unit 2": [0 for _ in result["data"]["labels"]]
        }

        for index in range(len(result["data"]["labels"])):
            generation_unit1 = result["data"]["generation_dataset"][0]["data"][index]
            generation_unit2 = result["data"]["generation_dataset"][1]["data"][index]
            consumption_unit1 = result["data"]["consumption_dataset"][0]["data"][index]
            consumption_unit2 = result["data"]["consumption_dataset"][1]["data"][index]

            # Specific coal for Unit 1
            if generation_unit1 > 0:
                result["data"]["specific_coal_consumption"]["Unit 1"][index] = round(consumption_unit1 / generation_unit1, 2)

            # Specific coal for Unit 2
            if generation_unit2 > 0:
                result["data"]["specific_coal_consumption"]["Unit 2"][index] = round(consumption_unit2 / generation_unit2, 2)


        # Calculate total averages only using non-zero values
        total_avg_generation_unit1 = round(total_generation_unit1 / non_zero_gen_count_unit1, 2) if non_zero_gen_count_unit1 > 0 else 0
        total_avg_generation_unit2 = round(total_generation_unit2 / non_zero_gen_count_unit2, 2) if non_zero_gen_count_unit2 > 0 else 0
        total_avg_consumption_unit1 = round(total_consumption_unit1 / non_zero_con_count_unit1, 2) if non_zero_con_count_unit1 > 0 else 0
        total_avg_consumption_unit2 = round(total_consumption_unit2 / non_zero_con_count_unit2, 2) if non_zero_con_count_unit2 > 0 else 0

        # Calculate specific coal and add totals
        specific_coal_unit1 = round(total_consumption_unit1 / total_generation_unit1, 2) if total_generation_unit1 > 0 else 0
        specific_coal_unit2 = round(total_consumption_unit2 / total_generation_unit2, 2) if total_generation_unit2 > 0 else 0

        result["data"]["total_average"] = {
                    "generation_unit1": {
                        "sum":total_generation_unit1,
                        "value": total_avg_generation_unit1,
                        "title": "Unit 1 Average Generation(MW)",
                        "icon": "energy"},

                    "generation_unit2": {
                        "sum":total_generation_unit2,
                        "value": total_avg_generation_unit2,
                        "title": "Unit 2 Average Generation(MW)",
                        "icon": "energy"},

                    "consumption_unit1": {
                        "sum":total_consumption_unit1,
                        "value": total_avg_consumption_unit1,
                        "title": "Unit 1 Coal Consumption(MT)",
                        "icon": "coal"},

                    "consumption_unit2": {
                        "sum":total_consumption_unit2,
                        "value": total_avg_consumption_unit2,
                        "title": "Unit 2 Coal Consumption(MT)",
                        "icon": "coal"}}
        
        result["data"]["total_specific_coal"] = {
            "Unit 1": specific_coal_unit1,
            "Unit 2": specific_coal_unit2
        }

        result["data"]["labels"] = copy.deepcopy(modified_labels)
        return result

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


def coal_analysis_infocard(
        type: Optional[str] = "Daily",
        Month: Optional[str] = None,
        Daily: Optional[str] = None,
        Year: Optional[str] = None):
    try:
        date=None
        if type == "Daily":
            date = Daily
            start_date = datetime.datetime.strptime(f'{date} 00:00:00',"%Y-%m-%d %H:%M:%S")
            end_date = datetime.datetime.strptime(f"{date} 23:59:59", "%Y-%m-%d %H:%M:%S")

        elif type == "Week":
            start_date = (datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                          + UTC_OFFSET_TIMEDELTA
                          - datetime.timedelta(days=7))

            end_date = datetime.datetime.strptime(f"{datetime.datetime.utcnow().date().__str__()} 23:59:59",
                                                   "%Y-%m-%d %H:%M:%S")

        elif type == "Month":
            date = Month
            format_data = "%Y-%m-%d"
            start_date=datetime.datetime.strptime(f'{date}-01',format_data)
            end_date = start_date + relativedelta( day=31)

        elif type == "Year":
            date = Year
            format_data = "%Y-%m-%d %H:%M:%S"
            end_date=datetime.datetime.strptime(f'{date}-12-31 23:59:59',format_data)
            start_date=datetime.datetime.strptime(f'{date}-01-01 00:00:00',format_data)

        pipeline = [
            {
                "$match": {
                    "created_date": {"$gte": start_date, "$lte": end_date},
                    "tagid": {"$in": [2,3536,16,3538]},
                    "sum": {"$ne": None}
                }
            },
            {
                "$sort": {
                    "created_date": -1
                }
            },
            {
                "$group": {
                    "_id": {
                        "tagid": "$tagid",
                        "created_date": {
                            "$dateToString": {
                                "format": "%Y-%m-%d %H:%M:%S",
                                "date": "$created_date"
                            }
                        }
                    },
                    "latest_sum": {
                        "$first": {
                            "$toDouble": "$sum"
                        }
                    }
                }
            },
            {
                "$group": {
                    "_id": "$_id.tagid",
                    "total_sum": { "$sum": "$latest_sum" },
                    "count": { "$sum": 1 }  # Count the number of entries for each tagid
                }
            }
        ]

        result = Historian.objects.aggregate(pipeline)

        response_data = []

        for doc in result:
            tagid = doc["_id"]
            count = doc["count"]
            total_sum = doc["total_sum"]
            
            # Calculate the average for the current tagid
            avg_result = total_sum / count if count > 0 else 0
            
            if tagid == 16:
                response_data.append({
                    "tagid": tagid,
                    "title": "Unit 1 Coal Consumption(MT)",
                    "icon": "coal",
                    "data": round(avg_result, 2),
                    "last_updated": date
                })
            
            if tagid == 3538:
                response_data.append({
                    "tagid": tagid,
                    "title": "Unit 2 Coal Consumption(MT)",
                    "icon": "coal",
                    "data": round(avg_result, 2),
                    "last_updated": date
                })
            
            if tagid == 2:
                response_data.append({
                    "tagid": tagid,
                    "title": "Unit 1 Average Generation(MW)",
                    "icon": "energy",
                    "data": round(avg_result, 2),
                    "last_updated": date
                })
            
            if tagid == 3536:
                response_data.append({
                    "tagid": tagid,
                    "title": "Unit 2 Average Generation(MW)",
                    "icon": "energy",
                    "data": round(avg_result, 2),
                    "last_updated": date
                })

        return response_data

    except Exception as e:
        console_logger.debug(f"-----  Error ----- {e}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return {"error": str(e)}
    

def summary_info_card(
        type: Optional[str] = "Daily",
        Month: Optional[str] = None,
        Daily: Optional[str] = None,
        Year: Optional[str] = None):
    try:
        timezone = pytz.timezone('Asia/Kolkata')
        date=None
        if type == "Daily":
            date = Daily
            
            start_date = convert_to_utc_format(f'{date} 00:00:00',"%Y-%m-%d %H:%M:%S")
            end_date = convert_to_utc_format(f"{date} 23:59:59", "%Y-%m-%d %H:%M:%S")

        elif type == "Week":
            startd_date = (
                    datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
                    - datetime.timedelta(days=7))
            
            start_date = convert_to_utc_format(startd_date.__str__(), "%Y-%m-%d %H:%M:%S")
            end_date = datetime.datetime.strptime(f"{datetime.datetime.utcnow().date().__str__()} 23:59:59",
                                                   "%Y-%m-%d %H:%M:%S")
            
            date = f'{datetime.datetime.now().date().__str__()}T00:00'

        elif type == "Month":
            date = Month
            
            format_data = "%Y-%m-%d"
            start_date = timezone.localize(datetime.datetime.strptime(f'{date}-01',format_data))
            end_date = start_date + relativedelta( day=31)

        elif type == "Year":
            date = Year
            format_data = "%Y-%m-%d %H:%M:%S"
            end_date = timezone.localize(datetime.datetime.strptime(f'{date}-12-31 23:59:59',format_data))
            start_date = timezone.localize(datetime.datetime.strptime(f'{date}-01-01 00:00:00',format_data))

        pipeline = [
                {
                    "$match": {
                        "GWEL_Tare_Time": {"$gte": start_date, "$lte": end_date},
                        "net_qty": {"$ne": None},
                        "actual_net_qty": {"$ne": None},
                    },
                },
                {
                    "$project": {
                        "ts": None,
                        "actual_net_qty": {"$toDouble": "$actual_net_qty"},
                        "net_qty": {"$toDouble": "$net_qty"},
                        "_id": 0,
                    },
                },
                {
                    "$group": {
                        "_id": None,
                        "total_actual_net_qty": {"$sum": "$actual_net_qty"},
                        "total_net_qty": {"$sum": "$net_qty"},
                    },
                },
                {
                    "$project": {
                        "_id": 0,
                        "total_actual_net_qty": 1,
                        "total_net_qty": 1,
                        "transit_loss": {
                            "$subtract": ["$total_actual_net_qty", "$total_net_qty"],
                        },
                    },
                },
            ]

        result = Gmrdata.objects.aggregate(pipeline)

        gwel = 0
        grn = 0
        transit_loss = 0

        for doc in result:
            gwel = round(doc["total_actual_net_qty"],2)
            grn = round(doc["total_net_qty"],2)
            transit_loss = round(doc["transit_loss"],2)

        scanned_count = Gmrdata.objects(GWEL_Tare_Time__ne=None,
                                        GWEL_Tare_Time__gte=start_date,
                                        GWEL_Tare_Time__lte=end_date
                                        ).count()
        
        # in_count =  Gmrdata.objects(GWEL_Tare_Time__ne=None,
        #                             GWEL_Tare_Time__gte=start_date,
        #                             GWEL_Tare_Time__lte=end_date
        #                             ).count()
        
        # out_count = Gmrdata.objects(GWEL_Tare_Time__ne=None,
        #                             GWEL_Tare_Time__gte=start_date,
        #                             GWEL_Tare_Time__lte=end_date
        #                             ).count()

        rake_loaded = RailData.objects(
                                    placement_date__ne=None,
                                    placement_date__icontains=date,  
                                ).count()

        rake_transit = RailData.objects(
                                    placement_date__ne=None,
                                    avery_placement_date=None,
                                    placement_date__icontains=date
                                ).count()

        rake_received = RailData.objects(
                                        avery_placement_date__ne = None,
                                        avery_placement_date__icontains=date
                                    ).count()

        return {
                "road_datails":{
                    "icon" : "truck",
                        "cards": {
                            "Mine_Vehicle_Scanned" : scanned_count if scanned_count else None,
                            "Gate_Vehicle_In" : scanned_count if scanned_count else None,
                            "Gate_Vehicle_Out" : scanned_count if scanned_count else None
                        }       
                    },

                "coal_receipt_datails":{
                    "icon" : "energy",
                        "cards": {
                            "Total_GRN_Coal(MT)" : grn if grn else None,
                            "Total_GWEL_Coal(MT)" : gwel if gwel else None,
                            "Total_Transit_Loss(MT)" : transit_loss if transit_loss else None
                        }       
                    },
                
                "rail_datails":{
                    "icon" : "rail",
                        "cards": {
                            "Rakes_Loaded" : rake_loaded if rake_loaded else None,
                            "Rakes_In-Transit" : rake_transit if rake_transit else None,
                            "Rakes_Received" : rake_received if rake_received else None
                        }       
                    }
                }

    except Exception as e:
        console_logger.debug("----- Info Card Count Error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return e


def summary_caol_receipt_graph(
    type: Optional[str] = "Daily",
    Daily: Optional[str] = None,
    Month: Optional[str] = None,
    Year: Optional[str] = None
):
    try:
        data = {}
        timezone = pytz.timezone('Asia/Kolkata')

        basePipeline = [
            {
                "$match": {
                    "GWEL_Tare_Time": {"$gte": None},
                    "net_qty": {"$ne": None},
                    "actual_net_qty": {"$ne": None}
                },
            },
            {
                "$project": {
                    "ts": None,
                    "actual_net_qty": {"$toDouble": "$actual_net_qty"},
                    "net_qty": {"$toDouble": "$net_qty"},
                    "_id": 0
                },
            },
            {
                "$group": {
                    "_id": {"ts": "$ts"},
                    "actual_net_qty_sum": {"$sum": "$actual_net_qty"},
                    "net_qty_sum": {"$sum": "$net_qty"},
                },
            },
            {
                "$project": {
                    "_id": 0,
                    "ts": "$_id.ts",
                    "net_qty_sum": 1,
                    "actual_net_qty_sum": 1,
                    "transit_loss": {
                        "$subtract": ["$actual_net_qty_sum", "$net_qty_sum"]
                    },
                },
            },
        ]

        if type == "Daily":
            date = Daily
            end_date = f"{date} 23:59:59"
            start_date = f"{date} 00:00:00"
            format_data = "%Y-%m-%d %H:%M:%S"
            endd_date = convert_to_utc_format(end_date, format_data)
            startd_date = convert_to_utc_format(start_date, format_data)

            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date

            basePipeline[1]["$project"]["ts"] = {
                "$hour": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}
            }

            result = {
                "data": {
                    "labels": [str(i) for i in range(1, 25)],
                    "datasets": [
                        {"label": "Total_GRN_Coal(MT)", "data": [0 for i in range(1, 25)]},
                        {"label": "Total_GWEL_Coal(MT)", "data": [0 for i in range(1, 25)]},
                        {"label": "Total_Transit_Loss(MT)", "data": [0 for i in range(1, 25)]},
                    ],
                }
            }

        elif type == "Week":
            
            start_date = convert_to_utc_format((
                datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
                - datetime.timedelta(days=7)).__str__(), "%Y-%m-%d %H:%M:%S")
            
            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = start_date
            
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
                        {"label": "Total_GRN_Coal(MT)", "data": [0 for i in range(1, 8)]},
                        {"label": "Total_GWEL_Coal(MT)", "data": [0 for i in range(1, 8)]},
                        {"label": "Total_Transit_Loss(MT)", "data": [0 for i in range(1, 8)]},
                    ],
                }
            }
        
        elif type == "Month":
            date = Month
            format_data = "%Y-%m-%d"
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
                        (startd_date + datetime.timedelta(days=i)).strftime("%d")
                        for i in range(int(end_label))
                    ],
                    "datasets": [
                        {"label": "Total_GRN_Coal(MT)", "data": [0 for i in range(int(end_label))]},
                        {"label": "Total_GWEL_Coal(MT)", "data": [0 for i in range(int(end_label))]},
                        {"label": "Total_Transit_Loss(MT)", "data": [0 for i in range(int(end_label))]},
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
                            basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"]
                            + relativedelta(months=i)
                        ).strftime("%m")
                        for i in range(0, 12)
                    ],
                    "datasets": [
                        {"label": "Total_GRN_Coal(MT)", "data": [0 for i in range(0, 12)]},
                        {"label": "Total_GWEL_Coal(MT)", "data": [0 for i in range(0, 12)]},
                        {"label": "Total_Transit_Loss(MT)", "data": [0 for i in range(0, 12)]},
                    ],
                }
            }

        output = Gmrdata.objects().aggregate(basePipeline)
        outputDict = {}

        for data in output:
            ts = data["ts"]
            net_qty = data["net_qty_sum"]
            actual_net_qty = data["actual_net_qty_sum"]
            transit_loss = data["transit_loss"]
            outputDict[ts] = {
                "net_qty": net_qty,
                "actual_net_qty": actual_net_qty,
                "transit_loss": transit_loss,
            }
        modified_labels = [i for i in range(0, 24)]

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
                        startd_date + datetime.timedelta(days=i + 1)
                    ).strftime("%d-%b")
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

            if type == "Year":
                ts = index + 1
            else:
                ts = int(label)
            
            if ts in outputDict:
                result["data"]["datasets"][0]["data"][index] = round(outputDict[ts]["net_qty"], 2)
                result["data"]["datasets"][1]["data"][index] = round(outputDict[ts]["actual_net_qty"], 2)
                result["data"]["datasets"][2]["data"][index] = round(outputDict[ts]["transit_loss"], 2)

        result["data"]["labels"] = copy.deepcopy(modified_labels)
        return result
    
    except Exception as e:
        console_logger.debug(f"----- Summary Coal Graph ----- {e}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        return {"error": str(e)}


def summary_road_detail_graph(
    type: Optional[str] = "Daily",
    Daily: Optional[str] = None,
    Month: Optional[str] = None,
    Year: Optional[str] = None
):
    timezone = pytz.timezone('Asia/Kolkata')
    data = {}
    basePipeline = [
        {
            "$match": {
                "GWEL_Tare_Time": {
                    "$gte": None,
                },
            },
        },
        {
            "$group": {
                "_id": None,
                "scanned_count": {"$sum": 1},
            },
        },
    ]

    if type == "Daily":
        date = Daily
        end_date = f"{date} 23:59:59"
        start_date = f"{date} 00:00:00"
        format_data = "%Y-%m-%d %H:%M:%S"
        endd_date = convert_to_utc_format(end_date, format_data)
        startd_date = convert_to_utc_format(start_date, format_data)
    
        basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date
        basePipeline[0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date

        basePipeline[1]["$group"]["_id"] = {
            "$hour": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}
        }

        result = {
                "data": {
                    "labels": [str(i) for i in range(1, 25)],
                    "datasets": [
                        {"label": "Mine_Vehicle_Scanned", "data": [0 for i in range(1, 25)]},
                        {"label": "Gate_Vehicle_In", "data": [0 for i in range(1, 25)]},
                        {"label": "Gate_Vehicle_Out", "data": [0 for i in range(1, 25)]},
                    ],
                }
            }
        
    elif type == "Week":
        start_date = convert_to_utc_format((
                datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
                - datetime.timedelta(days=7)).__str__(), "%Y-%m-%d %H:%M:%S")
        
        basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = start_date

        basePipeline[1]["$group"]["_id"] = {"$dayOfMonth": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}

        result = {
                "data": {
                    "labels": [
                        (
                         convert_to_utc_format(start_date.__str__(),"%Y-%m-%d %H:%M:%S") + datetime.timedelta(days=i+1)
                        ).strftime("%d")
                        for i in range(1, 8)
                    ],
                    "datasets": [
                        {"label": "Mine_Vehicle_Scanned", "data": [0 for i in range(1, 8)]},
                        {"label": "Gate_Vehicle_In", "data": [0 for i in range(1, 8)]},
                        {"label": "Gate_Vehicle_Out", "data": [0 for i in range(1, 8)]},
                    ],
                }
            }

    elif type == "Month":
        date = Month
        format_data = "%Y-%m-%d"
        start_date = f'{date}-01'
        startd_date = timezone.localize(datetime.datetime.strptime(start_date, format_data))

        end_date = startd_date + relativedelta(day=31)
        end_label = end_date.strftime("%d")

        basePipeline[0]["$match"]["GWEL_Tare_Time"]["$lte"] = end_date
        basePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date
        basePipeline[1]["$group"]["_id"] = {"$dayOfMonth": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}

        result = {
                "data": {
                    "labels": [
                        (startd_date + datetime.timedelta(days=i)).strftime("%d")
                        for i in range(-1, (int(end_label))-1)
                    ],
                    "datasets": [
                        {"label": "Mine_Vehicle_Scanned", "data": [0 for i in range(-1, (int(end_label))-1)]},
                        {"label": "Gate_Vehicle_In", "data": [0 for i in range(-1, (int(end_label))-1)]},
                        {"label": "Gate_Vehicle_Out", "data": [0 for i in range(-1, (int(end_label))-1)]},
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

        basePipeline[1]["$group"]["_id"] = {"$month": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}

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
                        {"label": "Mine_Vehicle_Scanned", "data": [0 for i in range(0, 12)]},
                        {"label": "Gate_Vehicle_In", "data": [0 for i in range(0, 12)]},
                        {"label": "Gate_Vehicle_Out", "data": [0 for i in range(0, 12)]},
                    ],
                }
            }

    output = Gmrdata.objects().aggregate(basePipeline)
    outputDict = {}

    for data in output:
        ts = data["_id"]
        count = data["scanned_count"]
        outputDict[ts] = {"vehicle_count": count}

    modified_labels = [i for i in range(0, 24)]

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
        
        if type == "Year":
            ts = index + 1
        else:
            ts = int(label)
        
        if ts in outputDict:
            result["data"]["datasets"][0]["data"][index] = outputDict[ts]["vehicle_count"]
            result["data"]["datasets"][1]["data"][index] = outputDict[ts]["vehicle_count"]
            result["data"]["datasets"][2]["data"][index] = outputDict[ts]["vehicle_count"]

    result["data"]["labels"] = copy.deepcopy(modified_labels)
    return result