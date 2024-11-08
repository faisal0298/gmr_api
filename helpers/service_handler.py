from fastapi import Response
from typing import Optional
from helpers.logger import console_logger
import os, sys
import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from database.models import *
import copy



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

        result["data"]["total_averages"] = [
                        {"generation_unit1": total_avg_generation_unit1,
                        "title": "Unit 1 Average Generation(MW)",
                        "icon": "energy"},
                        
                        {"generation_unit2": total_avg_generation_unit2,
                        "title": "Unit 2 Average Generation(MW)",
                        "icon": "energy"},

                        {"consumption_unit1": total_avg_consumption_unit1,
                        "title": "Unit 1 Coal Consumption(MT)",
                        "icon": "coal"},

                        {"consumption_unit2": total_avg_consumption_unit2,
                        "title": "Unit 2 Coal Consumption(MT)",
                        "icon": "coal"}]
        
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