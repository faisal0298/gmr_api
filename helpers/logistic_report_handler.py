from helpers.general_helpers import error_handler, convert_to_utc_format, get_financial_year
import pytz, datetime, copy, os, xlsxwriter
from dateutil.relativedelta import relativedelta
from database.models import *
from service import host, db_port, username, password, ip
from pymongo import MongoClient
import polars as pl
from collections import defaultdict

client = MongoClient(f"mongodb://{host}:{db_port}/")
db = client.gmrDB
gmrdata = db.gmrdata
db = client.gmrDB.get_collection("gmrdata")
short_mine_collection = db.short_mine
sapdb = client.gmrDB.get_collection("SapRecords")
sapraildb = client.gmrDB.get_collection("sapRecordsRail")
raildb = client.gmrDB.get_collection("raildata")
coaltestdb = client.gmrDB.get_collection("coaltesting")
receiptCoalQualityAnalysisdb = client.gmrDB.get_collection("RecieptCoalQualityAnalysis")

gmrDB = client.get_database("gmrDB")
collectionList = gmrDB.list_collection_names()

class LogisticsHandlers:
    def __init__(self) -> None:
        pass
    
    def fetchTransitLoss(self, type: str, Daily: str, Month: str, Year: str, Overall: str):
        try:
            data = {}
            timezone = pytz.timezone('Asia/Kolkata')
            current_time = datetime.datetime.now(timezone)
            UTC_OFFSET_TIMEDELTA = datetime.datetime.utcnow() - datetime.datetime.now()

            # financial year
            # road part
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
                    "$match": {
                        "GWEL_Tare_Time": {
                            "$gte": None,  # You can adjust this with a valid start date
                        },
                    },
                },
                {
                    "$project": {
                        "ts": None,
                        "actual_net_qty": { "$toDouble": "$actual_net_qty" },
                        "net_qty": { "$toDouble": "$net_qty" },
                        "label": "Road",
                        "_id": 0
                    }
                },
            ]

            # historic data - gmrdatahistoric
            historicBasePipeline = [
                {
                    "$match": {
                        "GWEL_Tare_Time": {
                            "$gte": None,  # You can adjust this with a valid start date
                        },
                    },
                },
                {
                    "$project": {
                        "ts": None,
                        "actual_net_qty": { "$toDouble": "$actual_net_qty" },
                        "net_qty": { "$toDouble": "$net_qty" },
                        "label": "Road",
                        "_id": 0
                    }
                },
            ]

            #rail part
            railBasePipeline = [
                {
                    '$match': {
                        'month': {
                            '$exists': True, 
                            '$ne': None
                        }
                    }
                }, 
                # converting from string to date
                {
                    '$addFields': {
                        'month': {
                            '$dateFromString': {
                                'dateString': '$month'
                            }
                        }
                    }
                }, 
                {
                    "$match": {
                        "month": {
                            "$gte": None,  # You can adjust this with a valid start date
                            "$lte": None,  # You can adjust this with a valid start date
                        },
                    },
                },
                {
                    "$project": {
                        "ts": None,
                        "actual_net_qty": { "$toDouble": "$total_secl_net_wt" },
                        "net_qty": { "$toDouble": "$Total_gwel_net" },
                        "label": "Rail",
                        "_id": 0
                    }
                },
            ]

            rcrRailBasePipeline = [
                {
                    '$match': {
                        'month': {
                            '$exists': True, 
                            '$ne': None
                        }
                    }
                }, 
                # # converting from string to date
                {
                    '$addFields': {
                        'month': {
                            '$dateFromString': {
                                'dateString': '$month'
                            }
                        }
                    }
                }, 
                {
                    "$match": {
                        "month": {
                            "$gte": None,  # You can adjust this with a valid start date
                            "$lte": None,  # You can adjust this with a valid start date
                        },
                    },
                },
                {
                    "$project": {
                        "ts": None,
                        "actual_net_qty": { "$toDouble": "$total_rly_net_wt" },
                        "net_qty": { "$toDouble": "$Total_gwel_net" },
                        "label": "Rail",
                        "_id": 0
                    }
                },
            ]

            if type=="Year" or type=="Overall":
                year_data = {
                    "$project": {
                        "index": {
                            "$switch": {
                            "branches": [
                                { "case": { "$eq": ["$month", 4] }, "then": 0 },  # April 2024
                                { "case": { "$eq": ["$month", 5] }, "then": 1 },  # May 2024
                                { "case": { "$eq": ["$month", 6] }, "then": 2 },  # June 2024
                                { "case": { "$eq": ["$month", 7] }, "then": 3 },  # July 2024
                                { "case": { "$eq": ["$month", 8] }, "then": 4 },  # August 2024
                                { "case": { "$eq": ["$month", 9] }, "then": 5 },  # September 2024
                                { "case": { "$eq": ["$month", 10] }, "then": 6 }, # October 2024
                                { "case": { "$eq": ["$month", 11] }, "then": 7 }, # November 2024
                                { "case": { "$eq": ["$month", 12] }, "then": 8 }, # December 2024
                                { "case": { "$eq": ["$month", 1] }, "then": 9 },  # January 2025
                                { "case": { "$eq": ["$month", 2] }, "then": 10 }, # February 2025
                                { "case": { "$eq": ["$month", 3] }, "then": 11 }  # March 2025
                            ],
                            "default": -1
                            }
                        },
                        "actual_net_qty": "$actual_net_qty",
                        "net_qty": "$net_qty",
                        "label": "$label"
                    }
                }
                # road part - gmrdata
                basePipeline.append(year_data)
                # road part - gmrdatahistoric
                historicBasePipeline.append(year_data)
                # rail part
                railBasePipeline.append(year_data)
                # rcr rail part
                rcrRailBasePipeline.append(year_data)

            # financial year
            # road part - gmrdata
            basePipeline.extend([
                {
                    "$group": {
                        "_id": {
                            "ts": "$ts",
                            "label": "$label"
                        },
                        "actual_net_qty_sum": { "$sum": "$actual_net_qty" },
                        "net_qty_sum": { "$sum": "$net_qty" }
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "ts": "$_id.ts",
                        "label": "$_id.label",
                        "data": { "$subtract": ["$actual_net_qty_sum", "$net_qty_sum"] }
                    }
                }
            ])

            # road part - gmrdataHistoric
            historicBasePipeline.extend([
                {
                    "$group": {
                        "_id": {
                            "ts": "$ts",
                            "label": "$label"
                        },
                        "actual_net_qty_sum": { "$sum": "$actual_net_qty" },
                        "net_qty_sum": { "$sum": "$net_qty" }
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "ts": "$_id.ts",
                        "label": "$_id.label",
                        "data": { "$subtract": ["$actual_net_qty_sum", "$net_qty_sum"] }
                    }
                }
            ])

            # rail part
            railBasePipeline.extend([
                {
                    "$group": {
                        "_id": {
                            "ts": "$ts",
                            "label": "$label"
                        },
                        "actual_net_qty_sum": { "$sum": "$actual_net_qty" },
                        "net_qty_sum": { "$sum": "$net_qty" }
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "ts": "$_id.ts",
                        "label": "$_id.label",
                        "data": { "$subtract": ["$net_qty_sum", "$actual_net_qty_sum"] }
                    }
                }
            ])

            # rcr rail part
            rcrRailBasePipeline.extend([
                {
                    "$group": {
                        "_id": {
                            "ts": "$ts",
                            "label": "$label"
                        },
                        "actual_net_qty_sum": { "$sum": "$actual_net_qty" },
                        "net_qty_sum": { "$sum": "$net_qty" }
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "ts": "$_id.ts",
                        "label": "$_id.label",
                        "data": { "$subtract": ["$net_qty_sum", "$actual_net_qty_sum"] }
                    }
                }
            ])

            if type == "Daily":
                date = Daily
                end_date = f'{date} 23:59:59'
                start_date = f'{date} 00:00:00'
                format_data = "%Y-%m-%d %H:%M:%S"
                endd_date = convert_to_utc_format(end_date.__str__(), format_data)
                startd_date = convert_to_utc_format(start_date.__str__(), format_data)
                rail_endd_date = datetime.datetime.strptime(end_date, format_data)
                rail_startd_date = datetime.datetime.strptime(start_date, format_data)
                
                # road part - gmrdata
                basePipeline[1]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
                basePipeline[1]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date
                basePipeline[2]["$project"]["ts"] = {"$hour": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}
                # road part - gmrdatahistoric
                historicBasePipeline[0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
                historicBasePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date
                historicBasePipeline[1]["$project"]["ts"] = {"$hour": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}
                # rail part
                railBasePipeline[2]["$match"]["month"]["$lte"] = rail_endd_date
                railBasePipeline[2]["$match"]["month"]["$gte"] = rail_startd_date
                railBasePipeline[3]["$project"]["ts"] = {"$hour": {"date": "$month"}}
                # rcr rail part
                rcrRailBasePipeline[2]["$match"]["month"]["$lte"] = rail_endd_date
                rcrRailBasePipeline[2]["$match"]["month"]["$gte"] = rail_startd_date
                rcrRailBasePipeline[3]["$project"]["ts"] = {"$hour": {"date": "$month"}}

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
                # road part - gmrdata
                basePipeline[1]["$match"]["GWEL_Tare_Time"]["$gte"] = convert_to_utc_format(start_date.__str__(), "%Y-%m-%d %H:%M:%S")
                basePipeline[2]["$project"]["ts"] = {"$dayOfMonth": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}
                # road part - gmrdatahistoric
                historicBasePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = convert_to_utc_format(start_date.__str__(), "%Y-%m-%d %H:%M:%S")
                historicBasePipeline[1]["$project"]["ts"] = {"$dayOfMonth": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}
                # rail part
                railBasePipeline[2]["$match"]["month"]["$gte"] = datetime.datetime.strptime(start_date.__str__(), "%Y-%m-%d %H:%M:%S")
                railBasePipeline[3]["$project"]["ts"] = {"$dayOfMonth": {"date": "$month"}}
                # rcr rail part
                rcrRailBasePipeline[2]["$match"]["month"]["$gte"] = datetime.datetime.strptime(start_date.__str__(), "%Y-%m-%d %H:%M:%S")
                rcrRailBasePipeline[3]["$project"]["ts"] = {"$dayOfMonth": {"date": "$month"}}
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

                rail_startd_date = datetime.datetime.strptime(start_date, format_data)
                rail_endd_date = rail_startd_date + relativedelta(day=31)

                end_date = startd_date + relativedelta(day=31)
                end_label = end_date.strftime("%d")
                # road part - gmrdata
                basePipeline[1]["$match"]["GWEL_Tare_Time"]["$lte"] = end_date
                basePipeline[1]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date
                basePipeline[2]["$project"]["ts"] = {"$dayOfMonth": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}
                # road part - gmrdataHistoric
                historicBasePipeline[0]["$match"]["GWEL_Tare_Time"]["$lte"] = end_date
                historicBasePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date
                historicBasePipeline[1]["$project"]["ts"] = {"$dayOfMonth": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}
                # rail part
                railBasePipeline[2]["$match"]["month"]["$lte"] = rail_endd_date
                railBasePipeline[2]["$match"]["month"]["$gte"] = rail_startd_date
                railBasePipeline[3]["$project"]["ts"] = {"$dayOfMonth": {"date": "$month"}}
                # rcr rail part
                rcrRailBasePipeline[2]["$match"]["month"]["$lte"] = rail_endd_date
                rcrRailBasePipeline[2]["$match"]["month"]["$gte"] = rail_startd_date
                rcrRailBasePipeline[3]["$project"]["ts"] = {"$dayOfMonth": {"date": "$month"}}
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
                year_range = Year.split("-")
                start_date = f'{year_range[0]}-04-01 00:00:00'
                end_date = f'{year_range[1]}-03-31 23:59:59'
                format_data = "%Y-%m-%d %H:%M:%S"
                startd_date = timezone.localize(datetime.datetime.strptime(start_date, format_data))
                endd_date = timezone.localize(datetime.datetime.strptime(end_date, format_data))

                rail_endd_date = datetime.datetime.strptime(end_date, format_data)
                rail_startd_date = datetime.datetime.strptime(start_date, format_data)

                # road part - gmr data
                basePipeline[1]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
                basePipeline[1]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date
                basePipeline[2]["$project"]["month"] = {"$month": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}
                basePipeline[4]["$group"] = {
                    "_id": {
                        "index": "$index",
                        "label": "$label"
                    },
                    "actual_net_qty_sum": { "$sum": "$actual_net_qty" },
                    "net_qty_sum": { "$sum": "$net_qty" }
                }
                basePipeline[5]["$project"] = {
                    "_id": 0,
                    "ts": "$_id.index",
                    "label": "$_id.label",
                    "data": { "$subtract": ["$actual_net_qty_sum", "$net_qty_sum"] }
                }
                # road part - gmrdatahistoric
                historicBasePipeline[0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
                historicBasePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date
                historicBasePipeline[1]["$project"]["month"] = {"$month": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}
                historicBasePipeline[3]["$group"] = {
                    "_id": {
                        "index": "$index",
                        "label": "$label"
                    },
                    "actual_net_qty_sum": { "$sum": "$actual_net_qty" },
                    "net_qty_sum": { "$sum": "$net_qty" }
                }
                historicBasePipeline[4]["$project"] = {
                    "_id": 0,
                    "ts": "$_id.index",
                    "label": "$_id.label",
                    "data": { "$subtract": ["$actual_net_qty_sum", "$net_qty_sum"] }
                }
                # rail part
                railBasePipeline[2]["$match"]["month"]["$lte"] = rail_endd_date
                railBasePipeline[2]["$match"]["month"]["$gte"] = rail_startd_date
                railBasePipeline[3]["$project"]["month"] = {"$month": {"date": "$month"}}
                railBasePipeline[5]["$group"] = {
                    "_id": {
                        "index": "$index",
                        "label": "$label"
                    },
                    "actual_net_qty_sum": { "$sum": "$actual_net_qty" },
                    "net_qty_sum": { "$sum": "$net_qty" }
                }
                railBasePipeline[6]["$project"] = {
                    "_id": 0,
                    "ts": "$_id.index",
                    "label": "$_id.label",
                    "data": { "$subtract": ["$net_qty_sum", "$actual_net_qty_sum"] }
                }
                # rcr rail part
                rcrRailBasePipeline[2]["$match"]["month"]["$lte"] = rail_endd_date
                rcrRailBasePipeline[2]["$match"]["month"]["$gte"] = rail_startd_date
                rcrRailBasePipeline[3]["$project"]["month"] = {"$month": {"date": "$month"}}
                rcrRailBasePipeline[5]["$group"] = {
                    "_id": {
                        "index": "$index",
                        "label": "$label"
                    },
                    "actual_net_qty_sum": { "$sum": "$actual_net_qty" },
                    "net_qty_sum": { "$sum": "$net_qty" }
                }
                rcrRailBasePipeline[6]["$project"] = {
                    "_id": 0,
                    "ts": "$_id.index",
                    "label": "$_id.label",
                    "data": { "$subtract": ["$net_qty_sum", "$actual_net_qty_sum"] }
                }
                result = {
                    "data": {
                        "labels": [
                            (
                                startd_date + relativedelta(months=i)
                            ).strftime("%b %y")
                            for i in range(12)  # Iterate over 12 months starting from the financial year start
                        ],
                        "datasets": [
                            {"label": "Road", "data": [0 for _ in range(12)]},  # Initialize 12 months for Road
                            {"label": "Rail", "data": [0 for _ in range(12)]},  # Initialize 12 months for Rail
                        ],
                    }
                }
            elif type == "Overall":
                date = Overall.split("-")[0]
                end_date = f'{int(date) + 1}-03-31 23:59:59'
                start_date = f'{date}-04-01 00:00:00'
                format_data = "%Y-%m-%d %H:%M:%S"
                startd_date = timezone.localize(datetime.datetime.strptime(start_date, format_data))
                endd_date = timezone.localize(datetime.datetime.strptime(end_date, format_data))

                rail_endd_date = datetime.datetime.strptime(end_date, format_data)
                rail_startd_date = datetime.datetime.strptime(start_date, format_data)
                # road part - gmrdata
                basePipeline[1]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
                basePipeline[1]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date
                basePipeline[2]["$project"]["month"] = {"$month": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}
                basePipeline[4]["$group"] = {
                    "_id": {
                        "index": "$index",
                        "label": "$label"
                    },
                    "actual_net_qty_sum": { "$sum": "$actual_net_qty" },
                    "net_qty_sum": { "$sum": "$net_qty" }
                }
                basePipeline[5]["$project"] = {
                    "_id": 0,
                    "ts": "$_id.index",
                    "label": "$_id.label",
                    "data": { "$subtract": ["$actual_net_qty_sum", "$net_qty_sum"] }
                }
                # road part - gmrdatahistoric
                historicBasePipeline[0]["$match"]["GWEL_Tare_Time"]["$lte"] = endd_date
                historicBasePipeline[0]["$match"]["GWEL_Tare_Time"]["$gte"] = startd_date
                historicBasePipeline[1]["$project"]["month"] = {"$month": {"date": "$GWEL_Tare_Time", "timezone": "Asia/Kolkata"}}
                historicBasePipeline[3]["$group"] = {
                    "_id": {
                        "index": "$index",
                        "label": "$label"
                    },
                    "actual_net_qty_sum": { "$sum": "$actual_net_qty" },
                    "net_qty_sum": { "$sum": "$net_qty" }
                }
                historicBasePipeline[4]["$project"] = {
                    "_id": 0,
                    "ts": "$_id.index",
                    "label": "$_id.label",
                    "data": { "$subtract": ["$actual_net_qty_sum", "$net_qty_sum"] }
                }
                # rail part
                railBasePipeline[2]["$match"]["month"]["$lte"] = rail_endd_date
                railBasePipeline[2]["$match"]["month"]["$gte"] = rail_startd_date
                # railBasePipeline[3]["$project"]["month"] = {"$month": {"date": "$month"}}
                railBasePipeline[5]["$group"] = {
                    "_id": {
                        "index": "$index",
                        "label": "$label"
                    },
                    "actual_net_qty_sum": { "$sum": "$actual_net_qty" },
                    "net_qty_sum": { "$sum": "$net_qty" }
                }
                railBasePipeline[6]["$project"] = {
                    "_id": 0,
                    "ts": "$_id.index",
                    "label": "$_id.label",
                    "data": { "$subtract": ["$net_qty_sum", "$actual_net_qty_sum"] }
                }

                # rcr rail part
                rcrRailBasePipeline[2]["$match"]["month"]["$lte"] = rail_endd_date
                rcrRailBasePipeline[2]["$match"]["month"]["$gte"] = rail_startd_date
                # rcrRailBasePipeline[3]["$project"]["month"] = {"$month": {"date": "$month"}}
                rcrRailBasePipeline[5]["$group"] = {
                    "_id": {
                        "index": "$index",
                        "label": "$label"
                    },
                    "actual_net_qty_sum": { "$sum": "$actual_net_qty" },
                    "net_qty_sum": { "$sum": "$net_qty" }
                }
                rcrRailBasePipeline[6]["$project"] = {
                    "_id": 0,
                    "ts": "$_id.index",
                    "label": "$_id.label",
                    "data": { "$subtract": ["$net_qty_sum", "$actual_net_qty_sum"] }
                }

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
            historicOutput = gmrdataHistoric.objects().aggregate(historicBasePipeline)
            railOutput = RailData.objects().aggregate(railBasePipeline)
            rcrRailOutput = RcrData.objects().aggregate(rcrRailBasePipeline)
            outputDict = {}
            if type == "Overall":
                modified_labels = [f"{int(date)} - {int(date) + 1}"]
                total_loss = 0
                # road part - gmr
                for data in output:
                    label = data["label"]
                    total_loss += data["data"]
                    outputDict[label] = total_loss
                # road part - gmrhistoric
                for data in historicOutput:
                    label = data["label"]
                    total_loss += data["data"]
                    outputDict[label] = total_loss
                # rail part
                for data in railOutput:
                    label = data["label"]
                    total_loss = data["data"]
                    outputDict[label] = total_loss
                # rcr rail part
                for data in rcrRailOutput:
                    label = data["label"]
                    console_logger.debug(label)
                    total_loss = data["data"]
                    outputDict[label] += total_loss
    
                for dataset in result["data"]["datasets"]:
                    label = dataset["label"]
                    if label in outputDict:
                        dataset["data"] = [outputDict[label]]
            else:
                # road part - gmrdata
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

                # road part - gmrdataHistoric
                for data in historicOutput:
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
                
                # rail part
                for data in railOutput:
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

                # rcr rail part
                for data in rcrRailOutput:
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
                                    result["data"]["datasets"][0]["data"][index] = round(val, 2)
                                elif key == "Rail":
                                    result["data"]["datasets"][1]["data"][index] = round(val, 2)
                            else:
                                if key == "Road":
                                    result["data"]["datasets"][0]["data"][index] = round(val, 2)
                                elif key == "Rail":
                                    result["data"]["datasets"][1]["data"][index] = round(val, 2)

            result["data"]["labels"] = copy.deepcopy(modified_labels)
            return result
        except Exception as e:
            error_handler(e)

    def minewiseAverageGcvFetch(self, type: str, Month: str, Daily: str, Year: str):
        try:
            if type == "Daily":
                specified_date = datetime.datetime.strptime(Daily, "%Y-%m-%d")
                start_of_month = specified_date.replace(day=1)
                start_date = datetime.datetime.strftime(start_of_month, '%Y-%m-%d')
                end_date = datetime.datetime.strftime(specified_date, '%Y-%m-%d')
                month_val = datetime.datetime.strftime(specified_date, '%m')
            elif type == "Week":
                specified_date = datetime.datetime.now().date()
                start_of_week = specified_date - datetime.timedelta(days=7)
                start_date = datetime.datetime.strftime(start_of_week, '%Y-%m-%d')
                end_date = datetime.datetime.strftime(specified_date, '%Y-%m-%d')
                month_val = datetime.datetime.strftime(specified_date, '%m')
            elif type == "Month":
                date=Month
                datestructure = date.replace(" ", "").split("-")
                final_month = f"{datestructure[0]}-{str(datestructure[1]).zfill(2)}"
                start_month = f"{final_month}-01"
                startd_date = datetime.datetime.strptime(start_month, "%Y-%m-%d")
                endd_date = startd_date + datetime.timedelta(days=30)
                start_date = datetime.datetime.strftime(startd_date, '%Y-%m-%d')
                end_date = datetime.datetime.strftime(endd_date, '%Y-%m-%d')
                month_val = datetime.datetime.strftime(endd_date, '%m')
            elif type == "Year":
                date = Year
                endd_date =f'{date.split("-")[0]}-12-31'
                startd_date = f'{date.split("-")[0]}-01-01'
                format_data = "%Y-%m-%d"
                end_date=datetime.datetime.strftime(datetime.datetime.strptime(endd_date,format_data), format_data)
                start_date=datetime.datetime.strftime(datetime.datetime.strptime(startd_date,format_data), format_data)
                year_range = Year.split("-")
                if len(year_range) == 2:
                    start_year = int(year_range[0])
                    end_year = int(year_range[1])
                    
                    # Define the start and end dates for the financial year
                    startd_date = f"{start_year}-04-01"
                    endd_date = f"{end_year}-03-31"
                else:
                    raise ValueError("Invalid financial year format. Expected 'YYYY-YYYY'.")
                
                format_data = "%Y-%m-%d"
                start_date = datetime.datetime.strptime(startd_date, format_data).strftime(format_data)
                end_date = datetime.datetime.strptime(endd_date, format_data).strftime(format_data)
                month_val = "04"  # Financial year always starts in April

            if "RecieptCoalQualityAnalysis" not in collectionList:
                RCA = gmrDB.create_collection("RecieptCoalQualityAnalysis")
            else:
                RCA = gmrDB.get_collection("RecieptCoalQualityAnalysis")

            listData = []
            rr_no_values = {}
            fetchRCAQualityRoad = RecieptCoalQualityAnalysis.objects(plant_analysis_date__gte=datetime.datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"), plant_analysis_date__lte=datetime.datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"), mode="Road")
            fetchRCAQualityRail = RecieptCoalQualityAnalysis.objects(plant_analysis_date__gte=datetime.datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"), plant_analysis_date__lte=datetime.datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y-%m-%dT%H:%M"), mode="Rail")
            
            for single_data_train in fetchRCAQualityRail:
                rrNo = single_data_train.sample_id
                
                if type == "Year":
                    year = int(Year.split("-")[0])
                    year_start_date = datetime.datetime(year, 4, 1)
                    year_end_date = datetime.datetime(year + 1, 3, 31)
                    filter = {
                        'mine': single_data_train.mine,
                        'plant_analysis_date': {
                            '$gte': year_start_date,
                            # '$lte': year_end_date,
                        },
                    }
                else:
                    filter = {
                        'mine':single_data_train.mine,
                        'plant_analysis_date': {
                            '$gte': datetime.datetime.now(datetime.timezone.utc).replace(day=1,month=int(month_val),hour=0,minute=0,second=0,microsecond=0),
                        # '$lte':end_day,
                    }, }

                df = pl.DataFrame(list(RCA.find(filter=filter)))
                
                df = df.with_columns([
                    pl.lit(0.0).alias("cum_wt"),
                    pl.lit(0.0).alias("weighted_gcv"),
                    pl.lit(0.0).alias("cum_weighted_gcv"),
                    pl.lit(0.0).alias("gcv")
                ])

                if "sample_qty" in df.columns:
                    if df["sample_qty"][0]:
                        df = df.with_columns([
                            pl.lit(float(df["sample_qty"][0])).alias('cum_wt')
                        ])
                        
                        # initialize data to previous values    
                        cum_wt_list = [float(df['sample_qty'][0])]
                        weighted_gcv_list = [float(df['sample_qty'][0]) * float(df['plant_arb_gcv'][0])]
                        cum_weighted_gcv_list = [weighted_gcv_list[0]]
                        gcv_list = [cum_weighted_gcv_list[0]/cum_wt_list[0]]
                        
                        for i in range(1, len(df)):
                            
                            cum_wt_total = float(df['sample_qty'][i]) + cum_wt_list[i-1]
                            weighted_gcv_total = float(df['sample_qty'][i]) * float(df['plant_arb_gcv'][i])
                            cum_weighted_gcv_total = float(weighted_gcv_total) + cum_weighted_gcv_list[i-1]
                            gcv_total = cum_weighted_gcv_total/cum_wt_total
                            
                            cum_wt_list.append(cum_wt_total)
                            weighted_gcv_list.append(weighted_gcv_total)
                            cum_weighted_gcv_list.append(cum_weighted_gcv_total)
                            gcv_list.append(gcv_total)
                        
                        df = df.with_columns(
                            pl.Series('cum_wt', cum_wt_list),
                            pl.Series('weighted_gcv', weighted_gcv_list),
                            pl.Series('cum_weighted_gcv',cum_weighted_gcv_list),
                            pl.Series('gcv', gcv_list)
                        )
                        rr_no_values[single_data_train.mine] = gcv_list[-1]
                        # df.write_excel("gcv_calculation.xlsx")

            for single_data_raod in fetchRCAQualityRoad:
                rrNo = single_data_raod.sample_id
                if type == "Year":
                    year = int(Year.split("-")[0])
                    fin_start_date = datetime.datetime(year, 4, 1)
                    fin_end_date = datetime.datetime(year + 1, 3, 31)
                    filter = {
                        'type_consumer': single_data_raod.type_consumer,
                        'plant_analysis_date': {
                            '$gte': fin_start_date,
                            # '$lte': fin_end_date,
                        },
                    }

                else:
                    filter = {
                    'type_consumer':single_data_raod.type_consumer,
                    'plant_analysis_date': {
                        '$gte': datetime.datetime.now(datetime.timezone.utc).replace(day=1,month=int(month_val),hour=0,minute=0,second=0,microsecond=0),
                        # '$lte':end_day,
                    }, }

                df = pl.DataFrame(list(RCA.find(filter=filter)))
                
                df = df.with_columns([
                    pl.lit(0.0).alias("cum_wt"),
                    pl.lit(0.0).alias("weighted_gcv"),
                    pl.lit(0.0).alias("cum_weighted_gcv"),
                    pl.lit(0.0).alias("gcv")
                ])
                if "sample_qty" in df.columns:
                    if df["sample_qty"][0]:
                        df = df.with_columns([
                            pl.lit(float(df["sample_qty"][0])).alias('cum_wt')
                        ])
                        
                        # initialize data to previous values    
                        cum_wt_list = [float(df['sample_qty'][0])]
                        weighted_gcv_list = [float(df['sample_qty'][0]) * float(df['plant_arb_gcv'][0])]
                        cum_weighted_gcv_list = [weighted_gcv_list[0]]
                        gcv_list = [cum_weighted_gcv_list[0]/cum_wt_list[0]]
                        
                        for i in range(1, len(df)):
                            cum_wt_total = float(df['sample_qty'][i]) + cum_wt_list[i-1]
                            weighted_gcv_total = float(df['sample_qty'][i]) * float(df['plant_arb_gcv'][i])
                            cum_weighted_gcv_total = float(weighted_gcv_total) + cum_weighted_gcv_list[i-1]
                            gcv_total = cum_weighted_gcv_total/cum_wt_total

                            cum_wt_list.append(cum_wt_total)
                            weighted_gcv_list.append(weighted_gcv_total)
                            cum_weighted_gcv_list.append(cum_weighted_gcv_total)
                            gcv_list.append(gcv_total)
                        
                        df = df.with_columns(
                            pl.Series('cum_wt', cum_wt_list),
                            pl.Series('weighted_gcv', weighted_gcv_list),
                            pl.Series('cum_weighted_gcv',cum_weighted_gcv_list),
                            pl.Series('gcv', gcv_list)
                        )
                        rr_no_values[single_data_raod.type_consumer] = gcv_list[-1]
                        # df.write_excel("gcv_calculation.xlsx")
                        
            aopList = []
            fetchAopTarget = AopTarget.objects()
            if fetchAopTarget:
                for single_aop_target in fetchAopTarget:
                    aopList.append(single_aop_target.payload())

            target_dict = {item['source_name']: int(item['aop_target']) for item in aopList}
            
            values_dictData = [x if x is not None else 'None' for x in rr_no_values.keys()]

            aligned_target_data = [target_dict.get(label, 0) for label in values_dictData]

            rr_noList = list(rr_no_values.keys())

            if None in rr_no_values.keys():

                Noneval = rr_noList.index(None)
                # changing keys of dictionary
                # rr_no_values['None'] = rr_no_values[None]
                rr_noList[Noneval] = 'None'
                # del rr_no_values[None]

            result = {
                    "data": {
                        # "labels": list(rr_no_values.keys()),
                        "labels": rr_noList,
                        "datasets": [
                            # {"label": "Mine", "data": [data for data in list(rr_no_values.values())], "order": 1, "type": "bar"},
                            {"label": "Mine", "data": list(rr_no_values.values()), "order": 1, "type": "bar"},
                            {"label": "Target", "data": aligned_target_data, "order": 0, "type": "line"},
                        ],
                    }
                }

            return result
        except Exception as e:
            error_handler(e)

    def fetchWclLinkageMaterialization(self, year_data: str):
        try:
            if len(year_data) == 9:
                final_date = f"{year_data.split('-')[0]}-12-01"
            else:
                final_date = year_data
            financial_year = get_financial_year(final_date)
            financial_year_start = financial_year.get("start_date")
            financial_year_end = financial_year.get("end_date")
            final_year_data = datetime.datetime.strptime(financial_year_start, "%Y-%m-%d").date().year
            next_year = datetime.datetime.strptime(financial_year_end, "%Y-%m-%d").date().year

            #new with consumer_type 
            sapRecordsPipeline = [
                {
                    '$project': {
                        'month': {
                            '$cond': {
                                'if': {
                                    '$and': [
                                        {
                                            '$gte': [
                                                {
                                                    '$strLenCP': {
                                                        '$ifNull': [
                                                            '$slno', ''
                                                        ]
                                                    }
                                                }, 6
                                            ]
                                        }, {
                                            '$regexMatch': {
                                                'input': '$slno', 
                                                'regex': '^[0-9]{6}'
                                            }
                                        }
                                    ]
                                }, 
                                'then': {
                                    '$dateToString': {
                                        'format': '%Y%m', 
                                        'date': {
                                            '$dateFromString': {
                                                'dateString': {
                                                    '$concat': [
                                                        {
                                                            '$substr': [
                                                                '$slno', 0, 4
                                                            ]
                                                        }, '-', {
                                                            '$substr': [
                                                                '$slno', 4, 2
                                                            ]
                                                        }, '-01'
                                                    ]
                                                }, 
                                                'format': '%Y-%m-%d'
                                            }
                                        }
                                    }
                                }, 
                                'else': None
                            }
                        }, 
                        'year': {
                            '$substr': [
                                '$slno', 0, 4
                            ]
                        }, 
                        'do_qty': {
                            '$toDouble': '$do_qty'
                        }, 
                        'do_no': 1, 
                        'consumer_type': 1
                    }
                }, {
                    '$match': {
                        'year': {'$in': [str(final_year_data), str(next_year)]},  
                        'month': {
                            '$ne': None
                        }, 
                        # 'consumer_type': 'WCL Shakti B(iii) Round 5 Coal', 
                        'consumer_type': {
                            '$regex': '^WCL Shakti B\\(iii\\)'
                        },
                        '$expr': {
                            '$and': [
                                {
                                    '$gte': [
                                        {
                                            '$dateFromString': {
                                                'dateString': {
                                                    '$concat': [
                                                        {
                                                            '$substr': [
                                                                '$month', 0, 4
                                                            ]
                                                        }, '-', {
                                                            '$substr': [
                                                                '$month', 4, 2
                                                            ]
                                                        }, '-01'
                                                    ]
                                                }, 
                                                'format': '%Y-%m-%d'
                                            }
                                        }, {
                                            '$dateFromString': {
                                                'dateString': financial_year_start
                                            }
                                        }
                                    ]
                                }, {
                                    '$lte': [
                                        {
                                            '$dateFromString': {
                                                'dateString': {
                                                    '$concat': [
                                                        {
                                                            '$substr': [
                                                                '$month', 0, 4
                                                            ]
                                                        }, '-', {
                                                            '$substr': [
                                                                '$month', 4, 2
                                                            ]
                                                        }, '-01'
                                                    ]
                                                }, 
                                                'format': '%Y-%m-%d'
                                            }
                                        }, {
                                            '$dateFromString': {
                                                'dateString': financial_year_end
                                            }
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                }, {
                    '$group': {
                        '_id': '$month', 
                        'total_do_qty': {
                            '$sum': '$do_qty'
                        }, 
                        'do_nos': {
                            '$addToSet': '$do_no'
                        }
                    }
                }, {
                    '$sort': {
                        '_id': 1
                    }
                }
            ]

            roadDataSap_result_cursor = SapRecords.objects().aggregate(sapRecordsPipeline)
            sapData_result = list(roadDataSap_result_cursor)

            listData = []
            for month_data in sapData_result:
                month = month_data['_id']
                do_nos = month_data['do_nos']

                pipelineData = [
                    {
                        '$match': {
                            'arv_cum_do_number': {
                                '$in': do_nos,
                            },
                            # 'type_consumer': 'WCL Shakti B(iii) Round 5 Coal',
                            'type_consumer': { '$regex': '^WCL Shakti B\\(iii\\)'},
                        }
                    }, 
                    {
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
                    }, 
                    {
                        '$match': {
                            'net_qty': {
                                '$ne': None
                            }
                        }
                    }, 
                    {
                        '$group': {
                            '_id': month, 
                            'total_net_qty': {
                                '$sum': '$net_qty'
                            }
                        }
                    }
                ]

                gmrdata_result_cursor = Gmrdata.objects().aggregate(pipelineData)
                gmrdataHist_result_cursor = gmrdataHistoric.objects().aggregate(pipelineData)

                gmrData_result = list(gmrdata_result_cursor)
                gmrDataHist_result = list(gmrdataHist_result_cursor)
                listData.append(gmrData_result)
                listData.append(gmrDataHist_result)

            flat_gmrData_result = [item for sublist in listData for item in sublist]

            clubbed_agg_data = {
                month: sum(round(item['total_net_qty'], 2) for item in flat_gmrData_result if item['_id'] == month)
                for month in {item['_id'] for item in flat_gmrData_result}
            }
            chart_data = {
                'labels': [],
                'datasets': [{
                    'label': 'Percentage',
                    'data': [],
                    'borderWidth': 1
                }]
            }

            for month_data in sapData_result:
                month = month_data['_id']
                total_do_qty = month_data['total_do_qty']
                total_net_qty = clubbed_agg_data.get(month, 0)

                percentage = total_net_qty / total_do_qty * 100 if total_do_qty > 0 else 0
                new_percentage = percentage if percentage < 100 else 100.0
                # chart_data['labels'].append(total_do_qty)
                chart_data['labels'].append(datetime.datetime.strptime(month, "%Y%m").strftime("%b"))
                chart_data['datasets'][0]['data'].append(new_percentage)
            

            twnty_date_format = "%Y-%m-%d"
            twnty_start = datetime.datetime.strptime('2024-04-01', twnty_date_format)
            twnty_end = datetime.datetime.strptime('2025-03-31', twnty_date_format)
            chck_final = datetime.datetime.strptime(final_date, twnty_date_format)
            if twnty_start < chck_final < twnty_end:
                # added on 24-11-2024 as said by sachin bhai once data is clear and sorted we will remove start
                chart_data["labels"].insert(5, "Sep")
            chart_data["datasets"][0]["data"] = [100.0] * 5 + [100.0] + chart_data["datasets"][0]["data"][5:]
            # added on 24-11-2024 as said by sachin bhai once data is clear and sorted we will remove end
            targets = [100 for n in range(0, len(chart_data["datasets"][0]["data"]))]
            chart_data["datasets"].append({"label": "Target", "data": targets,"order": 0, "type": "line"})
            return chart_data
        except Exception as e:
            error_handler(e)

    def fetchSeclLinkageMaterialization(self, year_data: str):
        try:
            if len(year_data) == 9:
                final_date = f"{year_data.split('-')[0]}-12-01"
            else:
                final_date = year_data
            financial_year = get_financial_year(final_date)
            start_date = datetime.datetime.strptime(f'{financial_year.get("start_date")} 00:00:00', "%Y-%m-%d %H:%M:%S")
            end_date = datetime.datetime.strptime(f'{financial_year.get("end_date")} 23:59:59', "%Y-%m-%d %H:%M:%S")
            # start_month_year = start_date.strftime("%m-%Y")
            # end_month_year = end_date.strftime("%m-%Y")
            basePipeline = [
                {
                    '$addFields': {
                        'month_date': {
                            '$dateFromString': {
                                'dateString': {
                                    '$concat': [
                                        '01-', '$month'
                                    ]
                                }, 
                                'format': '%d-%m-%Y'
                            }
                        }
                    }
                }, {
                    '$match': {
                        'source_type': 'SECL Linkage(U1)', 
                        'month_date': {
                            '$gte': start_date, 
                            '$lte': end_date
                        }
                    }
                }, {
                    '$sort': {
                        'month_date': 1
                    }
                }
            ]
            queryData = rakeQuota.objects().aggregate(basePipeline)
            listData = []
            for log in queryData:
                rake_year = log.get("year")
                rake_month = log.get("month")
                month_year = f"{rake_year}-{rake_month[:2].upper()}"
                date_obj = datetime.datetime.strptime(month_year, "%Y-%m")
                formatted_date = date_obj.strftime("%Y-%m")
                dictData = {"month": "", "rake_planned_for_month": "", "total_rakes_received_for_month": 0}
                dictData["month"] = datetime.datetime.strptime(log.get("month"), "%m-%Y").strftime("%b-%Y")
                dictData["rake_planned_for_month"] = log.get("rake_alloted")
                if log.get("cancelled_rakes"):
                    dictData["cancelled_rakes"] = log.get("cancelled_rakes")
                    cancelled_rakes = int(dictData["cancelled_rakes"])
                dictData["total_rakes_received_for_month"] = RailData.objects.filter(
                            Q(month__icontains=formatted_date) & 
                            Q(avery_placement_date__ne=None) &
                            Q(source_type__iexact = log.get("source_type"))
                        ).count()
                planned = int(dictData["rake_planned_for_month"])
                received = dictData['total_rakes_received_for_month']
                
                # Calculate the percentage
                if log.get("cancelled_rakes"):
                    percentage = (received + cancelled_rakes) / planned * 100 if planned > 0 else 0
                else:
                    percentage = received / planned * 100 if planned > 0 else 0
                new_percentage = percentage if percentage < 100 else 100.0 
                dictData["percentage_received"] = new_percentage
                listData.append(dictData)

            # Extract months and percentages
            months = [datetime.datetime.strptime(entry['month'], "%b-%Y").strftime("%b") for entry in listData]
            percentages = [round(entry['percentage_received'], 2) for entry in listData]
            targets = [100 for n in range(0, len(months))]
            chart_data = {
                "labels": months,
                "datasets": [
                    {
                        "label": "Percentage",
                        "data": percentages,
                        "borderWidth": 1,
                    },
                    {
                        "label": "Target",
                        "data": targets,
                        "order": 0,
                        "type": "line"
                    }
                ]
            }
            return chart_data
        except Exception as e:
            error_handler(e)


    def coalLogisticsReportDashboard(self, specified_date: str, search_text: str, currentPage: int, perPage: int, mine: str, consumer_type: str, type: str):
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
                
                skip_value = (page_no - 1) * page_len

                # Apply filters based on specified conditions
                if mine and mine != "All":
                    mine_filter = {'$match': {'mine': {'$regex': f'{mine.upper()}', '$options': 'i'}}}
                else:
                    mine_filter = {}

                if consumer_type and consumer_type != "All":
                    consumer_type_filter = {'$match': {'type_consumer': consumer_type}}
                else:
                    consumer_type_filter = {}

                if search_text:
                    if search_text.isdigit():
                        search_filter = {'$match': {'arv_cum_do_number': {'$regex': f'{search_text}', '$options': 'i'}}}
                    else:
                        search_filter = {'$match': {'mine': {'$regex': f'{search_text}', '$options': 'i'}}}
                else:
                    search_filter = {}

                # Date filter using specified_date
                if specified_date:
                    from_ts = convert_to_utc_format(f'{specified_date} 00:00:00', "%Y-%m-%d %H:%M:%S")
                    to_ts = convert_to_utc_format(f'{specified_date} 23:59:59', "%Y-%m-%d %H:%M:%S")
                    date_filter = {
                        '$match': {
                            'GWEL_Tare_Time': {
                                '$ne': None,
                                # '$gte': from_ts,
                                '$lte': to_ts
                            }
                        }
                    }
                    challan_date_filter = {
                        '$match': {
                            'GWEL_Tare_Time': {
                                '$ne': None,
                                '$gte': from_ts,
                                '$lte': to_ts
                            }
                        }
                    }
                    rcr_date_filter = {
                        '$match': {
                            'tar_wt_date': {
                                '$ne': None,
                                '$lte': to_ts
                            }
                        }
                    }
                    rcr_challan_date_filter = {
                        '$match': {
                            'tar_wt_date': {
                                '$ne': None,
                                '$gte': from_ts,
                                '$lte': to_ts
                            }
                        }
                    }
                else:
                    date_filter = {}
                    challan_date_filter = {}
                    rcr_date_filter = {}
                    rcr_challan_date_filter = {}
                created_at_date = datetime.datetime(2024, 9, 23, 19, 50, 51, 572000)
                basePipeline = [
                    {
                        '$match': {
                            'created_at': {
                                '$gt': created_at_date,
                            }
                        }
                    },
                    date_filter,  # Add date filter
                    mine_filter,  # Add mine filter
                    consumer_type_filter,  # Add consumer_type filter
                    search_filter,  # Add search_text filter if any
                    {
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
                    },
                    {
                        '$lookup': {
                            'from': 'gmrdata', 
                            'let': {
                                'do_number': '$_id'
                            }, 
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$arv_cum_do_number', '$$do_number'
                                                    ]
                                                }, {
                                                    '$ne': [
                                                        '$GWEL_Tare_Time', None
                                                    ]
                                                }, {
                                                    '$lte': [
                                                        '$GWEL_Tare_Time', to_ts
                                                    ]
                                                }, {
                                                    '$gt': [
                                                        '$created_at', created_at_date
                                                    ]
                                                }
                                            ]
                                        }
                                    }
                                }
                            ], 
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
                                            # '$toDouble': '$$item.net_qty'
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
                    },
                ]
                # Remove empty filters from the pipeline
                basePipeline = [stage for stage in basePipeline if stage]

                challanlrqtybasePipeline = [
                    {
                        '$match': {
                            'created_at': {
                                '$gt': created_at_date,
                            }
                        }
                    },
                    challan_date_filter,  # Add date filter
                    mine_filter,  # Add mine filter
                    consumer_type_filter,  # Add consumer_type filter
                    search_filter,  # Add search_text filter if any
                    {
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
                    }, 
                    {
                        '$lookup': {
                            'from': 'gmrdata', 
                            'let': {
                                'do_number': '$_id'
                            }, 
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$arv_cum_do_number', '$$do_number'
                                                    ]
                                                }, {
                                                    '$ne': [
                                                        '$GWEL_Tare_Time', None
                                                    ]
                                                }, {
                                                    '$lte': [
                                                        '$GWEL_Tare_Time', to_ts
                                                    ]
                                                }, {
                                                    '$gt': [
                                                        '$created_at', created_at_date
                                                    ]
                                                }
                                            ]
                                        }
                                    }
                                }
                            ], 
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


                challanlrqtybasePipeline = [stage for stage in challanlrqtybasePipeline if stage] 

                basePipelineHistoric = [
                    date_filter,  # Add date filter
                    mine_filter,  # Add mine filter
                    consumer_type_filter,  # Add consumer_type filter
                    search_filter,  # Add search_text filter if any
                    {
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
                    },
                    {
                        '$lookup': {
                            'from': 'gmrdataHistoric', 
                            'let': {
                                'do_number': '$_id'
                            }, 
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$arv_cum_do_number', '$$do_number'
                                                    ]
                                                }, {
                                                    '$ne': [
                                                        '$GWEL_Tare_Time', None
                                                    ]
                                                }, {
                                                    '$lte': [
                                                        '$GWEL_Tare_Time', to_ts
                                                    ]
                                                }
                                            ]
                                        }
                                    }
                                }
                            ], 
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
                                            '$toDouble': '$$item.net_qty'
                                        }
                                    }
                                }
                            }
                        }
                    },
                ]

                basePipelineHistoric = [stage for stage in basePipelineHistoric if stage]
                

                basepipelineHistoricChallanLrQty = [
                    challan_date_filter,  # Add date filter
                    mine_filter,  # Add mine filter
                    consumer_type_filter,  # Add consumer_type filter
                    search_filter,  # Add search_text filter if any
                    {
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
                    }, 
                    {
                        '$lookup': {
                            'from': 'gmrdataHistoric', 
                            'let': {
                                'do_number': '$_id'
                            }, 
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$arv_cum_do_number', '$$do_number'
                                                    ]
                                                }, {
                                                    '$ne': [
                                                        '$GWEL_Tare_Time', None
                                                    ]
                                                }, {
                                                    '$lte': [
                                                        '$GWEL_Tare_Time', to_ts
                                                    ]
                                                }
                                            ]
                                        }
                                    }
                                }
                            ], 
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

                # Remove empty filters from the pipeline
                basepipelineHistoricChallanLrQty = [stage for stage in basepipelineHistoricChallanLrQty if stage]
                
                basePipelineRcrRoad = [
                    rcr_date_filter,  # Add date filter
                    mine_filter,  # Add mine filter
                    consumer_type_filter,  # Add consumer_type filter
                    search_filter,  # Add search_text filter if any
                    {
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
                    }, 
                    {
                        '$lookup': {
                            'from': 'RcrRoadData', 
                            'let': {
                                'do_no': '$_id'
                            }, 
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$do_number', '$$do_no'
                                                    ]
                                                }, {
                                                    '$ne': [
                                                        '$tar_wt_date', None
                                                    ]
                                                }, {
                                                    '$lte': [
                                                        '$tar_wt_date', to_ts
                                                    ]
                                                }
                                            ]
                                        }
                                    }
                                }
                            ], 
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
                                                'input': '$$item.dc_net_wt', 
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

                basePipelineRcrRoad = [stage for stage in basePipelineRcrRoad if stage]

                basepipelineRcrRoadChallanLrQty = [
                    rcr_challan_date_filter,  # Add date filter
                    mine_filter,  # Add mine filter
                    consumer_type_filter,  # Add consumer_type filter
                    search_filter,  # Add search_text filter if any
                    {
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
                    }, 
                    {
                        '$lookup': {
                            'from': 'RcrRoadData', 
                            'let': {
                                'do_no': '$_id'
                            }, 
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$do_number', '$$do_no'
                                                    ]
                                                }, {
                                                    '$ne': [
                                                        '$tar_wt_date', None
                                                    ]
                                                }, {
                                                    '$lte': [
                                                        '$tar_wt_date', to_ts
                                                    ]
                                                }
                                            ]
                                        }
                                    }
                                }
                            ], 
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
                                                'input': '$$item.dc_net_wt', 
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

                basepipelineRcrRoadChallanLrQty = [stage for stage in basepipelineRcrRoadChallanLrQty if stage]

                saprecordsPipeline = [
                    {
                        '$match': {
                            '$expr': {
                                '$and': [
                                    {
                                        '$lte': [
                                            {
                                                '$dateFromString': {
                                                    'dateString': '$start_date'
                                                }
                                            }, datetime.datetime.strptime(specified_date, "%Y-%m-%d")
                                        ]
                                    },
                                    {
                                        '$gte': [
                                            {
                                                '$dateFromString': {
                                                    'dateString': '$end_date'
                                                }
                                            }, datetime.datetime.strptime(specified_date, "%Y-%m-%d")
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    mine_filter,  # Add mine filter
                    consumer_type_filter,  # Add consumer_type filter
                    search_filter,  # Add search_text filter if any
                    {
                        '$group': {
                            '_id': '$do_no',
                            'mine_name': {
                                '$first': '$mine_name'
                            },
                            'do_qty': {
                                '$sum': {
                                    '$toDouble': '$do_qty'
                                }
                            },
                            'start_date': {
                                '$first': '$start_date'
                            },
                            'end_date': {
                                '$first': '$end_date'
                            },
                            'source_type': {
                                '$first': '$source'
                            },
                            'slno': {
                                '$first': '$slno'
                            }
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
                            'slno': 1
                        }
                    },
                ]

                saprecordsPipeline = [stage for stage in saprecordsPipeline if stage]


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
                    mine_filter,  # Add mine filter
                    consumer_type_filter,  # Add consumer_type filter
                    search_filter,  # Add search_text filter if any
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

                saprecordsRcrRoadPipeline = [stage for stage in saprecordsRcrRoadPipeline if stage]


                # saprecordsPipeline = [stage for stage in saprecordsPipeline if stage]

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
                    dictData["challan_lr_/_qty"] = 0
                    # dictData["challan_lr_/_qty"] = round(singleData.get("challan_lr_qty"), 2)
                    dictData["cumulative_challan_lr_/_qty"] = round(singleData.get("cumulative_challan_lr_qty"), 2)
                    # dictData["grade"] = singleData.get("Grade")
                    dictData["grn_status"] = singleData.get("grn_status")
                    dictData["date"] = singleData.get("date").strftime("%Y-%m-%d")
                    if singleData.get("start_date"):
                        dictData["start_date"] = singleData.get("start_date")
                    else:
                        dictData["start_date"] = "N/A"
                    if singleData.get("end_date"):
                        dictData["end_date"] = singleData.get("end_date")
                    else:
                        dictData["end_date"] = "N/A"
                    dictData["source_type"] = singleData.get("type_consumer")
                    # dictData["month"] = singleData.get("slno")
                    dictData["month"] = datetime.datetime.strptime(singleData.get("slno"), "%Y%m").strftime("%B %Y") if singleData.get("slno") else "-"
                    if singleData.get("Grade") is not None:
                        if '-' in singleData.get("Grade"):
                            dictData["average_GCV_Grade"] = singleData.get("Grade").split("-")[0]
                        elif " " in singleData.get("Grade"):
                            dictData["average_GCV_Grade"] = singleData.get("Grade").split(" ")[0]
                        else:
                            dictData["average_GCV_Grade"] = singleData.get("Grade")
                    else:
                        dictData["average_GCV_Grade"] = "N/A"
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
                        dictDataHIstoric["DO_Qty"] = float(singleDataHistoric.get("do_qty"))
                    else:
                        dictDataHIstoric["DO_Qty"] = 0
                    dictDataHIstoric["challan_lr_/_qty"] = 0
                    # dictDataHIstoric["DO_Qty"] = int(singleDataHistoric.get("do_qty"))
                    # dictDataHIstoric["challan_lr_/_qty"] = round(singleDataHistoric.get("challan_lr_qty"), 2)
                    dictDataHIstoric["cumulative_challan_lr_/_qty"] = round(singleDataHistoric.get("cumulative_challan_lr_qty"), 2)
                    # dictDataHIstoric["grade"] = singleDataHistoric.get("Grade")
                    dictDataHIstoric["grn_status"] = singleDataHistoric.get("grn_status")
                    dictDataHIstoric["date"] = singleDataHistoric.get("date").strftime("%Y-%m-%d")
                    if singleDataHistoric.get("start_date"):
                        dictDataHIstoric["start_date"] = singleDataHistoric.get("start_date")
                    else:
                        dictDataHIstoric["start_date"] = "N/A"
                    if singleDataHistoric.get("end_date"):
                        dictDataHIstoric["end_date"] = singleDataHistoric.get("end_date")
                    else:
                        dictDataHIstoric["end_date"] = "N/A"
                    dictDataHIstoric["source_type"] = singleDataHistoric.get("type_consumer")
                    # dictDataHIstoric["month"] = singleDataHistoric.get("slno")
                    dictDataHIstoric["month"] = datetime.datetime.strptime(singleDataHistoric.get("slno"), "%Y%m").strftime("%B %Y") if singleDataHistoric.get("slno") else "-"
                    if singleDataHistoric.get("Grade") is not None:
                        if '-' in singleDataHistoric.get("Grade"):
                            dictDataHIstoric["average_GCV_Grade"] = singleDataHistoric.get("Grade").split("-")[0]
                        elif " " in singleDataHistoric.get("Grade"):
                            dictDataHIstoric["average_GCV_Grade"] = singleDataHistoric.get("Grade").split(" ")[0]
                        else:
                            dictDataHIstoric["average_GCV_Grade"] = singleDataHistoric.get("Grade")
                    else:
                        dictDataHIstoric["average_GCV_Grade"] = "N/A"
                    if singleDataHistoric.get("start_date") is not None and singleDataHistoric.get("end_date") is not None:
                        tomorrow_date = datetime.datetime.strptime(singleDataHistoric.get("end_date"), "%Y-%m-%d").date() + datetime.timedelta(days=1)
                        balance_days = tomorrow_date - datetime.datetime.strptime(specified_date, "%Y-%m-%d").date()
                        dictDataHIstoric["balance_days"] = balance_days.days
                    else:
                        dictDataHIstoric["balance_days"] = 0

                    if singleDataHistoric.get("do_qty") is not None:
                        single_do_qty = singleDataHistoric.get("do_qty")
                    else:
                        single_do_qty = 0

                    if single_do_qty != 0:
                        dictDataHIstoric['percent_supply'] = round((singleDataHistoric.get('cumulative_challan_lr_qty') / float(single_do_qty)) * 100, 2)
                    else:
                        dictDataHIstoric['percent_supply'] = 0
                        
                    dictDataHIstoric["balance_qty"] = round(float(single_do_qty) - singleDataHistoric.get("cumulative_challan_lr_qty"), 2)
                        
                    if dictDataHIstoric['balance_days'] and dictDataHIstoric['balance_qty'] != 0:
                        dictDataHIstoric['asking_rate'] = round(dictDataHIstoric['balance_qty'] / dictDataHIstoric['balance_days'], 2)
                    else:
                        dictDataHIstoric["asking_rate"] = 0
                    
                    do_no_exists = any(item['DO_No'] == dictDataHIstoric["DO_No"] for item in listData)

                    if not do_no_exists:
                        listData.append(dictDataHIstoric)

                for singleDataRcrData in fetchRcrRoadData:
                    dictDataRcr = {}
                    dictDataRcr["DO_No"] = singleDataRcrData.get("_id")
                    dictDataRcr["mine_name"] = singleDataRcrData.get("mine_name")
                    if singleDataRcrData.get("do_qty"):
                        dictDataRcr["DO_Qty"] = int(singleDataRcrData.get("do_qty"))
                    else:
                        dictDataRcr["DO_Qty"] = 0
                    dictDataRcr["challan_lr_/_qty"] = 0
                    dictDataRcr["cumulative_challan_lr_/_qty"] = round(singleDataRcrData.get("cumulative_challan_lr_qty"), 2)
                    dictDataRcr["grn_status"] = singleDataRcrData.get("grn_status")
                    dictDataRcr["date"] = singleDataRcrData.get("date").strftime("%Y-%m-%d")
                    dictDataRcr["start_date"] = singleDataRcrData.get("start_date")
                    dictDataRcr["end_date"] = singleDataRcrData.get("end_date")
                    dictDataRcr["source_type"] = singleDataRcrData.get("type_consumer")
                    dictDataRcr["month"] = datetime.datetime.strptime(singleDataRcrData.get("slno"), "%Y%m").strftime("%B %Y") if singleDataRcrData.get("slno") else "-"
                    if singleDataRcrData.get("Grade") is not None:
                        if '-' in singleDataRcrData.get("Grade"):
                            dictDataRcr["average_GCV_Grade"] = singleDataRcrData.get("Grade").split("-")[0]
                        elif " " in singleDataRcrData.get("Grade"):
                            dictDataRcr["average_GCV_Grade"] = singleDataRcrData.get("Grade").split(" ")[0]
                        else:
                            dictDataRcr["average_GCV_Grade"] = singleDataRcrData.get("Grade")
                    else:
                        dictDataRcr["average_GCV_Grade"] = "N/A"
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

                    do_no_existsRcr = any(item['DO_No'] == dictDataRcr["DO_No"] for item in listData)

                    if not do_no_existsRcr:
                        listData.append(dictDataRcr)
                
                for saprecordsSingle in fetchSapRecordsData:
                    sapdict = {}
                    sapdict["DO_No"] = saprecordsSingle.get("_id")
                    sapdict["mine_name"] = saprecordsSingle.get("mine_name")
                    sapdict["DO_Qty"] = int(saprecordsSingle.get("do_qty"))
                    sapdict["start_date"] = saprecordsSingle.get("start_date")
                    sapdict["end_date"] = saprecordsSingle.get("end_date")
                    sapdict["source_type"] = saprecordsSingle.get("source_type")
                    sapdict["challan_lr_/_qty"] = 0
                    sapdict["cumulative_challan_lr_/_qty"] = 0
                    sapdict["date"] = "N/A"
                    sapdict["month"] = datetime.datetime.strptime(saprecordsSingle.get("slno"), "%Y%m").strftime("%B %Y") if saprecordsSingle.get("slno") else "-"
                    if saprecordsSingle.get("Grade") is not None:
                        if '-' in saprecordsSingle.get("Grade"):
                            sapdict["average_GCV_Grade"] = saprecordsSingle.get("Grade").split("-")[0]
                        elif " " in saprecordsSingle.get("Grade"):
                            sapdict["average_GCV_Grade"] = saprecordsSingle.get("Grade").split(" ")[0]
                        else:
                            sapdict["average_GCV_Grade"] = saprecordsSingle.get("Grade")
                    else:
                        sapdict["average_GCV_Grade"] = "N/A"
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
                        sapdict['percent_supply'] = round((sapdict["cumulative_challan_lr_/_qty"] / int(do_qty_val)) * 100, 2)
                    else:
                        sapdict['percent_supply'] = 0
                    sapdict["balance_qty"] = round(int(do_qty_val) - sapdict["cumulative_challan_lr_/_qty"], 2)
                    if sapdict['balance_days'] and sapdict['balance_qty'] != 0:
                        sapdict['asking_rate'] = round(sapdict['balance_qty'] / sapdict['balance_days'], 2)
                    else:
                        sapdict["asking_rate"] = 0

                    sap_do_no_exists = any(item['DO_No'] == sapdict["DO_No"] for item in listData)

                    if not sap_do_no_exists:
                        listData.append(sapdict)


                for saprecordsRcrSingle in fetchSapRecordsRcrData:
                    sapdictRcr = {}
                    sapdictRcr["DO_No"] = saprecordsRcrSingle.get("_id")
                    sapdictRcr["mine_name"] = saprecordsRcrSingle.get("mine_name")
                    sapdictRcr["DO_Qty"] = int(saprecordsRcrSingle.get("do_qty"))
                    sapdictRcr["start_date"] = saprecordsRcrSingle.get("start_date")
                    sapdictRcr["end_date"] = saprecordsRcrSingle.get("end_date")
                    sapdictRcr["source_type"] = saprecordsRcrSingle.get("source_type")
                    sapdictRcr["challan_lr_/_qty"] = 0
                    sapdictRcr["date"] = "N/A"
                    sapdictRcr["cumulative_challan_lr_/_qty"] = 0
                    sapdictRcr["month"] = datetime.datetime.strptime(saprecordsRcrSingle.get("slno"), "%Y%m").strftime("%B %Y") if saprecordsRcrSingle.get("slno") else "-"
                    if saprecordsRcrSingle.get("Grade") is not None:
                        if '-' in saprecordsRcrSingle.get("Grade"):
                            sapdictRcr["average_GCV_Grade"] = saprecordsRcrSingle.get("Grade").split("-")[0]
                        elif " " in saprecordsRcrSingle.get("Grade"):
                            sapdictRcr["average_GCV_Grade"] = saprecordsRcrSingle.get("Grade").split(" ")[0]
                        else:
                            sapdictRcr["average_GCV_Grade"] = saprecordsRcrSingle.get("Grade")
                    else:
                        sapdictRcr["average_GCV_Grade"] = "N/A"
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
                        sapdictRcr['percent_supply'] = round((sapdictRcr["cumulative_challan_lr_/_qty"] / int(do_qty_val)) * 100, 2)
                    else:
                        sapdictRcr['percent_supply'] = 0
                    sapdictRcr["balance_qty"] = round(int(do_qty_val) - sapdictRcr["cumulative_challan_lr_/_qty"], 2)
                    if sapdictRcr['balance_days'] and sapdictRcr['balance_qty'] != 0:
                        sapdictRcr['asking_rate'] = round(sapdictRcr['balance_qty'] / sapdictRcr['balance_days'], 2)
                    else:
                        sapdictRcr["asking_rate"] = 0

                    sap_do_no_rcr_exists = any(item['DO_No'] == sapdictRcr.get("DO_No") for item in listData)
                    
                    if not sap_do_no_rcr_exists:
                        listData.append(sapdictRcr)

                
                for singlelrqtyData in fetchGmrDatachallanltqty:
                    dictDatalrQty = {}
                    dictDatalrQty["DO_No"] = singlelrqtyData.get("_id")
                    dictDatalrQty["mine_name"] = singlelrqtyData.get("mine_name")
                    if singlelrqtyData.get("do_qty"):
                        dictDatalrQty["DO_Qty"] = int(float(singlelrqtyData.get("do_qty")))
                    else:
                        dictDatalrQty["DO_Qty"] = 0
                    if singlelrqtyData.get("challan_lr_qty"):
                        dictDatalrQty["challan_lr_/_qty"] = round(singlelrqtyData.get("challan_lr_qty"), 2)
                    else:
                        dictDatalrQty["challan_lr_/_qty"] = 0
                    
                    # Check if there is an item with the same DO_No in listData
                    do_no_exists_historic = next((item for item in listData if item["DO_No"] == dictDatalrQty["DO_No"]), None)
                    
                    # If it exists, update the "challan_lr_qty" in listData
                    if do_no_exists_historic:
                        do_no_exists_historic["challan_lr_/_qty"] = dictDatalrQty["challan_lr_/_qty"]

                for singlehistoriclrqty in fetchGmrHistoricDataChallanLrQty:
                    dictDatahistoriclrQty = {}
                    dictDatahistoriclrQty["DO_No"] = singlehistoriclrqty.get("_id")
                    dictDatahistoriclrQty["mine_name"] = singlehistoriclrqty.get("mine_name")
                    if singlehistoriclrqty.get("do_qty"):
                        dictDatahistoriclrQty["DO_Qty"] = int(float(singlehistoriclrqty.get("do_qty")))
                    else:
                        dictDatahistoriclrQty["DO_Qty"] = 0
                    if singlehistoriclrqty.get("challan_lr_qty"):
                        dictDatahistoriclrQty["challan_lr_/_qty"] = round(singlehistoriclrqty.get("challan_lr_qty"), 2)
                    else:
                        dictDatahistoriclrQty["challan_lr_/_qty"] = 0
                    
                    # Check if there is an item with the same DO_No in listData
                    do_no_exists_historic = next((item for item in listData if item["DO_No"] == dictDatahistoriclrQty["DO_No"]), None)
                    
                    # If it exists, update the "challan_lr_qty" in listData
                    if do_no_exists_historic:
                        do_no_exists_historic["challan_lr_/_qty"] = dictDatahistoriclrQty["challan_lr_/_qty"]


                
                for singlercrroadlrqty in fetchRcrRoadchallanlrqty:
                    dictDataRcrRoadlrQty = {}
                    dictDataRcrRoadlrQty["DO_No"] = singlercrroadlrqty.get("_id")
                    dictDataRcrRoadlrQty["mine_name"] = singlercrroadlrqty.get("mine_name")
                    console_logger.debug(singlercrroadlrqty.get("do_qty"))
                    if singlercrroadlrqty.get("do_qty"):
                        dictDataRcrRoadlrQty["DO_Qty"] = int(float(singlercrroadlrqty.get("do_qty")))
                    else:
                        dictDataRcrRoadlrQty["DO_Qty"] = 0
                    if singlercrroadlrqty.get("challan_lr_qty"):
                        dictDataRcrRoadlrQty["challan_lr_/_qty"] = round(singlercrroadlrqty.get("challan_lr_qty"), 2)
                    else:
                        dictDataRcrRoadlrQty["challan_lr_/_qty"] = 0
                    
                    # Check if there is an item with the same DO_No in listData
                    do_no_exists_rcr_road = next((item for item in listData if item["DO_No"] == dictDataRcrRoadlrQty["DO_No"]), None)
                    
                    # If it exists, update the "challan_lr_qty" in listData
                    if do_no_exists_rcr_road:
                        do_no_exists_historic["challan_lr_/_qty"] = dictDataRcrRoadlrQty["challan_lr_/_qty"]
                
                # final_data = [
                #     d for d in listData 
                #     if d['start_date'] is not None and datetime.datetime.strptime(d['start_date'], '%Y-%m-%d').date() <= datetime.datetime.now().date()
                # ]

                # final_data = listData
                final_data_check = [
                    d for d in listData
                    if d['end_date'] not in [None, "N/A"] and 
                    (datetime.datetime.strptime(d['end_date'], '%Y-%m-%d') + datetime.timedelta(days=2)) > datetime.datetime.strptime(specified_date, "%Y-%m-%d")
                ]

                filtered_data = []
                for single_data_percent in final_data_check:
                    percent_supply = single_data_percent.get('percent_supply')

                    # Check for percent_supply greater than or equal to 100.0
                    if percent_supply >= 100.0:
                        # Query Gmrdata with the DO_No
                        fetchGmrData = Gmrdata.objects(arv_cum_do_number=single_data_percent.get("DO_No")).order_by("-GWEL_Tare_Time").first()
                        
                        # Check if data exists and GWEL_Tare_Time is not None
                        if fetchGmrData is not None and fetchGmrData.GWEL_Tare_Time:
                            # Compare GWEL_Tare_Time + 2 days with today's date
                            if (fetchGmrData.GWEL_Tare_Time + datetime.timedelta(days=2)) < datetime.datetime.now():
                                # console_logger.debug("Data removed due to GWEL_Tare_Time being older than today's date.")
                                continue  # Skip entry
                        
                    # If not removed, append to filtered_data
                    filtered_data.append(single_data_percent)

                fetchConsumerType = roadjourneyconsumertype.objects.get(mode="road")
                ordered_source_types = fetchConsumerType.consumer_type[:9]

                grouped_data = defaultdict(list)
                for single_data in filtered_data:
                    source_type = single_data.get("source_type")
                    grouped_data[source_type].append(single_data)
                
                # Create a dictionary to hold entries sorted by the specified order
                sorted_data = {key: grouped_data[key] for key in ordered_source_types if key in grouped_data}

                # Add any remaining source_types not in ordered_source_types at the end
                extra_types = {key: grouped_data[key] for key in grouped_data if key not in ordered_source_types}
                sorted_data.update(extra_types)

                finallistData = [singlecheck for datavalue in sorted_data.values() for singlecheck in datavalue]

                result["labels"] = ["month", "DO_No", "mine_name", "DO_Qty", "date", "challan_lr_/_qty", "cumulative_challan_lr_/_qty","balance_qty", "percent_supply", "asking_rate", "average_GCV_Grade", "start_date", "end_date", "balance_days"]
                result["total"] = len(finallistData)

                start_idx = (page_no - 1) * page_len
                end_idx = start_idx + page_len
                paginated_data = finallistData[start_idx:end_idx]
                result["datasets"] = paginated_data
                return result
            elif type and type == "download":
                del type
                file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
                target_directory = f"static_server/gmr_ai/{file}"
                os.umask(0)
                os.makedirs(target_directory, exist_ok=True, mode=0o777)
                page_no = 1
                page_len = result["page_size"]

                if currentPage:
                    page_no = currentPage

                if perPage:
                    page_len = perPage
                    result["page_size"] = perPage
                
                skip_value = (page_no - 1) * page_len

                # Apply filters based on specified conditions
                if mine and mine != "All":
                    mine_filter = {'$match': {'mine': {'$regex': f'{mine.upper()}', '$options': 'i'}}}
                else:
                    mine_filter = {}

                if consumer_type and consumer_type != "All":
                    consumer_type_filter = {'$match': {'type_consumer': consumer_type}}
                else:
                    consumer_type_filter = {}

                if search_text:
                    if search_text.isdigit():
                        search_filter = {'$match': {'arv_cum_do_number': {'$regex': f'{search_text}', '$options': 'i'}}}
                    else:
                        search_filter = {'$match': {'mine': {'$regex': f'{search_text}', '$options': 'i'}}}
                else:
                    search_filter = {}

                # Date filter using specified_date
                if specified_date:
                    from_ts = convert_to_utc_format(f'{specified_date} 00:00:00', "%Y-%m-%d %H:%M:%S")
                    to_ts = convert_to_utc_format(f'{specified_date} 23:59:59', "%Y-%m-%d %H:%M:%S")
                    date_filter = {
                        '$match': {
                            'GWEL_Tare_Time': {
                                '$ne': None,
                                # '$gte': from_ts,
                                '$lte': to_ts
                            }
                        }
                    }
                    challan_date_filter = {
                        '$match': {
                            'GWEL_Tare_Time': {
                                '$ne': None,
                                '$gte': from_ts,
                                '$lte': to_ts
                            }
                        }
                    }
                else:
                    date_filter = {}
                    challan_date_filter = {}
                created_at_date = datetime.datetime(2024, 9, 23, 19, 50, 51, 572000)
                basePipeline = [
                    {
                        '$match': {
                            'created_at': {
                                '$gt': created_at_date,
                            }
                        }
                    },
                    date_filter,  # Add date filter
                    mine_filter,  # Add mine filter
                    consumer_type_filter,  # Add consumer_type filter
                    search_filter,  # Add search_text filter if any
                    {
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
                    },
                    {
                        '$lookup': {
                            'from': 'gmrdata', 
                            'let': {
                                'do_number': '$_id'
                            }, 
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$arv_cum_do_number', '$$do_number'
                                                    ]
                                                }, {
                                                    '$ne': [
                                                        '$GWEL_Tare_Time', None
                                                    ]
                                                }, {
                                                    '$lte': [
                                                        '$GWEL_Tare_Time', to_ts
                                                    ]
                                                }, {
                                                    '$gt': [
                                                        '$created_at', created_at_date
                                                    ]
                                                }
                                            ]
                                        }
                                    }
                                }
                            ], 
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
                                            '$toDouble': '$$item.net_qty'
                                        }
                                    }
                                }
                            }
                        }
                    },
                ]

                # Remove empty filters from the pipeline
                basePipeline = [stage for stage in basePipeline if stage] 

                challanlrqtybasePipeline = [
                    {
                        '$match': {
                            'created_at': {
                                '$gt': created_at_date,
                            }
                        }
                    },
                    challan_date_filter,  # Add date filter
                    mine_filter,  # Add mine filter
                    consumer_type_filter,  # Add consumer_type filter
                    search_filter,  # Add search_text filter if any
                    {
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
                    },
                    {
                        '$lookup': {
                            'from': 'gmrdata', 
                            'let': {
                                'do_number': '$_id'
                            }, 
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$arv_cum_do_number', '$$do_number'
                                                    ]
                                                }, {
                                                    '$ne': [
                                                        '$GWEL_Tare_Time', None
                                                    ]
                                                }, {
                                                    '$lte': [
                                                        '$GWEL_Tare_Time', to_ts
                                                    ]
                                                }, {
                                                    '$gt': [
                                                        '$created_at', created_at_date
                                                    ]
                                                }
                                            ]
                                        }
                                    }
                                }
                            ], 
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


                challanlrqtybasePipeline = [stage for stage in challanlrqtybasePipeline if stage]

                basePipelineHistoric = [
                    date_filter,  # Add date filter
                    mine_filter,  # Add mine filter
                    consumer_type_filter,  # Add consumer_type filter
                    search_filter,  # Add search_text filter if any
                    {
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
                    },
                    {
                        '$lookup': {
                            'from': 'gmrdataHistoric', 
                            'let': {
                                'do_number': '$_id'
                            }, 
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$arv_cum_do_number', '$$do_number'
                                                    ]
                                                }, {
                                                    '$ne': [
                                                        '$GWEL_Tare_Time', None
                                                    ]
                                                }, {
                                                    '$lte': [
                                                        '$GWEL_Tare_Time', to_ts
                                                    ]
                                                }, {
                                                    '$gt': [
                                                        '$created_at', created_at_date
                                                    ]
                                                }
                                            ]
                                        }
                                    }
                                }
                            ], 
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
                                            '$toDouble': '$$item.net_qty'
                                        }
                                    }
                                }
                            }
                        }
                    },
                    {'$skip': skip_value},
                    {'$limit': page_len}
                ]

                # Remove empty filters from the pipeline
                basePipelineHistoric = [stage for stage in basePipelineHistoric if stage]

                basepipelineHistoricChallanLrQty = [
                    challan_date_filter,  # Add date filter
                    mine_filter,  # Add mine filter
                    consumer_type_filter,  # Add consumer_type filter
                    search_filter,  # Add search_text filter if any
                    {
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
                    },
                    {
                        '$lookup': {
                            'from': 'gmrdataHistoric', 
                            'let': {
                                'do_number': '$_id'
                            }, 
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$arv_cum_do_number', '$$do_number'
                                                    ]
                                                }, {
                                                    '$ne': [
                                                        '$GWEL_Tare_Time', None
                                                    ]
                                                }, {
                                                    '$lte': [
                                                        '$GWEL_Tare_Time', to_ts
                                                    ]
                                                }, {
                                                    '$gt': [
                                                        '$created_at', created_at_date
                                                    ]
                                                }
                                            ]
                                        }
                                    }
                                }
                            ], 
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

                # Remove empty filters from the pipeline
                basepipelineHistoricChallanLrQty = [stage for stage in basepipelineHistoricChallanLrQty if stage]


                basePipelineRcrRoad = [
                    date_filter,  # Add date filter
                    mine_filter,  # Add mine filter
                    consumer_type_filter,  # Add consumer_type filter
                    search_filter,  # Add search_text filter if any
                    {
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
                    },
                    {
                        '$lookup': {
                            'from': 'RcrRoadData', 
                            'let': {
                                'do_number': '$_id'
                            }, 
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$arv_cum_do_number', '$$do_number'
                                                    ]
                                                }, {
                                                    '$ne': [
                                                        '$GWEL_Tare_Time', None
                                                    ]
                                                }, {
                                                    '$lte': [
                                                        '$GWEL_Tare_Time', to_ts
                                                    ]
                                                }, {
                                                    '$gt': [
                                                        '$created_at', created_at_date
                                                    ]
                                                }
                                            ]
                                        }
                                    }
                                }
                            ], 
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

                basePipelineRcrRoad = [stage for stage in basePipelineRcrRoad if stage]

                basepipelineRcrRoadChallanLrQty = [
                    challan_date_filter,  # Add date filter
                    mine_filter,  # Add mine filter
                    consumer_type_filter,  # Add consumer_type filter
                    search_filter,  # Add search_text filter if any
                    {
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
                    }, 
                    {
                        '$lookup': {
                            'from': 'RcrRoadData', 
                            'let': {
                                'do_number': '$_id'
                            }, 
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$arv_cum_do_number', '$$do_number'
                                                    ]
                                                }, {
                                                    '$ne': [
                                                        '$GWEL_Tare_Time', None
                                                    ]
                                                }, {
                                                    '$lte': [
                                                        '$GWEL_Tare_Time', to_ts
                                                    ]
                                                }, {
                                                    '$gt': [
                                                        '$created_at', created_at_date
                                                    ]
                                                }
                                            ]
                                        }
                                    }
                                }
                            ], 
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

                basepipelineRcrRoadChallanLrQty = [stage for stage in basepipelineRcrRoadChallanLrQty if stage]


                saprecordsPipeline = [
                    {
                        '$match': {
                            '$expr': {
                                '$and': [
                                    {
                                        '$gte': [
                                            {
                                                '$dateFromString': {
                                                    'dateString': '$start_date'
                                                }
                                            }, specified_date
                                        ]
                                    },
                                    {
                                        '$lte': [
                                            {
                                                '$dateFromString': {
                                                    'dateString': '$end_date'
                                                }
                                            }, specified_date
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                    mine_filter,  # Add mine filter
                    consumer_type_filter,  # Add consumer_type filter
                    search_filter,  # Add search_text filter if any
                    {
                        '$group': {
                            '_id': '$do_no',
                            'mine_name': {
                                '$first': '$mine_name'
                            },
                            'do_qty': {
                                '$sum': {
                                    '$toDouble': '$do_qty'
                                }
                            },
                            'start_date': {
                                '$first': '$start_date'
                            },
                            'end_date': {
                                '$first': '$end_date'
                            },
                            'source_type': {
                                '$first': '$source'
                            },
                            'slno': {
                                '$first': '$slno'
                            }
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
                            'slno': 1
                        }
                    },
                ]

                # Remove empty filters from the pipeline
                saprecordsPipeline = [stage for stage in saprecordsPipeline if stage]

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
                    mine_filter,  # Add mine filter
                    consumer_type_filter,  # Add consumer_type filter
                    search_filter,  # Add search_text filter if any
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

                saprecordsRcrRoadPipeline = [stage for stage in saprecordsRcrRoadPipeline if stage]

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
                    # dictData["challan_lr_/_qty"] = round(singleData.get("challan_lr_qty"), 2)
                    dictData["cumulative_challan_lr_/_qty"] = round(singleData.get("cumulative_challan_lr_qty"), 2)
                    # dictData["grade"] = singleData.get("Grade")
                    dictData["grn_status"] = singleData.get("grn_status")
                    dictData["date"] = singleData.get("date").strftime("%Y-%m-%d")
                    if singleData.get("start_date"):
                        dictData["start_date"] = singleData.get("start_date")
                    else:
                        dictData["start_date"] = "N/A"
                    if singleData.get("end_date"):
                        dictData["end_date"] = singleData.get("end_date")
                    else:
                        dictData["end_date"] = "N/A"
                    dictData["source_type"] = singleData.get("type_consumer")
                    # dictData["month"] = singleData.get("slno")
                    dictData["slno"] = datetime.datetime.strptime(singleData.get("slno"), "%Y%m").strftime("%B %Y") if singleData.get("slno") else "-"
                    if singleData.get("Grade") is not None:
                        if '-' in singleData.get("Grade"):
                            dictData["average_GCV_Grade"] = singleData.get("Grade").split("-")[0]
                        elif " " in singleData.get("Grade"):
                            dictData["average_GCV_Grade"] = singleData.get("Grade").split(" ")[0]
                        else:
                            dictData["average_GCV_Grade"] = singleData.get("Grade")
                    else:
                        dictData["average_GCV_Grade"] = "N/A"
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
                        dictDataHIstoric["DO_Qty"] = float(singleDataHistoric.get("do_qty"))
                    else:
                        dictDataHIstoric["DO_Qty"] = 0
                    # dictDataHIstoric["DO_Qty"] = int(singleDataHistoric.get("do_qty"))
                    # dictDataHIstoric["challan_lr_/_qty"] = round(singleDataHistoric.get("challan_lr_qty"), 2)
                    dictDataHIstoric["cumulative_challan_lr_/_qty"] = round(singleDataHistoric.get("cumulative_challan_lr_qty"), 2)
                    # dictDataHIstoric["grade"] = singleDataHistoric.get("Grade")
                    dictDataHIstoric["grn_status"] = singleDataHistoric.get("grn_status")
                    dictDataHIstoric["date"] = singleDataHistoric.get("date").strftime("%Y-%m-%d")
                    if singleDataHistoric.get("start_date"):
                        dictDataHIstoric["start_date"] = singleDataHistoric.get("start_date")
                    else:
                        dictDataHIstoric["start_date"] = "N/A"
                    if singleDataHistoric.get("end_date"):
                        dictDataHIstoric["end_date"] = singleDataHistoric.get("end_date")
                    else:
                        dictDataHIstoric["end_date"] = "N/A"
                    dictDataHIstoric["source_type"] = singleDataHistoric.get("type_consumer")
                    # dictDataHIstoric["month"] = singleDataHistoric.get("slno")
                    dictDataHIstoric["slno"] = datetime.datetime.strptime(singleDataHistoric.get("slno"), "%Y%m").strftime("%B %Y") if singleDataHistoric.get("slno") else "-"
                    if singleDataHistoric.get("Grade") is not None:
                        if '-' in singleDataHistoric.get("Grade"):
                            dictDataHIstoric["average_GCV_Grade"] = singleDataHistoric.get("Grade").split("-")[0]
                        elif " " in singleDataHistoric.get("Grade"):
                            dictDataHIstoric["average_GCV_Grade"] = singleDataHistoric.get("Grade").split(" ")[0]
                        else:
                            dictDataHIstoric["average_GCV_Grade"] = singleDataHistoric.get("Grade")
                    else:
                        dictDataHIstoric["average_GCV_Grade"] = "N/A"
                    if singleDataHistoric.get("start_date") is not None and singleDataHistoric.get("end_date") is not None:
                        tomorrow_date = datetime.datetime.strptime(singleDataHistoric.get("end_date"), "%Y-%m-%d").date() + datetime.timedelta(days=1)
                        balance_days = tomorrow_date - datetime.datetime.strptime(specified_date, "%Y-%m-%d").date()
                        dictDataHIstoric["balance_days"] = balance_days.days
                    else:
                        dictDataHIstoric["balance_days"] = 0

                    if singleDataHistoric.get("do_qty") is not None:
                        single_do_qty = singleDataHistoric.get("do_qty")
                    else:
                        single_do_qty = 0

                    if single_do_qty != 0:
                        dictDataHIstoric['percent_supply'] = round((singleDataHistoric.get('cumulative_challan_lr_qty') / float(single_do_qty)) * 100, 2)
                    else:
                        dictDataHIstoric['percent_supply'] = 0
                        
                    dictDataHIstoric["balance_qty"] = round(float(single_do_qty) - singleDataHistoric.get("cumulative_challan_lr_qty"), 2)
                        
                    if dictDataHIstoric['balance_days'] and dictDataHIstoric['balance_qty'] != 0:
                        dictDataHIstoric['asking_rate'] = round(dictDataHIstoric['balance_qty'] / dictDataHIstoric['balance_days'], 2)
                    else:
                        dictDataHIstoric["asking_rate"] = 0
                    
                    do_no_exists = any(item['DO_No'] == dictDataHIstoric["DO_No"] for item in listData)
                    
                    if not do_no_exists:
                        # console_logger.debug("DO_No does not exist in final_data.")
                        listData.append(dictDataHIstoric)

                for singleDataRcrData in fetchRcrRoadData:
                    dictDataRcr = {}
                    dictDataRcr["DO_No"] = singleDataRcrData.get("_id")
                    dictDataRcr["mine_name"] = singleDataRcrData.get("mine_name")
                    if singleDataRcrData.get("do_qty"):
                        dictDataRcr["DO_Qty"] = int(singleDataRcrData.get("do_qty"))
                    else:
                        dictDataRcr["DO_Qty"] = 0
                    dictDataRcr["cumulative_challan_lr_/_qty"] = round(singleDataRcrData.get("cumulative_challan_lr_qty"), 2)
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
                    # listData.append(dictDataRcr)

                    do_no_existsRcr = any(item['DO_No'] == dictDataRcr["DO_No"] for item in listData)

                    if not do_no_existsRcr:
                        listData.append(dictDataRcr)
                
                for saprecordsSingle in fetchSapRecordsData:
                    sapdict = {}
                    sapdict["DO_No"] = saprecordsSingle.get("_id")
                    sapdict["mine_name"] = saprecordsSingle.get("mine_name")
                    sapdict["DO_Qty"] = int(saprecordsSingle.get("do_qty"))
                    sapdict["start_date"] = saprecordsSingle.get("start_date")
                    sapdict["end_date"] = saprecordsSingle.get("end_date")
                    sapdict["source_type"] = saprecordsSingle.get("source_type")
                    sapdict["challan_lr_/_qty"] = 0
                    sapdict["cumulative_challan_lr_/_qty"] = 0
                    sapdict["date"] = "N/A"
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
                    if saprecordsSingle.get("do_qty") is not None and sapdict["cumulative_challan_lr_/_qty"] != 0:
                        sapdict['percent_supply'] = round((sapdict["cumulative_challan_lr_/_qty"] / int(saprecordsSingle.get('do_qty'))) * 100, 2)
                    else:
                        sapdict["percent_supply"] = 0
                    if saprecordsSingle.get("do_qty") is not None and sapdict["cumulative_challan_lr_/_qty"] != 0:
                        sapdict["balance_qty"] = round(int(saprecordsSingle.get("do_qty")) - sapdict["cumulative_challan_lr_/_qty"], 2)
                    else:
                        sapdict["balance_qty"] = 0
                    if sapdict['balance_days'] and sapdict['balance_qty'] != 0:
                        sapdict['asking_rate'] = round(sapdict['balance_qty'] / sapdict['balance_days'], 2)
                    else:
                        sapdict["asking_rate"] = 0

                    sap_do_no_exists = any(item['DO_No'] == sapdict["DO_No"] for item in listData)

                    if not sap_do_no_exists:
                        listData.append(sapdict)


                for saprecordsRcrSingle in fetchSapRecordsRcrData:
                    sapdictRcr = {}
                    sapdictRcr["DO_No"] = saprecordsRcrSingle.get("_id")
                    sapdictRcr["mine_name"] = saprecordsRcrSingle.get("mine_name")
                    sapdictRcr["DO_Qty"] = int(saprecordsRcrSingle.get("do_qty"))
                    sapdictRcr["start_date"] = saprecordsRcrSingle.get("start_date")
                    sapdictRcr["end_date"] = saprecordsRcrSingle.get("end_date")
                    sapdictRcr["source_type"] = saprecordsRcrSingle.get("source_type")
                    sapdictRcr["challan_lr_/_qty"] = 0
                    sapdictRcr["date"] = "N/A"
                    sapdictRcr["cumulative_challan_lr_/_qty"] = 0
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
                        sapdictRcr['percent_supply'] = round((sapdictRcr["cumulative_challan_lr_/_qty"] / int(do_qty_val)) * 100, 2)
                    else:
                        sapdictRcr['percent_supply'] = 0
                    sapdictRcr["balance_qty"] = round(int(do_qty_val) - sapdictRcr["cumulative_challan_lr_/_qty"], 2)
                    if sapdictRcr['balance_days'] and sapdictRcr['balance_qty'] != 0:
                        sapdictRcr['asking_rate'] = round(sapdictRcr['balance_qty'] / sapdictRcr['balance_days'], 2)
                    else:
                        sapdictRcr["asking_rate"] = 0

                    sap_do_no_rcr_exists = any(item['DO_No'] == sapdictRcr.get("DO_No") for item in listData)
                    
                    if not sap_do_no_rcr_exists:
                        listData.append(sapdictRcr)

                for singlelrqtyData in fetchGmrDatachallanltqty:
                    dictDatalrQty = {}
                    dictDatalrQty["DO_No"] = singlelrqtyData.get("_id")
                    dictDatalrQty["mine_name"] = singlelrqtyData.get("mine_name")
                    if singlelrqtyData.get("do_qty"):
                        dictDatalrQty["DO_Qty"] = int(float(singlelrqtyData.get("do_qty")))
                    else:
                        dictDatalrQty["DO_Qty"] = 0
                    dictDatalrQty["challan_lr_/_qty"] = round(singlelrqtyData.get("challan_lr_qty"), 2)
                    
                    # Check if there is an item with the same DO_No in listData
                    do_no_exists_data = next((item for item in listData if item["DO_No"] == dictDatalrQty["DO_No"]), None)
                    
                    # If it exists, update the "challan_lr_qty" in listData
                    if do_no_exists_data:
                        do_no_exists_data["challan_lr_/_qty"] = dictDatalrQty["challan_lr_/_qty"]

                for singlehistoriclrqty in fetchGmrHistoricDataChallanLrQty:
                    dictDatahistoriclrQty = {}
                    dictDatahistoriclrQty["DO_No"] = singlehistoriclrqty.get("_id")
                    dictDatahistoriclrQty["mine_name"] = singlehistoriclrqty.get("mine_name")
                    if singlehistoriclrqty.get("do_qty"):
                        dictDatahistoriclrQty["DO_Qty"] = int(float(singlehistoriclrqty.get("do_qty")))
                    else:
                        dictDatahistoriclrQty["DO_Qty"] = 0
                    dictDatahistoriclrQty["challan_lr_/_qty"] = round(singlehistoriclrqty.get("challan_lr_qty"), 2)
                    
                    # Check if there is an item with the same DO_No in listData
                    do_no_exists_historic = next((item for item in listData if item["DO_No"] == dictDatahistoriclrQty["DO_No"]), None)
                    
                    # If it exists, update the "challan_lr_qty" in listData
                    if do_no_exists_historic:
                        do_no_exists_historic["challan_lr_/_qty"] = dictDatahistoriclrQty["challan_lr_/_qty"]


                
                for singlercrroadlrqty in fetchRcrRoadchallanlrqty:
                    dictDataRcrRoadlrQty = {}
                    dictDataRcrRoadlrQty["DO_No"] = singlercrroadlrqty.get("_id")
                    dictDataRcrRoadlrQty["mine_name"] = singlercrroadlrqty.get("mine_name")
                    if singlercrroadlrqty.get("do_qty"):
                        dictDataRcrRoadlrQty["DO_Qty"] = int(float(singlercrroadlrqty.get("do_qty")))
                    else:
                        dictDataRcrRoadlrQty["DO_Qty"] = 0
                    dictDataRcrRoadlrQty["challan_lr_/_qty"] = round(singlercrroadlrqty.get("challan_lr_qty"), 2)
                    
                    # Check if there is an item with the same DO_No in listData
                    do_no_exists_rcr_road = next((item for item in listData if item["DO_No"] == dictDataRcrRoadlrQty["DO_No"]), None)
                    
                    # If it exists, update the "challan_lr_qty" in listData
                    if do_no_exists_rcr_road:
                        do_no_exists_historic["challan_lr_/_qty"] = dictDataRcrRoadlrQty["challan_lr_/_qty"]

                logo_path = f"{os.path.join(os.getcwd(), 'static_server/receipt/report_logo.png')}"
                if listData:
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
                    cell_format2.set_align("vcenter")

                    cell_format2.set_text_wrap(True)
                    cell_format2.set_border(1)

                    header_format = workbook.add_format({'bold': True, 'font_size': 40, 'align': 'center'})
                    date_format = workbook.add_format({'align': 'center', 'font_size': 12, "bold": True})
                    report_name_format = workbook.add_format({'align': 'center', 'font_size': 15, "bold": True})
                    # report_name_format.set_text_wrap(True)

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
                    worksheet.merge_range("A1:M1", main_header, header_format)  # Merge cells A1 to H1 for the header
                    
                    # Write the current date on the left side (A2)
                    # worksheet.write("A2", f"Date: {datetime.datetime.now().strftime('%d-%m-%Y')}", date_format)
                    worksheet.merge_range("A2:B2", f"Date: {datetime.datetime.now().strftime('%d-%m-%Y')}", date_format)
                    worksheet.merge_range("C2:M2", f"Road Coal Logistics Report", report_name_format)

                    result["datasets"] = listData

                    headers = ["Month", "Mine Name", "DO No", "Grade", "DO Qty", "Challan Lr / Qty", "Cumulative Challan Lr / Qty", "Balance Qty", "% of Supply", "Balance Days", "Asking Rate", "Do Start Date", "Do End Date"]
                    
                    for index, header in enumerate(headers):
                        worksheet.write(2, index, header, cell_format2)
                    
                    row = 3
                    for single_data in result["datasets"]:
                        worksheet.write(row, 0, single_data["slno"], cell_format)
                        worksheet.write(row, 1, single_data["mine_name"], cell_format)
                        worksheet.write(row, 2, single_data["DO_No"], cell_format)
                        worksheet.write(row, 3, single_data["average_GCV_Grade"], cell_format)
                        worksheet.write(row, 4, single_data["DO_Qty"], cell_format)
                        if single_data.get("challan_lr_/_qty"):
                            worksheet.write(row, 5, single_data["challan_lr_/_qty"], cell_format)
                        else:
                            worksheet.write(row, 5, 0, cell_format)
                        worksheet.write(row, 6, single_data["cumulative_challan_lr_/_qty"], cell_format)
                        worksheet.write(row, 7, single_data["balance_qty"], cell_format)
                        worksheet.write(row, 8, single_data["percent_supply"], cell_format)
                        worksheet.write(row, 9, single_data["balance_days"], cell_format)
                        worksheet.write(row, 10, single_data["asking_rate"], cell_format)
                        worksheet.write(row, 11, single_data["start_date"], cell_format)
                        worksheet.write(row, 12, single_data["end_date"], cell_format)

                        # count -= 1
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
            error_handler(e)

    def cumulative_coal_lifted(self):
        try:
            start_date = f'{datetime.datetime.today().replace(day=1).strftime("%Y-%m-%d")} 00:00:00'
            startd_date = datetime.datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
            end_date = f'{datetime.datetime.today().strftime("%Y-%m-%d")} 23:59:59'
            endd_date = datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")

            rail_start_date = f'{datetime.datetime.today().replace(day=1).strftime("%Y-%m-%d")}T00:00'
            rail_end_date = f'{datetime.datetime.today().strftime("%Y-%m-%d")}T23:59'

            raild_start_date = datetime.datetime.strptime(rail_start_date, "%Y-%m-%dT%H:%M")
            raild_end_date = datetime.datetime.strptime(rail_end_date, "%Y-%m-%dT%H:%M")


            basePipeline = [
                {
                    '$match': {
                        'GWEL_Tare_Time': {
                            '$ne': None, 
                            '$gte': startd_date, 
                            '$lte': endd_date
                        }
                    }
                }, {
                    '$group': {
                        '_id': '$arv_cum_do_number', 
                        'actual_net_qty': {
                            '$sum': {
                                '$toDouble': '$actual_net_qty'
                            }
                        }
                    }
                }
            ]

            railBasePipeline = [
                {
                    '$match': {
                        'avery_placement_date': {
                            '$exists': True, 
                            '$ne': None
                        }
                    }
                }, {
                    '$addFields': {
                        'avery_placement_date': {
                            '$dateFromString': {
                                'dateString': '$avery_placement_date'
                            }
                        }
                    }
                }, {
                    '$match': {
                        'avery_placement_date': {
                            '$ne': None, 
                            '$gte': raild_start_date, 
                            '$lte': raild_end_date
                        }
                    }
                }, {
                    '$group': {
                        '_id': '$rr_no', 
                        'Total_gwel_net': {
                            '$sum': {
                                '$toDouble': '$Total_gwel_net'
                            }
                        }
                    }
                }, {
                    '$project': {
                        '_id': '$rr_no', 
                        'Total_gwel_net': {
                            '$ifNull': [
                                '$Total_gwel_net', 0
                            ]
                        }
                    }
                }
            ]

            fetchGmrData = Gmrdata.objects.aggregate(basePipeline)
            fetchRailData = RailData.objects.aggregate(railBasePipeline)

            gwel_road = 0
            for single_road_data in fetchGmrData:
                gwel_road += single_road_data.get("actual_net_qty")
            gwel_rail = 0
            for single_rail_data in fetchRailData:
                gwel_rail += single_rail_data.get("Total_gwel_net")

            final_count_gwel = gwel_road + gwel_rail

            return {"cumulative_coal_lifted": round(final_count_gwel, 2)}
        except Exception as e:
            error_handler(e)