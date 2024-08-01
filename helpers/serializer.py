from pydantic import BaseModel, validator, ValidationError, root_validator, EmailStr
from typing import Optional, List,Dict,Union
from enum import Enum



class bodyfield(BaseModel):
    vehicle_number : str
    company_id : str
    site_name : str
    location : str
    type : str
    device_name : str
    group_id : str
    visited_datetime : str
    vehicle_image : str
    number_plate : str

    @validator("*",pre=True)
    def validate_date(cls, v):
        print(cls, v)
        # if not len(values["some_list"]) < 2:
        #     values["some_date"] = values["some_list"][0]
        # return values


class HistorianData(BaseModel):
    StartTime: str 
    EndTime: str
    TagID: List[str]


class wclData(BaseModel):
    id: str
    coal_data: dict


class seclData(BaseModel):
    id: str
    coal_data: dict


class WCLtest(BaseModel):
    id: str
    coal_data: dict

class WCLtestMain(BaseModel):
    # data: List[WCLtest]
    data: List[dict]

class ReportEnum(str, Enum):
    daily = "daily"
    weekly = "weekly"
    monthly = "monthly"


class WeekDays(str, Enum):
    sunday = "sun"
    monday = "mon"
    tuesday = "tue"
    wednesday = "wed"
    thursday = "thu"
    friday = "fri"
    saturday = "sat"


class ReportInstantPostIn(BaseModel):
    report_name: str
    report_type: ReportEnum
    report_day: Optional[WeekDays] = None
    report_action: List[str]


class MisReportData(BaseModel):
    report_name: str
    recipient_list: List[str]
    cc_list: List[str]
    bcc_list: List[str]
    filter: str
    schedule: str
    shift_schedule: Optional[List] = []
    time: str

class AopTargetData(BaseModel):
    source_name: str
    aop_target: str

class SmtpSettingsPostIn(BaseModel):
    Smtp_ssl: bool
    Smtp_port: int
    Smtp_host: str
    Smtp_user: str
    Smtp_password: str
    Emails_from_email: EmailStr
    Emails_from_name: str

class LatLongPostIn(BaseModel):
    name: str
    latlong: List[float]
    type: str
    geofencing: List


class ShortMineName(BaseModel):
    mine_name: str
    short_code: str


class RequestData(BaseModel):
    Delivery_Challan_Number: str = None
    ARV_Cum_DO_Number: str = None
    Mine_Name: str = None
    Vehicle_Truck_Registration_No: str = None
    Net_Qty: str = None
    Delivery_Challan_Date: str = None
    Total_Net_Amount_of_Figures: str = None
    Chassis_No: Optional[str] = None
    Certificate_will_expire_on: str = None


class RailwayDataDetails(BaseModel):
    indexing: str
    wagon_owner: str
    wagon_type: str
    wagon_no: str
    secl_cc_wt: str
    secl_gross_wt: str
    secl_tare_wt: str
    secl_net_wt: str
    secl_ol_wt: str
    secl_ul_wt: str
    secl_chargable_wt: str
    rly_cc_wt: str
    rly_gross_wt: str
    rly_tare_wt: str
    rly_net_wt: str
    rly_permissible_cc_wt: str
    rly_ol_wt: str
    rly_norm_rate: str
    rly_pun_rate: str
    rly_chargable_wt: str
    rly_sliding_adjustment: str

class RailwayData(BaseModel):
    rr_no: str
    rr_qty: str
    po_no: str
    po_date: str
    line_item: str
    source: str
    placement_date: str
    completion_date: str
    drawn_date: str
    total_ul_wt: str
    boxes_supplied: str
    total_secl_gross_wt: str
    total_secl_tare_wt: str
    total_secl_net_wt: str
    total_secl_ol_wt: str
    boxes_loaded: str
    total_rly_gross_wt: str
    total_rly_tare_wt: str
    total_rly_net_wt: str
    total_rly_ol_wt: str
    total_secl_chargable_wt: str
    total_rly_chargable_wt: str
    freight: str
    gst: str
    pola: str
    total_freight: str
    source_type: str
    secl_rly_data: List[dict]


class EmailRequest(BaseModel):
    sender_email: EmailStr
    subject: Optional[str]
    password: str
    smtp_host: str
    smtp_port: int
    receiver_email: List[EmailStr]
    body: str
    file_path: Optional[Union[str, Dict[str, str]]]
    cc_list: Optional[List[EmailStr]] = []
    bcc_list: Optional[List[EmailStr]] = []

class BunkerAnalysisData(BaseModel):
    id: str
    mgcv: str
    hgcv: str
    ratio: str


class TruckTareEmailTrigger(BaseModel):
    vehicle_number: str
    current_gwel_tare_time: str
    current_gwel_tare_wt: str
    min_GWEL_Tare_Wt: str
    max_GWEL_Tare_Wt: str
    difference: str

class TruckEmailTrigger(BaseModel):
    details: List[TruckTareEmailTrigger]


class geofenceEmailTrigger(BaseModel):
    vehicle_number: str
    lat_long: str
    mine_name: str
    geo_fence: str


class ShiftSchedule(BaseModel):
    shift_name: str
    start_shift_time: str
    end_shift_time: str


class ShiftMainData(BaseModel):
    data : List[ShiftSchedule]


class geoFence(BaseModel):
    name: str
    geofence: list