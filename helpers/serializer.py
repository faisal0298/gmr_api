from pydantic import BaseModel, validator, ValidationError, root_validator, EmailStr
from typing import Optional, List,Dict,Union
from enum import Enum
import datetime



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
    Tare_Qty: Optional[str] = None
    Actual_Tare_Qty: Optional[str] = None


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
    rr_no: Optional[str] = None
    rr_qty: Optional[str] = None
    po_no: Optional[str] = None
    po_date: Optional[str] = None
    line_item: Optional[str] = None
    source: Optional[str] = None
    placement_date: Optional[str] = None
    completion_date: Optional[str] = None
    drawn_date: Optional[str] = None
    total_ul_wt: Optional[str] = None
    boxes_supplied: Optional[str] = None
    total_secl_gross_wt: Optional[str] = None
    total_secl_tare_wt: Optional[str] = None
    total_secl_net_wt: Optional[str] = None
    total_secl_ol_wt: Optional[str] = None
    boxes_loaded: Optional[str] = None
    total_rly_gross_wt: Optional[str] = None
    total_rly_tare_wt: Optional[str] = None
    total_rly_net_wt: Optional[str] = None
    total_rly_ol_wt: Optional[str] = None
    total_secl_chargable_wt: Optional[str] = None
    total_rly_chargable_wt: Optional[str] = None
    freight: Optional[str] = None
    gst: Optional[str] = None
    pola: Optional[str] = None
    sd: Optional[str] = None
    total_freight: Optional[str] = None
    source_type: Optional[str] = None
    secl_rly_data: List[dict]
    month: Optional[str] = None


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
    shift_name: Optional[str]
    start_shift_time: Optional[str]
    end_shift_time: Optional[str]
    schedule: Optional[str]
    time: Optional[str]
    filter: Optional[str]
    duration: Optional[str]


class ShiftMainData(BaseModel):
    data : List[ShiftSchedule]


class geoFence(BaseModel):
    name: str
    geofence: list


class rakeQuotaManual(BaseModel):
    month: Optional[str]
    year: Optional[str]
    valid_upto: Optional[str]
    coal_field: Optional[str]
    rake_alloted: Optional[str]
    rake_received: Optional[str]
    due: Optional[str]
    grade: Optional[str]


class rakeQuotaUpdate(BaseModel):
    month: Optional[str]
    source_type: Optional[str]
    rakes_planned_for_month: Optional[int]
    expected_rakes: Optional[dict]
    cancelled_rakes: Optional[str]
    remarks: Optional[str]



class averyUserData(BaseModel):
    indexing : Optional[str]
    wagon_owner : Optional[str]
    wagon_type : Optional[str]
    wagon_no : Optional[str]
    ser_no : Optional[str]
    rake_no : Optional[str]
    rake_id : Optional[str]
    wagon_no_avery : Optional[str]
    wagon_id : Optional[str]
    wagon_type : Optional[str]
    wagon_cc : Optional[str]
    mode : Optional[str]
    tip_startdate : Optional[str]
    tip_starttime : Optional[str]
    tip_enddate : Optional[str]
    tip_endtime : Optional[str]
    tipple_time : Optional[str]
    status : Optional[str]
    gwel_gross_wt : Optional[str]
    gwel_tare_wt : Optional[str]
    gwel_net_wt : Optional[str]
    time_in_tipp : Optional[str]
    po_number : Optional[str]
    coal_grade : Optional[str]
    data_from: Optional[str]


class mainAveryData(BaseModel):
    data : List[averyUserData]


class taxInvoiceGmr(BaseModel):
    id: Optional[str]
    do_no: Optional[str]
    dc_date: Optional[str]
    challan_no: Optional[str]
    grade: Optional[str]
    truck_no: Optional[str]
    tare: Optional[str]
    gross: Optional[str]
    net: Optional[str]
    invoice_no: Optional[str]

class mineNameUpdate(BaseModel):
    id: Optional[str]
    mine_code: Optional[str]
    mine_mode: Optional[str]
    source_type: Optional[str]

class rakequotaUpload(BaseModel):
    # date: Optional[str]
    month: Optional[str]
    year: Optional[str]
    valid: Optional[str]
    coal_field: Optional[str]
    rake_alloted: Optional[str]
    grade: Optional[str]
    source_type: Optional[str]


class roadSapUpload(BaseModel):
    # do_no: Optional[str]
    # do_date: Optional[str]
    # start_date: Optional[str]
    # end_date: Optional[str]
    # slno: Optional[str]
    # consumer_type: Optional[str]
    # grade: Optional[str]
    # mine_name: Optional[str]
    # line_item: Optional[str]
    # do_qty: Optional[str]
    # po_amount: Optional[str]
    data: List[dict]


class grnStatus(BaseModel):
    invoice_date: Optional[str]
    invoice_no: Optional[str]
    sale_date: Optional[str]
    grade: Optional[str]
    dispatch_date: Optional[str]
    mine: Optional[str]
    do_qty: Optional[str]


class grnPdf(BaseModel):
    # delivery_doc_no: Optional[str]
    # ship_to_party: Optional[str]
    sales_doc_no: Optional[str]
    dispatch_date_time: Optional[str]
    challan_number: Optional[str]
    grade_size: Optional[str]
    truck_number: Optional[str]
    tare_weight: Optional[str]
    gross_weight: Optional[str]
    net_weight: Optional[str]


class GrnFileData(BaseModel):
    do_no: Optional[str]
    dc_date: Optional[str]
    invoice_date: Optional[str]
    invoice_no: Optional[str]
    sale_date: Optional[str]
    grade: Optional[str]
    dispatch_date: Optional[str]
    mine: Optional[str]
    do_qty: Optional[str]
    table_data: Optional[List[grnPdf]]


class CategoryDataModel(BaseModel):
    remark: Optional[str] = None
    uom: Optional[float]
    mou_coal: Optional[float]
    linkage: Optional[float]
    aiwib_washery: Optional[float]
    open_mkt: Optional[float]
    spot_eauction: Optional[float]
    spl_for_eauction: Optional[float]
    imported: Optional[float]
    total: Optional[float]
    shakti_b: Optional[float]
    shakti_b3: Optional[float]
    particular: Optional[str]

class CoalDataModel(BaseModel):
    osd_month: CategoryDataModel
    vos_month: CategoryDataModel
    qty_supplied: CategoryDataModel
    adj_qty: CategoryDataModel
    coal_supplied: CategoryDataModel
    norm_transit_loss: CategoryDataModel
    net_supplied: CategoryDataModel
    amt_charged: CategoryDataModel
    adj_amt: CategoryDataModel
    unloading_charges: CategoryDataModel
    total_amt_charged: CategoryDataModel
    trans_charges: CategoryDataModel
    adj_trans_charges: CategoryDataModel
    demurrage: CategoryDataModel
    diesel_cost: CategoryDataModel
    total_trans_charges: CategoryDataModel
    total_amt_incl_trans: CategoryDataModel
    qty_at_station: CategoryDataModel
    total_amt_for_coal: CategoryDataModel
    landed_cost: CategoryDataModel
    qty_consumed: CategoryDataModel
    value_consumed: CategoryDataModel
    wtd_avg_gcv_prev: CategoryDataModel
    wtd_avg_gcv_recv: CategoryDataModel
    wtd_avg_gcv_less_85: CategoryDataModel
    closing_coal_stock: CategoryDataModel
    closing_coal_stock_value: CategoryDataModel
    month: Optional[datetime.datetime]


class UserListName(BaseModel):
    email: List[List[dict]]
    approval_name: Optional[str]
    bypass_level: Optional[bool] = False
    disabled: Optional[bool] = False


class grnUpdateTax(BaseModel):
    do_no: Optional[str]
    invoice_date: Optional[str]
    invoice_no: Optional[str]
    sale_date: Optional[str]
    grade: Optional[str]
    dispatch_date: Optional[str]
    mine: Optional[str]
    do_qty: Optional[str]
    original_data: List[dict]
    new_data: List[dict]
    particulars: Optional[dict]
    # approvals: Optional[dict]
    changed_by: Optional[str]

class roadConsumertype(BaseModel):
    roadConsumertype: List[str]

class TableSubjectData(BaseModel):
    table_name: Optional[str]
    table_subject: Optional[str]

class TableExportData(BaseModel):
    start_date: str
    end_date: str
    subject: str
    to: List[str]
    cc: List[str]
    bcc: List[str]
    message: str
    table_name: str
    filter_type: Optional[str]
    filter_data: Optional[list]

class grnupdateStatusData(BaseModel):
    user_name: Optional[str]
    status: Optional[str]
    do_no: Optional[str]
    comment: Optional[str]
    level_name: Optional[str]
    level_no: Optional[str]
    invoice_no: Optional[str]
    # edited_by: Optional[str]

class cmplInput(BaseModel):
    tno: Optional[int]
    companycode: Optional[str]
    financialyearcode: Optional[str]
    locationcode: Optional[str]
    lrno: Optional[str]
    lrdate: Optional[str]
    partycode: Optional[str]
    source_location_tno : Optional[str]
    consignor_code : Optional[str]
    destination_location_tno : Optional[str]
    consigneecode : Optional[str]
    vehicle_no : Optional[str]
    freightamount : Optional[int]
    item_code : Optional[str]
    nos : Optional[int]
    quantity1 : Optional[int] 
    quantity2 : Optional[int]
    invoice_no : Optional[str]
    invoice_date : Optional[str]
    consignor_name : Optional[str]
    consignor_address : Optional[str]
    consignor_citycode : Optional[str]
    consignor_statecode : Optional[str]
    consignor_phoneno : Optional[str]
    consignee_name : Optional[str]
    consignee_address : Optional[str]
    consignee_citycode : Optional[str]
    consignee_statecode : Optional[str]
    consignee_phoneno : Optional[str]
    invoice_amount : Optional[int]
    challon_no : Optional[str]
    challan_date : Optional[str]
    driver_name : Optional[str]
    driver_licenseno : Optional[str]
    eway_billno : Optional[str]
    eway_billdate : Optional[str]
    balance_qty : Optional[int]
    do_qty : Optional[int]
    delivery_order_tno : Optional[int]