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

class rcraveryUserData(BaseModel):
    indexing : Optional[str] = None
    wagon_owner : Optional[str] = None
    wagon_type : Optional[str] = None
    wagon_no : Optional[str] = None
    ser_no : Optional[str] = None
    rake_no : Optional[str] = None
    rake_id : Optional[str] = None
    wagon_no_avery : Optional[str] = None
    wagon_id : Optional[str] = None
    wagon_type : Optional[str] = None
    wagon_cc : Optional[str] = None
    mode : Optional[str] = None
    tip_startdate : Optional[str] = None
    tip_starttime : Optional[str] = None
    tip_enddate : Optional[str] = None
    tip_endtime : Optional[str] = None
    tipple_time : Optional[str] = None
    status : Optional[str] = None
    gwel_gross_wt : Optional[str] = None
    gwel_tare_wt : Optional[str] = None
    gwel_net_wt : Optional[str] = None
    time_in_tipp : Optional[str] = None
    po_number : Optional[str] = None
    coal_grade : Optional[str] = None
    data_from: Optional[str] = None

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
    GWEL_pending_wagons: Optional[str] = None
    GWEL_received_wagons: Optional[str] = None
    total_gwel_gross_wt: Optional[str] = None
    total_gwel_net_wt: Optional[str] = None
    total_gwel_tare_wt: Optional[str] = None
    avery_completion_date: Optional[str] = None
    avery_placement_date: Optional[str] = None
    avery_rly_data : List[rcraveryUserData] = None


class EmailRequest(BaseModel):
    sender_email: EmailStr
    subject: Optional[str] = None
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
    shift_name: Optional[str] = None
    start_shift_time: Optional[str] = None
    end_shift_time: Optional[str] = None
    schedule: Optional[str] = None
    time: Optional[str] = None
    filter: Optional[str] = None
    duration: Optional[str] = None


class ShiftMainData(BaseModel):
    data : List[ShiftSchedule]


class geoFence(BaseModel):
    name: str
    geofence: list


class rakeQuotaManual(BaseModel):
    month: Optional[str] = None
    year: Optional[str] = None
    valid_upto: Optional[str] = None
    coal_field: Optional[str] = None
    rake_alloted: Optional[str] = None
    rake_received: Optional[str] = None
    due: Optional[str] = None
    grade: Optional[str] = None


class rakeQuotaUpdate(BaseModel):
    month: Optional[str] = None
    source_type: Optional[str] = None
    rakes_planned_for_month: Optional[int] = None
    expected_rakes: Optional[dict]
    cancelled_rakes: Optional[str] = None
    remarks: Optional[str] = None



class averyUserData(BaseModel):
    indexing : Optional[str] = None
    wagon_owner : Optional[str] = None
    wagon_type : Optional[str] = None
    wagon_no : Optional[str] = None
    ser_no : Optional[str] = None
    rake_no : Optional[str] = None
    rake_id : Optional[str] = None
    wagon_no_avery : Optional[str] = None
    wagon_id : Optional[str] = None
    wagon_type : Optional[str] = None
    wagon_cc : Optional[str] = None
    mode : Optional[str] = None
    tip_startdate : Optional[str] = None
    tip_starttime : Optional[str] = None
    tip_enddate : Optional[str] = None
    tip_endtime : Optional[str] = None
    tipple_time : Optional[str] = None
    status : Optional[str] = None
    gwel_gross_wt : Optional[str] = None
    gwel_tare_wt : Optional[str] = None
    gwel_net_wt : Optional[str] = None
    time_in_tipp : Optional[str] = None
    po_number : Optional[str] = None
    coal_grade : Optional[str] = None
    data_from: Optional[str] = None


class mainAveryData(BaseModel):
    data : List[averyUserData]


class taxInvoiceGmr(BaseModel):
    id: Optional[str] = None
    do_no: Optional[str] = None
    dc_date: Optional[str] = None
    challan_no: Optional[str] = None
    grade: Optional[str]  = None
    truck_no: Optional[str] = None
    tare: Optional[str] = None
    gross: Optional[str] = None
    net: Optional[str] = None
    invoice_no: Optional[str] = None

class mineNameUpdate(BaseModel):
    id: Optional[str] = None
    mine_code: Optional[str] = None
    mine_mode: Optional[str] = None
    source_type: Optional[str] = None

class rakequotaUpload(BaseModel):
    # date: Optional[str]
    month: Optional[str] = None
    year: Optional[str] = None
    valid: Optional[str] = None
    coal_field: Optional[str] = None
    rake_alloted: Optional[str] = None
    grade: Optional[str] = None
    source_type: Optional[str] = None


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
    invoice_date: Optional[str] = None
    invoice_no: Optional[str] = None
    sale_date: Optional[str] = None
    grade: Optional[str] = None
    dispatch_date: Optional[str] = None
    mine: Optional[str] = None
    do_qty: Optional[str] = None


class grnPdf(BaseModel):
    # delivery_doc_no: Optional[str]
    # ship_to_party: Optional[str]
    sales_doc_no: Optional[str] = None
    dispatch_date_time: Optional[str] = None
    challan_number: Optional[str] = None
    grade_size: Optional[str] = None
    truck_number: Optional[str] = None
    tare_weight: Optional[str] = None
    gross_weight: Optional[str] = None
    net_weight: Optional[str] = None


class GrnFileData(BaseModel):
    do_no: Optional[str] = None
    dc_date: Optional[str] = None
    invoice_date: Optional[str] = None
    invoice_no: Optional[str] = None
    sale_date: Optional[str] = None
    grade: Optional[str] = None
    dispatch_date: Optional[str] = None
    mine: Optional[str] = None
    do_qty: Optional[str] = None
    table_data: Optional[List[grnPdf]]

class grnpdfRail(BaseModel):
    indexing: Optional[str] = None
    wagon_owner: Optional[str] = None
    wagon_type: Optional[str] = None
    wagon_no: Optional[str] = None
    ser_no: Optional[str] = None
    rake_no: Optional[str] = None
    rake_id: Optional[str] = None
    wagon_no_avery: Optional[str] = None
    wagon_id: Optional[str] = None
    wagon_cc: Optional[str] = None
    mode: Optional[str] = None
    tip_startdate: Optional[str] = None
    tip_starttime: Optional[str] = None
    tip_enddate: Optional[str] = None
    tip_endtime: Optional[str] = None
    tipple_time: Optional[str] = None
    gwel_gross_wt: Optional[str] = None
    gwel_tare_wt: Optional[str] = None
    gwel_net_wt: Optional[str] = None
    # time_in_tipp: Optional[str]
    po_number: Optional[str] = None
    coal_grade: Optional[str] = None

class GrnFileDataRail(BaseModel):
    do_no: Optional[str] = None
    dc_date: Optional[str] = None
    invoice_date: Optional[str] = None
    invoice_no: Optional[str] = None
    sale_date: Optional[str] = None
    grade: Optional[str] = None
    dispatch_date: Optional[str] = None
    mine: Optional[str] = None
    do_qty: Optional[str] = None
    # table_data: Optional[List[grnpdfRail]]
    table_data: Optional[List]


class CategoryDataModel(BaseModel):
    remark: Optional[str] = None
    uom: Optional[str] = None
    mou_coal: Optional[float] = None
    linkage: Optional[float] = None
    aiwib_washery: Optional[float] = None
    open_mkt: Optional[float] = None
    spot_eauction: Optional[float] = None
    spl_for_eauction: Optional[float] = None
    imported: Optional[float] = None
    total: Optional[float] = None
    shakti_b: Optional[float] = None
    shakti_b3: Optional[float] = None
    particular: Optional[str] = None

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


class CoalDataModelManual(BaseModel):
    osd_month: CategoryDataModel #1
    adj_qty: CategoryDataModel #4
    norm_transit_loss: CategoryDataModel #6
    amt_charged: CategoryDataModel #8
    adj_amt: CategoryDataModel #9
    unloading_charges: CategoryDataModel #10
    trans_charges: CategoryDataModel #12
    adj_trans_charges: CategoryDataModel #13
    demurrage: CategoryDataModel #14
    diesel_cost: CategoryDataModel #15
    qty_consumed: CategoryDataModel #21
    wtd_avg_gcv_prev: CategoryDataModel #23
    wtd_avg_gcv_recv: CategoryDataModel #24
    wtd_avg_gcv_less_85: CategoryDataModel #25
    month: Optional[datetime.datetime] #28

class UserListName(BaseModel):
    email: List[List[dict]]
    approval_name: Optional[str] = None
    bypass_level: Optional[bool] = False
    disabled: Optional[bool] = False


class grnUpdateTax(BaseModel):
    do_no: Optional[str] = None
    invoice_date: Optional[str] = None
    invoice_no: Optional[str] = None
    sale_date: Optional[str] = None
    grade: Optional[str] = None
    dispatch_date: Optional[str] = None
    mine: Optional[str] = None
    do_qty: Optional[str] = None
    original_data: List[dict]
    new_data: List[dict]
    particulars: Optional[dict]
    # approvals: Optional[dict]
    posting_date: Optional[str] = None
    changed_by: Optional[str] = None
    type_consumer: Optional[str] = None

class manualgrnUpdateTax(BaseModel):
    do_no: Optional[str] = None
    invoice_date: Optional[str] = None
    invoice_no: Optional[str] = None
    # sale_date: Optional[str]
    grade: Optional[str] = None
    dispatch_date: Optional[str] = None
    mine: Optional[str] = None
    do_qty: Optional[str] = None
    original_data: List[dict]
    new_data: List[dict]
    # particulars: Optional[dict]
    # approvals: Optional[dict]
    posting_date: Optional[str] = None
    changed_by: Optional[str] = None
    type_consumer: Optional[str] = None

class finalManualGrnUpdateTax(BaseModel):
    grnData: List[manualgrnUpdateTax]

class roadConsumertype(BaseModel):
    roadConsumertype: List[str]
    mode: Optional[str] = None

class TableSubjectData(BaseModel):
    table_name: Optional[str] = None
    table_subject: Optional[str] = None

class TableExportData(BaseModel):
    start_date: str
    end_date: str
    subject: str
    to: List[str]
    cc: List[str]
    bcc: List[str]
    message: str
    table_name: str
    filter_type: Optional[str] = None
    filter_data: Optional[list]

class grnupdateStatusData(BaseModel):
    user_name: Optional[str] = None
    status: Optional[str] = None
    do_no: Optional[str] = None
    comment: Optional[str] = None
    level_name: Optional[str] = None
    level_no: Optional[str] = None
    invoice_no: Optional[str] = None
    mode: Optional[str] = "road"
    # edited_by: Optional[str]

class cmplInput(BaseModel):
    tno: Optional[int] = None
    companycode: Optional[str] = None
    financialyearcode: Optional[str] = None
    locationcode: Optional[str] = None
    lrno: Optional[str] = None
    lrdate: Optional[str] = None
    partycode: Optional[str] = None
    source_location_tno : Optional[str] = None
    consignor_code : Optional[str] = None
    destination_location_tno : Optional[str] = None
    consigneecode : Optional[str] = None
    vehicle_no : Optional[str] = None
    freightamount : Optional[int] = None
    item_code : Optional[str] = None
    nos : Optional[int] = None
    quantity1 : Optional[int] = None
    quantity2 : Optional[int] = None
    invoice_no : Optional[str] = None
    invoice_date : Optional[str] = None
    consignor_name : Optional[str] = None
    consignor_address : Optional[str] = None
    consignor_citycode : Optional[str] = None
    consignor_statecode : Optional[str] = None
    consignor_phoneno : Optional[str] = None
    consignee_name : Optional[str] = None
    consignee_address : Optional[str] = None
    consignee_citycode : Optional[str] = None
    consignee_statecode : Optional[str] = None
    consignee_phoneno : Optional[str] = None
    invoice_amount : Optional[int] = None
    challon_no : Optional[str] = None
    challan_date : Optional[str] = None
    driver_name : Optional[str] = None
    driver_licenseno : Optional[str] = None
    eway_billno : Optional[str] = None
    eway_billdate : Optional[str] = None
    balance_qty : Optional[int] = None
    do_qty : Optional[int] = None
    delivery_order_tno : Optional[int] = None


class railGrnPost(BaseModel):
    rr_no: Optional[str] = None #do_no
    rr_qty: Optional[str] = None #do_qty
    mine: Optional[str] = None #source
    grade: Optional[str] = None
    invoice_date: Optional[str] = None
    invoice_no: Optional[str] = None
    sale_date: Optional[str] = None
    avery_placement_date: Optional[str] = None #dispatch_date as per road
    new_data: Optional[List[dict]] #avery_data
    # total_gwel_net: Optional[float] # for form15 value under grn 
    sizing_charges: Optional[float] = None
    evac_facility_charge: Optional[float] = None
    royality_charges: Optional[float] = None
    nmet_charges: Optional[float] = None
    dmf: Optional[float] = None
    adho_sanrachna_vikas: Optional[float] = None
    pariyavaran_upkar: Optional[float] = None
    assessable_value: Optional[float] = None
    igst: Optional[float] = None
    gst_comp_cess: Optional[float] = None
    gross_bill_value: Optional[float] = None
    less_underloading_charges: Optional[float] = None
    net_value: Optional[float] = None
    total_amount: Optional[float] = None
    changed_by: Optional[str] = None
    source_type: Optional[str] = None
    posting_date: Optional[str] = None


class aopStatic(BaseModel):
    percentage: Optional[str] = None
    gcv_crushing: Optional[str] = None
    qty_saving: Optional[str] = None
    month: Optional[str] = None


class reEditGrnData(BaseModel):
    do_no: Optional[str] = None
    invoice_no: Optional[str] = None
    mode: Optional[str] = None

class updateGrnData(BaseModel):
    do_no: Optional[str] = None
    invoice_date: Optional[str] = None
    invoice_no: Optional[str] = None
    sale_date: Optional[str] = None
    grade: Optional[str] = None
    dispatch_date: Optional[str] = None
    mine: Optional[str] = None
    do_qty: Optional[str] = None
    new_data: Optional[List[dict]]
    original_data: Optional[List[dict]]
    changed_by: Optional[str] = None
    posting_date: Optional[str] = None

class updateGrnDataRail(BaseModel):
    rr_no: Optional[str] = None
    invoice_date: Optional[str] = None
    invoice_no: Optional[str] = None
    sale_date: Optional[str] = None
    grade: Optional[str] = None
    dispatch_date: Optional[str] = None
    mine: Optional[str] = None
    rr_qty: Optional[str] = None
    new_data: Optional[List[dict]]
    avery_placement_date: Optional[str] = None
    changed_by: Optional[str] = None
    posting_date: Optional[str] = None

class rakequoataAnnexure(BaseModel):
    month: Optional[str] = None
    year: Optional[str] = None
    coal_field: Optional[str] = None
    rake_alloted: Optional[str] = None
    validity: Optional[str] = None
    source_type: Optional[str] = None

class sapRecordsCheck(BaseModel):
    do_no: Optional[List[str]] = None


class weightBridgeReport(BaseModel):
    record_id: Optional[str] = None
    delivery_challan_number: Optional[str] = None
    arv_cum_do_number: Optional[str] = None
    mine: Optional[str] = None
    vehicle_number: Optional[str] = None
    po_no: Optional[str] = None
    gross_qty: Optional[str] = None
    tare_qty: Optional[str] = None
    net_qty: Optional[str] = None
    actual_gross_qty: Optional[str] = None
    actual_tare_qty: Optional[str] = None
    actual_tare_qty: Optional[str] = None
    actual_net_qty: Optional[str] = None


class rcrRoadUpdate(BaseModel):
    truck_no: Optional[str] = None
    do_no: Optional[str] = None
    mine_name: Optional[str] = None
    weightment_serial_no: Optional[str] = None
    gross_weight: Optional[str] = None
    tare_weight: Optional[str] = None
    net_weight: Optional[str] = None
    # do_date: Optional[str] = None
    grade: Optional[str] = None
    gross_date_time: Optional[str] = None
    tare_date_time: Optional[str] = None