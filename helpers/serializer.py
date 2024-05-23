from pydantic import BaseModel, validator, ValidationError, root_validator, EmailStr
from typing import Optional, List
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
    filter: str
    schedule: str
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

