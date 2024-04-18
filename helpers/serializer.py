from pydantic import BaseModel, validator
from typing import Optional,List



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