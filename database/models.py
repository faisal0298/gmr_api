from mongoengine import *
from helpers.logger import console_logger
import datetime
import uuid
from mongoengine import signals
import pytz
from dateutil import tz

to_zone = tz.gettz("Asia/Kolkata")
file = str(datetime.datetime.utcnow().strftime("%d-%m-%Y"))

class UsecaseParameters(Document):
    Camera_id = StringField()
    Camera_name = StringField()
    Location = StringField()
    Parameters = DictField()
    Timestamp = DateTimeField(default=datetime.datetime.utcnow)

    meta = {"db_alias" : "gmrDB-alias" , "collection" : "usecaseparameters"}


class ParentServiceMeta(EmbeddedDocument):
    Parent_id = StringField()
    Labels = DictField()

    meta = {"db_alias" : "gmrDB-alias" , "collection" : "parentservicemeta"}

    def payload(self):
        return {self.Parent_id: self.Labels}


class DeveloperParameters(Document):
    Name = StringField()
    Service_id = StringField()
    Type = StringField()
    Default_params = DictField()
    Parent_service_meta = EmbeddedDocumentListField(ParentServiceMeta)

    meta = {"db_alias" : "gmrDB-alias" , "collection" : "developerparameters"}

    def fetchParentIds(self):
        parent_ids = []
        for embedDoc in self.Parent_service_meta:
            parent_ids.append(embedDoc.Parent_id)
        return parent_ids

    def fetchLabels(self):
        labels = {}
        for embedDoc in self.Parent_service_meta:
            labels.update(embedDoc.payload())
        return labels


class PricingDetails(Document):
    vehicle_type = StringField()
    rate_per_hr = FloatField()

    meta = {"db_alias" : "gmrDB-alias" , "collection" : "pricingdetails"}

    def payload(self):
        return {
            "vehicle_type": self.vehicle_type,
            "rate_per_hr": self.rate_per_hr,
        }


class VehicleDetails(Document):
    vehicle_type = StringField()
    rate_per_hr = StringField()

    meta = {"db_alias" : "gmrDB-alias" , "collection" : "vehicledetails"}

    def payload(self):
        return {
            "_id": str(self.id),
            "vehicle_type": self.vehicle_type,
            "rate_per_hr": self.rate_per_hr,
        }


class OwnerDetails(Document):
    vehicle_number = StringField()
    owner_name = StringField()
    category = StringField()

    meta = {"db_alias" : "gmrDB-alias" , "collection" : "ownerdetails"}

    def payload(self):
        return {
            "_id": str(self.id),
            "vehicle_type": self.vehicle_number,
            "owner_name": self.owner_name,
            "category": self.category,
        }


class CameraDetails(Document):
    camera_id = StringField()
    gate_name = StringField()
    direction = StringField()

    meta = {"db_alias" : "gmrDB-alias" , "collection" : "cameradetails"}

    def payload(self):
        return {
            "_id": str(self.id),
            "camera_id": self.camera_id,
            "gate_name": self.gate_name,
            "direction": self.direction,
        }


class AlertLogs(Document):
    camera_name = StringField()
    camera_location = StringField()
    alert = StringField()
    image_url_in = StringField(default=None, null=True)
    image_url_out = StringField(default=None, null=True)
    time_stamp = DateTimeField()

    meta = {"db_alias" : "gmrDB-alias" , "collection" : "alertlogs"}

    def payload(self):
        local_timestamp = self.time_stamp.replace(
            tzinfo=datetime.timezone.utc
        ).astimezone(tz=None)
        return {
            "camera_name": self.camera_name,
            "camera_location": self.camera_location,
            "alert": self.alert,
            "image_url_in": self.image_url_in,
            "image_url_out": self.image_url_out,
            "time_stamp": self.time_stamp,
            "date": local_timestamp.replace(microsecond=0).date(),
            "time": local_timestamp.replace(microsecond=0).time(),
        }


class Historian(Document):
    tagid = IntField()
    sum = StringField()
    created_date =  DateTimeField()
    created_at = DateTimeField(default=datetime.datetime.utcnow)
    ID = IntField(min_value=1)

    meta = {"db_alias" : "gmrDB-alias" , "collection" : "historian"}

    def payload(self):
        return {
                "tagid": self.tagid,
                "sum": self.sum
            }


class CoalTesting(Document):
    location = StringField()
    rrNo = StringField()
    rR_Qty = StringField()
    rake_no = StringField()
    supplier = StringField()
    parameters = ListField(DictField())
    receive_date = DateTimeField()
    ID = IntField(min_value=1)
    third_party_report_no = StringField(null=True)
    third_party_upload_date = DateTimeField()
    created_at = DateTimeField(default=datetime.datetime.utcnow())

    meta = {"db_alias" : "gmrDB-alias" , "collection" : "coaltesting"}
        
    def payload(self):
        
        payload_dict = {
            "Sr.No": self.ID,
            "Mine": self.location,
            "Lot_No": self.rake_no,
            "DO_No": self.rrNo,
            "DO_Qty": self.rR_Qty,
            "Supplier": self.supplier,
            "Third_Party_Report_No": self.third_party_report_no,
            "Third_Party_Upload_Date": self.third_party_upload_date,
            "Date": datetime.datetime.fromisoformat(
                    self.receive_date.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d") if self.receive_date else None,
                    
            "Time": datetime.datetime.fromisoformat(
                    self.receive_date.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%H:%M:%S") if self.receive_date else None,
            "Id": str(self.pk)}

        for param in self.parameters:
            # console_logger.debug(param)
            param_name = f"{param['parameter_Name']}_{param['unit_Val'].replace(' ','')}"
            if "Third" in param_name:
                payload_dict[f"{param_name}"] = param["val1"]
            else:
                payload_dict[f"GWEL_{param_name}"] = param["val1"]


        return payload_dict
    
    def gradepayload(self):
        payload_data = {
            "id": str(self.pk),
            "Sr.No": self.ID,
            "DO_No": self.rrNo,
            "Mine": self.location,
            "DO_Qty": self.rR_Qty,
        }

        for single_param in self.parameters:
            # console_logger.debug(single_param)
            param_name = f"Gross_Calorific_Value_(Adb)"
            if single_param["parameter_Name"] == "Gross_Calorific_Value_(Adb)":
                # console_logger.debug("inside gcv")
                payload_data[param_name] = single_param["val1"]
                if single_param.get("grade"):
                    payload_data[f"grade"] = single_param["grade"]
                else:
                    payload_data[f"grade"] = None
                if single_param.get("gcv_difference"):
                    payload_data["gcv_difference"] = single_param["gcv_difference"]
                else:
                    payload_data["gcv_difference"] = None
                if single_param.get("Third_Party_Gross_Calorific_Value_(Adb)"):
                    payload_data["Third_Party_Gross_Calorific_Value_(Adb)"] = single_param["Third_Party_Gross_Calorific_Value_(Adb)"]
                else:
                    payload_data["Third_Party_Gross_Calorific_Value_(Adb)"] = None
                if single_param.get("thrd_grade"):
                    payload_data["thrd_grade"] = single_param["thrd_grade"]
                else:
                    payload_data["thrd_grade"] = None
                if single_param.get("grade_diff"):
                    payload_data["grade_diff"] = single_param["grade_diff"]
                else:
                    payload_data["grade_diff"] = None
            if single_param.get("parameter_Name") == "Third_Party_Total_Moisture":
                payload_data["Third_Party_Total_Moisture"] = single_param.get("val1")
            if single_param.get("parameter_Name") == "Third_Party_Volatile_Matter_(Arb)":
                payload_data["Third_Party_Volatile_Matter_(Arb)"] = single_param.get("val1")
            if single_param.get("parameter_Name") == "Third_Party_Ash_(Arb)":
                payload_data["Third_Party_Ash_(Arb)"] = single_param.get("val1")
            if single_param.get("parameter_Name") == "Third_Party_Fixed_Carbon_(Arb)":   
                payload_data["Third_Party_Fixed_Carbon_(Arb)"] = single_param.get("val1")
            if single_param.get("parameter_Name") == "Third_Party_Gross_Calorific_Value_(Arb)":  
                payload_data["Third_Party_Gross_Calorific_Value_(Arb)"] = single_param.get("val1")
            if single_param.get("parameter_Name") == "Third_Party_Inherent_Moisture_(Adb)":
                payload_data["Third_Party_Inherent_Moisture_(Adb)"] = single_param.get("val1")
            if single_param.get("parameter_Name") == "Third_Party_Volatile_Matter_(Adb)":
                payload_data["Third_Party_Volatile_Matter_(Adb)"] = single_param.get("val1")
            if single_param.get("parameter_Name") == "Third_Party_Volatile_Matter_(Arb)":
                payload_data["Third_Party_Ash_(Adb)"] = single_param.get("val1")
            if single_param.get("parameter_Name") == "Third_Party_Gross_Calorific_Value_(Adb)":
                payload_data["Third_Party_Gross_Calorific_Value_(Adb)"] = single_param.get("val1")

        payload_data["Date"] = datetime.datetime.fromisoformat(
                    self.receive_date.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d")
        
        payload_data["Time"] = datetime.datetime.fromisoformat(
                    self.receive_date.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%H:%M:%S")

            
        # console_logger.debug(payload_data)
        return payload_data



class CoalTestingTrain(Document):
    location = StringField()
    rrNo = StringField()
    rR_Qty = StringField()
    rake_no = StringField()
    supplier = StringField()
    parameters = ListField(DictField())
    receive_date = DateTimeField()
    ID = IntField(min_value=1)
    third_party_report_no = StringField(null=True)
    third_party_upload_date = DateTimeField()
    created_at = DateTimeField(default=datetime.datetime.utcnow())

    meta = {"db_alias" : "gmrDB-alias" , "collection" : "coaltestingtrain"}
        
    def payload(self):

        payload_dict = {
            "Sr.No": self.ID,
            "Mine": self.location,
            "Lot_No": self.rake_no,
            "RR_No": self.rrNo,
            "RR_Qty": self.rR_Qty,
            "Supplier": self.supplier,
            "Third_Party_Report_No": self.third_party_report_no,
            "Third_Party_Upload_Date": self.third_party_upload_date,
            "Date": datetime.datetime.fromisoformat(
                    self.receive_date.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d") if self.receive_date else None,
            "Time": datetime.datetime.fromisoformat(
                    self.receive_date.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%H:%M:%S") if self.receive_date else None,
            "Id": str(self.pk)}

        # for param in self.parameters:
        #     param_name = f"{param['parameter_Name']}_{param['unit_Val'].replace(' ','')}"
        #     payload_dict[f"GWEL_{param_name}"] = param["val1"]

        for param in self.parameters:
            # console_logger.debug(param)
            param_name = f"{param['parameter_Name']}_{param['unit_Val'].replace(' ','')}"
            if "Third" in param_name:
                payload_dict[f"{param_name}"] = param["val1"]
            else:
                payload_dict[f"GWEL_{param_name}"] = param["val1"]

        return payload_dict

    def gradepayload(self):
        local_timestamp = self.receive_date.replace(
            tzinfo=datetime.timezone.utc
        ).astimezone(tz=None)

        payload_data = {
            "id": str(self.pk),
            "Sr.No": self.ID,
            "rrNo": self.rrNo,
            "Mine": self.location,
            "RR_Qty": self.rR_Qty,
        }

        for single_param in self.parameters:
            # console_logger.debug(single_param)
            param_name = f"Gross_Calorific_Value_(Adb)"
            if single_param["parameter_Name"] == "Gross_Calorific_Value_(Adb)":
                # console_logger.debug("inside gcv")
                payload_data[param_name] = single_param["val1"]
                if single_param.get("grade"):
                    payload_data[f"grade"] = single_param["grade"]
                else:
                    payload_data[f"grade"] = None
                if single_param.get("gcv_difference"):
                    payload_data["gcv_difference"] = single_param["gcv_difference"]
                else:
                    payload_data["gcv_difference"] = None
                if single_param.get("Third_Party_Gross_Calorific_Value_(Adb)"):
                    payload_data["Third_Party_Gross_Calorific_Value_(Adb)"] = single_param["Third_Party_Gross_Calorific_Value_(Adb)"]
                else:
                    payload_data["Third_Party_Gross_Calorific_Value_(Adb)"] = None
                if single_param.get("thrd_grade"):
                    payload_data["thrd_grade"] = single_param["thrd_grade"]
                else:
                    payload_data["thrd_grade"] = None
                if single_param.get("grade_diff"):
                    payload_data["grade_diff"] = single_param["grade_diff"]
                else:
                    payload_data["grade_diff"] = None

            if single_param.get("parameter_Name") == "Third_Party_Total_Moisture":
                payload_data["Third_Party_Total_Moisture"] = single_param.get("val1")
            if single_param.get("parameter_Name") == "Third_Party_Volatile_Matter_(Arb)":
                payload_data["Third_Party_Volatile_Matter_(Arb)"] = single_param.get("val1")
            if single_param.get("parameter_Name") == "Third_Party_Ash_(Arb)":
                payload_data["Third_Party_Ash_(Arb)"] = single_param.get("val1")
            if single_param.get("parameter_Name") == "Third_Party_Fixed_Carbon_(Arb)":   
                payload_data["Third_Party_Fixed_Carbon_(Arb)"] = single_param.get("val1")
            if single_param.get("parameter_Name") == "Third_Party_Gross_Calorific_Value_(Arb)":  
                payload_data["Third_Party_Gross_Calorific_Value_(Arb)"] = single_param.get("val1")
            if single_param.get("parameter_Name") == "Third_Party_Inherent_Moisture_(Adb)":
                payload_data["Third_Party_Inherent_Moisture_(Adb)"] = single_param.get("val1")
            if single_param.get("parameter_Name") == "Third_Party_Volatile_Matter_(Adb)":
                payload_data["Third_Party_Volatile_Matter_(Adb)"] = single_param.get("val1")
            if single_param.get("parameter_Name") == "Third_Party_Volatile_Matter_(Arb)":
                payload_data["Third_Party_Ash_(Adb)"] = single_param.get("val1")
            if single_param.get("parameter_Name") == "Third_Party_Gross_Calorific_Value_(Adb)":
                payload_data["Third_Party_Gross_Calorific_Value_(Adb)"] = single_param.get("val1")

        payload_data["Date"] = datetime.datetime.fromisoformat(
                    self.receive_date.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d")
        
        payload_data["Time"] = datetime.datetime.fromisoformat(
                    self.receive_date.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%H:%M:%S")

            
        # console_logger.debug(payload_data)
        return payload_data


class Gmrrequest(Document):
    record_id = StringField(default=uuid.uuid4().hex, unique=True)
    mine = StringField()
    vehicle_number = StringField()
    delivery_challan_number = StringField()
    arv_cum_do_number = StringField()
    vehicle_chassis_number = StringField()
    certificate_expiry = StringField()
    delivery_challan_date = StringField()
    net_qty = StringField()
    tare_qty = StringField()
    actual_tare_qty = StringField()
    total_net_amount = StringField()
    expiry_validation = BooleanField(default = True)
    request = StringField(null=True)
    approved_at = DateTimeField(null=True)
    remark = StringField(null=True)
    created_at = DateTimeField(default=datetime.datetime.utcnow())
    ID = IntField(min_value=1)

    meta = {"db_alias" : "gmrDB-alias" , "collection" : "gmrrequest"}

    def payload(self):
        return {
                "Sr.No.":self.ID,
                "Request_type": self.request.replace("_", " "),
                "Mine":self.mine,
                "Vehicle_Number":self.vehicle_number,
                "Delivery_Challan_No":self.delivery_challan_number,
                "DO_No":self.arv_cum_do_number,
                "Vehicle_Chassis_No":self.vehicle_chassis_number,
                "Fitness_Expiry":self.certificate_expiry,
                "DC_Date":self.delivery_challan_date,
                "Challan_Net_Wt(MT)" : self.net_qty,
                "Total_net_amount":self.total_net_amount,
                "Request_Time" : datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
                "Comment" : self.remark,
                }
    
    def tare_payload(self):
        return {
                "Sr.No.":self.ID,
                "Request_type": self.request.replace("_", " "),
                "Mine":self.mine,
                "Vehicle_Number":self.vehicle_number,
                "Delivery_Challan_No":self.delivery_challan_number,
                "DO_No":self.arv_cum_do_number,
                "Vehicle_Chassis_No":self.vehicle_chassis_number,
                "Fitness_Expiry":self.certificate_expiry,
                "DC_Date":self.delivery_challan_date,
                "Challan_Net_Wt(MT)" : self.net_qty,
                "Challan_Tare_Wt(MT)" : self.tare_qty,
                "GWEL_Tare_Wt(MT)" : self.actual_tare_qty,
                "Total_net_amount":self.total_net_amount,
                "Request_Time" : datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
                "Comment" : self.remark,
                }

    def history_payload(self):

        tat = None
        if self.created_at and self.approved_at:
            diff = self.approved_at - self.created_at
            days = diff.days
            seconds = diff.seconds
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            seconds = seconds % 60
            components = []
            if days > 0:
                components.append(f"{days} days")
            if hours > 0:
                components.append(f"{hours} hours")
            if minutes > 0:
                components.append(f"{minutes} minutes")
            if seconds > 0:
                components.append(f"{seconds} seconds")
            
            tat = ", ".join(components)

        return {
                "Sr.No.":self.ID,
                "Request_type": self.request.replace("_", " "),
                "Mine":self.mine,
                "Vehicle_Number":self.vehicle_number,
                "Delivery_Challan_No":self.delivery_challan_number,
                "DO_No":self.arv_cum_do_number,
                "Vehicle_Chassis_No":self.vehicle_chassis_number,
                "Fitness_Expiry":self.certificate_expiry,
                "DC_Date":self.delivery_challan_date,
                "Challan_Net_Wt(MT)" : self.net_qty,
                "Total_net_amount":self.total_net_amount,
                "Remark" : self.remark,

                "Request_Time" : datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,

                "Approval_Time" : datetime.datetime.fromisoformat(
                    self.approved_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.approved_at else None,

                "TAT":tat                # Turn Around Time
                }

    def history_tare_payload(self):

        tat = None
        if self.created_at and self.approved_at:
            diff = self.approved_at - self.created_at
            days = diff.days
            seconds = diff.seconds
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            seconds = seconds % 60
            components = []
            if days > 0:
                components.append(f"{days} days")
            if hours > 0:
                components.append(f"{hours} hours")
            if minutes > 0:
                components.append(f"{minutes} minutes")
            if seconds > 0:
                components.append(f"{seconds} seconds")
            
            tat = ", ".join(components)

        return {
                "Sr.No.":self.ID,
                "Request_type": self.request.replace("_", " "),
                "Mine":self.mine,
                "Vehicle_Number":self.vehicle_number,
                "Delivery_Challan_No":self.delivery_challan_number,
                "DO_No":self.arv_cum_do_number,
                "Vehicle_Chassis_No":self.vehicle_chassis_number,
                "Fitness_Expiry":self.certificate_expiry,
                "DC_Date":self.delivery_challan_date,
                "Challan_Net_Wt(MT)" : self.net_qty,
                "Challan_Tare_Wt(MT)" : self.tare_qty,
                "GWEL_Tare_Wt(MT)" : self.actual_tare_qty,
                "Total_net_amount":self.total_net_amount,
                "Remark" : self.remark,

                "Request_Time" : datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,

                "Approval_Time" : datetime.datetime.fromisoformat(
                    self.approved_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.approved_at else None,

                "TAT":tat                # Turn Around Time
                }


class Gmrdata(Document):
    record_id = StringField(default=uuid.uuid4().hex, unique=True)
    camera_name = StringField()
    out_camera_name = StringField()
    direction = StringField()
    vehicle_type = StringField()
    vehicle_brand = StringField()
    vehicle_number = StringField()
    plate_image = StringField()
    out_plate_image = StringField()
    vehicle_image = StringField()
    out_vehicle_image = StringField()
    vehicle_out_time = DateTimeField(null=True)
    
    delivery_challan_number = StringField()
    arv_cum_do_number = StringField()
    mine = StringField()
    gross_qty = StringField()                       # gross weight extracted from challan
    tare_qty = StringField()                        # tare weight extracted from challan        
    net_qty = StringField()                         # net weight extracted from challan
    delivery_challan_date = StringField()
    type_consumer = StringField()
    grade = StringField()
    weightment_date = StringField() 
    weightment_time = StringField()
    total_net_amount = StringField() 
    challan_file = StringField()

    # lr_fasttag = BooleanField(default=False)
    lr_fasttag = BooleanField(default=True)
    
    driver_name = StringField()
    gate_pass_no = StringField()
    fr_file = StringField()

    transporter_lr_no = StringField(null=True)
    transporter_lr_date = StringField(null=True)
    transporter_lr_time = StringField(null=True)
    e_way_bill_no = StringField(null=True)
    gate_user = StringField(null=True)

    gate_approved = BooleanField(default=False)
    gate_fastag  = BooleanField(default=False)
    
    vehicle_chassis_number = StringField()
    certificate_expiry = StringField()
    actual_gross_qty = StringField(null=True)            # actual gross weight measured from weightbridge
    actual_tare_qty = StringField(null=True)             # actual tare weight measured from weightbridge
    actual_net_qty = StringField(null=True)             # actual net weight measured from weightbridge
    # wastage = StringField(null=True)
    fitness_file = StringField()
    lr_file = StringField()
    po_no = StringField(null=True)
    po_date = StringField(null=True)
    po_qty = StringField(null=True)

    gross_weighbridge = StringField(null=True)
    tare_weighbridge = StringField(null=True)

    dc_request = BooleanField(default=False)
    dc_request_status = BooleanField(default=None, null=True)
    
    tare_request = BooleanField(default=False)
    tare_request_status = BooleanField(default=None, null=True)

    start_date = StringField(null=True)
    end_date = StringField(null=True)

    do_date = StringField(null=True)
    do_qty = StringField(null=True)
    po_amount = StringField(null=True)
    slno = StringField(null=True)

    created_at = DateTimeField(default=datetime.datetime.utcnow())

    # remark = StringField(null=True)
  
    vehicle_in_time = DateTimeField(null=True)
    lot = StringField()
    line_item = StringField(null=True)
    GWEL_Gross_Time = DateTimeField(null=True)
    GWEL_Tare_Time = DateTimeField(null=True)
    grn_status  = BooleanField(default=False)
    mine_invoice = StringField(null=True)   #added on 11-11-2024 on 06:29pm
    

    ID = IntField(min_value=1)

    meta = {"db_alias" : "gmrDB-alias" , "collection" : "gmrdata"}

    def payload(self):

        Loss = None
        transit_loss=None
        tat=None

        if self.net_qty is not None and self.actual_net_qty is not None:
            Loss = float(self.actual_net_qty) - float(self.net_qty)
            transit_loss = round(Loss,5)
            
        if self.vehicle_in_time is not None and self.GWEL_Tare_Time is not None:
            diff = self.GWEL_Tare_Time - self.vehicle_in_time
            days = diff.days
            seconds = diff.seconds
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            seconds = seconds % 60
            components = []
            if days > 0:
                components.append(f"{days} days")
            if hours > 0:
                components.append(f"{hours} hours")
            if minutes > 0:
                components.append(f"{minutes} minutes")
            if seconds > 0:
                components.append(f"{seconds} seconds")
            
            tat = ", ".join(components)

        return {"record_id":self.record_id,
                "Sr.No.":self.ID,
                "Mines_Name":self.mine,
                "PO_No":self.po_no,
                "PO_Date":self.po_date,
                "DO_Qty":self.po_qty, 
                "Delivery_Challan_No":self.delivery_challan_number,
                "DO_No":self.arv_cum_do_number,
                "Grade":self.grade,
                "Type_of_consumer":self.type_consumer,
                "DC_Date":self.delivery_challan_date,
                "vehicle_number":self.vehicle_number,
                "Vehicle_Chassis_No":self.vehicle_chassis_number,
                "Fitness_Expiry":self.certificate_expiry,
                "Total_net_amount":self.total_net_amount,
                # "In gate": self.camera_name if self.camera_name else None,
                "Weightment_Date" : self.weightment_date,
                "Weightment_Time" : self.weightment_time,
                # "Out gate": self.out_camera_name if self.out_camera_name else None,
                "Challan_Gross_Wt(MT)" : self.gross_qty,
                "Challan_Tare_Wt(MT)" : self.tare_qty,
                "Challan_Net_Wt(MT)" : self.net_qty,
                "GWEL_Gross_Wt(MT)" : self.actual_gross_qty,
                "GWEL_Tare_Wt(MT)" : self.actual_tare_qty,
                "GWEL_Net_Wt(MT)" : self.actual_net_qty,
                # "Wastage" : self.wastage,
                "Driver_Name" : self.driver_name,
                "Gate_Pass_No" : self .gate_pass_no,
                "Transporter_LR_No": self.transporter_lr_no,
                "Transporter_LR_Date": self.transporter_lr_date,
                "Eway_bill_No": self.e_way_bill_no,
                # "Gate_verified_time" : datetime.datetime.fromisoformat(
                #                     self.gate_verified_time.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                #                     ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.gate_verified_time else None,

                "Vehicle_in_time" : datetime.datetime.fromisoformat(
                                    self.vehicle_in_time.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.vehicle_in_time else None,

                "Vehicle_out_time" : datetime.datetime.fromisoformat(
                                    self.vehicle_out_time.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.vehicle_out_time else None,
                
                "Challan_image" : self.challan_file if self.challan_file else None,
                "Fitness_image": self.fitness_file if self.fitness_file else None,
                "Face_image": self.fr_file if self.fr_file else None,
                "Transit_Loss": transit_loss if transit_loss else 0,
                "LOT":self.lot,
                "start_date": self.start_date,
                "end_date": self.end_date,
                "do_date": self.do_date,
                "po_amount": self.po_amount,
                "slno": self.slno,
                "grn_status": self.grn_status,
                "Line_Item" : self.line_item if self.line_item else None,

                "GWEL_Gross_Time" : datetime.datetime.fromisoformat(
                                    self.GWEL_Gross_Time.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.GWEL_Gross_Time else None,

                "GWEL_Tare_Time" : datetime.datetime.fromisoformat(
                                    self.GWEL_Tare_Time.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.GWEL_Tare_Time else None,

                "Scanned_Time" : datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,

                "mine_date" : datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,

                "TAT_difference": tat,
                }
    
class gmrdataHistoric(Document):
    record_id = StringField(default=uuid.uuid4().hex, unique=True)
    camera_name = StringField()
    out_camera_name = StringField()
    direction = StringField()
    vehicle_type = StringField()
    vehicle_brand = StringField()
    vehicle_number = StringField()
    plate_image = StringField()
    out_plate_image = StringField()
    vehicle_image = StringField()
    out_vehicle_image = StringField()
    vehicle_out_time = DateTimeField(null=True)
    
    delivery_challan_number = StringField()
    arv_cum_do_number = StringField()
    mine = StringField()
    gross_qty = StringField()                       # gross weight extracted from challan
    tare_qty = StringField()                        # tare weight extracted from challan        
    net_qty = StringField()                         # net weight extracted from challan
    delivery_challan_date = StringField()
    type_consumer = StringField()
    grade = StringField()
    weightment_date = StringField() 
    weightment_time = StringField()
    total_net_amount = StringField() 
    challan_file = StringField()

    # lr_fasttag = BooleanField(default=False)
    lr_fasttag = BooleanField(default=True)
    
    driver_name = StringField()
    gate_pass_no = StringField()
    fr_file = StringField()

    transporter_lr_no = StringField(null=True)
    transporter_lr_date = StringField(null=True)
    transporter_lr_time = StringField(null=True)
    e_way_bill_no = StringField(null=True)
    gate_user = StringField(null=True)

    gate_approved = BooleanField(default=False)
    gate_fastag  = BooleanField(default=False)
    
    vehicle_chassis_number = StringField()
    certificate_expiry = StringField()
    actual_gross_qty = StringField(null=True)            # actual gross weight measured from weightbridge
    actual_tare_qty = StringField(null=True)             # actual tare weight measured from weightbridge
    actual_net_qty = StringField(null=True)             # actual net weight measured from weightbridge
    # wastage = StringField(null=True)
    fitness_file = StringField()
    lr_file = StringField()
    po_no = StringField(null=True)
    po_date = StringField(null=True)
    po_qty = StringField(null=True)

    gross_weighbridge = StringField(null=True)
    tare_weighbridge = StringField(null=True)

    dc_request = BooleanField(default=False)
    dc_request_status = BooleanField(default=None, null=True)
    
    tare_request = BooleanField(default=False)
    tare_request_status = BooleanField(default=None, null=True)

    start_date = StringField(null=True)
    end_date = StringField(null=True)

    do_date = StringField(null=True)
    do_qty = StringField(null=True)
    po_amount = StringField(null=True)
    slno = StringField(null=True)

    created_at = DateTimeField(default=datetime.datetime.utcnow())

    # remark = StringField(null=True)
  
    vehicle_in_time = DateTimeField(null=True)
    lot = StringField()
    line_item = StringField(null=True)
    GWEL_Gross_Time = DateTimeField(null=True)
    GWEL_Tare_Time = DateTimeField(null=True)
    grn_status  = BooleanField(default=False)
    

    ID = IntField(min_value=1)

    meta = {"db_alias" : "gmrDB-alias" , "collection" : "gmrdataHistoric"}

    def payload(self):

        Loss = None
        transit_loss=None
        tat=None

        if self.net_qty is not None and self.actual_net_qty is not None:
            Loss = float(self.actual_net_qty) - float(self.net_qty)
            transit_loss = round(Loss,5)
            
        if self.vehicle_in_time is not None and self.GWEL_Tare_Time is not None:
            diff = self.GWEL_Tare_Time - self.vehicle_in_time
            days = diff.days
            seconds = diff.seconds
            hours = seconds // 3600
            minutes = (seconds % 3600) // 60
            seconds = seconds % 60
            components = []
            if days > 0:
                components.append(f"{days} days")
            if hours > 0:
                components.append(f"{hours} hours")
            if minutes > 0:
                components.append(f"{minutes} minutes")
            if seconds > 0:
                components.append(f"{seconds} seconds")
            
            tat = ", ".join(components)

        return {"record_id":self.record_id,
                "Sr.No.":self.ID,
                "Mines_Name":self.mine,
                "PO_No":self.po_no,
                "PO_Date":self.po_date,
                "DO_Qty":self.po_qty, 
                "Delivery_Challan_No":self.delivery_challan_number,
                "DO_No":self.arv_cum_do_number,
                "Grade":self.grade,
                "Type_of_consumer":self.type_consumer,
                "DC_Date":self.delivery_challan_date,
                "vehicle_number":self.vehicle_number,
                "Vehicle_Chassis_No":self.vehicle_chassis_number,
                "Fitness_Expiry":self.certificate_expiry,
                "Total_net_amount":self.total_net_amount,
                # "In gate": self.camera_name if self.camera_name else None,
                "Weightment_Date" : self.weightment_date,
                "Weightment_Time" : self.weightment_time,
                # "Out gate": self.out_camera_name if self.out_camera_name else None,
                "Challan_Gross_Wt(MT)" : self.gross_qty,
                "Challan_Tare_Wt(MT)" : self.tare_qty,
                "Challan_Net_Wt(MT)" : self.net_qty,
                "GWEL_Gross_Wt(MT)" : self.actual_gross_qty,
                "GWEL_Tare_Wt(MT)" : self.actual_tare_qty,
                "GWEL_Net_Wt(MT)" : self.actual_net_qty,
                # "Wastage" : self.wastage,
                "Driver_Name" : self.driver_name,
                "Gate_Pass_No" : self .gate_pass_no,
                "Transporter_LR_No": self.transporter_lr_no,
                "Transporter_LR_Date": self.transporter_lr_date,
                "Eway_bill_No": self.e_way_bill_no,
                # "Gate_verified_time" : datetime.datetime.fromisoformat(
                #                     self.gate_verified_time.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                #                     ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.gate_verified_time else None,

                "Vehicle_in_time" : datetime.datetime.fromisoformat(
                                    self.vehicle_in_time.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.vehicle_in_time else None,

                "Vehicle_out_time" : datetime.datetime.fromisoformat(
                                    self.vehicle_out_time.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.vehicle_out_time else None,
                
                "Challan_image" : self.challan_file if self.challan_file else None,
                "Fitness_image": self.fitness_file if self.fitness_file else None,
                "Face_image": self.fr_file if self.fr_file else None,
                "Transit_Loss": transit_loss if transit_loss else 0,
                "LOT":self.lot,
                "start_date": self.start_date,
                "end_date": self.end_date,
                "do_date": self.do_date,
                "po_amount": self.po_amount,
                "slno": self.slno,
                "grn_status": self.grn_status,
                "Line_Item" : self.line_item if self.line_item else None,

                "GWEL_Gross_Time" : datetime.datetime.fromisoformat(
                                    self.GWEL_Gross_Time.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.GWEL_Gross_Time else None,

                "GWEL_Tare_Time" : datetime.datetime.fromisoformat(
                                    self.GWEL_Tare_Time.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.GWEL_Tare_Time else None,

                "Scanned_Time" : datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,

                "mine_date" : datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,

                "TAT_difference": tat,
                }
    

class CoalGrades(Document):
    grade = StringField()
    start_value = StringField(null=True)
    end_value = StringField(null=True)

    meta = {"db_alias": "gmrDB-alias", "collection": "coalgrades"}

    def payload(self):
        return {
            "grade": self.grade,
            "start_value": self.start_value,
            "end_value": self.end_value,
        }


class ReportScheduler(Document):
    report_name = StringField()
    recipient_list = ListField(StringField(unique=True), default=[])
    cc_list = ListField(StringField(unique=True), default=[])
    bcc_list = ListField(StringField(unique=True), default=[])
    filter = StringField(default="")
    schedule = StringField(default="")
    # shift_schedule = DictField(null=True)
    shift_schedule = ListField(default=[])
    time = StringField(default="")
    active = BooleanField(default=False)
    created_at = DateTimeField(default=datetime.datetime.utcnow())

    meta = {"db_alias": "gmrDB-alias", "collection": "reportscheduler"}

    def payload(self):
        return {
            "id": str(self.id),
            "report_name": self.report_name,
            "recipient_list": self.recipient_list,
            "cc_list": self.cc_list,
            "bcc_list": self.bcc_list,
            "filter": self.filter,
            "schedule": self.schedule,
            "shift_schedule": self.shift_schedule,
            "time": self.time,
            "active": self.active,
            "created_at": self.created_at,
        }
    
    def report_payload(self):
        return{
            "id": str(self.id),
            "name": self.report_name,
        } 

    def status_payload(self):
        return{
            "id": str(self.id),
            "name": self.report_name,
            "active": self.active,
        } 

    
class SmtpSettings(Document):
    Smtp_ssl = BooleanField()
    Smtp_port = IntField()
    Smtp_host = StringField()
    Smtp_user = StringField()
    Smtp_password = StringField()
    Emails_from_email = EmailField()
    Emails_from_name = StringField()

    meta = {"db_alias": "gmrDB-alias", "collection": "SmtpSettings"}

    def payload(self):
        return {
            "smtp_ssl": self.Smtp_ssl,
            "smtp_port": self.Smtp_port,
            "smtp_host": self.Smtp_host,
            "smtp_user": self.Smtp_user,
            "smtp_password": self.Smtp_password,
            "emails_from_email": self.Emails_from_email,
            "emails_from_name": self.Emails_from_name,
        }

class AopTarget(Document):
    source_name = StringField()
    aop_target = StringField()
    created_at = DateTimeField(default=datetime.datetime.utcnow())

    meta = {"db_alias": "gmrDB-alias", "collection": "AopTarget"}

    def payload(self):
        return {
            "id": str(self.id),
            "source_name": self.source_name,
            "aop_target": self.aop_target,
            "created_at": self.created_at,
        }

    def reportpayload(self):
        return {
            "source_name": self.source_name,
            "aop_target": self.aop_target,
        }
    
class SapRecords(Document):
    slno = StringField(null=True)
    source = StringField(null=True)
    mine_name = StringField(null=True)
    sap_po = StringField(null=True)                      #po_number
    line_item = StringField(null=True)
    do_no = StringField(null=True)
    do_qty = StringField(null=True)
    start_date = StringField(null=True)
    end_date = StringField(null=True)
    grade = StringField(null=True)
    do_date = StringField(null=True)
    consumer_type = StringField(null=True)
    po_amount = StringField(null=True)
    transport_code = StringField(null=True)
    transport_name = StringField(null=True)
    material_code = StringField(null=True)
    material_description = StringField(null=True)
    plant_code = StringField(null=True)
    storage_location = StringField(null=True)
    valuation_type = StringField(null=True)
    po_open_quantity = StringField(null=True)
    uom = StringField(null=True)

    #particulars start 
    basic_price = FloatField(null=True)
    sizing_charges = FloatField(null=True)
    stc_charges = FloatField(null=True)
    evac_facility_charges = FloatField(null=True)
    royality_charges = FloatField(null=True)
    nmet_charges = FloatField(null=True)
    dmf = FloatField(null=True)
    cgst = FloatField(null=True)
    sgst = FloatField(null=True)
    gst_comp_cess = FloatField(null=True)
    so_value_grand_total = FloatField(null=True)
    #particulars end

    created_at = DateTimeField(default=datetime.datetime.utcnow())

    meta = {"db_alias": "gmrDB-alias", "collection": "SapRecords"}

    def payload(self):
        return {
            "id": str(self.id),
            "slno": self.slno,
            "source": self.source,
            "mine_name": self.mine_name,
            "sap_po": self.sap_po,
            "line_item": self.line_item,
            "do_no": self.do_no,
            "do_qty": self.do_qty,
        }

    def SimplePayload(self):
        return {
            "id": str(self.id),
            "slno": self.slno,
            "source": self.source,
            "mine_name": self.mine_name,
            "sap_po": self.sap_po,
            "line_item": self.line_item,
            "do_no": self.do_no,
            "do_qty": self.do_qty,
            # "rake_no": self.rake_no,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "grade": self.grade,
            # "po_date": self.po_date,
        }
    
    
class RcrRoadData(Document):
    rrs_wt_date = DateTimeField(null=True)  
    grs_wt_time = StringField(null=True) 
    received_gross_weight = FloatField(null=True) 
    tar_wt_date = DateTimeField(null=True) 
    tar_wt_time = StringField(null=True) 
    received_tare_weight = FloatField(null=True)  
    received_net_weight = FloatField(null=True) 
    unloading_slip_number = StringField(null=True)
    vehicle_no = StringField(null=True)
    transporter = StringField(null=True)
    tp_number = IntField(null=True)
    do_number = StringField(null=True)
    mine = StringField(null=True)
    secl_delivery_challan_number = StringField(null=True)
    dc_gross_wt = FloatField(null=True)  
    dc_tare_wt = FloatField(null=True)  
    dc_net_wt = FloatField(null=True) 
    loading_date = DateTimeField(null=True) 
    out_time = StringField(null=True)
    lr_no = IntField(null=True)
    lr_date = DateTimeField(null=True)  
    sap_po = StringField(null=True)
    line_item = StringField(null=True)
    po_date = StringField(null=True)
    do_date = StringField(null=True)
    start_date = StringField(null=True)
    end_date = StringField(null=True)
    slno = StringField(null=True)
    type_consumer = StringField(null=True)
    grade = StringField(null=True)
    po_qty = StringField(null=True)
    po_amount = StringField(null=True)
    created_at = DateTimeField(default=datetime.datetime.utcnow)

    meta = {"db_alias": "gmrDB-alias", "collection": "RcrRoadData"}

    def payload(self):
        return {
            "rrs_wt_date": self.rrs_wt_date,	
            "grs_wt_time": self.grs_wt_time,
            "received_gross_weight": self.received_gross_weight,
            "tar_wt_date": self.tar_wt_date,
            "tar_wt_time": self.tar_wt_time, 
            "received_tare_weight" : self.received_tare_weight,
            "received_net_weight": self.received_net_weight,	
            "unloading_slip_number": self.unloading_slip_number,	
            "vehicle_no": self.vehicle_no,	
            "transporter": self.transporter,
            "tp_number": self.tp_number,	
            "do_number": self.do_number,	
            "mine": self.mine,	
            "secl_delivery_challan_number": self.secl_delivery_challan_number,	
            "dc_gross_wt": self.dc_gross_wt,
            "dc_tare_wt": self.dc_tare_wt,	
            "dc_net_wt": self.dc_net_wt,	
            "loading_date": self.loading_date,	
            "out_time": self.out_time,	
            "lr_no": self.lr_no,	
            "lr_date": self.lr_date,
            "sap_po": self.sap_po,
            "line_item": self.line_item,
            "po_date": self.po_date,
            "do_date": self.do_date,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "slno": self.slno,
            "type_consumer": self.type_consumer,
            "grade": self.grade,
            "po_qty": self.po_qty,
            "po_amount": self.po_amount,
            "created_at": datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
        }


class SapRecordsRcrRoad(Document):
    slno = StringField(null=True)
    source = StringField(null=True)
    mine_name = StringField(null=True)
    sap_po = StringField(null=True)
    line_item = StringField(null=True)
    do_no = StringField(null=True)
    do_qty = StringField(null=True)
    start_date = StringField(null=True)
    end_date = StringField(null=True)
    grade = StringField(null=True)
    do_date = StringField(null=True)
    consumer_type = StringField(null=True)
    po_amount = StringField(null=True)
    # particulars start
    basic_charges = FloatField(null=True)
    sizing_charges = FloatField(null=True)
    stc_charges = FloatField(null=True)
    evac_facility_charges = FloatField(null=True)
    royality_charges = FloatField(null=True)
    nmet_charges = FloatField(null=True)
    dmf = FloatField(null=True)
    adho_sanrachna_vikas = FloatField(null=True)
    pariyavarn_upkar = FloatField(null=True)
    terminal_tax = FloatField(null=True)
    assessable_value = FloatField(null=True)
    igst = FloatField(null=True)
    gst_comp_cess = FloatField(null=True)
    requisite_payment = FloatField(null=True)
    # particulars end
    created_at = DateTimeField(default=datetime.datetime.utcnow())

    meta = {"db_alias": "gmrDB-alias", "collection": "SapRecordsRcrRoad"}

    def payload(self):
        return {
            "id": str(self.id),
            "slno": self.slno,
            "source": self.source,
            "mine_name": self.mine_name,
            "sap_po": self.sap_po,
            "line_item": self.line_item,
            "do_no": self.do_no,
            "do_qty": self.do_qty,
        }

    def SimplePayload(self):
        return {
            "id": str(self.id),
            "slno": self.slno,
            "source": self.source,
            "mine_name": self.mine_name,
            "sap_po": self.sap_po,
            "line_item": self.line_item,
            "do_no": self.do_no,
            "do_qty": self.do_qty,
            # "rake_no": self.rake_no,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "grade": self.grade,
            # "po_date": self.po_date,
        }

    

class SchedulerError(Document):
    JobId = StringField()
    ErrorMsg = StringField()
    Created_at = DateTimeField(default=datetime.datetime.utcnow())

    meta = {"db_alias": "gmrDB-alias", "collection": "SchedulerError"}


class SelectedLocation(Document):
    name = StringField()
    latlong = ListField()
    type = StringField()
    geofence = ListField()
    Created_at = DateTimeField(default=datetime.datetime.utcnow())

    meta = {"db_alias": "gmrDB-alias", "collection": "SelectedLocation"}

    def payload(self):
        return {
            "id": str(self.id),
            "name": self.name,
            "latlong": self.latlong,
            "geofence": self.geofence,
            "type": self.type,
        }


class PdfReportName(Document):
    name = StringField()
    created_at = DateTimeField(default=datetime.datetime.utcnow())

    meta = {"db_alias": "gmrDB-alias", "collection": "PdfReportName"}

    def payload(self):
        return {
            "id": str(self.id),
            "name": self.name,
            "created_at": self.created_at,
        }
    
class AveryRailData(EmbeddedDocument):
    indexing = StringField()
    wagon_owner = StringField()
    wagon_type = StringField()
    wagon_no = StringField()
    ser_no = StringField()
    rake_no = StringField()
    rake_id = StringField()
    wagon_no_avery = StringField()
    wagon_id = StringField()
    wagon_type_avery = StringField()
    wagon_cc = StringField()
    mode = StringField()
    tip_startdate = StringField()
    tip_starttime = StringField()
    tip_enddate = StringField()
    tip_endtime = StringField()
    tipple_time = StringField()
    status = StringField()
    # wagon_gross_wt = StringField()
    # wagon_tare_wt = StringField()
    # wagon_net_wt = StringField()
    gwel_gross_wt = StringField()
    gwel_tare_wt = StringField()
    gwel_net_wt = StringField()
    time_in_tipp = StringField()
    po_number = StringField()
    coal_grade = StringField()
    data_from = StringField()

    def payload(self):
        return {
            "indexing": self.indexing,
            "wagon_owner": self.wagon_owner,
            "wagon_type": self.wagon_type,
            "wagon_no": self.wagon_no,
            "ser_no": self.ser_no,
            "rake_no": self.rake_no,
            "rake_id": self.rake_id,
            "wagon_no_avery": self.wagon_no_avery,
            "wagon_id": self.wagon_id,
            "wagon_type": self.wagon_type,
            "wagon_cc": self.wagon_cc,
            "mode": self.mode,
            "tip_startdate": self.tip_startdate,
            "tip_starttime": self.tip_starttime,
            "tip_enddate": self.tip_enddate,
            "tip_endtime": self.tip_endtime,
            "tipple_time": self.tipple_time,
            "status": self.status,
            "wagon_type_avery": self.wagon_type_avery,
            # "wagon_gross_wt": self.wagon_gross_wt,
            # "wagon_tare_wt": self.wagon_tare_wt,
            # "wagon_net_wt": self.wagon_net_wt,
            "gwel_gross_wt": self.gwel_gross_wt,
            "gwel_tare_wt": self.gwel_tare_wt,
            "gwel_net_wt": self.gwel_net_wt,
            "time_in_tipp": self.time_in_tipp,
            "po_number": self.po_number,
            "coal_grade": self.coal_grade,
            "data_from": self.data_from,
        }


class SeclRailData(EmbeddedDocument):
    indexing = StringField()
    wagon_owner = StringField()
    wagon_type = StringField()
    wagon_no = StringField()
    secl_cc_wt = StringField()
    secl_gross_wt = StringField()
    secl_tare_wt = StringField()
    secl_net_wt = StringField()
    secl_ol_wt = StringField()
    secl_ul_wt = StringField()
    secl_chargable_wt = StringField()
    rly_cc_wt = StringField()
    rly_gross_wt = StringField()
    rly_tare_wt = StringField()
    rly_net_wt = StringField()
    rly_permissible_cc_wt = StringField()
    rly_ol_wt = StringField()
    rly_norm_rate = StringField()
    rly_pun_rate = StringField()
    rly_chargable_wt = StringField()
    rly_sliding_adjustment = StringField()

    def payload(self):
        return {
            "indexing": self.indexing,
            "wagon_owner": self.wagon_owner, 
            "wagon_type": self.wagon_type,
            "wagon_no": self.wagon_no,
            "secl_cc_wt": self.secl_cc_wt,
            "secl_gross_wt": self.secl_gross_wt,
            "secl_tare_wt": self.secl_tare_wt,
            "secl_net_wt": self.secl_net_wt,
            "secl_ol_wt": self.secl_ol_wt,
            "secl_ul_wt": self.secl_ul_wt,
            "secl_chargable_wt": self.secl_chargable_wt,
            "rly_cc_wt": self.rly_cc_wt,
            "rly_gross_wt": self.rly_gross_wt,
            "rly_tare_wt": self.rly_tare_wt,
            "rly_net_wt": self.rly_net_wt,
            "rly_permissible_cc_wt": self.rly_permissible_cc_wt,
            "rly_ol_wt": self.rly_ol_wt,
            "rly_norm_rate": self.rly_norm_rate,
            "rly_pun_rate": self.rly_pun_rate,
            "rly_chargable_wt": self.rly_chargable_wt,
            "rly_sliding_adjustment": self.rly_sliding_adjustment,
        }

    def rlypayload(self):
        return {
            "indexing": self.indexing,
            "wagon_owner": self.wagon_owner, 
            "wagon_type": self.wagon_type,
            "wagon_no": self.wagon_no,
            "rly_cc_wt": self.rly_cc_wt,
            "rly_gross_wt": self.rly_gross_wt,
            "rly_tare_wt": self.rly_tare_wt,
            "rly_net_wt": self.rly_net_wt,
            "rly_permissible_cc_wt": self.rly_permissible_cc_wt,
            "rly_ol_wt": self.rly_ol_wt,
            "rly_norm_rate": self.rly_norm_rate,
            "rly_pun_rate": self.rly_pun_rate,
            "rly_chargable_wt": self.rly_chargable_wt,
            "rly_sliding_adjustment": self.rly_sliding_adjustment,
        }


class RailData(Document):
    rr_no = StringField()
    rr_qty = StringField(null=True)
    po_no = StringField(null=True)
    po_date = StringField(null=True)
    line_item = StringField(null=True)
    source = StringField(null=True)
    placement_date = StringField(null=True)
    completion_date = StringField(null=True)
    avery_placement_date = StringField(null=True)
    avery_completion_date = StringField(null=True)
    drawn_date = StringField(null=True)
    total_ul_wt = StringField(null=True)
    boxes_supplied = StringField(null=True)
    total_secl_gross_wt = StringField(null=True)
    total_secl_tare_wt = StringField(null=True)
    total_secl_net_wt = StringField(null=True)
    total_secl_ol_wt = StringField(null=True)
    boxes_loaded = StringField(null=True)
    total_rly_gross_wt = StringField(null=True)
    total_rly_tare_wt = StringField(null=True)
    total_rly_net_wt = StringField(null=True)
    total_rly_ol_wt = StringField(null=True)
    total_secl_chargable_wt = StringField(null=True)
    total_rly_chargable_wt = StringField(null=True)
    freight = StringField(null=True)
    gst = StringField(null=True)
    pola = StringField(null=True)
    total_freight = StringField(null=True)
    sd = StringField(null=True)
    source_type = StringField(null=True)
    month = StringField(null=True)
    rr_date = StringField(null=True)
    siding = StringField(null=True)
    mine = StringField(null=True)
    grade = StringField(null=True)
    po_amount = StringField(null=True)
    rake_no = StringField(null=True)
    GWEL_received_wagons = StringField(null=True)
    GWEL_pending_wagons = StringField(null=True)
    Total_gwel_gross = StringField(null=True)
    Total_gwel_tare = StringField(null=True)
    Total_gwel_net = StringField(null=True)

    penalty_ol = StringField(null=True)                    # modified by faisal
    penal_ul = StringField(null=True)                      # modified by faisal
    freight_pmt = StringField(null=True)                   # modified by faisal              

    secl_rly_data = EmbeddedDocumentListField(SeclRailData)
    avery_rly_data = EmbeddedDocumentListField(AveryRailData)
    created_at = DateTimeField(default=datetime.datetime.utcnow)

    meta = {"db_alias": "gmrDB-alias", "collection": "raildata"}

    def payload(self):
        seclrail = []
        for serl_data in self.secl_rly_data:
            seclrail.append(serl_data.payload())

        return {
            "id": str(self.id),
            "rr_no": self.rr_no,
            "rr_qty": self.rr_qty,
            "po_no": self.po_no,
            "po_date": self.po_date,
            "line_item": self.line_item,
            "source": self.source,
            "placement_date": self.placement_date,
            "completion_date": self.completion_date,
            "avery_placement_date": self.avery_placement_date,
            "avery_completion_date": self.avery_completion_date,
            "drawn_date": self.drawn_date,
            "total_ul_wt": self.total_ul_wt,
            "boxes_supplied": self.boxes_supplied,
            "total_secl_gross_wt": self.total_secl_gross_wt,
            "total_secl_tare_wt": self.total_secl_tare_wt,
            "total_secl_net_wt": self.total_secl_net_wt,
            "total_secl_ol_wt": self.total_secl_ol_wt,
            "boxes_loaded": self.boxes_loaded,
            "total_rly_gross_wt": self.total_rly_gross_wt,
            "total_rly_tare_wt": self.total_rly_tare_wt,
            "total_rly_net_wt": self.total_rly_net_wt,
            "total_rly_ol_wt": self.total_rly_ol_wt,
            "total_secl_chargable_wt": self.total_secl_chargable_wt,
            "total_rly_chargable_wt": self.total_rly_chargable_wt,
            "freight": self.freight,
            "gst": self.gst,
            "pola": self.pola,
            "total_freight": self.total_freight,
            "source_type": self.source_type,
            "month": self.month,
            "rr_date": self.rr_date,
            "siding": self.siding,
            "mine": self.mine,
            "grade": self.grade,
            "po_amount": self.po_amount,
            "rake_no": self.rake_no,
            "Total_gwel_gross": self.Total_gwel_gross,
            "Total_gwel_tare": self.Total_gwel_tare,
            "Total_gwel_net": self.Total_gwel_net,
            "secl_rly_data": seclrail,
            "created_at": datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
        }
    
    def averyPayload(self):
        averyrail = []
        for avery_data in self.avery_rly_data:
            averyrail.append(avery_data.payload())

        seclrail = []
        for serl_data in self.secl_rly_data:
            seclrail.append(serl_data.payload())

        return {
            "rr_no": self.rr_no,
            "rr_qty": self.rr_qty,
            "po_no": self.po_no,
            "po_date": self.po_date,
            "line_item": self.line_item,
            "source": self.source,
            "placement_date": self.placement_date,
            "completion_date": self.completion_date,
            "drawn_date": self.drawn_date,
            "total_ul_wt": self.total_ul_wt,
            "boxes_supplied": self.boxes_supplied,
            "total_secl_gross_wt": self.total_secl_gross_wt,
            "total_secl_tare_wt": self.total_secl_tare_wt,
            "total_secl_net_wt": self.total_secl_net_wt,
            "total_secl_ol_wt": self.total_secl_ol_wt,
            "boxes_loaded": self.boxes_loaded,
            "total_rly_gross_wt": self.total_rly_gross_wt,
            "total_rly_tare_wt": self.total_rly_tare_wt,
            "total_rly_net_wt": self.total_rly_net_wt,
            "total_rly_ol_wt": self.total_rly_ol_wt,
            "total_secl_chargable_wt": self.total_secl_chargable_wt,
            "total_rly_chargable_wt": self.total_rly_chargable_wt,
            "freight": self.freight,
            "gst": self.gst,
            "pola": self.pola,
            "total_freight": self.total_freight,
            "source_type": self.source_type,
            "month": self.month,
            "rr_date": self.rr_date,
            "siding": self.siding,
            "mine": self.mine,
            "grade": self.grade,
            "po_amount": self.po_amount,
            "rake_no": self.rake_no,
            "GWEL_received_wagons": self.GWEL_received_wagons,
            "GWEL_pending_wagons": self.GWEL_pending_wagons,
            "GWEL_Total_gwel_gross": self.Total_gwel_gross,
            "GWEL_Total_gwel_tare": self.Total_gwel_tare,
            "GWEL_Total_gwel_net": self.Total_gwel_net,
            "secl_rly_data": seclrail,
            "avery_rly_data": averyrail,
            "created_at": datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
        }

    def averyPayloadMain(self):
        return {
            "rr_no": self.rr_no,
            "rr_qty": self.rr_qty,
            "po_no": self.po_no,
            "po_date": self.po_date,
            "line_item": self.line_item,
            "source": self.source,
            "GWEL_placement_date": self.avery_placement_date,
            "GWEL_completion_date": self.avery_completion_date,
            "boxes_loaded": self.boxes_loaded,
            # "GWEL_received_wagons"
            # "GWEL_pending_wagons"
            # "total_gwel_gross_wt"
            # "total_gwel_tare_wt"
            # "total_gwel_net_wt"
            "total_secl_gross_wt": self.total_secl_gross_wt,
            "total_secl_tare_wt": self.total_secl_tare_wt,
            "total_secl_net_wt": self.total_secl_net_wt,
            "total_rly_gross_wt": self.total_rly_gross_wt,
            "total_rly_tare_wt": self.total_rly_tare_wt,
            "total_rly_net_wt": self.total_rly_net_wt,
            "source_type": self.source_type,
            "month": self.month,
            "rr_date": self.rr_date,
            "siding": self.siding,
            "mine": self.mine,
            "grade": self.grade,
            "GWEL_received_wagons": self.GWEL_received_wagons,
            "GWEL_pending_wagons": self.GWEL_pending_wagons,
            "GWEL_Total_gwel_gross": self.Total_gwel_gross,
            "GWEL_Total_gwel_tare": self.Total_gwel_tare,
            "GWEL_Total_gwel_net": self.Total_gwel_net,
            "created_at": datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
        }

    def simplepayload(self):
        return {
            "rr_no": self.rr_no,
            "rr_qty": self.rr_qty,
            "po_no": self.po_no,
            "po_date": self.po_date,
            "line_item": self.line_item,
            "source": self.source,
            "placement_date": self.placement_date,
            "completion_date": self.completion_date,
            "drawn_date": self.drawn_date,
            "total_ul_wt": self.total_ul_wt,
            "boxes_supplied": self.boxes_supplied,
            "total_secl_gross_wt": self.total_secl_gross_wt,
            "total_secl_tare_wt": self.total_secl_tare_wt,
            "total_secl_net_wt": self.total_secl_net_wt,
            "total_secl_ol_wt": self.total_secl_ol_wt,
            "boxes_loaded": self.boxes_loaded,
            "total_rly_gross_wt": self.total_rly_gross_wt,
            "total_rly_tare_wt": self.total_rly_tare_wt,
            "total_rly_net_wt": self.total_rly_net_wt,
            "total_rly_ol_wt": self.total_rly_ol_wt,
            "total_secl_chargable_wt": self.total_secl_chargable_wt,
            "total_rly_chargable_wt": self.total_rly_chargable_wt,
            "freight": self.freight,
            "gst": self.gst,
            "pola": self.pola,
            "total_freight": self.total_freight,
            "source_type": self.source_type,
            "month": self.month,
            "rr_date": self.rr_date,
            "siding": self.siding,
            "mine": self.mine,
            "grade": self.grade,
            "po_amount": self.po_amount,
            "rake_no": self.rake_no,
            "created_at": datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
        }
    
class RcrData(Document):
    rr_no = StringField()
    rr_qty = StringField(null=True)
    po_no = StringField(null=True)
    po_date = StringField(null=True)
    line_item = StringField(null=True)
    source = StringField(null=True)
    placement_date = StringField(null=True)
    completion_date = StringField(null=True)
    avery_placement_date = StringField(null=True)
    avery_completion_date = StringField(null=True)
    drawn_date = StringField(null=True)
    total_ul_wt = StringField(null=True)
    boxes_supplied = StringField(null=True)
    total_secl_gross_wt = StringField(null=True)
    total_secl_tare_wt = StringField(null=True)
    total_secl_net_wt = StringField(null=True)
    total_secl_ol_wt = StringField(null=True)
    boxes_loaded = StringField(null=True)
    total_rly_gross_wt = StringField(null=True)
    total_rly_tare_wt = StringField(null=True)
    total_rly_net_wt = StringField(null=True)
    total_rly_ol_wt = StringField(null=True)
    total_secl_chargable_wt = StringField(null=True)
    total_rly_chargable_wt = StringField(null=True)
    freight = StringField(null=True)
    gst = StringField(null=True)
    pola = StringField(null=True)
    total_freight = StringField(null=True)
    sd = StringField(null=True)
    source_type = StringField(null=True)
    month = StringField(null=True)
    rr_date = StringField(null=True)
    siding = StringField(null=True)
    mine = StringField(null=True)
    grade = StringField(null=True)
    po_amount = StringField(null=True)
    rake_no = StringField(null=True)
    GWEL_received_wagons = StringField(null=True)
    GWEL_pending_wagons = StringField(null=True)
    Total_gwel_gross = StringField(null=True)
    Total_gwel_tare = StringField(null=True)
    Total_gwel_net = StringField(null=True)
    start_date = StringField(null=True)
    end_date = StringField(null=True)
    slno = StringField(null=True)
    type_consumer = StringField(null=True)
    po_qty = StringField(null=True)

    penalty_ol = StringField(null=True)                    # modified by faisal
    penal_ul = StringField(null=True)                      # modified by faisal
    freight_pmt = StringField(null=True)                   # modified by faisal              

    secl_rly_data = EmbeddedDocumentListField(SeclRailData)
    avery_rly_data = EmbeddedDocumentListField(AveryRailData)
    created_at = DateTimeField(default=datetime.datetime.utcnow)

    meta = {"db_alias": "gmrDB-alias", "collection": "RcrData"}

    def payload(self):
        seclrail = []
        for serl_data in self.secl_rly_data:
            seclrail.append(serl_data.rlypayload())

        return {
            "id": str(self.id),
            "rr_no": self.rr_no,
            "rr_qty": self.rr_qty,
            "po_no": self.po_no,
            "po_date": self.po_date,
            "line_item": self.line_item,
            "source": self.source,
            "placement_date": self.placement_date,
            "completion_date": self.completion_date,
            "avery_placement_date": self.avery_placement_date,
            "avery_completion_date": self.avery_completion_date,
            "drawn_date": self.drawn_date,
            "total_ul_wt": self.total_ul_wt,
            "boxes_supplied": self.boxes_supplied,
            "total_secl_gross_wt": self.total_secl_gross_wt,
            "total_secl_tare_wt": self.total_secl_tare_wt,
            "total_secl_net_wt": self.total_secl_net_wt,
            "total_secl_ol_wt": self.total_secl_ol_wt,
            "boxes_loaded": self.boxes_loaded,
            "total_rly_gross_wt": self.total_rly_gross_wt,
            "total_rly_tare_wt": self.total_rly_tare_wt,
            "total_rly_net_wt": self.total_rly_net_wt,
            "total_rly_ol_wt": self.total_rly_ol_wt,
            "total_secl_chargable_wt": self.total_secl_chargable_wt,
            "total_rly_chargable_wt": self.total_rly_chargable_wt,
            "freight": self.freight,
            "gst": self.gst,
            "pola": self.pola,
            "total_freight": self.total_freight,
            "source_type": self.source_type,
            "month": self.month,
            "rr_date": self.rr_date,
            "siding": self.siding,
            "mine": self.mine,
            "grade": self.grade,
            "po_amount": self.po_amount,
            "rake_no": self.rake_no,
            "secl_rly_data": seclrail,
            "created_at": datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
        }
    
    def averyPayload(self):
        averyrail = []
        for avery_data in self.avery_rly_data:
            averyrail.append(avery_data.payload())

        seclrail = []
        for serl_data in self.secl_rly_data:
            seclrail.append(serl_data.payload())

        return {
            "rr_no": self.rr_no,
            "rr_qty": self.rr_qty,
            "po_no": self.po_no,
            "po_date": self.po_date,
            "line_item": self.line_item,
            "source": self.source,
            "placement_date": self.placement_date,
            "completion_date": self.completion_date,
            "drawn_date": self.drawn_date,
            "total_ul_wt": self.total_ul_wt,
            "boxes_supplied": self.boxes_supplied,
            "total_secl_gross_wt": self.total_secl_gross_wt,
            "total_secl_tare_wt": self.total_secl_tare_wt,
            "total_secl_net_wt": self.total_secl_net_wt,
            "total_secl_ol_wt": self.total_secl_ol_wt,
            "boxes_loaded": self.boxes_loaded,
            "total_rly_gross_wt": self.total_rly_gross_wt,
            "total_rly_tare_wt": self.total_rly_tare_wt,
            "total_rly_net_wt": self.total_rly_net_wt,
            "total_rly_ol_wt": self.total_rly_ol_wt,
            "total_secl_chargable_wt": self.total_secl_chargable_wt,
            "total_rly_chargable_wt": self.total_rly_chargable_wt,
            "freight": self.freight,
            "gst": self.gst,
            "pola": self.pola,
            "total_freight": self.total_freight,
            "source_type": self.source_type,
            "month": self.month,
            "rr_date": self.rr_date,
            "siding": self.siding,
            "mine": self.mine,
            "grade": self.grade,
            "po_amount": self.po_amount,
            "rake_no": self.rake_no,
            "GWEL_received_wagons": self.GWEL_received_wagons,
            "GWEL_pending_wagons": self.GWEL_pending_wagons,
            "GWEL_Total_gwel_gross": self.Total_gwel_gross,
            "GWEL_Total_gwel_tare": self.Total_gwel_tare,
            "GWEL_Total_gwel_net": self.Total_gwel_net,
            "secl_rly_data": seclrail,
            "avery_rly_data": averyrail,
            "created_at": datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
        }

    def averyPayloadMain(self):
        return {
            "rr_no": self.rr_no,
            "rr_qty": self.rr_qty,
            "po_no": self.po_no,
            "po_date": self.po_date,
            "line_item": self.line_item,
            "source": self.source,
            "GWEL_placement_date": self.avery_placement_date,
            "GWEL_completion_date": self.avery_completion_date,
            "boxes_loaded": self.boxes_loaded,
            # "GWEL_received_wagons"
            # "GWEL_pending_wagons"
            # "total_gwel_gross_wt"
            # "total_gwel_tare_wt"
            # "total_gwel_net_wt"
            "total_secl_gross_wt": self.total_secl_gross_wt,
            "total_secl_tare_wt": self.total_secl_tare_wt,
            "total_secl_net_wt": self.total_secl_net_wt,
            "total_rly_gross_wt": self.total_rly_gross_wt,
            "total_rly_tare_wt": self.total_rly_tare_wt,
            "total_rly_net_wt": self.total_rly_net_wt,
            "source_type": self.source_type,
            "month": self.month,
            "rr_date": self.rr_date,
            "siding": self.siding,
            "mine": self.mine,
            "grade": self.grade,
            "GWEL_received_wagons": self.GWEL_received_wagons,
            "GWEL_pending_wagons": self.GWEL_pending_wagons,
            "GWEL_Total_gwel_gross": self.Total_gwel_gross,
            "GWEL_Total_gwel_tare": self.Total_gwel_tare,
            "GWEL_Total_gwel_net": self.Total_gwel_net,
            "created_at": datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
        }

    def simplepayloadold(self):
        return {
            "rr_no": self.rr_no,
            "rr_qty": self.rr_qty,
            "po_no": self.po_no,
            "po_date": self.po_date,
            "line_item": self.line_item,
            "source": self.source,
            "placement_date": self.placement_date,
            "completion_date": self.completion_date,
            "drawn_date": self.drawn_date,
            "total_ul_wt": self.total_ul_wt,
            "boxes_supplied": self.boxes_supplied,
            "total_secl_gross_wt": self.total_secl_gross_wt,
            "total_secl_tare_wt": self.total_secl_tare_wt,
            "total_secl_net_wt": self.total_secl_net_wt,
            "total_secl_ol_wt": self.total_secl_ol_wt,
            "boxes_loaded": self.boxes_loaded,
            "total_rly_gross_wt": self.total_rly_gross_wt,
            "total_rly_tare_wt": self.total_rly_tare_wt,
            "total_rly_net_wt": self.total_rly_net_wt,
            "total_rly_ol_wt": self.total_rly_ol_wt,
            "total_secl_chargable_wt": self.total_secl_chargable_wt,
            "total_rly_chargable_wt": self.total_rly_chargable_wt,
            "freight": self.freight,
            "gst": self.gst,
            "pola": self.pola,
            "total_freight": self.total_freight,
            "source_type": self.source_type,
            "month": self.month,
            "rr_date": self.rr_date,
            "siding": self.siding,
            "mine": self.mine,
            "grade": self.grade,
            "po_amount": self.po_amount,
            "rake_no": self.rake_no,
            "created_at": datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
        }

    def simplepayload(self):
        return {
            "rr_no": self.rr_no,
            "rr_qty": self.rr_qty,
            "po_no": self.po_no,
            "po_date": self.po_date,
            "line_item": self.line_item,
            "source": self.source,
            "placement_date": self.placement_date,
            "completion_date": self.completion_date,
            "drawn_date": self.drawn_date,
            "total_ul_wt": self.total_ul_wt,
            "boxes_supplied": self.boxes_supplied,
            "boxes_loaded": self.boxes_loaded,
            "total_rly_gross_wt": self.total_rly_gross_wt,
            "total_rly_tare_wt": self.total_rly_tare_wt,
            "total_rly_net_wt": self.total_rly_net_wt,
            "total_rly_ol_wt": self.total_rly_ol_wt,
            "total_rly_chargable_wt": self.total_rly_chargable_wt,
            "freight": self.freight,
            "gst": self.gst,
            "pola": self.pola,
            "total_freight": self.total_freight,
            "source_type": self.source_type,
            "month": self.month,
            "rr_date": self.rr_date,
            "siding": self.siding,
            "mine": self.mine,
            "grade": self.grade,
            "po_amount": self.po_amount,
            "rake_no": self.rake_no,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "po_qty": self.po_qty,
            "type_consumer": self.type_consumer,
            "created_at": datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
        }
    
class sampleParametersData(EmbeddedDocument):
    sample_details_id = IntField()
    parameters_id = IntField()
    parameter_name =  StringField(null=True)
    unit_val = StringField(null=True)
    test_method = StringField(null=True)
    val1 = StringField(null=True)
    parameter_type = StringField(null=True)

    def payload(self):
        return {
            "sample_details_id": self.sample_details_id,
            "parameters_id": self.parameters_id,
            "parameter_name":  self.parameter_name,
            "unit_val": self.unit_val,
            "test_method": self.test_method,
            "val1": self.val1,
            "parameter_type": self.parameter_type,
        }

class BunkerData(Document):
    sample_details_id = IntField()
    work_order_id = IntField()
    test_report_no = StringField(null=True)
    ulr_no = StringField(null=True)
    test_report_date = StringField(null=True)
    sample_id_no = StringField(null=True)
    sample_desc = StringField(null=True)
    # rake_no = StringField(null=True)
    # rrNo = StringField(null=True)
    rR_Qty = StringField(null=True)
    supplier = StringField(null=True)
    received_condition = StringField(null=True)
    from_sample_condition_date = StringField(null=True)
    to_sample_condition_date = StringField(null=True)
    sample_received_date = StringField(null=True)
    sample_date = StringField(null=True)
    analysis_date = StringField(null=True)
    sample_qty = StringField(null=True)
    # method_reference = StringField(null=True)
    humidity = StringField(null=True)
    test_temp = StringField(null=True)
    sample_parameters = EmbeddedDocumentListField(sampleParametersData)
    created_at = DateTimeField(default=datetime.datetime.utcnow)

    meta = {"db_alias": "gmrDB-alias", "collection": "bunkerdata"}

    def payload(self):
        sampleDetails = []
        for sample_data in self.sample_parameters:
            sampleDetails.append(sample_data.payload())
        return {
            "id": str(self.id),
            "sample_details_id": self.sample_details_id,
            "work_order_id": self.work_order_id,
            "test_report_no": self.test_report_no,
            "ulr_no": self.ulr_no,
            "test_report_date": self.test_report_date,
            "sample_id_no": self.sample_id_no,
            "sample_desc": self.sample_desc,
            # "rake_no": self.rake_no,
            # "rrNo": self.rrNo,
            "rR_Qty": self.rR_Qty,
            "supplier": self.supplier,
            "received_condition": self.received_condition,
            "from_sample_condition_date": self.from_sample_condition_date,
            "to_sample_condition_date": self.to_sample_condition_date,
            "sample_received_date": self.sample_received_date,
            "sample_date": self.sample_date,
            "analysis_date": self.analysis_date,
            "sample_qty": self.sample_qty,
            # "method_reference": self.method_reference,
            "humidity": self.humidity,
            "test_temp": self.test_temp,
            "sample_parameters": sampleDetails,
            "created_at": self.created_at,
        }

    def simplepayload(self):
        return {
            # "id": str(self.id),
            "sample_details_id": self.sample_details_id,
            "work_order_id": self.work_order_id,
            "test_report_no": self.test_report_no,
            "ulr_no": self.ulr_no,
            "test_report_date": self.test_report_date,
            "sample_id_no": self.sample_id_no,
            "sample_desc": self.sample_desc,
            # "rake_no": self.rake_no,
            # "rrNo": self.rrNo,
            "rR_Qty": self.rR_Qty,
            "supplier": self.supplier,
            "received_condition": self.received_condition,
            "from_sample_condition_date": self.from_sample_condition_date,
            "to_sample_condition_date": self.to_sample_condition_date,
            "sample_received_date": self.sample_received_date,
            "sample_date": self.sample_date,
            "analysis_date": self.analysis_date,
            "sample_qty": self.sample_qty,
            # "method_reference": self.method_reference,
            "humidity": self.humidity,
            "test_temp": self.test_temp,
            "created_at": self.created_at,
        }
    


class emailNotifications(Document):
    notification_name = StringField()
    time_log = DateTimeField(default=datetime.datetime.utcnow)
    created_at = DateTimeField(default=datetime.datetime.utcnow)
    expireOn = DateTimeField(
        default=datetime.datetime.utcnow() + datetime.timedelta(minutes=7 * 24 * 60)
    )
    meta = {"indexes": [{"fields": ["expireOn"], "expireAfterSeconds": 0}], 'db_alias': 'gmrDB-alias', 'collection': 'emailNotifications'}


    # meta = {
    #     'indexes': [
    #         {
    #             'fields': ['created_at'],
    #             'expireAfterSeconds': 60*60*24*7 # delete after 7 days
    #         }
    #     ],
    #     'db_alias': 'gmrDB-alias', 'collection': 'emailNotifications'
    # }

    def payload(self):
        return {
            "notification_name": self.notification_name,
            "time_log": self.time_log,
            "created_at": str(self.created_at)
        }
    

class bunkerAnalysis(Document):
    units = StringField(default=None)
    tagid = IntField()
    bunkering = StringField(default=None)
    mgcv = StringField(default=None)
    hgcv = StringField(default=None)
    ratio = StringField(default=None)
    shift_name = StringField(default=None)
    start_date = DateTimeField()
    created_date = DateTimeField()
    ID = IntField(min_value=1)
    created_at = DateTimeField(default=datetime.datetime.utcnow())

    meta = {"db_alias": "gmrDB-alias", "collection": "bunkerAnalysis"}

    def payload(self):
        return {
            "id": str(self.id),
            "Sr.No": self.ID,
            "shift_name": self.shift_name,
            "unit": self.units,
            # "tagid": self.tagid,
            "bunkering": self.bunkering,
            "mgcv": self.mgcv,
            "hgcv": self.hgcv,
            "ratio": self.ratio,

            # "Date": datetime.datetime.fromisoformat(
            #         self.created_date.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
            #         ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_date else None,
            "start_date": self.start_date.strftime("%Y-%m-%d %H:%M:%S"),
            "Date": self.created_date.strftime("%Y-%m-%d %H:%M:%S"),

            # "created_at": datetime.datetime.fromisoformat(
            #         self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
            #         ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
        }


class shiftScheduler(Document):
    shift_name = StringField(default=None)
    start_shift_time = StringField(default=None)
    end_shift_time = StringField(default=None)
    report_name = StringField(default=None)
    filter = StringField(default=None)
    schedule = StringField(default="")
    time = StringField(default="")
    duration = StringField(default="")
    created_at = DateTimeField(default=datetime.datetime.utcnow)

    meta = {"db_alias": "gmrDB-alias", "collection": "shiftScheduler"}

    def payload(self):
        return {
            "id": str(self.id),
            "shift_name": self.shift_name,
            "start_shift_time": self.start_shift_time,
            "end_shift_time": self.end_shift_time,
            # "report_name": self.report_name,
            "filter": self.filter,
            "schedule": self.schedule,
            "time": self.time,
            "duration": self.duration,
            "created_at": self.created_at,
        }
    

class EmailDevelopmentCheck(Document):
    development = StringField(default=None)
    avery_id = StringField(default=None)
    avery_pass = StringField(defaut=None)
    wagontrippler1 = StringField(default=None)
    wagontrippler2 = StringField(default=None)
    port = StringField(default=None)

    meta = {"db_alias": "gmrDB-alias", "collection": "EmailDevelopmentCheck"}

    def payload(self):
        return {
            "development": self.development,
        }
    
class SchedulerShifts(Document):
    scheduler_name = StringField(default=None)
    created_at = DateTimeField(default=datetime.datetime.utcnow)

    meta = {"db_alias": "gmrDB-alias", "collection": "SchedulerShifts"}

    def payload(self):
        return {
            "scheduler_name": self.scheduler_name,
            "created_at": self.created_at,
        }
    

class rakeQuota(Document):
    ID = IntField(min_value=1)
    month = StringField(default=None)
    year = StringField(default=None)
    valid_upto = StringField(default=None)
    coal_field =  StringField(default=None)
    rake_alloted = StringField(default=None)
    rake_received = StringField(default=None)
    due = StringField(default=None)
    grade = StringField(default=None)
    expected_rakes = DictField(null=True)
    source_type = StringField(null=True)
    cancelled_rakes = StringField(null=True)
    remarks = StringField(null=True)
    created_at = DateTimeField(default=datetime.datetime.utcnow)

    meta = {"db_alias": "gmrDB-alias", "collection": "rakeQuota"}

    def payload(self):
        return {
            "SrNo": self.ID,
            "month": self.month,
            "year": self.year,
            "valid_upto": self.valid_upto,
            "rake_alloted": self.rake_alloted,
            "rake_received": self.rake_received,
            "due": self.due,
            "expected_rakes": self.expected_rakes,
            "source_type": self.source_type,
            "cancelled_rakes": self.cancelled_rakes,
            "remarks": self.remarks,
            "created_at": datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
        }


class rcrrakeQuota(Document):
    ID = IntField(min_value=1)
    month = StringField(default=None)
    year = StringField(default=None)
    valid_upto = StringField(default=None)
    coal_field =  StringField(default=None)
    rake_alloted = StringField(default=None)
    rake_received = StringField(default=None)
    due = StringField(default=None)
    grade = StringField(default=None)
    expected_rakes = DictField(null=True)
    source_type = StringField(null=True)
    cancelled_rakes = StringField(null=True)
    remarks = StringField(null=True)
    created_at = DateTimeField(default=datetime.datetime.utcnow)

    meta = {"db_alias": "gmrDB-alias", "collection": "rcrrakeQuota"}

    def payload(self):
        return {
            "SrNo": self.ID,
            "month": self.month,
            "year": self.year,
            "valid_upto": self.valid_upto,
            "rake_alloted": self.rake_alloted,
            "rake_received": self.rake_received,
            "due": self.due,
            "expected_rakes": self.expected_rakes,
            "source_type": self.source_type,
            "created_at": datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
        }

    
class sapRecordsRCR(Document):
    rr_no = StringField(null=True)
    sap_po = StringField(null=True)
    rr_date = StringField(null=True)
    start_date = StringField(null=True)
    end_date = StringField(null=True)
    month = StringField(null=True)
    consumer_type = StringField(null=True)
    grade = StringField(null=True)
    mine = StringField(null=True)
    line_item = StringField(null=True)
    rr_qty = StringField(null=True)
    po_amount = StringField(null=True)
    
    secl_mode_transport = StringField(null=True)
    area = StringField(null=True)
    secl_basic_price = FloatField(null=True)
    secl_sizing_charges = FloatField(null=True)
    secl_stc_charges = FloatField(null=True)
    secl_evac_facility_charges = FloatField(null=True)
    secl_royality_charges = FloatField(null=True)
    secl_nmet_charges = FloatField(null=True)
    secl_dmf = FloatField(null=True)
    secl_adho_sanrachna_vikas = FloatField(null=True)
    secl_pariyavaran_upkar = FloatField(null=True)
    secl_terminal_tax = FloatField(null=True)
    secl_assessable_tax = FloatField(null=True)
    secl_igst = FloatField(null=True)
    secl_gst_comp_cess = FloatField(null=True)
    sap_po = StringField(null=True)
    created_at = DateTimeField(default=datetime.datetime.utcnow)
    # id = IntField(min_value=1)

    meta = {"db_alias": "gmrDB-alias", "collection": "sapRecordsRCR"}

    def payload(self):
        return {
            # "srno": str(self.id),
            "rr_no": self.rr_no,
            "rr_date": self.rr_date,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "month": self.month,
            "consumer_type": self.consumer_type,
            "grade": self.grade,
            "mine": self.mine,
            "line_item": self.line_item,
            "rr_qty": self.rr_qty,
            "po_amount": self.po_amount,
            "sap_po": self.sap_po,
            "created_at": datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
        }
    

class sapRecordsRail(Document):
    month = StringField(null=True)
    rr_no = StringField(null=True)
    rr_date = StringField(null=True)
    siding = StringField(null=True)
    mine = StringField(null=True)
    grade = StringField(null=True)
    rr_qty = StringField(null=True)
    po_amount = StringField(null=True)
    sap_po = StringField(null=True)
    do_date = StringField(null=True) #sap po date
    line_item = StringField(null=True)

    #particulars start

    sizing_charges = FloatField(null=True)
    evac_facility_charge = FloatField(null=True)
    royality_charges = FloatField(null=True)
    nmet_charges = FloatField(null=True)
    dmf = FloatField(null=True)
    adho_sanrachna_vikas= FloatField(null=True)
    pariyavaran_upkar = FloatField(null=True)
    assessable_value = FloatField(null=True)
    igst = FloatField(null=True)
    gst_comp_cess = FloatField(null=True)
    gross_bill_value = FloatField(null=True)
    less_underloading_charges = FloatField(null=True)
    net_value = FloatField(null=True)
    total_amount = FloatField(null=True)

    #particulars end


    created_at = DateTimeField(default=datetime.datetime.utcnow)

    meta = {"db_alias": "gmrDB-alias", "collection": "sapRecordsRail"}

    def payload(self):
        return {
            "month": self.month,
            "rr_no": self.rr_no,
            "rr_date": self.rr_date,
            "siding": self.siding,
            "mine": self.mine,
            "grade": self.grade,
            "rr_qty": self.rr_qty,
            "po_amount": self.po_amount,
            "sap_po": self.sap_po,
            "do_date": self.do_date,
            "line_item": self.line_item,
            "created_at": datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
        }
    


class BunkerDataExtra(Document):
    sample_details_id = IntField()
    work_order_id = IntField()
    test_report_no = StringField(null=True)
    ulr_no = StringField(null=True)
    test_report_date = StringField(null=True)
    sample_id_no = StringField(null=True)
    sample_desc = StringField(null=True)
    # rake_no = StringField(null=True)
    # rrNo = StringField(null=True)
    rR_Qty = StringField(null=True)
    supplier = StringField(null=True)
    received_condition = StringField(null=True)
    from_sample_condition_date = StringField(null=True)
    to_sample_condition_date = StringField(null=True)
    sample_received_date = StringField(null=True)
    sample_date = StringField(null=True)
    analysis_date = StringField(null=True)
    sample_qty = StringField(null=True)
    # method_reference = StringField(null=True)
    humidity = StringField(null=True)
    test_temp = StringField(null=True)
    sample_parameters = EmbeddedDocumentListField(sampleParametersData)
    created_at = DateTimeField(default=datetime.datetime.utcnow)

    meta = {"db_alias": "gmrDB-alias", "collection": "BunkerDataExtra"}

    def payload(self):
        sampleDetails = []
        for sample_data in self.sample_parameters:
            sampleDetails.append(sample_data.payload())
        return {
            "id": str(self.id),
            "sample_details_id": self.sample_details_id,
            "work_order_id": self.work_order_id,
            "test_report_no": self.test_report_no,
            "ulr_no": self.ulr_no,
            "test_report_date": self.test_report_date,
            "sample_id_no": self.sample_id_no,
            "sample_desc": self.sample_desc,
            # "rake_no": self.rake_no,
            # "rrNo": self.rrNo,
            "rR_Qty": self.rR_Qty,
            "supplier": self.supplier,
            "received_condition": self.received_condition,
            "from_sample_condition_date": self.from_sample_condition_date,
            "to_sample_condition_date": self.to_sample_condition_date,
            "sample_received_date": self.sample_received_date,
            "sample_date": self.sample_date,
            "analysis_date": self.analysis_date,
            "sample_qty": self.sample_qty,
            # "method_reference": self.method_reference,
            "humidity": self.humidity,
            "test_temp": self.test_temp,
            "sample_parameters": sampleDetails,
            "created_at": self.created_at,
        }

    def simplepayload(self):
        return {
            # "id": str(self.id),
            "sample_details_id": self.sample_details_id,
            "work_order_id": self.work_order_id,
            "test_report_no": self.test_report_no,
            "ulr_no": self.ulr_no,
            "test_report_date": self.test_report_date,
            "sample_id_no": self.sample_id_no,
            "sample_desc": self.sample_desc,
            # "rake_no": self.rake_no,
            # "rrNo": self.rrNo,
            "rR_Qty": self.rR_Qty,
            "supplier": self.supplier,
            "received_condition": self.received_condition,
            "from_sample_condition_date": self.from_sample_condition_date,
            "to_sample_condition_date": self.to_sample_condition_date,
            "sample_received_date": self.sample_received_date,
            "sample_date": self.sample_date,
            "analysis_date": self.analysis_date,
            "sample_qty": self.sample_qty,
            # "method_reference": self.method_reference,
            "humidity": self.humidity,
            "test_temp": self.test_temp,
            "created_at": self.created_at,
        }


class gcvComparisionAnalysis(Document):
    coal_receipt_year = IntField()
    coal_receipt_month = IntField()
    coal_receipt_domestic_qty = IntField()
    coal_receipt_domestic_gcv = IntField()
    coal_receipt_imported_qty = IntField()
    coal_receipt_imported_gcv = IntField()
    coal_receipt_weighted_gcv = IntField()
    coal_receipt_ytd_weighted_gcv = IntField()
    bunker_coal_weighted_gcv = IntField()
    bunker_coal_imported_qty = IntField()
    bunker_coal_domestic_qty = IntField()
    bunker_coal_weighted_gcv_ytd = IntField()
    difference_mtd = IntField()
    difference_ytd = IntField()

    meta = {"db_alias": "gmrDB-alias", "collection": "gcvComparisionAnalysis"}

    def payload(self):
        return {
            "coal_receipt_year": self.coal_receipt_year,
            "coal_receipt_month": self.coal_receipt_month,
            "coal_receipt_domestic_qty": self.coal_receipt_domestic_qty,
            "coal_receipt_domestic_gcv": self.coal_receipt_domestic_gcv,
            "coal_receipt_imported_qty": self.coal_receipt_imported_qty,
            "coal_receipt_imported_gcv": self.coal_receipt_imported_gcv,
            "coal_receipt_weighted_gcv": self.coal_receipt_weighted_gcv,
            "coal_receipt_ytd_weighted_gcv": self.coal_receipt_ytd_weighted_gcv,
            "bunker_coal_weighted_gcv": self.bunker_coal_weighted_gcv,
            "bunker_coal_imported_qty": self.bunker_coal_imported_qty,
            "bunker_coal_domestic_qty": self.bunker_coal_domestic_qty,
            "bunker_coal_weighted_gcv_ytd": self.bunker_coal_weighted_gcv_ytd,
            "difference_mtd": self.difference_mtd,
            "difference_ytd": self.difference_ytd,
        }
    

class BunkerQualitySummary(Document):
    date = DateTimeField(required=True, default=datetime.datetime.utcnow)
    cum_total_qty = FloatField(required=True)
    cum_weighted_domestic_gcv = FloatField(required=True)
    domestic_qty = FloatField(required=True)
    imported_qty = IntField(required=True)
    total_qty = FloatField(required=True)
    weighted_domestic_gcv = FloatField(required=True)
    weighted_gcv = FloatField(required=True)
    wt_gcv = FloatField(required=True)

    cr_domestic_gcv_mtd = FloatField(null=True)
    cr_weighted_gcv_ytd = FloatField(null=True)
    cr_domestic_qty_mtd = FloatField(null=True)
    cr_imported_qty_mtd = FloatField(null=True)
    cr_imported_gcv_mtd = FloatField(null=True)
    cr_weighted_gcv_mtd = FloatField(null=True)
    difference_in_gcv_mtd = FloatField(null=True)
    difference_in_gcv_ytd = FloatField(null=True)

    meta = {"db_alias": "gmrDB-alias", "collection": "BunkerQualitySummary"}

    def payload(self):
        return {
            "date": self.date,
            "cum_total_qty": self.cum_total_qty,
            "cum_weighted_domestic_gcv": self.cum_weighted_domestic_gcv,
            "domestic_qty": self.domestic_qty,
            "imported_qty":self.imported_qty, 
            "total_qty": self.total_qty,
            "weighted_domestic_gcv": self.weighted_domestic_gcv,
            "weighted_gcv": self.weighted_gcv,
            "wt_gcv": self.wt_gcv, 
            "cr_domestic_gcv_mtd": self.cr_domestic_gcv_mtd,
            "cr_weighted_gcv_ytd": self.cr_weighted_gcv_ytd,
            "cr_domestic_qty_mtd": self.cr_domestic_qty_mtd,
            "cr_imported_qty_mtd": self.cr_imported_qty_mtd,
            "cr_imported_gcv_mtd": self.cr_imported_gcv_mtd,
            "cr_weighted_gcv_mtd": self.cr_weighted_gcv_mtd,
            "difference_in_gcv_mtd": self.difference_in_gcv_mtd,
            "difference_in_gcv_ytd": self.difference_in_gcv_ytd,
        }

    def simplepayload(self):
        if self.date.month < 4:  # Months: Jan(1), Feb(2), Mar(3)
            financial_year = f"FY {self.date.year - 1}-{str(self.date.year+1)[2:]}"
        else:  # Months: Apr(4), May(5), ..., Dec(12)
            financial_year = f"FY {self.date.year}-{str(self.date.year+1)[2:]}"
        return {
            # "date": self.date,
            "year": financial_year,
            "month": self.date.strftime("%Y-%m-%d"),
            # "cum_total_qty": self.cum_total_qty,
            # "cum_weighted_domestic_gcv": self.cum_weighted_domestic_gcv,
            "cb_domestic_qty_mtd": round(self.domestic_qty, 2),
            "cb_imported_qty_mtd": round(self.imported_qty, 2), 
            # "total_qty": self.total_qty,
            # "weighted_domestic_gcv": self.weighted_domestic_gcv,
            "cb_weighted_gcv_ytd": round(self.weighted_gcv, 2),
            "cb_weighted_gcv_mtd": round(self.wt_gcv, 2), 
            "cr_domestic_gcv_mtd": self.cr_domestic_gcv_mtd if self.cr_domestic_gcv_mtd else 0,
            "cr_weighted_gcv_ytd": self.cr_weighted_gcv_ytd if self.cr_weighted_gcv_ytd else 0,
            "cr_domestic_qty_mtd": self.cr_domestic_qty_mtd if self.cr_domestic_qty_mtd else 0,
            "cr_imported_qty_mtd": self.cr_imported_qty_mtd if self.cr_imported_qty_mtd else 0,
            "cr_imported_gcv_mtd": self.cr_imported_gcv_mtd if self.cr_domestic_gcv_mtd else 0,
            "cr_weighted_gcv_mtd": self.cr_weighted_gcv_mtd if self.cr_weighted_gcv_mtd else 0,
            "difference_in_gcv_mtd": self.difference_in_gcv_mtd if self.difference_in_gcv_mtd else 0,
            "difference_in_gcv_ytd": self.difference_in_gcv_ytd if self.difference_in_gcv_ytd else 0,
        }
    
class BunkerQualityAnalysis(Document):
    slno = IntField()
    ulr = StringField(null=True)
    certificate_no = StringField(null=True)
    test_report_date = DateTimeField()
    unit_no = IntField()
    sample_date = DateTimeField()
    analysis_date = DateTimeField()
    bunkered_qty = FloatField(null=True)
    sample_name = StringField(null=True)
    lab_temp = FloatField(null=True)
    lab_rh = FloatField(null=True)
    adb_im = FloatField(null=True)
    adb_ash = FloatField(null=True)
    adb_vm = FloatField(null=True)
    adb_gcv = IntField(null=True)
    arb_tm = FloatField(null=True)
    arb_vm = FloatField(null=True)
    arb_ash = FloatField(null=True)
    arb_fc = FloatField(null=True)
    arb_gcv = IntField(null=True)
    created_at = DateTimeField(default=datetime.datetime.utcnow)
    bunker_wt_gcv = FloatField(null=True)
    cumulative_wt = FloatField(null=True)
    cumulative_wt_gcv = FloatField(null=True)
    wt_gcv = FloatField(null=True)

    meta = {"db_alias": "gmrDB-alias", "collection": "BunkerQualityAnalysis"}

    def payload(self):
        return {
            "slno": self.slno,
            "ulr": self.ulr,
            "certificate_no": self.certificate_no,
            "test_report_date": self.test_report_date,
            "unit_no": self.unit_no,
            "sample_date": self.sample_date,
            "analysis_date": self.analysis_date,
            "bunkered_qty": self.bunkered_qty,
            "sample_name": self.sample_name,
            "lab_temp": self.lab_temp,
            "lab_rh": self.lab_rh,
            "adb_im": self.adb_im,
            "adb_ash": self.adb_ash,
            "adb_vm": self.adb_vm,
            "adb_gcv": self.adb_gcv,
            "arb_tm": self.arb_tm,
            "arb_vm": self.arb_vm,
            "arb_ash": self.arb_ash,
            "arb_fc": self.arb_fc,
            "arb_gcv": self.arb_gcv,
            "created_at": self.created_at,
            "bunker_wt_gcv": self.bunker_wt_gcv,
            "cumulative_wt": self.cumulative_wt,
            "cumulative_wt_gcv": self.cumulative_wt,
            "wt_gcv": self.wt_gcv,
        }

class RecieptCoalQualityAnalysis(Document):
    plant_certificate_id = StringField(null=True)
    plant_sample_id = StringField(null=True)
    sample_no = StringField(null=True)
    sample_id = StringField(null=True)
    plant_sample_date = StringField(null=True)
    plant_preperation_date = StringField(null=True)
    plant_analysis_date = DateTimeField()
    sample_qty = FloatField(null=True)
    mine = StringField(null=True)
    mine_grade = StringField(null=True)
    mode = StringField(null=True)
    type_consumer =  StringField(null=True)
    plant_lab_temp = FloatField(null=True)
    plant_lab_rh = FloatField(null=True)
    plant_arb_tm = FloatField(null=True)
    plant_arb_vm = FloatField(null=True)
    plant_arb_ash = FloatField(null=True)
    plant_arb_fc = FloatField(null=True)
    plant_arb_gcv = FloatField(null=True)
    plant_adb_im = FloatField(null=True)
    plant_adb_vm = FloatField(null=True)
    plant_adb_ash = FloatField(null=True)
    plant_adb_fc = FloatField(null=True)
    plant_adb_gcv = FloatField(null=True)
    plant_ulr_id = StringField(null=True)
    plant_gcv_grade = StringField(null=True)
    thirdparty_report_date = StringField(null=True)
    thirdparty_reference_no = StringField(null=True)
    thirdparty_sample_date = StringField(null=True)
    thirdparty_arb_tm = FloatField(null=True)
    thirdparty_arb_vm = FloatField(null=True)
    thirdparty_arb_ash = FloatField(null=True)
    thirdparty_arb_fc = FloatField(null=True)
    thirdparty_arb_gcv = FloatField(null=True)
    thirdparty_adb_im = FloatField(null=True)
    thirdparty_adb_vm = FloatField(null=True)
    thirdparty_adb_ash = FloatField(null=True)
    thirdparty_adb_fc = FloatField(null=True)
    thirdparty_adb_gcv =  FloatField(null=True)
    thirdparty_gcv_grade = StringField(null=True)
    thirdparty_created_date = DateTimeField(null=True)

    meta = {"db_alias": "gmrDB-alias", "collection": "RecieptCoalQualityAnalysis"}

    def payload(self):
        return {
            "plant_certificate_id": self.plant_certificate_id,
            "plant_certificate_id": self.plant_sample_id,
            "sample_no": self.sample_no,
            "do_no": self.sample_id,
            "GWEL_sample_date": self.plant_sample_date,
            "GWEL_preparation_date": self.plant_preperation_date,
            "GWEL_analysis_date": self.plant_analysis_date,
            "sample_qty": self.sample_qty,
            "mine": self.mine,
            "mine_grade": self.mine_grade,
            "mode": self.mode,
            "type_consumer": self.type_consumer,
            "GWEL_LAB_TEMP": self.plant_lab_temp,
            "GWEL_LAB_RH": self.plant_lab_rh,
            "GWEL_ARB_TM": self.plant_arb_tm,
            "GWEL_ARB_VM": self.plant_arb_vm,
            "GWEL_ARB_ASH": self.plant_arb_ash,
            "GWEL_ARB_FC": self.plant_arb_fc,
            "GWEL_ARB_GCV": self.plant_arb_gcv,
            "GWEL_ADB_IM": self.plant_adb_im,
            "GWEL_ADB_VM": self.plant_adb_vm,
            "GWEL_ADB_ASH": self.plant_adb_ash,
            "GWEL_ADB_FC": self.plant_adb_fc,
            "GWEL_ADB_GCV": self.plant_adb_gcv,
            "GWEL_ULR_ID": self.plant_ulr_id,
            "GWEL_GCV_GRADE": self.plant_gcv_grade,
            "THIRDPARTY_REPORT_DATE": self.thirdparty_report_date,
            "THIRDPARTY_REFERENCE_NO": self.thirdparty_reference_no,
            "THIRDPARTY_SAMPLE_DATE": self.thirdparty_sample_date,
            "THIRDPARTY_ARB_TM": self.thirdparty_arb_tm,
            "THIRDPARTY_ARB_VM": self.thirdparty_arb_vm,
            "THIRDPARTY_ARB_ASH": self.thirdparty_arb_ash,
            "THIRDPARTY_ARB_FC": self.thirdparty_arb_fc,
            "THIRDPARTY_ARB_GCV": self.thirdparty_arb_gcv,
            "THIRDPARTY_ADB_IM": self.thirdparty_adb_im,
            "THIRDPARTY_ADB_VM": self.thirdparty_adb_vm,
            "THIRDPARTY_ADB_ASH": self.thirdparty_adb_ash,
            "THIRDPARTY_ADB_FC": self.thirdparty_adb_fc,
            "THIRDPARTY_ADB_GCV": self.thirdparty_adb_gcv,
            "THIRDPARTY_GCV_GRADE": self.thirdparty_gcv_grade,
            "THIRDPARTY_CREATED_DATE": self.thirdparty_created_date,
        }
    
    
class SapRecordsFinal(Document):
    do_no = StringField(default=None)
    do_date = DateField(default=None)
    start_date = DateField(default=None)
    end_date = DateField(default=None)
    slno = StringField(default=None)
    consumer_type = StringField(default=None)
    mode_of_transport = StringField(default=None)
    grade = StringField(default=None)
    size = StringField(default=None)
    mine = StringField(default=None)
    line_item = StringField(default=None)
    material_description = StringField(default=None)
    do_qty = FloatField(default=None)
    po_amount = FloatField(default=None)
    basic_price_rate = FloatField(default=None)
    basic_price_amount = FloatField(default=None)
    sizing_charges_rate = FloatField(default=None)
    sizing_charges_amount = FloatField(default=None)
    stc_charges_rate = FloatField(default=None)
    stc_charges_amount = FloatField(default=None)
    evac_facility_charge_rate = FloatField(default=None)
    evac_facility_charge_amount = FloatField(default=None)
    royalty_charges_rate = FloatField(default=None)
    royalty_charges_amount = FloatField(default=None)
    nmet_rate = FloatField(default=None)
    nmet_amount = FloatField(default=None)
    dmf_rate = FloatField(default=None)
    dmf_amount = FloatField(default=None)
    cgst_rate = FloatField(default=None)
    cgst_amount = FloatField(default=None)
    sgst_rate = FloatField(default=None)
    sgst_amount = FloatField(default=None)
    gst_rate = FloatField(default=None)
    gst_amount = FloatField(default=None)
    so_value_rate = FloatField(default=None)
    so_value_amount = FloatField(default=None)
    emd_rate = FloatField(default=None)
    emd_amount = FloatField(default=None)
    so_value_excluding_emd_rate = FloatField(default=None)
    so_value_excluding_emd_amount = FloatField(default=None)

    meta = {"db_alias": "gmrDB-alias", "collection": "SapRecordsFinal"}

    def payload(self):
        return {
            "do_no": self.do_no,
            "do_date": self.do_date,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "slno": self.slno,
            "consumer_type": self.consumer_type,
            "Mode_of_Transport": self.mode_of_transport,
            "grade": self.grade,
            "size": self.size,
            "mine": self.mine,
            "line_item": self.line_item,
            "material_description": self.material_description,
            "do_qty": self.do_qty,
            "po_amount": self.po_amount,
            "basic_price_rate": self.basic_price_rate,
            "basic_price_amount": self.basic_price_amount,
            "sizing_charges_rate": self.sizing_charges_rate,
            "sizing_charges_amount": self.sizing_charges_amount,
            "stc_charges_rate": self.stc_charges_rate,
            "stc_charges_amount": self.stc_charges_amount,
            "evac_facility_charge_rate": self.evac_facility_charge_rate,
            "evac_facility_charge_amount": self.evac_facility_charge_amount,
            "royalty_charges_rate": self.royalty_charges_rate,
            "royalty_charges_amount": self.royalty_charges_amount,
            "nmet_rate": self.nmet_rate,
            "nmet_amount": self.nmet_amount,
            "dmf_rate": self.dmf_rate,
            "dmf_amount": self.dmf_amount,
            "cgst_rate": self.cgst_rate,
            "cgst_amount": self.cgst_amount,
            "sgst_rate": self.sgst_rate,
            "sgst_amount": self.sgst_amount,
            "gst_rate": self.gst_rate,
            "gst_amount": self.gst_amount,
            "so_value_rate": self.so_value_rate,
            "so_value_amount": self.so_value_amount,
            "emd_rate": self.emd_rate,
            "emd_amount": self.emd_amount,
            "so_value_excluding_emd_rate": self.so_value_excluding_emd_rate,
            "so_value_excluding_emd_amount": self.so_value_excluding_emd_amount,
        }


class roadjourneyconsumertype(Document):
    # consumer_type = StringField(null=True)
    consumer_type = ListField()
    created_at = DateTimeField(default=datetime.datetime.utcnow())

    meta = {"db_alias": "gmrDB-alias", "collection": "roadjourneyconsumertype"}

    def payload(self):
        return {
            "id": str(self.id),
            "consumer_type": self.consumer_type,
            "created_at": self.created_at,
        }
    

class grnData(Document):
    invoice_data = StringField(null=True)
    invoice_no = StringField(null=True)
    sale_date = StringField(null=True)
    grade = StringField(null=True)
    dispatch_date = StringField(null=True)
    mine = StringField(null=True)
    do_qty = StringField(null=True)

    meta = {"db_alias": "gmrDB-alias", "collection": "grnData"}

    def payload(self):
        return {
            "invoice_data": self.invoice_data,
            "invoice_no": self.invoice_no,
            "sale_date": self.sale_date,
            "grade": self.grade,
            "dispatch_date": self.dispatch_date,
            "mine": self.mine,
            "do_qty": self.do_qty,
        }

class CategoryData(EmbeddedDocument):
    remark = StringField()
    uom = FloatField()
    mou_coal = FloatField()
    linkage = FloatField()
    aiwib_washery = FloatField()
    open_mkt = FloatField()
    spot_eauction = FloatField()
    spl_for_eauction = FloatField()
    imported = FloatField()
    total = FloatField()
    shakti_b = FloatField()
    shakti_b3 = FloatField()
    particular = StringField()

class Form15Data(Document):
    osd_month = EmbeddedDocumentField(CategoryData)                                # Opening stock of coal as on 1st Day of the Month
    vos_month = EmbeddedDocumentField(CategoryData)                                # Value of opening stock as on 1st Day of the Month
    qty_supplied = EmbeddedDocumentField(CategoryData)                             # Quantity of Coal/Lignite supplied by Coal/Lignite Company
    adj_qty = EmbeddedDocumentField(CategoryData)                                  # Adjustment (+/-) in quantity supplied made by Coal/Lignite Company
    coal_supplied = EmbeddedDocumentField(CategoryData)                            # Coal Supplied by Coal Lignite company (3+4)
    norm_transit_loss = EmbeddedDocumentField(CategoryData)                        # Normative Transit & Handling Losses
    net_supplied = EmbeddedDocumentField(CategoryData)                             # Net Coal/Lignite Supplied (5-6)
    amt_charged = EmbeddedDocumentField(CategoryData)                              # Amount charged by the Coal/Lignite Company
    adj_amt = EmbeddedDocumentField(CategoryData)                                  # Adjustments (+/-) in amount charged by Coal/Lignite Company
    unloading_charges = EmbeddedDocumentField(CategoryData)                        # Unloading, Sampling Charges, AMM etc.
    total_amt_charged = EmbeddedDocumentField(CategoryData)                        # Total amount Charged (8+9+10)
    trans_charges = EmbeddedDocumentField(CategoryData)                            # Transportation charges by Rail/Ship/Road
    adj_trans_charges = EmbeddedDocumentField(CategoryData)                        # Adjustment (+/-) in amount charged by railway transport
    demurrage = EmbeddedDocumentField(CategoryData)                                # Demurrage Charge, if any
    diesel_cost = EmbeddedDocumentField(CategoryData)                              # Cost of diesel in transporting coal
    total_trans_charges = EmbeddedDocumentField(CategoryData)                      # Total transportation charges (12+13+14+15)
    total_amt_incl_trans = EmbeddedDocumentField(CategoryData)                     # Total amount charged for Coal/lignite including transportation (11+16)
    qty_at_station = EmbeddedDocumentField(CategoryData)                           # Quantity of coal at station for the month (1+7)
    total_amt_for_coal = EmbeddedDocumentField(CategoryData)                       # Total amount charged for coal (2+17)
    landed_cost = EmbeddedDocumentField(CategoryData)                              # Landed cost of coal (19/18)
    qty_consumed = EmbeddedDocumentField(CategoryData)                             # Coal Quantity consumed
    value_consumed = EmbeddedDocumentField(CategoryData)                           # Value of coal Consumed (20*21)
    wtd_avg_gcv_prev = EmbeddedDocumentField(CategoryData)                         # Weighted average GCV with previous month's coal
    wtd_avg_gcv_recv = EmbeddedDocumentField(CategoryData)                         # Wtd. Average as received GCV
    wtd_avg_gcv_less_85 = EmbeddedDocumentField(CategoryData)                      # Weighted Average GCV of caol as received
    closing_coal_stock = EmbeddedDocumentField(CategoryData)                       # Closing stock of coal as on last Day of the Month 
    closing_coal_stock_value = EmbeddedDocumentField(CategoryData)                 # Value of Closing stock as on  last Day of the Month 
    
    month = DateField(null=True)
    created_at = DateTimeField(default=datetime.datetime.now(datetime.timezone.utc))
    
    meta = {"db_alias": "gmrDB-alias", "collection": "form_15"}

class UserDataPermission(EmbeddedDocument):
    user = ListField(DictField())  # Correctly define the ListField

    def payload(self):
        return {
            "user": self.user,
        }

class MultiApproval(Document):
    approval_name = StringField(default=None)
    levels = EmbeddedDocumentListField(UserDataPermission)  
    bypass_level = BooleanField(default=False)
    disabled = BooleanField(default=False)

    meta = {"db_alias": "gmrDB-alias", "collection": "MultiApproval"}

    def payload(self):
        listData = []
        for single_level in self.levels:
            listData.append(single_level.payload())


        return {
            "approval_name": self.approval_name,
            "levels": listData,
            "bypass_level": self.bypass_level,
            "disabled": self.disabled,
        }

class ApprovalTableList(Document):
    approval_list = ListField(StringField())

    meta = {"db_alias": "gmrDB-alias", "collection": "ApprovalTableList"}


class Grn(Document):
    do_no = StringField(null=True)
    invoice_date= StringField(null=True)
    invoice_no=StringField(null=True)
    sale_date = StringField(null=True)
    grade = StringField(null=True)
    dispatch_date = StringField(null=True)
    mine = StringField(null=True)    
    do_qty = StringField(null=True)
    # header_data = DictField()
    original_data = ListField(DictField())
    new_data = ListField(DictField())
    # approvals = ListField(DictField())
    approvals = DictField()
    changed_by = StringField(null=True)
    #particulars start
    basic_price = FloatField(null=True)
    sizing_charges = FloatField(null=True)
    stc_charges = FloatField(null=True)
    evac_facility_charge = FloatField(null=True)
    royalty_charges = FloatField(null=True)
    nmet_charges = FloatField(null=True)
    imf = FloatField(null=True)
    cgst = FloatField(null=True)
    sgst = FloatField(null=True)
    gst_comp_cess = FloatField(null=True)
    gross_bill_value = FloatField(null=True)
    net_value = FloatField(null=True)
    total_amount = FloatField(null=True)
    #particulars end
    created_at = DateTimeField(default=datetime.datetime.utcnow())

    meta = {"db_alias": "gmrDB-alias", "collection": "Grn"}

    def payload(self):
        return {
            "do_no": self.do_no,
            "invoice_date": self.invoice_date,
            "invoice_no": self.invoice_no,
            "sale_date": self.sale_date,
            "grade": self.grade,
            "dispatch_date": self.dispatch_date,
            "mine": self.mine,    
            "do_qty": self.do_qty,
            "original_data": self.original_data,
            "new_data": self.new_data,
            "approvals": self.approvals,
            "changed_by": self.changed_by,
            # "basic_price": self.basic_price,
            # "sizing_charges": self.sizing_charges,
            # "stc_charges": self.stc_charges,
            # "evac_facility_charge": self.evac_facility_charge,
            # "royalty_charges": self.royalty_charges,
            # "nmet_charges": self.nmet_charges,
            # "imf": self.imf,
            # "cgst": self.cgst,
            # "sgst": self.sgst,
            # "gst_comp_cess": self.gst_comp_cess,
            # "gross_bill_value": self.gross_bill_value,
            # "net_value": self.net_value,
            # "total_amount": self.total_amount,
            "created_at": self.created_at,
        }
    
    def frpayload(self):
        return {
            "do_no": self.do_no,
            "invoice_date": self.invoice_date,
            "invoice_no": self.invoice_no,
            "sale_date": self.sale_date,
            "grade": self.grade,
            "dispatch_date": self.dispatch_date,
            "mine": self.mine,    
            "do_qty": self.do_qty,
            "new_data": self.new_data,
            "created_at": self.created_at,
        }
    

class minesamplequalityanalysis(Document):
    mine_thirdparty_sample_reference_no = StringField(null=True)
    source = StringField(null=True)
    sample_id = StringField(null=True) # unique
    sample_collection_date = DateTimeField(null=True)
    sample_preparation_date = DateTimeField(null=True)
    sample_received_date = DateTimeField(null=True)
    sample_analysis_date = DateTimeField()
    rr_qty = FloatField(null=True)
    rr_no = IntField(null=True)
    rr_date = StringField(null=True)
    declared_grade = StringField(null=True)
    mine_thirdparty_grade = StringField(null=True)
    plant_grade = StringField(null=True)
    plant_certificate_id = StringField(null=True)
    plant_sample_date = DateTimeField(null=True)
    plant_preparation_date = DateTimeField(null=True)
    plant_analysis_date = DateTimeField(null=True)
    plant_lab_temp = FloatField(null=True)
    plant_arb_tm = FloatField(null=True)
    plant_arb_vm = FloatField(null=True)
    plant_arb_ash = FloatField(null=True)
    plant_arb_fc = FloatField(null=True)
    plant_arb_gcv = FloatField(null=True)
    plant_adb_im = FloatField(null=True)
    plant_adb_vm = FloatField(null=True)
    plant_adb_ash = FloatField(null=True)
    plant_adb_fc = FloatField(null=True)
    plant_adb_gcv = FloatField(null=True)
    plant_ulr_id = StringField(null=True) #29
    plant_gcv_grade = StringField(null=True)
    mine_thirdparty_tm_arb = FloatField(null=True)
    mine_thirdparty_humidity = FloatField(null=True)
    mine_thirdparty_temperature = FloatField(null=True)
    mine_thirdparty_adb_moisture = FloatField(null=True)
    mine_thirdparty_adb_ash = FloatField(null=True)
    mine_thirdparty_adb_gcv = FloatField(null=True)
    mine_thirdparty_arb_moisture = FloatField(null=True)
    mine_thirdparty_arb_ash = FloatField(null=True)
    mine_thirdparty_arb_gcv = FloatField(null=True)
    # mine_thirdparty_gcv_grade = StringField(null=True)
    analysed_grade = StringField(null=True)
    created_at = DateTimeField(default=datetime.datetime.utcnow())

    meta = {"db_alias": "gmrDB-alias", "collection": "minesamplequalityanalysis"}

    def payload(self):
        return {
            "mine_thirdparty_sample_reference_no": self.mine_thirdparty_sample_reference_no,
            "source": self.source,
            "sample_id": self.sample_id,
            "sample_collection_date": self.sample_collection_date,
            "sample_preparation_date": self.sample_preparation_date,
            "sample_received_date": self.sample_received_date,
            "sample_analysis_date": self.sample_analysis_date,
            "rr_qty": self.rr_qty,
            "rr_no": self.rr_no,
            "rr_date": self.rr_date,
            "declared_grade": self.declared_grade,
            "mine_thirdparty_grade": self.mine_thirdparty_grade,
            "plant_grade": self.plant_grade,
            "plant_certificate_id": self.plant_certificate_id,
            "plant_sample_date": self.plant_sample_date,
            "plant_preparation_date": self.plant_preparation_date,
            "plant_analysis_date": self.plant_analysis_date,
            "plant_lab_temp": self.plant_lab_temp,
            "plant_arb_tm": self.plant_arb_tm,
            "plant_arb_vm": self.plant_arb_vm,
            "plant_arb_ash": self.plant_arb_ash,
            "plant_arb_fc": self.plant_arb_fc,
            "plant_arb_gcv": self.plant_arb_gcv,
            "plant_adb_im": self.plant_adb_im,
            "plant_adb_vm": self.plant_adb_vm,
            "plant_adb_ash": self.plant_adb_ash,
            "plant_adb_fc": self.plant_adb_fc,
            "plant_adb_gcv": self.plant_adb_gcv,
            "plant_ulr_id": self.plant_ulr_id,
            "plant_gcv_grade": self.plant_gcv_grade,
            "mine_thirdparty_tm_arb": self.mine_thirdparty_tm_arb,
            "mine_thirdparty_humidity": self.mine_thirdparty_humidity,
            "mine_thirdparty_temperature": self.mine_thirdparty_temperature,
            "mine_thirdparty_adb_moisture": self.mine_thirdparty_adb_moisture,
            "mine_thirdparty_adb_ash": self.mine_thirdparty_adb_ash,
            "mine_thirdparty_adb_gcv": self.mine_thirdparty_adb_gcv,
            "mine_thirdparty_arb_moisture": self.mine_thirdparty_arb_moisture,
            "mine_thirdparty_arb_ash": self.mine_thirdparty_arb_ash,
            "mine_thirdparty_arb_gcv": self.mine_thirdparty_arb_gcv,
            # "mine_thirdparty_gcv_grade": self.mine_thirdparty_gcv_grade,
            "analysed_grade": self.analysed_grade,
            "created_at": self.created_at,
        }
    
class tableSubject(Document):
    table_name = StringField(null=True)
    table_subject = StringField(null=True)

    meta = {"db_alias": "gmrDB-alias", "collection": "tableSubject"}

    def payload(self):
        return {
            "table_name": self.table_name,
            "table_subject": self.table_subject,
        }

class cmplData(Document):
    tno = IntField(null=True)
    companycode = StringField(null=True)
    financialyearcode = StringField(null=True)
    locationcode = StringField(null=True)
    lrno = StringField(null=True)
    lrdate = DateField()
    partycode = StringField(null=True)
    source_location_tno = StringField(null=True)
    consignor_code = StringField(null=True)
    destination_location_tno = StringField(null=True)
    consigneecode = StringField(null=True)
    vehicle_no = StringField(null=True)
    freightamount = FloatField(null=True)
    item_code = StringField(null=True)
    nos = FloatField(null=True)
    quantity1 = IntField(null=True) 
    quantity2 = IntField(null=True)
    invoice_no = StringField(null=True)
    invoice_date = DateField()
    consignor_name = StringField(null=True)
    consignor_address = StringField(null=True)
    consignor_citycode = StringField(null=True)
    consignor_statecode =  StringField(null=True)
    consignor_phoneno = StringField(null=True)
    consignee_name = StringField(null=True)
    consignee_address = StringField(null=True)
    consignee_citycode = StringField(null=True)
    consignee_statecode =  StringField(null=True)
    consignee_phoneno = StringField(null=True)
    invoice_amount = FloatField(null=True)
    challon_no = StringField(null=True)
    challan_date = DateField()
    driver_name = StringField(null=True)
    driver_licenseno = StringField(null=True)
    eway_billno = StringField(null=True)
    eway_billdate = DateField()
    balance_qty = IntField(null=True)
    do_qty = IntField(null=True)
    delivery_order_tno = IntField(null=True)

    meta = {"db_alias": "gmrDB-alias", "collection": "cmplData"}

    def payload(self):
        return {
            "tno": self.tno,
            "companycode": self.companycode,
            "financialyearcode": self.financialyearcode,
            "locationcode": self.locationcode,
            "lrno": self.lrno,
            "lrdate": self.lrdate,
            "partycode": self.partycode,
            "source_location_tno": self.source_location_tno,
            "consignor_code": self.consignor_code,
            "destination_location_tno": self.destination_location_tno,
            "consigneecode": self.consigneecode,
            "vehicle_no": self.vehicle_no,
            "freightamount": self.freightamount,
            "item_code": self.item_code,
            "nos": self.nos,
            "quantity1": self.quantity1, 
            "quantity2": self.quantity2,
            "invoice_no": self.invoice_no,
            "invoice_date": self.invoice_date,
            "consignor_name": self.consignor_name,
            "consignor_address": self.consignor_address,
            "consignor_citycode": self.consignor_citycode,
            "consignor_statecode": self.consignor_statecode,
            "consignor_phoneno": self.consignor_phoneno,
            "consignee_name": self.consignee_name,
            "consignee_address": self.consignee_address,
            "consignee_citycode": self.consignee_citycode,
            "consignee_statecode": self.consignee_statecode,
            "consignee_phoneno": self.consignee_phoneno,
            "invoice_amount": self.invoice_amount,
            "challon_no": self.challon_no,
            "challan_date": self.challan_date,
            "driver_name": self.driver_name,
            "driver_licenseno": self.driver_licenseno,
            "eway_billno": self.eway_billno,
            "eway_billdate": self.eway_billdate,
            "balance_qty": self.balance_qty,
            "do_qty": self.do_qty,
            "delivery_order_tno": self.delivery_order_tno
        }