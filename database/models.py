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
    created_at = DateTimeField(default=datetime.datetime.utcnow())
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
            payload_dict[param_name] = param["val1"]

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
            "Date": datetime.datetime.fromisoformat(
                    self.receive_date.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d") if self.receive_date else None,
            "Time": datetime.datetime.fromisoformat(
                    self.receive_date.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%H:%M:%S") if self.receive_date else None,
            "Id": str(self.pk)}

        for param in self.parameters:
            param_name = f"{param['parameter_Name']}_{param['unit_Val'].replace(' ','')}"
            payload_dict[param_name] = param["val1"]

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
    out_time = DateTimeField(default=None, null=True)
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
    e_way_bill_no = StringField(null=True)
    gate_user = StringField(null=True)

    gate_approved = BooleanField(default=False)
    gate_fastag  = BooleanField(default=False)
    
    vehicle_chassis_number = StringField()
    certificate_expiry = StringField()
    actual_gross_qty = StringField(null=True)            # actual gross weight measured from weightbridge
    actual_tare_qty = StringField(null=True)             # actual tare weight measured from weightbridge
    actual_net_qty = StringField(null=True)             # actual net weight measured from weightbridge
    wastage = StringField(null=True)
    fitness_file = StringField()
    lr_file = StringField()
    po_no = StringField(null=True)
    po_date = StringField(null=True)
    po_qty = StringField(null=True)

    gross_weighbridge = StringField(null=True)
    tare_weighbridge = StringField(null=True)

    dc_request = BooleanField(default=False)

    created_at = DateTimeField(default=datetime.datetime.utcnow())

    # remark = StringField(null=True)
  
    vehicle_in_time = DateTimeField(null=True)
    lot = StringField()
    line_item = StringField(null=True)
    GWEL_Gross_Time = DateTimeField(null=True)
    GWEL_Tare_Time = DateTimeField(null=True)

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
                "Wastage" : self.wastage,
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
    slno = StringField()
    source = StringField()
    mine_name = StringField()
    sap_po = StringField()
    line_item = StringField()
    do_no = StringField()
    do_qty = StringField()
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


class RailData(Document):
    rr_no = StringField()
    rr_qty = StringField()
    po_no = StringField()
    po_date = StringField()
    line_item = StringField()
    source = StringField()
    placement_date = StringField()
    completion_date = StringField()
    drawn_date = StringField()
    total_ul_wt = StringField()
    boxes_supplied = StringField()
    total_secl_gross_wt = StringField()
    total_secl_tare_wt = StringField()
    total_secl_net_wt = StringField()
    total_secl_ol_wt = StringField()
    boxes_loaded = StringField()
    total_rly_gross_wt = StringField()
    total_rly_tare_wt = StringField()
    total_rly_net_wt = StringField()
    total_rly_ol_wt = StringField()
    total_secl_chargable_wt = StringField()
    total_rly_chargable_wt = StringField()
    freight = StringField()
    gst = StringField()
    pola = StringField()
    total_freight = StringField()
    source_type = StringField()
    secl_rly_data = EmbeddedDocumentListField(SeclRailData)
    created_at = DateTimeField(default=datetime.datetime.utcnow)

    meta = {"db_alias": "gmrDB-alias", "collection": "raildata"}

    def payload(self):
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
            "placement_date": self.placement_date.replace("T"," "),
            "completion_date": self.completion_date.replace("T"," "),
            "drawn_date": self.drawn_date.replace("T"," "),
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
            "secl_rly_data": seclrail,

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
            "placement_date": self.placement_date.replace("T"," "),
            "completion_date": self.completion_date.replace("T"," "),
            "drawn_date": self.drawn_date.replace("T"," "),
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
    created_date = DateTimeField()
    ID = IntField(min_value=1)
    created_at = DateTimeField(default=datetime.datetime.utcnow())

    meta = {"db_alias": "gmrDB-alias", "collection": "bunkerAnalysis"}

    def payload(self):
        return {
            "Sr.No": self.ID,
            # "id": str(self.id),
            "shift_name": self.shift_name,
            "unit": self.units,
            # "tagid": self.tagid,
            "bunkering": self.bunkering,
            "mgcv": self.mgcv,
            "hgcv": self.hgcv,
            "ratio": self.ratio,

            "Date": datetime.datetime.fromisoformat(
                    self.created_date.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_date else None,

            "created_at": datetime.datetime.fromisoformat(
                    self.created_at.strftime("%Y-%m-%d %H:%M:%S.%fZ")[:-1] + "+00:00"
                    ).astimezone(tz=to_zone).strftime("%Y-%m-%d %H:%M:%S") if self.created_at else None,
        }


class shiftScheduler(Document):
    shift_name = StringField(default=None)
    start_shift_time = StringField(default=None)
    end_shift_time = StringField(default=None)
    report_name = StringField(default=None)
    created_at = DateTimeField(default=datetime.datetime.utcnow)

    meta = {"db_alias": "gmrDB-alias", "collection": "shiftScheduler"}

    def payload(self):
        return {
            "id": str(self.id),
            "shift_name": self.shift_name,
            "start_shift_time": self.start_shift_time,
            "end_shift_time": self.end_shift_time,
            # "report_name": self.report_name,
            "created_at": self.created_at,
        }
    

class EmailDevelopmentCheck(Document):
    development = StringField(default=None)

    meta = {"db_alias": "gmrDB-alias", "collection": "EmailDevelopmentCheck"}

    def payload(self):
        return {
            "development": self.development,
        }