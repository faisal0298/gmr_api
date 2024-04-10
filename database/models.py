from mongoengine import *
from helpers.logger import console_logger
import datetime
import uuid
from mongoengine import signals


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
    created_at = DateTimeField(default=datetime.datetime.utcnow())

    meta = {"db_alias" : "gmrDB-alias" , "collection" : "coaltesting"}
        
    def payload(self):
        local_timestamp = self.created_at.replace(
            tzinfo=datetime.timezone.utc
        ).astimezone(tz=None)
        
        payload_dict = {
            "Sr.No": self.ID,
            "Mine": self.location,
            "Rake_No": self.rake_no,
            "RR_No": self.rrNo,
            "RR_Qty": self.rR_Qty,
            "Supplier": self.supplier,
            "Date": local_timestamp.strftime("%Y-%m-%d"),
            "Time": local_timestamp.strftime("%H:%M:%S"),}

        for param in self.parameters:
            param_name = f"{param['parameter_Name']} {param['unit_Val'].replace(' ','')}"
            payload_dict[param_name] = param["val1"]
        
        return payload_dict



class CoalTestingTrain(Document):
    location = StringField()
    rrNo = StringField()
    rR_Qty = StringField()
    rake_no = StringField()
    supplier = StringField()
    parameters = ListField(DictField())
    receive_date = DateTimeField()
    ID = IntField(min_value=1)
    created_at = DateTimeField(default=datetime.datetime.utcnow())

    meta = {"db_alias" : "gmrDB-alias" , "collection" : "coaltestingtrain"}
        
    def payload(self):
        local_timestamp = self.created_at.replace(
            tzinfo=datetime.timezone.utc
        ).astimezone(tz=None)
        
        payload_dict = {
            "Sr.No": self.ID,
            "Mine": self.location,
            "Rake_No": self.rake_no,
            "RR_No": self.rrNo,
            "RR_Qty": self.rR_Qty,
            "Supplier": self.supplier,
            "Date": local_timestamp.strftime("%Y-%m-%d"),
            "Time": local_timestamp.strftime("%H:%M:%S"),}

        for param in self.parameters:
            param_name = f"{param['parameter_Name']} {param['unit_Val'].replace(' ','')}"
            payload_dict[param_name] = param["val1"]
        
        return payload_dict



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
    created_at = DateTimeField(default=datetime.datetime.utcnow)

    gate_verified_time = DateTimeField(default=None)
    vehicle_in_time = DateTimeField(null=True)
    actual_gross_wt_time = DateTimeField(default=None)
    actual_tare_wt_time = DateTimeField(default=None)
    ID = IntField(min_value=1)

    meta = {"db_alias" : "gmrDB-alias" , "collection" : "gmrdata"}


    def payload(self):

        return {"Sr.No.":self.ID,
                "PO_No":self.po_no,
                "PO_Date":self.po_date,
                "PO_Qty":self.po_qty, 
                "Delivery_Challan_No":self.delivery_challan_number,
                "Arv_Cum_DO_No":self.arv_cum_do_number,
                "Mines_Name":self.mine,
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
                "Gross_challan_Wt(MT)" : self.gross_qty,
                "Tare_challan_Wt(MT)" : self.tare_qty,
                "Net_challan_Wt(MT)" : self.net_qty,
                "Gross_actual_Wt(MT)" : self.actual_gross_qty,
                "Tare_actual_Wt(MT)" : self.actual_tare_qty,
                "Net_actual_Wt(MT)" : self.actual_net_qty,
                "Wastage" : self.wastage,
                "Driver_Name" : self.driver_name,
                "Gate_Pass_No" : self .gate_pass_no,
                "Transporter_LR_No": self.transporter_lr_no,
                "Transporter_LR_Date": self.transporter_lr_date,
                "Eway_bill_No": self.e_way_bill_no,
                "Gate_verified_time" : self.gate_verified_time.strftime("%Y-%m-%d:%H:%M:%S") if self.gate_verified_time else None,
                "Vehicle_in_time" : self.vehicle_in_time.strftime("%Y-%m-%d:%H:%M:%S") if self.vehicle_in_time else None,
                "Vehicle_out_time" : self.vehicle_out_time.strftime("%Y-%m-%d:%H:%M:%S") if self.vehicle_out_time else None,
                "Actual_gross_wt_time" : self.actual_gross_wt_time.strftime("%Y-%m-%d:%H:%M:%S") if self.actual_gross_wt_time else None,
                "Actual_tare_wt_time" : self.actual_tare_wt_time.strftime("%Y-%m-%d:%H:%M:%S") if self.actual_tare_wt_time else None, 
                "Challan_image" : self.challan_file if self.challan_file else None,
                "Fitness_image": self.fitness_file if self.fitness_file else None,
                "Face_image": self.fr_file if self.fr_file else None,
                }