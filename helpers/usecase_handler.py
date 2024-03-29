import copy
import os,sys
from shapely.geometry import Point, Polygon

from helpers.general_helpers import convertToSeconds, convertToTime
from database.models import *
from helpers.logger import console_logger



service_id = os.environ.get("SERVICE_ID", "gmr_api")
parent_ids = os.environ.get("PARENTS_IDS", ["gmr_ai"])
parent_ids = (
    parent_ids.strip("][").replace("'", "").split(", ")
    if type(parent_ids) == str
    else parent_ids
)
usecase_var = "gmr_api"


default_data = {
    "Service_id": service_id,
    "Name": service_id,
    "Type": "Alert",
    "Default_params": {
        usecase_var: {
            "roi1": {
                "loicord": {
                    "x1": 1,
                    "y1": 239,
                    "x2": 640,
                    "y2": 240,
                    "loiName": "lineA",
                    "InDirection": "A TO B",
                },
                "roicords": {
                    "x1": 0,
                    "y1": 0,
                    "x2": 640,
                    "y2": 0,
                    "x3": 640,
                    "y3": 480,
                    "x4": 0,
                    "y4": 480,
                },
                "roiName": "roi1",
                "SAP IP": "",
                "SAP Duration": "1:0:0:0",
                "SAP Scheduler": {},
                "Avery Wagon IP": "",
                "Avery Wagon Duration": "1:0:0:0",
                "Avery Wagon Scheduler": {},
                "Coal Testing IP": "",
                "Coal Testing Duration": "1:0:0:0",
                "Coal Testing Scheduler": {
                    "filter": "daily",
                    "time": "00:01"},
                "Coal Consumption IP": "",
                "Coal Consumption Duration": "1:0:0:0",
                "Coal Consumption Scheduler": {
                    "filter": "daily",
                    "time": "00:01"},
            }
        }
    },
    "Parent_service_meta": [
        {
            "Parent_id": parent_ids[0],
            "Labels": {"2": "car", "3": "motorbike", "5": "bus", "7": "truck"},
        }
    ],
}


labels = {"2": "car", "3": "motorbike", "5": "bus", "7": "truck"}
camera_metadata = {}
object_counter = {}
handler = None



def pre_processing():
    DeveloperParameters.objects(Service_id=service_id).update_one(
        set__Name=default_data["Name"],
        set__Type=default_data["Type"],
        set__Default_params=default_data["Default_params"],
        set__Parent_service_meta=default_data["Parent_service_meta"],
        upsert=True,
    )

    return DeveloperParameters.objects.first()


def load_params(data):
    try:
        console_logger.debug(data)
        global camera_metadata, object_counter

        if "service_id" in data:
            del data["service_id"]

        camera_id = data.pop("camera_id")
        console_logger.debug(data)
        if camera_id in camera_metadata:
            camera_metadata[camera_id] = {}
            object_counter[camera_id] = {}

        usecase_parameters = UsecaseParameters.objects(Camera_id=camera_id).first()
        if usecase_parameters:
            dictionary = copy.deepcopy({camera_id: usecase_parameters.Parameters})
        elif "location" in data:
            dictionary = {camera_id: copy.deepcopy(default_data["Default_params"])}
        else:
            dictionary = copy.deepcopy({camera_id: data})

        UsecaseParameters.objects(Camera_id=camera_id).update_one(
            set__Parameters=data if usecase_var in data else dictionary[camera_id],
            set__Camera_name=data["camera_name"]
            if "camera_name" in data
            else usecase_parameters.Camera_name,
            set__Location=data["location"]
            if "location" in data
            else usecase_parameters.Location,
            upsert=True,
        )

        usecase_parameters = UsecaseParameters.objects(Camera_id=camera_id).first()
        dictionary = copy.deepcopy({camera_id: usecase_parameters.Parameters})
        # console_logger.debug(dictionary)
        for camera_id, params in dictionary.items():
            if camera_id not in object_counter:
                object_counter[camera_id] = {}

            if camera_id not in camera_metadata:
                camera_metadata[camera_id] = {}

            for roi in params[usecase_var].keys():
                if params[usecase_var][roi]["roiName"] not in object_counter[camera_id]:
                    object_counter[camera_id][params[usecase_var][roi]["roiName"]] = {}

                loi_cords = None
                loi_name = None
                in_direction = "A TO B"
                corners = []

                roi_metadata = {"roi_name": params[usecase_var][roi]["roiName"]}
                if "loicord" in params[usecase_var][roi]:
                    cords = params[usecase_var][roi].pop("loicord")
                    if cords.keys():
                        if "x1" not in cords:
                            cords = cords.pop("line1")
                        loi_cords = [cords["x1"], cords["y1"], cords["x2"], cords["y2"]]
                        loi_name = cords["loiName"]
                        in_direction = cords["InDirection"]

                for i in range(
                    int(len(params[usecase_var][roi]["roicords"].keys()) / 2)
                ):
                    corners.append(
                        (
                            params[usecase_var][roi]["roicords"]["x{}".format(i + 1)],
                            params[usecase_var][roi]["roicords"]["y{}".format(i + 1)],
                        )
                    )

                    roi_metadata["x{}".format(i + 1)] = params[usecase_var][roi][
                        "roicords"
                    ]["x{}".format(i + 1)]
                    roi_metadata["y{}".format(i + 1)] = params[usecase_var][roi][
                        "roicords"
                    ]["y{}".format(i + 1)]

                if camera_id not in camera_metadata or (
                    camera_id in camera_metadata
                    and not len(camera_metadata[camera_id].keys())):

                    camera_metadata[camera_id] = {
                        "camera_name": usecase_parameters.Camera_name,
                        "location": usecase_parameters.Location,
                        "roi_name": [params[usecase_var][roi]["roiName"]],
                        "roi": [roi_metadata],
                        "polygons": [Polygon(corners)]
                    }
                else:
                    camera_metadata[camera_id]["roi_name"].append(params[usecase_var][roi]["roiName"])
                    camera_metadata[camera_id]["roi"].append(roi_metadata)
                    camera_metadata[camera_id]["polygons"].append(Polygon(corners))
    except Exception as e:
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))

    # console_logger.debug(camera_metadata)
    # console_logger.debug(object_counter)
    # previous_data = copy.deepcopy(object_counter)
    


def save_image(camera_id, **data):
    global handler
    image_url = "{}_{}.jpg".format(
        service_id, datetime.datetime.utcnow().strftime("%m-%d-%Y-%H-%M-%S-%f")
    )

    data.update({"camera_id": camera_id, "image_name": image_url, "type": "Alert"})
    handler.send_data(destination=parent_ids[0], data_type="save_image", **data)
    return image_url


def send_alert(camera_id, roi, alert, detections, buffer_index):
    global handler
    cords = {"buffer_index": buffer_index, "ROIs": [{"bbox": detections}]}
    roi_data = camera_metadata[camera_id]["roi"][
        camera_metadata[camera_id]["roi_name"].index(roi)
    ]
    if "roi_name" in roi_data:
        roi_data["ROI_Name"] = roi_data.pop("roi_name")
    cords["ROIs"][0].update(roi_data)

    console_logger.debug(cords)

    image_name = save_image(camera_id=camera_id, **cords)
    
    dictionary = {
        "Camera_id": camera_id,
        "Camera_name": camera_metadata[camera_id]["camera_name"],
        "Location": camera_metadata[camera_id]["location"],
        "Service_id": service_id,
        "Alert": alert,
        "Timestamp": str(datetime.datetime.utcnow()),
        "Video_path": [],
        "Image_path": [image_name],
    }
    console_logger.debug(dictionary)
    handler.send_data(**dictionary)
    return image_name


def post_processing(data, roi, buffer_index):
    data.update(
        **{
            "image_name": send_alert(
                camera_id=data["camera_id"],
                alert=None,
                roi=roi,
                detections=data["metadata"],
                buffer_index=buffer_index,
            ),
            "timestamp": datetime.datetime.utcnow(),
        }
    )

    console_logger.debug(data)

    alert_data = {}
    alert="{} with registered number {} detected".format((data["metadata"]["vehicle_type"]).capitalize(),data["metadata"]["number_plate"])
    console_logger.debug(alert_data)
    console_logger.debug("Generating Alert")
    send_alert(
                camera_id=data["camera_id"],
                alert=alert,
                roi=roi,
                detections=data["metadata"],
                buffer_index=buffer_index
            )



