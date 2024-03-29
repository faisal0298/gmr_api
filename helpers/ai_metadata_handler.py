from shapely.geometry import Point
import copy

from helpers.read_timezone import read_timezone_from_file
from helpers.usecase_handler import camera_metadata, post_processing, labels
from helpers.logger import console_logger


def on_ai_call(data):
    try:
        # console_logger.debug(data)
        global camera_metadata
        for camera_id, dictionary in data["data"].items():
            for label in labels:
                for detected_object in dictionary[label]["detections"]:
                    if detected_object["number_plate"]:
                        x_coordinate = int(
                            (detected_object["lx2"] - detected_object["lx1"]) // 2
                        )
                        y_coordinate = int(
                            (detected_object["ly2"] - detected_object["ly1"]) // 2
                        )

                        for index, polygon in enumerate(
                            camera_metadata[camera_id]["polygons"]
                        ):

                            # if Point(x_coordinate, y_coordinate).within(polygon):
                            detection = copy.deepcopy(detected_object)
                            detection.update({"vehicle_type": labels[label]})
                            post_processing(
                                data={"camera_id": camera_id, "metadata": detection},
                                roi=camera_metadata[camera_id]["roi_name"][index],
                                buffer_index=dictionary["buffer_index"],
                            )
    except Exception as e:
        console_logger.debug(e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))