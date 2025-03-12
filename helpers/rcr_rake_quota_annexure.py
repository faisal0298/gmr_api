import re
import PyPDF3
from helpers.logger import console_logger
import os, sys


def enpoint_to_ocr_rcrrakequota_annexure(file_path):
    try:
        pdfFileObj = open(file_path, 'rb')
        pdfReader = PyPDF3.PdfFileReader(pdfFileObj)

        pageObj = pdfReader.getPage(0)
        mytext = pageObj.extractText()

        result={}
        date_match = re.search(r'No\.\s*([\w/-]+)', mytext, re.DOTALL)
        result["no"] = date_match.group(1) if date_match else None

        prog_id = re.search(r'PROG ID-\s*([\w/]+)', mytext, re.DOTALL)
        result["prog_id"] = prog_id.group(1) if prog_id else None

        booking_station = re.search(r'3\s*Booking Station \(Goods Shed/Siding\)\s*(\(.*\).*\n?.*)', mytext)
        result["booking_station"] = booking_station.group(1).replace('\n', ' ') if booking_station else None

        no_of_boxn = re.search(r'5\s*No\.\s*of\s*BOXN/BOBRN\s*Rakes\s*\(in\s*59\s*BOXN/BOBRN\)\s*(.*)', mytext)
        result["no_of_boxn"] = no_of_boxn.group(1) if no_of_boxn else None

        validity = re.search(r'Validity of this approval is for \((\d{2}-\d{2}-\d{4})\)', mytext)
        result["validity"] = validity.group(1) if validity else None

        npt_data = re.search(r"NPT/[A-Z]+/[A-Za-z0-9-]+/[A-Z]+/[A-Za-z0-9]+", mytext)
        result["npt_data"] = npt_data[0] if npt_data else None

        last_date = re.search(r"Dated: \d{2}-\d{2}-\d{4}", mytext, re.DOTALL)
        result["npt_date"] = last_date[0].split(": ")[1] if last_date else None
        return result
    except Exception as e:
        success = False
        console_logger.debug("----- rcr rake quota annexure error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        success = e