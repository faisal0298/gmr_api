from weasyprint import HTML, CSS
import base64
import os, sys
from weasyprint.text.fonts import FontConfiguration
from PIL import Image, ImageDraw, ImageFont, ImageOps, ImageChops
import PyPDF2
from reportlab.pdfgen import canvas
from io import BytesIO
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
import warnings
import shutil
from collections import OrderedDict, Counter, defaultdict
from dateutil.relativedelta import *
from datetime import datetime, timedelta, date
import random
import string
from helpers.logger import console_logger
from service import client, db
import subprocess, json, os, sys
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import pandas as pd
import matplotlib
import numpy as np
from itertools import cycle
import matplotlib.cm as cm
import warnings
import shutil
from dateutil.relativedelta import *
from pandas.tseries.offsets import DateOffset
import requests
from pathlib import Path
import matplotlib.patches as mpatches

from dateutil import tz
import pytz


# client = MongoClient(f"mongodb://{host}:{db_port}/")
# db = client.gmrDB.get_collection("gmrdata")
host = os.environ.get("IP", "192.168.1.57")

format_str = "%d %B %Y %H:%M %p"
current_date_time = datetime.now().strftime(format_str)
format_report_str = "%d %B %Y"
report_generated = datetime.now().strftime(format_report_str)
image_watermark_there = "false"





def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print("Error: Creating directory. " + directory)


def random_string_generation(value):
    """
    Function that will generate random string
        Parameters
        ----------
        value: str
            random number to generate string

        Returns
        -------
        random_string
    """
    try:
        random_string = "".join(
            random.choices(string.ascii_uppercase + string.digits, k=value)
        )
        return random_string
    except Exception as e:
        console_logger.debug(e)


random_string = random_string_generation(8)
watermrk_img = f"watermark_{random_string}.png"


# add image watermark start
def add_image_watermark(
    image_path,
    watermark_text=None,
    watermark_image=None,
    opacity=0.5,
    rotation_angle=30,
):
    """
    Function that will add watermark to image
        Parameters
        ----------
        image_path: str
            image_path as string
        watermark_text: str [Optional]
            text what we want to add as an watermark
        watermark_image: str [Optional]
            image that we want add as an watermark
        opacity: float
            opacity for watermark
        rotation_angle: int
            an angle to show watermark on image

        Returns
        -------
        watermark_img_path
    """
    try:
        # Open the original image
        img = Image.open(image_path).convert("RGBA")
        image_random_generate = "".join(
            random.choices(string.ascii_uppercase + string.digits, k=6)
        )

        # If watermark text is provided, create a text watermark
        if watermark_text:
            font = ImageFont.truetype(
                "./static_server/reports/arial_bold.ttf", size=36
            )  # Replace with the path to your font file
            watermark = Image.new("RGBA", img.size, (255, 255, 255, 0))
            draw = ImageDraw.Draw(watermark)
            text_width, text_height = draw.textsize(watermark_text, font)
            draw.text(
                ((img.width - text_width) // 2, (img.height - text_height) // 2),
                watermark_text,
                font=font,
                fill=(255, 255, 255, int(255 * opacity)),
            )

        # If watermark image is provided, open the image
        elif watermark_image:
            watermark = Image.open(watermark_image).convert("RGBA")
            watermark = watermark.rotate(rotation_angle, expand=True)
            # watermark = watermark.point(lambda x: min(x, 50))
            watermark = watermark.resize(img.size, Image.ANTIALIAS)
            watermark.putalpha(10)

        # Composite the watermark onto the image
        watermarked_img = Image.alpha_composite(img, watermark)

        # Save the watermarked image
        output_path = f"static/image/wm/watermarked_{image_random_generate}.png"
        watermarked_img.save(output_path)
        return output_path
    except Exception as e:
        console_logger.debug(e)


def encoded_data(image_path):
    """
    Function that will encode an image to base64
        Parameters
        ----------
        image_path: str
            path of image

        Returns
        -------
        encoded image
    """
    try:
        with open(image_path, "rb") as image_file:
            image_data = image_file.read()
            encoded_data = base64.b64encode(image_data).decode()
        return encoded_data
    except Exception as e:
        console_logger.debug(e)


def convert_time_to_range(hrs):
    try:
        hour = int(hrs)
        if 0 <= hour < 24:
            data = f"{hour}:00 - {hour+1}:00"
            return data
    except Exception as e:
        console_logger.debug(e)


# pdf watermark start
def create_text_watermark(text, wm_size):
    """
    Function that will text watermark
        Parameters
        ----------
        text: data which we want as watermark text on pdf
        wm_size: waternark size on pdf


        Returns
        -------

    """
    try:
        packet = BytesIO()
        c = canvas.Canvas(packet, pagesize=A4)
        c.setFillAlpha(0.2)
        c.setFillGray(0.5)
        if wm_size == "small":
            c.setFont("Helvetica", 20)
            c.rotate(52)
            text_data = f"{text} "
            c.drawString(50, 30, text_data * 20)
        elif wm_size == "big":
            c.setFont("Helvetica", 50)
            c.rotate(0) #45
            # c.drawString(400, 100, text) #45
            c.drawString(200, 400, text)
        c.save()
        packet.seek(0)
        return PyPDF2.PdfReader(packet).pages[0]
    except Exception as e:
        console_logger.debug(e)


def create_image_watermark(picture_path, data):
    """
    Function that will generate an image watermark
        Parameters
        ----------
        picture_path: picture_path for watermark on pdf


        Returns
        -------
        watermark image path
    """
    try:
        file = str(datetime.now().strftime("%d-%m-%Y"))
        c = canvas.Canvas(
            f"{os.path.join(os.getcwd())}/static_server/gmr_ai/{file}/watermark_empty_{random_string}.pdf",
            pagesize=A4,
        )

        c.setFillAlpha(0.1)
        im = Image.open(picture_path)
        width, height = im.size
        # vertical = 0, horizontal = 45, diagonal = 90
        if data["wm_angle"] == "vertical":
            c.rotate(0)
            # c.drawImage(picture_path, 180, 400, width, height, mask="auto") # a4 normal
            c.drawImage(picture_path, 300, 300, width, height, mask="auto") # a4 landscape
        elif data["wm_angle"] == "horizontal":
            c.rotate(45)
            # c.drawImage(picture_path, 400, 0, 250, 250, mask="auto")
            c.drawImage(picture_path, 400, 0, width, height, mask="auto") # a4 normal
        elif data["wm_angle"] == "diagonal":
            c.rotate(90)
            c.drawImage(picture_path, 300, -400, width, height, mask="auto")
        c.save()
        return f"{os.path.join(os.getcwd())}/static_server/gmr_ai/{file}/watermark_empty_{random_string}.pdf"
    except Exception as e:
        console_logger.debug(e)


def apply_pdf_watermark(
    pdf_path, output_path, picture_path=None, text_watermark=None, data=None
):
    """
    Function that will apply watermark to pdf
        Parameters
        ----------
        pdf_path: path for pdf
        outpath_path: path where we want to stor watermark pdf
        picture_path [Optional]: watermark image
        text_watermark [Optional]: watermark text


        Returns
        -------
        watermark image path
    """
    try:
        pdf_reader = PyPDF2.PdfReader(pdf_path)
        pdf_writer = PyPDF2.PdfWriter()

        if picture_path:
            watermark_path = create_image_watermark(picture_path, data)
            watermark = PyPDF2.PdfReader(watermark_path).pages[0]
        elif text_watermark:
            watermark = create_text_watermark(text_watermark, wm_size="big")

        for page in pdf_reader.pages:
            page.merge_page(watermark)
            pdf_writer.add_page(page)

        with open(output_path, "wb") as output_file:
            pdf_writer.write(output_file)

    except Exception as e:
        print(f"Error applying watermark to {pdf_path}: {e}")


# pdf watermark end


# delete file start
def delete_file(work_dir, extension):
    """
    Function that will delete file
        Parameters
        ----------
        work_dir: directory including sub directories where we want to delete files
        extension: extension of things we want to delete

        Returns
        -------

    """
    try:
        for root, dirnames, filenames in os.walk(work_dir):
            for filename in filenames:
                path = os.path.join(root, filename)
                if path.endswith(extension):
                    os.unlink(path)
    except Exception as e:
        console_logger.debug(e)


# delete file end


# convert default dict to dict
def default_to_regular(d):
    """
    Function that will convert default dictionary to regular dictionary including nested dictionary
        Parameters
        ----------
        d: default dictionary

        Returns
        -------
        regular dictionary

    """
    try:
        if isinstance(d, defaultdict):
            d = {k: default_to_regular(v) for k, v in d.items()}
        return d
    except Exception as e:
        console_logger.debug(e)


def breaksentenceOnNthPos(input, matchStr, repStr, nth):
    """
    Function that will break sentence after particular space for e.g.: after every 4th space it will add new line
        Parameters
        ----------
        input: sentence which we will send
        matchstr: " " [spaces which is there in sentences]
        repstr: "\n to add new line"
        nth: after how many spaces we want to add line

        Returns
        -------
        breakable sentences
    """
    try:
        findPos = input.find(matchStr)
        index = findPos != -1
        while findPos != -1 and index != nth:
            findPos = input.find(matchStr, findPos + 1)
            index += 1
        if index == nth:
            return input[:findPos] + repStr + input[findPos + len(matchStr) :]
        return input
    except Exception as e:
        console_logger.debug(e)

# add . and two zeros
def format_number(value):
    if isinstance(value, (int, float)):
        formatted = f"{value:.2f}"
        if formatted.endswith(".00"):
            return f"{int(value)}.00"
        return formatted
    return value

def convert_utc_to_ist(utc_time_str, pattern):
    """
    Convert UTC time to Indian Standard Time (IST).

    :param utc_time_str: A string representing UTC datetime in the format "%Y-%m-%d %H:%M:%S.%f"
    :return: A string representing IST datetime in the same format
    """
    # Define UTC and IST timezones
    utc = pytz.utc
    ist = pytz.timezone("Asia/Kolkata")

    # Convert string to datetime object
    utc_time = datetime.strptime(utc_time_str, pattern)

    # Localize UTC time
    utc_time = utc.localize(utc_time)

    # Convert to IST
    ist_time = utc_time.astimezone(ist)

    # Return IST time as string
    return ist_time

def weighbridge_report_generate(fetchGmrData, mPayload, fetchSapRecords):
    try:
        gmr_logo = encoded_data(f"{os.path.join(os.getcwd())}/static_server/receipt/report_logo.png")
        if len(str(fetchGmrData.vehicle_in_time)) == 26:
            vehicle_in_time_format = convert_utc_to_ist(str(fetchGmrData.vehicle_in_time), "%Y-%m-%d %H:%M:%S.%f")
            
        else:
            vehicle_in_time_format = convert_utc_to_ist(str(fetchGmrData.vehicle_in_time), "%Y-%m-%d %H:%M:%S")
        if len(str(fetchGmrData.GWEL_Tare_Time)) == 26:
            gwel_tare_time = convert_utc_to_ist(str(fetchGmrData.GWEL_Tare_Time), "%Y-%m-%d %H:%M:%S.%f")
        else:
            gwel_tare_time = convert_utc_to_ist(str(fetchGmrData.GWEL_Tare_Time), "%Y-%m-%d %H:%M:%S")
        
        if len(str(fetchGmrData.GWEL_Gross_Time)) == 26:
            gwel_gross_time = convert_utc_to_ist(str(fetchGmrData.GWEL_Gross_Time), "%Y-%m-%d %H:%M:%S.%f")
        else:
            gwel_gross_time = convert_utc_to_ist(str(fetchGmrData.GWEL_Gross_Time), "%Y-%m-%d %H:%M:%S")

        title = f"Weighbridge report"
        html_template = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>GMR Warora Energy Limited Receipt</title>
        </head>
        <body style="font-family: Arial, sans-serif; margin: 0px;">

            <div>
                <img src="data:image/png;base64,{gmr_logo}" style="width: 80px; position: absolute;">
                <h2 style="text-align: center; margin-bottom: 5px;">
                    GMR Warora Energy Limited
                </h2>
                <p style="text-align: center; margin-top: 0; margin-bottom: 0px;">
                    <strong>B-1, Mohabala MIDC Growth Cent, Chandrapur – 442907</strong>
                </p>

                <div style="display: flex; justify-content: space-between; font-size: 14px; margin: 0; padding: 0;">
                    <p style="margin-top: 5px; margin-bottom: 0px;"><strong>Transaction ID:</strong> <span style="font-weight: bold;">{fetchGmrData.id}</span></p>
                    <p style="margin-top: 5px; margin-bottom: 0px; width: 250px;"><strong>Gate In:</strong> <span style="font-weight: bold;">{vehicle_in_time_format.strftime("%d.%m.%Y - %H:%M:%S")}</span></p>
                </div>
                <div style="display: flex; justify-content: space-between; font-size: 14px; margin: 0; padding: 0;">
                    <p style="margin-top: 5px; margin-bottom: 0px;"><strong>Vehicle No.:</strong> <span style="font-weight: bold;">{mPayload.get('vehicle_number')}</span></p>
                    <p style="margin-top: 5px; margin-bottom: 0px; width: 250px;"><strong>Challan No.:</strong> <span style="font-weight: bold;">{fetchGmrData.delivery_challan_number}</span></p>
                </div>

                
                <h4 style="text-align: center; margin-bottom: 5px; margin-top: 5px;">
                    ----------------------------------- <span style="font-weight: normal;">Item Details</span> -----------------------------------
                </h4>

                <table style="width: 100%; border-collapse: collapse; text-align: center;">
                    <tr style="background: #ddd;">
                        <th style="border: 1px solid black; font-size: 13px;">Document</th>
                        <th style="border: 1px solid black; font-size: 13px;">Item</th>
                        <th style="border: 1px solid black; font-size: 13px;">Material</th>
                        <th style="border: 1px solid black; font-size: 13px; word-break: break-all;">Description</th>
                        <th style="border: 1px solid black; font-size: 13px;">Challan Gross Wt(MT)</th>
                        <th style="border: 1px solid black; font-size: 13px;">Challan Tare Wt(MT)</th>
                        <th style="border: 1px solid black; font-size: 13px;">Challan Net Wt(MT)</th>
                        <th style="border: 1px solid black; font-size: 13px;">GWEL Gross Wt(MT)</th>
                        <th style="border: 1px solid black; font-size: 13px;">GWEL Tare Wt(MT)</th>
                        <th style="border: 1px solid black; font-size: 13px;">GWEL Net Wt(MT)</th>
                        <th style="border: 1px solid black; font-size: 13px;">Transist Loss</th>
                    </tr>
                    <tr>
                        <td style="border: 1px solid black; font-size: 13px;">{fetchGmrData.po_no}</td>
                        <td style="border: 1px solid black; font-size: 13px;">{fetchGmrData.line_item}</td>
                        <td style="border: 1px solid black; font-size: 13px;">{fetchSapRecords.material_code if fetchSapRecords and fetchSapRecords.material_code else "N/A"}</td>
                        <td style="border: 1px solid black; font-size: 13px;">{fetchSapRecords.material_description if fetchSapRecords and fetchSapRecords.material_description else "N/A"}</td>
                        <td style="border: 1px solid black; font-size: 13px;">{format_number(int(mPayload.get("gross_qty")) if "." not in mPayload.get("gross_qty") else float(mPayload.get("gross_qty")))}</td>
                        <td style="border: 1px solid black; font-size: 13px;">{format_number(int(mPayload.get("tare_qty")) if "." not in mPayload.get("tare_qty") else float(mPayload.get("tare_qty")))}</td>
                        <td style="border: 1px solid black; font-size: 13px;">{format_number(int(mPayload.get("net_qty")) if "." not in mPayload.get("net_qty") else float(mPayload.get("net_qty")))}</td>
                        <td style="border: 1px solid black; font-size: 13px;">{format_number(int(mPayload.get("actual_gross_qty")) if "." not in mPayload.get("actual_gross_qty") else float(mPayload.get("actual_gross_qty")))}</td>
                        <td style="border: 1px solid black; font-size: 13px;">{format_number(int(mPayload.get("actual_tare_qty")) if "." not in mPayload.get("actual_tare_qty") else float(mPayload.get("actual_tare_qty")))}</td>
                        <td style="border: 1px solid black; font-size: 13px;">{format_number(int(mPayload.get("actual_net_qty")) if "." not in mPayload.get("actual_net_qty") else float(mPayload.get("actual_net_qty")))}</td>
                        <td style="border: 1px solid black; font-size: 13px;">{round(float(mPayload.get("net_qty"))-float(mPayload.get("actual_net_qty")), 2)}</td>
                    </tr>
                </table>
                <div style="display: flex; justify-content: space-between; margin-top: 18px;">
                     <div style="display: flex; flex-direction:column; gap:0px">
                        <p style="margin: 0px;"><strong>Gross Date Time:</strong> {gwel_gross_time.strftime('%d.%m.%Y - %H:%M:%S')}</p>
                        <p style="margin: 0px;"><strong>Tare Date Time:</strong> {gwel_tare_time.strftime('%d.%m.%Y - %H:%M:%S')}</p>
                     </div>
                     <p style="text-align: right; margin-bottom: 0px;"><strong>Operator Signature</strong></p>
                 </div>

                <h4 style="text-align: center; margin-top: 0px; margin-bottom: 0px;">
                    ----------------------------------- <span style="font-weight: normal;">Cut Here</span> -----------------------------------
                </h4>
            </div>

        </body>
        </html>
        """

        file = str(datetime.now().strftime("%d-%m-%Y"))
        store_data = os.path.join(os.getcwd(),"static_server", "gmr_ai", file)
        os.umask(0)
        os.makedirs(store_data, exist_ok=True, mode=0o777)

        pdf_name = datetime.now().strftime("%d-%m-%Y%H%M%S")
        # file_name_data = fetchBunkerSingleData.certificate_no.replace("/", "_")
        file_name = f"{fetchGmrData.delivery_challan_number}_weighbridge_report"

        # generating pdf based on above HTML
        HTML(string=html_template).write_pdf(
            f"{store_data}/{file_name}_{pdf_name}.pdf",
            stylesheets=[CSS(os.path.join(os.getcwd(), "helpers", "weighbridge_stylesheet.css"))],
        ) 
        return {"data": f"static_server/gmr_ai/{file}/{file_name}_{pdf_name}.pdf"}
    except Exception as e:
        success = False
        console_logger.debug("----- WeighBridge Report Error -----", e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        success = e

