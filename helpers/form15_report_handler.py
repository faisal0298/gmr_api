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
from fastapi import (
    APIRouter,
    HTTPException,
    Form,
    Query,
    File,
    Depends,
    UploadFile,
    Header,
    Request,
    Response,
    status,
)

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

def demonstrate_table(usecase_data):
    try:
        per_data = "<table class='logistic_report_data' style='width: 100%; text-align: center; border-spacing: 0px; border: 1px solid lightgray;'>"
        per_data += (
            "<thead style='background-color: #3a62ff; color: #ffffff; height: 20px'>"
        )
        per_data += "<tr style='height: 30px;'>"
        per_data += "<th class='logic_table_th' style='font-size: 12px; text-align: center;'>Sr.No</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px; text-align: center;'>Particulars</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px; text-align: center;'>Remarks</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px; text-align: center;'>UOM</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px; text-align: center;'>MoU Coal</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px; text-align: center;'>Linkage</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px; text-align: center;'>AIWIB Washery</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px; text-align: center;'>Open Mkt</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px; text-align: center;'>Spot E-auction</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px; text-align: center;'>Spl For-E-auction</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px; text-align: center;'>Imported</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px; text-align: center;'>Total</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px; text-align: center; width: 50px; background-color: #fff; border-top: 1px solid #fff; border-bottom: 1px solid #fff;'></th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px; text-align: center;'>ShaktiB(viii)*</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px; text-align: center;'>Shakti B3</th>"
        per_data += "</tr></thead><tbody style='border: 1px solid gray;'>"
        row_number = 1
        for row, query in enumerate(usecase_data):
            result = query.payload()
            for index, (key, data) in enumerate(result.items()):
                if type(data) == dict:
                    per_data += f"<tr style='height: 30px;'>"
                    per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {row_number}</span></td>"
                    per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {data.get('particular')}</span></td>"
                    per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {data.get('remark')}</span></td>"
                    per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {data.get('uom')}</span></td>"
                    per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(data.get('mou_coal'), 2)}</span></td>"
                    per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(data.get('linkage'), 2)}</span></td>"
                    per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(data.get('aiwib_washery'), 2)}</span></td>"
                    per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(data.get('open_mkt'), 2)}</span></td>"
                    per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(data.get('spot_eauction'), 2)}</span></td>"
                    per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(data.get('spl_for_eauction'), 2)}%</span></td>"
                    per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(data.get('imported'), 2)}</span></td>"
                    per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(data.get('total'), 2)}</span></td>"
                    per_data += f"<td class='logic_table_td' style='text-align: center; border-right: 1px solid #000; border-left: 1px solid #000; border-top: 1px solid #fff; border-bottom: 1px solid #fff;'><span style='font-size: 12px; font-weight: 600;'> </span></td>"
                    per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(data.get('shakti_b'), 2)}</span></td>"
                    per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(data.get('shakti_b3'), 2)}</span></td>"
                    per_data += "</tr>"
                    row_number += 1
        per_data += "</tbody>"
        per_data += "</table>"
        return per_data
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug("----- Form 15 ----- Error:", e)
        console_logger.debug(f"Exception type: {exc_type}, Filename: {fname}, Line number: {exc_tb.tb_lineno}")
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

def header_data_value(template_data, month_date):
    try:
        header_data = "<div style='width:100%; position: relative;'>"
        header_data += "<div style='width: 600px; position: absolute; left: 50%; top: 10%; transform: translate(-50%,-50%);'>"
        header_data += "<h1 style='color: #000; margin: 2px auto; font-size: 1.5rem; text-align:center; padding-bottom: 5px;'>GMR Warora Energy Limited</h1>"
        header_data += "<h1 style='color: #3a62ff; margin: 2px auto; font-size: 1.3rem; text-align:center;'>Name of Project: Warora Thermal Power Project</h1>"
        header_data += "</div>"
        header_data += "<div class='headerChild' style='display: flex; justify-content: space-between; margin-top:0px; font-size:16px;'>"
        header_data += "<div class='header_left' style='width:40%; font-size: 16px; line-height:20px;'>"
        if template_data.get("logo_path") != None:
            encoded_logo_image = encoded_data(f"{os.path.join(os.getcwd(), template_data['logo_path'])}")
        else:
            encoded_logo_image = encoded_data(f"{os.path.join(os.getcwd())}/static_server/receipt/report_logo.jpeg")
        if template_data.get("title") == None:
            text_watermark_pdf = "GMR"
        else:
            text_watermark_pdf = template_data["title"].upper()
        header_data += f"<div style='text-align: left; margin: 1px; font-weight: 600; font-size: 14px; width: 280px; display: flex; line-height:20px;'> <span style='margin:0px; color:green; width: 100%;'>Month :</span> <span style='margin:0px; width:100%; padding-left:1px;'> {datetime.strptime(month_date, '%Y-%m').strftime('%B, %Y')}</span></div>"
        header_data += "</div>"
        header_data += "</div>"
        header_data += "</div>"
        header_data += "</div>"

        return header_data, encoded_logo_image, text_watermark_pdf
    except Exception as e:
        console_logger.debug(e)

def single_generate_report_form15(usecase_data, month):
    try:
        # console_logger.debug(fetchBunkerSingleData.ulr)

        singleTableData = demonstrate_table(usecase_data)

        payload={}
        header={}
        template_url = f"http://{host}/api/v1/base/report/template"

        response = requests.request("GET", url=template_url, headers=header, data=payload)

        template_data = json.loads(response.text)

        header_data, encoded_logo_image, text_watermark_pdf = header_data_value(template_data, month)

        title = f"GMR Bunker performance report"
        html_template = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{title}</title>
        </head>
        <div class="header">
            <img src="data:image/png;base64,{encoded_logo_image}" alt="" class="header-sticky-top" />
        </div>
        <body>
            {header_data}
            <hr style="margin: 10px 0px;" />
                
            <div class="footertable" style="width:100%; margin-top:20px;">
                <div class="title" style="width: 100%; display: flex; flex-direction:row; gap: 10px; height:50px ; align-items:center">
                    <p style="color: #3a62ff; font-size: 16px; margin: 5px; font-weight: 600;">
                        Form - 15 Report for {datetime.strptime(month,'%Y-%m').strftime('%B, %Y')}
                    </p>
                </div>
                {singleTableData}
            </div>
            <br>
            <br>
            <div style="width: 100%; display: table;">
                <div style="display: table-row">
                    <div style="border: 1px solid black; display: table-cell;">Date: 23.12.2024<br>For GMR Warora Energy Limited</div>
                    <div style="text-align: right; border: 1px solid black; display: table-cell;">For Kuralkar Shastri & Co<br>Chartered Accountants</div>
                </div>
                <div style="display: table-row">
                    <div style="border: 1px solid black; display: table-cell; padding-top: 60px;">
                        <div style="display: flex; justify-content: space-between;">
                            <span>(Dhananjay Deshpande)<br>Chief Operating Officer</span>
                            <span style="text-align: right; margin-left: auto;">(Ashish Deshpande)<br>Head - F&A</span>
                        </div>
                    </div>
                    <div style="text-align: right; padding-top: 30px; border: 1px solid black; display: table-cell;">
                        (Nitin R. Kuralkar - Partner)<br>M. No. 106430<br>FRN : 110010W
                    </div>
                </div>
            </div>
        </body>
        <footer style="font-family: arial, sans-serif; font-size: 8px;">Autogenerated report | GMR</footer>

        </html>
        """

        file = str(datetime.now().strftime("%d-%m-%Y"))
        store_data = os.path.join(os.getcwd(),"static_server", "gmr_ai", file)
        os.umask(0)
        os.makedirs(store_data, exist_ok=True, mode=0o777)

        pdf_name = datetime.now().strftime("%d-%m-%Y%H%M%S")

        # generating pdf based on above HTML
        HTML(string=html_template).write_pdf(
            f"{store_data}/form15_{pdf_name}.pdf",
            stylesheets=[CSS(os.path.join(os.getcwd(), "helpers", "styleextrabunker.css"))],
        ) 
        return {"data": f"static_server/gmr_ai/{file}/form15_{pdf_name}.pdf"}
    except Exception as e:
        console_logger.debug(e)