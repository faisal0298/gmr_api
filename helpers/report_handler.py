# from weasyprint import HTML, CSS
import base64
import os, sys
# from weasyprint.text.fonts import FontConfiguration
# from PIL import Image, ImageDraw, ImageFont, ImageOps, ImageChops
# import PyPDF2
# from reportlab.pdfgen import canvas
# from io import BytesIO
# from reportlab.lib.pagesizes import letter, A4
# from reportlab.lib import colors
# from reportlab.lib.units import inch
# from reportlab.pdfgen import canvas
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
# import matplotlib.pyplot as plt
# from matplotlib.backends.backend_pdf import PdfPages
import pandas as pd
# import matplotlib
import numpy as np
from itertools import cycle
# import matplotlib.cm as cm
import warnings
import shutil
from dateutil.relativedelta import *
from pandas.tseries.offsets import DateOffset
import requests
from pathlib import Path
# import matplotlib.patches as mpatches


# client = MongoClient(f"mongodb://{host}:{db_port}/")
# db = client.gmrDB.get_collection("gmrdata")
host = os.environ.get("IP", "192.168.1.57")

transporter_collection = db.transporter
short_mine_collection = db.short_mine
supplier_collection = db.supplier
consignee_collection = db.consignee
truck_collection = db.truck
fastag_collection = db.fastag
coal_testing = db.coal_test
coal_consumption = db.coal_consumption


# matplotlib.use("agg")

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


def header_data_value(template_data, month_date):
    try:
        header_data = "<div style='width:100%; position: relative;'>"
        header_data += "<div style='width: 350px; position: absolute; left: 50%; top: 10%; transform: translate(-50%,-50%);'>"
        header_data += "<h1 style='color: #3a62ff; margin: 2px auto; font-size: 1.3rem;text-align:center;'>Daily Coal Logistic Report </h1>"
        header_data += "</div>"
        header_data += "<div class='headerChild' style='display: flex; justify-content: space-between; margin-top:0px; font-size:16px;'>"
        header_data += "<div class='header_left' style='width:40%; font-size: 16px; line-height:20px;'>"
        if template_data.get("logo_path") != None:
            console_logger.debug(f"{os.path.join(os.getcwd(), template_data['logo_path'])}")
        #     # encoded_logo_image = encoded_data(f"{os.path.join(os.getcwd())}/static_server/receipt/report_logo.jpeg")
            encoded_logo_image = encoded_data(f"{os.path.join(os.getcwd(), template_data['logo_path'])}")
        #     header_data += f"<img src='data:image/png;base64,{encoded_logo_image}' alt='img not found' style='width:100px; '/>"
        else:
        #     # encoded_logo_image = encoded_data(
        #     #     f"./static_server/recipt/report_logo.jpeg"
        #     # )
            encoded_logo_image = encoded_data(f"{os.path.join(os.getcwd())}/static_server/receipt/report_logo.jpeg")
        #     header_data += f"<img src='data:image/png;base64,{encoded_logo_image}' alt='img not found' style='width:100px; '/>"
        # if template_data.get("sitename") != None:
        #     header_data += f"<p style='margin: 5px auto;'> <span style='font-weight: 600; font-size: 16px'> Branch Name -</span> {template_data['sitename']}</p>"
        # if template_data.get("managername") != None:
        #     header_data += f"<p style='margin: 5px auto;'> <span style='font-weight:  600; font-size: 16px'> Branch Manager -</span> {template_data['managername']}</p>"

        # if (
        #     template_data.get("addressline1") != None
        #     or template_data.get("addressline2") != None
        #     or template_data.get("addressline3") != None
        # ):
        #     header_data += "<div style='display:flex; width: 280px; font-size: 16px'>"
        #     header_data += "<div style='font-weight: 600; width: 80px; font-size: 16px'> Address : </div>"
        #     header_data += (
        #         "<div style='display:flex; flex-direction:column; line-height:20px;'>"
        #     )
        #     if template_data.get("addressline1") != None:
        #         header_data += f"<p style='padding-left:5px; font-size: 12px; width:10vw; margin:1px;'>{template_data.get('addressline1')}</p>"
        #     if template_data.get("addressline2") != None:
        #         header_data += f"<p style='padding-left:5px; font-size: 12px; width:10vw; margin:1px;'>{template_data.get('addressline2')}</p>"
        #     if template_data.get("addressline3") != None:
        #         header_data += f"<p style='padding-left:5px; font-size: 12px; width:10vw; margin:1px;'>{template_data.get('addressline3')}</p>"
        #     header_data += "</div>"
        #     header_data += "</div>"
        # header_data += "</div>"
        # header_data += "<div class='header_right' style='width:40%; margin:0px; margin-left:auto; line-height:20px;'>"
        if template_data.get("title") == None:
        #     header_data += "<h2 style='font-style:bold; font-size: 16px; margin: 5px; text-align: right;line-height:20px;'>RDX</h2>"
            text_watermark_pdf = "GMR"
        else:
        #     header_data += f"<h2 style='font-style:bold; font-size: 16px; margin: 5px; text-align: right;line-height:20px;'>{template_data['title'].upper()}</h2>"
            text_watermark_pdf = template_data["title"].upper()
        # header_data += "<div style='display: flex; width: 280px; justify-content: space-between; font-size: 16px;line-height:20px;margin-top:18px;'>"
        # header_data += "<div style='font-weight:600; width: 100px;line-height:20px;'> Usecase : </div>"
        # header_data += f"<div style='width:220px; padding-left:3px;line-height:20px;'> GMR</div>"
        # header_data += "</div>"

        # if template_data.get("title") != None:
        #     end_date = template_data["data"]["data"]["end_time"].replace(",", "")
        #     start_date = template_data["data"]["data"]["start_time"].replace(",", "")
        #     # format_str = "%d %B %Y %H:%M %p"
        #     format_data = "%m/%d/%Y %H:%M:%S"

        #     to_date = datetime.strptime(end_date, format_data).strftime(
        #         "%d %B %Y %H:%M %p"
        #     )
        #     from_date = datetime.strptime(start_date, format_data).strftime(
        #         "%d %B %Y %H:%M %p"
        #     )

        #     header_data += f"<div style='text-align: left; margin: 1px; font-weight: 600; font-size: 14px; width: 280px; display:flex;line-height:20px;'> <p style='margin:0px; color:blue; width: 100px;'> From :</p> <p style='margin:0px; width:220px ;padding-left:3px;'> {from_date}</p> </div>"
        #     header_data += f"<div style='text-align: left; margin: 1px; font-weight: 600; font-size: 14px; width: 280px; display: flex;line-height:20px;'> <p style='margin:0px; color:blue;width: 100px;'> To :</p> <p style='margin:0px; width:220px; padding-left:3px;'> {to_date}</p> </div>"
        header_data += f"<div style='text-align: left; margin: 1px; font-weight: 600; font-size: 14px; width: 280px; display: flex; line-height:20px;'> <span style='margin:0px; color:green; width: 100%;'>Report date :</span> <span style='margin:0px; width:100%; padding-left:1px;'> {datetime.strptime(month_date,'%Y-%m-%d').strftime('%d %B %Y')}</span></div>"
        # header_data += f"<div style='text-align: left; margin: 1px; font-weight: 600; font-size: 14px; width: 280px;  display: flex;line-height:20px;'> <p style='margin:0px; color:green;width: 100px;'>Generated date :</p> <p style='margin:0px; width:220px; padding-left:3px;'> {current_date_time}</p></div>"
        header_data += "</div>"
        header_data += "</div>"
        header_data += "</div>"
        header_data += "</div>"

        return header_data, encoded_logo_image, text_watermark_pdf
    except Exception as e:
        console_logger.debug(e)


def logistic_report_table(data):
    try:
        per_data = "<table class='logistic_report_data' style='width: 100%; text-align: center; border-spacing: 0px; border: 1px solid lightgray;'>"
        per_data += (
            "<thead style='background-color: #3a62ff; color: #ffffff; height: 20px'>"
        )
        per_data += "<tr style='height: 30px;'>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>Mine Name</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>DO_No</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>Grade</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>DO Qty</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>Challan LR Qty</th>"
        # per_data += "<th class='logic_table_th' style='font-size: 12px;'>Cumulative Challan LR Qty</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>C.C. LR Qty</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>Balance Qty</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>% of Supply</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>Balance Days</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>Asking Rate</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>Do Start date</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>Do End date</th></tr></thead><tbody style='border: 1px solid gray;'>"
        for single_data in data:
            per_data += f"<tr style='height: 30px;'>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {single_data.get('mine_name')}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {single_data.get('DO_No')}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {single_data.get('average_GCV_Grade')}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(single_data.get('DO_Qty'), 2)}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(single_data.get('challan_lr_qty'), 2)}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(single_data.get('cumulative_challan_lr_qty'), 2)}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(single_data.get('balance_qty'), 2)}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(single_data.get('percent_supply'), 2)}%</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {single_data.get('balance_days')}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(single_data.get('asking_rate'))}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {datetime.strptime(single_data.get('start_date'),'%Y-%m-%d').strftime('%d %B %y')}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {datetime.strftime(single_data.get('end_date'),'%d %B %y')}</span></td>"
            per_data += "</tr>"
        per_data += "</tbody></table>"
        return per_data
    except Exception as e:
        console_logger.debug(e)


def logistic_report_table_rail(fetchRailData):
    try:
        console_logger.debug(fetchRailData)
        per_data = "<table class='logistic_report_data' style='width: 100%; text-align: center; border-spacing: 0px; border: 1px solid lightgray;'>"
        per_data += (
            "<thead style='background-color: #3a62ff; color: #ffffff; height: 20px'>"
        )
        per_data += "<tr style='height: 30px;'>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>Mine Name</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>DO No</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>Grade</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>DO Qty</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>Challan LR Qty</th>"
        # per_data += "<th class='logic_table_th' style='font-size: 12px;'>Cumulative Challan LR Qty</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>C.C. LR Qty</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>Balance Qty</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>% of Supply</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>Balance Days</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>Asking Rate</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>Do Start date</th>"
        per_data += "<th class='logic_table_th' style='font-size: 12px;'>Do End date</th></tr></thead><tbody style='border: 1px solid gray;'>"
        for single_data in fetchRailData:
            per_data += f"<tr style='height: 30px;'>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {single_data.get('mine_name')}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {single_data.get('rr_no')}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {single_data.get('average_GCV_Grade')}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(single_data.get('rr_qty'), 2)}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(single_data.get('challan_lr_qty'), 2)}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(single_data.get('cumulative_challan_lr_qty'), 2)}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(single_data.get('balance_qty'), 2)}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(single_data.get('percent_supply'), 2)}%</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {single_data.get('balance_days')}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {round(single_data.get('asking_rate'))}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {single_data.get('start_date')}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 12px; font-weight: 600;'> {single_data.get('end_date')}</span></td>"
            per_data += "</tr>"
        per_data += "</tbody></table>"
        return per_data
    except Exception as e:
        console_logger.debug(e)


def bubbleSort(arr, dependency_arr_1, dependency_arr_2):


    n = len(arr)
    # optimize code, so if the array is already sorted, it doesn't need
    # to go through the entire process
    # Traverse through all array elements
    for i in range(n-1):

        # range(n) also work but outer loop will
        # repeat one time more than needed.
        # Last i elements are already in place
        swapped = False
        for j in range(0, n-i-1):

            # traverse the array from 0 to n-i-1
            # Swap if the element found is greater
            # than the next element
            if arr[j] > arr[j + 1]:
                swapped = True
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
                dependency_arr_1[j], dependency_arr_1[j + 1] = dependency_arr_1[j + 1], dependency_arr_1[j]
                dependency_arr_2[j], dependency_arr_2[j + 1] = dependency_arr_2[j + 1], dependency_arr_2[j]
        if not swapped:
            # if we haven't needed to make a single swap, we
            # can just exit the main loop.
            return arr, dependency_arr_1, dependency_arr_2
        
    return arr, dependency_arr_1, dependency_arr_2

def bar_graph_gcv_wise(rrNo_values, aopList, month_date):
    try:
        if rrNo_values:
            # Extract rrNo and their aggregated values
            rrNo = list(rrNo_values.keys())
            values = list(rrNo_values.values())
            # indexes = [rrNo.index(item) for item in rrNo_values.keys()]
            indexes = list(range(len(rrNo)))

            if aopList:

                line_x = [rrNo.index(aop['source_name']) for aop in aopList if aop['source_name'] in rrNo]
                line_y = [int(aop['aop_target']) for aop in aopList if aop['source_name'] in rrNo]

                console_logger.debug(set(indexes)-set(line_x))
                for _not_present_index in list(set(indexes)-set(line_x)):
                    line_x.append(_not_present_index)
                    line_y.append(0)
                
                # line_x, line_y, values =  bubbleSort(line_x, line_y, values)
                sorted_indices = sorted(range(len(line_x)), key=lambda k: line_x[k])
                line_x = [line_x[i] for i in sorted_indices]
                line_y = [line_y[i] for i in sorted_indices]


            # Create the bar graph
            plt.figure(figsize=(10, 6))
            bars = plt.bar(rrNo, values, color='#3a62ff')
            plt.xlabel('Mines', fontsize=15)
            plt.ylabel('Average GCV Value (Arb)', fontsize=15)
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            plt.grid(color='gray', linestyle=':', linewidth=0.5, zorder=0)
            plt.gca().set_axisbelow(True)  # Ensure grid lines are drawn below the bars

            pop_a = mpatches.Patch(color='#3a62ff', label='Mines') 
            pop_b = mpatches.Patch(color='red', label='Target') 
            plt.legend(handles=[pop_a, pop_b], facecolor='white', framealpha=1)   
            title_font = {'size':'12', 'color':'#000', 'weight':'bold', 'verticalalignment':'bottom'}
            if aopList:
                # Plot the line
                plt.plot(line_x, line_y, marker='o', linestyle='-', color='red')

                # Annotate points on the line plot
                for x, y in zip(line_x, line_y):
                    plt.annotate(f'{y}', (x, y), textcoords="offset points", xytext=(0,10), ha='center')

            for bar, value in zip(bars, values):
                height = bar.get_height()
                # plt.text(bar.get_x() + bar.get_width() / 2, height, f'{value:.2f}', ha='center', va='bottom')
                plt.text(bar.get_x() + bar.get_width() / 2, 2, f'{value:.2f}', horizontalalignment='center', **title_font)

            file = "reports_img"
            # store_file = f"static_server/gmr_ai/{file}"
            store_file = os.path.join(os.getcwd(),"static_server", "gmr_ai", file)
            os.umask(0)
            os.makedirs(store_file, exist_ok=True, mode=0o777)
            image_total_file = f"gcv_bar_{random_string}.png"
            plt.savefig(f"{store_file}/{image_total_file}")
            image_total_file_name = f"{store_file}/{image_total_file}"
            plt.close()

            # Show the bar graph
            # plt.show()
            encoded_bar_chart = encoded_data(image_total_file_name)
            return encoded_bar_chart
        else:
            return None
    except Exception as e:
        console_logger.debug(e)


def profit_loss_gmr_data(data):
    try:
        if data:
            # Extracting mine names and profit/loss values
            mine_names = list(data.keys())
            profit_loss_values = list(data.values())

            # Creating the bar plot
            plt.figure(figsize=(8, 6))
            # bars = plt.bar(mine_names, profit_loss_values, color=['red' if value < 0 else 'green' for value in profit_loss_values])
            bars = plt.bar(mine_names, profit_loss_values, color='red')

            # Adding labels and title
            plt.xlabel('Mine', fontsize=15)
            plt.ylabel('Gain/Loss (MT)', fontsize=15)
            # plt.title('Profit/Loss per Mine')
            # plt.grid(True)
            plt.grid(True, color='gray', linestyle=':', linewidth=0.5, zorder=0)
            plt.gca().set_axisbelow(True)  # Ensure grid lines are drawn below the bars
            plt.title('')
            pop_a = mpatches.Patch(color='red', label='Mines') 
            lgd = plt.legend(handles=[pop_a], facecolor='white', framealpha=1, loc='upper center', bbox_to_anchor=(0.5, 1.10), fancybox=True, shadow=True, ncol=3)   
            title_font = {'size':'18', 'color':'#000', 'weight':'bold', 'verticalalignment':'bottom'}
            for bar, value in zip(bars, profit_loss_values):
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width() / 2, height, f'{value:.2f}', ha='center', va='bottom', **title_font)

            file = "reports_img"
            # store_file = f"static_server/gmr_ai/{file}"
            store_file = os.path.join(os.getcwd(),"static_server", "gmr_ai", file)
            os.umask(0)
            os.makedirs(store_file, exist_ok=True, mode=0o777)
            image_total_file = f"profit_loss_bar_gmr_{random_string}.png"
            plt.savefig(f"{store_file}/{image_total_file}", bbox_extra_artists=(lgd,), bbox_inches='tight')
            image_total_file_name = f"{store_file}/{image_total_file}"
            plt.close()

            # Show plot
            # plt.show()
            encoded_bar_chart_profit_loss = encoded_data(image_total_file_name)
            return encoded_bar_chart_profit_loss
        else:
            return None

    except Exception as e:
        console_logger.debug(e)


def profit_loss_final_data(yearly_final_data, yearly_rail_final_data):
    try:
        # if yearly_final_data:

        #     # Extract year and net quantity
        #     year = list(yearly_final_data.keys())[0]
        #     net_qty = list(yearly_final_data.values())[0]

        #     # Create gain-loss bar graph
        #     plt.figure(figsize=(6, 6))
        #     # bars = plt.bar(['Road mode'], [net_qty], color='green' if net_qty >= 0 else 'red')
        #     bars = plt.bar(['Road mode'], [net_qty], color='red')
        #     # plt.title('Yearly Gain-Loss Bar Graph')
        #     # plt.xlabel('Year')
        #     # plt.ylabel('Net Quantity')
        #     # Add text labels at the bottom of each bar
        #     title_font = {'size':'18', 'color':'#000', 'weight':'bold', 'verticalalignment':'bottom'}
        #     for bar in bars:
        #         height = bar.get_height()
        #         plt.text(bar.get_x() + bar.get_width() / 2.0, -abs(height), f"{height:.2f}", ha='center', va='bottom', **title_font)

        #     plt.title('')
        #     plt.grid(axis='y', linestyle='--', alpha=0.7)
        #     plt.tight_layout()
        #     plt.ylabel('Gain/Loss (MT)', fontsize=15)
        #     pop_a = mpatches.Patch(color='red', label='Road Mode') 
        #     lgd = plt.legend(handles=[pop_a], facecolor='white', framealpha=1, loc='upper center', bbox_to_anchor=(0.5, 1.10), fancybox=True, shadow=True, ncol=3)  
        #     # plt.show()
        #     file = "reports_img"
        #     store_file = os.path.join(os.getcwd(),"static_server", "gmr_ai", file)
        #     os.umask(0)
        #     os.makedirs(store_file, exist_ok=True, mode=0o777)
        #     image_total_file = f"yearly_loss_gain_road_mode_{random_string}.png"
        #     plt.savefig(f"{store_file}/{image_total_file}", bbox_extra_artists=(lgd,), bbox_inches='tight')
        #     image_total_file_name = f"{store_file}/{image_total_file}"
        #     plt.close()

        #     # Show plot
        #     # plt.show()
        #     encoded_bar_chart_profit_loss = encoded_data(image_total_file_name)
        #     return encoded_bar_chart_profit_loss
        # else:
        #     return None
        # console_logger.debug(yearly_final_data)
        # console_logger.debug(yearly_rail_final_data)
        if yearly_final_data and yearly_rail_final_data:
            # Extract net quantities
            net_qty_road = list(yearly_final_data.values())[0]
            net_qty_rail = list(yearly_rail_final_data.values())[0]

            # Compute net transit Gain/Loss
            console_logger.debug(net_qty_road)
            console_logger.debug(net_qty_rail)
            net_transit_gain_loss = net_qty_road - net_qty_rail

            # Create gain-loss bar graph
            plt.figure(figsize=(10, 6))
            modes = ['Road mode', 'Rail mode', 'Net Gain/Loss']
            quantities = [net_qty_road, net_qty_rail, net_transit_gain_loss]
            # colors = ['red' if qty < 0 else 'green' for qty in quantities]
            colors = ['red']

            bars = plt.bar(modes, quantities, color=colors)
            
            title_font = {'size': '18', 'color': '#000', 'weight': 'bold', 'verticalalignment': 'bottom'}
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width() / 2.0, height if height >= 0 else -abs(height), 
                            f"{height:.2f}", ha='center', va='bottom' if height >= 0 else 'top', **title_font)

            # plt.grid(axis='y', linestyle='--', alpha=0.7)
            plt.grid(axis='y', color='gray', linestyle=':', linewidth=0.5, zorder=0)
            plt.gca().set_axisbelow(True)  # Ensure grid lines are drawn below the bars
            plt.tight_layout()
            plt.ylabel('Gain/Loss (MT)', fontsize=15)
            road_patch = mpatches.Patch(color='red', label='Road/Rail Mode')
            # rail_patch = mpatches.Patch(color='green', label='Rail Mode' if net_qty_rail >= 0 else 'Loss')
            rail_patch = mpatches.Patch(color='red', label='Rail Mode' if net_qty_rail >= 0 else 'Loss')
            lgd = plt.legend(handles=[road_patch, rail_patch], facecolor='white', framealpha=1, loc='upper center', 
                                bbox_to_anchor=(0.5, 1.10), fancybox=True, shadow=True, ncol=3)
            plt.show()
            file = "reports_img"
            store_file = os.path.join(os.getcwd(), "static_server", "gmr_ai", file)
            os.umask(0)
            os.makedirs(store_file, exist_ok=True, mode=0o777)
            image_total_file = f"yearly_loss_gain_road_rail_mode_{random_string}.png"
            plt.savefig(f"{store_file}/{image_total_file}", bbox_extra_artists=(lgd,), bbox_inches='tight')
            image_total_file_name = f"{store_file}/{image_total_file}"
            plt.close()

            encoded_bar_chart_profit_loss = encoded_data(image_total_file_name)
            return encoded_bar_chart_profit_loss
        else:
            return None

    except Exception as e:
        console_logger.debug(e)


def transit_loss_gain_road_mode_month(total_monthly_final_net_qty):
    try:
        if total_monthly_final_net_qty:
            # Extract months and net quantities
            months = list(total_monthly_final_net_qty.keys())
            net_qty = list(total_monthly_final_net_qty.values())

            # Create gain-loss bar graph
            plt.figure(figsize=(10, 6))
            # bars = plt.bar(months, net_qty, color=['green' if x >= 0 else 'red' for x in net_qty])
            bars = plt.bar(months, net_qty, color="red")
            plt.title('')
            plt.xlabel('Months', fontsize=15)
            plt.ylabel('Gain/Loss (MT)', fontsize=15)
            plt.xticks(rotation=45)
            # plt.grid(axis='y', linestyle='--', alpha=0.7)
            plt.grid(color='gray', axis='y', linestyle=':', linewidth=0.5, zorder=0)
            plt.gca().set_axisbelow(True)  # Ensure grid lines are drawn below the bars
            plt.tight_layout()
            pop_a = mpatches.Patch(color="red", label='Months') 
            lgd = plt.legend(handles=[pop_a], facecolor='white', framealpha=1, loc='upper center', bbox_to_anchor=(0.5, 1.10), fancybox=True, shadow=True, ncol=3)  
            title_font = {'size':'18', 'color':'#000', 'weight':'bold', 'verticalalignment':'bottom'}
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width() / 2.0, -abs(height), f"{height:.2f}", ha='center', va='bottom', **title_font)
            # plt.show()
            
            file = "reports_img"
            store_file = os.path.join(os.getcwd(),"static_server", "gmr_ai", file)
            os.umask(0)
            os.makedirs(store_file, exist_ok=True, mode=0o777)
            image_total_file = f"transit_loss_gain_road_mode_{random_string}.png"
            plt.savefig(f"{store_file}/{image_total_file}", bbox_extra_artists=(lgd,), bbox_inches='tight')
            image_total_file_name = f"{store_file}/{image_total_file}"
            plt.close()

            encoded_transit_loss_gain_road = encoded_data(image_total_file_name)
            return encoded_transit_loss_gain_road
        else:
            return None

    except Exception as e:
        console_logger.debug(e)


def generate_report(data, rrNo_values, month_date, clubbed_data, clubbed_data_final, dayWiseVehicleInCount, dayWiseGrnReceive, dayWiseGwelReceive, dayWiseOutVehicelCount, total_monthly_final_net_qty, yearly_final_data, aopList, fetchRailData, yearly_rail_final_data):

    try:
        # supplierResult = supplier_collection.find({}, {"_id": 0})
        # consigneeResult = consignee_collection.find({}, {"_id": 0})
        # transportResult = transporter_collection.find({}, {"_id": 0})
        if rrNo_values:
            # Find the key-value pair with the highest value
            max_pair = max(rrNo_values.items(), key=lambda x: x[1])
            # Find the key-value pair with the lowest value
            min_pair = min(rrNo_values.items(), key=lambda x: x[1])
            max_key, max_value = max_pair
            # Lowest value
            min_key, min_value = min_pair
        else:
            max_key = ""
            max_value = 0
            min_key = ""
            min_value = 0


        payload={}
        header={}
        template_url = f"http://{host}/api/v1/base/report/template"

        response = requests.request("GET", url=template_url, headers=header, data=payload)

        template_data = json.loads(response.text)

        header_data, encoded_logo_image, text_watermark_pdf = header_data_value(template_data, month_date)
        if data:
            per_data = logistic_report_table(data)
        else:
            per_data = f"<b>No data found for {month_date}</b>"

        console_logger.debug(fetchRailData)
        if fetchRailData:
            per_rail_data = logistic_report_table_rail(fetchRailData)
        else:
            per_rail_data = f"<b>No data found for {month_date}</b>"

        bar_gcv_data = bar_graph_gcv_wise(rrNo_values, aopList, month_date)

        profit_loss_gmr = profit_loss_gmr_data(clubbed_data)

        transist_data_month = transit_loss_gain_road_mode_month(total_monthly_final_net_qty)

        # profit_loss_final = profit_loss_final_data(clubbed_data_final)
        profit_loss_final = profit_loss_final_data(yearly_final_data, yearly_rail_final_data)


        if bar_gcv_data:
            gcv_bar_data = f'<img src="data:image/png;base64,{bar_gcv_data}" alt="img not present" style="margin: 1px auto; width:90%; height:90%; object-fit: scale-down;"/>'
        else:
            gcv_bar_data = f"<div style='color: #000; font-size: 16px; margin: 5px ;font-weight: 600;'><b>No data found for {datetime.strptime(month_date,'%Y-%m-%d').strftime('%d %B %Y')}</b></div>"

        if profit_loss_gmr:
            profit_loss_gmr = f'<img src="data:image/png;base64,{profit_loss_gmr}" alt="img not present" style="width:100%; height:100%; object-fit: scale-down;"/>'
        else:
            profit_loss_gmr = f"<div style='color: #000; font-size: 16px; margin: 5px ;font-weight: 600;'><b>No data found for {datetime.strptime(month_date,'%Y-%m-%d').strftime('%d %B %Y')}</b></div>"

        if profit_loss_final:
            profit_loss_final = f'<img src="data:image/png;base64,{profit_loss_final}" alt="img not present" style="width:100%; height:100%; object-fit: scale-down;"/>'
        else:
            profit_loss_final = f"<div style='color: #000; font-size: 16px; margin: 5px ;font-weight: 600;'><b>No data found</b></div>"

        if transist_data_month:
            transist_data_month = f'<img src="data:image/png;base64,{transist_data_month}" alt="img not present" style="width:100%; height:100%; object-fit: scale-down;"/>'
        else:
            transist_data_month = f"<div style='color: #000; font-size: 16px; margin: 5px ;font-weight: 600;'><b>No data found</b></div>"

        title = f"GMR performance report"
        html_template = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{title}</title>
        </head>
        <div class="header">
            <img src="data:image/png;base64,{encoded_logo_image}" alt="" class="header-sticky-top"/>
        </div>
        <body>
            {header_data}
            <hr style="margin: 10px 0px;" />

            <div style="display: flex; justify-content:space-between; width:100vw; gap:10px;">
                <div style="width:49%;">
                    <table style="border-spacing: 0px; border: 1px solid lightgray;width:100% ">
                        <thead style="background-color: #3a62ff; color: #ffffff; height: 20px;">
                            <tr style="height: 30px;text-align:center">
                                <th>Insights</th>
                            </tr>
                        </thead>
                        <tbody style="border: 1px solid gray; font-size:1rem;">
                            <tr style="height: 30px;">
                                <td style=" display: flex; justify-content: space-between; padding: 5px;font-size: 14px;">
                                    <span style="font-weight: 500;">
                                        Highest GWEL GCV ({max_key}):
                                    </span>
                                    <b style="color: #3a62ff;"> {max_value} </b>
                                </td>
                            </tr>
                            <tr style="height: 30px">
                                <td style=" display: flex;justify-content:space-between;padding: 5px;font-size: 14px;">
                                    <span style="font-weight: 500;">
                                        Lowest GWEL GCV ({min_key}):
                                    </span>
                                    <b style="color: #3a62ff;"> {min_value} </b>
                                </td>
                            </tr>
                            <tr style="height: 30px">
                                <td style=" display: flex;justify-content:space-between;padding: 5px;font-size: 14px;">
                                    <span style="font-weight: 500;">
                                        Today's Transit loss:
                                    </span>
                                    <b style="color: #3a62ff;"> {round(dayWiseGrnReceive.get("data") - dayWiseGwelReceive.get("data"), 2)} </b>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
                <div style="width:49%;float:right;"> 
                    <table style="border-spacing: 0px;border: 1px solid lightgray;width:100%" >
                        <thead style="background-color: #3a62ff; color: #ffffff; height: 20px;">
                            <tr style="height: 30px;text-align:center"> 
                                <th>Counter</th>
                            </tr>
                        </thead>
                        <tbody style="font-size:1rem; border: 1px solid gray;">
                            <tr style="height: 30px">
                                <td style=" display: flex;justify-content:space-between;padding: 5px;font-size: 14px;">
                                    <span style="font-weight: 500;">
                                        {dayWiseVehicleInCount.get("title")}: 
                                    </span>
                                    <b style="color: #3a62ff;"> {dayWiseVehicleInCount.get("data")}</b>
                                </td>
                            </tr>
                            <tr style="height: 30px">
                                <td style=" display: flex;justify-content:space-between;padding: 5px;font-size: 14px;">
                                    <span style="font-weight: 500;">
                                        {dayWiseOutVehicelCount.get("title")}:
                                    </span>
                                    <b style="color: #3a62ff;"> {dayWiseOutVehicelCount.get("data")}</b>
                                </td>
                            </tr>
                            <tr style="height: 30px">
                                <td style=" display: flex;justify-content:space-between;padding: 5px;font-size: 14px;">
                                    <span style="font-weight: 500;">
                                        {dayWiseGrnReceive.get("title")}:
                                    </span>
                                    <b style="color: #3a62ff;"> {dayWiseGrnReceive.get("data")}</b>
                                </td>
                            </tr>
                            <tr style="height: 30px">
                                <td style=" display: flex;justify-content:space-between;padding: 5px;font-size: 14px;">
                                    <span style="font-weight: 500;">
                                        {dayWiseGwelReceive.get("title")}:
                                    </span>
                                    <b style="color: #3a62ff;"> {dayWiseGwelReceive.get("data")}</b>
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>

            <div class="footertable" style="width:100%;margin-top:20px;">
                <div class="title" style="width: 100%; display: flex ; flex-direction:row; gap: 10px; height:50px ; align-items:center">
                    <p style="color: #3a62ff; font-size: 16px; margin: 5px; font-weight: 600;">
                        Daily Road Coal Logistic Report for {datetime.strptime(month_date,'%Y-%m-%d').strftime('%d %B %Y')}
                    </p>
                </div>
                        {per_data}
            </div>
            <br><br>
            <div class="footertable" style="width:100%;margin-top:20px;">
                <div class="title" style="width: 100%; display: flex ; flex-direction:row; gap: 10px; height:50px ; align-items:center">
                    <p style="color: #3a62ff; font-size: 16px; margin: 5px; font-weight: 600;">
                        Daily Rail Coal Logistic Report for {datetime.strptime(month_date,'%Y-%m-%d').strftime('%d %B %Y')}
                    </p>
                </div>
                        {per_rail_data}
            </div>
            <br><br>
            <div class="body" style="margin-top:20px;">
                <div style="color: #3a62ff; font-size: 16px; margin: 5px auto; font-weight: 600;">
                        Monthly - Mine v/s Average GWEL GCV v/s AOP Target
                        {gcv_bar_data}
                </div>  
            </div>
            <div style="display:flex; flex-wrap:wrap;">
                <div class="body" style="margin-top:20px; width:50% ; height: 300px;">
                    <div style="color: #3a62ff; font-size: 16px; margin: 5px auto; font-weight: 600;display: flex; justify-content:center; flex-direction:column;  height: 300px;">
                    <p style="margin:1px; margin-bottom:5px;">
                        Monthly - Mine v/s Transit Loss/Gain
                    </p>
                        {profit_loss_gmr}
                </div>  
            </div> 
            <div class="body" style="margin-top:20px; width:50% ; height: 300px;">
                <div style="color: #3a62ff; font-size: 16px; margin: 5px auto; font-weight: 600;display: flex; justify-content:center; flex-direction:column;  height: 300px;">
                    <p style="margin:1px; margin-bottom:5px;">
                        Monthly - Month v/s Transit Loss/Gain
                    </p>
                        {transist_data_month}
                </div>
            </div>
            <div class="body" style="margin-top:20px; width:50%  ; height: 300px;">
                <div style="color: #3a62ff; font-size: 16px; margin: 5px auto; font-weight: 600;display: flex; justify-content:center; flex-direction:column;  height: 300px;">
                    <p style="margin:1px; margin-bottom:5px;">
                        Annually - Road & Rail Mode v/s Transit Loss/Gain
                    </p>
                        {profit_loss_final}
                </div>
            </div>
            </div>
        </body>
        <footer style="font-family: arial, sans-serif; font-size: 8px;">This is an autogenerated report | GMR</footer>
        </html>
        """

        file = str(datetime.now().strftime("%d-%m-%Y"))
        store_data = os.path.join(os.getcwd(),"static_server", "gmr_ai", file)
        os.umask(0)
        os.makedirs(store_data, exist_ok=True, mode=0o777)

        image_name = datetime.now().strftime("%d-%m-%Y%H%M%S")

        # generating pdf based on above HTML
        HTML(string=html_template).write_pdf(
            f"{store_data}/daily_coal_logistic_report_{image_name}.pdf",
            stylesheets=[CSS(os.path.join(os.getcwd(), "helpers", "styleextra.css"))],
        )
        # normal pdf name
        pdf_file = f"{store_data}/daily_coal_logistic_report_{image_name}.pdf"
        # watermark pdf name
        output_file = f"{store_data}/daily_coal_logistic_report_wm_{image_name}.pdf"
        data = {}
        if template_data.get("logo_as_watermark") == "title":
            data["wm_angle"] = "vertical"
            apply_pdf_watermark(pdf_file, output_file, text_watermark=text_watermark_pdf, data=data)
            data_file = f"static_server/gmr_ai/{file}/daily_coal_logistic_report_wm_{image_name}.pdf"
            # deleting original generated pdf as we are getting here two pdf first without watermark and second with watermark
            os.remove(f"{store_data}/daily_coal_logistic_report_{image_name}.pdf")
        
        elif template_data.get("logo_as_watermark") == "logo":
            data["wm_angle"] = "vertical"
            apply_pdf_watermark(pdf_file, output_file, picture_path=f'{os.path.join(os.getcwd())}/{template_data["logo_path"]}', data=data)
            data_file = f"static_server/gmr_ai/{file}/daily_coal_logistic_report_wm_{image_name}.pdf"  
            # deleting original generated pdf as we are getting here two pdf first without watermark and second with watermark
            os.remove(f"{store_data}/daily_coal_logistic_report_{image_name}.pdf")
        
        elif template_data.get("logo_as_watermark") == "None":
            data_file = f"static_server/gmr_ai/{file}/daily_coal_logistic_report_{image_name}.pdf"

        console_logger.debug(data_file)

        # deleting a temp watermark pdf which was getting generated during watermark
        for filename in Path(f"{os.path.join(os.getcwd())}/static_server/gmr_ai/{file}").glob("watermark_empty*.pdf"):
            filename.unlink()
        
        #deleting reports_img folder on static_server
        shutil.rmtree(os.path.join(os.getcwd(),"static_server", "gmr_ai", "reports_img")) 
        # recreating folder with reports_img name
        os.mkdir(os.path.join(os.getcwd(),"static_server", "gmr_ai", "reports_img"))

        return data_file
    except Exception as e:
        console_logger.debug(e)
