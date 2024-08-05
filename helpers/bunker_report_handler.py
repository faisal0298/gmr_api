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

def demonstrt_table_report(fetchBunkerdata):
    try:
        count = 1
        html_output = ""
        for single_data in fetchBunkerdata:
            per_data = "<tr class='row21'>"
            per_data += f"<td class='column1 style26 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; font-weight: bold;color: #000000; font-family: Calibri;font-size: 12px; background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;'>{count}</td>"
            per_data += f"<td class='column2 style21 s' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px;background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{datetime.strptime(single_data['sample_received_date'], '%Y-%m-%dT%H:%M:%S').strftime('%d.%m.%Y')}</td>"
            per_data += "<td class='column3 style21 s' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px;background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>03.04.2024</td>"
            per_data += f"<td class='column4 style7 n' style='text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: Calibri;font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;'>{round(int(float(single_data['rR_Qty'])), 2)}</td>"
            per_data += f"<td class='column5 style21 s' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000;font-family: Calibri; font-size: 12px;background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{single_data['sample_desc']}</td>"
            per_data += "<td class='column6 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>25.2</td>"
            per_data += "<td class='column7 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>38</td>"
            
            if single_data['sample_parameters']:
                for under_single_data in single_data['sample_parameters']:
                    if under_single_data['parameter_name'] == "IM" and under_single_data["parameter_type"] == "AirDryBasis_IM":
                        per_data += f"<td class='column8 style8 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{round(int(float(under_single_data['val1'])), 2)}</td>"
                    # else:
                    #     per_data += f"<td class='column8 style8 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                    if under_single_data['parameter_name'] == "Ash" and under_single_data["parameter_type"] == "AirDryBasis_Ash":
                        per_data += f"<td class='column9 style8 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{round(int(float(under_single_data['val1'])), 2)}</td>"
                    # else:
                    #     per_data += f"<td class='column9 style8 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                    if under_single_data['parameter_name'] == "VM" and under_single_data["parameter_type"] == "AirDryBasis_VM":
                        per_data += f"<td class='column10 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{round(int(float(under_single_data['val1'])), 2)}</td>"
                    # else:
                    #     per_data += f"<td class='column10 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                    if under_single_data['parameter_name'] == "GCV" and under_single_data["parameter_type"] == "AirDryBasis_GCV":
                        per_data += f"<td class='column11 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000;font-family: Calibri; font-size: 12px;background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{round(int(float(under_single_data['val1'])), 2)}</td>"
                    # else:
                    #     per_data += f"<td class='column11 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000;font-family: Calibri; font-size: 12px;background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                    if under_single_data['parameter_name'] == "TM" and under_single_data["parameter_type"] == "ReceivedBasis_TM":
                        per_data += f"<td class='column12 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{round(int(float(under_single_data['val1'])), 2)}</td>"
                    # else:    
                    #     per_data += f"<td class='column12 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                    if under_single_data["parameter_name"] == "VM" and under_single_data["parameter_type"] == "ReceivedBasis_VM":
                        per_data += f"<td class='column13 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{round(int(float(under_single_data['val1'])), 2)}</td>"
                    # else:    
                    #     per_data += f"<td class='column13 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                    if under_single_data["parameter_name"] == "ASH" and under_single_data["parameter_type"] == "ReceivedBasis_ASH":
                        per_data += f"<td class='column14 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{round(int(float(under_single_data['val1'])), 2)}</td>"
                    # else:    
                    #     per_data += f"<td class='column14 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                    if under_single_data["parameter_name"] == "FC" and under_single_data["parameter_type"] == "ReceivedBasis_FC":
                        per_data += f"<td class='column15 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{round(int(float(under_single_data['val1'])), 2)}</td>"
                    # else:    
                    #     per_data += f"<td class='column15 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                    if under_single_data["parameter_name"] == "GCV" and under_single_data["parameter_type"] == "ReceivedBasis_GCV":
                        per_data += f"<td class='column16 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{round(int(float(under_single_data['val1'])), 2)}</td>"
                    # else:    
                    #     per_data += f"<td class='column16 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
            else:
                per_data += f"<td class='column8 style8 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                per_data += f"<td class='column9 style8 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                per_data += f"<td class='column10 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                per_data += f"<td class='column11 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000;font-family: Calibri; font-size: 12px;background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                per_data += f"<td class='column12 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                per_data += f"<td class='column13 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                per_data += f"<td class='column14 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                per_data += f"<td class='column14 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                per_data += f"<td class='column15 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                # per_data += f"<td class='column16 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
            per_data += "</tr>"
            html_output += per_data
            count += 1
        return html_output
    except Exception as e:
        console_logger.debug(e)

def demonstrate_table_bunker(fetchBunkerSingleData):
    try:
        console_logger.debug(fetchBunkerSingleData['sample_parameters'])
        per_data = "<tr class='row21'>"
        per_data += f"<td class='column1 style26 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; font-weight: bold;color: #000000; font-family: Calibri;font-size: 12px; background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;'>1</td>"
        per_data += f"<td class='column2 style21 s' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px;background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{datetime.strptime(fetchBunkerSingleData['sample_received_date'], '%Y-%m-%dT%H:%M:%S').strftime('%d.%m.%Y')}</td>"
        per_data += f"<td class='column3 style21 s' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px;background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{fetchBunkerSingleData['rR_Qty']}</td>"
        per_data += f"<td class='column4 style7 n' style='text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: Calibri;font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;'>{round(int(float(fetchBunkerSingleData['rR_Qty'])), 2)}</td>"
        per_data += f"<td class='column5 style21 s' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000;font-family: Calibri; font-size: 12px;background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{fetchBunkerSingleData['sample_desc']}</td>"
        per_data += f"<td class='column6 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{fetchBunkerSingleData['test_temp']}</td>"
        per_data += f"<td class='column7 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{fetchBunkerSingleData['humidity']}</td>"
        
        if fetchBunkerSingleData['sample_parameters']:
            for under_single_data in fetchBunkerSingleData['sample_parameters']:
                # if under_single_data['parameter_name'] == "IM" and under_single_data["parameter_type"] == "AirDryBasis_IM":
                if under_single_data["parameter_type"] == "AirDryBasis_IM":
                    per_data += f"<td class='column8 style8 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{round(int(float(under_single_data['val1'])), 2)}</td>"
                # else:
                #     per_data += f"<td class='column8 style8 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                # if under_single_data['parameter_name'] == "Ash" and under_single_data["parameter_type"] == "AirDryBasis_Ash":
                if under_single_data["parameter_type"] == "AirDryBasis_Ash":
                    per_data += f"<td class='column9 style8 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{round(int(float(under_single_data['val1'])), 2)}</td>"
                # else:
                #     per_data += f"<td class='column9 style8 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                # if under_single_data['parameter_name'] == "VM" and under_single_data["parameter_type"] == "AirDryBasis_VM":
                if under_single_data["parameter_type"] == "AirDryBasis_VM":
                    per_data += f"<td class='column10 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{round(int(float(under_single_data['val1'])), 2)}</td>"
                # else:
                #     per_data += f"<td class='column10 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                # if under_single_data['parameter_name'] == "GCV" and under_single_data["parameter_type"] == "AirDryBasis_GCV":
                if under_single_data["parameter_type"] == "AirDryBasis_GCV":
                    per_data += f"<td class='column11 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000;font-family: Calibri; font-size: 12px;background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{round(int(float(under_single_data['val1'])), 2)}</td>"
                # else:
                #     per_data += f"<td class='column11 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000;font-family: Calibri; font-size: 12px;background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                # if under_single_data['parameter_name'] == "TM" and under_single_data["parameter_type"] == "ReceivedBasis_TM":
                if under_single_data["parameter_type"] == "ReceivedBasis_TM":
                    per_data += f"<td class='column12 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{round(int(float(under_single_data['val1'])), 2)}</td>"
                # else:    
                #     per_data += f"<td class='column12 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                # if under_single_data["parameter_name"] == "VM" and under_single_data["parameter_type"] == "ReceivedBasis_VM":
                if under_single_data["parameter_type"] == "ReceivedBasis_VM":
                    per_data += f"<td class='column13 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{round(int(float(under_single_data['val1'])), 2)}</td>"
                # else:    
                #     per_data += f"<td class='column13 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                # if under_single_data["parameter_name"] == "Ash" and under_single_data["parameter_type"] == "ReceivedBasis_ASH":
                if under_single_data["parameter_type"] == "ReceivedBasis_ASH":
                    per_data += f"<td class='column14 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{round(int(float(under_single_data['val1'])), 2)}</td>"
                # else:    
                #     per_data += f"<td class='column14 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                # if under_single_data["parameter_name"] == "FC" and under_single_data["parameter_type"] == "ReceivedBasis_FC":
                if under_single_data["parameter_type"] == "ReceivedBasis_FC":
                    per_data += f"<td class='column15 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{round(int(float(under_single_data['val1'])), 2)}</td>"
                # else:    
                #     per_data += f"<td class='column15 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
                # if under_single_data["parameter_name"] == "GCV" and under_single_data["parameter_type"] == "ReceivedBasis_GCV":
                if under_single_data["parameter_type"] == "ReceivedBasis_GCV":
                    per_data += f"<td class='column16 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>{round(int(float(under_single_data['val1'])), 2)}</td>"
                # else:    
                #     per_data += f"<td class='column16 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
        else:
            per_data += f"<td class='column8 style8 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
            per_data += f"<td class='column9 style8 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
            per_data += f"<td class='column10 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
            per_data += f"<td class='column11 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000;font-family: Calibri; font-size: 12px;background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
            per_data += f"<td class='column12 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
            per_data += f"<td class='column13 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
            per_data += f"<td class='column14 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
            per_data += f"<td class='column14 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
            per_data += f"<td class='column15 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
            # per_data += f"<td class='column16 style21 n' style='text-align: center; border: 1px dotted black; vertical-align: middle; color: #000000; font-family: Calibri; font-size: 12px; background-color: #FFFFFF; border-bottom: 1px solid #000000 !important; border-top: 1px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 1px solid #000000 !important;'>-</td>"
        per_data += "</tr>"
        return per_data
    except Exception as e:
        console_logger.debug(e)

def bunker_single_generate_report(fetchBunkerSingleData):
    try:
        # console_logger.debug(fetchBunkerSingleData)

        singlebunkerData = demonstrate_table_bunker(fetchBunkerSingleData)

        console_logger.debug(f"{os.path.join(os.getcwd())}/static_server/receipt/report_logo.png")
        console_logger.debug(f"{os.path.join(os.getcwd())}/static_server/receipt/logo_certificate.png")

        gmr_logo = encoded_data(f"{os.path.join(os.getcwd())}/static_server/receipt/report_logo.png")
        logo_certificate = encoded_data(f"{os.path.join(os.getcwd())}/static_server/receipt/logo_certificate.png")

        title = f"GMR Bunker performance report"
        html_template = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>{title}</title>
        </head>
        <body>
            <table cellpadding="0" cellspacing="0" id="sheet0" class="sheet0 gridlines" style="width: 100% !important; border-collapse:collapse; page-break-after:always;">
                <tbody>
                <tr class="row1" style="height:20px;">
                    <td class="column1 style75 s style77" colspan="16" style="border: 1px dotted black;text-align: center;vertical-align: middle;border-bottom: none #000000;font-weight: bold;color: #000000; font-family: 'Calibri';font-size: 14px; background-color: #FFFFFF; border-top: 2px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 2px solid #000000 !important;">
                        <div style="position: relative;">
                            <img style="position: absolute; z-index: 1; left: 10px; top: 6px; width: 50px; height: 50px;" src="data:image/png;base64,{logo_certificate}" border="0">
                            <img style="position: absolute; z-index: 1; right: 25px; top: 10px; width: 62px; height: 30px;" src="data:image/png;base64,{gmr_logo}" border="0">
                        </div>
                    GWEL Laboartory</td>
                </tr>
                <tr class="row2" style="height:19px;">
                    <td class="column1 style75 s style77" colspan="16" style="border: 1px dotted black; text-align: center; vertical-align: middle; border-bottom: none #000000; font-weight: bold; color: #000000; font-family: 'Calibri';font-size: 14px; background-color: #FFFFFF; border-top: 2px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 2px solid #000000 !important;">GMR Warora Energy Limited</td>
                </tr>
                <tr class="row3" style="height:18px;">
                    <td class="column1 style78 s style80" colspan="16" style="border: 1px dotted black;text-align: center;vertical-align: middle;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 14px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 2px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">B1, Mohabala MIDC Growth Centre,Warora, Chandrapur, 442 907</td>
                </tr>
                <tr class="row4" style="height:18px;">
                    <td class="column1 style81 s style80" colspan="16" style="border: 1px dotted black;text-align: center;vertical-align: middle;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 14px;background-color: #FFFFFF;border-right: 2px solid #000000 !important;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;">Fired Coal Analysis Certificate</td>
                </tr>
                <tr class="row5" style="height:15px;">
                    <td class="column1 style69 s style71" colspan="7" style="border: 1px dotted black;text-align: left;vertical-align: top;padding-left: 0px;border-right: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-left: 2px solid #000000 !important;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;">ULR : {fetchBunkerSingleData['ulr_no']}<br></td>
                    <td class="column8 style72 s style74" colspan="9" style="border: 1px dotted black;text-align: center;vertical-align: middle;border-right: 2px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-left: 1px solid #000000 !important;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;">QMR/FR/FCAC, Issue:Rev no: 01:06, Issue: Rev Date:01.01.16:01.11.20</td>
                </tr>
                <tr class="row6" style="height:15px;">
                    <td class="column1 style85 s style86" colspan="4" style="border: 1px dotted black;text-align: left;vertical-align: middle;padding-left: 0px;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Certificate no: {fetchBunkerSingleData['test_report_no']}</td>
                    <td class="column5 style87 s style87" colspan="9" style="border: 1px dotted black;text-align: left;vertical-align: middle;padding-left: 0px;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Name of Customer : Operation, GWEL</td>
                    <td class="column15 style87 s style88" colspan="3" style="border: 1px dotted black;text-align: left;vertical-align: middle;padding-left: 0px;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">Date : {fetchBunkerSingleData['test_report_date']}</td>
                </tr>
                <tr class="row7" style="height:15px;">
                    <td class="column1 style89 s style91" colspan="16" style="border: 1px dotted black;text-align: left;vertical-align: middle;padding-left: 0px;border-right: 2px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-left: 2px solid #000000 !important;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;">Method Reference:IS 1350 (Part I) : 1984 (Proximate Analysis) &amp; IS 1350 (Part II) : 2017 (GCV analysis)</td>
                </tr>
                <tr class="row8" style="height:19px">
                    <td class="column1 style92 s style94" colspan="7" style="border: 1px dotted black;text-align: center;vertical-align: bottom;border-right: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-left: 2px solid #000000 !important;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;">Sample details</td>
                    <td class="column8 style95 s style94" colspan="4" style="border: 1px dotted black;text-align: center;vertical-align: bottom;border-left: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-right: 1px solid #000000 !important;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;">Air Dry Basis (ADB)</td>
                    <td class="column12 style96 s style97" colspan="5" style="border: 1px dotted black;text-align: center;vertical-align: middle;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">As Received Basis </td>
                </tr>
                <tr class="row9" style="height:27px">
                    <td class="column1 style59 s style60" rowspan="2" style="border: 1px dotted black;text-align: center;vertical-align: middle;border-bottom: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">&nbsp;&nbsp;Sr.No.</td>
                    <td class="column2 style61 s style62" rowspan="2" style="border: 1px dotted black;text-align: center;vertical-align: middle;border-bottom: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Sample Date<span style="font-weight:bold; color:#000000; font-family:'Calibri'; font-size:11pt"><sup>#</sup></span></td>
                    <td class="column3 style61 s style62" rowspan="2" style="border: 1px dotted black;text-align: center;vertical-align: middle;border-bottom: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Analysis Date</td>
                    <td class="column4 style61 s style62" rowspan="2" style="border: 1px dotted black;text-align: center;vertical-align: middle;border-bottom: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Bunkered Qunatity MT<span style="font-weight:bold; color:#000000; font-family:'Calibri'; font-size:11pt"><sup>#</sup></span></td>
                    <td class="column5 style61 s style62" rowspan="2" style="border: 1px dotted black;text-align: center;vertical-align: middle;border-bottom: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Sample Name<span style="font-weight:bold; color:#000000; font-family:'Calibri'; font-size:11pt"><sup>#</sup></span></td>
                    <td class="column6 style61 s style62" rowspan="2" style="border: 1px dotted black;text-align: center;vertical-align: middle;border-bottom: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Lab temp <span style="font-weight:bold; color:#000000; font-family:'Calibri'; font-size:11pt">⁰C</span></td>
                    <td class="column7 style61 s style62" rowspan="2" style="border: 1px dotted black;text-align: center;vertical-align: middle;border-bottom: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Lab RH %</td>
                    <td class="column8 style3 s" style="border: 1px dotted black;text-align: center;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">IM  </td>
                    <td class="column9 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Ash</td>
                    <td class="column10 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">VM </td>
                    <td class="column11 style5 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">GCV </td>
                    <td class="column12 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">TM </td>
                    <td class="column13 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">VM </td>
                    <td class="column14 style5 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">ASH </td>
                    <td class="column15 style5 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">FC </td>
                    <td class="column16 style4 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">GCV </td>
                </tr>
                <tr class="row10" style="height:15px;">
                    <td class="column8 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">%</td>
                    <td class="column9 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">%</td>
                    <td class="column10 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">%</td>
                    <td class="column11 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">&nbsp;cal/gm</td>
                    <td class="column12 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">%</td>
                    <td class="column13 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">%</td>
                    <td class="column14 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">%</td>
                    <td class="column15 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">%</td>
                    <td class="column16 style6 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">&nbsp;cal/gm</td>
                </tr>
                <tr class="row20">
                    <td class="column1 style66 s style68" colspan="16" style="text-align: center;border: 1px dotted black;vertical-align: middle;border-bottom: none #000000;border-right: 2px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-left: 2px solid #000000 !important;border-top: 1px solid #000000 !important;">Unit #2</td>
                    <td class="column17 style17 null" style="border: 1px dotted black;vertical-align: bottom;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
                    <td class="column18 style24 null" style="border: 1px dotted black;vertical-align: bottom;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
                </tr>
                
                {singlebunkerData}


                <tr class="row29" style="height:29.25pt;">
                    <td class="column1 style56 s style58" colspan="16" style="text-align: left;border: 1px dotted black;vertical-align: middle;padding-left: 0px;border-bottom: none #000000;border-right: 2px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-left: 2px solid #000000 !important;border-top: 1px solid #000000 !important;">Abbreviation: TM- Total moisture, IM- Inherent moisture, VM- Volatile matter, FC-Fixed carbon, GCV-Gross calorific value, Temp- Temperature, RH- Relative humidity, ADB- Air dry basis, ARB- As received basis,</td>
                </tr>
                <tr class="row30" style="height:15.75pt;">
                    <td class="column1 style9 null" style="border: 1px dotted black;vertical-align: middle;text-align: left;padding-left: 0px;border-bottom: none #000000;border-top: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-left: 2px solid #000000 !important;"></td>
                    <td class="column2 style29 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: white;"></td>
                    <td class="column3 style29 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: white;"></td>
                    <td class="column4 style30 null" style="border: 1px dotted black;vertical-align: bottom;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
                    <td class="column5 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
                    <td class="column6 style32 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
                    <td class="column7 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
                    <td class="column8 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
                    <td class="column9 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
                    <td class="column10 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
                    <td class="column11 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
                    <td class="column12 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
                    <td class="column13 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
                    <td class="column14 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
                    <td class="column15 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
                    <td class="column16 style10 null" style="border: 1px dotted black;vertical-align: bottom;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: white;border-right: 2px solid #000000 !important;"></td>
                </tr>
                <tr class="row31">
                    <td class="column1 style35 s style41" colspan="16" rowspan="4" style="text-align: left;border: 1px dotted black;vertical-align: middle;padding-left: 0px;border-bottom: 2px solid #000000 !important;border-top: none #000000;border-right: 2px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-left: 2px solid #000000 !important;">
                    <div style="display: flex; justify-content: space-between; width: 100%; text-align: center; margin-top: 50px; margin-bottom: 10px;">
                        <div>Analysed By <p style="margin: 0;">Name &amp; Designation</p></div>
                        <div>Review By <p style="margin: 0;">Name &amp; Designation</p></div>
                        <div>Approved By <p style="margin: 0;">Name &amp; Designation</p></div>
                    </div>
                </td></tr>
                <tr class="row32">
                    <!-- <td class="column0">&nbsp;</td>
                    <td class="column17">&nbsp;</td>
                    <td class="column18">&nbsp;</td>
                    <td class="column19">&nbsp;</td> -->
                </tr>
                <tr class="row33">
                    <!-- <td class="column0">&nbsp;</td>
                    <td class="column17">&nbsp;</td>
                    <td class="column18">&nbsp;</td>
                    <td class="column19">&nbsp;</td> -->
                </tr>
                <tr class="row34" style="height:15px">
                    <!-- <td class="column0">&nbsp;</td>
                    <td class="column17">&nbsp;</td>
                    <td class="column18">&nbsp;</td>
                    <td class="column19">&nbsp;</td> -->
                </tr>
                <tr class="row35">
                    <td class="column1 style42 s style44" colspan="16" style="text-align: left;border: 1px dotted black;vertical-align: bottom;padding-left: 0px;border-right: 2px solid #000000 !important;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-left: 2px solid #000000 !important;border-bottom: 1px solid #000000 !important;border-top: 2px solid #000000 !important;">Note :</td>
                </tr>
                <tr class="row36">
                    <td class="column1 style11 n" style="text-align: right;border: 1px dotted black;vertical-align: top;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">1&nbsp;</td>
                    <td class="column2 style48 s style49" colspan="15" style="text-align: left;border: 1px dotted black;vertical-align: bottom;padding-left: 0px;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">The result listed refers only to the tested sample and applicable parameter(s)</td>
                </tr>
                <tr class="row37" style="height:15px;">
                    <td class="column1 style11 n" style="text-align: right;border: 1px dotted black;vertical-align: top;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">2&nbsp;</td>
                    <td class="column2 style50 s style51" colspan="15" style="text-align: left;border: 1px dotted black;vertical-align: bottom;padding-left: 0px;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">This report is not to be reproduced except in full, without written approval of the GWEL Laboratory</td>
                </tr>
                <tr class="row38" style="height:15px;">
                    <td class="column1 style11 n" style="text-align: right;border: 1px dotted black;vertical-align: top;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">3&nbsp;</td>
                    <td class="column2 style52 s style53" colspan="15" style="text-align: left;border: 1px dotted black;vertical-align: bottom;padding-left: 0px;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">Remaining sample after test will be retained for 7 days from release of test certificate for external samples and others as per QTP guidelines.</td>
                </tr>
                <tr class="row39">
                    <td class="column1 style12 n" style="text-align: right;border: 1px dotted black;vertical-align: bottom;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">4&nbsp;</td>
                    <td class="column2 style54 s style55" colspan="15" style="text-align: left;border: 1px dotted black;vertical-align: bottom;padding-left: 0px;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">Reported GCV values are without acid correction.</td>
                </tr>
                <tr class="row40">
                    <td class="column1 style12 n" style="text-align: right;border: 1px dotted black;vertical-align: bottom;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">5&nbsp;</td>
                    <td class="column2 style48 s style49" colspan="15" style="text-align: left;border: 1px dotted black;vertical-align: bottom;padding-left: 0px;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">&nbsp;Sampling not carried out by laboratory.</td>
                </tr>
                <tr class="row41">
                    <td class="column1 style13 n" style="text-align: right;border: 1px dotted black;vertical-align: bottom;border-bottom: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">6&nbsp;</td>
                    <td class="column2 style14 s style49" colspan="15" style="text-align: left;border: 1px dotted black;vertical-align: bottom;padding-left: 0px;border-bottom: 1px solid #000000 !important;border-right: 2px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;">Details of the sample provided by the customer marked with &quot;#&quot;</td>
                </tr>
                <tr class="row42">
                    <td class="column1" colspan="16" style="border: 1px dotted black;">End of report</td>
                </tr>
                </tbody>
            </table>
        </body>
        </html>
        """

        file = str(datetime.now().strftime("%d-%m-%Y"))
        store_data = os.path.join(os.getcwd(),"static_server", "gmr_ai", file)
        os.umask(0)
        os.makedirs(store_data, exist_ok=True, mode=0o777)

        pdf_name = datetime.now().strftime("%d-%m-%Y%H%M%S")
        file_name_data = fetchBunkerSingleData['test_report_no'].replace("/", "_")

        # generating pdf based on above HTML
        HTML(string=html_template).write_pdf(
            f"{store_data}/{file_name_data}_{pdf_name}.pdf",
            stylesheets=[CSS(os.path.join(os.getcwd(), "helpers", "styleextrabunker.css"))],
        ) 
        return {"data": f"static_server/gmr_ai/{file}/{file_name_data}_{pdf_name}.pdf"}
    except Exception as e:
        console_logger.debug(e)

def bunker_generate_report(fetchBunkerdata):

    # console_logger.debug("hiiii")
    console_logger.debug(fetchBunkerdata)

    # fetchDemonstart = demonstrt_table_report(fetchBunkerdata)

    # console_logger.debug(f"{os.path.join(os.getcwd())}/static_server/receipt/report_logo.jpeg")
    # console_logger.debug(f"{os.path.join(os.getcwd())}/static_server/receipt/logo_certificate.png")

    # gmr_logo = encoded_data(f"{os.path.join(os.getcwd())}/static_server/receipt/report_logo.jpeg")
    # logo_certificate = encoded_data(f"{os.path.join(os.getcwd())}/static_server/receipt/logo_certificate.png")

    # title = f"GMR Bunker performance report"
    # html_template = f"""
    # <!DOCTYPE html>
    # <html>
    # <head>
    #     <title>{title}</title>
    # </head>
    # <body>
    #     <table cellpadding="0" cellspacing="0" id="sheet0" class="sheet0 gridlines" style="width: 50% !important; border-collapse:collapse; page-break-after:always;">
    #         <tbody>
    #         <tr class="row1" style="height:20px;">
    #             <td class="column1 style75 s style77" colspan="16" style="border: 1px dotted black;text-align: center;vertical-align: middle;border-bottom: none #000000;font-weight: bold;color: #000000; font-family: 'Calibri';font-size: 14px; background-color: #FFFFFF; border-top: 2px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 2px solid #000000 !important;">
    #                 <div style="position: relative;">
    #                     <img style="position: absolute; z-index: 1; left: 35px; top: 6px; width: 62px; height: 60px;" src="data:image/png;base64,{logo_certificate}" border="0">
    #                     <img style="position: absolute; z-index: 1; right: 35px; top: 10px; width: 62px; height: 35px;" src="data:image/png;base64,{gmr_logo}" border="0">
    #                 </div>
    #             GWEL Laboartory</td>
    #         </tr>
    #         <tr class="row2" style="height:19px;">
    #             <td class="column1 style75 s style77" colspan="16" style="border: 1px dotted black; text-align: center; vertical-align: middle; border-bottom: none #000000; font-weight: bold; color: #000000; font-family: 'Calibri';font-size: 14px; background-color: #FFFFFF; border-top: 2px solid #000000 !important; border-left: 1px solid #000000 !important; border-right: 2px solid #000000 !important;">GMR Warora Energy Limited</td>
    #         </tr>
    #         <tr class="row3" style="height:18px;">
    #             <td class="column1 style78 s style80" colspan="16" style="border: 1px dotted black;text-align: center;vertical-align: middle;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 14px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 2px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">B1, Mohabala MIDC Growth Centre,Warora, Chandrapur, 442 907</td>
    #         </tr>
    #         <tr class="row4" style="height:18px;">
    #             <td class="column1 style81 s style80" colspan="16" style="border: 1px dotted black;text-align: center;vertical-align: middle;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 14px;background-color: #FFFFFF;border-right: 2px solid #000000 !important;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;">Fired Coal Analysis Certificate</td>
    #         </tr>
    #         <tr class="row5" style="height:15px;">
    #             <td class="column1 style69 s style71" colspan="7" style="border: 1px dotted black;text-align: left;vertical-align: top;padding-left: 0px;border-right: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-left: 2px solid #000000 !important;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;">ULR : TC7785240000001250F<br></td>
    #             <td class="column8 style72 s style74" colspan="9" style="border: 1px dotted black;text-align: center;vertical-align: middle;border-right: 2px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-left: 1px solid #000000 !important;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;">QMR/FR/FCAC, Issue:Rev no: 01:06, Issue: Rev Date:01.01.16:01.11.20</td>
    #         </tr>
    #         <tr class="row6" style="height:15px;">
    #             <td class="column1 style85 s style86" colspan="4" style="border: 1px dotted black;text-align: left;vertical-align: middle;padding-left: 0px;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Certificate no: GWELL/BK/APR24/01</td>
    #             <td class="column5 style87 s style87" colspan="9" style="border: 1px dotted black;text-align: left;vertical-align: middle;padding-left: 0px;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Name of Customer : Operation, GWEL</td>
    #             <td class="column15 style87 s style88" colspan="3" style="border: 1px dotted black;text-align: left;vertical-align: middle;padding-left: 0px;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">Date : 10.04.2024</td>
    #         </tr>
    #         <tr class="row7" style="height:15px;">
    #             <td class="column1 style89 s style91" colspan="16" style="border: 1px dotted black;text-align: left;vertical-align: middle;padding-left: 0px;border-right: 2px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-left: 2px solid #000000 !important;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;">Method Reference:IS 1350 (Part I) : 1984 (Proximate Analysis) &amp; IS 1350 (Part II) : 2017 (GCV analysis)</td>
    #         </tr>
    #         <tr class="row8" style="height:19px">
    #             <td class="column1 style92 s style94" colspan="7" style="border: 1px dotted black;text-align: center;vertical-align: bottom;border-right: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-left: 2px solid #000000 !important;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;">Sample details</td>
    #             <td class="column8 style95 s style94" colspan="4" style="border: 1px dotted black;text-align: center;vertical-align: bottom;border-left: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-right: 1px solid #000000 !important;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;">Air Dry Basis (ADB)</td>
    #             <td class="column12 style96 s style97" colspan="5" style="border: 1px dotted black;text-align: center;vertical-align: middle;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">As Received Basis </td>
    #         </tr>
    #         <tr class="row9" style="height:27px">
    #             <td class="column1 style59 s style60" rowspan="2" style="border: 1px dotted black;text-align: center;vertical-align: middle;border-bottom: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">&nbsp;&nbsp;Sr.No.</td>
    #             <td class="column2 style61 s style62" rowspan="2" style="border: 1px dotted black;text-align: center;vertical-align: middle;border-bottom: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Sample Date<span style="font-weight:bold; color:#000000; font-family:'Calibri'; font-size:11pt"><sup>#</sup></span></td>
    #             <td class="column3 style61 s style62" rowspan="2" style="border: 1px dotted black;text-align: center;vertical-align: middle;border-bottom: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Analysis Date</td>
    #             <td class="column4 style61 s style62" rowspan="2" style="border: 1px dotted black;text-align: center;vertical-align: middle;border-bottom: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Bunkered Qunatity MT<span style="font-weight:bold; color:#000000; font-family:'Calibri'; font-size:11pt"><sup>#</sup></span></td>
    #             <td class="column5 style61 s style62" rowspan="2" style="border: 1px dotted black;text-align: center;vertical-align: middle;border-bottom: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Sample Name<span style="font-weight:bold; color:#000000; font-family:'Calibri'; font-size:11pt"><sup>#</sup></span></td>
    #             <td class="column6 style61 s style62" rowspan="2" style="border: 1px dotted black;text-align: center;vertical-align: middle;border-bottom: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Lab temp <span style="font-weight:bold; color:#000000; font-family:'Calibri'; font-size:11pt">⁰C</span></td>
    #             <td class="column7 style61 s style62" rowspan="2" style="border: 1px dotted black;text-align: center;vertical-align: middle;border-bottom: 1px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Lab RH %</td>
    #             <td class="column8 style3 s" style="border: 1px dotted black;text-align: center;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">IM  </td>
    #             <td class="column9 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Ash</td>
    #             <td class="column10 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">VM </td>
    #             <td class="column11 style5 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">GCV </td>
    #             <td class="column12 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">TM </td>
    #             <td class="column13 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">VM </td>
    #             <td class="column14 style5 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">ASH </td>
    #             <td class="column15 style5 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">FC </td>
    #             <td class="column16 style4 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">GCV </td>
    #         </tr>
    #         <tr class="row10" style="height:15px;">
    #             <td class="column8 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">%</td>
    #             <td class="column9 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">%</td>
    #             <td class="column10 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">%</td>
    #             <td class="column11 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">&nbsp;cal/gm</td>
    #             <td class="column12 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">%</td>
    #             <td class="column13 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">%</td>
    #             <td class="column14 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">%</td>
    #             <td class="column15 style3 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">%</td>
    #             <td class="column16 style6 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">&nbsp;cal/gm</td>
    #         </tr>
    #         <tr class="row11">
    #             <td class="column1 style63 s style65" colspan="16" style="text-align: center;border: 1px dotted black;vertical-align: middle;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">Unit # 1</td>
    #         </tr>
    #         <tr class="row12">

    #             <td class="column1 style26 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">1</td>
    #             <td class="column2 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">01.04.2024</td>
    #             <td class="column3 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">03.04.2024</td>
    #             <td class="column4 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4574.4</td>
    #             <td class="column5 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Bunker</td>
    #             <td class="column6 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">25.2</td>
    #             <td class="column7 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">38</td>
    #             <td class="column8 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">3.34</td>
    #             <td class="column9 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">41.22</td>
    #             <td class="column10 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">23.78</td>
    #             <td class="column11 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4037</td>
    #             <td class="column12 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">13.82</td>
    #             <td class="column13 style8 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">21.20</td>
    #             <td class="column14 style8 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">36.75</td>
    #             <td class="column15 style8 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">28.22</td>
    #             <td class="column16 style23 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">3599</td>
    #             <td class="column17 style17 null" style="border: 1px dotted black;vertical-align: bottom;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #         </tr>
    #         <tr class="row13">
    #             <td class="column1 style26 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px; background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">2</td>
    #             <td class="column2 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">02.04.2024</td>
    #             <td class="column3 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">04.04.2024</td>
    #             <td class="column4 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4280.5</td>
    #             <td class="column5 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Bunker</td>
    #             <td class="column6 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">25.6</td>
    #             <td class="column7 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">38</td>
    #             <td class="column8 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4.18</td>
    #             <td class="column9 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">37.53</td>
    #             <td class="column10 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">24.90</td>
    #             <td class="column11 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4280</td>
    #             <td class="column12 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">13.80</td>
    #             <td class="column13 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">22.4</td>
    #             <td class="column14 style8 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">33.76</td>
    #             <td class="column15 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">30.0</td>
    #             <td class="column16 style23 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">3850</td>
    #             <td class="column17 style17 null" style="border: 1px dotted black;vertical-align: bottom;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #         </tr>
    #         <tr class="row14">
    #             <td class="column1 style26 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">3</td>
    #             <td class="column2 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">03.04.2024</td>
    #             <td class="column3 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">05.04.2024</td>
    #             <td class="column4 style7 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4191.0</td>
    #             <td class="column5 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Bunker</td>
    #             <td class="column6 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">25.3</td>
    #             <td class="column7 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">39</td>
    #             <td class="column8 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4.87</td>
    #             <td class="column9 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">32.27</td>
    #             <td class="column10 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">26.59</td>
    #             <td class="column11 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4663</td>
    #             <td class="column12 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">19.37</td>
    #             <td class="column13 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">22.5</td>
    #             <td class="column14 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">27.3</td>
    #             <td class="column15 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">30.7</td>
    #             <td class="column16 style23 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">3952</td>
    #             <td class="column17 style20 null" style="border: 1px dotted black;vertical-align: bottom;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #         </tr>
    #         <tr class="row15">
    #             <td class="column1 style26 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">4</td>
    #             <td class="column2 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">04.04.2024</td>
    #             <td class="column3 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">06.04.2024</td>
    #             <td class="column4 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4165.9</td>
    #             <td class="column5 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Bunker</td>
    #             <td class="column6 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">25.1</td>
    #             <td class="column7 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">37</td>
    #             <td class="column8 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4.82</td>
    #             <td class="column9 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">33.71</td>
    #             <td class="column10 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">25.98</td>
    #             <td class="column11 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4597</td>
    #             <td class="column12 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">17.62</td>
    #             <td class="column13 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">22.5</td>
    #             <td class="column14 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">29.2</td>
    #             <td class="column15 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">30.7</td>
    #             <td class="column16 style23 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">3979</td>
    #             <td class="column17 style17 null" style="border: 1px dotted black;vertical-align: bottom;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #         </tr>
    #         <tr class="row16">
    #             <td class="column1 style26 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">5</td>
    #             <td class="column2 style22 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">05.04.2024</td>
    #             <td class="column3 style22 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">07.04.2024</td>
    #             <td class="column4 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4261.9</td>
    #             <td class="column5 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Bunker</td>
    #             <td class="column6 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">25.2</td>
    #             <td class="column7 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">38</td>
    #             <td class="column8 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">3.58</td>
    #             <td class="column9 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">37.86</td>
    #             <td class="column10 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">24.72</td>
    #             <td class="column11 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4316</td>
    #             <td class="column12 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">14.05</td>
    #             <td class="column13 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">22.0</td>
    #             <td class="column14 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">33.8</td>
    #             <td class="column15 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">30.2</td>
    #             <td class="column16 style23 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">3848</td>
    #             <td class="column17 style17 null" style="border: 1px dotted black;vertical-align: bottom;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #             <td class="column18 style24 null" style="border: 1px dotted black;vertical-align: bottom;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #         </tr>
    #         <tr class="row17">
    #             <td class="column1 style34 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: white;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">6</td>
    #             <td class="column2 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">06.04.2024</td>
    #             <td class="column3 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">08.04.2024</td>
    #             <td class="column4 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4156.8</td>
    #             <td class="column5 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Bunker</td>
    #             <td class="column6 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">24.5</td>
    #             <td class="column7 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">36</td>
    #             <td class="column8 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">3.50</td>
    #             <td class="column9 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">36.50</td>
    #             <td class="column10 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">24.65</td>
    #             <td class="column11 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4434</td>
    #             <td class="column12 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">15.90</td>
    #             <td class="column13 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">21.5</td>
    #             <td class="column14 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">31.8</td>
    #             <td class="column15 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">30.8</td>
    #             <td class="column16 style23 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">3864</td>
    #             <td class="column17 style17 null" style="border: 1px dotted black;vertical-align: bottom;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #             <td class="column18 style24 null" style="border: 1px dotted black;vertical-align: bottom;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #         </tr>
    #         <tr class="row18">
    #             <td class="column1 style26 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">7</td>
    #             <td class="column2 style22 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">07.04.2024</td>
    #             <td class="column3 style22 s" style="text-align: center;border: 1px dotted black;vertical-align: bottom;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">09.04.2024</td>
    #             <td class="column4 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4239.5</td>
    #             <td class="column5 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Bunker</td>
    #             <td class="column6 style7 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">24.8</td>
    #             <td class="column7 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">36</td>
    #             <td class="column8 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">3.81</td>
    #             <td class="column9 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">37.76</td>
    #             <td class="column10 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">24.61</td>
    #             <td class="column11 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4275</td>
    #             <td class="column12 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">15.40</td>
    #             <td class="column13 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">21.6</td>
    #             <td class="column14 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">33.2</td>
    #             <td class="column15 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">29.7</td>
    #             <td class="column16 style23 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">3760</td>
    #             <td class="column17 style17 null" style="border: 1px dotted black;vertical-align: bottom;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #             <td class="column18 style24 null" style="border: 1px dotted black;vertical-align: bottom;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #         </tr>
    #         <tr class="row19">
    #             <td class="column1 style26 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">8</td>
    #             <td class="column2 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">08.04.2024</td>
    #             <td class="column3 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">10.04.2024</td>
    #             <td class="column4 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4246.1</td>
    #             <td class="column5 style21 s" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">Bunker</td>
    #             <td class="column6 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">26.8</td>
    #             <td class="column7 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">37</td>
    #             <td class="column8 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">6.32</td>
    #             <td class="column9 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">37.05</td>
    #             <td class="column10 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">24.68</td>
    #             <td class="column11 style21 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">4223</td>
    #             <td class="column12 style8 n" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">17.46</td>
    #             <td class="column13 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">21.7</td>
    #             <td class="column14 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">32.6</td>
    #             <td class="column15 style7 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 1px solid #000000 !important;">28.2</td>
    #             <td class="column16 style23 f" style="text-align: center;border: 1px dotted black;vertical-align: middle;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">3721</td>
    #             <td class="column17 style17 null" style="border: 1px dotted black;vertical-align: bottom;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #             <td class="column18 style24 null" style="border: 1px dotted black;vertical-align: bottom;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #         </tr>
    #         <tr class="row20">
    #             <td class="column1 style66 s style68" colspan="16" style="text-align: center;border: 1px dotted black;vertical-align: middle;border-bottom: none #000000;border-right: 2px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-left: 2px solid #000000 !important;border-top: 1px solid #000000 !important;">Unit #2</td>
    #             <td class="column17 style17 null" style="border: 1px dotted black;vertical-align: bottom;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #             <td class="column18 style24 null" style="border: 1px dotted black;vertical-align: bottom;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #         </tr>
            
    #             {fetchDemonstart}


    #         <tr class="row29" style="height:29.25pt;">
    #             <td class="column1 style56 s style58" colspan="16" style="text-align: left;border: 1px dotted black;vertical-align: middle;padding-left: 0px;border-bottom: none #000000;border-right: 2px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-left: 2px solid #000000 !important;border-top: 1px solid #000000 !important;">Abbreviation: TM- Total moisture, IM- Inherent moisture, VM- Volatile matter, FC-Fixed carbon, GCV-Gross calorific value, Temp- Temperature, RH- Relative humidity, ADB- Air dry basis, ARB- As received basis,</td>
    #         </tr>
    #         <tr class="row30" style="height:15.75pt;">
    #             <td class="column1 style9 null" style="border: 1px dotted black;vertical-align: middle;text-align: left;padding-left: 0px;border-bottom: none #000000;border-top: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-left: 2px solid #000000 !important;"></td>
    #             <td class="column2 style29 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: white;"></td>
    #             <td class="column3 style29 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: white;"></td>
    #             <td class="column4 style30 null" style="border: 1px dotted black;vertical-align: bottom;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #             <td class="column5 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #             <td class="column6 style32 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #             <td class="column7 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #             <td class="column8 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #             <td class="column9 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #             <td class="column10 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #             <td class="column11 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #             <td class="column12 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #             <td class="column13 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #             <td class="column14 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #             <td class="column15 style31 null" style="border: 1px dotted black;vertical-align: middle;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;border-right: none #000000;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;"></td>
    #             <td class="column16 style10 null" style="border: 1px dotted black;vertical-align: bottom;text-align: center;border-bottom: none #000000;border-top: none #000000;border-left: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: white;border-right: 2px solid #000000 !important;"></td>
    #         </tr>
    #         <tr class="row31">
    #             <td class="column1 style35 s style41" colspan="16" rowspan="4" style="text-align: left;border: 1px dotted black;vertical-align: middle;padding-left: 0px;border-bottom: 2px solid #000000 !important;border-top: none #000000;border-right: 2px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-left: 2px solid #000000 !important;">
    #             <div style="display: flex; justify-content: space-between; width: 100%; text-align: center; margin-top: 50px; margin-bottom: 10px;">
    #                 <div>Analysed By <p style="margin: 0;">Name &amp; Designation</p></div>
    #                 <div>Review By <p style="margin: 0;">Name &amp; Designation</p></div>
    #                 <div>Approved By <p style="margin: 0;">Name &amp; Designation</p></div>
    #             </div>
    #         </td></tr>
    #         <tr class="row32">
    #             <!-- <td class="column0">&nbsp;</td>
    #             <td class="column17">&nbsp;</td>
    #             <td class="column18">&nbsp;</td>
    #             <td class="column19">&nbsp;</td> -->
    #         </tr>
    #         <tr class="row33">
    #             <!-- <td class="column0">&nbsp;</td>
    #             <td class="column17">&nbsp;</td>
    #             <td class="column18">&nbsp;</td>
    #             <td class="column19">&nbsp;</td> -->
    #         </tr>
    #         <tr class="row34" style="height:15px">
    #             <!-- <td class="column0">&nbsp;</td>
    #             <td class="column17">&nbsp;</td>
    #             <td class="column18">&nbsp;</td>
    #             <td class="column19">&nbsp;</td> -->
    #         </tr>
    #         <tr class="row35">
    #             <td class="column1 style42 s style44" colspan="16" style="text-align: left;border: 1px dotted black;vertical-align: bottom;padding-left: 0px;border-right: 2px solid #000000 !important;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-left: 2px solid #000000 !important;border-bottom: 1px solid #000000 !important;border-top: 2px solid #000000 !important;">Note :</td>
    #         </tr>
    #         <tr class="row36">
    #             <td class="column1 style11 n" style="text-align: right;border: 1px dotted black;vertical-align: top;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">1&nbsp;</td>
    #             <td class="column2 style48 s style49" colspan="15" style="text-align: left;border: 1px dotted black;vertical-align: bottom;padding-left: 0px;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">The result listed refers only to the tested sample and applicable parameter(s)</td>
    #         </tr>
    #         <tr class="row37" style="height:15px;">
    #             <td class="column1 style11 n" style="text-align: right;border: 1px dotted black;vertical-align: top;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">2&nbsp;</td>
    #             <td class="column2 style50 s style51" colspan="15" style="text-align: left;border: 1px dotted black;vertical-align: bottom;padding-left: 0px;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">This report is not to be reproduced except in full, without written approval of the GWEL Laboratory</td>
    #         </tr>
    #         <tr class="row38" style="height:15px;">
    #             <td class="column1 style11 n" style="text-align: right;border: 1px dotted black;vertical-align: top;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">3&nbsp;</td>
    #             <td class="column2 style52 s style53" colspan="15" style="text-align: left;border: 1px dotted black;vertical-align: bottom;padding-left: 0px;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">Remaining sample after test will be retained for 7 days from release of test certificate for external samples and others as per QTP guidelines.</td>
    #         </tr>
    #         <tr class="row39">
    #             <td class="column1 style12 n" style="text-align: right;border: 1px dotted black;vertical-align: bottom;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">4&nbsp;</td>
    #             <td class="column2 style54 s style55" colspan="15" style="text-align: left;border: 1px dotted black;vertical-align: bottom;padding-left: 0px;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">Reported GCV values are without acid correction.</td>
    #         </tr>
    #         <tr class="row40">
    #             <td class="column1 style12 n" style="text-align: right;border: 1px dotted black;vertical-align: bottom;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">5&nbsp;</td>
    #             <td class="column2 style48 s style49" colspan="15" style="text-align: left;border: 1px dotted black;vertical-align: bottom;padding-left: 0px;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-bottom: 1px solid #000000 !important;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;border-right: 2px solid #000000 !important;">&nbsp;Sampling not carried out by laboratory.</td>
    #         </tr>
    #         <tr class="row41">
    #             <td class="column1 style13 n" style="text-align: right;border: 1px dotted black;vertical-align: bottom;border-bottom: none #000000;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-top: 1px solid #000000 !important;border-left: 2px solid #000000 !important;border-right: 1px solid #000000 !important;">6&nbsp;</td>
    #             <td class="column2 style14 s style49" colspan="15" style="text-align: left;border: 1px dotted black;vertical-align: bottom;padding-left: 0px;border-bottom: 1px solid #000000 !important;border-right: 2px solid #000000 !important;font-weight: bold;color: #000000;font-family: 'Calibri';font-size: 12px;background-color: #FFFFFF;border-top: 1px solid #000000 !important;border-left: 1px solid #000000 !important;">Details of the sample provided by the customer marked with &quot;#&quot;</td>
    #         </tr>
    #         <tr class="row42">
    #             <td class="column1" colspan="16" style="border: 1px dotted black;">End of report</td>
    #         </tr>
    #         </tbody>
    #     </table>
    # </body>
    # </html>
    # """

    # file = str(datetime.now().strftime("%d-%m-%Y"))
    # store_data = os.path.join(os.getcwd(),"static_server", "gmr_ai", file)
    # os.umask(0)
    # os.makedirs(store_data, exist_ok=True, mode=0o777)

    # pdf_name = datetime.now().strftime("%d-%m-%Y%H%M%S")

    # # generating pdf based on above HTML
    # HTML(string=html_template).write_pdf(
    #     f"{store_data}/daily_coal_bunker_{pdf_name}.pdf",
    #     stylesheets=[CSS(os.path.join(os.getcwd(), "helpers", "styleextrabunker.css"))],
    # ) 

    # return {"data": f"{store_data}/daily_coal_bunker_{pdf_name}.pdf"}

