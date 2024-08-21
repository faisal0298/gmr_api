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
            c.drawImage(picture_path, 180, 400, width, height, mask="auto") # a4 normal
            # c.drawImage(picture_path, 300, 300, width, height, mask="auto") # a4 landscape
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
        header_data += "<h1 style='color: #3a62ff; margin: 2px auto; font-size: 15px;text-align:center;'>Daily Specific Coal Consumption Report</h1><br><br>"
        header_data += "</div>"
        header_data += "<div class='headerChild' style='display: flex; justify-content: space-between; margin-top:0px; font-size:16px;'>"
        header_data += "<div class='header_left' style='width:40%; font-size: 12px; line-height:10px;'>"
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
        #     header_data += f"<p style='margin: 5px auto;'> <span style='font-weight: 600; font-size: 12px'> Branch Name -</span> {template_data['sitename']}</p>"
        # if template_data.get("managername") != None:
        #     header_data += f"<p style='margin: 5px auto;'> <span style='font-weight:  600; font-size: 12px'> Branch Manager -</span> {template_data['managername']}</p>"

        # if (
        #     template_data.get("addressline1") != None
        #     or template_data.get("addressline2") != None
        #     or template_data.get("addressline3") != None
        # ):
        #     header_data += "<div style='display:flex; width: 280px; font-size: 12px'>"
        #     header_data += "<div style='font-weight: 600; width: 80px; font-size: 12px'> Address : </div>"
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
        #     header_data += "<h2 style='font-style:bold; font-size: 12px; margin: 5px; text-align: right;line-height:20px;'>RDX</h2>"
            text_watermark_pdf = "GMR"
        else:
        #     header_data += f"<h2 style='font-style:bold; font-size: 12px; margin: 5px; text-align: right;line-height:20px;'>{template_data['title'].upper()}</h2>"
            text_watermark_pdf = template_data["title"].upper()
        # header_data += "<div style='display: flex; width: 280px; justify-content: space-between; font-size: 12px;line-height:20px;margin-top:18px;'>"
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
        header_data += f"<div style='text-align: left; margin: 1px; font-weight: 600; font-size: 14px; width: 280px; display: flex; line-height:10px;'> <span style='margin:0px; color:green; width: 100%;'>Report date :</span> <span style='margin:0px; width:100%; padding-left:1px;'> {datetime.strptime(month_date,'%Y-%m-%d').strftime('%d %B %Y')}</span></div>"
        # header_data += f"<div style='text-align: left; margin: 1px; font-weight: 600; font-size: 14px; width: 280px;  display: flex;line-height:20px;'> <p style='margin:0px; color:green;width: 100px;'>Generated date :</p> <p style='margin:0px; width:220px; padding-left:3px;'> {current_date_time}</p></div>"
        header_data += "</div>"
        header_data += "</div>"
        header_data += "</div>"
        header_data += "</div>"

        return header_data, encoded_logo_image, text_watermark_pdf
    except Exception as e:
        console_logger.debug(e)

def annotate_values(x, y):
    """Function to annotate each point with its value."""
    for i, j in zip(x, y):
        plt.text(i, j, f"{j:.2f}", fontsize=9, ha="right", va="bottom")

def specific_coal_consumption_graph_unit1(fetchtableData):
    try:
        plt.figure(figsize=(10, 6))
        plt.plot(
            fetchtableData["Unit 1"]["label"],
            fetchtableData["Unit 1"]["specific_coal"],
            marker="o",
            linestyle="-",
            color="blue",
            label="Unit 1",
        )
        plt.title("Specific Coal Consumption (TPH/MW)")
        plt.xlabel("Time (hours)")
        plt.ylabel("Specific Coal Consumption")
        plt.xticks(ticks=range(0, 24), labels=[f"{i}" for i in range(0, 24)])
        # plt.legend()
        plt.grid(True)
        annotate_values(fetchtableData["Unit 1"]["label"], fetchtableData["Unit 1"]["specific_coal"])
        # plt.savefig("specific_coal_consumption_unit_1.png")
        file = "reports_img"
        # store_file = f"static_server/gmr_ai/{file}"
        store_file = os.path.join(os.getcwd(),"static_server", "gmr_ai", file)
        os.umask(0)
        os.makedirs(store_file, exist_ok=True, mode=0o777)
        unit1_image_total_file = f"specific_coal_consumption_unit_1_{random_string}.png"
        plt.savefig(f"{store_file}/{unit1_image_total_file}")
        unit_1_image_total_file_name = f"{store_file}/{unit1_image_total_file}"
        plt.close()

        unit_1_encoded_bar_chart_profit_loss = encoded_data(unit_1_image_total_file_name)

        
        return unit_1_encoded_bar_chart_profit_loss
    except Exception as e:
        console_logger.debug(e)

def specific_coal_consumption_graph_unit2(fetchtableData):
    try:
        x_data = fetchtableData["Unit 2"]["label"]
        y_data = fetchtableData["Unit 2"]["specific_coal"]
        min_length = min(len(x_data), len(y_data))
        x_data = x_data[:min_length]
        y_data = y_data[:min_length]
        console_logger.debug(fetchtableData)
        # Plotting for Unit 2
        plt.figure(figsize=(10, 6))
        # plt.plot(
        #     fetchtableData["Unit 2"]["label"],
        #     fetchtableData["Unit 2"]["specific_coal"],
        #     marker="o",
        #     linestyle="-",
        #     color="blue",
        #     label="Unit 2",
        # )
        plt.plot(
            x_data,
            y_data,
            marker="o",
            linestyle="-",
            color="blue",
            label="Unit 2",
        )
        plt.title("Specific coal consumption (TPH/MW)")
        plt.xlabel("Time (hours)")
        plt.ylabel("Specific Coal Consumption")
        plt.xticks(ticks=range(0, 24), labels=[f"{i}" for i in range(0, 24)])
        # plt.legend()
        plt.grid(True)
        # annotate_values(fetchtableData["Unit 2"]["label"], fetchtableData["Unit 2"]["specific_coal"])
        annotate_values(x_data, y_data)
        # plt.savefig("specific_coal_consumption_unit_2.png")
        # plt.close()
        file = "reports_img"
        # store_file = f"static_server/gmr_ai/{file}"
        store_file = os.path.join(os.getcwd(),"static_server", "gmr_ai", file)
        os.umask(0)
        os.makedirs(store_file, exist_ok=True, mode=0o777)
        unit2_image_total_file = f"specific_coal_consumption_unit_2_{random_string}.png"
        plt.savefig(f"{store_file}/{unit2_image_total_file}")
        uni2_image_total_file_name = f"{store_file}/{unit2_image_total_file}"
        plt.close()

        # Show plot
        # plt.show()
        unit2_encoded_bar_chart_profit_loss = encoded_data(uni2_image_total_file_name)
        return unit2_encoded_bar_chart_profit_loss
    except Exception as e:
        console_logger.debug(e)

def length_excluding_trailing_zeros(data):
    while data and data[-1] == 0:
        data.pop()
    return len(data)

def generate_html_table(unit_name, unit_data):
    try:
        per_data = f"<span style='font-size: 10px; font-weight: 600'>#{unit_name}</span>"
        per_data += "<table class='logistic_report_data' style='width: 100%; margin: 5px; text-align: center; border-spacing: 0px; border: 1px solid lightgray;'>"
        per_data += (
            "<thead style='background-color: #3a62ff; color: #ffffff; height: 5px'>"
        )
        per_data += "<tr style='height: 5px;'>"
        per_data += "<th class='logic_table_th' style='font-size: 8px;'>Time</th>"
        per_data += "<th class='logic_table_th' style='font-size: 8px;'>Load(MW)</th>"
        per_data += "<th class='logic_table_th' style='font-size: 8px;'>Coal Flow(TPH)</th>"
        per_data += "<th class='logic_table_th' style='font-size: 8px;'>Specific coal consumption (TPH/MW)</th>"
        per_data += "</tr></thead><tbody style='border: 1px solid gray;'>"
        for label, generation_tag, consumption_tag, specific_coal in zip(unit_data['label'], unit_data['generation_tag'], unit_data['consumption_tag'], unit_data['specific_coal']):
            per_data += f"<tr style='height: 5px;'>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 8px; font-weight: 600;'> {label}:00</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 8px; font-weight: 600;'> {generation_tag}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 8px; font-weight: 600;'> {consumption_tag}</span></td>"
            per_data += f"<td class='logic_table_td' style='text-align: center;'><span style='font-size: 8px; font-weight: 600;'> {specific_coal}</span></td>"
            per_data += "</tr>"
        per_data += f"<tr><td class='logic_table_td' style='text-align: center; font-size: 8px;'><strong>Total</strong></td>"
        per_data += f"<td class='logic_table_td' style='text-align: center; font-size: 8px;'><strong>{round(unit_data['total_generation_sum']/length_excluding_trailing_zeros(unit_data['generation_tag']), 2) if unit_data['total_generation_sum'] and length_excluding_trailing_zeros(unit_data['generation_tag']) != 0 else 0}</strong></td>"
        per_data += f"<td class='logic_table_td' style='text-align: center;font-size: 8px;'><strong>{round(unit_data['total_consumption_sum']/length_excluding_trailing_zeros(unit_data['consumption_tag']), 2) if unit_data['total_generation_sum'] and length_excluding_trailing_zeros(unit_data['consumption_tag']) != 0 else 0}</strong></td>"
        per_data += f"<td class='logic_table_td' style='text-align: center;font-size: 8px;'><strong>{round(unit_data['total_specific_coal']/length_excluding_trailing_zeros(unit_data['specific_coal']), 2) if unit_data['total_generation_sum'] and length_excluding_trailing_zeros(unit_data['specific_coal']) != 0 else 0}</strong></td></tr>"
        per_data += "</tbody></table>"
        # per_data += "<br><br>"
        return per_data
    except Exception as e:
        console_logger.debug(e)

# def tableData(fetchtableData):
#     # try:
#     html_output = ""
#     console_logger.debug(fetchtableData)
#     for unit_name, unit_data in fetchtableData.items():
#         html_output += generate_html_table(unit_name, unit_data)
#     return html_output
    # except Exception as e:
    #     console_logger.debug(e)

def tableData(fetchtableData):
    # Initialize variables to store the HTML output for each unit
    unit1_html_output = ""
    unit2_html_output = ""

    # Loop through the data and generate HTML for each unit
    for unit_name, unit_data in fetchtableData.items():
        if unit_name == "Unit 1":
            unit1_html_output = generate_html_table(unit_name, unit_data)
        elif unit_name == "Unit 2":
            unit2_html_output = generate_html_table(unit_name, unit_data)

    # Return the HTML outputs for each unit separately
    return unit1_html_output, unit2_html_output


def generate_report_consumption(specified_date, fetchtableData):
    # try:
    payload={}
    header={}
    template_url = f"http://{host}/api/v1/base/report/template"

    response = requests.request("GET", url=template_url, headers=header, data=payload)

    template_data = json.loads(response.text)

    header_data, encoded_logo_image, text_watermark_pdf = header_data_value(template_data, specified_date)

    unit1fetchTabledata, unit2fetchTabledata = tableData(fetchtableData)

    unit1_graph = specific_coal_consumption_graph_unit1(fetchtableData)

    unit2_graph = specific_coal_consumption_graph_unit2(fetchtableData)


    if unit1_graph:
        unit1_data_graph = f'<img src="data:image/png;base64,{unit1_graph}" alt="img not present" style="margin: 1px auto; width:90%; height:90%; display:block; object-fit: scale-down;"/>'
    else:
        unit1_data_graph = f"<div style='color: #000; font-size: 12px; margin: 5px ;font-weight: 600;'><b>No data found for {datetime.strptime(specified_date,'%Y-%m-%d').strftime('%d %B %Y')}</b></div>"


    if unit2_graph:
        unit2_data_graph = f'<img src="data:image/png;base64,{unit2_graph}" alt="img not present" style="margin: 1px auto; width:90%; height:90%; display:block; object-fit: scale-down;"/>'
    else:
        unit2_data_graph = f"<div style='color: #000; font-size: 12px; margin: 5px ;font-weight: 600;'><b>No data found for {datetime.strptime(specified_date,'%Y-%m-%d').strftime('%d %B %Y')}</b></div>"
    
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
        <hr style="margin: 5px 0px;" />

        <div class="footertable" style="width:100%;margin-top:0px;">
            <div class="title" style="width: 100%; display: flex ; flex-direction:row; gap: 10px; height:20px; align-items:center">
                <p style="color: #3a62ff; font-size: 12px; margin: 0px; font-weight: 600;">
                    Daily Specific Coal Consumption Report for {datetime.strptime(specified_date,'%Y-%m-%d').strftime('%d %B %Y')}
                </p>
            </div>
                    {unit1fetchTabledata}
        </div>
        <div class="body" style="margin-top:80px;">
                <div style="color: #3a62ff; font-size: 12px; margin: 5px auto; font-weight: 600;">
                        {unit1_data_graph}
                </div>  
            </div>  

        
        <div class="footertable" style="width:100%;margin-top:0px;">
            <div class="title" style="width: 100%; display: flex ; flex-direction:row; gap: 10px; height:20px; align-items:center">
            </div>
                    {unit2fetchTabledata}
        </div>
        <div class="body" style="margin-top:80px;">
                <div style="color: #3a62ff; font-size: 12px; margin: 5px auto; font-weight: 600;">
                        {unit2_data_graph}
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
        f"{store_data}/daily_specific_coal_consumption_report_{image_name}.pdf",
        stylesheets=[CSS(os.path.join(os.getcwd(), "helpers", "consumptionstyle.css"))],
    )
    # normal pdf name 
    pdf_file = f"{store_data}/daily_specific_coal_consumption_report_{image_name}.pdf"
    # watermark pdf name
    output_file = f"{store_data}/daily_specific_coal_consumption_report__{image_name}.pdf"
    data = {}
    if template_data.get("logo_as_watermark") == "title":
        data["wm_angle"] = "vertical"
        apply_pdf_watermark(pdf_file, output_file, text_watermark=text_watermark_pdf, data=data)
        data_file = f"static_server/gmr_ai/{file}/daily_specific_coal_consumption_report__{image_name}.pdf"
        # deleting original generated pdf as we are getting here two pdf first without watermark and second with watermark
        os.remove(f"{store_data}/daily_specific_coal_consumption_report_{image_name}.pdf")
    
    elif template_data.get("logo_as_watermark") == "logo":
        data["wm_angle"] = "vertical"
        apply_pdf_watermark(pdf_file, output_file, picture_path=f'{os.path.join(os.getcwd())}/{template_data["logo_path"]}', data=data)
        data_file = f"static_server/gmr_ai/{file}/daily_specific_coal_consumption_report__{image_name}.pdf"  
        # deleting original generated pdf as we are getting here two pdf first without watermark and second with watermark
        os.remove(f"{store_data}/daily_specific_coal_consumption_report_{image_name}.pdf")
    
    elif template_data.get("logo_as_watermark") == "None":
        data_file = f"static_server/gmr_ai/{file}/daily_specific_coal_consumption_report_{image_name}.pdf"

    # deleting a temp watermark pdf which was getting generated during watermark
    for filename in Path(f"{os.path.join(os.getcwd())}/static_server/gmr_ai/{file}").glob("watermark_empty*.pdf"):
        filename.unlink()
    
    # deleting reports_img folder on static_server
    shutil.rmtree(os.path.join(os.getcwd(),"static_server", "gmr_ai", "reports_img")) 
    # recreating folder with reports_img name
    os.mkdir(os.path.join(os.getcwd(),"static_server", "gmr_ai", "reports_img"))

    return data_file
    # except Exception as e:
    #     console_logger.debug(e)
