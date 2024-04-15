import copy
import datetime
import pytz
import os,sys
import threading
import logging, xlsxwriter
from database.models import *
from helpers.logger import console_logger
from helpers.read_timezone import read_timezone_from_file
import helpers.usecase_handler
from dateutil.relativedelta import relativedelta



timezone = read_timezone_from_file()
tzInfo = pytz.timezone(timezone)

file = str(datetime.datetime.now().strftime("%d-%m-%Y"))
if not os.path.exists(os.path.join(os.getcwd(), "static_server","gmr_ai",file)):
    os.umask(0)
    os.makedirs(os.path.join(os.getcwd(), "static_server","gmr_ai",file), mode=0o777)

