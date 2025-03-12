import datetime
from helpers.logger import console_logger
from dateutil import tz
import os, sys

def convertToTime(seconds, without_seconds=False):
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    if hour != 0:
        if without_seconds:
            return "%d hr %d min %d sec" % (hour, minutes, seconds)
        else:
            return "%d hr %d min" % (hour, minutes)
    elif minutes != 0:
        return "%d min %d sec" % (minutes, seconds)
    else:
        return "%d sec" % (seconds)


def convertToSeconds(time):
    for (index, values) in enumerate(time.split(":")):
        if index == 0:
            hours_to_seconds = int(values) * 3600
        elif index == 1:
            minutes_to_seconds = int(values) * 60
        else:
            seconds = int(values)

    return hours_to_seconds + minutes_to_seconds + seconds


def convertToHours(seconds):
    return "%02d" % (seconds / 3600)


def convertToMinutes(seconds):
    return "%02d" % (seconds / 60)

def convert_to_utc_format(date_time, format, timezone= "Asia/Kolkata", start = True):
    to_zone = tz.gettz(timezone)
    _datetime = datetime.datetime.strptime(date_time, format)

    if not start:
        _datetime =_datetime.replace(hour=23,minute=59)
    return _datetime.replace(tzinfo=to_zone).astimezone(datetime.timezone.utc).replace(tzinfo=None)

def error_handler(e):
    console_logger.debug(e)
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    console_logger.debug((exc_type, fname, exc_tb.tb_lineno))
    console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
    return e

def get_financial_year(datestring):
    try:
        date = convert_to_utc_format(datestring, "%Y-%m-%d").date()
        year_of_date = date.year
        financial_year_start_date = datetime.date(year_of_date, 4, 1)
        if date < financial_year_start_date:
            financial_year_start_date = datetime.date(year_of_date - 1, 4, 1)
        financial_year_end_date = datetime.date(financial_year_start_date.year + 1, 3, 31)
        return {
            "start_date": financial_year_start_date.strftime("%Y-%m-%d"),
            "end_date": financial_year_end_date.strftime("%Y-%m-%d")
        }
    except Exception as e:
        error_handler(e)