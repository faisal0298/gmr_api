from Crypto.Cipher import AES
import base64
import json
import requests
from helpers.logger import console_logger
import os, sys

KEY = b'1DFGS-3456@$G@U98LQCD2XP'  
IV = b'GH$@7T-TMAPT@X0P'
URI = "https://igicoalapi.vvsindia.com/api/ReceiveSampleResult/GetResultReceiveSample"

def pad(text):
    """Pad the text to make it a multiple of 16 bytes."""
    return text + (16 - len(text) % 16) * chr(16 - len(text) % 16)

def unpad(text):
    """Remove the padding from the text."""
    return text[:-ord(text[-1])]

def encrypt(plain_text):
    cipher = AES.new(KEY, AES.MODE_CBC, IV)
    padded_text = pad(plain_text)
    encrypted = cipher.encrypt(padded_text.encode('utf-8'))
    return base64.b64encode(encrypted).decode('utf-8')

def decrypt(cipher_text):
    cipher = AES.new(KEY, AES.MODE_CBC, IV)
    decoded = base64.b64decode(cipher_text)
    decrypted = cipher.decrypt(decoded).decode('utf-8')
    return unpad(decrypted)

def call_api(message):
    try:
        payload = json.dumps({"X_KEY":encrypt(json.dumps(message))})
        proxies = {
            "http": None,
            "https": None
        }
        headers = {
            'Content-Type': 'application/json'
        }
        response = requests.request("POST",url=URI, headers=headers, data=payload, proxies=proxies)
        if response.status_code != 200:
            return None
        return decrypt(json.loads(response.content).get("X_KEY"))
    except Exception as e:
        success = False
        console_logger.debug("----- call api igi helpers error -----",e)
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        console_logger.debug(exc_type, fname, exc_tb.tb_lineno)
        console_logger.debug("Error {} on line {} ".format(e, sys.exc_info()[-1].tb_lineno))
        success = e