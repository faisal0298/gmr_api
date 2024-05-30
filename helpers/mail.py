from email.mime.base import MIMEBase
import pandas as pd
import smtplib, ssl
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import datetime
from helpers.logger import console_logger
import cryptocode

def send_email(sender_email, subject, password, smtp_host, smtp_port, receiver_email, body, file_path):
    # Create a MIMEMultipart object for the email

    try:
        # Create a MIMEMultipart object for the email
        message = MIMEMultipart()
        message["Subject"] = subject
        message["From"] = sender_email
        message["To"] = receiver_email

        # Attach the body of the email
        message.attach(MIMEText(body, "plain"))

        # Attach the file
        with open(file_path, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {file_path}",
        )
        message.attach(part)

        # Connect to SMTP server and send email
        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls(context=context)
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, message.as_string())
        console_logger.debug("Email Sent!")
    except Exception as e:
        console_logger.error(f"Failed to send email: {e}")

def send_test_email(payload):
    console_logger.debug(payload.dict())
    message = MIMEMultipart()
    message["Subject"] = "Test email to check smtp credentials"
    message["From"] = "contact@easemyai.com"
    message["To"] = payload.dict()["Smtp_user"]
    body = '''
    <html>
        <body>
        <h3>Test email to check smtp credentials</h3>
        </body>
    </html>
    '''
    message.attach(MIMEText(body, 'html'))
    context = ssl.create_default_context()
    with smtplib.SMTP(payload.dict()["Smtp_host"], payload.dict()["Smtp_port"]) as server:
        server.starttls(context=context)
        server.login(payload.dict()["Smtp_user"], payload.dict()["Smtp_password"])
        # server.sendmail(sender_email, receiver_email, message.as_string())
        server.sendmail("contact@easemyai.com", payload.dict()["Smtp_user"], message.as_string())
    console_logger.debug("Email Sent!")
    return "success"