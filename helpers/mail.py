from email.mime.base import MIMEBase
import pandas as pd
import smtplib, ssl
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
from email.utils import formataddr
import datetime
from helpers.logger import console_logger
import cryptocode
import json

# def send_email(sender_email, subject, password, smtp_host, smtp_port, receiver_email, body, file_path):
#     # Create a MIMEMultipart object for the email

#     try:
#         # Create a MIMEMultipart object for the email
#         message = MIMEMultipart()
#         message["Subject"] = subject
#         message["From"] = sender_email
#         message["To"] = receiver_email

#         # Attach the body of the email
#         message.attach(MIMEText(body, "plain"))

#         # Attach the file
#         with open(file_path, "rb") as attachment:
#             part = MIMEBase("application", "octet-stream")
#             part.set_payload(attachment.read())
#         encoders.encode_base64(part)
#         part.add_header(
#             "Content-Disposition",
#             f"attachment; filename= {file_path}",
#         )
#         message.attach(part)

#         # Connect to SMTP server and send email
#         context = ssl.create_default_context()
#         with smtplib.SMTP(smtp_host, smtp_port) as server:
#             server.starttls(context=context)
#             server.login(sender_email, password)
#             server.sendmail(sender_email, receiver_email, message.as_string())
#         console_logger.debug("Email Sent!")
#     except Exception as e:
#         console_logger.error(f"Failed to send email: {e}")



# def send_email(sender_email, subject, password, smtp_host, smtp_port, receiver_email, body, file_path):
#     try:
#         # Create a MIMEMultipart object for the email
#         message = MIMEMultipart()
#         message["Subject"] = subject
#         message["From"] = sender_email
#         message["To"] = receiver_email

#         # Attach the body of the email
#         if file_path:
#             message.attach(MIMEText(body, "plain"))
#         else:
#             message.attach(MIMEText(body, "html"))

#         # Attach the file
#         if file_path:
#             with open(file_path, "rb") as attachment:
#                 part = MIMEBase("application", "octet-stream")
#                 part.set_payload(attachment.read())
#             encoders.encode_base64(part)
#             part.add_header(
#                 "Content-Disposition",
#                 f"attachment; filename={file_path.rsplit('/', 1)[-1]}",
#             )
#             message.attach(part)

#         # Connect to SMTP server and send email
#         context = ssl.create_default_context()
#         with smtplib.SMTP(smtp_host, smtp_port) as server:
#             server.ehlo()  # Can be omitted
#             server.starttls()
#             server.ehlo()  # Can be omitted
#             server.login(sender_email, password)
#             server.sendmail(sender_email, receiver_email, message.as_string())
#             console_logger.debug("Email Sent!")
#             server.quit()
#     except smtplib.SMTPException as e:
#         console_logger.error(f"SMTP error occurred: {e}")
#     except FileNotFoundError as e:
#         console_logger.error(f"File not found: {e}")
#     except Exception as e:
#         console_logger.error(f"Failed to send email: {e}")

def send_email(sender_email, subject, password, smtp_host, smtp_port, receiver_email, body, file_path, cc_list, bcc_list):
    try:
        # Create a MIMEMultipart object for the email
        message = MIMEMultipart()
        message["Subject"] = subject
        message["From"] = formataddr((str(Header('GMR', 'utf-8')), 'contact@easemyai.com'))  
        message["To"] = ', '.join(receiver_email)
        if cc_list:
            message["Cc"] = ', '.join(cc_list)
        if bcc_list:
            message["Bcc"] = ', '.join(bcc_list)

        # file_data = json.loads(file_path)

        console_logger.debug(file_path)

        # Attach the body of the email
        if file_path:
            message.attach(MIMEText(body, "plain"))
        else:
            message.attach(MIMEText(body, "html"))

        console_logger.debug(type(file_path))

        # Attach the file
        if file_path:
            console_logger.debug("inside file path")
            if isinstance(file_path, dict):
                console_logger.debug("inside dict")
                for each_file_path in file_path.values():
                    try:
                        console_logger.debug(each_file_path)
                        with open(each_file_path, "rb") as attachment:
                            part = MIMEBase("application", "octet-stream")
                            part.set_payload(attachment.read())
                        encoders.encode_base64(part)
                        part.add_header(
                            "Content-Disposition",
                            f"attachment; filename={each_file_path.rsplit('/', 1)[-1]}",
                        )
                        message.attach(part)
                        # file_name=each_file_path.split("/")[-1]
                        # part = MIMEBase('application', "octet-stream")
                        # part.set_payload(open(each_file_path, "rb").read())

                        # Encoders.encode_base64(part)
                        # part.add_header('Content-Disposition', 'attachment' ,filename=file_name)
                        # msg.attach(part)
                    except:
                        print("could not attache file")
            else:    
                with open(file_path, "rb") as attachment:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(attachment.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f"attachment; filename={file_path.rsplit('/', 1)[-1]}",
                )
                message.attach(part)
        
        # if embedded_image:    
        #     # Attach embedded image
        #     with open('path/to/image.png', 'rb') as img:
        #         img_part = MIMEBase('image', 'png', filename='image.png')
        #         img_part.set_payload(img.read())
        #         encoders.encode_base64(img_part)
        #         img_part.add_header('Content-ID', '<image1>')
        #         img_part.add_header('Content-Disposition', 'inline', filename='image.png')
        #         message.attach(img_part)


        # Combine all recipients for the sendmail method
        recipients = receiver_email + cc_list + bcc_list
        console_logger.debug(f"Recipients: {recipients}")

        # Connect to SMTP server and send email
        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()  # Can be omitted
            server.starttls()
            server.ehlo()  # Can be omitted
            server.login(sender_email, password)
            # server.sendmail(sender_email, receiver_email, message.as_string())
            server.sendmail(sender_email, recipients, message.as_string())
            console_logger.debug("Email Sent!")
            server.quit()
    except smtplib.SMTPException as e:
        console_logger.error(f"SMTP error occurred: {e}")
    # except FileNotFoundError as e:
    #     console_logger.error(f"File not found: {e}")
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
    console_logger.debug(body)
    message.attach(MIMEText(body, 'html'))
    context = ssl.create_default_context()
    with smtplib.SMTP(payload.dict()["Smtp_host"], payload.dict()["Smtp_port"]) as server:
        server.starttls(context=context)
        server.login(payload.dict()["Smtp_user"], payload.dict()["Smtp_password"])
        # server.sendmail(sender_email, receiver_email, message.as_string())
        server.sendmail("contact@easemyai.com", payload.dict()["Smtp_user"], message.as_string())
    console_logger.debug("Email Sent!")
    return "success"