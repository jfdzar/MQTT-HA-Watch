import smtplib
from email.message import EmailMessage
import time
import logging
import json


def send_email(content, subject, mail_from, mail_to):
    """ Send Email with the function parameters """

    logging.info("Sending Message")
    msg = EmailMessage()
    msg.set_content(content)
    msg['Subject'] = subject
    msg['From'] = mail_from
    msg['To'] = mail_to

    # Send the message via our own SMTP server.
    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.starttls()
    with open('include/credentials.json', 'r') as file:
        credentials = json.load(file)
    pwd = credentials['pass']
    username = credentials['username']

    # Authentication
    s.login(username, pwd)
    s.send_message(msg)
    time.sleep(5)
    s.close()
    # s.quit()
    logging.info("Email was correctly executed")


if __name__ == '__main__':

    logging.basicConfig(
        format='%(asctime)s: %(message)s',
        level=logging.INFO,
        datefmt='%H:%M:%S')

    with open('include/credentials.json', 'r') as f:
        mail_credentials = json.load(f)

    msg_content = 'Test E-Mail from Module'
    msg_subject = 'E-Mail Module Test'
    msg_from = 'Module Test'
    send_email(msg_content, msg_subject, msg_from,
               mail_credentials['email'])
