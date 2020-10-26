import smtplib
from string import Template
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import socket
import os
from . import utils

# print("CWD: ", os.getcwd())
config = utils.get_config('C:\Projects\config.ini')

from_email = config['email']['from_email']
to_email = config['email']['to_email']
email_user = config['email']['user']
password = config['email']['password']
smtp_server = config['email']['server']



def get_contacts(filename):
    """Return the names and emails of email recipients from file"""
    names = []
    emails = []
    with open(filename, mode='r', encoding='utf-8') as contacts_file:
        for a_contact in contacts_file:
            names.append(a_contact.split()[0])
            emails.append(a_contact.split()[1])
#     print(names, emails)
    return names, emails


def read_template(filename):
    """Return the template of the email text"""
    with open(filename, 'r', encoding='utf-8') as template_file:
        template_file_content = template_file.read()
    return Template(template_file_content)

def send_email(contacts_file,mssg_file,subject=None,e=None):
    """
    Send an emil to recipients in contacts_file, containing the message from mssg_file

    Keyword arguments:
    contacts_file -- file with contact info
    mssg_file -- file with main message
    subject -- subject line (default None)
    e -- Error (default None)

    """

    s = smtplib.SMTP(smtp_server)
    s.starttls()
    s.login(email_user, password)


    names, emails = get_contacts(contacts_file)  # read contacts
    message_template = read_template(mssg_file)

    for name, email in zip(names, emails):
#         print(name, email)
        msg = MIMEMultipart()       # create a message

            # add in the actual person name to the message template
        message = message_template.substitute(PERSON_NAME=name.title(), ERROR=e)

            # Prints out the message body for our sake
        print(message)

            # setup the parameters of the message
        msg['From']=from_email
        msg['To']=email
        msg['Subject']=subject

            # add in the message body
        msg.attach(MIMEText(message, 'plain'))

            # send the message via the server set up earlier.
        s.send_message(msg)
        del msg

        # Terminate the SMTP session and close the connection
    s.quit()

