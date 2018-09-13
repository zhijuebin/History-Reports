# coding=utf-8
import sys
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from configs.truckerpath_eld_config import email_config as config


def set_utf8():
    coding = 'utf-8'
    if sys.getdefaultencoding() != coding:
        reload(sys)
        sys.setdefaultencoding(coding)


def send_email(receivers, title, file_paths):
    sender = config.get('SENDER')
    items = receivers.strip().split(',')
    default_items = config.get('DEFAULT_RECEIVER').strip().split(',')
    if config.get('DEBUG_MODE'):
        receiver = default_items
    else:
        receiver = items + default_items
    msg = MIMEMultipart('related')
    msg['subject'] = title
    msg['from'] = sender
    msg['to'] = receivers

    file_paths = file_paths.strip().split(',')

    for file_path in file_paths:
        if not os.path.exists(file_path):
            msg['subject'] = 'ERROR FOR GENERATE DATASHEETS'
            msg_text = MIMEText('Error, {} doesn\'t not generate'.format(file_path))
            msg.attach(msg_text)
            receiver = default_items
            break

        msg_body = MIMEText(open(file_path, 'rb').read(), 'base64', 'utf-8')

        msg_body['Content-Type'] = 'application/csv'
        msg_body['Content-Disposition'] = 'attachment; ' + 'filename =' + os.path.basename(file_path)
        msg.attach(msg_body)


    while True:
        try:
            smtp = smtplib.SMTP(config.get('SERVER_IP'), 587)
            smtp.ehlo()
            smtp.starttls()
            smtp.login(config.get('LOGIN_USER'), config.get('LOGIN_PWD'))
            smtp.sendmail(sender, receiver, msg.as_string())
            smtp.quit()
        except:
            continue
        else:
            break

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print ("need param <htmlfile> <subject> <to>")
        sys.exit()
    set_utf8()
    # html文件名,title,收件人列表
    print (sys.argv[1], sys.argv[2], sys.argv[3])
    send_email(sys.argv[1], sys.argv[2], sys.argv[3])
